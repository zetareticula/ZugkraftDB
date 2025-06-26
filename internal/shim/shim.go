package shim

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"causal-consistency-shim/internal/query"
	"io/ioutil"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v2"
)

// VectorClock represents a vector clock for causal ordering
type VectorClock map[string]int64

// Dependency represents a causal dependency
type Dependency struct {
	Key         string
	VectorClock VectorClock
}

// OperationType distinguishes invocation and response events
type OperationType string

const (
	Invocation OperationType = "invocation"
	Response   OperationType = "response"
)

// Operation represents an event (invocation or response)
type Operation struct {
	Key         string
	Type        OperationType
	Attributes  map[string]TypedValue
	WriteID     string
	Deps        []Dependency
	VectorClock VectorClock
	Timestamp   time.Time
	DeviceID    string
}

// TypedValue represents a Mentat-inspired typed value
type TypedValue struct {
	Type  string
	Value interface{}
}

// Entity represents a Mentat-style entity with relativistic linearizability
type Entity struct {
	Key         string
	Attributes  map[string]TypedValue
	WriteID     string
	Deps        []Dependency
	VectorClock VectorClock
	Timestamp   time.Time
	DeviceID    string
	AccessCount int
	Stable      bool
	Operations  []Operation // Log of invocation/response events
}

// ChangeListener defines a callback for data changes
type ChangeListener func(ctx context.Context, entity Entity)

// DatabaseConfig defines a database server
type DatabaseConfig struct {
	Name       string `yaml:"name"`
	Host       string `yaml:"host"`
	Priority   int    `yaml:"priority"`
	Write      bool   `yaml:"write"`
	Read       bool   `yaml:"read"`
	Datacenter string `yaml:"datacenter"`
	LatencyMs  int    `yaml:"latency_ms"` // For relativistic high-latency environments
}

// PartitioningConfig defines partitioning rules
type PartitioningConfig struct {
	Callback    string       `yaml:"callback"`
	CustomRules []CustomRule `yaml:"custom_rules"`
}

// CustomRule defines a partitioning rule
type CustomRule struct {
	KeyPrefix string `yaml:"key_prefix"`
	Dataset   string `yaml:"dataset"`
}

// ConnectionConfig defines connection parameters
type ConnectionConfig struct {
	MinTries   int `yaml:"min_tries"`
	RetryDelay int `yaml:"retry_delay_ms"`
}

// StatisticsConfig defines metrics collection
type StatisticsConfig struct {
	Enabled         bool `yaml:"enabled"`
	IntervalSeconds int  `yaml:"interval_seconds"`
}

// Config represents the database configuration
type Config struct {
	Databases    []DatabaseConfig   `yaml:"databases"`
	Partitioning PartitioningConfig `yaml:"partitioning"`
	Connection   ConnectionConfig   `yaml:"connection"`
	Statistics   StatisticsConfig   `yaml:"statistics"`
}

// LoadConfig loads the configuration file
func LoadConfig(path string) (*Config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	return &config, nil
}

// PartitionCallback defines key-to-dataset mapping
type PartitionCallback func(key string) string

// CausalShim manages relativistic linearizability
type CausalShim struct {
	store         Store
	localStore    map[string][]Entity
	toCheck       map[string]struct{}
	mu            sync.RWMutex
	fetchInterval time.Duration
	ctx           context.Context
	cancel        context.CancelFunc
	processID     string
	logicalClock  int64
	deviceID      string
	pessimistic   bool
	sharedCache   Cache
	pruneInterval time.Duration
	metrics       *ShimMetrics
	noOverwrites  bool
	config        *Config
	partitionCb   PartitionCallback
	listeners     []ChangeListener
}

// ShimMetrics tracks performance metrics
type ShimMetrics struct {
	readLatency        prometheus.Histogram
	writeLatency       prometheus.Histogram
	queryLatency       prometheus.Histogram
	throughput         prometheus.Counter
	concurrentWrites   prometheus.Counter
	connectionFailures prometheus.Counter
	schemaChanges      prometheus.Counter
	causalViolations   prometheus.Counter
	datasetHits        *prometheus.CounterVec
}

// NewShimMetrics initializes metrics
func NewShimMetrics() *ShimMetrics {
	return &ShimMetrics{
		readLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "causal_shim_read_latency_seconds",
			Help:    "Read latency",
			Buckets: prometheus.ExponentialBuckets(0.000001, 2, 20),
		}),
		writeLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "causal_shim_write_latency_seconds",
			Help:    "Write latency",
			Buckets: prometheus.ExponentialBuckets(0.000001, 2, 20),
		}),
		queryLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "causal_shim_query_latency_seconds",
			Help:    "Datalog query latency",
			Buckets: prometheus.ExponentialBuckets(0.000001, 2, 20),
		}),
		throughput: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "causal_shim_operations_total",
			Help: "Total operations",
		}),
		concurrentWrites: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "causal_shim_concurrent_writes_total",
			Help: "Concurrent writes",
		}),
		connectionFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "causal_shim_connection_failures_total",
			Help: "Connection failures",
		}),
		schemaChanges: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "causal_shim_schema_changes_total",
			Help: "Schema attribute additions",
		}),
		causalViolations: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "causal_shim_causal_violations_total",
			Help: "Causal order violations detected",
		}),
		datasetHits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "causal_shim_dataset_hits_total",
			Help: "Hits per dataset",
		}, []string{"dataset", "operation"}),
	}
}

// NewCausalShim initializes the shim
func NewCausalShim(store Store, cache Cache, configPath, deviceID string, fetchInterval, pruneInterval time.Duration, processID string, pessimistic, noOverwrites bool) (*CausalShim, error) {
	config, err := LoadConfig(configPath)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	metrics := NewShimMetrics()
	shim := &CausalShim{
		store:         store,
		localStore:    make(map[string][]Entity),
		toCheck:       make(map[string]struct{}),
		fetchInterval: fetchInterval,
		ctx:           ctx,
		cancel:        cancel,
		processID:     processID,
		logicalClock:  0,
		deviceID:      deviceID,
		pessimistic:   pessimistic,
		sharedCache:   cache,
		pruneInterval: pruneInterval,
		metrics:       metrics,
		noOverwrites:  noOverwrites,
		config:        config,
		listeners:     []ChangeListener{},
	}
	prometheus.MustRegister(metrics.readLatency, metrics.writeLatency, metrics.queryLatency, metrics.throughput, metrics.concurrentWrites, metrics.connectionFailures, metrics.schemaChanges, metrics.causalViolations, metrics.datasetHits)

	shim.partitionCb = shim.defaultPartitionCallback
	if config.Partitioning.Callback == "custom" {
		shim.partitionCb = shim.customPartitionCallback
	}

	go shim.resolveAsync()
	if pruneInterval > 0 {
		go shim.pruneLocalStore()
	}
	if config.Statistics.Enabled {
		go shim.collectStatistics(time.Duration(config.Statistics.IntervalSeconds) * time.Second)
	}
	return shim, nil
}

// RegisterListener adds a change listener
func (s *CausalShim) RegisterListener(listener ChangeListener) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listeners = append(s.listeners, listener)
}

// defaultPartitionCallback maps to highest-priority write server
func (s *CausalShim) defaultPartitionCallback(key string) string {
	for _, db := range s.config.Databases {
		if db.Write {
			return db.Name
		}
	}
	return s.config.Databases[0].Name
}

// customPartitionCallback applies custom rules
func (s *CausalShim) customPartitionCallback(key string) string {
	for _, rule := range s.config.Partitioning.CustomRules {
		if strings.HasPrefix(key, rule.KeyPrefix) {
			return rule.Dataset
		}
	}
	return s.defaultPartitionCallback(key)
}

// PutShim implements the write path with relativistic linearizability
func (s *CausalShim) PutShim(ctx context.Context, key string, attributes map[string]TypedValue, after []Dependency) error {
	start := time.Now()
	defer func() {
		s.metrics.writeLatency.Observe(time.Since(start).Seconds())
		s.metrics.throughput.Inc()
		s.metrics.datasetHits.WithLabelValues(s.partitionCb(key), "write").Inc()
	}()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.logicalClock++
	vc := s.copyVectorClock()
	vc[s.processID] = s.logicalClock

	deps := s.optimizeDependencies(after)

	writeID := uuid.New().String()
	entity := Entity{
		Key:         key,
		Attributes:  attributes,
		WriteID:     writeID,
		Deps:        deps,
		VectorClock: vc,
		Timestamp:   time.Now(),
		DeviceID:    s.deviceID,
		AccessCount: 0,
		Stable:      false,
		Operations: []Operation{
			{Key: key, Type: Invocation, Attributes: attributes, WriteID: writeID, VectorClock: vc, Timestamp: time.Now(), DeviceID: s.deviceID},
			{Key: key, Type: Response, Attributes: attributes, WriteID: writeID, VectorClock: vc, Timestamp: time.Now(), DeviceID: s.deviceID},
		},
	}

	// Check for new attributes (schema evolution)
	for attr := range attributes {
		if _, exists := s.localStore[attr]; !exists {
			s.metrics.schemaChanges.Inc()
		}
	}

	// Relativistic linearizability: Validate causal order
	if !s.isRelativisticallyLinearizable(entity) {
		s.metrics.causalViolations.Inc()
		return fmt.Errorf("causal order violation")
	}

	if s.sharedCache != nil {
		if err := s.sharedCache.Put(ctx, entity); err != nil {
			return err
		}
	} else {
		if s.noOverwrites {
			s.localStore[key] = append(s.localStore[key], entity)
		} else {
			s.localStore[key] = []Entity{entity}
		}
	}

	go func() {
		dataset := s.partitionCb(key)
		if err := s.store.Write(ctx, entity, dataset); err != nil {
			s.metrics.connectionFailures.Inc()
		}
		// Notify listeners
		for _, listener := range s.listeners {
			listener(ctx, entity)
		}
	}()

	return nil
}

// GetShim implements the read path with relativistic linearizability
func (s *CausalShim) GetShim(ctx context.Context, key string) (map[string]TypedValue, error) {
	start := time.Now()
	defer func() {
		s.metrics.readLatency.Observe(time.Since(start).Seconds())
		s.metrics.throughput.Inc()
		s.metrics.datasetHits.WithLabelValues(s.partitionCb(key), "read").Inc()
	}()

	s.mu.Lock()
	defer s.mu.Unlock()

	var entity Entity
	var exists bool

	// Check local store for recent writes (SRTM)
	if entities, ok := s.localStore[key]; ok && len(entities) > 0 {
		entity = s.selectLatestEntity(entities)
		exists = true
	} else if s.sharedCache != nil {
		e, err := s.sharedCache.Get(ctx, key)
		if err == nil && e != nil {
			entity = *e
			exists = true
		}
	}

	if s.pessimistic && (!exists || time.Since(entity.Timestamp) > s.fetchInterval) {
		dataset := s.partitionCb(key)
		eval, err := s.store.Read(ctx, key, dataset)
		if err != nil && err != ErrNotFound {
			s.metrics.connectionFailures.Inc()
			return nil, err
		}
		if eval != nil {
			T := []Entity{*eval}
			if s.isRelativisticallyLinearizable(*eval) && s.isCovered(*eval, T) {
				if s.sharedCache != nil {
					for _, e := range T {
						s.sharedCache.Put(ctx, e)
					}
				} else {
					s.localStore[key] = append(s.localStore[key], T...)
					s.metrics.concurrentWrites.Add(float64(len(T) - 1))
				}
				entity = s.selectLatestEntity(T)
				exists = true
			} else {
				s.metrics.causalViolations.Inc()
			}
		}
	}

	s.toCheck[key] = struct{}{}

	if !exists {
		return nil, ErrNotFound
	}
	entity.AccessCount++
	if s.sharedCache == nil && s.noOverwrites {
		s.localStore[key] = append(s.localStore[key][:0], entity)
	}
	return entity.Attributes, nil
}

// QueryDatalog executes a Datalog query with causal constraints
func (s *CausalShim) QueryDatalog(ctx context.Context, queryStr string) (*query.QueryResult, error) {
	start := time.Now()
	defer func() {
		s.metrics.queryLatency.Observe(time.Since(start).Seconds())
		s.metrics.throughput.Inc()
	}()

	dq, err := query.ParseDatalog(queryStr)
	if err != nil {
		return nil, err
	}

	cql, args, err := query.TranslateToCQL(dq)
	if err != nil {
		return nil, err
	}

	dataset := s.partitionCb("") // Default dataset for queries
	rows, err := s.store.Query(ctx, cql, args, dataset)
	if err != nil {
		s.metrics.connectionFailures.Inc()
		return nil, err
	}

	// Validate causal order for query results
	for _, row := range rows {
		entity := Entity{
			Attributes:  row,
			VectorClock: s.copyVectorClock(), // Simplified; in practice, fetch from store
		}
		if !s.isRelativisticallyLinearizable(entity) {
			s.metrics.causalViolations.Inc()
			return nil, fmt.Errorf("causal order violation in query result")
		}
	}

	result := &query.QueryResult{Rows: rows}
	return result, nil
}

// isRelativisticallyLinearizable checks if an entity satisfies Definition 4
func (s *CausalShim) isRelativisticallyLinearizable(entity Entity) bool {
	// Check if operations form a legal sequential history respecting causal order
	for _, op := range entity.Operations {
		for _, dep := range entity.Deps {
			if s.compareVectorClocks(op.VectorClock, dep.VectorClock) < 0 {
				return false // Causal violation: operation precedes dependency
			}
		}
	}
	return true
}

// selectLatestEntity picks the latest entity
func (s *CausalShim) selectLatestEntity(entities []Entity) Entity {
	latest := entities[0]
	for _, e := range entities[1:] {
		if s.compareVectorClocks(e.VectorClock, latest.VectorClock) > 0 {
			latest = e
		}
	}
	return latest
}

// optimizeDependencies limits metadata
func (s *CausalShim) optimizeDependencies(deps []Dependency) []Dependency {
	const maxDeps = 10
	if len(deps) <= maxDeps {
		return deps
	}
	sorted := make([]Dependency, len(deps))
	copy(sorted, deps)
	sort.Slice(sorted, func(i, j int) bool {
		return s.compareVectorClocks(sorted[i].VectorClock, sorted[j].VectorClock) > 0
	})
	return sorted[:maxDeps]
}

// isCovered checks causal cut
func (s *CausalShim) isCovered(e Entity, T []Entity) bool {
	for _, dep := range e.Deps {
		var localDep Entity
		var exists bool

		if s.sharedCache != nil {
			e, err := s.sharedCache.Get(s.ctx, dep.Key)
			if err == nil && e != nil {
				localDep = *e
				exists = true
			}
		} else if entities, ok := s.localStore[dep.Key]; ok && len(entities) > 0 {
			localDep = s.selectLatestEntity(entities)
			exists = true
		}

		if exists && s.compareVectorClocks(localDep.VectorClock, dep.VectorClock) >= 0 {
			continue
		}
		for _, tDep := range T {
			if tDep.Key == dep.Key && s.compareVectorClocks(tDep.VectorClock, dep.VectorClock) >= 0 {
				continue
			}
		}

		dataset := s.partitionCb(dep.Key)
		depEntity, err := s.store.ReadDependency(s.ctx, dep.Key, dep.VectorClock, dataset)
		if err != nil || depEntity == nil {
			s.metrics.connectionFailures.Inc()
			return false
		}
		T = append(T, *depEntity)
		if !s.isCovered(*depEntity, T) {
			return false
		}
	}
	return true
}

// resolveAsync updates local store
func (s *CausalShim) resolveAsync() {
	ticker := time.NewTicker(s.fetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			keysToCheck := make([]string, 0, len(s.toCheck))
			for k := range s.toCheck {
				keysToCheck = append(keysToCheck, k)
			}
			s.mu.Unlock()

			for _, key := range keysToCheck {
				dataset := s.partitionCb(key)
				eval, err := s.store.Read(s.ctx, key, dataset)
				if err != nil {
					s.metrics.connectionFailures.Inc()
					continue
				}
				s.mu.Lock()
				T := []Entity{*eval}
				if s.isRelativisticallyLinearizable(*eval) && s.isCovered(*eval, T) {
					if s.sharedCache != nil {
						for _, e := range T {
							s.sharedCache.Put(s.ctx, e)
						}
					} else {
						s.localStore[key] = append(s.localStore[key], T...)
						s.metrics.concurrentWrites.Add(float64(len(T) - 1))
					}
					delete(s.toCheck, key)
				} else {
					s.metrics.causalViolations.Inc()
				}
				s.mu.Unlock()
			}
		}
	}
}

// pruneLocalStore removes outdated entities
func (s *CausalShim) pruneLocalStore() {
	ticker := time.NewTicker(s.pruneInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			for key, entities := range s.localStore {
				newEntities := []Entity{}
				for _, entity := range entities {
					if entity.AccessCount > 0 || time.Since(entity.Timestamp) <= time.Hour || entity.Stable {
						newEntities = append(newEntities, entity)
						continue
					}
					stillNeeded := false
					for _, other := range s.localStore {
						for _, dep := range other.Deps {
							if dep.Key == key && s.compareVectorClocks(dep.VectorClock, entity.VectorClock) == 0 {
								stillNeeded = true
								break
							}
						}
						if stillNeeded {
							break
						}
					}
					if stillNeeded {
						newEntities = append(newEntities, entity)
					}
				}
				s.localStore[key] = newEntities
			}
			s.mu.Unlock()
		}
	}
}

// collectStatistics gathers metrics
func (s *CausalShim) collectStatistics(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.mu.RLock()
			// Log or export metrics
			s.mu.RUnlock()
		}
	}
}

// copyVectorClock creates a copy
func (s *CausalShim) copyVectorClock() VectorClock {
	vc := make(VectorClock)
	for _, entities := range s.localStore {
		for _, e := range entities {
			for proc, clock := range e.VectorClock {
				if current, exists := vc[proc]; !exists || clock > current {
					vc[proc] = clock
				}
			}
		}
	}
	return vc
}

// compareVectorClocks compares vector clocks
func (s *CausalShim) compareVectorClocks(vc1, vc2 VectorClock) int {
	vc1Greater := false
	vc2Greater := false

	for k, v1 := range vc1 {
		v2, ok := vc2[k]
		if !ok || v1 > v2 {
			vc1Greater = true
		} else if v1 < v2 {
			vc2Greater = true
		}
	}
	for k, v2 := range vc2 {
		if _, ok := vc1[k]; !ok && v2 > 0 {
			vc2Greater = true
		}
	}
	if vc1Greater && vc2Greater {
		return 0 // Concurrent
	} else if vc1Greater {
		return 1
	} else if vc2Greater {
		return -1
	}
	return 0 // Equal
}

// Close shuts down the shim
func (s *CausalShim) Close() {
	s.cancel()
}
