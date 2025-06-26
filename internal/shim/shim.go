package shim

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

// VectorClock represents a vector clock for tracking causality
type VectorClock map[string]int64 // ProcessID -> LogicalClock

// Dependency represents a causal dependency
type Dependency struct {
	Key         string
	VectorClock VectorClock
}

// Write represents a write operation with causality metadata
type Write struct {
	Key         string
	Value       string
	WriteID     string       // Unique identifier
	Deps        []Dependency // Explicit causality dependencies
	VectorClock VectorClock  // Vector clock for this write
	Timestamp   time.Time
	AccessCount int // For pruning
}

// CausalShim manages causal consistency atop an ECDS
type CausalShim struct {
	store         Store
	localStore    map[string]Write    // Local store as a causal cut
	toCheck       map[string]struct{} // Keys to check for updates
	mu            sync.RWMutex
	fetchInterval time.Duration
	ctx           context.Context
	cancel        context.CancelFunc
	processID     string        // Unique shim identifier
	logicalClock  int64         // Local logical clock
	pessimistic   bool          // Enable pessimistic reads
	sharedCache   Cache         // Optional shared cache (e.g., Redis)
	pruneInterval time.Duration // Interval for pruning local store
	metrics       *ShimMetrics  // Prometheus metrics
}

// ShimMetrics tracks performance metrics
type ShimMetrics struct {
	readLatency  prometheus.Histogram
	writeLatency prometheus.Histogram
	throughput   prometheus.Counter
}

// NewShimMetrics initializes Prometheus metrics
func NewShimMetrics() *ShimMetrics {
	return &ShimMetrics{
		readLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "causal_shim_read_latency_seconds",
			Help:    "Read latency of the causal shim",
			Buckets: prometheus.ExponentialBuckets(0.000001, 2, 20),
		}),
		writeLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "causal_shim_write_latency_seconds",
			Help:    "Write latency of the causal shim",
			Buckets: prometheus.ExponentialBuckets(0.000001, 2, 20),
		}),
		throughput: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "causal_shim_operations_total",
			Help: "Total number of operations processed by the shim",
		}),
	}
}

// NewCausalShim initializes the shim
func NewCausalShim(store Store, cache Cache, fetchInterval, pruneInterval time.Duration, processID string, pessimistic bool) *CausalShim {
	ctx, cancel := context.WithCancel(context.Background())
	metrics := NewShimMetrics()
	shim := &CausalShim{
		store:         store,
		localStore:    make(map[string]Write),
		toCheck:       make(map[string]struct{}),
		fetchInterval: fetchInterval,
		ctx:           ctx,
		cancel:        cancel,
		processID:     processID,
		logicalClock:  0,
		pessimistic:   pessimistic,
		sharedCache:   cache,
		pruneInterval: pruneInterval,
		metrics:       metrics,
	}
	prometheus.MustRegister(metrics.readLatency, metrics.writeLatency, metrics.throughput)
	go shim.resolveAsync()
	if pruneInterval > 0 {
		go shim.pruneLocalStore()
	}
	return shim
}

// PutShim implements the write path with explicit causality
func (s *CausalShim) PutShim(ctx context.Context, key, value string, after []Dependency) error {
	start := time.Now()
	defer func() {
		s.metrics.writeLatency.Observe(time.Since(start).Seconds())
		s.metrics.throughput.Inc()
	}()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Increment logical clock
	s.logicalClock++
	vc := s.copyVectorClock()
	vc[s.processID] = s.logicalClock

	// Optimize metadata for explicit causality
	deps := s.optimizeDependencies(after)

	write := Write{
		Key:         key,
		Value:       value,
		WriteID:     uuid.New().String(),
		Deps:        deps,
		VectorClock: vc,
		Timestamp:   time.Now(),
		AccessCount: 0,
	}

	// Update local store or shared cache
	if s.sharedCache != nil {
		if err := s.sharedCache.Put(ctx, write); err != nil {
			return err
		}
	} else {
		s.localStore[key] = write
	}

	// Asynchronously persist to ECDS
	go s.store.Write(ctx, write)

	return nil
}

// GetShim implements the read path (causal or pessimistic)
func (s *CausalShim) GetShim(ctx context.Context, key string) (string, error) {
	start := time.Now()
	defer func() {
		s.metrics.readLatency.Observe(time.Since(start).Seconds())
		s.metrics.throughput.Inc()
	}()

	s.mu.Lock()
	defer s.mu.Unlock()

	var write Write
	var exists bool

	// Read from shared cache or local store
	if s.sharedCache != nil {
		w, err := s.sharedCache.Get(ctx, key)
		if err != nil && err != ErrNotFound {
			return "", err
		}
		if w != nil {
			write = *w
			exists = true
		}
	} else {
		write, exists = s.localStore[key]
	}

	if s.pessimistic && (!exists || time.Since(write.Timestamp) > s.fetchInterval) {
		// Pessimistic read: fetch latest from ECDS
		eval, err := s.store.Read(ctx, key)
		if err != nil && err != ErrNotFound {
			return "", err
		}
		if eval != nil {
			T := []Write{*eval}
			if s.isCovered(*eval, T) {
				if s.sharedCache != nil {
					for _, w := range T {
						s.sharedCache.Put(ctx, w)
					}
				} else {
					for _, w := range T {
						s.localStore[w.Key] = w
					}
				}
				write = *eval
				exists = true
			}
		}
	}

	// Add key to toCheck for async resolution
	s.toCheck[key] = struct{}{}

	if !exists {
		return "", ErrNotFound
	}
	write.AccessCount++
	if s.sharedCache == nil {
		s.localStore[key] = write // Update access count
	}
	return write.Value, nil
}

// optimizeDependencies limits metadata for explicit causality
func (s *CausalShim) optimizeDependencies(deps []Dependency) []Dependency {
	const maxDeps = 10 // Reflects paper's single-digit to tens
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

// isCovered checks if a write forms a causal cut
func (s *CausalShim) isCovered(w Write, T []Write) bool {
	for _, dep := range w.Deps {
		var localDep Write
		var exists bool

		if s.sharedCache != nil {
			w, err := s.sharedCache.Get(s.ctx, dep.Key)
			if err == nil && w != nil {
				localDep = *w
				exists = true
			}
		} else {
			localDep, exists = s.localStore[dep.Key]
		}

		if exists && s.compareVectorClocks(localDep.VectorClock, dep.VectorClock) >= 0 {
			continue
		}
		for _, tDep := range T {
			if tDep.Key == dep.Key && s.compareVectorClocks(tDep.VectorClock, dep.VectorClock) >= 0 {
				continue
			}
		}

		depWrite, err := s.store.ReadDependency(s.ctx, dep.Key, dep.VectorClock)
		if err != nil || depWrite == nil {
			return false
		}
		T = append(T, *depWrite)
		if !s.isCovered(*depWrite, T) {
			return false
		}
	}
	return true
}

// resolveAsync updates the local store asynchronously
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
				eval, err := s.store.Read(s.ctx, key)
				if err != nil {
					continue
				}
				s.mu.Lock()
				T := []Write{*eval}
				if s.isCovered(*eval, T) {
					if s.sharedCache != nil {
						for _, w := range T {
							s.sharedCache.Put(s.ctx, w)
						}
					} else {
						for _, w := range T {
							s.localStore[w.Key] = w
						}
					}
					delete(s.toCheck, key)
				}
				s.mu.Unlock()
			}
		}
	}
}

// pruneLocalStore removes outdated writes
func (s *CausalShim) pruneLocalStore() {
	ticker := time.NewTicker(s.pruneInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			for key, write := range s.localStore {
				if write.AccessCount == 0 && time.Since(write.Timestamp) > time.Hour {
					stillNeeded := false
					for _, other := range s.localStore {
						for _, dep := range other.Deps {
							if dep.Key == key && s.compareVectorClocks(dep.VectorClock, write.VectorClock) == 0 {
								stillNeeded = true
								break
							}
						}
						if stillNeeded {
							break
						}
					}
					if !stillNeeded {
						delete(s.localStore, key)
					}
				}
			}
			s.mu.Unlock()
		}
	}
}

// copyVectorClock creates a copy of the current vector clock
func (s *CausalShim) copyVectorClock() VectorClock {
	vc := make(VectorClock)
	for _, v := range s.localStore {
		for proc, clock := range v.VectorClock {
			if current, exists := vc[proc]; !exists || clock > current {
				vc[proc] = clock
			}
		}
	}
	return vc
}

// compareVectorClocks compares two vector clocks
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
