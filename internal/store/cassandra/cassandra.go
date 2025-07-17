package cassandra

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"zugkraftdb/internal/shim"

	"github.com/gocql/gocql"
)

// CassandraStore implements the Store interface
// using Cassandra as the backend database.
// It supports relativistic linearizability by managing multiple clusters
// and selecting appropriate sessions based on read/write capabilities and dataset priorities.
// It also supports eventual consistency writes that bypass the shim.
type CassandraStore struct {
	clusters map[string]*gocql.ClusterConfig
	sessions map[string]*gocql.Session
	config   *shim.Config
	mu       sync.RWMutex
}

// NewCassandraStore creates a new store
func NewCassandraStore(config *shim.Config) (*CassandraStore, error) {
	store := &CassandraStore{
		clusters: make(map[string]*gocql.ClusterConfig),
		sessions: make(map[string]*gocql.Session),
		config:   config,
	}

	for _, db := range config.Databases {
		cluster := gocql.NewCluster(db.Host)
		cluster.Consistency = gocql.Quorum
		cluster.NumConns = 2
		// Simulate relativistic latency
		if db.LatencyMs > 0 {
			cluster.ConnectTimeout = time.Duration(db.LatencyMs) * time.Millisecond
		}
		store.clusters[db.Name] = cluster
		session, err := cluster.CreateSession()
		if err != nil {
			return nil, err
		}
		store.sessions[db.Name] = session
	}
	return store, nil
}

// Write persists an entity
func (s *CassandraStore) Write(ctx context.Context, entity shim.Entity, dataset string) error {
	session, err := s.getWriteSession(dataset)
	if err != nil {
		return err
	}

	data, err := json.Marshal(entity)
	if err != nil {
		return err
	}

	for i := 0; i < s.config.Connection.MinTries; i++ {
		err = session.Query("INSERT INTO shim.entities (key, write_id, data) VALUES (?, ?, ?)",
			entity.Key, entity.WriteID, data).WithContext(ctx).Exec()
		if err == nil {
			return nil
		}
		time.Sleep(time.Duration(s.config.Connection.RetryDelay) * time.Millisecond)
	}
	return err
}

// Read retrieves an entity
func (s *CassandraStore) Read(ctx context.Context, key, dataset string) (*shim.Entity, error) {
	session, err := s.getReadSession(dataset)
	if err != nil {
		return nil, err
	}

	var writeID string
	var data []byte
	for i := 0; i < s.config.Connection.MinTries; i++ {
		err = session.Query("SELECT write_id, data FROM shim.entities WHERE key = ? LIMIT 1",
			key).WithContext(ctx).Scan(&writeID, &data)
		if err == nil {
			var entity shim.Entity
			if err := json.Unmarshal(data, &entity); err != nil {
				return nil, err
			}
			return &entity, nil
		}
		if err == gocql.ErrNotFound {
			return nil, shim.ErrNotFound
		}
		time.Sleep(time.Duration(s.config.Connection.RetryDelay) * time.Millisecond)
	}
	return nil, err
}

// ReadDependency retrieves a dependency
func (s *CassandraStore) ReadDependency(ctx context.Context, key string, vc shim.VectorClock, dataset string) (*shim.Entity, error) {
	session, err := s.getReadSession(dataset)
	if err != nil {
		return nil, err
	}

	iter := session.Query("SELECT data FROM shim.entities WHERE key = ?",
		key).WithContext(ctx).Iter()
	defer iter.Close()

	for {
		var data []byte
		if !iter.Scan(&data) {
			break
		}
		var entity shim.Entity
		if err := json.Unmarshal(data, &entity); err != nil {
			continue
		}
		if s.compareVectorClocks(entity.VectorClock, vc) == 0 {
			return &entity, nil
		}
	}
	return nil, shim.ErrNotFound
}

// Query executes a Datalog-translated CQL query
func (s *CassandraStore) Query(ctx context.Context, cql string, args []interface{}, dataset string) ([]map[string]shim.TypedValue, error) {
	session, err := s.getReadSession(dataset)
	if err != nil {
		return nil, err
	}

	iter := session.Query(cql, args...).WithContext(ctx).Iter()
	defer iter.Close()

	var results []map[string]shim.TypedValue
	for {
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			break
		}
		result := make(map[string]shim.TypedValue)
		for k, v := range row {
			result[k] = shim.TypedValue{Type: "string", Value: v}
		}
		results = append(results, result)
	}
	return results, nil
}

// getWriteSession selects a write session
func (s *CassandraStore) getWriteSession(dataset string) (*gocql.Session, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var candidates []string
	for _, db := range s.config.Databases {
		if db.Write && (dataset == "" || db.Name == dataset) {
			candidates = append(candidates, db.Name)
		}
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no write-capable servers for dataset %s", dataset)
	}
	return s.sessions[candidates[rand.Intn(len(candidates))]], nil
}

// getReadSession selects a read session
func (s *CassandraStore) getReadSession(dataset string) (*gocql.Session, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var priorityGroups [][]string
	maxPriority := 0
	for _, db := range s.config.Databases {
		if db.Read && (dataset == "" || db.Name == dataset) {
			if db.Priority > maxPriority {
				maxPriority = db.Priority
			}
		}
	}
	priorityGroups = make([][]string, maxPriority+1)
	for _, db := range s.config.Databases {
		if db.Read && (dataset == "" || db.Name == dataset) {
			priorityGroups[db.Priority] = append(priorityGroups[db.Priority], db.Name)
		}
	}

	for _, group := range priorityGroups {
		if len(group) > 0 {
			return s.sessions[group[rand.Intn(len(group))]], nil
		}
	}
	return nil, fmt.Errorf("no read-capable servers for dataset %s", dataset)
}

// compareVectorClocks compares vector clocks
func (s *CassandraStore) compareVectorClocks(vc1, vc2 shim.VectorClock) int {
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
		return 0
	} else if vc1Greater {
		return 1
	} else if vc2Greater {
		return -1
	}
	return 0
}
