package cassandra

import (
	"context"
	"encoding/json"

	"causal-consistency-shim/internal/shim"

	"github.com/gocql/gocql"
)

// CassandraStore implements the Store interface using Cassandra
type CassandraStore struct {
	session *gocql.Session
}

// NewCassandraStore creates a new Cassandra store
func NewCassandraStore(cluster *gocql.ClusterConfig) (*CassandraStore, error) {
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &CassandraStore{session: session}, nil
}

// Write persists a write to Cassandra
func (s *CassandraStore) Write(ctx context.Context, write shim.Write) error {
	data, err := json.Marshal(write)
	if err != nil {
		return err
	}
	return s.session.Query("INSERT INTO shim.writes (key, write_id, data) VALUES (?, ?, ?)",
		write.Key, write.WriteID, data).WithContext(ctx).Exec()
}

// Read retrieves the latest write for a key
func (s *CassandraStore) Read(ctx context.Context, key string) (*shim.Write, error) {
	var writeID string
	var data []byte
	err := s.session.Query("SELECT write_id, data FROM shim.writes WHERE key = ? LIMIT 1",
		key).WithContext(ctx).Scan(&writeID, &data)
	if err == gocql.ErrNotFound {
		return nil, shim.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var write shim.Write
	if err := json.Unmarshal(data, &write); err != nil {
		return nil, err
	}
	return &write, nil
}

// ReadDependency retrieves a specific dependency
func (s *CassandraStore) ReadDependency(ctx context.Context, key string, vc shim.VectorClock) (*shim.Write, error) {
	iter := s.session.Query("SELECT data FROM shim.writes WHERE key = ?",
		key).WithContext(ctx).Iter()
	defer iter.Close()

	for {
		var data []byte
		if !iter.Scan(&data) {
			break
		}
		var write shim.Write
		if err := json.Unmarshal(data, &write); err != nil {
			continue
		}
		if s.compareVectorClocks(write.VectorClock, vc) == 0 {
			return &write, nil
		}
	}
	return nil, shim.ErrNotFound
}

// FetchDependency simulates async dependency fetching
func (s *CassandraStore) FetchDependency(ctx context.Context, writeID string) error {
	// Cassandra does not support callbacks; simulate async fetch
	return nil
}

// compareVectorClocks compares two vector clocks
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
		return 0 // Concurrent
	} else if vc1Greater {
		return 1
	} else if vc2Greater {
		return -1
	}
	return 0 // Equal
}
