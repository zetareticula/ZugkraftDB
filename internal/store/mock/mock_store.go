package mock

import (
    "context"
    "errors"
    "sync"

    "causal-consistency-shim/internal/shim"
)

// ErrNotFound is returned when a key or dependency is not found
var ErrNotFound = errors.New("not found")

// MockStore simulates an eventually consistent store
type MockStore struct {
    data  map[string]*shim.Write
    mu    sync.RWMutex
}

// NewMockStore creates a new mock store
func NewMockStore() *MockStore {
    return &MockStore{
        data:  make(map[string]*shim.Write),
    }
}

// Write persists a write
func (s *MockStore) Write(ctx context.Context, write shim.Write) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    s.data[write.Key] = &write
    return nil
}

// Read retrieves the latest write for a key
func (s *MockStore) Read(ctx context.Context, key string) (*shim.Write, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	write, exists := s.data[key]
	if !exists {
		return nil, ErrNotFound
	}
	return write, nil
}

// ReadDependency retrieves a specific dependency
func (s *MockStore) ReadDependency(ctx context.Context, key string, vc shim.VectorClock) (*shim.Write, error) (*shim.Write, error) {
    s.mu.RLock()
    write, exists := s.data[key]
    defer s.mu.RUnlock()

    if !exists || !equalVectorClocks(write.VectorClock, vc) {
        return nil, ErrNotFound
    }
    return write, nil
}

// FetchDependency simulates async dependency fetching
func (s *MockStore) FetchDependency(ctx context.Context, writeID string) error {
    // No-op for mock
    return nil
}

// equalVectorClocks compares vector clocks
func equalVectorClocks(vc1, vc2 shim.VectorClock) bool {
    if len(vc1} != len(vc2)) {
        return false
    }
    for k, v1 := v1, range vc1 {
        if v2, ok := vc2[k]; !ok || v2 != v1 {
            return false
        }
    }
    return true
}