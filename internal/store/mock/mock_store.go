package mock

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/google/uuid"
	"github.com/zetareticula/zugkraftdb/internal/shim"
)

// ErrNotFound is returned when a key or dependency is not found
var ErrNotFound = errors.New("not found")

// MockStore simulates an eventually consistent store
type MockStore struct {
	data     map[string]*shim.EntityWithMetadata
	writeIDs map[string]string // writeID -> key
	mu       sync.RWMutex
	dataset string
}

// NewMockStore creates a new mock store
func NewMockStore(dataset string) *MockStore {
	return &MockStore{
		data:     make(map[string]*shim.EntityWithMetadata),
		writeIDs: make(map[string]string),
		dataset:  dataset,
	}
}

// Write persists a write with metadata
func (s *MockStore) Write(ctx context.Context, key, value, writeID string, dependencies []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Parse the value as JSON into a map of TypedValue
	var attrs map[string]shim.TypedValue
	if err := json.Unmarshal([]byte(value), &attrs); err != nil {
		return err
	}

	// Convert dependencies to the expected format
	var deps []shim.Dependency
	for _, dep := range dependencies {
		deps = append(deps, shim.Dependency{
			Key:         dep,
			VectorClock: make(shim.VectorClock),
		})
	}

	s.data[key] = &shim.EntityWithMetadata{
		Entity: shim.Entity{
			Key:          key,
			WriteID:      writeID,
			Attributes:   attrs,
			Dependencies: deps,
		},
		Metadata: map[string]string{
			"write_id": writeID,
		},
	}
	s.writeIDs[writeID] = key
	return nil
}

// Read retrieves the latest value for a key
func (s *MockStore) Read(ctx context.Context, key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entity, exists := s.data[key]
	if !exists {
		return "", ErrNotFound
	}

	// Convert attributes to JSON
	data, err := json.Marshal(entity.Attributes)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// ReadDependency retrieves metadata for a specific write
func (s *MockStore) ReadDependency(ctx context.Context, writeID string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key, exists := s.writeIDs[writeID]
	if !exists {
		return "", ErrNotFound
	}

	entity, exists := s.data[key]
	if !exists {
		return "", ErrNotFound
	}

	// Convert entity to JSON
	data, err := json.Marshal(entity)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// FetchDependency asynchronously fetches a dependency
func (s *MockStore) FetchDependency(ctx context.Context, writeID string) error {
	// In the mock implementation, we don't actually fetch anything
	return nil
}

// GetDataset returns the dataset name for the store
func (s *MockStore) GetDataset() string {
	return s.dataset
}

// Close closes the store connection
func (s *MockStore) Close() error {
	return nil
}

// GetWriteID returns a new write ID
func (s *MockStore) GetWriteID(ctx context.Context) (string, error) {
	return uuid.New().String(), nil
}

// equalVectorClocks compares vector clocks
func equalVectorClocks(vc1, vc2 shim.VectorClock) bool {
	if len(vc1) != len(vc2) {
		return false
	}
	for k, v1 := range vc1 {
		if v2, ok := vc2[k]; !ok || v2 != v1 {
			return false
		}
	}
	return true
}