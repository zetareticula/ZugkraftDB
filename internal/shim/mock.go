package shim

import (
	"context"
	"fmt"
	"time"
)

// MockShim is a mock implementation of the CausalShim interface for testing purposes
type MockShim struct {
	store  Store
	cache  interface{} // Using interface{} to avoid circular dependency
	device string
	timeout time.Duration
	dataset string
}

// NewMockShim creates a new mock shim instance for testing
func NewMockShim() (*MockShim, error) {
	// Create a basic store implementation for testing
	store := &mockStore{
		data: make(map[string]string),
	}
	
	return &MockShim{
		store:   store,
		cache:   nil, // Not using cache for this simple implementation
		device:  "test-device",
		timeout: 1 * time.Second,
		dataset: "test-dataset",
	}, nil
}

// mockStore is a simple in-memory store for testing
type mockStore struct {
	data map[string]string
}

func (m *mockStore) Write(ctx context.Context, key, value, writeID string, deps []string) error {
	m.data[key] = value
	return nil
}

func (m *mockStore) Read(ctx context.Context, key string) (string, error) {
	if val, ok := m.data[key]; ok {
		return val, nil
	}
	return "", fmt.Errorf("key not found")
}

func (m *mockStore) ReadDependency(ctx context.Context, writeID string) (string, error) {
	// Simple implementation for testing
	return "", nil
}

func (m *mockStore) FetchDependency(ctx context.Context, writeID string) error {
	return nil
}

func (m *mockStore) GetDataset() string {
	return "test-dataset"
}

func (m *mockStore) Close() error {
	return nil
}

func (m *mockStore) GetWriteID(ctx context.Context) (string, error) {
	return "test-write-id", nil
}

// TestMockShim demonstrates how to use the mock shim in tests
func TestMockShim() {
	ctx := context.Background()
	shimInstance, err := NewMockShim()

	if err != nil {
		fmt.Printf("Error creating mock shim: %v\n", err)
		return
	}

	// Example usage of the mock shim
	err = shimInstance.store.Write(ctx, "test-key", "test-value", "test-write-id", nil)
	if err != nil {
		fmt.Printf("Error writing to store: %v\n", err)
		return
	}

	value, err := shimInstance.store.Read(ctx, "test-key")
	if err != nil {
		fmt.Printf("Error reading from store: %v\n", err)
		return
	}

	fmt.Printf("Successfully read value: %s\n", value)
}
