package mock

import (
	"context"
	"sync"

	"causal-consistency-shim/internal/shim"
)

// MockCache simulates a shared cache (e.g., Redis)
type MockCache struct {
	data map[string]shim.Write
	mu   sync.RWMutex
}

// NewMockCache creates a new mock cache
func NewMockCache() *MockCache {
	return &MockCache{
		data: make(map[string]shim.Write),
	}
}

// Get retrieves a write from the cache
func (c *MockCache) Get(ctx context.Context, key string) (*shim.Write, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	write, exists := c.data[key]
	if !exists {
		return nil, shim.ErrNotFound
	}
	return &write, nil
}

// Put stores a write in the cache
func (c *MockCache) Put(ctx context.Context, write shim.Write) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data[write.Key] = write
	return nil
}

// Delete removes a write from the cache
func (c *MockCache) Delete(ctx context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.data, key)
	return nil
}
