package mock

import (
	"context"
	"sync"
	"github.com/zetareticula/zugkraftdb/internal/shim"
)

// MockCache simulates a shared cache (e.g., Redis)
type MockCache struct {
	data map[string]*shim.EntityWithMetadata
	mu   sync.RWMutex
}

// NewMockCache creates a new mock cache
func NewMockCache() *MockCache {
	return &MockCache{
		data: make(map[string]*shim.EntityWithMetadata),
	}
}

// Get retrieves a write from the cache
func (c *MockCache) Get(ctx context.Context, key string) (*shim.EntityWithMetadata, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if write, ok := c.data[key]; ok {
		return write, nil
	}
	return nil, shim.ErrNotFound
}

// Put stores a write in the cache
func (c *MockCache) Put(ctx context.Context, write *shim.EntityWithMetadata) error {
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



