package shim

import "context"

// Store defines the interface for an eventually consistent data store
type Store interface {
	// Write persists a write with metadata
	Write(ctx context.Context, key, value, writeID string, dependencies []string) error
	// Read retrieves the latest value for a key
	Read(ctx context.Context, key string) (string, error)
	// ReadDependency retrieves metadata for a specific write
	ReadDependency(ctx context.Context, writeID string) (string, error)
	// FetchDependency asynchronously fetches a dependency
	FetchDependency(ctx context.Context, writeID string) error
}
