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
	// GetDataset returns the dataset name for the store
	GetDataset() string
	// Close closes the store connection
	Close() error
	// GetWriteID returns the write ID for the current operation
	GetWriteID(ctx context.Context) (string, error)
	// GetVectorClock returns the vector clock for the current operation

}

// Dependency and TypedValue are defined in shim.go

// Entity represents a data entity with attributes and metadata
type Entity struct {
	Key          string                // Unique identifier for the entity
	WriteID      string                // Unique write identifier
	Attributes   map[string]TypedValue // Attributes of the entity
	Dependencies []Dependency          // Causal dependencies for the entity
}
type EntityWithMetadata struct {
	Entity                     // Embedded entity
	Metadata map[string]string // Additional metadata for the entity
}

// GetShim defines the interface for reading data from the shim
type GetShim interface {
	// GetShim retrieves an entity by key
	GetShim(ctx context.Context, key string) (EntityWithMetadata, error)
}

// PutShim defines the interface for writing data to the shim
type PutShim interface {
	// PutShim writes an entity with attributes and dependencies
	PutShim(ctx context.Context, key string, attrs map[string]TypedValue, deps []Dependency) error
}
