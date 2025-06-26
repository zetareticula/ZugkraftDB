package shim

import "context"

// Config defines the configuration for the shim
type Config struct {
	// DeviceID is the unique identifier for the device
	DeviceID string `json:"device_id"`
	// Dataset is the name of the dataset to use
	Dataset string `json:"dataset"`
	// WriteTimeout is the timeout for write operations
	WriteTimeout int64 `json:"write_timeout"`
	// ReadTimeout is the timeout for read operations
	ReadTimeout int64 `json:"read_timeout"`
	// EnableCache indicates whether to enable caching
	EnableCache bool `json:"enable_cache"`
	// EnableCausalConsistency indicates whether to enable causal consistency
	EnableCausalConsistency bool `json:"enable_causal_consistency"`
}

// CausalShim defines the interface for a causal consistency shim
type CausalShim interface {
	// GetShim retrieves an entity by key
	GetShim(ctx context.Context, key string) (EntityWithMetadata, error)
	// PutShim writes an entity with attributes and dependencies
	PutShim(ctx context.Context, key string, attrs map[string]TypedValue, deps []Dependency) error
	// GetWriteID returns the write ID for the current operation
	GetWriteID(ctx context.Context) (string, error)
	// GetVectorClock returns the vector clock for the current operation
	GetVectorClock(ctx context.Context) (VectorClock, error)
}

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

type VectorClock map[string]int // VectorClock represents a vector clock for causal consistency
// Dependency represents a causal dependency with a key and vector clock
type Dependency struct {
	Key         string      // Key of the dependent entity
	VectorClock VectorClock // Vector clock for the dependency
}
type TypedValue struct {
	Type  string      // Type of the value (e.g., "string", "int", "ref")
	Value interface{} // Actual value, can be any type
}

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
