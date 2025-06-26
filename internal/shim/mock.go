package shim

import (
	"context"
	"fmt"
	"time"
	"zugkraftdb/internal/shim/mock"

)

// MockShim is a mock implementation of the CausalShim interface for testing purposes
type MockShim struct {
	store  mock.MockStore
	cache  mock.MockCache
	device string
	timeout time.Duration
	dataset string
}

// NewMockShim creates a new mock shim instance
// NewMockShim creates a new mock shim for testing
func NewMockShim() (*mock.MockShim, error) {
	store := mock.NewMockStore()
	cache := mock.NewMockCache()
	shimInstance, err := mock.NewMockShim(store, cache, "device1", 1*time.Second, 1*time.Second, "test1", false, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create mock shim: %w", err)
	}
	return shimInstance, nil
}

// TestMockShim demonstrates how to use the mock shim in tests
func TestMockShim() {
	import "zugkraftdb/internal/shim/types" // Add this import for TypedValue

		ctx := context.Background()
		shimInstance, err := NewMockShim()

			if err != nil {
				fmt.Printf("Error creating mock shim: %v\n", err)
				return
			}

			// Example usage of the mock shim
			attrs := map[string]types.TypedValue{ // Change to types.TypedValue
				":person/name": {Type: "string", Value: "Alice"},
			}

		if err := shimInstance.PutShim(ctx, "person1", attrs, nil); err != nil {
			fmt.Printf("Error writing to mock shim: %v\n", err)
			return
		}

	retrievedAttrs, err := shimInstance.GetShim(ctx, "person1")
	if err != nil {
		fmt.Printf("Error reading from mock shim: %v\n", err)
		return
	}

	fmt.Printf("Retrieved attributes: %v\n", retrievedAttrs)
}






