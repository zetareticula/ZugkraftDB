package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"zugkraftdb/internal/shim"
	"zugkraftdb/internal/store/mock"
)

// Example usage of the CausalShim with mock store and cache

// This example demonstrates how to use the CausalShim to write and read data with explicit causality,
// and how to bypass the shim for eventual consistency writes.
// It uses a mock store and cache for testing purposes.

// Ensure you have the necessary imports
func main() {
	ctx := context.Background()
	store := mock.NewMockStore()
	cache := mock.NewMockCache()
	shimInstance := shim.NewCausalShim(store, cache, 5*time.Second, 1*time.Hour, "client1", false)

	// Write with explicit causality
	deps := []shim.Dependency{
		{Key: "post1", VectorClock: shim.VectorClock{"client2": 1}},
	}
	if err := shimInstance.PutShim(ctx, "post2", "Hello, World!", deps); err != nil {
		log.Fatalf("Failed to write: %v", err)
	}

	// Read with causal consistency
	value, err := shimInstance.GetShim(ctx, "post2")
	if err != nil {
		log.Fatalf("Failed to read: %v", err)
	}
	fmt.Printf("Read value: %s\n", value)

	// Eventual consistency bypass
	if err := store.Write(ctx, shim.Write{Key: "post3", Value: "Bypass shim"}); err != nil {
		log.Fatalf("Failed to bypass write: %v", err)
	}
	value, err = store.Read(ctx, "post3")
	if err != nil {
		log.Fatalf("Failed to bypass read: %v", err)
	}
	fmt.Printf("Bypass read value: %s\n", value)

	shimInstance.Close()
}

//	t.Fatalf("Failed to write: %v", err)
// 	}
//
//

// 	// Verify causal order
// 	attrs, err := shimInstance.GetShim(ctx, "person3")
// 	if err != nil || attrs[":person/friend"].Value != "person2" {
