package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"causal-consistency-shim/internal/shim"
	"causal-consistency-shim/internal/store/mock"
)

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
