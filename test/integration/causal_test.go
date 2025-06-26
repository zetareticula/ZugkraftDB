package integration

import (
	"context"
	"testing"
	"time"

	"causal-consistency-shim/internal/shim"
	"causal-consistency-shim/internal/store/mock"
)

func TestCausalConsistency(t *testing.T) {
	ctx := context.Background()
	store := mock.NewMockStore()
	cache := mock.NewMockCache()
	shimInstance := shim.NewCausalShim(store, cache, 1*time.Second, 1*time.Second, "test1", false)

	// Write post1
	if err := shimInstance.PutShim(ctx, "post1", "Hello", nil); err != nil {
		t.Fatalf("Failed to write post1: %v", err)
	}

	// Write post2 depending on post1
	deps := []shim.Dependency{{Key: "post1", VectorClock: shim.VectorClock{"test1": 1}}}
	if err := shimInstance.PutShim(ctx, "post2", "World", deps); err != nil {
		t.Fatalf("Failed to write post2: %v", err)
	}

	// Read post2 should return "World"
	value, err := shimInstance.GetShim(ctx, "post2")
	if err != nil || value != "World" {
		t.Errorf("Expected World, got %s, err: %v", value, err)
	}

	// Read post1 should return "Hello"
	value, err = shimInstance.GetShim(ctx, "post1")
	if err != nil || value != "Hello" {
		t.Errorf("Expected Hello, got %s, err: %v", value, err)
	}
}

func TestPruning(t *testing.T) {
	ctx := context.Background()
	store := mock.NewMockStore()
	cache := mock.NewMockCache()
	shimInstance := shim.NewCausalShim(store, cache, 1*time.Second, 100*time.Millisecond, "test2", false)

	// Write post1
	if err := shimInstance.PutShim(ctx, "post1", "Old", nil); err != nil {
		t.Fatalf("Failed to write post1: %v", err)
	}

	// Wait for pruning
	time.Sleep(200 * time.Millisecond)

	// Read post1 should return not found
	_, err := shimInstance.GetShim(ctx, "post1")
	if err != shim.ErrNotFound {
		t.Errorf("Expected not found, got err: %v", err)
	}
}
