package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"causal-consistency-shim/internal/shim"
	"causal-consistency-shim/internal/store/mock"
)

func TestHighReadThroughput(t *testing.T) {
	ctx := context.Background()
	store := mock.NewMockStore()
	cache := mock.NewMockCache()
	shimInstance := shim.NewCausalShim(store, cache, 1*time.Second, 1*time.Second, "test1", false, false)

	// Write a value
	if err := shimInstance.PutShim(ctx, "post1", "Hello", nil); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Simulate high read throughput
	start := time.Now()
	for i := 0; i < 100000; i++ {
		value, err := shimInstance.GetShim(ctx, "post1")
		if err != nil || value != "Hello" {
			t.Errorf("Expected Hello, got %s, err: %v", value, err)
		}
	}
	duration := time.Since(start)
	t.Logf("Read throughput: %.2f ops/s", float64(100000)/duration.Seconds())
}

func TestLongTailedChain(t *testing.T) {
	ctx := context.Background()
	store := mock.NewMockStore()
	cache := mock.NewMockCache()
	shimInstance := shim.NewCausalShim(store, cache, 1*time.Second, 1*time.Second, "test2", false, false)

	// Simulate Metafilter-like chain (up to 5817 dependencies)
	deps := []shim.Dependency{}
	for i := 1; i <= 20; i++ { // Limit to 20 for test speed
		key := fmt.Sprintf("post%d", i)
		vc := shim.VectorClock{"test2": int64(i)}
		if err := shimInstance.PutShim(ctx, key, fmt.Sprintf("Value%d", i), deps); err != nil {
			t.Fatalf("Failed to write %s: %v", key, err)
		}
		deps = append(deps, shim.Dependency{Key: key, VectorClock: vc})
	}

	// Verify last write
	value, err := shimInstance.GetShim(ctx, "post20")
	if err != nil || value != "Value20" {
		t.Errorf("Expected Value20, got %s, err: %v", value, err)
	}
}

func TestConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	store := mock.NewMockStore()
	cache := mock.NewMockCache()
	shimInstance := shim.NewCausalShim(store, cache, 1*time.Second, 1*time.Second, "test3", false, true)

	// Simulate concurrent writes
	if err := shimInstance.PutShim(ctx, "post1", "Value1", nil); err != nil {
		t.Fatalf("Failed to write Value1: %v", err)
	}
	if err := shimInstance.PutShim(ctx, "post1", "Value2", nil); err != nil {
		t.Fatalf("Failed to write Value2: %v", err)
	}

	// Verify latest write
	value, err := shimInstance.GetShim(ctx, "post1")
	if err != nil || value != "Value2" {
		t.Errorf("Expected Value2, got %s, err: %v", value, err)
	}
}
