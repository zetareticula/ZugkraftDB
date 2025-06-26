package integration

import (
	"context"
	"testing"
	"time"

	"zugkraftdb/internal/shim"
	"zugkraftdb/internal/shim/mock"
)

func TestRelativisticLinearizability(t *testing.T) {
	ctx := context.Background()
	store := mock.NewMockStore()
	cache := mock.NewMockCache()
	shimInstance, err := shim.NewCausalShim(store, cache, "", "device1", 1*time.Second, 1*time.Second, "test1", false, true)
	if err != nil {
		t.Fatalf("Failed to create shim: %v", err)
	}

	// Write with causal dependency
	deps := []shim.Dependency{{Key: "person2", VectorClock: shim.VectorClock{"test1": 1}}}
	attrs1 := map[string]shim.TypedValue{
		":person/email": {Type: "string", Value: "alice@example.com"},
	}
	if err := shimInstance.PutShim(ctx, "person1", attrs1, deps); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Verify causal order
	attrs, err := shimInstance.GetShim(ctx, "person1")
	if err != nil || attrs[":person/email"].Value != "alice@example.com" {
		t.Errorf("Expected email, got %v, err: %v", attrs, err)
	}
}

func TestCausalDatalogQuery(t *testing.T) {
	ctx := context.Background()
	store := mock.NewMockStore()
	cache := mock.NewMockCache()
	shimInstance, err := shim.NewCausalShim(store, cache, "", "device2", 1*time.Second, 1*time.Second, "test2", false, true)
	if err != nil {
		t.Fatalf("Failed to create shim: %v", err)
	}

	attrs1 := map[string]shim.TypedValue{
		":person/email": {Type: "string", Value: "bob@example.com"},
	}
	if err := shimInstance.PutShim(ctx, "person2", attrs1, nil); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	deps := []shim.Dependency{{Key: "person2", VectorClock: shim.VectorClock{"test2": 1}}}
	attrs2 := map[string]shim.TypedValue{
		":person/friend": {Type: "ref", Value: "person2"},
	}
	if err := shimInstance.PutShim(ctx, "person3", attrs2, deps); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	queryStr := "[:find ?friend :where [?person :person/friend ?friend person2]]"
	result, err := shimInstance.QueryDatalog(ctx, queryStr)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["friend"].Value != "person2" {
		t.Errorf("Expected friend, got %v", result.Rows)
	}
}

func TestLocality(t *testing.T) {
	ctx := context.Background()
	store := mock.NewMockStore()
	cache := mock.NewMockCache()
	shimInstance, err := shim.NewCausalShim(store, cache, "", "device3", 1*time.Second, 1*time.Second, "test3", false, true)
	if err != nil {
		t.Fatalf("Failed to create shim: %v", err)
	}

	// Write to multiple objects
	attrs1 := map[string]shim.TypedValue{
		":person/email": {Type: "string", Value: "charlie@example.com"},
	}
	if err := shimInstance.PutShim(ctx, "person4", attrs1, nil); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	attrs2 := map[string]shim.TypedValue{
		":person/email": {Type: "string", Value: "dave@example.com"},
	}
	if err := shimInstance.PutShim(ctx, "person5", attrs2, nil); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Verify each object is consistent
	attrs, err := shimInstance.GetShim(ctx, "person4")
	if err != nil || attrs[":person/email"].Value != "charlie@example.com" {
		t.Errorf("Expected charlie, got %v, err: %v", attrs, err)
	}

	attrs, err = shimInstance.GetShim(ctx, "person5")
	if err != nil || attrs[":person/email"].Value != "dave@example.com" {
		t.Errorf("Expected dave, got %v, err: %v", attrs, err)
	}
}
