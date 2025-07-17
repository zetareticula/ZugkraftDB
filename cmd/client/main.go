package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/zetareticula/zugkraftdb/internal/shim"
	"github.com/zetareticula/zugkraftdb/internal/store/mock"
)

// Example usage of the CausalShim with mock store and cache

// This example demonstrates how to use the CausalShim to write and read data with explicit causality,
// and how to bypass the shim for eventual consistency writes.
// It uses a mock store and cache for testing purposes.

// Ensure you have the necessary imports
func main() {
	ctx := context.Background()
	store := mock.NewMockStore("test-dataset")

	// Create a temporary config file
	configContent := `databases:
  - name: local
    host: localhost:6379
    priority: 1
    write: true
    read: true
    datacenter: local
    latency_ms: 1
`
	configPath := "config.yaml"
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		log.Fatalf("Failed to create config file: %v", err)
	}
	defer os.Remove(configPath)

	// Create a simple write operation
	writeID, err := store.GetWriteID(ctx)
	if err != nil {
		log.Fatalf("Failed to get write ID: %v", err)
	}

	// Prepare attributes
	attrs := map[string]shim.TypedValue{
		"content": {
			Type:  "string",
			Value: "Hello, World!",
		},
	}

	// Convert attributes to JSON
	attrsJSON, err := json.Marshal(attrs)
	if err != nil {
		log.Fatalf("Failed to marshal attributes: %v", err)
	}

	// Write data
	err = store.Write(ctx, "post1", string(attrsJSON), writeID, nil)
	if err != nil {
		log.Fatalf("Failed to write: %v", err)
	}

	// Read data
	value, err := store.Read(ctx, "post1")
	if err != nil {
		log.Fatalf("Failed to read: %v", err)
	}

	// Unmarshal the attributes
	var result map[string]shim.TypedValue
	if err := json.Unmarshal([]byte(value), &result); err != nil {
		log.Fatalf("Failed to unmarshal result: %v", err)
	}

	fmt.Printf("Read value: %+v\n", result)

	// Read using the write ID
	depValue, err := store.ReadDependency(ctx, writeID)
	if err != nil {
		log.Fatalf("Failed to read dependency: %v", err)
	}
	fmt.Printf("Read by write ID: %s\n", depValue)

	shimInstance, err := shim.NewCausalShim(store, configPath, "client1", 5*time.Second, 1*time.Hour, "client1", false, false)
	if err != nil {
		log.Fatalf("Failed to create CausalShim: %v", err)
	}
	defer shimInstance.Close()
}

//	t.Fatalf("Failed to write: %v", err)
// 	}
//
//

// 	// Verify causal order
// 	attrs, err := shimInstance.GetShim(ctx, "person3")
// 	if err != nil || attrs[":person/friend"].Value != "person2" {
