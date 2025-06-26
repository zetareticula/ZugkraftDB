package query

import (
	"fmt"
	"strings"
	"zugkraftdb/internal/shim"
)

// Package query provides functionality to parse and translate Datalog queries to CQL
// and execute them against a causal consistency shim.
// It includes structures for representing Datalog queries, clauses, and results,
// as well as functions to parse Datalog syntax and translate it into CQL statements.

// DatalogQuery represents a Datalog query
type DatalogQuery struct {
	Find   []string                   // Variables or attributes to project
	Where  []Clause                   // Conditions
	Inputs map[string]shim.TypedValue // Bound variables
}

// Clause represents a Datalog where clause
type Clause struct {
	Entity    string // Entity variable or ID
	Attribute string // Attribute name
	Value     string // Value variable or constant
	CausalDep string // Optional causal dependency key
}

// QueryResult represents query output
type QueryResult struct {
	Rows []map[string]shim.TypedValue
}

// ParseDatalog parses a Datalog query string
func ParseDatalog(query string) (*DatalogQuery, error) {
	if !strings.HasPrefix(query, "[:find") {
		return nil, fmt.Errorf("invalid query: must start with :find")
	}

	dq := &DatalogQuery{
		Inputs: make(map[string]shim.TypedValue),
	}

	parts := strings.Split(query, ":find")
	findPart := strings.Trim(parts[1], " :where")
	findVars := strings.Fields(findPart)
	dq.Find = findVars

	wherePart := strings.Split(query, ":where")[1]
	wherePart = strings.Trim(wherePart, " []")
	clauses := strings.Split(wherePart, "][")
	for _, clause := range clauses {
		clause = strings.Trim(clause, " []")
		parts := strings.Fields(clause)
		if len(parts) < 3 || len(parts) > 4 {
			return nil, fmt.Errorf("invalid clause: %s", clause)
		}
		c := Clause{
			Entity:    parts[0],
			Attribute: parts[1],
			Value:     parts[2],
		}
		if len(parts) == 4 {
			c.CausalDep = parts[3]
		}
		dq.Where = append(dq.Where, c)
	}

	return dq, nil
}

// TranslateToCQL translates Datalog to CQL
func TranslateToCQL(dq *DatalogQuery) (string, []interface{}, error) {
	var conditions []string
	var args []interface{}
	var bindings = make(map[string]string)

	for _, clause := range dq.Where {
		if strings.HasPrefix(clause.Value, "?") {
			if existingKey, ok := bindings[clause.Entity]; ok {
				conditions = append(conditions, fmt.Sprintf("key = ? AND attribute = ?"))
				args = append(args, existingKey, clause.Attribute)
				if clause.CausalDep != "" {
					conditions = append(conditions, "EXISTS (SELECT 1 FROM shim.entities WHERE key = ? AND vector_clock >= ?)")
					args = append(args, clause.CausalDep, "{}") // Simplified; actual VC comparison needed
				}
			} else {
				return "", nil, fmt.Errorf("unbound variable: %s", clause.Entity)
			}
		} else {
			conditions = append(conditions, fmt.Sprintf("key = ? AND attribute = ?"))
			args = append(args, clause.Entity, clause.Attribute)
			bindings[clause.Value] = clause.Entity
			if clause.CausalDep != "" {
				conditions = append(conditions, "EXISTS (SELECT 1 FROM shim.entities WHERE key = ? AND vector_clock >= ?)")
				args = append(args, clause.CausalDep, "{}")
			}
		}
	}

	cql := fmt.Sprintf("SELECT key, value FROM shim.entities WHERE %s", strings.Join(conditions, " AND "))
	return cql, args, nil
}

// ExecuteDatalog executes a Datalog query against the causal shim
func ExecuteDatalog(shimInstance shim.GetShim, query string) (*QueryResult, error) {
	dq, err := ParseDatalog(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Datalog query: %w", err)
	}

	cql, args, err := TranslateToCQL(dq)
	if err != nil {
		return nil, fmt.Errorf("failed to translate Datalog to CQL: %w", err)
	}

	var results []map[string]shim.TypedValue
	for _, clause := range dq.Where {
		entity, err := shimInstance.GetShim(context.Background(), clause.Entity)
		if err != nil {
			return nil, fmt.Errorf("failed to get entity %s: %w", clause.Entity, err)
		}
		result := make(map[string]shim.TypedValue)
		for attr, value := range entity.Attributes {
			result[attr] = value
		}
		results = append(results, result)
	}

	return &QueryResult{Rows: results}, nil
}

// QueryDatalog executes a Datalog query and returns the results
func QueryDatalog(shimInstance shim.GetShim, query string) (*QueryResult, error) {
	result, err := ExecuteDatalog(shimInstance, query)
	if err != nil {
		return nil, fmt.Errorf("Datalog query execution failed: %w", err)
	}
	return result, nil
}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}

	if result.Rows[0]["friend"].Value != "person2" {
		t.Errorf("Expected friend to be person2, got %s", result.Rows[0]["friend"].Value)
	}

		
		t.Errorf("Expected friend to be person2, got %s", result.Rows[0]["friend"].Value)
	}
		return
	}
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
		if result.Rows[0]["friend"].Value != "person2" {
		t.Errorf("Expected friend to be person2, got %s", result.Rows[0]["friend"].Value)
	}

}
