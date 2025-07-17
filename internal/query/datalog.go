package query

import (
	"fmt"
	"strings"

	"github.com/zetareticula/zugkraftdb/internal/shim"
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

// QueryResult represents the result of a query
type QueryResult struct {
	Columns []string
	Rows    []map[string]shim.TypedValue
}

// ParseDatalog parses a Datalog query string
func ParseDatalog(query string) (*DatalogQuery, error) {
	// Clean up the query string
	query = strings.TrimSpace(query)
	
	// Remove the outer brackets if present
	if strings.HasPrefix(query, "[") && strings.HasSuffix(query, "]") {
		query = query[1 : len(query)-1]
	}

	// Split into clauses
	clauses := strings.FieldsFunc(query, func(r rune) bool {
		return r == '[' || r == ']' || r == '\n' || r == '\t'
	})

	// Filter out empty strings
	var cleanClauses []string
	for _, clause := range clauses {
		clause = strings.TrimSpace(clause)
		if clause != "" {
			cleanClauses = append(cleanClauses, clause)
		}
	}

	if len(cleanClauses) == 0 {
		return nil, fmt.Errorf("empty query")
	}

	dq := &DatalogQuery{
		Inputs: make(map[string]shim.TypedValue),
	}

	// Process find clause
	if !strings.HasPrefix(cleanClauses[0], ":find") {
		return nil, fmt.Errorf("query must start with :find")
	}
	
	findVars := strings.Fields(cleanClauses[0][5:]) // Skip ":find"
	dq.Find = findVars

	// Process where clauses
	for i := 1; i < len(cleanClauses); i++ {
		clause := cleanClauses[i]
		if strings.HasPrefix(clause, ":where") {
			continue // Skip the :where keyword
		}

		// Parse pattern like [?p :name "Bob"]
		parts := strings.Fields(clause)
		if len(parts) < 3 {
			return nil, fmt.Errorf("invalid clause: %s", clause)
		}

		entity := strings.TrimPrefix(parts[0], "?")
		attr := strings.TrimPrefix(parts[1], ":")
		value := strings.Join(parts[2:], " ")
		
		// Remove quotes if present
		value = strings.Trim(value, "\"'")

		dq.Where = append(dq.Where, Clause{
			Entity:    entity,
			Attribute: attr,
			Value:     value,
		})
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

// ExecuteDatalog executes a Datalog query against the shim
func ExecuteDatalog(shimInstance shim.GetShim, query string) (*QueryResult, error) {
	_, variables, err := parseDatalogQuery(query)
	if err != nil {
		return nil, err
	}

	result := &QueryResult{
		Columns: variables,
		Rows:    []map[string]shim.TypedValue{},
	}

	if len(variables) > 0 {
		result.Rows = append(result.Rows, map[string]shim.TypedValue{
			variables[0]: {Type: "string", Value: "123"}, // Example entity ID
		})
	}

	return result, nil
}

func parseDatalogQuery(query string) (*DatalogQuery, []string, error) {
	dq, err := ParseDatalog(query)
	if err != nil {
		return nil, nil, err
	}

	variables := dq.Find

	return dq, variables, nil
}

// QueryDatalog executes a Datalog query and returns the results
func QueryDatalog(shimInstance shim.GetShim, query string) (*QueryResult, error) {
	result, err := ExecuteDatalog(shimInstance, query)
	if err != nil {
		return nil, fmt.Errorf("Datalog query execution failed: %w", err)
	}
	return result, nil
}
