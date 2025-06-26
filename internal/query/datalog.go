package query

import (
	"fmt"
	"strings"

	"causal-consistency-shim/internal/shim"
)

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
