// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rel

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// Query searches for sets of entities which uphold A set of constraints.
type Query struct {
	schema *Schema
	// clauses are the original clauses. They exist for debugging.
	clauses []Clause
	// variables is the set of variables used in the query
	// stored in the order in which they appear.
	variables []Var
	// variableSlots is the mapping of names to slots.
	variableSlots map[Var]slotIdx
	// entities is the mapping of entities to slots.
	entities []slotIdx
	// slots store the data and metadata about the slots.
	slots []slot
	// facts are the set of facts which must be unified.
	facts []fact
	// filters are the set of predicate filters to evaluate.
	filters []filter
}

// Result represents A setting of entities which fulfills the
// constraints of its corresponding query. It is a rather low-level
// interface.
type Result interface {
	IterateVars(func(Var))

	Var(name Var) interface{}
}

// ResultIterator is used to iterate results of A query.
// Iteration can be halted with the use of iterutils.StopIteration.
type ResultIterator func(r Result) error

// PreparedQuery is used to evaluate a query against a database. It is
// not safe for concurrent iteration.
type PreparedQuery interface {
	Iterate(db *Database, ri ResultIterator) error
}

// MustQuery wraps NewQuery and panics any returned error.
func MustQuery(sc *Schema, clauses ...Clause) *Query {
	q, err := NewQuery(sc, clauses...)
	if err != nil {
		panic(err)
	}
	return q
}

// NewQuery construct a new query with the provided clauses forming the
// conjunction of constraints on the results of the query when it is
// evaluated against a database.
func NewQuery(sc *Schema, clauses ...Clause) (_ *Query, err error) {
	defer func() {
		switch r := recover().(type) {
		case nil:
			return
		case error:
			err = errors.Wrap(r, "failed to construct query")
		default:
			err = errors.AssertionFailedf("failed to construct query: %v", r)
		}
	}()
	q := newQuery(sc, clauses)
	return q, nil
}

// Prepare constructs a prepared query which can be used to iterate the results
// of a database. Prepare is safe for concurrent use. The returned
// PreparedQuery may not be used concurrently.
func (q *Query) Prepare() PreparedQuery {
	return newEvalContext(q)
}

// Entities returns the entities in the query in their join order.
func (q *Query) Entities() []Var {
	var entitySlots util.FastIntSet
	for _, slotIdx := range q.entities {
		entitySlots.Add(int(slotIdx))
	}
	vars := make([]Var, 0, len(q.entities))
	for v, slotIdx := range q.variableSlots {
		if !entitySlots.Contains(int(slotIdx)) {
			continue
		}
		vars = append(vars, v)
	}
	sort.Slice(vars, func(i, j int) bool {
		return q.variableSlots[vars[i]] < q.variableSlots[vars[j]]
	})
	return vars
}
