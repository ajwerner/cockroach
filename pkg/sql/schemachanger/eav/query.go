package eav

import (
	"sort"

	"github.com/cockroachdb/errors"
)

// Query searches for sets of entities which uphold A set of constraints.
type Query struct {
	schema *Schema
	// rules are the original rules. They exist for debugging.
	rules []Clause

	// variables is the mapping of names to slots.
	variables map[Var]slotIdx
	// entities is the mapping of entities to slots.
	entities []slotIdx
	// slots store the data and metadata about the slots.
	slots []slot
	// facts are the set of facts which must be unified.
	facts []fact
}

// Result represents A setting of entities which fulfills the
// constraints of its corresponding query. It is a rather low-level
// interface.
type Result interface {
	IterateVars(func(Var, interface{}))

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
		case error:
			err = errors.Wrap(r, "failed to construct query")
		default:
			err = errors.AssertionFailedf("failed to construct query: %v", r)
		}
	}()
	q := newQuery(sc, clauses)
	return q, nil
}

func newQuery(sc *Schema, clauses []Clause) *Query {
	p := &queryBuilder{
		sc:   sc,
		vars: map[Var]slotIdx{},
	}
	for _, t := range clauses {
		p.processClause(t)
	}

	// Order the facts for unification. The ordering is first by entity
	// variable and then by attribute.
	//
	// TODO(ajwerner): For disjunctions using Any, the code currently uses
	// the index to constrain the search for each value in the "first"
	// such fact for the entity. Maybe we should trust the user order of
	// facts for a given entity rather than sorting by attribute ordinal.
	// However, we do need all the facts with the same entity and attribute
	// to be adjacent for the unification fixed point evaluation to work.
	entities := p.findEntitySlots()
	sort.SliceStable(p.facts, func(i, j int) bool {
		if p.facts[i].entity == p.facts[j].entity {
			return p.facts[i].attr.Ordinal() < p.facts[j].attr.Ordinal()
		}
		return p.facts[i].entity < p.facts[j].entity
	})
	// Ensure that the query does not already contain a contradiction as that
	// is almost definitely a bug.
	if contradictionFound := unify(p.facts, p.slots, nil); contradictionFound {
		panic(errors.Errorf("query contains contradiction"))
	}
	return &Query{
		schema:    sc,
		variables: p.vars,
		rules:     clauses,
		entities:  entities,
		facts:     p.facts,
		slots:     p.slots,
	}
}

// Prepare constructs a prepared query which can be used to iterate the results
// of a database. Prepare is safe for concurrent use. The returned
// PreparedQuery may not be used concurrently.
func (q *Query) Prepare() PreparedQuery {
	return newEvalContext(q)
}
