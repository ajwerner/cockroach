package rel

import (
	"sort"

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
	defer catchError(&err)
	q := newQuery(sc, clauses)
	return q, nil
}

func newQuery(sc *Schema, clauses []Clause) *Query {
	p := &queryBuilder{
		sc:            sc,
		variableSlots: map[Var]slotIdx{},
	}
	for _, t := range clauses {
		p.processClause(t)
	}

	// Order the facts for unification. The ordering is first by variable
	// variable and then by attribute.
	//
	// TODO(ajwerner): For disjunctions using Any, the code currently uses
	// the index to constrain the search for each value in the "first"
	// such fact for the variable. Maybe we should trust the user order of
	// facts for a given variable rather than sorting by attribute ordinal.
	// However, we do need all the facts with the same variable and attribute
	// to be adjacent for the unification fixed point evaluation to work.
	entities := p.findEntitySlots()
	sort.SliceStable(p.facts, func(i, j int) bool {
		if p.facts[i].variable == p.facts[j].variable {
			return attrLess(p.facts[i].attr, p.facts[j].attr)
		}
		return p.facts[i].variable < p.facts[j].variable
	})
	// Ensure that the query does not already contain a contradiction as that
	// is almost definitely a bug.
	if contradictionFound, _, attr := unify(p.facts, p.slots, nil); contradictionFound {
		panic(errors.Errorf("query contains contradiction on %v", attr))
	}
	return &Query{
		schema:        sc,
		variables:     p.variables,
		variableSlots: p.variableSlots,
		clauses:       clauses,
		entities:      entities,
		facts:         p.facts,
		slots:         p.slots,
		filters:       p.filters,
	}
}

func attrLess(a, b Attribute) bool {
	switch {
	case a != nil && b != nil:
		return a.Ordinal() < b.Ordinal()
	case a != nil:
		return false
	case b != nil:
		return true
	default:
		return false
	}
}

// Prepare constructs a prepared query which can be used to iterate the results
// of a database. Prepare is safe for concurrent use. The returned
// PreparedQuery may not be used concurrently.
func (q *Query) Prepare() PreparedQuery {
	return newEvalContext(q)
}
