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

// Clause is the basic building block of a query. A query is defined as
// the conjunction of clauses.
type Clause interface {
	// clause is a marker interface to prevent external package from implementing
	// the interface.
	clause()
}

// Attr declares that an attribute of the entity represented by this var
// should have a value equal to expr.
func (v Var) Attr(a Attribute, e Expr) Clause {
	return &tripleDecl{v, a, e}
}

// Type returns a clause enforcing that the variable has one of the types
// passed by constraining its Type to the output of passing the
// args to Types. It is syntactic sugar around existing primitives.
func (v Var) Type(valueForTypeOf interface{}, moreValuesForTypeOf ...interface{}) Clause {
	return v.Attr(Type, Types(valueForTypeOf, moreValuesForTypeOf...))
}

// Eq return a clause enforcing that the var has the value
// specified by the expr.
func (v Var) Eq(expr Expr) Clause {
	return &eqDecl{v, expr}
}

// Entities is a shorthand for defining all the entities as having this
// variable as their value for this attribute.
//
// TODO(ajwerner): Better name.
func (v Var) Entities(attr Attribute, entities ...Var) Clause {
	terms := make([]Clause, len(entities))
	for i, e := range entities {
		terms[i] = e.Attr(attr, v)
	}
	return And(terms...)
}

// And constructs a clause represents a set of clauses which should
// be taken in conjunction and exist so that go functions can be written to
// return a single clause without needing to get involved in appending to
// a slice of clauses.
func And(terms ...Clause) Clause {
	return (*and)(&terms)
}

// Filter is used to construct a clause which runs an arbitrary predicate
// over variables.
func Filter(name string, vars ...Var) func(predicateFunc interface{}) Clause {
	return func(predicateFunc interface{}) Clause {
		return &filterDecl{
			name:          name,
			vars:          vars,
			predicateFunc: predicateFunc,
		}
	}
}
