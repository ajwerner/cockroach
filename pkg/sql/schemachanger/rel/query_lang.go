package rel

import (
	"reflect"

	"github.com/cockroachdb/errors"
)

// Var is a variable name. Everything is convention, but, when you create
// clauses and use variable names which are not part of the defined scope of
// the rule, the new variableSlots which will be created will have a scope prefix
// to try to ensure that they are unique. Given that, don't put `:` in your
// variable names.
type Var string

// Clause is the basic building block of a query. The most foundational
// clause is Datom which declares some fact about an attribute of a named
// variable.
type Clause interface {
	// clause is a marker interface to prevent external package from implementing
	// the interface.
	clause()
}

// Datom is a basic Clause. It declares that the provided attribute of the
// referenced variable must be the provided value. Value can be a constant,
// a Var, or a set of constants as returned from Any.
func Datom(entity Var, attr Attribute, value Expr) Clause {
	return &datomDecl{entity: entity, attribute: attr, value: value}
}

// Expr is an expression value in rel.
type Expr interface {
	expr() // marker
}

func (v Var) eq(expr Expr) Clause {
	return &eqDecl{v, expr}
}

type eqDecl struct {
	v    Var
	expr Expr
}

func (v Var) Attr(a Attribute, e Expr) Clause {
	return Datom(v, a, e)
}

// Type returns a clause enforcing that the variable has one of the types
// passed by constraining its TypeAttribute to the output of passing the
// args to Types.
func (v Var) Type(valuesForTypeOf ...interface{}) Clause {
	return Datom(v, TypeAttribute, Types(valuesForTypeOf...))
}

func (e *eqDecl) clause() {}

func (v Var) Eq(value interface{}) Clause {
	return v.eq(Value(value))
}

func (v Var) EqAny(val ...interface{}) Clause {
	return v.eq(Any(val...))
}

// Value returns an expr corresponding to a raw value.
func Value(value interface{}) Expr {
	return valueExpr{value: value}
}

// Var is an Expr.
func (Var) expr() {}

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

type datomDecl struct {
	entity    Var
	attribute Attribute
	value     Expr
}

func (f *datomDecl) clause() {}

var _ Clause = (*datomDecl)(nil)

// Any constructs an Expr that matches one of the provided values.
func Any(v ...interface{}) Expr {
	return anyExpr(v)
}

type valueExpr struct {
	value interface{}
}

func (v valueExpr) expr() {}

type anyExpr []interface{}

func (a anyExpr) expr() {}

// And constructs a clause represents a set of clauses which should
// be taken in conjunction and exist so that go functions can be written to
// return a single clause without needing to get involved in appending to
// lists. It can be viewed as syntactic sugar.
func And(terms ...Clause) Clause {
	return (*and)(&terms)
}

type and []Clause

func (a *and) clause() {}

// Types returns an expr consisting of the types of the arguments. If multiple
// values are passed, that it returns an expr representing Any of these types.
// The function panics if no values are passed. Note that this is expecting
// values and will call reflect.TypeOf internally; do not pass a reflect.Type.
func Types(valuesForTypeOf ...interface{}) Expr {
	switch len(valuesForTypeOf) {
	case 0:
		panic(errors.AssertionFailedf("must provide at least one value to Types"))
	case 1:
		ti := reflect.TypeOf(valuesForTypeOf[0])
		return Value(ti)
	default:
		types := make([]interface{}, len(valuesForTypeOf))
		for i, v := range valuesForTypeOf {
			types[i] = reflect.TypeOf(v)
		}
		return Any(types...)
	}
}
