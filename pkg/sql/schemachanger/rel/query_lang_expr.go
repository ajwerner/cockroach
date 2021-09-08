package rel

import (
	"reflect"

	"github.com/cockroachdb/errors"
)

// Expr is an expression value in rel.
type Expr interface {
	expr() // marker
	forYAML() interface{}
}

// Var is a variable name. Everything is convention, but, when you create
// clauses and use variable names which are not part of the defined scope of
// the rule, the new variableSlots which will be created will have a scope prefix
// to try to ensure that they are unique. Given that, don't put `:` in your
// variable names.
type Var string

// Var is an Expr.
func (Var) expr() {}

// Value returns an expr corresponding to a raw value.
func Value(value interface{}) Expr {
	return valueExpr{value: value}
}

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
