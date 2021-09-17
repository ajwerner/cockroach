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

import "reflect"

// Expr is an expression value in rel.
type Expr interface {
	expr() // marker

	// encoded returns a value for use in serialization.
	encoded() interface{}
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
// Note that there's nothing magic in this function, it could be implemented
// elsewhere and should be thought of just a stdlib helper.
func Types(valueForTypeOf interface{}, moreValuesForTypeOf ...interface{}) Expr {
	typ := reflect.TypeOf(valueForTypeOf)
	if len(moreValuesForTypeOf) == 0 {
		return Value(typ)
	}

	types := make([]interface{}, 0, len(moreValuesForTypeOf)+1)
	types = append(types, typ)
	for _, v := range moreValuesForTypeOf {
		types = append(types, reflect.TypeOf(v))
	}
	return Any(types...)
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
