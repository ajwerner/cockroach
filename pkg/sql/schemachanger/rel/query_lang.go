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
// entity variable.
type Clause interface {
	// clause is a marker interface to prevent external package from implementing
	// the interface.
	clause()
}

// Datom is a basic Clause. It declares that the provided attribute of the
// referenced entity must be the provided value. Value can be a constant,
// a Var, or a set of constants as returned from Any.
func Datom(entity Var, attr Attribute, value interface{}) Clause {
	return &datomDecl{entity: entity, attribute: attr, value: value}
}

type datomDecl struct {
	entity    Var
	attribute Attribute
	value     interface{}
}

func (f *datomDecl) clause() {}

var _ Clause = (*datomDecl)(nil)

// Any is a mechanism to create a value which accepts
// more than one value.
//
// TOOD(ajwerner): Consider replacing with the more general Or
// that takes terms.
func Any(v ...interface{}) interface{} {
	return any(v)
}

type any []interface{}

// And constructs a clause represents a set of clauses which should
// be taken in conjunction and exist so that go functions can be written to
// return a single clause without needing to get involved in appending to
// lists. It can be viewed as syntactic sugar.
func And(terms ...Clause) Clause {
	return (*and)(&terms)
}

type and []Clause

func (a *and) clause() {}

// EntityType returns a clause enforcing that the entity has one of the types
// passed by constraining its TypeAttribute. The function panics if no valuesMap
// are passed. Note that this is expecting valuesMap and will call reflect.TypeOf
// internally; do not pass a reflect.Type here.
func EntityType(entity Var, valuesForTypeOf ...interface{}) Clause {
	switch len(valuesForTypeOf) {
	case 0:
		panic(errors.AssertionFailedf("must provide at least one value to EntityType"))
	case 1:
		ti := reflect.TypeOf(valuesForTypeOf[0])
		return Datom(entity, TypeAttribute, ti)
	default:
		types := make([]interface{}, len(valuesForTypeOf))
		for i, v := range valuesForTypeOf {
			types[i] = reflect.TypeOf(v)
		}
		return Datom(entity, TypeAttribute, Any(types...))
	}
}
