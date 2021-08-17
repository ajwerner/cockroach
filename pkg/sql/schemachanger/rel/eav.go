package rel

import "github.com/cockroachdb/errors"

// entityIterator is used to iterate Entities.
type entityIterator interface {
	// Visit visits an entity. If iterutil.StopIteration
	// is returned, iteration will stop but no error is returned.
	visit(*entity) error
}

// Attribute is used to define a property of entities. Attributes in a given
// schema have a type. They are one the members of the 3-tuple which defines
// a Datom.
type Attribute interface {

	// String is used when formatting the attribute.
	String() string

	// Ordinal is used to cheaply identify the attribute.
	Ordinal() Ordinal
}

// Ordinal is used to correlate attributes in a schema.
// It enables use of the ordinalSet.
type Ordinal uint64

func panicf(format string, args ...interface{}) {
	panic(errors.AssertionFailedWithDepthf(1, format, args...))
}
