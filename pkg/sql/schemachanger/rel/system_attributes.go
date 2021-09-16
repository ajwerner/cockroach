package rel

// systemAttribute is a type which represents attributes offerred by the
// system for all entities stored in a database. In particular they capture
// the type and address of the variable.
//
// The system attribute may be extended to cover other structural attributes.
// TODO(ajwerner): Add support for slices, arrays, and maps and then provide
// system attributes to access slice/array indexes and map keys and valuesMap.
type systemAttribute int8

//go:generate stringer -type SystemAttribute

const (
	_ systemAttribute = 64 - iota

	// Type is an attribute which stores the type of an variable.
	Type

	// Self is an attribute which stores the variable itself.
	Self

	maxUserAttribute ordinal = 64 - iota
)

func isSystemAttribute(a Attribute) bool {
	_, isSystemAttr := a.(systemAttribute)
	return isSystemAttr
}

var _ Attribute = systemAttribute(0)
