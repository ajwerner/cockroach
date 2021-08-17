package rel

// SystemAttribute is a type which represents attributes offerred by the
// system for all entities stored in a database. In particular they capture
// the type and address of the entity.
//
// The system attribute may be extended to cover other structural attributes.
// TODO(ajwerner): Add support for slices, arrays, and maps and then provide
// system attributes to access slice/array indexes and map keys and valuesMap.
type SystemAttribute int8

//go:generate stringer -type SystemAttribute

// Ordinal is part of the Attribute interface.
func (s SystemAttribute) Ordinal() Ordinal {
	return Ordinal(s)
}

const (
	_ SystemAttribute = 64 - iota

	// TypeAttribute is an attribute which stores the type of an entity.
	TypeAttribute

	// IDAttribute is an attribute which stores the ID of an entity.
	IDAttribute

	maxUserAttribute Ordinal = 64 - iota
)

var _ Attribute = SystemAttribute(0)
