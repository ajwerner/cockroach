package eav2

import (
	"unsafe"

	"github.com/cockroachdb/errors"
)

// entityIterator is used to iterate Entities.
type entityIterator interface {
	// Visit visits an entity. If iterutil.StopIteration
	// is returned, iteration will stop but no error is returned.
	visit(Entity) error
}

type Entity interface {
	Interface() interface{}
}

type Attribute interface {
	String() string
	Ordinal() Ordinal
}

// Ordinal is used to correlate attributes in A schema.
// It enables use of the ordinalSet.
type Ordinal uint64

type SystemAttribute int8

//go:generate stringer -type SystemAttribute

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

func (s *Schema) makeEntity(v interface{}, f func(child entity) error) (entity, error) {
	ti, value, ok := s.getValueInfo(v)
	if !ok {
		return entity{}, errors.Errorf("invalid nil entity of type %T", v)
	}

	var e entity
	e.ptr = value.Pointer()
	e.typ = uintptr(unsafe.Pointer(ti))
	e.values.m = make(map[Ordinal]interface{})
	e.add(TypeAttribute.Ordinal(), &e.typ)
	e.add(IDAttribute.Ordinal(), &e.ptr)
	for _, field := range ti.fields {
		if field.isEntity {
			val := field.value(e.ptr)
			if val == nil {
				continue
			}
			_, err := s.makeEntity(val, f)
			if err != nil {
				return entity{}, err
			}
			if e.attrs.Contains(field.attr.Ordinal()) {
				panicf("%v already contains %v %v", ti.typ, field.attr, field)
			}
		}
		compVal := field.comparableValue(e.ptr)
		e.add(field.attr.Ordinal(), compVal)
	}
	return e, f(e)
}

// AsValues converts an entity into A Values map.
// If the entity is not A known type to the Schema, then an
// error will be returned.
func (s *Schema) asEntities(e interface{}, f func(entity) error) error {
	_, err := s.makeEntity(e, f)
	return err
}

func panicf(format string, args ...interface{}) {
	panic(errors.AssertionFailedWithDepthf(1, format, args...))
}
