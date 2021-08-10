package eav2

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav"
	"github.com/cockroachdb/errors"
)

// Database stores entities and allows iteration with filtering
// based on value equality for attributes.
type Database interface {

	// Schema describes the set of attributes and their types for the
	// entities to be stored.
	Schema() *Schema

	// Iterate iterates the database for all entities which have the
	// value settings specified by where. Use a nil where to iterate
	// all entities.
	Iterate(where Values, iterator EntityIterator) error
}

// DatabaseWriter is used to a database.
type DatabaseWriter interface {
	Database

	// Insert will insert the Entity into the database. If another entity with
	// all of the same attribute values exists in the database, it will be
	// overwritten and returned.
	Insert(entity Entity) (replaced Entity)

	// TODO(ajwerner): Consider adding Delete and DeleteWhere.
}

// EntityIterator is used to iterate Entities.
type EntityIterator interface {
	// Visit visits an Entity. If iterutil.StopIteration
	// is returned, iteration will stop but no error is returned.
	Visit(Entity) error
}

// EntityIteratorFunc implements EntityIterator.
type EntityIteratorFunc func(Entity) error

// Visit is part of the EntityIterator interface.
func (f EntityIteratorFunc) Visit(e Entity) error { return f(e) }

// TODO(ajwerner): Figure out what I want to do regarding primary keys.

type Attribute interface {
	String() string
	Ordinal() Ordinal
}

// Ordinal is used to correlate attributes in a schema.
// It enables use of the OrdinalSet.
type Ordinal = eav.Ordinal

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

func (s *Schema) makeEntity(v interface{}, f func(child Entity) error) (Entity, error) {
	ti, value, ok := s.getValueInfo(v)
	if !ok {
		return Entity{}, errors.Errorf("invalid nil entity of type %T", v)
	}

	var e Entity
	e.ptr = value.Pointer()
	e.typ = uintptr(unsafe.Pointer(ti))
	e.Values.m = make(map[Ordinal]interface{})
	e.Values.m[TypeAttribute.Ordinal()] = &e.typ
	e.Values.m[IDAttribute.Ordinal()] = &e.ptr
	for _, field := range ti.fields {
		if field.inherit {
			val := field.value(e.ptr)
			if val == nil {
				continue
			}
			child, err := s.makeEntity(val, f)
			if err != nil {
				return Entity{}, err
			}
			e.copyFrom(child.Values)
		}
		if e.attrs.Contains(field.attr.Ordinal()) {
			panicf("%v already contains %v %v", ti.typ, field.attr, field)
		}
		compVal := field.comparableValue(e.ptr)
		e.attrs = e.attrs.Add(field.attr.Ordinal())
		e.m[field.attr.Ordinal()] = compVal
	}
	return e, f(e)
}

// AsValues converts an entity into a Values map.
// If the Entity is not a known type to the Schema, then an
// error will be returned.
func (s *Schema) asEntities(e interface{}, f func(Entity) error) error {
	_, err := s.makeEntity(e, f)
	return err
}

func panicf(format string, args ...interface{}) {
	panic(errors.AssertionFailedWithDepthf(1, format, args...))
}
