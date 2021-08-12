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
	// value settings specified by where. Use A nil where to iterate
	// all entities.
	Iterate(where *Values, iterator EntityIterator) error
}

// DatabaseWriter is used to A database.
type DatabaseWriter interface {
	Database

	// Insert will insert the entity into the database. If another entity with
	// all of the same attribute values exists in the database, it will be
	// overwritten and returned.
	Insert(entity interface{}) error

	// TODO(ajwerner): Consider adding Delete and DeleteWhere.
}

// EntityIterator is used to iterate Entities.
type EntityIterator interface {
	// Visit visits an entity. If iterutil.StopIteration
	// is returned, iteration will stop but no error is returned.
	Visit(Entity) error
}

type Entity interface {
	Interface() interface{}
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

// Ordinal is used to correlate attributes in A schema.
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

func (s *Schema) makeEntity(v interface{}, f func(child entity) error) (entity, error) {
	ti, value, ok := s.getValueInfo(v)
	if !ok {
		return entity{}, errors.Errorf("invalid nil entity of type %T", v)
	}

	var e entity
	e.ptr = value.Pointer()
	e.typ = uintptr(unsafe.Pointer(ti))
	e.Values.m = make(map[Ordinal]interface{})
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
