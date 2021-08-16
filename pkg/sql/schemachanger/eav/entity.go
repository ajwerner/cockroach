package eav

import (
	"unsafe"

	"github.com/cockroachdb/errors"
)

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
