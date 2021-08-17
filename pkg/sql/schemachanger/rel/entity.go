package rel

import (
	"reflect"
	"unsafe"

	"github.com/cockroachdb/errors"
)

// entity is the internal representation of a struct pointer.
// The idea is that ptr is the pointer itself and typ is a
// pointer to the entityTypeSchema.
type entity struct {
	// Part of the reason ptr exists her and not just in the map is that we need
	// to store a pointer to a value everywhere. Where better to attach that
	// pointer than here, to this struct? The value stored in valuesMap will be
	// pointing to this field.
	ptr uintptr // interface{}
	typ uintptr // *entityTypeSchema

	// valuesMap stores all of the attributes, including the types and pointer.
	// TODO(ajwerner): I seem to recall that we were not setting the type or
	// pointer value in the bitmap. Figure that out.
	valuesMap
}

func (e *entity) getTypeInfo() *entityTypeSchema {
	return (*entityTypeSchema)(unsafe.Pointer(e.typ))
}

// TODO(ajwerner): document what's going on here. For scalar fields we know
// the type because we do not permit oneOf behavior. For entity fields, we
// don't store the type because it's dynamic. Instead we know that if the
// field points to an entity, then the database has the entity indexed by
// its address and the entity knows its type. Because of that, typ will
// be nil if isEntity is true.
func (e *entity) getValueAndType(
	attr Attribute,
) (value interface{}, typ reflect.Type, isEntity bool) {
	if attr == TypeAttribute {
		return e.get(attr), schemaTypePtrType, false
	}
	ti := e.getTypeInfo()
	fi, ok := ti.scalarAttrFields[attr]
	if !ok {
		return e.get(attr), nil, true
	}
	return e.get(attr), fi.typ, false
}

// asEntities decomposes
func asEntities(s *Schema, v interface{}, f func(child entity) error) error {
	ti, value, err := getEntityValueInfo(s, v)
	if err != nil {
		return err
	}

	var e entity
	e.ptr = value.Pointer()
	e.typ = uintptr(unsafe.Pointer(ti))
	e.valuesMap.m = make(map[Ordinal]interface{})
	e.add(TypeAttribute.Ordinal(), &e.typ)
	e.add(IDAttribute.Ordinal(), &e.ptr)
	for _, field := range ti.fields {
		if field.isEntity {
			val := field.value(e.ptr)
			if val == nil {
				continue
			}
			if err := asEntities(s, val, f); err != nil {
				return errors.Wrapf(err, "field %s", field.path)
			}
			if e.attrs.Contains(field.attr.Ordinal()) {
				return errors.Errorf("%v contains more than one non-nil entry for %v at %s", ti.typ, field.attr, field)
			}
		}
		compVal := field.comparableValue(e.ptr)
		e.add(field.attr.Ordinal(), compVal)
	}
	return f(e)
}

func getEntityValueInfo(s *Schema, v interface{}) (*entityTypeSchema, reflect.Value, error) {
	vv := reflect.ValueOf(v)
	if !vv.IsValid() {
		return nil, reflect.Value{}, errors.Errorf("invalid nil entity value")
	}
	t, ok := s.entityTypeSchemas[vv.Type()]
	if !ok {
		return nil, reflect.Value{}, errors.Errorf("unknown type handler for %T", v)
	}
	// Note that the fact that we have an entry for this entity type is
	// how we get to assume that this type must be a struct pointer.
	// TODO(ajwerner): Consider how do deal with non-pointer structs here.
	// We could allocate a pointer here and take a shallow clone.
	if vv.IsNil() {
		return nil, reflect.Value{}, errors.Errorf("invalid nil %T entity value", vv.Type())
	}
	return t, vv, nil
}

func makeValueGetter(t reflect.Type, offset uintptr) func(uintptr) reflect.Value {
	return func(u uintptr) reflect.Value {
		return reflect.NewAt(t, unsafe.Pointer(u+offset))
	}
}
