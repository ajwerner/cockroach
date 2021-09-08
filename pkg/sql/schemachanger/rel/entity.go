package rel

import (
	"reflect"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
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

	// valuesMap stores all the attributes, including the types and pointer.
	valuesMap
}

func (sc *Schema) EqualOn(attrs []Attribute, a, b interface{}) (eq bool) {
	_, eq = sc.CompareOn(attrs, a, b)
	return eq
}

// CompareOn compares two entities. Note that it will panic if either variable
// is malformed.
func (sc *Schema) CompareOn(attrs []Attribute, a, b interface{}) (less, eq bool) {
	toPopulate := makeOrdinalSetWithAttributes(attrs)
	var ae, be entity
	set := func(v interface{}, e *entity) {
		if err := asEntities(sc, toPopulate, v, func(child entity) error {
			*e = child
			return nil
		}); err != nil {
			panic(err)
		}
	}
	set(a, &ae)
	set(b, &be)
	for _, a := range attrs {
		if less, eq = compareOn(a, &ae.valuesMap, &be.valuesMap); !eq {
			return less, eq
		}
	}
	return false, true
}

func (sc *Schema) IterateAttributes(
	entityI interface{}, f func(attribute Attribute, value interface{}) error,
) (err error) {
	var v entity
	if err := asEntities(sc, allOrdinals, entityI, func(child entity) error {
		v = child
		return nil
	}); err != nil {
		return err
	}
	ti := v.getTypeInfo()
	v.attrs.ForEach(sc, func(a Attribute) (wantMore bool) {
		if _, isScalar := ti.scalarAttrFields[a]; !isScalar ||
			// Only propagate user attributes.
			a.Ordinal() >= maxUserAttribute {
			return true
		}
		tv, ok := v.getTypedValue(a, nil)
		if !ok {
			err = errors.AssertionFailedf(
				"failed to get typed value for populated scalar attribute %v for %T",
				a, entityI,
			)
		} else {
			err = f(a, tv.toInterface())
		}
		return err == nil
	})
	if iterutil.Done(err) {
		err = nil
	}
	return err
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
//
// Another approach would be to store entities by the pointer to their
// box rather than its value. That way, we could avoid needing the database
// at the expense of allocating the box every time we decompose into an entity.
func (e *entity) getTypedValue(attr Attribute, entities map[uintptr]*entity) (typedValue, bool) {
	val := e.get(attr)
	if val == nil {
		return typedValue{}, false
	}
	var typ reflect.Type
	if attr == Type {
		typ = schemaTypePtrType
	} else if fi, ok := e.getTypeInfo().scalarAttrFields[attr]; ok {
		typ = fi.typ
	} else {
		ee, ok := entities[*val.(*uintptr)]
		if !ok {
			panic(errors.AssertionFailedf(
				"variable references an entity not stored in the database",
			))
		}
		val, typ = &ee.ptr, ee.getTypeInfo().typ
	}
	return typedValue{
		typ:   typ,
		value: val,
	}, true
}

func (sc *Schema) GetScalarField(attribute Attribute, v interface{}) (interface{}, error) {
	ti, value, err := getEntityValueInfo(sc, v)
	if err != nil {
		return nil, err
	}
	fi, ok := ti.scalarAttrFields[attribute]
	if !ok {
		return nil, errors.Errorf("no scalar field defined on %v for %v", ti.typ, attribute)
	}
	return fi.value(value.Pointer()), nil
}

// asEntities decomposes
func asEntities(s *Schema, toPopulate ordinalSet, v interface{}, f func(child entity) error) error {
	ti, value, err := getEntityValueInfo(s, v)
	if err != nil {
		return err
	}

	var e entity
	e.ptr = value.Pointer()
	e.typ = uintptr(unsafe.Pointer(ti))
	e.valuesMap.m = make(map[Ordinal]interface{})
	e.add(Type.Ordinal(), &e.typ)
	e.add(Self.Ordinal(), &e.ptr)
	for _, field := range ti.fields {
		if field.isEntity {
			val := field.value(e.ptr)
			if val == nil {
				continue
			}
			if err := asEntities(s, toPopulate, val, f); err != nil {
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
		return nil, reflect.Value{}, errors.Errorf("invalid nil variable value")
	}
	t, ok := s.entityTypeSchemas[vv.Type()]
	if !ok {
		return nil, reflect.Value{}, errors.Errorf("unknown type handler for %T", v)
	}
	// Note that the fact that we have an entry for this variable type is
	// how we get to assume that this type must be a struct pointer.
	// TODO(ajwerner): Consider how do deal with non-pointer structs here.
	// We could allocate a pointer here and take a shallow clone.
	if vv.IsNil() {
		return nil, reflect.Value{}, errors.Errorf("invalid nil %T variable value", vv.Type())
	}
	return t, vv, nil
}

func makeValueGetter(t reflect.Type, offset uintptr) func(uintptr) reflect.Value {
	return func(u uintptr) reflect.Value {
		return reflect.NewAt(t, unsafe.Pointer(u+offset))
	}
}
