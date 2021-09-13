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
type entity valuesMap

// Part of the reason ptr exists here and not just in the map is that we need
// to store a pointer to a value everywhere. Where better to attach that
// pointer than here, to this struct? The value stored in valuesMap will be
// pointing to this field. The other approach to all of this might be to
// just have the map and in it just store the object we've used to construct
// this. In practice, the interface header is the pointer and the type info.
// Then, to be honest, we could just use the schema to look up the field information.
//	ptr uintptr // interface{}
//typ uintptr // *entityTypeSchema

// valuesMap stores all the attributes, including the types and pointer.
//valuesMap
//}

func (sc *Schema) EqualOn(attrs []Attribute, a, b interface{}) (eq bool) {
	_, eq = sc.CompareOn(attrs, a, b)
	return eq
}

// CompareOn compares two entities. Note that it will panic if either variable
// is malformed.
func (sc *Schema) CompareOn(attrs []Attribute, a, b interface{}) (less, eq bool) {
	toPopulate := makeOrdinalSetWithAttributes(attrs)
	var ae, be *entity
	set := func(v interface{}, e **entity) {
		if err := asEntities(sc, toPopulate, v, func(child *entity) error {
			*e = child
			return nil
		}); err != nil {
			panic(err)
		}
	}
	set(a, &ae)
	set(b, &be)
	defer putValues((*valuesMap)(ae))
	defer putValues((*valuesMap)(be))
	for _, a := range attrs {
		if less, eq = compareOn(a, (*valuesMap)(ae), (*valuesMap)(be)); !eq {
			return less, eq
		}
	}
	return false, true
}

func (sc *Schema) IterateAttributes(
	entityI interface{}, f func(attribute Attribute, value interface{}) error,
) (err error) {
	var v *entity
	if err := asEntities(sc, allOrdinals, entityI, func(child *entity) error {
		if v != nil {
			putValues((*valuesMap)(v))
			v = nil
		}
		v = child
		return nil
	}); err != nil {
		return err
	}
	v.attrs.ForEach(sc, func(a Attribute) (wantMore bool) {
		if a.Ordinal() >= maxUserAttribute {
			return true
		}
		tv, ok := v.getTypedValue(sc, a)
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

func (e *entity) get(attribute Attribute) interface{} {
	switch attribute {
	case Type:
		return reflect.TypeOf(e.get(Self))
	default:
		return (*valuesMap)(e).get(attribute)
	}
}

func (e *entity) getTypeInfo(sc *Schema) *entityTypeSchema {
	return sc.entityTypeSchemas[e.get(Type).(reflect.Type)]
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
func (e *entity) getTypedValue(sc *Schema, attr Attribute) (typedValue, bool) {
	val := e.get(attr)
	if val == nil {
		return typedValue{}, false
	}
	var typ reflect.Type
	if attr == Type {
		typ = reflectTypeType
	} else if fi, ok := e.getTypeInfo(sc).attrFields[attr]; ok && !fi[0].isEntity {
		// This is a bit of a hack to deal with the fact that an attribute
		// might have multiple fields which can lead to its value.
		typ = fi[0].typ
	} else {
		typ = reflect.TypeOf(val)
	}
	return typedValue{
		typ:   typ,
		value: val,
	}, true
}

func (e *entity) asMap() *valuesMap {
	return (*valuesMap)(e)
}

func asEntities(
	s *Schema, toPopulate ordinalSet, v interface{}, f func(child *entity) error,
) error {
	ti, value, err := getEntityValueInfo(s, v)
	if err != nil {
		return err
	}

	e := getValues()
	e.add(Type.Ordinal(), value.Type())
	e.add(Self.Ordinal(), v)
	for _, field := range ti.fields {
		if field.isEntity {
			val := field.value(value.Pointer())
			if val == nil {
				continue
			}
			if err := asEntities(s, toPopulate, val, f); err != nil {
				return errors.Wrapf(err, "field %s", field.path)
			}
			if e.attrs.Contains(field.attr.Ordinal()) {
				return errors.Errorf("%v contains more than one non-nil entry for %v at %s", ti.typ, field.attr, field)
			}
			e.add(field.attr.Ordinal(), val)
			continue
		} else {
			compVal := field.comparableValue(value.Pointer())
			if field.isPtr && compVal == nil {
				continue
			}
			if compVal == nil {
				return errors.AssertionFailedf("got nil value for non-pointer scalar attribute %s of type %s", field.attr, ti.typ)
			}
			e.add(field.attr.Ordinal(), compVal)
		}
	}
	return f((*entity)(e))
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
