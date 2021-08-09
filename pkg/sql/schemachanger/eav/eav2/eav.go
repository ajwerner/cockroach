package eav2

import (
	"reflect"
	"strings"
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

func (s *Schema) At(o Ordinal) Attribute {
	attr, _ := s.attributesByOrdinal[o]
	return attr
}

// GetAttribute returns the attribute value for the provided attribute for
// an entity. It returns nil if the entity type is not defined or the
// entity does not contain this type.
func (s *Schema) GetAttribute(a Attribute, v interface{}) interface{} {
	t, vv, ok := s.getValueInfo(v)
	if !ok {
		return nil
	}
	f, ok := t.attrValues[a]
	if !ok {
		return nil
	}
	return f(vv.Pointer())
}

func (s *Schema) getValueInfo(v interface{}) (*entityTypeSchema, reflect.Value, bool) {
	vv := reflect.ValueOf(v)
	if !vv.IsValid() {
		return nil, reflect.Value{}, false
	}
	t, ok := s.entityTypeHandlers[vv.Type()]
	if !ok {
		panic(errors.AssertionFailedf("unknown type handler for %T", v))
	}
	return t, vv, ok
}

func (s *Schema) getComparableValue(attr Attribute, e Entity) interface{} {
	if v, ok := e.(Values); ok {
		vv := v.Get(attr)
		if vv == nil {
			return nil
		}
		vvv := reflect.ValueOf(vv)
		compType := s.comparableTypeMap[vvv.Type()]
		if compType == nil {
			panic(errors.AssertionFailedf("failed to get comparable type for %T %v", vv, s.comparableTypeMap))
		}
		return vvv.Convert(compType).Interface()
	}
	t, vv, ok := s.getValueInfo(e)
	if !ok {
		return nil
	}
	f, ok := t.attrComparableValues[attr]
	if !ok {
		return nil
	}
	return f(vv.Pointer())
}

func (sc *Schema) GetAttributes(entity Entity) OrdinalSet {
	if v, ok := entity.(Values); ok {
		return v.Attributes()
	}
	ei, _, ok := sc.getValueInfo(entity)
	if !ok {
		panic("here")
	}
	return ei.attributes
}

func (sc *Schema) Set(vm Values, a Attribute, v interface{}) {
	if vm.sc != sc {
		panic("here")
	}
	typ := sc.attributeTypes[a]
	vv := reflect.ValueOf(v)
	if vv.Type().Kind() == reflect.Ptr && vv.Type().Elem() == typ {
		vm.m[a.Ordinal()] = v
		return
	}
	if vv.Type() == typ {
		vp := reflect.New(vv.Type())
		vp.Elem().Set(vv)
		vm.m[a.Ordinal()] = vp.Interface()
		return
	}
	panic(errors.AssertionFailedf("expected %v for attribute %s, got %T", typ, a, v))
}

var AttrType Attribute = attrType{}

type attrType struct{}

func (a attrType) String() string {
	return "type"
}

func (a attrType) Ordinal() Ordinal {
	return 0
}

func NewSchema(m Mappings) *Schema {
	panicf := func(format string, args ...interface{}) {
		panic(errors.AssertionFailedWithDepthf(1, format, args...))
	}
	structTypeFromStructPointer := func(t interface{}) reflect.Type {
		tt := reflect.TypeOf(t)
		if tt.Kind() != reflect.Ptr {
			panicf("%T is not a pointer to a struct", t)
		}
		if tt.Elem().Kind() != reflect.Struct {
			panicf("%T is not a pointer to a struct", t)
		}
		return tt.Elem()
	}
	attrTypes := make(map[Attribute]reflect.Type)
	attrByOrd := make(map[Ordinal]Attribute)
	maybeAddAttribute := func(a Attribute, typ reflect.Type) {
		// TODO(ajwerner): Validate that t is an okay type for an attribute
		// to be.
		if prev, exists := attrTypes[a]; exists && prev != typ {
			panicf(
				"%T not %T as previously defined for %s",
				typ, prev, a,
			)
		}
		attrTypes[a] = typ
		if prev, exists := attrByOrd[a.Ordinal()]; exists && prev != a {
			panicf(
				"%s not %s as previously defined for %d",
				prev, a, a.Ordinal(),
			)
		}
		attrByOrd[a.Ordinal()] = a
	}
	for a, t := range m.AttributeTypes {
		maybeAddAttribute(a, reflect.TypeOf(t))
	}
	maybeAddAttribute(AttrType, reflectTypeType)
	// We want to know what all of the entity types are
	entityTypeHandlers := make(map[reflect.Type]*entityTypeSchema)
	typeToComparableType := make(map[reflect.Type]reflect.Type)
	for t, fields := range m.TypeFieldMappings {
		toValue := make(map[Attribute]func(uintptr) interface{})
		toComparableValue := make(map[Attribute]func(uintptr) interface{})
		tet := structTypeFromStructPointer(t)
		var attrs OrdinalSet
		toValue[AttrType] = func(uintptr uintptr) interface{} { return reflect.TypeOf(t) }
		toComparableValue[AttrType] = toValue[AttrType]

		for fieldName, attr := range fields {
			names := strings.Split(fieldName, ".")
			// TODO(ajwerner): Decide if we're willing to go pointer chasing.
			var offset uintptr
			cur := tet
			for _, n := range names {
				sf, ok := cur.FieldByName(n)
				if !ok {
					panicf("%T.%s is not a field", t, fieldName)
				}
				offset += sf.Offset
				cur = sf.Type
			}

			// Check to make sure this is not re-defining the attribute.
			// Compute the lookup function.
			attrs = attrs.Add(attr.Ordinal())

			maybeAddAttribute(attr, cur)
			compType, ok := typeToComparableType[reflect.PtrTo(cur)]
			if !ok {
				compType = getComparableType(cur)
				typeToComparableType[reflect.PtrTo(cur)] = compType
			}
			f := makeValueGetter(cur, offset)
			toValue[attr] = func(u uintptr) interface{} {
				return f(u).Interface()
			}
			toComparableValue[attr] = func(u uintptr) interface{} {
				return f(u).Convert(compType).Interface()
			}
		}
		entityTypeHandlers[reflect.TypeOf(t)] = &entityTypeSchema{
			attributes:           attrs,
			attrComparableValues: toComparableValue,
			attrValues:           toValue,
		}
	}
	return &Schema{
		attributes:          nil,
		attributesByOrdinal: attrByOrd,
		attributeTypes:      attrTypes,
		entityTypeHandlers:  entityTypeHandlers,
		comparableTypeMap:   typeToComparableType,
	}
}

var kindTypeMap = map[reflect.Kind]reflect.Type{
	reflect.Int:    reflect.TypeOf((*int)(nil)),
	reflect.Int64:  reflect.TypeOf((*int64)(nil)),
	reflect.Int32:  reflect.TypeOf((*int32)(nil)),
	reflect.Int16:  reflect.TypeOf((*int16)(nil)),
	reflect.Int8:   reflect.TypeOf((*int8)(nil)),
	reflect.Uint:   reflect.TypeOf((*uint)(nil)),
	reflect.Uint64: reflect.TypeOf((*uint64)(nil)),
	reflect.Uint32: reflect.TypeOf((*uint32)(nil)),
	reflect.Uint16: reflect.TypeOf((*uint16)(nil)),
	reflect.Uint8:  reflect.TypeOf((*uint8)(nil)),
	reflect.String: reflect.TypeOf((*string)(nil)),
}

func getComparableType(t reflect.Type) reflect.Type {
	ct, ok := kindTypeMap[t.Kind()]
	if !ok {
		panic(errors.AssertionFailedf(
			"unsupported type %T of kind %v",
			t, t.Kind(),
		))
	}
	return ct
}

var (
	reflectTypeType = reflect.TypeOf((*reflect.Type)(nil)).Elem()
)

func makeValueGetter(t reflect.Type, offset uintptr) func(uintptr) reflect.Value {
	return func(u uintptr) reflect.Value {
		return reflect.NewAt(t, unsafe.Pointer(u+offset))
	}
}

type Schema struct {
	comparableTypeMap   map[reflect.Type]reflect.Type
	attributes          []Attribute
	attributesByOrdinal map[Ordinal]Attribute
	attributeTypes      map[Attribute]reflect.Type
	entityTypeHandlers  map[reflect.Type]*entityTypeSchema
}

type value interface {
	compare(other value) int
}

type entityTypeSchema struct {
	attributes           OrdinalSet
	attrComparableValues map[Attribute]func(uintptr) interface{}
	attrValues           map[Attribute]func(uintptr) interface{}
}

func (s *entityTypeSchema) getComparableValue(attr Attribute, av reflect.Value) interface{} {
	f, ok := s.attrComparableValues[attr]
	if !ok {
		return nil
	}
	return f(av.Pointer())
}

type FieldMappings map[string]Attribute

type ChildMappings map[Attribute]interface{}

type Mappings struct {
	// Will be inferred from fields. Must be defined for
	// attributes which are not in fields.
	AttributeTypes    map[Attribute]interface{}
	TypeFieldMappings map[interface{}]FieldMappings
	TypeChildMappings map[interface{}]ChildMappings
}
