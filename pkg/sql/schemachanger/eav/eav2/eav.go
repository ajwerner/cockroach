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

func (e *Entity) init(value reflect.Value, ti *entityTypeSchema, f func(child Entity) error) error {
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
			ti.sc.asEntities(val, func(entity Entity) error {
				if err := f(entity); err != nil {
					return err
				}
				if entity.Interface() == val {
					e.copyFrom(entity.Values)
				}
				return nil
			})
		}
		if e.attrs.Contains(field.attr.Ordinal()) {
			panicf("%v already contains %v %v", ti.typ, field.attr, field)
		}
		compVal := field.comparableValue(e.ptr)
		e.attrs = e.attrs.Add(field.attr.Ordinal())
		e.m[field.attr.Ordinal()] = compVal
	}
	return nil
}

// AsValues converts an entity into a Values map.
// If the Entity is not a known type to the Schema, then an
// error will be returned.
func (s *Schema) asEntities(e interface{}, f func(Entity) error) error {
	ti, v, ok := s.getValueInfo(e)
	if !ok {
		return errors.Errorf("unknown type %T", e)
	}
	var entity Entity
	if err := entity.init(v, ti, f); err != nil {
		return err
	}
	return f(entity)
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

func panicf(format string, args ...interface{}) {
	panic(errors.AssertionFailedWithDepthf(1, format, args...))
}

func NewSchema(m Mappings) *Schema {
	isStructPointer := func(tt reflect.Type) bool {
		return tt.Kind() == reflect.Ptr && tt.Elem().Kind() == reflect.Struct
	}
	attrTypes := make(map[Attribute]reflect.Type)
	attrByOrd := make(map[Ordinal]Attribute)
	sc := &Schema{}
	maybeAddAttribute := func(a Attribute, typ reflect.Type) {
		// TODO(ajwerner): Validate that t is an okay type for an attribute
		// to be.
		if prev, exists := attrTypes[a]; exists {
			if prev.Kind() == reflect.Interface {
				if !typ.Implements(prev) {
					panicf(
						"%v does not implement %v as previously defined for %s",
						typ, prev, a,
					)
				}
			} else if prev != typ {
				panicf(
					"%v is not %v as previously defined for %s",
					typ, prev, a,
				)
			}
		} else {
			attrTypes[a] = typ
			attrByOrd[a.Ordinal()] = a
		}
	}
	for a, t := range m.AttributeTypes {
		maybeAddAttribute(a, t)
	}

	// We want to know what all of the entity types are
	entityTypeHandlers := make(map[reflect.Type]*entityTypeSchema)
	typeToComparableType := make(map[reflect.Type]reflect.Type)

	getComparableTypeMapping := func(typ reflect.Type) reflect.Type {
		compType, ok := typeToComparableType[reflect.PtrTo(typ)]
		if !ok {
			compType = getComparableType(typ)
			typeToComparableType[reflect.PtrTo(typ)] = compType
		}
		return compType
	}

	var maybeAddTypeMapping func(t reflect.Type, fields TypeMappings)
	maybeAddTypeMapping = func(t reflect.Type, fields TypeMappings) {
		// We mark the type as being added by putting a nil entry in the map.
		// This way, if we recurse into this closure, we'll detect the cycle.
		// TODO(ajwerner): Better cycle error reporting.
		{
			existing, ok := entityTypeHandlers[t]
			if ok {
				if existing != nil {
					return
				}
				panicf("cycle detected for type %v", t)
			}
			entityTypeHandlers[t] = nil
		}

		if !isStructPointer(t) {
			panicf("%v is not a pointer to a struct", t)
		}
		var fieldInfos []fieldInfo
		for fieldName, attr := range fields {
			names := strings.Split(fieldName, ".")
			// TODO(ajwerner): Decide if we're willing to go pointer chasing
			// and, if so, figure out how to reason about nil.
			var offset uintptr
			cur := t.Elem()
			for _, n := range names {
				sf, ok := cur.FieldByName(n)
				if !ok {
					panicf("%T.%s is not a field", t, fieldName)
				}
				offset += sf.Offset
				cur = sf.Type
			}
			// TODO(ajwerner): Deal with making entities out of structs themselves.
			maybeAddAttribute(attr, cur)
			curIsPtr := isStructPointer(cur)
			if curIsPtr {
				curFields, ok := m.TypeMappings[cur]
				if !ok {
					maybeAddTypeMapping(cur, curFields)
				}
			}

			f := fieldInfo{
				attr:    attr,
				inherit: curIsPtr,
			}
			{
				vg := makeValueGetter(cur, offset)
				if curIsPtr {
					f.value = func(u uintptr) interface{} {
						got := vg(u)
						if got.Elem().IsNil() {
							return nil
						}
						return got.Elem().Interface()
					}
				} else {
					f.value = func(u uintptr) interface{} { return vg(u).Interface() }
				}
			}
			{
				compType := getComparableTypeMapping(cur)
				vg := makeValueGetter(compType, offset)
				f.comparableValue = func(u uintptr) interface{} {
					return vg(u).Interface()
				}
			}
			fieldInfos = append(fieldInfos, f)
		}

		entityTypeHandlers[t] = &entityTypeSchema{
			typ:    t,
			sc:     sc,
			fields: fieldInfos,
		}
	}

	for t, fields := range m.TypeMappings {
		maybeAddTypeMapping(t, fields)
	}

	*sc = Schema{
		attributes:          nil,
		attributesByOrdinal: attrByOrd,
		attributeTypes:      attrTypes,
		entityTypeHandlers:  entityTypeHandlers,
		comparableTypeMap:   typeToComparableType,
	}
	return sc
}

var kindTypeMap = map[reflect.Kind]reflect.Type{
	reflect.Int:     reflect.TypeOf((*int)(nil)).Elem(),
	reflect.Int64:   reflect.TypeOf((*int64)(nil)).Elem(),
	reflect.Int32:   reflect.TypeOf((*int32)(nil)).Elem(),
	reflect.Int16:   reflect.TypeOf((*int16)(nil)).Elem(),
	reflect.Int8:    reflect.TypeOf((*int8)(nil)).Elem(),
	reflect.Uint:    reflect.TypeOf((*uint)(nil)).Elem(),
	reflect.Uint64:  reflect.TypeOf((*uint64)(nil)).Elem(),
	reflect.Uint32:  reflect.TypeOf((*uint32)(nil)).Elem(),
	reflect.Uint16:  reflect.TypeOf((*uint16)(nil)).Elem(),
	reflect.Uint8:   reflect.TypeOf((*uint8)(nil)).Elem(),
	reflect.Uintptr: reflect.TypeOf((*uintptr)(nil)).Elem(),
	reflect.String:  reflect.TypeOf((*string)(nil)).Elem(),
	reflect.Ptr:     reflect.TypeOf((*uintptr)(nil)).Elem(),
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

type childMap struct {
	attr Attribute
	ti   *entityTypeSchema
	gen  func(parent, childPtr interface{})
}

type entityTypeSchema struct {
	sc     *Schema
	typ    reflect.Type
	fields []fieldInfo
	// intensional          bool
}

type fieldInfo struct {
	attr            Attribute
	comparableValue func(uintptr) interface{}
	value           func(uintptr) interface{}
	inherit         bool
}

type TypeMappings map[string]Attribute

type junk struct {
	// Fields is a map from public field names to the corresponding attribute.
	Fields interface{}
	// Children is a mapping of Attribute to functions to generate child
	// entity objects.
	Children map[Attribute]interface{} // map[Attribute]func(this ThisT, child *ChildT)
	// Intensional indicates that there may be more than one entity in the
	// database which has all of the fields with values the same as this one,
	// other than its address. This is as opposed to extensional, meaning it
	// has an identity is defined as the setting of its attributes (other than
	// address). If false, a unique constraint will be added for values of this
	// type such that attempts to insert an equivalent value into the database
	// will result in an error and such that containment queries will return
	// results based on properties and not address equality.
	Intensional bool
}

type ParentMappings map[Attribute]interface{}

type UniqueConstraint struct {
	Name  string
	Attrs []Attribute
}

type Mappings struct {
	// Will be inferred from fields. Must be defined for
	// attributes which are not in fields.
	AttributeTypes map[Attribute]reflect.Type
	TypeMappings   map[reflect.Type]TypeMappings
	// TODO(ajwerner): Unique constraints, extensional types
}
