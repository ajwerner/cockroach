package eav2

import (
	"reflect"
	"strings"
	"unsafe"

	"github.com/cockroachdb/errors"
)

type Schema struct {
	attributesByOrdinal  map[Ordinal]Attribute
	attributeTypes       map[Attribute]reflect.Type
	entityTypeSchemas    map[reflect.Type]*entityTypeSchema
	typeToComparableType map[reflect.Type]reflect.Type
}

type entityTypeSchema struct {
	sc               *Schema
	typ              reflect.Type
	fields           []fieldInfo
	scalarAttrFields map[Attribute]*fieldInfo
	// intensional          bool
}

type fieldInfo struct {
	typ             reflect.Type
	attr            Attribute
	comparableValue func(uintptr) interface{}
	value           func(uintptr) interface{}
	isEntity        bool
}

// Mappings defines how to map data types to attributes.
type Mappings struct {

	// Will be inferred from fields. Must be defined for
	// attributes which are not in fields.
	AttributeTypes map[Attribute]reflect.Type

	// TypeMappings is A map from A type to A map of fields to attributes.
	// The types must be struct pointers. The fields must be exported and
	// may be either primitive types or struct pointers.
	//
	// For struct pointers, new entities will be added and the reference to
	// that type will be stored in the current entity. An attribute may appear
	// more than once in A mapping in the case that all of the times it appears
	// are for pointers and at most one of those pointers is non-nil.
	//
	// TODO(ajwerner): Support pointers to primitive types as well as interface
	// values. Interface values get tricky.
	TypeMappings map[reflect.Type]map[string]Attribute

	// TODO(ajwerner): Unique constraints, extensional types
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
	maybeAddAttribute(TypeAttribute, reflect.TypeOf((*reflect.Type)(nil)).Elem())
	maybeAddAttribute(IDAttribute, reflect.TypeOf((*interface{})(nil)).Elem())

	// We want to know what all of the entity types are
	entityTypeHandlers := make(map[reflect.Type]*entityTypeSchema)
	typeToComparableType := make(map[reflect.Type]reflect.Type)

	getComparableTypeMapping := func(typ reflect.Type) reflect.Type {
		compType, ok := typeToComparableType[typ]
		if !ok {
			compType = getComparableType(typ)
			typeToComparableType[typ] = compType
		}
		return compType
	}

	var maybeAddTypeMapping func(t reflect.Type, fields map[string]Attribute)
	maybeAddTypeMapping = func(t reflect.Type, fields map[string]Attribute) {
		// We mark the type as being added by putting A nil entry in the map.
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
			panicf("%v is not A pointer to A struct", t)
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
					panicf("%T.%s is not A field", t, fieldName)
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
				attr:     attr,
				isEntity: curIsPtr,
				typ:      cur,
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
		scalarAttrFields := make(map[Attribute]*fieldInfo)
		for i := range fieldInfos {
			fi := &fieldInfos[i]
			if fi.isEntity {
				continue
			}
			scalarAttrFields[fi.attr] = fi
		}
		entityTypeHandlers[t] = &entityTypeSchema{
			typ:              t,
			sc:               sc,
			fields:           fieldInfos,
			scalarAttrFields: scalarAttrFields,
		}
	}

	for t, fields := range m.TypeMappings {
		maybeAddTypeMapping(t, fields)
	}

	*sc = Schema{
		attributesByOrdinal:  attrByOrd,
		attributeTypes:       attrTypes,
		entityTypeSchemas:    entityTypeHandlers,
		typeToComparableType: typeToComparableType,
	}
	return sc
}

func (s *Schema) At(o Ordinal) Attribute {
	attr, _ := s.attributesByOrdinal[o]
	return attr
}

func (s *Schema) getValueInfo(v interface{}) (*entityTypeSchema, reflect.Value, bool) {
	vv := reflect.ValueOf(v)
	if !vv.IsValid() {
		return nil, reflect.Value{}, false
	}
	t, ok := s.entityTypeSchemas[vv.Type()]
	if !ok {
		panic(errors.AssertionFailedf("unknown type handler for %T", v))
	}
	return t, vv, ok
}

func makeValueGetter(t reflect.Type, offset uintptr) func(uintptr) reflect.Value {
	return func(u uintptr) reflect.Value {
		return reflect.NewAt(t, unsafe.Pointer(u+offset))
	}
}
