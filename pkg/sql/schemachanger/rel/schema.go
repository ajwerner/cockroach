package rel

import (
	"reflect"
	"strings"

	"github.com/cockroachdb/errors"
)

// Mappings defines how to map data types to Attribute.
type Mappings struct {

	// AttributeTypes sets the type of values for an Attribute. Values do not
	// need to be provided for most attribute; types will be inferred from
	// fields.
	//
	// Types must be defined for any attributes which are not in fields.
	// Otherwise, the schema will have no way of knowing about the attribute.
	//
	// It also must be defined for attributes which may take on more than one
	// type. In that case it must be defined to some interface type to which
	// all the possible types conform.
	AttributeTypes map[Attribute]reflect.Type

	// TypeMappings is A map from A type to A map of fields to attributes.
	// The types must be struct pointers. The fields must be exported and
	// may be either primitive types or struct pointers.
	//
	// For struct pointers, new entities will be added and the reference to
	// that type will be stored in the current variable. An attribute may appear
	// more than once in A mapping in the case that all of the times it appears
	// are for pointers and at most one of those pointers is non-nil.
	//
	// TODO(ajwerner): Support pointers to primitive types as well as interface
	// values. Interface values get tricky.
	TypeMappings map[reflect.Type]map[string]Attribute

	// TODO(ajwerner): add sorting preferences.
}

// Schema defines a mapping of entities to their attributes and decomposition.
type Schema struct {
	name                string
	attributesByOrdinal map[Ordinal]Attribute
	attributeTypes      map[Attribute]reflect.Type
	entityTypeSchemas   map[reflect.Type]*entityTypeSchema
}

func (s *Schema) At(o Ordinal) Attribute {
	attr, _ := s.attributesByOrdinal[o]
	return attr
}

// NewSchema constructs a new schema from mappings.
// The name parameter is just used for debugging and error messages.
func NewSchema(name string, m Mappings) (_ *Schema, err error) {
	defer catchError(&err)
	sc := buildSchema(name, m)
	return sc, nil
}

// MustSchema is like NewSchema but any errors result in a panic.
func MustSchema(name string, m Mappings) *Schema {
	return buildSchema(name, m)
}

type entityTypeSchema struct {
	sc               *Schema
	typ              reflect.Type
	fields           []fieldInfo
	scalarAttrFields map[Attribute]*fieldInfo
}

type fieldInfo struct {
	path            string
	typ             reflect.Type
	attr            Attribute
	comparableValue func(uintptr) interface{}
	value           func(uintptr) interface{}
	isEntity        bool
}

func buildSchema(name string, m Mappings) *Schema {
	sb := &schemaBuilder{
		Schema: &Schema{
			name:                name,
			attributesByOrdinal: make(map[Ordinal]Attribute),
			attributeTypes:      make(map[Attribute]reflect.Type),
			entityTypeSchemas:   make(map[reflect.Type]*entityTypeSchema),
		},
		m: m,
	}

	for a, t := range m.AttributeTypes {
		sb.maybeAddAttribute(a, t)
	}
	sb.maybeAddAttribute(Type, reflectTypeType)
	sb.maybeAddAttribute(Self, emptyInterfaceType)

	// We want to know what all the variable types are.
	for t, fields := range m.TypeMappings {
		sb.maybeAddTypeMapping(t, fields)
	}
	return sb.Schema
}

type schemaBuilder struct {
	*Schema
	m Mappings
}

func (sb *schemaBuilder) maybeAddAttribute(a Attribute, typ reflect.Type) {
	// TODO(ajwerner): Validate that t is an okay type for an attribute
	// to be.
	prev, exists := sb.attributeTypes[a]
	if !exists {
		sb.attributeTypes[a] = typ
		sb.attributesByOrdinal[a.Ordinal()] = a
		return
	}
	if err := checkType(typ, prev); err != nil {
		panic(errors.Wrapf(err, "type mismatch for %v", a))
	}
}

func checkType(typ, exp reflect.Type) error {
	if typ == schemaTypePtrType && exp == reflectTypeType {
		return nil
	}
	switch exp.Kind() {
	case reflect.Interface:
		if !typ.Implements(exp) {
			return errors.Errorf("%v does not implement %v", typ, exp)
		}
	default:
		if typ != exp && !(exp.Kind() == reflect.Ptr && typ == exp.Elem()) {
			return errors.Errorf("%v is not %v", typ, exp)
		}
	}
	return nil
}

func (sb *schemaBuilder) getComparableTypeMapping(typ reflect.Type) reflect.Type {
	return getComparableType(typ)
}

func (sb *schemaBuilder) maybeAddTypeMapping(t reflect.Type, fields map[string]Attribute) {
	isStructPointer := func(tt reflect.Type) bool {
		return tt.Kind() == reflect.Ptr && tt.Elem().Kind() == reflect.Struct
	}

	// We mark the type as being added by putting A nil entry in the map.
	// This way, if we recurse into this closure, we'll detect the cycle.
	// TODO(ajwerner): Better cycle error reporting.
	{
		existing, ok := sb.entityTypeSchemas[t]
		if ok {
			if existing != nil {
				return
			}
			panic(errors.Errorf("cycle detected for type %v", t))
		}
		sb.entityTypeSchemas[t] = nil
	}

	if !isStructPointer(t) {
		panic(errors.Errorf("%v is not a pointer to a struct", t))
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
				panic(errors.Errorf("%v.%s is not a field", t, fieldName))
			}
			offset += sf.Offset
			cur = sf.Type
		}
		// TODO(ajwerner): Deal with making entities out of structs themselves.
		isPtr := cur.Kind() == reflect.Ptr
		isStructPtr := isPtr && cur.Elem().Kind() == reflect.Struct
		if isStructPtr {
			curFields, ok := sb.m.TypeMappings[cur]
			if !ok {
				sb.maybeAddTypeMapping(cur, curFields)
			}
		}
		isScalarPtr := isPtr && isSupportScalarKind(cur.Elem().Kind())
		if isScalarPtr {

		}
		sb.maybeAddAttribute(attr, cur)
		f := fieldInfo{
			path:     fieldName,
			attr:     attr,
			isEntity: isStructPtr,
			typ:      cur,
		}
		{
			vg := makeValueGetter(cur, offset)
			if isPtr {
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
			compType := sb.getComparableTypeMapping(cur)
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
	sb.entityTypeSchemas[t] = &entityTypeSchema{
		typ:              t,
		sc:               sb.Schema,
		fields:           fieldInfos,
		scalarAttrFields: scalarAttrFields,
	}
}
