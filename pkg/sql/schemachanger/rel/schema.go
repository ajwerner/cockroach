package rel

import (
	"reflect"
	"sort"
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

	// TODO(ajwerner): consider add sorting preferences or primary keys.
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
	typ        reflect.Type
	fields     []fieldInfo
	attrFields map[Attribute][]fieldInfo
}

type fieldInfo struct {
	path            string
	typ             reflect.Type
	attr            Attribute
	comparableValue func(uintptr) interface{}
	value           func(uintptr) interface{}
	isPtr, isEntity bool
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
		typ := cur
		if isScalarPtr {
			typ = cur.Elem()
		}
		sb.maybeAddAttribute(attr, typ)
		f := fieldInfo{
			path:     fieldName,
			attr:     attr,
			isEntity: isStructPtr,
			isPtr:    isPtr,
			typ:      typ,
		}
		getPtrValue := func(vg func(uintptr) reflect.Value) func(u uintptr) interface{} {
			return func(u uintptr) interface{} {
				got := vg(u)
				if got.Elem().IsNil() {
					return nil
				}
				return got.Elem().Interface()
			}
		}
		{
			vg := makeValueGetter(cur, offset)
			if isPtr {
				f.value = getPtrValue(vg)
			} else {
				f.value = func(u uintptr) interface{} {
					return vg(u).Interface()
				}
			}
		}
		{
			if isStructPtr {
				f.comparableValue = getPtrValue(makeValueGetter(cur, offset))
			} else {
				compType := sb.getComparableTypeMapping(typ)
				if isScalarPtr {
					compType = reflect.PtrTo(compType)
				}
				vg := makeValueGetter(compType, offset)
				if isScalarPtr {
					f.comparableValue = getPtrValue(vg)
				} else {
					f.comparableValue = func(u uintptr) interface{} {
						return vg(u).Interface()
					}
				}
			}
		}
		fieldInfos = append(fieldInfos, f)
	}
	sort.Slice(fieldInfos, func(i, j int) bool {
		return fieldInfos[i].attr.Ordinal() < fieldInfos[j].attr.Ordinal()
	})
	attributeFields := make(map[Attribute][]fieldInfo)

	for i := 0; i < len(fieldInfos); {
		cur := fieldInfos[i].attr
		j := i + 1
		for ; j < len(fieldInfos); j++ {
			if fieldInfos[j].attr != cur {
				break
			}
		}
		attributeFields[cur] = fieldInfos[i:j]
		i = j
	}
	sb.entityTypeSchemas[t] = &entityTypeSchema{
		typ:        t,
		fields:     fieldInfos,
		attrFields: attributeFields,
	}
}

func (sc *Schema) GetAttribute(attribute Attribute, v interface{}) (interface{}, error) {
	ti, value, err := getEntityValueInfo(sc, v)
	if err != nil {
		return nil, err
	}

	fi, ok := ti.attrFields[attribute]
	if !ok {
		return nil, errors.Errorf("no scalar field defined on %v for %v", ti.typ, attribute)
	}
	for i := range fi {
		got := fi[i].value(value.Pointer())
		if got != nil {
			return got, nil
		}
	}
	return nil, nil
}
