// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	name               string
	attributes         []Attribute
	attributeTypes     []reflect.Type
	attributeToOrdinal map[Attribute]ordinal
	entityTypeSchemas  map[reflect.Type]*entityTypeSchema
}

// NewSchema constructs a new schema from mappings.
// The name parameter is just used for debugging and error messages.
func NewSchema(name string, m Mappings) (_ *Schema, err error) {
	defer func() {
		switch r := recover().(type) {
		case nil:
			return
		case error:
			err = errors.Wrap(r, "failed to construct schema")
		default:
			err = errors.AssertionFailedf("failed to construct schema: %v", r)
		}
	}()
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
	attrFields map[ordinal][]fieldInfo
}

type fieldInfo struct {
	path            string
	typ             reflect.Type
	attr            ordinal
	comparableValue func(uintptr) interface{}
	value           func(uintptr) interface{}
	isPtr, isEntity bool
}

func buildSchema(name string, m Mappings) *Schema {
	sb := &schemaBuilder{
		Schema: &Schema{
			name:               name,
			attributeToOrdinal: make(map[Attribute]ordinal),
			entityTypeSchemas:  make(map[reflect.Type]*entityTypeSchema),
		},
		m: m,
	}

	sb.maybeAddAttribute(Self, emptyInterfaceType)
	sb.maybeAddAttribute(Type, reflectTypeType)
	for a, t := range m.AttributeTypes {
		sb.maybeAddAttribute(a, t)
	}

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

func (sb *schemaBuilder) maybeAddAttribute(a Attribute, typ reflect.Type) ordinal {
	// TODO(ajwerner): Validate that t is an okay type for an attribute
	// to be.
	ord, exists := sb.attributeToOrdinal[a]
	if !exists {
		ord = ordinal(len(sb.attributes))
		if ord >= maxUserAttribute {
			panic(errors.Errorf("too many attributes"))
		}
		sb.attributes = append(sb.attributes, a)
		sb.attributeTypes = append(sb.attributeTypes, typ)
		sb.attributeToOrdinal[a] = ord
		return ord
	}
	prev := sb.attributeTypes[ord]
	if err := checkType(typ, prev); err != nil {
		panic(errors.Wrapf(err, "type mismatch for %v", a))
	}
	return ord
}

// checkType determines whether, either, the
func checkType(typ, exp reflect.Type) error {
	switch exp.Kind() {
	case reflect.Interface:
		if !typ.Implements(exp) {
			return errors.Errorf("%v does not implement %v", typ, exp)
		}
	default:
		if typ != exp && !(typ.Kind() == reflect.Ptr && typ.Elem() == exp) {
			return errors.Errorf("%v is not %v", typ, exp)
		}
	}
	return nil
}

func (sb *schemaBuilder) maybeAddTypeMapping(t reflect.Type, fields map[string]Attribute) {
	isStructPointer := func(tt reflect.Type) bool {
		return tt.Kind() == reflect.Ptr && tt.Elem().Kind() == reflect.Struct
	}

	// We mark the type as being added by putting a nil entry in the map.
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
		offset, cur := getOffsetAndTypeFromSelector(t, fieldName)

		// TODO(ajwerner): Deal with making entities out of structs themselves.
		// This gets complicated given the pointer equality used to determine
		// whether entities exist. We'd otherwise need some mechanism for interning
		// structs or something like that.
		isPtr := cur.Kind() == reflect.Ptr
		isStructPtr := isPtr && cur.Elem().Kind() == reflect.Struct
		isScalarPtr := isPtr && isSupportScalarKind(cur.Elem().Kind())
		if !isScalarPtr && !isStructPtr && !isSupportScalarKind(cur.Kind()) {
			panic(errors.Errorf(
				"selector %q of %v has unsupported type %v",
				fieldName, t, cur,
			))
		}

		typ := cur
		if isScalarPtr {
			typ = cur.Elem()
		}
		ord := sb.maybeAddAttribute(attr, typ)

		if isStructPtr {
			_, ok := sb.m.TypeMappings[cur]
			if !ok {
				// This will teach the schema about a type with no declared fields.
				// In the case of recursion, this
				sb.maybeAddTypeMapping(cur, nil)
			}
		}

		f := fieldInfo{
			path:     fieldName,
			attr:     ord,
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
			if isStructPtr {
				f.value = getPtrValue(vg)
			} else {
				if isScalarPtr {
					f.value = func(u uintptr) interface{} {
						got := vg(u)
						if got.Elem().IsNil() {
							return nil
						}
						return got.Elem().Elem().Interface()
					}
				} else {
					f.value = func(u uintptr) interface{} {
						return vg(u).Elem().Interface()
					}
				}
			}
		}
		{
			if isStructPtr {
				f.comparableValue = getPtrValue(makeValueGetter(cur, offset))
			} else {
				compType := getComparableType(typ)
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
		return fieldInfos[i].attr < fieldInfos[j].attr
	})
	attributeFields := make(map[ordinal][]fieldInfo)

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

// getOffsetAndTypeForSelector takes an entity (struct pointer) type and a
// selector string and finds its offset within the struct. Note that this
// allows one to select fields in struct members of the current struct but
// not in referenced structs.
func getOffsetAndTypeFromSelector(
	structPointer reflect.Type, selector string,
) (uintptr, reflect.Type) {
	names := strings.Split(selector, ".")
	var offset uintptr
	cur := structPointer.Elem()
	for _, n := range names {
		sf, ok := cur.FieldByName(n)
		if !ok {
			panic(errors.Errorf("%v.%s is not a field", structPointer, selector))
		}
		offset += sf.Offset
		cur = sf.Type
	}
	return offset, cur
}

func (sc *Schema) getOrd(attribute Attribute) ordinal {
	ord, ok := sc.attributeToOrdinal[attribute]
	if !ok {
		panic(errors.Errorf("unknown attribute %s in schema %s", attribute, sc.name))
	}
	return ord
}
