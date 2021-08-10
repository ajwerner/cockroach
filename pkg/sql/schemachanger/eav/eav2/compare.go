// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eav2

import (
	"reflect"
	"unsafe"

	"github.com/cockroachdb/errors"
)

// compare assumes that a and b are comparable and of the same type.
func compare(a, b interface{}) (less, eq bool) {
	// I want generics.
	switch a := a.(type) {
	case *int:
		b := b.(*int)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case *int64:
		b := b.(*int64)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case *int32:
		b := b.(*int32)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case *int16:
		b := b.(*int16)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case *int8:
		b := b.(*int8)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case *uint:
		b := b.(*uint)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case *uint64:
		b := b.(*uint64)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case *uint32:
		b := b.(*uint32)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case *uint16:
		b := b.(*uint16)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case *uint8:
		b := b.(*uint8)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	case *string:
		b := b.(*string)
		if *a < *b {
			return true, false
		}
		return false, *a == *b
	default:
		panic(errors.AssertionFailedf("incomparable types %T and %T", a, b))
	}
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

type Entity struct {
	ptr uintptr // interface{}
	typ uintptr // *entityTypeSchema
	Values
}

func (e *Entity) Interface() interface{} {
	ti := e.getTypeInfo()
	return reflect.NewAt(ti.typ.Elem(), unsafe.Pointer(e.ptr)).Interface()
}

func (e *Entity) getTypeInfo() *entityTypeSchema {
	return (*entityTypeSchema)(unsafe.Pointer(e.typ))
}

// compareOn compares two elements on a given attribute.
// If the entities do not return the same type of value for the
// attribute, this function will panic. Note that it is fine if
// either or both do not contain this attribute. The lack of a
// value is considered the highest value; you can think of this
// library as sorting with NULLS LAST.
func compareOn(attr Attribute, a, b Values) (less, eq bool) {
	av := a.get(attr)
	bv := b.get(attr)
	switch {
	case av == nil && bv == nil:
		return false, true
	case av == nil:
		return false, false
	case bv == nil:
		return true, false
	default:
		return compare(av, bv)
	}
}

// Compare compares two elements by their attributes.
func compareEntities(s *Schema, a, b Entity) (less, eq bool) {
	if a.ptr == b.ptr {
		return false, true
	}
	OrdinalSet.Union(
		a.attrs, b.attrs,
	).ForEach(s, func(attr Attribute) (wantMore bool) {
		less, eq = compareOn(attr, a.Values, b.Values)
		return eq
	})
	return less, eq
}

// Equal returns true if the two elements have identical attributes.
func Equal(s *Schema, a, b Entity) bool {
	_, eq := compareEntities(s, a, b)
	return eq
}
