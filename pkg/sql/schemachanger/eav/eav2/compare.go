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
	case reflect.Type:
		b := b.(reflect.Type)
		if a == b {
			return false, true
		}
		if a.PkgPath() == b.PkgPath() {
			return a.Name() < b.Name(), false
		}
		return a.PkgPath() < b.PkgPath(), false
	default:
		panic(errors.AssertionFailedf("incomparable types %T and %T", a, b))
	}
}

type Entity interface{}

// compareOn compares two elements on a given attribute.
// If the entities do not return the same type of value for the
// attribute, this function will panic. Note that it is fine if
// either or both do not contain this attribute. The lack of a
// value is considered the highest value; you can think of this
// library as sorting with NULLS LAST.
func (sc *Schema) compareOn(attr Attribute, a, b Entity) (less, eq bool) {
	av := sc.getComparableValue(attr, a)
	bv := sc.getComparableValue(attr, b)
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
func Compare(s *Schema, a, b Entity) (less, eq bool) {
	OrdinalSet.Union(
		s.GetAttributes(a), s.GetAttributes(b),
	).ForEach(s, func(attr Attribute) (wantMore bool) {
		less, eq = s.compareOn(attr, a, b)
		return eq
	})
	return less, eq
}

// Equal returns true if the two elements have identical attributes.
func Equal(s *Schema, a, b Entity) bool {
	_, eq := Compare(s, a, b)
	return eq
}
