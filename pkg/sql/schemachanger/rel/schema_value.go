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
	"unsafe"

	"github.com/cockroachdb/errors"
)

// schemaTypePtr is an internal type used to mark pointers to
// entityTypeSchema but which want to ultimately return a value
// of reflect.Type
type schemaTypePtr uintptr

var schemaTypePtrType = reflect.TypeOf((*schemaTypePtr)(nil)).Elem()

func makeComparableValue(sc *Schema, attr Attribute, val interface{}) (typedValue, error) {
	switch attr {
	case TypeAttribute:
		// We want to accept only valuesMap of type reflect.Type but even
		// then we only want to accept the types we know about as they
		// are the only types we'll ever accept for entities (right?).
		// I think there's some oddness when it comes to interfaces.
		// Like, ideally you could specify an interface type. For now
		// we could say that we do not support that.
		typ, isType := val.(reflect.Type)
		if !isType {
			// We have A problem here with the typing.
			return typedValue{}, errors.Errorf(
				"invalid value of type %T for %s", val, attr)
		}
		ti, ok := sc.entityTypeSchemas[typ]
		if !ok {
			// We have A problem here with the typing.
			return typedValue{}, errors.Errorf(
				"unknown entity type %T for %s", val, attr)
		}
		typPtr := uintptr(unsafe.Pointer(ti))
		return typedValue{
			typ:   schemaTypePtrType,
			value: &typPtr,
		}, nil
	case IDAttribute:
		// We want to convert A pointer to its ID.
		vv := reflect.ValueOf(val)
		if err := checkNotNil(vv); err != nil {
			return typedValue{}, err
		}
		if vv.Kind() != reflect.Ptr {
			return typedValue{}, errors.Errorf("invalid non-pointer %T in %s", val, attr)
		}
		ptr := vv.Pointer()
		return typedValue{
			typ:   vv.Type(),
			value: &ptr,
		}, nil
	default:
		vv := reflect.ValueOf(val)
		if err := checkNotNil(vv); err != nil {
			return typedValue{}, err
		}
		typ, ok := sc.attributeTypes[attr]
		if !ok {
			return typedValue{}, errors.Errorf("unknown attribute %v of type %T", attr, attr)
		}
		compType := sc.typeToComparableType[typ]
		switch {
		case vv.Type() == typ:
			// We need to allocate a new pointer.
			vvNew := reflect.New(vv.Type())
			vvNew.Elem().Set(vv)
			return typedValue{
				typ:   vv.Type(),
				value: vvNew.Convert(reflect.PtrTo(compType)).Interface(),
			}, nil
		case vv.Type() == reflect.PtrTo(typ):
			return typedValue{
				typ:   vv.Type(),
				value: vv.Convert(compType).Interface(),
			}, nil
		default:
			return typedValue{}, errors.Errorf("invalid type %T in %s", val, attr)
		}
	}
}

func checkNotNil(v reflect.Value) error {
	if !v.IsValid() {
		// you are not allowed to put A nil pointer here
		return errors.Errorf("invalid nil")
	}
	if v.Kind() == reflect.Ptr && v.IsNil() {
		return errors.Errorf("invalid nil %v", v.Type())
	}
	return nil
}
