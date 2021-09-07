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

var (
	schemaTypePtrType  = reflect.TypeOf((*schemaTypePtr)(nil)).Elem()
	reflectTypeType    = reflect.TypeOf((*reflect.Type)(nil)).Elem()
	emptyInterfaceType = reflect.TypeOf((*interface{})(nil)).Elem()
)

func makeComparableValue(sc *Schema, val interface{}) (typedValue, error) {
	// We want to accept only valuesMap of type reflect.Type but even
	// then we only want to accept the types we know about as they
	// are the only types we'll ever accept for entities (right?).
	// I think there's some oddness when it comes to interfaces.
	// Like, ideally you could specify an interface type. For now
	// we could say that we do not support that.
	if typ, isType := val.(reflect.Type); isType {
		ti, ok := sc.entityTypeSchemas[typ]
		if !ok {
			// We have A problem here with the typing.
			return typedValue{}, errors.Errorf(
				"unknown variable type %T", val)
		}
		typPtr := uintptr(unsafe.Pointer(ti))
		return typedValue{
			typ:   schemaTypePtrType,
			value: &typPtr,
		}, nil
	}
	vv := reflect.ValueOf(val)
	if err := checkNotNil(vv); err != nil {
		return typedValue{}, err
	}
	typ := vv.Type()
	switch {
	case isSupportScalarKind(typ.Kind()):
		// We need to allocate a new pointer.
		compType := getComparableType(typ)
		vvNew := reflect.New(vv.Type())
		vvNew.Elem().Set(vv)
		return typedValue{
			typ:   vv.Type(),
			value: vvNew.Convert(reflect.PtrTo(compType)).Interface(),
		}, nil
	case typ.Kind() == reflect.Ptr:
		switch {
		case isSupportScalarKind(typ.Elem().Kind()):
			compType := getComparableType(typ.Elem())
			return typedValue{
				typ:   vv.Type(),
				value: vv.Convert(reflect.PtrTo(compType)).Interface(),
			}, nil
		case typ.Elem().Kind() == reflect.Struct:
			ptr := vv.Pointer()
			return typedValue{
				typ:   vv.Type(),
				value: &ptr,
			}, nil
		default:
			return typedValue{}, errors.Errorf(
				"unsupported scalar kind %v for type %T", typ.Elem().Kind(), val,
			)
		}
	default:
		return typedValue{}, errors.Errorf(
			"unsupported scalar kind %v for type %T", typ.Kind(), val,
		)
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
