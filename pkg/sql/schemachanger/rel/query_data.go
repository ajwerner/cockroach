package rel

import (
	"reflect"
	"unsafe"
)

type slotIdx int

type fact struct {
	variable slotIdx
	attr     Attribute
	value    slotIdx
}

// slot represents an potentially unbound value referenced in a query.
type slot struct {
	typedValue

	// any holds the acceptable valuesMap which may occupy this slot as
	// indicated from an Any value.
	//
	// TODO(ajwerner): Consider boxing this inside of typedValue.
	any []typedValue
}

// typedValue is a value in its comparable form, which is to say, it is a
// pointer to a primitive type. The type is the type from which the value
// was derived and which should be presented to the user.
type typedValue struct {
	typ   reflect.Type
	value interface{}
}

func (tv typedValue) toInterface() interface{} {
	if tv.typ == schemaTypePtrType {
		return (*entityTypeSchema)(unsafe.Pointer(*tv.value.(*uintptr))).typ
	}
	if tv.typ.Kind() == reflect.Ptr {
		if tv.typ.Elem().Kind() == reflect.Struct {
			return reflect.NewAt(tv.typ.Elem(), unsafe.Pointer(*tv.value.(*uintptr))).Interface()
		}
		return reflect.ValueOf(tv.value).Convert(tv.typ).Interface()
	}
	return reflect.ValueOf(tv.value).Convert(reflect.PtrTo(tv.typ)).Elem().Interface()
}

func (s *slot) eq(other slot) bool {
	// TODO(ajwerner): Deal with types.
	switch {
	case s.value == nil && other.value == nil:
		return true
	case s.value == nil:
		return false
	case other.value == nil:
		return false
	default:
		_, eq := compare(s.value, other.value)
		return eq
	}
}

func (s *slot) empty() bool {
	return s.value == nil
}

func (s *slot) set(tv typedValue) {
	s.typedValue = tv
}

func (s *slot) shouldSet(val interface{}) (unset, foundContradiction bool) {
	if !s.empty() {
		if _, eq := compare(s.value, val); !eq {
			return false, true
		}
		return false, false
	}

	if s.any == nil {
		return true, false
	}

	var foundMatch bool
	for _, v := range s.any {
		if _, foundMatch = compare(v.value, val); foundMatch {
			break
		}
	}
	if !foundMatch {
		return false, true // contradiction
	}
	return true, false
}
