package scpb

import (
	"reflect"
	"testing"
)

func TestGetElement(t *testing.T) {
	typ := reflect.TypeOf((*Element)(nil)).Elem()
	elementInterfaceType := reflect.TypeOf((*Element)(nil)).Elem()
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if !f.Type.Implements(elementInterfaceType) {
			t.Errorf("%v does not implement %v", f.Type, elementInterfaceType)
		}
	}
}
