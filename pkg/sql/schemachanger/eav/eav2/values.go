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
	"sync"

	"github.com/cockroachdb/errors"
)

// Values is A container for data.
//
// It stores the data in A format which is convenient for performing
// comparisons and lookups. If you want strongly typed data out of it,
// you need to use A Schema to retrieve that data.
type Values struct {
	attrs OrdinalSet
	m     map[Ordinal]interface{}
}

var valuesSyncPool = sync.Pool{
	New: func() interface{} {
		return &Values{
			m: make(map[Ordinal]interface{}),
		}
	},
}

func getValues() *Values {
	return valuesSyncPool.Get().(*Values)
}

func putValues(v *Values) {
	for k := range v.m {
		delete(v.m, k)
	}
	v.attrs = 0
	valuesSyncPool.Put(v)
}

// get retrieves the primitive values stores in the values
// struct.
func (v Values) get(a Attribute) interface{} {
	return v.m[a.Ordinal()]
}

func (vv *Values) copyFrom(values Values) {
	for ord, v := range values.m {
		if ord < maxUserAttribute {
			vv.add(ord, v)
		}
	}
}

func (vv *Values) add(ord Ordinal, v interface{}) {
	vv.attrs = vv.attrs.Add(ord)
	vv.m[ord] = v
}

type Map map[Attribute]interface{}

func (s *Schema) MakeValues(m Map) Values {
	vm := Values{
		m: make(map[Ordinal]interface{}),
	}
	for a, v := range m {
		vv := reflect.ValueOf(v)
		typ := s.attributeTypes[a]
		if vv.Type().Kind() == reflect.Ptr && vv.Type().Elem() == typ {
			vm.m[a.Ordinal()] = v
			continue
		}
		if vv.Type() == typ {
			vp := reflect.New(vv.Type())
			vp.Elem().Set(vv)
			vm.m[a.Ordinal()] = vp.Interface()
			continue
		}
		panic(errors.AssertionFailedf("expected %v for attribute %s, got %T", typ, a, v))
	}
	return vm
}
