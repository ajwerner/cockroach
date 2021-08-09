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

import "sync"

// Values is used to store a select elements from a Tree.
type Values struct {
	sc *Schema
	m  map[Ordinal]interface{}
}

// GetValues retrieves a Values instance from the sync pool. Use
// Release to put it back in the pool. Values are often used in
// contexts with well-defined lifecycles, hence the pooling.
func GetValues(sc *Schema) Values {
	m := valuesSyncPool.Get().(map[Ordinal]interface{})
	return Values{
		sc: sc,
		m:  m,
	}
}

// Copy clones the Values into a newly allocated map.
func (vv *Values) Copy() Values {
	cpy := GetValues(vv.sc)
	for k, v := range vv.m {
		cpy.m[k] = v
	}
	return cpy
}

// Release releases the Values back into the pool.
func (v *Values) Release() {
	for k := range v.m {
		delete(v.m, k)
	}
	valuesSyncPool.Put(v.m)
	*v = Values{}
}

// Attributes returns the set of attributes defined on this Values.
func (vv Values) Attributes() OrdinalSet {
	var ret OrdinalSet
	for o := range vv.m {
		ret = ret.Add(o)
	}
	return ret
}

// Set sets the given attribute value. Note that v may be nil and it will
// still set mark this attribute as being set.
func (vv Values) Set(attr Attribute, v interface{}) {
	// TODO(ajwerner): Type checking.
	if v != nil {
		vv.m[attr.Ordinal()] = v
	}
}

// Get retrieves the given attribute value.
func (vv Values) Get(a Attribute) interface{} {
	return vv.m[a.Ordinal()]
}

var valuesSyncPool = sync.Pool{
	New: func() interface{} {
		return make(map[Ordinal]interface{})
	},
}
