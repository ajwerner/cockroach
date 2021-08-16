// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eav

import "sync"

// values is a container for data.
//
// It stores the data in a format which is convenient for performing
// comparisons and lookups. If you want strongly typed data out of it,
// you need to use A Schema to retrieve that data.
type values struct {
	attrs ordinalSet
	m     map[Ordinal]interface{}
}

var valuesSyncPool = sync.Pool{
	New: func() interface{} {
		return &values{
			m: make(map[Ordinal]interface{}),
		}
	},
}

func getValues() *values {
	return valuesSyncPool.Get().(*values)
}

func putValues(v *values) {
	for k := range v.m {
		delete(v.m, k)
	}
	v.attrs = 0
	valuesSyncPool.Put(v)
}

// get retrieves the primitive values stores in the values
// struct.
func (v values) get(a Attribute) interface{} {
	return v.m[a.Ordinal()]
}

func (vv *values) copyFrom(values values) {
	for ord, v := range values.m {
		if ord < maxUserAttribute {
			vv.add(ord, v)
		}
	}
}

func (vv *values) add(ord Ordinal, v interface{}) {
	vv.attrs = vv.attrs.Add(ord)
	vv.m[ord] = v
}
