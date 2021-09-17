// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testproto

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
)

// testAttr is a rel.Attribute used for testing.
type testAttr int8

var _ rel.Attribute = testAttr(0)

//go:generate stringer --type testAttr  --tags test
const (
	m testAttr = iota
	m1
	m2
	c
	name
)

// This schema exercises cyclic references.
var schema = rel.MustSchema("testschema", rel.Mappings{
	TypeMappings: map[reflect.Type]map[string]rel.Attribute{
		reflect.TypeOf((*M1)(nil)): {
			"C":    c,
			"M1":   m1,
			"M2":   m2,
			"Name": name,
		},
		reflect.TypeOf((*M2)(nil)): {
			"C":    c,
			"M1":   m1,
			"M2":   m2,
			"Name": name,
		},
		reflect.TypeOf((*Container)(nil)): {
			"M1": m,
			"M2": m,
		},
	},
})
