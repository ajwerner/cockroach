// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package entitynodetest

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel/internal/reltest"
	"gopkg.in/yaml.v3"
)

type entity struct {
	I8       int8
	PI8      *int8
	I16      int16
	PI16     *int16
	I32      int32
	PI32     *int32
	I64      int64
	PI64     *int64
	UI8      uint8
	PUI8     *uint8
	UI16     uint16
	PUI16    *uint16
	UI32     uint32
	PUI32    *uint32
	UI64     uint64
	PUI64    *uint64
	Str      string
	PStr     *string
	Uintptr  uintptr
	PUintptr *uintptr
}

type node struct {
	Value       *entity
	Left, Right *node
}

func (n *node) EncodeToYAML(t *testing.T, r *reltest.DataRegistry) interface{} {
	yn := yaml.Node{Kind: yaml.MappingNode, Style: yaml.FlowStyle}
	for _, f := range []struct {
		name  string
		field interface{}
		ok    bool
	}{
		{"value", n.Value, n.Value != nil},
		{"left", n.Left, n.Left != nil},
		{"right", n.Right, n.Right != nil},
	} {
		if !f.ok {
			continue
		}
		yn.Content = append(yn.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: f.name},
			&yaml.Node{Kind: yaml.ScalarNode, Value: r.MustGetName(t, f.field)},
		)
	}
	return &yn
}

var _ reltest.RegistryYAMLEncoder = (*node)(nil)

// testAttr is a rel.Attribute used for testing.
type testAttr int8

var _ rel.Attribute = testAttr(0)

//go:generate stringer --type TestAttr  --tags test
const (
	i8 testAttr = iota
	pi8
	i16
	pi16
	i32
	pi32
	i64
	pi64
	ui8
	pui8
	ui16
	pui16
	ui32
	pui32
	ui64
	pui64
	str
	pstr
	_uintptr
	puintptr
	value
	left
	right
)

var schema = rel.MustSchema("testschema", rel.Mappings{
	TypeMappings: map[reflect.Type]map[string]rel.Attribute{
		reflect.TypeOf((*entity)(nil)): {
			"I8":       i8,
			"PI8":      pi8,
			"I16":      i16,
			"PI16":     pi16,
			"I32":      i32,
			"PI32":     pi32,
			"I64":      i64,
			"PI64":     pi64,
			"UI8":      ui8,
			"PUI8":     pui8,
			"UI16":     ui16,
			"PUI16":    pui16,
			"UI32":     ui32,
			"PUI32":    pui32,
			"UI64":     ui64,
			"PUI64":    pui64,
			"Str":      str,
			"PStr":     pstr,
			"Uintptr":  _uintptr,
			"PUintptr": puintptr,
		},
		reflect.TypeOf((*node)(nil)): {
			"Value": value,
			"Left":  left,
			"Right": right,
		},
	},
})
