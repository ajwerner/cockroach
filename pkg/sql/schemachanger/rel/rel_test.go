// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rel_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel/internal/entitynodetest"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel/internal/reltest"
	"github.com/stretchr/testify/require"
)

func TestRel(t *testing.T) {
	for _, s := range []reltest.Suite{
		entitynodetest.Suite,
	} {
		t.Run(s.Name, func(t *testing.T) {
			s.Run(t)
		})
	}
}

func TestContradiction(t *testing.T) {
	type A struct{}
	type B struct{}
	schema, err := rel.NewSchema("junk", rel.Mappings{
		TypeMappings: map[reflect.Type]map[string]rel.Attribute{
			reflect.TypeOf((*A)(nil)): {},
			reflect.TypeOf((*B)(nil)): {},
		},
	})
	var a, b, typ rel.Var = "a", "b", "typ"
	require.NoError(t, err)
	_, err = rel.NewQuery(schema,
		a.Type((*A)(nil)),
		b.Type((*B)(nil)),
		a.Attr(rel.Type, typ),
		b.Attr(rel.Type, typ),
	)
	require.Regexp(t, "failed to construct query: query contains contradiction on Type", err)
}

func TestInvalidData(t *testing.T) {
	type A struct{}
	type B struct{}
	schema := rel.MustSchema("junk", rel.Mappings{
		TypeMappings: map[reflect.Type]map[string]rel.Attribute{
			reflect.TypeOf((*A)(nil)): {},
			reflect.TypeOf((*B)(nil)): {},
		},
	})
	t.Run("attributes", func(t *testing.T) {
		for _, tc := range []struct {
			value interface{}
			errRE string
		}{
			{struct{}{}, `unknown type handler for struct \{\}`},
			{&struct{}{}, `unknown type handler for \*struct \{\}`},
			{(*A)(nil), `invalid nil \*rel_test.A value`},
		} {
			t.Run(fmt.Sprintf("%T(%v)", tc.value, tc.value), func(t *testing.T) {
				_, err := schema.GetAttribute(rel.Self, tc.value)
				require.Regexp(t, tc.errRE, err)
			})
		}
	})
	const invalidClausePrefix = `failed to construct query: failed to process invalid clause `
	t.Run("nil query values", func(t *testing.T) {
		_, err := rel.NewQuery(schema, rel.Var("a").Eq(rel.Value(nil)))
		require.Regexp(t, invalidClausePrefix+`\$a = null: invalid nil`, err)

		_, err = rel.NewQuery(schema, rel.Var("a").Eq(rel.Value((*A)(nil))))
		require.Regexp(t, invalidClausePrefix+`\$a = null: invalid nil`, err)
	})
	t.Run("nil entity for attributes", func(t *testing.T) {
		{
			_, err := schema.GetAttribute(rel.Self, nil)
			require.EqualError(t, err, "invalid nil value")
		}
		{
			_, err := schema.GetAttribute(rel.Self, (*A)(nil))
			require.EqualError(t, err, "invalid nil *rel_test.A value")
		}
		{
			require.EqualError(t, schema.IterateAttributes((*A)(nil), func(attribute rel.Attribute, value interface{}) error {
				return nil
			}), "invalid nil *rel_test.A value")
		}

	})
	t.Run("bad filters", func(t *testing.T) {
		_, err := rel.NewQuery(schema, rel.Filter("oneArg", "arg")(func() bool {
			panic("unimplemented")
		}))
		require.EqualError(
			t, err, invalidClausePrefix+
				`oneArg()($arg): invalid func() bool filter `+
				`function for variables [arg] accepts 0 inputs`,
		)

		_, err = rel.NewQuery(schema, rel.Filter("noArgs")(func(a bool) bool {
			panic("unimplemented")
		}))
		require.EqualError(
			t, err, invalidClausePrefix+
				`noArgs(bool)(): invalid func(bool) bool filter `+
				`function for variables [] accepts 1 inputs`,
		)

		_, err = rel.NewQuery(schema, rel.Filter("badReturn", "arg")(func(a bool) (bool, error) {
			panic("unimplemented")
		}))
		require.EqualError(
			t, err, invalidClausePrefix+
				`badReturn(bool)($arg): invalid non-bool return from `+
				`func(bool) (bool, error) filter function for variables [arg]`,
		)
	})
	t.Run("bad mappings", func(t *testing.T) {
		{
			type T struct{ C chan int }
			_, err := rel.NewSchema("junk", rel.Mappings{
				TypeMappings: map[reflect.Type]map[string]rel.Attribute{
					reflect.TypeOf((*T)(nil)): {
						"C": stringAttribute("c"),
					},
					reflect.TypeOf((*B)(nil)): {},
				},
			})
			require.EqualError(t, err,
				`failed to construct schema: selector "C" of *rel_test.T has unsupported type chan int`)
		}
	})
}

type stringAttribute string

func (sa stringAttribute) String() string { return string(sa) }
