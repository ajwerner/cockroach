package entitynodetest

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel/internal/reltest"
)

type v = rel.Var

var (
	Suite = reltest.Suite{
		Name:           "entitynode",
		Schema:         Schema,
		Registry:       r,
		QueryTests:     queryCases,
		AttributeTests: attributeCases,
	}

	r  = reltest.NewDataRegistry()
	a  = r.FromYAML("a", `{i16: 1, i8: 1, pi8: 1}`, &Entity{}).(*Entity)
	b  = r.FromYAML("b", `{i16: 2, i8: 2}`, &Entity{}).(*Entity)
	c  = r.FromYAML("c", `{i16: 1, i8: 2}`, &Entity{}).(*Entity)
	na = r.Register("na", &Node{E: a}).(*Node)
	nb = r.Register("nb", &Node{E: b, L: na}).(*Node)
	nc = r.Register("nc", &Node{E: c, R: nb}).(*Node)

	queryCases = []reltest.DatabaseTest{
		{
			Name: "foo",
			Data: []string{"a", "b", "c", "na", "nb", "nc"},
			Indexes: [][][]rel.Attribute{
				{},
				{{E}, {PI8}, {rel.Type}},
			},
			QueryCases: []reltest.QueryTest{
				{
					Name: "a fields",
					Query: rel.MustQuery(
						Schema,
						v("a").Attr(I16, rel.Value(int16(1))),
						v("a").Attr(I8, v("ai8")),
						v("a").Attr(PI8, v("api8")),
					),
					ResVars: []v{"a", "ai8", "api8"},
					Results: [][]interface{}{
						{a, int8(1), int8(1)},
					},
				},
				{
					Name: "a-c-b join",
					Query: rel.MustQuery(
						Schema,
						v("a").Attr(I8, rel.Value(int8(1))),
						v("b").Attr(I16, rel.Value(int16(2))),
						v("b").Attr(I8, rel.Value(int8(2))),
						v("c").Attr(I16, rel.Value(int16(1))),
						v("c").Attr(I8, rel.Value(int8(2))),
					),
					ResVars: []v{"a", "b", "c"},
					Results: [][]interface{}{
						{a, b, c},
					},
				},
				{
					Name: "nil values don't show up",
					Query: rel.MustQuery(
						Schema,
						v("e").Attr(PI8, rel.Value(int8(1))),
					),
					ResVars: []v{"e"},
					Results: [][]interface{}{
						{a},
					},
				},
				{
					Name: "list all the values",
					Query: rel.MustQuery(
						Schema,
						v("e").Attr(I8, v("i8")),
					),
					ResVars: []v{"e", "i8"},
					Results: [][]interface{}{
						{a, int8(1)},
						{b, int8(2)},
						{c, int8(2)},
					},
				},
				{
					Name: "nodes with elements where i8=2",
					Query: rel.MustQuery(
						Schema,
						v("i8").Eq(rel.Value(int8(2))),
						v("i8").Entities(I8, "e"), // using this notation just to exercise it
						v("n").Attr(E, v("e")),
					),
					ResVars: []v{"n", "e"},
					Results: [][]interface{}{
						{nb, b},
						{nc, c},
					},
				},
				{
					Name: "list all the i8 values",
					Query: rel.MustQuery(
						Schema,
						v("e").Attr(I8, v("i8")),
					),
					ResVars: []v{"i8"},
					// Note that you get the value for all the entities
					// which can offer it. That's maybe surprising.
					Results: [][]interface{}{
						{int8(1)},
						{int8(2)},
						{int8(2)},
					},
				},
				{
					Name: "types of all the entities",
					Query: rel.MustQuery(
						Schema,
						v("e").Attr(rel.Type, v("typ")),
					),
					ResVars: []v{"e", "typ"},
					Results: [][]interface{}{
						{a, reflect.TypeOf((*Entity)(nil))},
						{b, reflect.TypeOf((*Entity)(nil))},
						{c, reflect.TypeOf((*Entity)(nil))},
						{na, reflect.TypeOf((*Node)(nil))},
						{nb, reflect.TypeOf((*Node)(nil))},
						{nc, reflect.TypeOf((*Node)(nil))},
					},
				},
				{
					Name: "nodes by type",
					Query: rel.MustQuery(
						Schema,
						v("na").Type((*Node)(nil)),
						v("na").Attr(E, v("a")),
						v("nb").Attr(L, v("na")),
						v("nc").Attr(R, v("nb")),
					),
					ResVars: []v{"na", "nb", "nc", "a"},
					Results: [][]interface{}{
						{na, nb, nc, a},
					},
				},
				{
					Name: "nodes by type",
					Query: rel.MustQuery(
						Schema,
						v("n").Type((*Node)(nil)),
					),
					ResVars: []v{"n"},
					Results: [][]interface{}{
						{na},
						{nb},
						{nc},
					},
				},
				{
					Name: "basic any",
					Query: rel.MustQuery(
						Schema,
						v("entity").Type((*Node)(nil), (*Entity)(nil)),
					),
					ResVars: []v{"entity"},
					Results: [][]interface{}{
						{a},
						{b},
						{c},
						{na},
						{nb},
						{nc},
					},
				},
			},
		},
	}
	attributeCases = []reltest.AttributeTestCase{
		{
			"a",
			addToEmptyEntityMap(map[rel.Attribute]interface{}{
				PI8: int8(1),
				I8:  int8(1),
				I16: int16(1),
			}),
		},
		{
			"b",
			addToEmptyEntityMap(map[rel.Attribute]interface{}{
				I8:  int8(2),
				I16: int16(2),
			}),
		},
		{
			"c",
			addToEmptyEntityMap(map[rel.Attribute]interface{}{
				I8:  int8(2),
				I16: int16(1),
			}),
		},
		{
			"na",
			map[rel.Attribute]interface{}{
				E: a,
			},
		},
		{
			"nb",
			map[rel.Attribute]interface{}{
				E: b,
				L: na,
			},
		},
		{
			"nc",
			map[rel.Attribute]interface{}{
				E: c,
				R: nb,
			},
		},
	}
)

func addToEmptyEntityMap(m map[rel.Attribute]interface{}) map[rel.Attribute]interface{} {
	base := map[rel.Attribute]interface{}{
		I8:      int8(0),
		I16:     int16(0),
		I32:     int32(0),
		I64:     int64(0),
		UI8:     uint8(0),
		UI16:    uint16(0),
		UI32:    uint32(0),
		UI64:    uint64(0),
		String:  "",
		Uintptr: uintptr(0),
	}
	for k, v := range m {
		base[k] = v
	}
	return base
}
