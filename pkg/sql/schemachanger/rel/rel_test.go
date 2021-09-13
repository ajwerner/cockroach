package rel_test

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel/internal/testschema"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type queryCase struct {
	name    string
	q       *rel.Query
	resVars []rel.Var
	results [][]interface{}
}
type queryTestCase struct {
	name    string
	data    []interface{}
	queries []queryCase
}

func (qc queryCase) runQueryCase(t *testing.T, db *rel.Database) {
	var results [][]interface{}
	require.NoError(t, qc.q.Prepare().Iterate(db, func(r rel.Result) error {
		var cur []interface{}
		for _, v := range qc.resVars {
			cur = append(cur, r.Var(v))
		}
		results = append(results, cur)
		return nil
	}))
	expResults := append(qc.results[:0:0], qc.results...)
	findResultInExp := func(res []interface{}) (found bool) {
		for i, exp := range expResults {
			if reflect.DeepEqual(exp, res) {
				expResults = append(expResults[:i], expResults[i+1:]...)
				return true
			}
		}
		return false
	}

	for _, res := range results {
		if !findResultInExp(res) {
			t.Fatalf("failed to find %v in %v", res, expResults)
		}
	}
	require.Empty(t, expResults, "got", results)
}

func (tc queryTestCase) runTestCase(t *testing.T) {
	db := rel.NewDatabase(Schema, nil)
	for _, v := range tc.data {
		require.NoError(t, db.Insert(v))
	}
	for _, qc := range tc.queries {
		t.Run(qc.name, func(t *testing.T) {
			qc.runQueryCase(t, db)
		})
	}
}

type v = rel.Var

type registry map[string]interface{}

func (r registry) register(name string, v interface{}) interface{} {
	if existing, exists := r[name]; exists {
		panic(errors.AssertionFailedf(
			"entity with name %s already registered %v, trying to register %v",
			name, existing, v,
		))
	}
	r[name] = v
	return v
}

func (r registry) fromYAML(name, yamlData string, dest interface{}) interface{} {
	if err := yaml.Unmarshal([]byte(yamlData), dest); err != nil {
		panic(err)
	}
	r.register(name, dest)
	return dest
}

type attrTestCase struct {
	name   string
	entity interface{}
	m      map[rel.Attribute]interface{}
}

func (c attrTestCase) run(t *testing.T, r registry) {
	got := make(map[rel.Attribute]interface{})
	require.NoError(t, Schema.IterateAttributes(r[c.name], func(
		attribute rel.Attribute, value interface{},
	) error {
		got[attribute] = value
		return nil
	}))
	require.Equal(t, c.m, got)
}

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

func TestAttributes(t *testing.T) {
	r := registry{}
	var (
		a  = r.fromYAML("a", `{i16: 1, i8: 1, pi8: 1}`, &Entity{}).(*Entity)
		b  = r.fromYAML("b", `{i16: 2, i8: 2}`, &Entity{}).(*Entity)
		c  = r.fromYAML("c", `{i16: 1, i8: 2}`, &Entity{}).(*Entity)
		na = r.register("na", &Node{E: a}).(*Node)
		nb = r.register("nb", &Node{E: b, L: na}).(*Node)
		nc = r.register("nc", &Node{E: c, R: nb}).(*Node)
	)
	attrTestCases := []attrTestCase{
		{
			"a", a,
			addToEmptyEntityMap(map[rel.Attribute]interface{}{
				PI8: int8(1),
				I8:  int8(1),
				I16: int16(1),
			}),
		},
		{
			"b", b,
			addToEmptyEntityMap(map[rel.Attribute]interface{}{
				I8:  int8(2),
				I16: int16(2),
			}),
		},
		{
			"c", c,
			addToEmptyEntityMap(map[rel.Attribute]interface{}{
				I8:  int8(2),
				I16: int16(1),
			}),
		},
		{
			"na", na,
			map[rel.Attribute]interface{}{
				E: a,
			},
		},
		{
			"nb", nb,
			map[rel.Attribute]interface{}{
				E: b,
				L: na,
			},
		},
		{
			"nc", nc,
			map[rel.Attribute]interface{}{
				E: c,
				R: nb,
			},
		},
	}
	for _, tc := range attrTestCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.run(t, r)
		})
	}
}

func TestQueries(t *testing.T) {
	r := registry{}

	var (
		a  = r.fromYAML("a", `{i16: 1, i8: 1, pi8: 1}`, &Entity{}).(*Entity)
		b  = r.fromYAML("b", `{i16: 2, i8: 2}`, &Entity{}).(*Entity)
		c  = r.fromYAML("c", `{i16: 1, i8: 2}`, &Entity{}).(*Entity)
		na = r.register("na", &Node{E: a}).(*Node)
		nb = r.register("nb", &Node{E: b, L: na}).(*Node)
		nc = r.register("nc", &Node{E: c, R: nb}).(*Node)
	)
	var (
		queryTestCases = []queryTestCase{
			{
				name: "foo",
				data: []interface{}{a, b, c, na, nb, nc},
				queries: []queryCase{
					{
						name: "a fields",
						q: rel.MustQuery(
							Schema,
							v("a").Attr(I16, rel.Value(int16(1))),
							v("a").Attr(I8, v("ai8")),
							v("a").Attr(PI8, v("api8")),
						),
						resVars: []v{"a", "ai8", "api8"},
						results: [][]interface{}{
							{a, int8(1), int8(1)},
						},
					},
					{
						name: "a-c-b join",
						q: rel.MustQuery(
							Schema,
							v("a").Attr(I8, rel.Value(int8(1))),
							v("b").Attr(I16, rel.Value(int16(2))),
							v("b").Attr(I8, rel.Value(int8(2))),
							v("c").Attr(I16, rel.Value(int16(1))),
							v("c").Attr(I8, rel.Value(int8(2))),
						),
						resVars: []v{"a", "b", "c"},
						results: [][]interface{}{
							{a, b, c},
						},
					},
					{
						name: "nil values don't show up",
						q: rel.MustQuery(
							Schema,
							v("e").Attr(PI8, rel.Value(int8(1))),
						),
						resVars: []v{"e"},
						results: [][]interface{}{
							{a},
						},
					},
					{
						name: "list all the values",
						q: rel.MustQuery(
							Schema,
							v("e").Attr(I8, v("i8")),
						),
						resVars: []v{"e", "i8"},
						results: [][]interface{}{
							{a, int8(1)},
							{b, int8(2)},
							{c, int8(2)},
						},
					},
					{
						name: "nodes with elements where i8=2",
						q: rel.MustQuery(
							Schema,
							v("i8").Eq(rel.Value(int8(2))),
							v("i8").Entities(I8, "e"), // using this notation just to exercise it
							v("n").Attr(E, v("e")),
						),
						resVars: []v{"n", "e"},
						results: [][]interface{}{
							{nb, b},
							{nc, c},
						},
					},
					{
						name: "list all the i8 values",
						q: rel.MustQuery(
							Schema,
							v("e").Attr(I8, v("i8")),
						),
						resVars: []v{"i8"},
						// Note that you get the value for all the entities
						// which can offer it. That's maybe surprising.
						results: [][]interface{}{
							{int8(1)},
							{int8(2)},
							{int8(2)},
						},
					},
					{
						name: "types of all the entities",
						q: rel.MustQuery(
							Schema,
							v("e").Attr(rel.Type, v("typ")),
						),
						resVars: []v{"e", "typ"},
						results: [][]interface{}{
							{a, reflect.TypeOf((*Entity)(nil))},
							{b, reflect.TypeOf((*Entity)(nil))},
							{c, reflect.TypeOf((*Entity)(nil))},
							{na, reflect.TypeOf((*Node)(nil))},
							{nb, reflect.TypeOf((*Node)(nil))},
							{nc, reflect.TypeOf((*Node)(nil))},
						},
					},
					{
						name: "nodes by type",
						q: rel.MustQuery(
							Schema,
							v("na").Type((*Node)(nil)),
							v("na").Attr(E, v("a")),
							v("nb").Attr(L, v("na")),
							v("nc").Attr(R, v("nb")),
						),
						resVars: []v{"na", "nb", "nc", "a"},
						results: [][]interface{}{
							{na, nb, nc, a},
						},
					},
					{
						name: "nodes by type",
						q: rel.MustQuery(
							Schema,
							v("n").Type((*Node)(nil)),
						),
						resVars: []v{"n"},
						results: [][]interface{}{
							{na},
							{nb},
							{nc},
						},
					},
					{
						name: "basic any",
						q: rel.MustQuery(
							Schema,
							v("entity").Type((*Node)(nil), (*Entity)(nil)),
						),
						resVars: []v{"entity"},
						results: [][]interface{}{
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
	)
	for _, tc := range queryTestCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.runTestCase(t)
		})
	}
}
