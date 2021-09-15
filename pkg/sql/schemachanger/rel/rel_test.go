package rel_test

import (
	"flag"
	"io/ioutil"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel/internal/testschema"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

var rewrite bool

func init() {
	flag.BoolVar(&rewrite, "rewrite", false, "set to rewrite the test output")
}

type Suite struct {
	Registry       *DataRegistry
	QueryTests     []QueryTestCase
	AttributeTests []AttributeTestCase
}

func TestRel(t *testing.T) {
	suite.Run(t)
}

func (s Suite) Run(t *testing.T) {
	for _, tc := range s.QueryTests {
		t.Run(tc.Name, func(t *testing.T) {
			tc.runTestCase(t, s.Registry)
		})
	}
	for _, tc := range s.AttributeTests {
		t.Run(tc.Name, func(t *testing.T) {
			tc.run(t, s.Registry)
		})
	}
}

type QueryCase struct {
	Name    string
	Query   *rel.Query
	ResVars []rel.Var
	Results [][]interface{}
}

type QueryTestCase struct {
	Name       string
	Data       []string
	QueryCases []QueryCase
}

type registryYAMLMarshaler interface {
	marshalYAML(t *testing.T, r *DataRegistry, n *yaml.Node)
}

func (qc QueryCase) runQueryCase(t *testing.T, db *rel.Database) {
	var results [][]interface{}
	require.NoError(t, qc.Query.Prepare().Iterate(db, func(r rel.Result) error {
		var cur []interface{}
		for _, v := range qc.ResVars {
			cur = append(cur, r.Var(v))
		}
		results = append(results, cur)
		return nil
	}))
	expResults := append(qc.Results[:0:0], qc.Results...)
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

func (tc QueryTestCase) runTestCase(t *testing.T, r *DataRegistry) {
	db := rel.NewDatabase(Schema, nil)
	var dataNode, queriesNode yaml.Node
	dataNode.Kind = yaml.MappingNode
	queriesNode.Kind = yaml.MappingNode
	addData := func(t *testing.T, k string, v interface{}) {
		var n yaml.Node
		require.NoError(t, n.Encode(v))
		n.Style = yaml.FlowStyle
		dataNode.Content = append(dataNode.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: k},
			&n,
		)
	}
	scalar := func(value string) *yaml.Node {
		return &yaml.Node{
			Kind:  yaml.ScalarNode,
			Value: value,
		}
	}

	encodeValues := func(t *testing.T, v []interface{}) *yaml.Node {
		var seq yaml.Node
		seq.Kind = yaml.SequenceNode
		seq.Style = yaml.FlowStyle
		for _, v := range v {
			name, ok := r.GetName(v)
			if ok {
				seq.Content = append(seq.Content, scalar(name))
			} else if typ, isType := v.(reflect.Type); isType {
				seq.Content = append(seq.Content, scalar(typ.String()))
			} else {
				var content yaml.Node
				require.NoError(t, content.Encode(v))
				seq.Content = append(seq.Content, &content)
			}
		}
		return &seq
	}
	encodeResults := func(t *testing.T, results [][]interface{}) *yaml.Node {
		var res yaml.Node
		res.Kind = yaml.SequenceNode
		for _, r := range results {
			res.Content = append(res.Content, encodeValues(t, r))
		}
		return &res
	}
	addQuery := func(t *testing.T, qc QueryCase) {
		var query, varsNode yaml.Node
		require.NoError(t, query.Encode(qc.Query.Clauses()))
		require.NoError(t, varsNode.Encode(qc.ResVars))
		varsNode.Style = yaml.FlowStyle
		queriesNode.Content = append(queriesNode.Content,
			scalar(qc.Name),
			&yaml.Node{
				Kind: yaml.MappingNode,
				Content: []*yaml.Node{
					scalar("query"),
					&query,
					scalar("result-vars"),
					&varsNode,
					scalar("results"),
					encodeResults(t, qc.Results),
				},
			})
	}
	outer := yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			scalar("data"),
			&dataNode,
			scalar("queries"),
			&queriesNode,
		},
	}
	for _, k := range tc.Data {
		v := r.MustGetByName(t, k)
		addData(t, k, v)
		require.NoError(t, db.Insert(v))
	}
	for _, qc := range tc.QueryCases {
		t.Run(qc.Name, func(t *testing.T) {
			qc.runQueryCase(t, db)
			addQuery(t, qc)
		})
	}
	out, err := yaml.Marshal(&outer)
	require.NoError(t, err)
	tdp := testutils.TestDataPath(t, strings.Replace(strings.TrimPrefix(t.Name(), "Test"), "/", "_", -1))
	if rewrite {
		require.NoError(t, ioutil.WriteFile(tdp, out, 0777))
	} else {
		exp, err := ioutil.ReadFile(tdp)
		require.NoError(t, err)
		require.Equal(t, exp, out)
	}
}

type v = rel.Var

type DataRegistry struct {
	valueToName map[interface{}]string
	nameToValue map[string]interface{}

	// Some entities are initialized from yaml literals, preserve those
	// for formatting later as they are nicer.
	nameToYAML map[string]string
}

func NewDataRegistry() *DataRegistry {
	return &DataRegistry{
		valueToName: make(map[interface{}]string),
		nameToValue: make(map[string]interface{}),
		nameToYAML:  make(map[string]string),
	}
}

func (r *DataRegistry) Register(name string, v interface{}) interface{} {
	if existing, exists := r.nameToValue[name]; exists {
		panic(errors.AssertionFailedf(
			"entity with name %s already registered %v, trying to register %v",
			name, existing, v,
		))
	}
	r.nameToValue[name] = v
	r.valueToName[v] = name
	return v
}

func (r DataRegistry) FromYAML(name, yamlData string, dest interface{}) interface{} {
	if err := yaml.Unmarshal([]byte(yamlData), dest); err != nil {
		panic(err)
	}
	r.Register(name, dest)
	r.nameToYAML[name] = yamlData
	return dest
}

func (r DataRegistry) MustGetByName(t *testing.T, k string) interface{} {
	got, ok := r.nameToValue[k]
	require.Truef(t, ok, "MustGetByName(%s)", k)
	return got
}

func (r *DataRegistry) GetName(i interface{}) (string, bool) {
	got, ok := r.valueToName[i]
	return got, ok
}

type AttributeTestCase struct {
	Name     string
	Expected map[rel.Attribute]interface{}
}

func (c AttributeTestCase) run(t *testing.T, r *DataRegistry) {
	got := make(map[rel.Attribute]interface{})
	require.NoError(t, Schema.IterateAttributes(r.MustGetByName(t, c.Name), func(
		attribute rel.Attribute, value interface{},
	) error {
		got[attribute] = value
		return nil
	}))
	require.Equal(t, c.Expected, got)
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

var (
	r  = NewDataRegistry()
	a  = r.FromYAML("a", `{i16: 1, i8: 1, pi8: 1}`, &Entity{}).(*Entity)
	b  = r.FromYAML("b", `{i16: 2, i8: 2}`, &Entity{}).(*Entity)
	c  = r.FromYAML("c", `{i16: 1, i8: 2}`, &Entity{}).(*Entity)
	na = r.Register("na", &Node{E: a}).(*Node)
	nb = r.Register("nb", &Node{E: b, L: na}).(*Node)
	nc = r.Register("nc", &Node{E: c, R: nb}).(*Node)

	queryCases = []QueryTestCase{
		{
			Name: "foo",
			Data: []string{"a", "b", "c", "na", "nb", "nc"},
			QueryCases: []QueryCase{
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
	suite = Suite{
		Registry:   r,
		QueryTests: queryCases,
		AttributeTests: []AttributeTestCase{
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
		},
	}
)
