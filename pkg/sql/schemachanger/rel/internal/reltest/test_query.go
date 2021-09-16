package reltest

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// DatabaseTest tests a set of queries in the context of a database which
// has items specified in Data in a Database.
type DatabaseTest struct {
	Name string
	Data []string
	// Each of the QueryCases will be run with the set of indexes.
	Indexes    [][][]rel.Attribute
	QueryCases []QueryTest
}

// QueryTest is a subtest of a DatabaseTest which ensures that the results
// of a query match the expectations.
type QueryTest struct {
	Name    string
	Query   *rel.Query
	ResVars []rel.Var
	Results [][]interface{}
}

func (tc DatabaseTest) run(t *testing.T, ec execContext) {
	for _, databaseIndexes := range tc.databaseIndexes() {
		t.Run(fmt.Sprintf("%s", databaseIndexes), func(t *testing.T) {
			db := rel.NewDatabase(ec.Schema, databaseIndexes)
			for _, k := range tc.Data {
				v := ec.MustGetByName(t, k)
				require.NoError(t, db.Insert(v))
			}
			for _, qc := range tc.QueryCases {
				t.Run(qc.Name, func(t *testing.T) {
					qc.run(t, db)
				})
			}
		})
	}
}

func (qc QueryTest) run(t *testing.T, db *rel.Database) {
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

func (tc DatabaseTest) encodeData() *yaml.Node {
	dataNode := yaml.Node{Kind: yaml.SequenceNode, Style: yaml.FlowStyle}
	for _, k := range tc.Data {
		dataNode.Content = append(dataNode.Content, scalarYAML(k))
	}
	return &dataNode
}

func (tc DatabaseTest) encodeQueries(t *testing.T, ec execContext) *yaml.Node {
	queriesNode := yaml.Node{Kind: yaml.MappingNode}

	encodeValues := func(t *testing.T, v []interface{}) *yaml.Node {
		var seq yaml.Node
		seq.Kind = yaml.SequenceNode
		seq.Style = yaml.FlowStyle
		for _, v := range v {
			name, ok := ec.GetName(v)
			if ok {
				seq.Content = append(seq.Content, scalarYAML(name))
			} else if typ, isType := v.(reflect.Type); isType {
				seq.Content = append(seq.Content, scalarYAML(typ.String()))
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
	encodeResultVars := func(resultVars []rel.Var) *yaml.Node {
		n := yaml.Node{Kind: yaml.SequenceNode, Style: yaml.FlowStyle}
		for _, v := range resultVars {
			n.Content = append(n.Content, scalarYAML("$"+string(v)))
		}
		return &n
	}
	addQuery := func(t *testing.T, qc QueryTest) {
		var query yaml.Node
		require.NoError(t, query.Encode(qc.Query.Clauses()))
		queriesNode.Content = append(queriesNode.Content,
			scalarYAML(qc.Name),
			&yaml.Node{
				Kind: yaml.MappingNode,
				Content: []*yaml.Node{
					scalarYAML("query"),
					&query,
					scalarYAML("result-vars"),
					encodeResultVars(qc.ResVars),
					scalarYAML("results"),
					encodeResults(t, qc.Results),
				},
			})
	}
	for _, qc := range tc.QueryCases {
		addQuery(t, qc)
	}
	return &queriesNode
}

func (tc DatabaseTest) databaseIndexes() [][][]rel.Attribute {
	if len(tc.Indexes) == 0 {
		return [][][]rel.Attribute{{}}
	}
	return tc.Indexes
}

func (tc DatabaseTest) encode(t *testing.T, ec execContext) *yaml.Node {
	return &yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			scalarYAML("indexes"),
			tc.encodeIndexes(),
			scalarYAML("data"),
			tc.encodeData(),
			scalarYAML("queries"),
			tc.encodeQueries(t, ec),
		},
	}
}

func (tc DatabaseTest) encodeIndexes() *yaml.Node {
	databaseIndexesNode := yaml.Node{Kind: yaml.SequenceNode}
	for _, indexes := range tc.databaseIndexes() {
		indexesNode := yaml.Node{Kind: yaml.SequenceNode}
		for _, idx := range indexes {
			indexNode := yaml.Node{Kind: yaml.SequenceNode, Style: yaml.FlowStyle}
			for _, attr := range idx {
				indexNode.Content = append(indexNode.Content, scalarYAML(attr.String()))
			}
			indexesNode.Content = append(indexesNode.Content, &indexNode)
		}
		databaseIndexesNode.Content = append(databaseIndexesNode.Content, &indexesNode)
	}
	return &databaseIndexesNode
}
