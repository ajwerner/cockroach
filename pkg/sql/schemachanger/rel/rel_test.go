package rel_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel/internal/testschema"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestRel(t *testing.T) {
	mkEntity := func(t *testing.T, data string) interface{} {
		var e Entity
		require.NoError(t, yaml.Unmarshal([]byte(data), &e))
		return &e
	}
	var (
		a = mkEntity(t, `{pi8: 1, i8: 1}`)
		b = mkEntity(t, `{pi8: 2, i8: 2}`)
	)

	type queryCase struct {
		name    string
		q       *rel.Query
		resVars []rel.Var
		results [][]interface{}
	}
	type testCase struct {
		name    string
		data    []interface{}
		queries []queryCase
	}
	runQueryCase := func(t *testing.T, db *rel.Database, qc queryCase) {
		var results [][]interface{}
		require.NoError(t, qc.q.Prepare().Iterate(db, func(r rel.Result) error {
			var cur []interface{}
			for _, v := range qc.resVars {
				cur = append(cur, r.Var(v))
			}
			results = append(results, cur)
			return nil
		}))
		require.ElementsMatch(t, qc.results, results)
	}
	runTestCase := func(t *testing.T, tc testCase) {
		db := rel.NewDatabase(Schema, nil)
		for _, v := range tc.data {
			require.NoError(t, db.Insert(v))
		}
		for _, qc := range tc.queries {
			t.Run(qc.name, func(t *testing.T) {
				runQueryCase(t, db, qc)
			})
		}
	}
	type v = rel.Var
	for _, tc := range []testCase{
		{
			name: "foo",
			data: []interface{}{a, b},
			queries: []queryCase{
				{
					name: "basic",
					q: rel.MustQuery(
						Schema,
						v("a").Attr(PI8, rel.Value(int8(1))),
						v("a").Attr(I8, v("ai8")),
					),
					resVars: []v{"a", "ai8"},
					results: [][]interface{}{
						{a, int8(1)},
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runTestCase(t, tc)
		})
	}
}
