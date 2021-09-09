package rel_test

import (
	"fmt"
	"reflect"
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
		expResults := append(qc.results[:0:0], qc.results...)
		findResulInExp := func(res []interface{}) (found bool) {
			for i, exp := range expResults {
				if reflect.DeepEqual(exp, res) {
					expResults = append(expResults[:i], expResults[i+1:]...)
					return true
				}
			}
			return false
		}

		for _, res := range results {
			if !findResulInExp(res) {
				t.Fatalf("failed to find %v in %v", res, expResults)
			}
		}
		require.Empty(t, expResults)
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
	var (
		a = mkEntity(t, `{i16: 1, i8: 1, pi8: 1}`)
		b = mkEntity(t, `{i16: 2, i8: 2}`)
	)

	Schema.IterateAttributes(a, func(attribute rel.Attribute, value interface{}) error {
		fmt.Printf("%s %T %v\n", attribute, value, value)
		return nil
	})
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
						v("a").Attr(I16, rel.Value(int16(1))),
						v("a").Attr(I8, v("ai8")),
						v("a").Attr(PI8, v("api8")),
					),
					resVars: []v{"a", "ai8", "api8"},
					results: [][]interface{}{
						{a, int8(1), int8(1)},
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
