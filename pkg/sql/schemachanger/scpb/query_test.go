// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scpb_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/stretchr/testify/require"
)

func TestQueryBasic(t *testing.T) {
	mkType := func(id descpb.ID) *scpb.Target {
		return scpb.NewTarget(scpb.Target_ADD, &scpb.Type{TypeID: id})
	}
	mkTypeRef := func(typID, descID descpb.ID) *scpb.Target {
		return scpb.NewTarget(scpb.Target_ADD, &scpb.TypeReference{
			TypeID: typID,
			DescID: descID,
		})
	}
	mkTable := func(id descpb.ID) *scpb.Target {
		return scpb.NewTarget(scpb.Target_ADD, &scpb.Table{TableID: id})
	}
	concatNodes := func(nodes ...[]*scpb.Node) []*scpb.Node {
		var ret []*scpb.Node
		for _, n := range nodes {
			ret = append(ret, n...)
		}
		return ret
	}
	mkNodes := func(status scpb.Status, targets ...*scpb.Target) []*scpb.Node {
		var ret []*scpb.Node
		for _, t := range targets {
			ret = append(ret, &scpb.Node{Status: status, Target: t})
		}
		return ret
	}
	mkTableTypeRef := func(typID, tabID descpb.ID) []*scpb.Target {
		return []*scpb.Target{
			mkType(typID),
			mkTable(tabID),
			mkTypeRef(typID, tabID),
		}
	}
	type v = eav.Var
	var (
		d             = eav.Datom
		pathJoinQuery = eav.MustQuery(scpb.AttrSchema,
			eav.EntityType("table", (*scpb.Table)(nil)),
			eav.EntityType("ref", (*scpb.TypeReference)(nil)),
			eav.EntityType("type", (*scpb.Type)(nil)),
			d("table", scpb.AttrDescID, v("table-id")),
			d("ref", scpb.AttrDescID, v("table-id")),
			d("ref", scpb.AttrReferencedDescID, v("type-id")),
			d("type", eav.TypeAttribute, reflect.TypeOf((*scpb.Type)(nil))),
			d("type", scpb.AttrDescID, v("type-id")),
			scpb.NodeRule("table", v("direction"), v("status")),
			scpb.NodeRule("ref", v("direction"), v("status")),
			scpb.NodeRule("type", v("direction"), v("status")),
		).Prepare()
	)
	type queryExpectations struct {
		query eav.PreparedQuery
		nodes []string
		exp   []string
	}
	for _, c := range []struct {
		nodes   []*scpb.Node
		queries []queryExpectations
	}{
		{
			nodes: concatNodes(
				mkNodes(scpb.Status_ABSENT, mkTableTypeRef(1, 2)...),
				mkNodes(scpb.Status_PUBLIC, mkTableTypeRef(1, 2)...),
				mkNodes(scpb.Status_ABSENT, mkTableTypeRef(3, 4)...),
				mkNodes(scpb.Status_PUBLIC,
					mkType(5),
					mkTable(6),
					mkTypeRef(6, 5)),
			),
			queries: []queryExpectations{
				{
					query: pathJoinQuery,
					nodes: []string{"table", "type"},
					exp: []string{`
[Table: {DescID: 2}, ABSENT, ADD]
[Type: {DescID: 1}, ABSENT, ADD]`, `
[Table: {DescID: 2}, PUBLIC, ADD]
[Type: {DescID: 1}, PUBLIC, ADD]`, `
[Table: {DescID: 4}, ABSENT, ADD]
[Type: {DescID: 3}, ABSENT, ADD]`,
					},
				},
				{
					query: pathJoinQuery,
					nodes: []string{"table", "type", "ref"},
					exp: []string{`
[Table: {DescID: 2}, ABSENT, ADD]
[Type: {DescID: 1}, ABSENT, ADD]
[TypeReference: {DescID: 2, ReferencedDescID: 1}, ABSENT, ADD]`, `
[Table: {DescID: 2}, PUBLIC, ADD]
[Type: {DescID: 1}, PUBLIC, ADD]
[TypeReference: {DescID: 2, ReferencedDescID: 1}, PUBLIC, ADD]`, `
[Table: {DescID: 4}, ABSENT, ADD]
[Type: {DescID: 3}, ABSENT, ADD]
[TypeReference: {DescID: 4, ReferencedDescID: 3}, ABSENT, ADD]`,
					},
				},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			tr := eav.NewDatabase(scpb.AttrSchema, [][]eav.Attribute{
				{scpb.AttrColumnID},
			})
			for _, n := range c.nodes {
				tr.Insert(n)
			}
			for _, q := range c.queries {
				t.Run("", func(t *testing.T) {
					var results []string
					require.NoError(t, q.query.Iterate(tr, func(r eav.Result) error {
						results = append(results, formatResults(r, q.nodes))
						return nil
					}))
					require.Equal(t, q.exp, results)
				})
			}
		})
	}
}

func TestContradiction(t *testing.T) {
	require.Panics(t, func() {
		eav.NewQuery(scpb.AttrSchema,
			eav.EntityType("a", (*scpb.Type)(nil)),
			eav.EntityType("b", (*scpb.Table)(nil)),
			eav.Datom("a", eav.TypeAttribute, eav.Var("typ")),
			eav.Datom("b", eav.TypeAttribute, eav.Var("typ")),
		)
	})
}

func formatResults(r eav.Result, nodes []string) string {
	var buf strings.Builder
	for _, n := range nodes {
		buf.WriteString("\n")
		got := r.Var(eav.Var(n))
		fmt.Fprintf(&buf, "%T: %v", got, got)
	}
	return buf.String()
}
