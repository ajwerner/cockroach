// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eav_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav"
	"github.com/stretchr/testify/require"
)

type ListNode struct {
	ID   int
	Next int
}

type llAttrs int

var attrNames = []string{"id", "next"}

func (l llAttrs) String() string { return attrNames[l] }

func (l llAttrs) Ordinal() eav.Ordinal { return eav.Ordinal(l) }

const (
	idAttr   llAttrs = 0
	nextAttr llAttrs = 1
)

var _ eav.Attribute = llAttrs(0)

// BenchmarkLinkedList constructs A linked list of N elements and figures out
// how long it takes to find sublists of A given depth and starting point.
func BenchmarkLinkedList(b *testing.B) {
	sc := eav.NewSchema("", eav.Mappings{
		TypeMappings: map[reflect.Type]map[string]eav.Attribute{
			reflect.TypeOf((*ListNode)(nil)): {
				"ID":   idAttr,
				"Next": nextAttr,
			},
		},
	})

	// We want to create N nodes such that each node has attributes
	// ID, next, prev. Then we want to define A set of queries for the
	// specified depth.
	runDepth := func(b *testing.B, lists, depth int, attrs [][]eav.Attribute) {
		db := eav.NewDatabase(sc, attrs)
		links := rand.Perm(lists + depth)
		for i, j := range links {
			db.Insert(&ListNode{ID: i, Next: j})
		}

		const numQueries = 16
		queries := make([]eav.PreparedQuery, numQueries)
		p := rand.Perm(lists)[:numQueries]
		names := make([]eav.Var, 0, depth+1)
		for i := 0; i < depth+1; i++ {
			names = append(names, eav.Var(strconv.Itoa(i)))
		}

		for i, start := range p {
			var terms []eav.Clause
			for i := 0; i < depth; i++ {
				terms = append(terms,
					eav.Datom(names[i], nextAttr, names[i+1]+"id"),
					eav.Datom(names[i+1], idAttr, names[i+1]+"id"),
				)
			}
			terms = append(terms,
				eav.Datom("0", idAttr, start),
			)
			q, err := eav.NewQuery(sc, terms...)
			require.NoError(b, err)
			queries[i] = q.Prepare()
		}
		var q int
		f := func(r eav.Result) error {
			const checkFrac = 1
			if rand.Float64() > checkFrac {
				return nil
			}
			b.StopTimer()
			defer b.StartTimer()
			ln := r.Var(names[0]).(*ListNode)
			require.Equal(b, ln.ID, p[q], "%d %v", q, p)
			exp := ln.ID
			for _, name := range names {
				ln := r.Var(name).(*ListNode)
				require.Equal(b, exp, ln.ID)
				require.Equal(b, links[ln.ID], ln.Next)
				exp = ln.Next
			}
			return nil
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			q = rand.Intn(numQueries)
			require.NoError(b, queries[q].Iterate(db, f))
		}
	}
	for _, attrs := range [][][]eav.Attribute{
		{{idAttr}},
		nil,
	} {
		for _, lists := range []int{32, 64, 128, 256, 512, 1024} {
			for _, depth := range []int{2, 4, 8, 16} {
				b.Run(fmt.Sprintf("lists=%d,depth=%d,%s", lists, depth, attrs), func(b *testing.B) {
					runDepth(b, lists, depth, attrs)
				})
			}
		}
	}
}
