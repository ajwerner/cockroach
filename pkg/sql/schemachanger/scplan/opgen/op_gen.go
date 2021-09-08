// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/errors"
)

type registry struct {
	targets []target
}

var opRegistry = &registry{}

func BuildGraph(initial scpb.State) (*scgraph.Graph, error) {
	return opRegistry.buildGraph(initial)
}

func (r *registry) buildGraph(initial scpb.State) (*scgraph.Graph, error) {
	g, err := scgraph.New(initial)
	if err != nil {
		return nil, err
	}
	tr := rel.NewDatabase(screl.Schema, [][]rel.Attribute{
		{rel.Type, screl.Direction},
	})
	for _, n := range initial {
		tr.Insert(n)
	}
	for _, t := range r.targets {
		if err := t.iterateFunc(tr, func(n *scpb.Node) error {
			var in bool
			for _, op := range t.stages {
				if in = in || op.From == n.Status; !in {
					g.AddOpEdges(n.Target, op.From, op.From, true)
				} else {
					if op.To == scpb.Status_UNKNOWN {
						panic(errors.Errorf("here %T %s", n.Target.GetElement(), op.From))
					}
					g.AddOpEdges(n.Target, op.From, op.To, op.Revertible, op.Ops.emit(n.GetElement())...)
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return g, nil
}

func (r *registry) register(
	e scpb.Element, dir scpb.Target_Direction, initialStatus scpb.Status, specs ...*transition,
) {
	target, err := makeTarget(e, dir, initialStatus, specs...)
	if err != nil {
		panic(errors.Wrapf(err, "generating stage for %T:%s", e, dir))
	}
	r.targets = append(r.targets, target)
}
