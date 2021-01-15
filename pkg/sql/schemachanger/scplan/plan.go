// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scplan

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

type Phase int

const (
	PostStatementPhase Phase = iota
	PreCommitPhase
	PostCommitPhase
)

type Params struct {
	ExecutionPhase       Phase
	CreatedDescriptorIDs catalog.DescriptorIDSet
}

type Plan struct {
	Params       Params
	InitialNodes []*scpb.Node
	Graph        *scgraph.Graph
	Stages       []Stage
}

type Stage struct {
	Before, After []*scpb.Node
	Ops           scop.Ops
}

func MakePlan(initialStates []*scpb.Node, params Params) (_ Plan, err error) {
	defer func() {
		if r := recover(); r != nil {
			rAsErr, ok := r.(error)
			if !ok {
				rAsErr = errors.Errorf("panic during MakePlan: %v", r)
			}
			err = errors.CombineErrors(err, rAsErr)
		}
	}()

	g, err := scgraph.New(initialStates)
	if err != nil {
		return Plan{}, err
	}
	// TODO(ajwerner): Generate the stages for all of the phases as it will make
	// debugging easier.
	for _, ts := range initialStates {
		p[reflect.TypeOf(ts.Element())].ops(g, ts.Target, ts.State, params)
	}
	if err := g.ForEachNode(func(n *scpb.Node) error {
		d, ok := p[reflect.TypeOf(n.Element())]
		if !ok {
			return errors.Errorf("not implemented for %T", n.Target)
		}
		d.deps(g, n.Target, n.State)
		return nil
	}); err != nil {
		return Plan{}, err
	}
	stages := buildStages(initialStates, g)
	return Plan{
		Params:       params,
		InitialNodes: initialStates,
		Graph:        g,
		Stages:       stages,
	}, nil
}

func buildStages(init []*scpb.Node, g *scgraph.Graph) []Stage {
	// TODO(ajwerner): deal with the case where the target state was
	// fulfilled by something that preceded the initial state.
	cur := init
	fulfilled := map[*scpb.Node]struct{}{}
	filterUnsatisfiedEdgesStep := func(edges []*scgraph.OpEdge) ([]*scgraph.OpEdge, bool) {
		candidates := make(map[*scpb.Node]struct{})
		for _, e := range edges {
			candidates[e.To()] = struct{}{}
		}
		// Check to see if the current set of edges will have their dependencies met
		// if they are all run. Any which will not must be pruned. This greedy
		// algorithm works, but a justification is in order.
		failed := map[*scgraph.OpEdge]struct{}{}
		for _, e := range edges {
			_ = g.ForEachDepEdgeFrom(e.To(), func(de *scgraph.DepEdge) error {
				_, isFulfilled := fulfilled[de.To()]
				_, isCandidate := candidates[de.To()]
				if isFulfilled || isCandidate {
					return nil
				}
				failed[e] = struct{}{}
				return iterutil.StopIteration()
			})
		}
		if len(failed) == 0 {
			return edges, true
		}
		truncated := edges[:0]
		for _, e := range edges {
			if _, found := failed[e]; !found {
				truncated = append(truncated, e)
			}
		}
		return truncated, false
	}
	filterUnsatisfiedEdges := func(edges []*scgraph.OpEdge) ([]*scgraph.OpEdge, bool) {
		for len(edges) > 0 {
			if filtered, done := filterUnsatisfiedEdgesStep(edges); !done {
				edges = filtered
			} else {
				return filtered, true
			}
		}
		return edges, false
	}
	buildStageType := func(edges []*scgraph.OpEdge) (Stage, bool) {
		edges, ok := filterUnsatisfiedEdges(edges)
		if !ok {
			return Stage{}, false
		}
		next := append(cur[:0:0], cur...)
		var ops []scop.Op
		for i, ts := range cur {
			for _, e := range edges {
				if e.From() == ts {
					next[i] = e.To()
					ops = append(ops, e.Op())
					break
				}
			}
		}
		return Stage{
			Before: cur,
			After:  next,
			Ops:    scop.MakeOps(ops...),
		}, true
	}

	var stages []Stage
	for {
		// Note that the current nodes are fulfilled for the sake of dependency
		// checking.
		for _, ts := range cur {
			fulfilled[ts] = struct{}{}
		}

		// Extract the set of op edges for the current stage.
		var opEdges []*scgraph.OpEdge
		for _, t := range cur {
			// TODO(ajwerner): improve the efficiency of this lookup.
			// Look for an opEdge from this node. Then, for the other side
			// of the opEdge, look for dependencies.
			if oe, ok := g.GetOpEdgeFrom(t); ok {
				opEdges = append(opEdges, oe)
			}
		}

		// Group the op edges a per-type basis.
		opTypes := make(map[scop.Type][]*scgraph.OpEdge)
		for _, oe := range opEdges {
			opTypes[oe.Op().Type()] = append(opTypes[oe.Op().Type()], oe)
		}

		// Greedily attempt to find a stage which can be executed. This is sane
		// because once a dependency is met, it never becomes unmet.
		var didSomething bool
		var s Stage
		for _, typ := range []scop.Type{
			scop.MutationType,
			scop.BackfillType,
			scop.ValidationType,
		} {
			if s, didSomething = buildStageType(opTypes[typ]); didSomething {
				break
			}
		}
		if !didSomething {
			break
		}
		stages = append(stages, s)
		cur = s.After
	}
	return stages
}
