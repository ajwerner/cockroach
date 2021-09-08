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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/errors"
)

// target represents a set of transitions for a target.
type target struct {
	element     scpb.Element
	dir         scpb.Target_Direction
	stages      []stage
	iterateFunc func(*rel.Database, func(*scpb.Node) error) error
}

func (t *target) Sequence() []scpb.Status {
	seq := make([]scpb.Status, len(t.stages)+1)
	seq[0] = t.stages[0].From
	for i, o := range t.stages {
		seq[i+1] = o.To
	}
	return seq
}

type stage struct {
	From, To   scpb.Status
	Revertible bool
	Ops        opFuncs
}

func makeTarget(
	e scpb.Element, dir scpb.Target_Direction, initialStatus scpb.Status, specs ...*transition,
) (target, error) {
	transitions := make([]stage, len(specs))
	from := func(i int) scpb.Status {
		if i == 0 {
			return initialStatus
		}
		return transitions[i-1].To
	}
	for i, s := range specs {
		var err error
		transitions[i], err = s.generate(e, from(i))
		if err != nil {
			return target{}, err
		}
	}
	iterateFunc, err := makeQuery(e, dir)
	if err != nil {
		return target{}, err
	}
	return target{
		element:     e,
		dir:         dir,
		stages:      transitions,
		iterateFunc: iterateFunc,
	}, nil
}

func makeQuery(
	e scpb.Element, d scpb.Target_Direction,
) (func(*rel.Database, func(*scpb.Node) error) error, error) {
	var element, target, node, dir rel.Var = "element", "target", "node", "dir"
	q, err := rel.NewQuery(screl.Schema,
		element.Type(e),
		dir.Eq(rel.Value(d)),
		screl.JoinTargetNode(element, target, node),
		target.Attr(screl.Direction, dir),
	)
	if err != nil {
		return nil, errors.WithAssertionFailure(err)
	}
	return func(database *rel.Database, f func(*scpb.Node) error) error {
		return q.Prepare().Iterate(database, func(r rel.Result) error {
			return f(r.Var(node).(*scpb.Node))
		})
	}, nil
}
