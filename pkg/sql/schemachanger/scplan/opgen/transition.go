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
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/errors"
)

type transition struct {
	from, to   scpb.Status
	revertible bool
	phase      scop.Phase
	emitFns    []interface{}
}

func (t transition) generate(el scpb.Element, from scpb.Status) (stage, error) {
	t.from = from
	fn, err := makeOpsFunc(el, t.emitFns)
	if err != nil {
		return stage{}, err
	}
	return stage{
		From:       from,
		To:         t.to,
		Revertible: t.revertible,
		Ops:        fn,
	}, nil
}

type transitionProperty interface {
	apply(spec *transition)
}

func to(to scpb.Status, properties ...transitionProperty) *transition {
	ts := transition{
		to:         to,
		revertible: true,
	}
	for _, p := range properties {
		p.apply(&ts)
	}
	return &ts
}

func revertible(b bool) transitionProperty {
	return revertibleProperty(b)
}

func phase(p scop.Phase) transitionProperty {
	return phaseProperty(p)
}

type phaseProperty scop.Phase

func (p phaseProperty) apply(spec *transition) {
	spec.phase = scop.Phase(p)
}

type revertibleProperty bool

func (r revertibleProperty) apply(spec *transition) {
	spec.revertible = bool(r)
}

var _ transitionProperty = revertibleProperty(true)

func emit(fn interface{}) transitionProperty {
	return emitFn{fn}
}

type emitFn struct {
	fn interface{}
}

func (e emitFn) apply(spec *transition) {
	spec.emitFns = append(spec.emitFns, e.fn)
}

type opFuncs struct {
	funcs []reflect.Value
}

func (f opFuncs) emit(element scpb.Element) []scop.Op {
	ret := make([]scop.Op, 0, len(f.funcs))
	in := []reflect.Value{reflect.ValueOf(element)}
	for _, fn := range f.funcs {
		out := fn.Call(in)
		ret = append(ret, out[0].Interface().(scop.Op))
	}
	return ret
}

func makeOpsFunc(el scpb.Element, fns []interface{}) (opFuncs, error) {
	var ret opFuncs
	for _, fn := range fns {
		if err := checkOpFunc(el, fn); err != nil {
			return opFuncs{}, err
		}
		ret.funcs = append(ret.funcs, reflect.ValueOf(fn))
	}
	return ret, nil
}

var opType = reflect.TypeOf((*scop.Op)(nil)).Elem()

func checkOpFunc(el scpb.Element, fn interface{}) error {
	fnV := reflect.ValueOf(fn)
	fnT := fnV.Type()
	if fnT.Kind() != reflect.Func {
		return errors.Errorf(
			"%v is a %s, expected %s", fnT, fnT.Kind(), reflect.Func,
		)
	}
	elType := reflect.TypeOf(el)
	if fnT.NumIn() != 1 || fnT.In(0) != elType {
		return errors.Errorf(
			"expected %v to be a func with one argument of type %s", fnT, elType,
		)
	}
	if fnT.NumOut() != 1 || !fnT.Out(0).Implements(opType) {
		return errors.Errorf(
			"expected %v to be a func with one return value of type %s", fnT, opType,
		)
	}
	return nil
}
