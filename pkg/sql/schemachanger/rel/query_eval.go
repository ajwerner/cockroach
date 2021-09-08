// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rel

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// evalContext implements Result and accumulates the state during the
// evaluation of A query.
type evalContext struct {
	q  *Query
	db *Database
	ri ResultIterator

	facts      []fact
	depth, cur int
	slots      []slot
}

func newEvalContext(q *Query) *evalContext {
	return &evalContext{
		q:     q,
		depth: len(q.entities),
		slots: append(make([]slot, 0, len(q.slots)), q.slots...),
		facts: q.facts,
	}
}

type evalResult evalContext

func (ec *evalResult) IterateVars(f func(Var)) {
	for _, v := range ec.q.variables {
		f(v)
	}
}

func (ec *evalResult) Var(name Var) interface{} {
	n, ok := ec.q.variableSlots[name]
	if !ok {
		// TODO(ajwerner): it is far from clear that this should ever happen.
		return nil
	}
	return ec.slots[n].toInterface()
}

// Iterate is part of the PreparedQuery interface.
func (ec *evalContext) Iterate(db *Database, ri ResultIterator) error {
	if db.schema != ec.q.schema {
		return errors.Errorf(
			"query and database are not from the same schema: %s != %s",
			db.schema.name, ec.q.schema.name,
		)
	}
	defer func() { ec.db, ec.ri = nil, nil }()
	ec.db, ec.ri = db, ri

	// TODO(ajwerner): Decide if we should allow depth-zero queries to exist.
	if ec.depth == 0 {
		return nil
	}
	return ec.iterateNext()
}

func (ec *evalContext) iterateNext() error {
	if done, err := ec.maybeFoundResult(); done || err != nil {
		return err
	}
	if done, err := ec.maybeVisitAlreadyBoundEntity(); done || err != nil {
		return err
	}
	where, anyAttr, anyValues := ec.buildWhere()
	defer putValues(where)
	if anyAttr == nil {
		return ec.db.iterate(where, ec)
	}
	for _, v := range anyValues {
		where.add(anyAttr.Ordinal(), v.value)
		if err := ec.db.iterate(where, ec); err != nil {
			return err
		}
	}
	return nil
}

func (ec *evalContext) visit(e *entity) error {
	// Keep track of which slots were filled as part of this step in the
	// evaluation and then unset them when we pop out of this stack frame.
	var slotsFilled util.FastIntSet
	defer func() {
		slotsFilled.ForEach(func(i int) {
			ec.slots[i].typedValue = typedValue{}
		})
	}()

	// Apply the information about this variable to the slots and find a new
	// fixed point given this information.
	unifyFacts := func() (foundContradiction bool) {
		foundContradiction, _, _ = unify(ec.facts, ec.slots, &slotsFilled)
		return foundContradiction
	}
	if foundContradiction := ec.setEntitySlot(e, &slotsFilled) ||
		ec.propagateCurEntityValues(e, &slotsFilled) ||
		unifyFacts(); foundContradiction {
		return nil
	}

	// Step down to the next variable, or, if at the bottom, ensure that
	// all the required slots are filled and pass the result to the caller.
	ec.cur++
	defer func() { ec.cur-- }()

	// If we're not done, recurse to the next level of the join.
	return ec.iterateNext()
}

func (ec *evalContext) maybeFoundResult() (done bool, _ error) {
	if ec.cur != ec.depth {
		return false, nil
	}
	// We're at the bottom of the join.
	// Check to see if all the variableSlots have been assigned a value.
	// If not, then we did not successfully unify everything (right?).
	for _, v := range ec.q.variableSlots {
		if ec.slots[v].value == nil {
			return true, nil
		}
	}
	for _, f := range ec.q.filters {
		// TODO(ajwerner): Catch panics here and convert them to errors.
		ins := make([]reflect.Value, len(f.input))
		insI := make([]interface{}, len(f.input))
		for i, idx := range f.input {
			inI := ec.slots[idx].typedValue.toInterface()
			in := reflect.ValueOf(inI)
			// Note that this will enforce that the type of the input to the filter
			// matches the expectation by omitting results of the wrong type. This
			// may or may not be the right behavior.
			//
			// TODO(ajwerner): Enforce the typing at a lower layer. See the TODO
			// where this filter was built.
			inType := f.predicate.Type().In(i)
			if in.Type() != inType {
				if in.Type().ConvertibleTo(inType) {
					in = in.Convert(inType)
				} else {
					return true, nil
				}
			}
			ins[i] = in
			insI[i] = inI
		}
		outs := f.predicate.Call(ins)
		if !outs[0].Bool() {
			return true, nil
		}
	}
	return true, ec.ri((*evalResult)(ec))
}

// Construct a where clause with all of the bound valuesMap.
// The logic here is that if there's an any for a slotIdx with a fact for the
// current entity, maybe we want to use it to bound our search. In general
// it will help if we have an index that covers the current facts plus this
// value. There may be more than one any, in which case, this is not going
// to be very smart.
func (ec *evalContext) buildWhere() (where *valuesMap, anyAttr Attribute, anyValues []typedValue) {
	where = getValues()

	// TODO(ajwerner): Make this filter push-down smarter based on the indexes
	// which exist.
	for _, f := range ec.facts {
		if f.variable != ec.q.entities[ec.cur] {
			continue
		}

		s := ec.slots[f.value]
		if !s.empty() {
			where.add(f.attr.Ordinal(), s.value)
		} else if anyAttr == nil && s.any != nil {
			anyAttr, anyValues = f.attr, s.any
		}
	}
	return where, anyAttr, anyValues
}

func unify(
	facts []fact, s []slot, set *util.FastIntSet,
) (contradictionFound bool, eIdx slotIdx, attr Attribute) {
	// TODO(ajwerner): As we unify we could determine that some facts are no
	// longer relevant. When we do that we could move them to the front and keep
	// track of some offset. In principle, we could do this and then each time
	// we step back up a frame, merge sort the fact back into sorted order and,
	// in that way, reduce the set of facts which need to be searched while
	// still requiring only linear operations in the number of facts. As it
	// stands, this algorithm is quadratic in the number of facts.
	setSlot := func(dst, src slotIdx) {
		s[dst] = s[src]
		if set != nil {
			set.Add(int(dst))
		}
	}
	for {
		var somethingChanged bool
		var prev, cur *fact
		for i := 1; i < len(facts); i++ {
			prev, cur = &facts[i-1], &facts[i]
			if prev.variable != cur.variable || prev.attr != cur.attr ||
				// This case is weird. I guess we could do more to get
				// rid of this case.
				prev.value == cur.value ||
				s[prev.value].eq(s[cur.value]) {
				continue
			}
			if s[prev.value].empty() {
				setSlot(prev.value, cur.value)
			} else if s[cur.value].empty() {
				setSlot(cur.value, prev.value)
			} else {
				return true, cur.variable, cur.attr
			}
			somethingChanged = true
		}
		if !somethingChanged {
			return false, 0, nil
		}
	}
}

func (ec *evalContext) propagateCurEntityValues(
	e *entity, slotsFilled *util.FastIntSet,
) (foundContradiction bool) {
	// TODO(ajwerner): Constrain to just the facts about this variable.
	for _, f := range ec.facts {
		if f.variable != ec.q.entities[ec.cur] {
			continue
		}
		tv, ok := e.getTypedValue(f.attr, ec.db.entities)
		if !ok {
			return true // we have no value for this attribute, contradiction
		}
		s := &ec.slots[f.value]
		ok, foundContradiction := s.shouldSet(tv.value)
		if foundContradiction {
			return true
		}
		if ok {
			s.set(tv)
			slotsFilled.Add(int(f.value))
		}
	}
	return false
}

func (ec *evalContext) setEntitySlot(
	e *entity, slotsFilled *util.FastIntSet,
) (foundContradiction bool) {
	eSlot := ec.q.entities[ec.cur]
	s := &ec.slots[eSlot]
	idVal := e.get(Self)
	ok, foundContradiction := s.shouldSet(idVal)
	if foundContradiction {
		return true
	}
	if ok {
		s.set(typedValue{
			typ:   e.getTypeInfo().typ,
			value: idVal,
		})
		slotsFilled.Add(int(eSlot))
	}
	return false
}

// Check if the slot is already filled with a value because it was
// already bound. If it does not exist in the database, then there's
// a contraction and we can return. If it does, then we can
func (ec *evalContext) maybeVisitAlreadyBoundEntity() (done bool, _ error) {
	s := &ec.slots[ec.q.entities[ec.cur]]
	if s.empty() {
		return false, nil
	}
	v, ok := s.value.(*uintptr)
	if !ok {
		return true, errors.AssertionFailedf(
			"expected *uintptr for variable value, found %T", s.value,
		)
	}
	e, ok := ec.db.entities[*v]
	if !ok {
		return true, nil // contradiction
	}
	return true, ec.visit(e)
}
