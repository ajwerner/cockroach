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

// iterateNext steps down in the join to either decide that we've
// iterated through all the entity variables and need to process
// filters and pass along the result or that we need to go on and
// join the next entity.
func (ec *evalContext) iterateNext() error {

	// We're at the bottom of the iteration, check if all conditions have
	// been satisfied, and then invoke the client.
	if ec.cur == ec.depth {
		if ec.haveUnboundSlots() || ec.checkFilters() {
			return nil
		}
		return ec.ri((*evalResult)(ec))
	}

	// If we've already populated the next entity in the join as variable,
	// skip iterating the database.
	if done, err := ec.maybeVisitAlreadyBoundEntity(); done || err != nil {
		return err
	}

	where, anyAttr, anyValues := ec.buildWhere()
	defer putValues(where)
	if anyValues == nil {
		return ec.db.iterate(where, ec)
	}
	for _, v := range anyValues {
		where.add(anyAttr, v.value)
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

// Check to see if all the variableSlots have been assigned a value.
// If not, then we did not successfully unify everything.
func (ec *evalContext) haveUnboundSlots() bool {
	for _, v := range ec.q.variableSlots {
		if ec.slots[v].value == nil {
			return true
		}
	}
	return false
}

func (ec *evalContext) checkFilters() (done bool) {
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
					return true
				}
			}
			ins[i] = in
			insI[i] = inI
		}
		outs := f.predicate.Call(ins)
		if !outs[0].Bool() {
			return true
		}
	}
	return false
}

// Construct a where clause with all of the bound valuesMap.
// The logic here is that if there's an any for a slotIdx with a fact for the
// current entity, maybe we want to use it to bound our search. In general
// it will help if we have an index that covers the current facts plus this
// value. There may be more than one any, in which case, this is not going
// to be very smart.
func (ec *evalContext) buildWhere() (where *valuesMap, anyAttr ordinal, anyValues []typedValue) {
	where = getValues()

	// TODO(ajwerner): Make this filter push-down smarter based on the indexes
	// which exist.
	for _, f := range ec.facts {
		if f.variable != ec.q.entities[ec.cur] {
			continue
		}
		s := ec.slots[f.value]
		if !s.empty() {
			where.add(f.attr, s.value)
		} else if anyValues == nil && s.any != nil {
			anyAttr, anyValues = f.attr, s.any
		}
	}
	return where, anyAttr, anyValues
}

// unify attempts to unify away facts by assigning values to slots.
// If a contradiction is found, the returned slot of the entity in
// question and the ordinal of the attribute of conflict are returned.
func unify(
	facts []fact, s []slot, set *util.FastIntSet,
) (contradictionFound bool, eIdx slotIdx, attr ordinal) {
	// TODO(ajwerner): As we unify we could determine that some facts are no
	// longer relevant. When we do that we could move them to the front and keep
	// track of some offset. In principle, we could do this and then each time
	// we step back up a frame, merge sort the fact back into sorted order and,
	// in that way, reduce the set of facts which need to be searched while
	// still requiring only linear operations in the number of facts. As it
	// stands, this algorithm is quadratic in the number of facts.
	setSlot := func(dst, src slotIdx) (contradictionFound bool) {
		shouldSet, contradictionFound := s[dst].shouldSet(s[src].value)
		if contradictionFound || !shouldSet {
			return contradictionFound
		}
		s[dst].set(s[src].typedValue)
		if set != nil {
			set.Add(int(dst))
		}
		return false
	}
	for {
		var somethingChanged bool
		var prev, cur *fact
		for i := 1; i < len(facts); i++ {
			prev, cur = &facts[i-1], &facts[i]
			if prev.variable != cur.variable ||
				prev.attr != cur.attr ||
				// If the slots are the same, they've already been unified.
				// TODO(ajwerner): We could probably prune equal facts elsewhere.
				prev.value == cur.value ||
				// Similarly, we could avoid looking at fully unified facts.
				s[prev.value].eq(s[cur.value]) {
				continue
			}

			var unified bool
			for _, v := range []struct{ dst, src slotIdx }{
				{prev.value, cur.value},
				{cur.value, prev.value},
			} {
				if s[v.dst].empty() {
					if contradictionFound = setSlot(v.dst, v.src); contradictionFound {
						return true, cur.variable, cur.attr
					}
					unified = true
					break
				}
			}
			if unified {
				somethingChanged = true
			} else {
				return true, cur.variable, cur.attr
			}
		}
		if !somethingChanged {
			return false, 0, 0
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
		tv, ok := e.getTypedValue(ec.db.schema, f.attr)
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
	idVal := e.getComparableValue(ec.db.schema, Self)
	ok, foundContradiction := s.shouldSet(idVal)
	if foundContradiction {
		return true
	}
	if ok {
		s.set(typedValue{
			typ:   e.getTypeInfo(ec.db.Schema()).typ,
			value: idVal,
		})
		slotsFilled.Add(int(eSlot))
	}
	return false
}

// Check if the slot is already filled with a value because it was
// already bound. If it does not exist in the database, then there's
// a contraction, and we can return. If it does, then we can visit
// the entity as opposed to needing to find it.
func (ec *evalContext) maybeVisitAlreadyBoundEntity() (done bool, _ error) {
	s := &ec.slots[ec.q.entities[ec.cur]]
	if s.empty() {
		return false, nil
	}
	e, ok := ec.db.entities[s.value]

	// This is the case where we've somehow bound the entity to a
	// value that does not exist in the database. This could happen
	// if, say, the caller provided an entity as a value. I don't know
	// if we need to permit this, but it's definitely an edge. The claim
	// of the API is that we are going to iterate the set of entities in
	// the database which conform to the query.
	if !ok {
		return true, nil // contradiction
	}
	return true, ec.visit(e)
}
