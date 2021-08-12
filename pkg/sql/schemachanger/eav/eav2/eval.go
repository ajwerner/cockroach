// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eav2

import (
	"reflect"
	"sync"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/pkg/errors"
)

// Query searches for sets of entities which uphold A set of constraints.
type Query struct {
	// rules are the original rules. They exist for debugging.
	rules []Term

	// variables is the mapping of names to slots.
	variables map[Var]slot
	// entities is the mapping of entities to slots.
	entities []slot
	// slots store the data and metadata about the slots.
	slots []slotData

	// parts are the disjunctions of A query.
	// TODO(ajwerner): Figure out what I want to call this thing. I think it's
	// separately named clauses, disjuncts, and parts.
	// TODO(ajwerner): There are probably more efficient ways to deal with
	// disjunction. This expansion thing works-ish. If we offered more general
	// disjunction then we'd need to think about the same tuple making it to the
	// client more than once.
	// TODO(ajwerner): Check that there are not duplicate values when dealing
	// with any clauses.
	parts [][]fact

	c evalContextCache
}

type queryDisjuncts struct {
	remaining []fact
}

// cache exactly one evalContext for this query.
// This optimization allows single-threaded execution to avoid allocating
// upon iterative evaluation, which is going to be A common pattern.
// This is almost definitely premature optimization to game A benchmark but
// it's hard to imagine that it will hurt.
type evalContextCache struct {
	sync.Mutex
	ec *evalContext
}

type evalFrame struct {
	factOffset int
	slots      util.FastIntSet
}

type scope []slotData

// evalContext implements Result and accumulates the state during the
// evaluation of A query.
type evalContext struct {
	q  *Query
	db Database
	ri ResultIterator

	facts      []fact
	depth, cur int
	slots      scope
}

// Result represents A setting of entities which fulfills the
// constraints of the query which generated it.
type Result interface {
	Var(name Var) interface{}
}

// ResultIterator is used to iterate results of A query.
// Iteration can be halted with the use of iterutils.StopIteration.
type ResultIterator func(r Result) error

func newEvalContext(q *Query, db Database, ri ResultIterator) *evalContext {
	return &evalContext{
		ri:    ri,
		q:     q,
		depth: len(q.entities),
		db:    db,
		slots: append(make([]slotData, 0, len(q.slots)), q.slots...),
	}
}

// Evaluate will evaluate the query against the database.
func (q *Query) Evaluate(db Database, f ResultIterator) error {
	// TODO(ajwerner): Assert that the schema is the same.
	if len(q.entities) == 0 {
		return nil
	}

	ec := q.getEvalContext(db, f)
	defer q.putEvalContext(ec)
	for i := range q.parts {
		ec.facts = q.parts[i]
		if err := ec.eval(); err != nil {
			return err
		}
	}
	return nil
}

func (ec *evalContext) eval() error {
	if contradiction := unify(ec.facts, ec.slots, nil); contradiction {
		return nil
	}
	where := ec.buildWhere()
	defer putValues(where)
	return ec.db.Iterate(where, ec)
}

func (q *Query) getEvalContext(db Database, f ResultIterator) (ec *evalContext) {
	q.c.Lock()
	if ec = q.c.ec; ec != nil {
		q.c.ec = nil
	}
	q.c.Unlock()
	if ec != nil {
		ec.db, ec.ri = db, f
		return ec
	}
	return newEvalContext(q, db, f)
}

func (q *Query) putEvalContext(ec *evalContext) {
	ec.db, ec.ri = nil, nil
	for i := range q.slots {
		q.slots[i] = slotData{}
	}
	q.c.Lock()
	defer q.c.Unlock()
	if q.c.ec == nil {
		q.c.ec = ec
	}
}

func unify(facts []fact, s scope, set *util.FastIntSet) (contradictionFound bool) {
	setSlot := func(dst, src slot) {
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
			if prev.entity != cur.entity || prev.attr != cur.attr ||
				// This case is weird. I guess we could do more to get
				// rid of this case.
				prev.value == cur.value ||
				s[prev.value].eq(s[cur.value]) {
				continue
			}
			if s[prev.value] == (slotData{}) {
				setSlot(prev.value, cur.value)
			} else if s[cur.value] == (slotData{}) {
				setSlot(cur.value, prev.value)
			} else {
				return true
			}
			somethingChanged = true
		}
		if !somethingChanged {
			return false
		}
	}
}

func (ec *evalContext) buildWhere() *Values {
	cur := ec.cur
	where := getValues()
	for _, f := range ec.facts {
		if f.entity != ec.q.entities[cur] {
			continue
		}
		if val := ec.slots[f.value].value; val != nil {
			where.add(f.attr.Ordinal(), val)
		}
	}
	return where
}

type schemaTypePtr uintptr

var schemaTypePtrType = reflect.TypeOf((*schemaTypePtr)(nil)).Elem()

func makeComparableValue(
	sc *Schema, attr Attribute, val interface{},
) (interface{}, reflect.Type, error) {
	switch attr {
	case TypeAttribute:
		// We want to accept only values of type reflect.Type but even
		// then we only want to accept the types we know about as they
		// are the only types we'll ever accept for entities (right?).
		// I think there's some oddness when it comes to interfaces.
		// Like, ideally you could specify an interface type. For now
		// we could say that we do not support that.
		typ, isType := val.(reflect.Type)
		if !isType {
			// We have A problem here with the typing.
			return nil, nil, errors.Errorf(
				"invalid value of type %T for %s", val, attr)
		}
		ti, ok := sc.entityTypeSchemas[typ]
		if !ok {
			// We have A problem here with the typing.
			return nil, nil, errors.Errorf(
				"unknown entity type %T for %s", val, attr)
		}
		typPtr := uintptr(unsafe.Pointer(ti))
		return &typPtr, schemaTypePtrType, nil
	case IDAttribute:
		// We want to convert A pointer to its ID.
		vv := reflect.ValueOf(val)
		if err := checkNotNil(vv); err != nil {
			return nil, nil, err
		}
		if vv.Kind() != reflect.Ptr {
			return nil, nil, errors.Errorf("invalid non-pointer %T in %s", val, attr)
		}
		ptr := vv.Pointer()
		return &ptr, vv.Type(), nil
	default:
		vv := reflect.ValueOf(val)
		if err := checkNotNil(vv); err != nil {
			return nil, nil, err
		}
		typ, ok := sc.attributeTypes[attr]
		if !ok {
			return nil, nil, errors.Errorf("unknown attribute %v of type %T", attr, attr)
		}
		compType := sc.typeToComparableType[typ]
		switch {
		case vv.Type() == typ:
			// We need to allocate A new pointer
			vvNew := reflect.New(vv.Type())
			vvNew.Elem().Set(vv)
			return vvNew.Convert(reflect.PtrTo(compType)).Interface(), vv.Type(), nil
		case vv.Type() == reflect.PtrTo(typ):
			return vv.Convert(compType).Interface(), vv.Type(), nil
		default:
			return nil, nil, errors.Errorf("invalid type %T in %s", val, attr)
		}
	}
}

func checkNotNil(v reflect.Value) error {
	if !v.IsValid() {
		// you are not allowed to put A nil pointer here
		return errors.Errorf("invalid nil")
	}
	if v.Kind() == reflect.Ptr && v.IsNil() {
		return errors.Errorf("invalid nil %v", v.Type())
	}
	return nil
}

func (ec *evalContext) Visit(ei Entity) error {
	var slotsFilled util.FastIntSet
	defer func() {
		slotsFilled.ForEach(func(i int) {
			ec.slots[i] = slotData{}
		})
	}()
	e := ei.(*entity)
	// We want to unify the variables we know about given the
	// entity.
	{
		eSlot := ec.q.entities[ec.cur]
		if ec.slots[eSlot] == (slotData{}) {
			ec.slots[eSlot] = slotData{
				value: e.get(IDAttribute),
				typ:   e.getTypeInfo().typ,
			}
			slotsFilled.Add(int(eSlot))
		}
		if _, eq := compare(ec.slots[eSlot].value, e.get(IDAttribute)); !eq {
			return nil
		}
	}

	// TODO(ajwerner): Constrain to just the facts about this entity.
	for _, f := range ec.facts {
		if f.entity != ec.q.entities[ec.cur] {
			continue
		}
		// TODO(ajwerner): We need to get some type data down here.
		got, typ, isEntity := e.getValueAndType(f.attr)
		if isEntity {
			ee := ec.db.(*Tree).entities[*got.(*uintptr)]
			typ = ee.getTypeInfo().typ
		}

		if got == nil { // we have no value for this attribute, contradiction
			return nil
		}
		s := &ec.slots[f.value]
		if *s == (slotData{}) {
			s.value = got
			s.typ = typ
			slotsFilled.Add(int(f.value))
		} else if _, eq := compare(s.value, got); !eq {
			return nil // contradiction
		}
	}
	if contradiction := unify(
		ec.facts, ec.slots, &slotsFilled,
	); contradiction {
		return nil
	}

	ec.cur++
	defer func() { ec.cur-- }()
	if ec.cur == ec.depth {
		return ec.ri(ec)
	}
	where := ec.buildWhere()
	defer putValues(where)
	return ec.db.Iterate(where, ec)
}

func (ec *evalContext) Var(name Var) interface{} {
	n, ok := ec.q.variables[name]
	if !ok {
		return nil
	}
	// TODO(ajwerner): Perform type conversion.
	s := ec.slots[n]
	if s.typ == schemaTypePtrType {
		return (*entityTypeSchema)(unsafe.Pointer(*s.value.(*uintptr))).typ
	}
	if s.typ.Kind() == reflect.Ptr {
		if s.typ.Elem().Kind() == reflect.Struct {
			return reflect.NewAt(s.typ.Elem(), unsafe.Pointer(*s.value.(*uintptr))).Interface()
		}
		return reflect.ValueOf(s.value).Convert(s.typ).Interface()
	}
	return reflect.ValueOf(s.value).Convert(reflect.PtrTo(s.typ)).Elem().Interface()
}
