package rel

import (
	"reflect"

	"github.com/cockroachdb/errors"
)

type filter struct {
	input     []slotIdx
	predicate reflect.Value
}

type queryBuilder struct {
	sc            *Schema
	variables     []Var
	variableSlots map[Var]slotIdx
	facts         []fact
	slots         []slot
	filters       []filter

	// Track whether the slotIdx holds an entity separately. We want to
	// know this in planning, but it'll be implicit during execution.
	// This might be badly named. What we really mean here is that the
	// slotIdx is A join target.
	slotIsEntity []bool
}

func (p *queryBuilder) processClause(t Clause) {
	switch t := t.(type) {
	case *datomDecl:
		p.processFactDecl(t)
	case *eqDecl:
		p.processEqDecl(t)
	case *and:
		for _, term := range *t {
			p.processClause(term)
		}
	case *filterDecl:
		p.processFilterDecl(t)
	default:
		panic(errors.AssertionFailedf("unknown clause type %T", t))
	}
}

func (p *queryBuilder) processFactDecl(fd *datomDecl) {
	f := fact{
		variable: p.maybeAddVar(fd.entity, true),
		attr:     fd.attribute,
	}
	f.value = p.processValueExpr(fd.value)
	p.typeCheck(f)
	p.facts = append(p.facts, f)
}

func (p *queryBuilder) processValueExpr(rawValue Expr) slotIdx {
	switch v := rawValue.(type) {
	case Var:
		return p.maybeAddVar(v, false)
	case anyExpr:
		sd := slot{
			any: make([]typedValue, len(v)),
		}
		for i, vv := range v {
			tv, err := makeComparableValue(p.sc, vv)
			if err != nil {
				panic(err)
			}
			sd.any[i] = tv
		}
		return p.fillSlot(sd, false)
	case valueExpr:
		tv, err := makeComparableValue(p.sc, v.value)
		if err != nil {
			panic(err)
		}
		return p.fillSlot(slot{typedValue: tv}, false)
	default:
		panic(errors.Errorf("unknown expr type %T", rawValue))
	}
}

func (p *queryBuilder) maybeAddVar(v Var, entity bool) slotIdx {
	id, exists := p.variableSlots[v]
	if exists {
		if entity && !p.slotIsEntity[id] {
			p.slotIsEntity[id] = entity
		}
		return id
	}
	id = p.fillSlot(slot{}, entity)
	p.variables = append(p.variables, v)
	p.variableSlots[v] = id
	return id
}

func (p *queryBuilder) fillSlot(sd slot, isEntity bool) slotIdx {
	s := slotIdx(len(p.slots))
	p.slots = append(p.slots, sd)
	p.slotIsEntity = append(p.slotIsEntity, isEntity)
	return s
}

// findEntitySlots finds the slots which correspond to entity variableSlots in
// the order in which they appear. This will imply the user-requested join
// order.
func (p *queryBuilder) findEntitySlots() (entitySlots []slotIdx) {
	for i := range p.slots {
		if p.slotIsEntity[i] {
			entitySlots = append(entitySlots, slotIdx(i))
		}
	}
	return entitySlots
}

func (p *queryBuilder) processEqDecl(t *eqDecl) {
	varIdx := p.maybeAddVar(t.v, false)
	valueIdx := p.processValueExpr(t.expr)
	// This is somewhat inefficient but what it does is it lets
	// us state that the variable is equal to itself and that it
	// is equal to the value.
	//
	// Note that there's no need to typeCheck because the SelfAttribute accepts
	// all types. We'll do a pass of type-checking at the end.
	p.facts = append(p.facts,
		fact{
			variable: varIdx,
			attr:     SelfAttribute,
			value:    valueIdx,
		},
		fact{
			variable: varIdx,
			attr:     SelfAttribute,
			value:    varIdx,
		})
}

// typeCheck asserts that the value types for the fact are sane given the
// attribute.
func (p *queryBuilder) typeCheck(f fact) {
	s := &p.slots[f.value]
	if s.empty() {
		return
	}
	switch f.attr {
	case TypeAttribute:
		checkSlotType(s, schemaTypePtrType)
	default:
		checkSlotType(s, p.sc.attributeTypes[f.attr])
	}
}

var boolType = reflect.TypeOf((*bool)(nil)).Elem()

func (p *queryBuilder) processFilterDecl(t *filterDecl) {
	fv := reflect.ValueOf(t.predicateFunc)
	// Type check the function.
	if err := checkNotNil(fv); err != nil {
		panic(errors.Wrapf(err, "nil filter function for variables %s", t.vars))
	}
	if fv.Kind() != reflect.Func {
		panic(errors.Errorf(
			"non-function %T filter function for variables %s",
			t.predicateFunc, t.vars,
		))
	}
	ft := fv.Type()
	if ft.NumOut() != 1 || ft.Out(0) != boolType {
		panic(errors.Errorf(
			"invalid non-bool return from %T filter function for variables %s",
			t.predicateFunc, t.vars,
		))
	}
	if ft.NumIn() != len(t.vars) {
		panic(errors.Errorf(
			"invalid %T filter function for variables %s accepts %d inputs",
			t.predicateFunc, t.vars, ft.NumIn(),
		))
	}

	slots := make([]slotIdx, len(t.vars))
	for i, v := range t.vars {
		slots[i] = p.maybeAddVar(v, false)
		// TODO(ajwerner): This should end up constraining the slot type, but
		// it currently doesn't. In fact, we have no way of constraining the
		// type for a non-entity variable. Probably the way this should go is
		// that the slots should carry constraints like types and any values.
		// Then, when we go to populate them, we can enforce the constraints.
		//
		// Instead, as a hack, we've got a runtime check on the types to fail
		// out if any of the types are not right.
		checkSlotType(&p.slots[slots[i]], ft.In(i))
	}
	p.filters = append(p.filters, filter{
		input:     slots,
		predicate: fv,
	})
}

func checkSlotType(s *slot, exp reflect.Type) {
	if !s.empty() {
		if err := checkType(s.typ, exp); err != nil {
			panic(err)
		}
	}
	for i := range s.any {
		if err := checkType(s.any[i].typ, exp); err != nil {
			panic(err)
		}
	}
}
