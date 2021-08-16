package eav

import "github.com/cockroachdb/errors"

type queryBuilder struct {
	sc    *Schema
	vars  map[Var]slotIdx
	facts []fact
	slots []slot

	// Track whether the slotIdx holds an entity separately. We want to
	// know this in planning but it'll be implicit during execution.
	// This might be badly named. What we really mean here is that the
	// slotIdx is A join target.
	slotIsEntity []bool
}

func (p *queryBuilder) processClause(t Clause) {
	switch t := t.(type) {
	case *datomDecl:
		p.processFactDecl(t)
	case *and:
		for _, term := range *t {
			p.processClause(term)
		}
	default:
		panic(errors.AssertionFailedf("unknown clause type %T", t))
	}
}

func (p *queryBuilder) processFactDecl(fd *datomDecl) {
	f := fact{
		entity: p.maybeAddVar(fd.entity, true),
		attr:   fd.attribute,
	}
	switch v := fd.value.(type) {
	case Var:
		f.value = p.maybeAddVar(v, false)
	case any:
		sd := slot{
			any: make([]typedValue, len(v)),
		}
		for i, vv := range v {
			tv, err := makeComparableValue(p.sc, fd.attribute, vv)
			if err != nil {
				panic(err)
			}
			sd.any[i] = tv
		}
		f.value = p.fillSlot(sd, false)
	default:
		tv, err := makeComparableValue(p.sc, fd.attribute, fd.value)
		if err != nil {
			panic(err)
		}
		f.value = p.fillSlot(slot{typedValue: tv}, false)
	}
	p.facts = append(p.facts, f)
}

func (p *queryBuilder) maybeAddVar(v Var, entity bool) slotIdx {
	id, exists := p.vars[v]
	if exists {
		if entity && !p.slotIsEntity[id] {
			p.slotIsEntity[id] = entity
		}
		return id
	}
	id = p.fillSlot(slot{}, entity)
	p.vars[v] = id
	return id
}

func (p *queryBuilder) fillSlot(sd slot, isEntity bool) slotIdx {
	s := slotIdx(len(p.slots))
	p.slots = append(p.slots, sd)
	p.slotIsEntity = append(p.slotIsEntity, isEntity)
	return s
}

// findEntitySlots finds the slots which correspond to entity variables in
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
