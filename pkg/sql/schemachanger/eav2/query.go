package eav2

import (
	"reflect"
	"sort"

	"github.com/cockroachdb/errors"
)

// Var is a variable name. Everything is convention, but, when you create
// rules and use variable names which are not part of the defined scope of
// the rule, the new variables which will be created will have a scope prefix
// to try to ensure that they are unique. Given that, don't put `:` in your
// variable names.
type Var string

type Term interface {
	term()
}

type any []interface{}

// Any is a mechanism to create a value which accepts
// more than one value.
//
// TOOD(ajwerner): Consider replacing with the more general Or
// that takes terms.
func Any(v ...interface{}) interface{} {
	return any(v)
}

// Datom is a basic fact.
func Datom(entity Var, attr Attribute, value interface{}) Term {
	return &factDecl{e: entity, a: attr, v: value}
}

type factDecl struct {
	e Var
	a Attribute
	v interface{}
}

func (f *factDecl) term() {}

var _ Term = (*factDecl)(nil)

type slot int

type fact struct {
	entity slot
	attr   Attribute
	value  slot
}

type slotData struct {
	typ   reflect.Type
	value interface{}
}

func (d slotData) eq(other slotData) bool {
	// TODO(ajwerner): Deal with types.
	switch {
	case d.value == nil && other.value == nil:
		return true
	case d.value == nil:
		return false
	case other.value == nil:
		return false
	default:
		_, eq := compare(d.value, other.value)
		return eq
	}
}

// We're going to want to figure out all of the
func Prepare(sc *Schema, rules ...Term) *Query {
	// We want to determine A set of variables and their types.
	var (
		vars  = map[Var]slot{}
		slots []slotData
		// Track whether the slot holds an entity separately. We want to
		// know this in planning but it'll be implicit during execution.
		// This might be badly named. What we really mean here is that the
		// slot is A join target.
		slotIsEntity []bool
	)
	fillSlot := func(val interface{}, typ reflect.Type, isEntity bool) slot {
		s := slot(len(slots))
		slots = append(slots, slotData{
			typ:   typ,
			value: val,
		})
		slotIsEntity = append(slotIsEntity, isEntity)
		return s
	}
	maybeAddVar := func(v Var, entity bool) slot {
		id, exists := vars[v]
		if exists {
			if entity && !slotIsEntity[id] {
				slotIsEntity[id] = entity
			}
			return id
		}
		id = fillSlot(nil, nil, entity)
		vars[v] = id
		return id
	}
	appendFactToClauses := func(clauses [][]fact, fd *factDecl) {
		f := fact{
			entity: maybeAddVar(fd.e, true),
			attr:   fd.a,
		}
		switch v := fd.v.(type) {
		case nil:
			panicf("illegal nil value in attribute %s", fd.a)
		case Var:
			f.value = maybeAddVar(v, false)
		default:
			val, typ, err := makeComparableValue(sc, fd.a, fd.v)
			if err != nil {
				panic(err)
			}
			f.value = fillSlot(val, typ, false)
		}
		for i := range clauses {
			clauses[i] = append(clauses[i], f)
		}
	}
	processFactDecl := func(clauses [][]fact, fd *factDecl) [][]fact {
		switch v := fd.v.(type) {
		default:
			appendFactToClauses(clauses, fd)
		case any:
			// Duplicate all of the clauses.
			cur := clauses
			for _, c := range cur {
				for range v[1:] {
					clauses = append(clauses, append([]fact{}, c...))
				}
			}
			var disjunct factDecl
			for i, val := range v {
				disjunct = *fd
				disjunct.v = val
				appendFactToClauses(
					clauses[len(cur)*i:len(cur)*(i+1)],
					&disjunct,
				)
			}
		}
		return clauses
	}
	processTerm := func(t Term, clauses [][]fact) [][]fact {
		switch t := t.(type) {
		case *factDecl:
			return processFactDecl(clauses, t)
		default:
			panic(errors.AssertionFailedf("unknown term type %T", t))
		}
	}
	clauses := [][]fact{{}}
	for _, r := range rules {
		clauses = processTerm(r, clauses)
	}

	// Here we have code related to laying things out for execution.
	// It's possible that it's all too complex. My indexing scheme is
	// per entity as opposed to per datum so we unify based on entities.
	// Maybe this is A mistake. For now, the way this proceeds is to
	// group the rules by entity and by attribute.

	var entities []slot
	for i := range slots {
		if slotIsEntity[i] {
			entities = append(entities, slot(i))
		}
	}

	for _, c := range clauses {
		sort.SliceStable(c, func(i, j int) bool {
			if c[i].entity == c[j].entity {
				return c[i].attr.Ordinal() < c[j].attr.Ordinal()
			}
			return c[i].entity < c[j].entity
		})
	}

	return &Query{
		variables: vars,
		rules:     rules,
		entities:  entities,
		parts:     clauses,
		slots:     slots,
	}
}
