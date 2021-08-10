package eav2

import "reflect"

type Var string

type Rule interface {
	rule()
}

type any []interface{}

func Any(v ...interface{}) interface{} {
	return any(v)
}

func (v Var) Constrain(a Attribute, value interface{}) Rule {
	return &factDecl{e: v, a: a, v: value}
}

func (v Var) Bind(a Attribute, valueVar Var, value interface{}) Rule {
	return factDecls{
		{e: v, a: a, v: value},
		{e: v, a: a, v: valueVar},
	}
}

type factDecls []*factDecl

func (f factDecls) rule() {}

type factDecl struct {
	e Var
	a Attribute
	v interface{}
}

func (f *factDecl) rule() {}

var _ Rule = (*factDecl)(nil)

type Query struct {
}

type variable int

type fact struct {
	entity variable
	attr   Attribute
	value  interface{}
}

// We're going to want to figure out all of the
func Prepare(sc *Schema, rules ...Rule) *Query {
	// We want to determine a set of variables and their types.
	var (
		vars     = map[Var]variable{}
		varTypes = []reflect.Type{}
		isEntity []bool
		clauses  = [][]fact{{}}
	)
	maybeAddVar := func(v Var, entity bool) variable {
		id, exists := vars[v]
		if exists {
			isEntity[id] = isEntity[id] || entity
			return id
		}
		id = variable(len(vars))
		vars[v] = id
		varTypes = append(varTypes, nil)
		isEntity = append(isEntity, entity)
		return id
	}
	var processRule func(r Rule)
	processRule = func(r Rule) {
		switch r := r.(type) {
		case *factDecl:
			id := maybeAddVar(r.e, true)
			switch v := r.v.(type) {
			case any:
				// TODO(ajwerner): Decide whether or not to allow nested any.

				// Duplicate all of the clauses.
				cur := clauses
				for _, c := range cur {
					for range v[1:] {
						clauses = append(clauses, append([]fact{}, c...))
					}
				}
				for i := range cur {
					for j, val := range v {
						n := i*len(cur) + j
						clauses[n] = append(clauses[n], fact{id, r.a, val})
					}
				}
			default:
				for i := range clauses {
					clauses[i] = append(clauses[i], fact{id, r.a, r.v})
				}
			}
		case factDecls:
			for _, d := range r {
				processRule(d)
			}
		}
	}
	for _, r := range rules {
		processRule(r)
	}

	// I guess now we need to do some type checking.
	panic("unimplemented")
}
