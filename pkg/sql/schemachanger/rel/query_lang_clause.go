package rel

import "gopkg.in/yaml.v3"

// Clause is the basic building block of a query. The most foundational
// clause is datom which declares some fact about an attribute of a named
// variable.
type Clause interface {
	// clause is a marker interface to prevent external package from implementing
	// the interface.
	clause()

	yaml.Marshaler
}

type Clauses []Clause

func (c Clauses) flattened() Clauses {
	if !c.hasAnd() {
		return c
	}
	var ret Clauses
	for _, cl := range c {
		switch cl := cl.(type) {
		case *and:
			for _, cl := range Clauses(*cl).flattened() {
				ret = append(ret, cl)
			}
		default:
			ret = append(ret, cl)
		}
	}
	return ret
}

func (c Clauses) hasAnd() bool {
	for _, cl := range c {
		if _, isAnd := cl.(*and); isAnd {
			return true
		}
	}
	return false
}

func (c Clauses) MarshalYAML() (interface{}, error) {
	var n yaml.Node
	if err := n.Encode([]Clause(c)); err != nil {
		return nil, err
	}
	n.Style = yaml.LiteralStyle
	return &n, nil
}

// datom is a basic Clause. It declares that the provided attribute of the
// referenced variable must be the provided value. Value can be a constant,
// a Var, or a set of constants as returned from Any.
func datom(entity Var, attr Attribute, value Expr) Clause {
	return &datomDecl{entity: entity, attribute: attr, value: value}
}

type eqDecl struct {
	v    Var
	expr Expr
}

func (e *eqDecl) clause() {}

type datomDecl struct {
	entity    Var
	attribute Attribute
	value     Expr
}

func (f *datomDecl) clause() {}

var _ Clause = (*datomDecl)(nil)

type and []Clause

func (a *and) clause() {}

type filterDecl struct {
	name          string
	vars          []Var
	predicateFunc interface{}
}

func (f filterDecl) clause() {}
