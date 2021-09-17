package rel

import "gopkg.in/yaml.v3"

// Clauses exists to handle flattening of a slice of clauses before marshaling.
type Clauses []Clause

func flattened(c Clauses) Clauses {
	hasAnd := func() bool {
		for _, cl := range c {
			if _, isAnd := cl.(*and); isAnd {
				return true
			}
		}
		return false
	}
	if !hasAnd() {
		return c
	}
	var ret Clauses
	for _, cl := range c {
		switch cl := cl.(type) {
		case *and:
			for _, cl := range flattened(Clauses(*cl)) {
				ret = append(ret, cl)
			}
		default:
			ret = append(ret, cl)
		}
	}
	return ret
}

func (c Clauses) MarshalYAML() (interface{}, error) {
	fc := flattened(c)
	var n yaml.Node
	if err := n.Encode([]Clause(fc)); err != nil {
		return nil, err
	}
	n.Style = yaml.LiteralStyle
	return &n, nil
}
