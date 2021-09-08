package rel

import (
	"fmt"
	"reflect"

	"gopkg.in/yaml.v3"
)

func valueForYAML(v interface{}) interface{} {
	switch v := v.(type) {
	case fmt.Stringer:
		return v.String()
	default:
		return v
	}
}

func (v valueExpr) forYAML() interface{} {
	return valueForYAML(v.value)
}

func (a anyExpr) forYAML() interface{} {
	ret := make([]interface{}, 0, len(a))
	for _, v := range a {
		ret = append(ret, valueForYAML(v))
	}
	return ret
}

func (v Var) forYAML() interface{} {
	return string(v)
}

func (e *eqDecl) MarshalYAML() (interface{}, error) {
	var expr yaml.Node
	if err := expr.Encode(e.expr.forYAML()); err != nil {
		return nil, err
	}
	return &yaml.Node{
		Kind:  yaml.SequenceNode,
		Style: yaml.FlowStyle,
		Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Value: "="},
			{Kind: yaml.ScalarNode, Value: "$" + string(e.v)},
			&expr,
		},
	}, nil
}

func (f *datomDecl) MarshalYAML() (interface{}, error) {
	var expr yaml.Node
	if v, isVar := f.value.(Var); isVar {
		expr = yaml.Node{
			Kind:  yaml.ScalarNode,
			Value: "$" + string(v),
		}
	} else {
		if err := expr.Encode(f.value.forYAML()); err != nil {
			return nil, err
		}
	}
	return &yaml.Node{
		Kind:  yaml.SequenceNode,
		Style: yaml.FlowStyle,
		Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Value: f.attribute.String()},
			{Kind: yaml.ScalarNode, Value: "$" + string(f.entity)},
			&expr,
		},
	}, nil
}

func (a *and) MarshalYAML() (interface{}, error) {
	return []Clause(*a), nil
}

func (f filterDecl) MarshalYAML() (interface{}, error) {
	var paramTypes []interface{}
	{
		ft := reflect.TypeOf(f.predicateFunc)
		for i := 0; i < ft.NumIn(); i++ {
			paramTypes = append(paramTypes, ft.In(i).String())
		}
	}
	l := make([]interface{}, 0, len(f.vars)+1)
	l = append(l, []interface{}{"func", f.name, paramTypes})
	for _, v := range f.vars {
		l = append(l, "$"+string(v))
	}
	var ret yaml.Node
	if err := ret.Encode(l); err != nil {
		return nil, err
	}
	ret.Style = yaml.FlowStyle
	return &ret, nil
}
