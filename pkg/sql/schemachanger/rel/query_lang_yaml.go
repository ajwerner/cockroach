package rel

import (
	"fmt"
	"reflect"
	"strings"

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
	return clauseStr("$"+string(e.v), e.expr)
}

func exprToString(e Expr) (string, error) {
	var expr yaml.Node
	if err := expr.Encode(e.forYAML()); err != nil {
		return "", err
	}
	expr.Style = yaml.FlowStyle
	out, err := yaml.Marshal(&expr)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), err
}

func (f *datomDecl) MarshalYAML() (interface{}, error) {
	return clauseStr(fmt.Sprintf("$%s[%s]", f.entity, f.attribute), f.value)
}

func clauseStr(lhs string, rhs Expr) (string, error) {
	rhsStr, err := exprToString(rhs)
	if err != nil {
		return "", err
	}
	op := "="
	if _, isAny := rhs.(anyExpr); isAny {
		op = "IN"
	}
	return fmt.Sprintf("%s %s %s", lhs, op, rhsStr), nil
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
