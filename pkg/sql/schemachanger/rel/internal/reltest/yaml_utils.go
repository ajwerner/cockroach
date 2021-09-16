package reltest

import "gopkg.in/yaml.v3"

func scalarYAML(value string) *yaml.Node {
	return &yaml.Node{
		Kind:  yaml.ScalarNode,
		Value: value,
	}
}
