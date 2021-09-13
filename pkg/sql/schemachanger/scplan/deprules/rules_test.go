package deprules

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/datadriven"
	"gopkg.in/yaml.v3"
)

// TestRulesYAML outputs the rules to yaml as a way to visualize changes.
func TestRulesYAML(t *testing.T) {
	datadriven.RunTest(t, testutils.TestDataPath(t, "rules"), func(t *testing.T, d *datadriven.TestData) string {
		if d.Cmd != "rules" {
			d.Fatalf(t, "rules is the only command")
		}
		out, err := yaml.Marshal(depRules)
		if err != nil {
			d.Fatalf(t, "failed to marshal: %v", err)
		}
		return string(out)
	})
}

func (r rule) MarshalYAML() (interface{}, error) {
	var query yaml.Node
	if err := query.Encode(r.q.Clauses()); err != nil {
		return nil, err
	}
	return &yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Value: "name"},
			{Kind: yaml.ScalarNode, Value: r.name},
			{Kind: yaml.ScalarNode, Value: "from"},
			{Kind: yaml.ScalarNode, Value: string(r.from)},
			{Kind: yaml.ScalarNode, Value: "to"},
			{Kind: yaml.ScalarNode, Value: string(r.to)},
			{Kind: yaml.ScalarNode, Value: "query"},
			&query,
		},
	}, nil
}
