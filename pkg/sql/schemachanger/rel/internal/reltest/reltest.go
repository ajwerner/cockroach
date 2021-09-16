// Package reltest provides tools for testing the rel package.
package reltest

import (
	"flag"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

var rewrite bool

func init() {
	flag.BoolVar(&rewrite, "rewrite", false, "set to rewrite the test output")
}

type Suite struct {
	Name           string
	Schema         *rel.Schema
	Registry       *DataRegistry
	QueryTests     []DatabaseTest
	AttributeTests []AttributeTestCase
}

func (s Suite) encodeData(t *testing.T) *yaml.Node {
	encodeValue := func(name string) *yaml.Node {
		return s.Registry.valueToYAML(t, name)
	}
	n := yaml.Node{Kind: yaml.MappingNode}
	for _, name := range s.Registry.names {
		n.Content = append(n.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: name},
			encodeValue(name),
		)
	}
	return &n
}

func (s Suite) Run(t *testing.T) {
	for _, tc := range s.QueryTests {
		t.Run(tc.Name, func(t *testing.T) {
			tc.run(t, s.execContext())
		})
	}
	for _, tc := range s.AttributeTests {
		t.Run(tc.Name, func(t *testing.T) {
			tc.run(t, s)
		})
	}
	t.Run("yaml", func(t *testing.T) {
		s.writeYAML(t)

	})
}

func (s Suite) execContext() execContext {
	return execContext{
		DataRegistry: s.Registry,
		Schema:       s.Schema,
	}
}

func (s Suite) writeYAML(t *testing.T) {
	out, err := yaml.Marshal(s.toYAML(t))
	require.NoError(t, err)
	tdp := testutils.TestDataPath(t, strings.Replace(strings.TrimPrefix(t.Name(), "Test"), "/", "_", -1))
	if rewrite {
		require.NoError(t, ioutil.WriteFile(tdp, out, 0777))
	} else {
		exp, err := ioutil.ReadFile(tdp)
		require.NoError(t, err)
		require.Equal(t, exp, out)
	}
}

func (s Suite) toYAML(t *testing.T) *yaml.Node {
	return &yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			scalarYAML("name"),
			scalarYAML(s.Name),
			scalarYAML("data"),
			s.encodeData(t),
			scalarYAML("queries"),
			s.encodeQueries(t),
		},
	}
}

func (s Suite) encodeQueries(t *testing.T) *yaml.Node {
	queries := &yaml.Node{Kind: yaml.SequenceNode}
	for _, q := range s.QueryTests {
		queries.Content = append(queries.Content, q.encode(t, s.execContext()))
	}
	return queries
}

type execContext struct {
	*DataRegistry
	*rel.Schema
}

type registryYAMLMarshaler interface {
	marshalYAML(t *testing.T, r *DataRegistry, n *yaml.Node)
}

type DataRegistry struct {
	names       []string
	valueToName map[interface{}]string
	nameToValue map[string]interface{}

	// Some entities are initialized from yaml literals, preserve those
	// for formatting later as they are nicer.
	nameToYAML map[string]string
}

func NewDataRegistry() *DataRegistry {
	return &DataRegistry{
		valueToName: make(map[interface{}]string),
		nameToValue: make(map[string]interface{}),
		nameToYAML:  make(map[string]string),
	}
}

func (r *DataRegistry) Register(name string, v interface{}) interface{} {
	if existing, exists := r.nameToValue[name]; exists {
		panic(errors.AssertionFailedf(
			"entity with name %s already registered %v, trying to register %v",
			name, existing, v,
		))
	}
	r.names = append(r.names, name)
	r.nameToValue[name] = v
	r.valueToName[v] = name
	return v
}

func (r *DataRegistry) FromYAML(name, yamlData string, dest interface{}) interface{} {
	if err := yaml.Unmarshal([]byte(yamlData), dest); err != nil {
		panic(err)
	}
	r.Register(name, dest)
	r.nameToYAML[name] = yamlData
	return dest
}

func (r *DataRegistry) MustGetByName(t *testing.T, k string) interface{} {
	got, ok := r.nameToValue[k]
	require.Truef(t, ok, "MustGetByName(%s)", k)
	return got
}

func (r *DataRegistry) MustGetName(t *testing.T, v interface{}) string {
	got, ok := r.GetName(v)
	require.Truef(t, ok, "MustGetName(%v)", v)
	return got
}

func (r *DataRegistry) GetName(i interface{}) (string, bool) {
	got, ok := r.valueToName[i]
	return got, ok
}

func (r *DataRegistry) valueToYAML(t *testing.T, name string) *yaml.Node {
	if yamlStr, hasToYAML := r.nameToYAML[name]; hasToYAML {
		var v interface{}
		require.NoError(t, yaml.Unmarshal([]byte(yamlStr), &v))
		var n yaml.Node
		require.NoError(t, n.Encode(v))
		n.Style = yaml.FlowStyle
		return &n
	}
	return r.EncodeToYAML(t, r.MustGetByName(t, name))
}

type RegistryYAMLEncoder interface {
	EncodeToYAML(t *testing.T, r *DataRegistry) interface{}
}

func (r *DataRegistry) EncodeToYAML(t *testing.T, v interface{}) *yaml.Node {
	toEncode := v
	if encoder, ok := v.(RegistryYAMLEncoder); ok {
		toEncode = encoder.EncodeToYAML(t, r)
	}
	var n yaml.Node
	require.NoError(t, n.Encode(toEncode))
	return &n
}

type AttributeTestCase struct {
	Name     string
	Expected map[rel.Attribute]interface{}
}

func (c AttributeTestCase) run(t *testing.T, s Suite) {
	got := make(map[rel.Attribute]interface{})
	require.NoError(t, s.Schema.IterateAttributes(s.Registry.MustGetByName(t, c.Name), func(
		attribute rel.Attribute, value interface{},
	) error {
		got[attribute] = value
		return nil
	}))
	require.Equal(t, c.Expected, got)
}
