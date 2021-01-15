// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scgraphviz

import (
	"encoding/json"
	"fmt"
	"html/template"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/emicklei/dot"
)

// DrawStageGraph returns a graphviz string of the stages of the Plan.
func DrawStages(p *scplan.Plan) (string, error) {
	gv, err := drawStages(p)
	if err != nil {
		return "", err
	}
	return gv.String(), nil
}

// DrawDepGraph returns a graphviz string of graph used to build the Plan.
func DrawDependencies(p *scplan.Plan) (string, error) {
	gv, err := drawDeps(p)
	if err != nil {
		return "", err
	}
	return gv.String(), nil
}

func drawStages(p *scplan.Plan) (*dot.Graph, error) {

	dg := dot.NewGraph()
	stagesSubgraph := dg.Subgraph("stages", dot.ClusterOption{})
	targetsSubgraph := stagesSubgraph.Subgraph("targets", dot.ClusterOption{})
	targetNodes := make(map[*scpb.Target]dot.Node, len(p.InitialNodes))
	for idx, n := range p.InitialNodes {
		t := n.Target
		tn := targetsSubgraph.Node(strconv.Itoa(idx))
		tn.Attr("label", htmlLabel(t))
		tn.Attr("fontsize", "9")
		tn.Attr("shape", "none")
		targetNodes[t] = tn
	}

	// Want to draw an edge to the initial target states with some dots
	// or something.
	curNodes := make([]dot.Node, len(p.InitialNodes))
	cur := p.InitialNodes
	for i, ts := range p.InitialNodes {
		label := targetStateID(i, ts.State)
		tsn := stagesSubgraph.Node(fmt.Sprintf("initial %d", i))
		tsn.Attr("label", label)
		tn := targetNodes[ts.Target]
		e := tn.Edge(tsn)
		e.Dashed()
		curNodes[i] = tsn
	}
	for id, st := range p.Stages {
		stage := fmt.Sprintf("stage %d", id)
		sg := stagesSubgraph.Subgraph(stage, dot.ClusterOption{})
		next := st.After
		nextNodes := make([]dot.Node, len(curNodes))
		for i, st := range next {
			cst := sg.Node(fmt.Sprintf("stage %d: %d", id, i))
			cst.Attr("label", targetStateID(i, st.State))
			if st != cur[i] {
				ge := curNodes[i].Edge(cst)
				oe, ok := p.Graph.GetOpEdgeFrom(cur[i])
				if ok {
					ge.Attr("label", htmlLabel(oe.Op()))
					ge.Attr("fontsize", "9")
				}
			} else {
				ge := curNodes[i].Edge(cst)
				ge.Dotted()
			}
			nextNodes[i] = cst
		}
		cur, curNodes = next, nextNodes
	}

	return dg, nil
}

func drawDeps(p *scplan.Plan) (*dot.Graph, error) {
	dg := dot.NewGraph()

	depsSubgraph := dg.Subgraph("deps", dot.ClusterOption{})
	targetsSubgraph := depsSubgraph.Subgraph("targets", dot.ClusterOption{})
	targetNodes := make(map[*scpb.Target]dot.Node, len(p.InitialNodes))
	targetIdxMap := make(map[*scpb.Target]int)
	for idx, n := range p.InitialNodes {
		t := n.Target
		tn := targetsSubgraph.Node(strconv.Itoa(idx))
		tn.Attr("label", htmlLabel(t))
		tn.Attr("fontsize", "9")
		tn.Attr("shape", "none")
		targetNodes[t] = tn
		targetIdxMap[t] = idx
	}

	tsNodes := make(map[*scpb.Node]dot.Node)
	_ = p.Graph.ForEachNode(func(n *scpb.Node) error {
		tsNodes[n] = depsSubgraph.Node(targetStateID(targetIdxMap[n.Target], n.State))
		return nil
	})

	// Want to draw an edge to the initial target states with some dots
	// or something.

	for _, ts := range p.InitialNodes {
		tsn := tsNodes[ts]
		tn := targetNodes[ts.Target]
		e := tn.Edge(tsn)
		e.Dashed()
	}

	_ = p.Graph.ForEachEdge(func(e scgraph.Edge) error {
		from := tsNodes[e.From()]
		to := tsNodes[e.To()]
		ge := from.Edge(to)
		switch e := e.(type) {
		case *scgraph.OpEdge:
			ge.Attr("label", htmlLabel(e.Op()))
			ge.Attr("fontsize", "9")
		case *scgraph.DepEdge:
			ge.Attr("color", "red")
		}
		return nil
	})
	return dg, nil
}

func targetStateID(targetID int, state scpb.State) string {
	return fmt.Sprintf("%d:%s", targetID, state)
}

func htmlLabel(o interface{}) dot.HTML {
	var buf strings.Builder
	if err := objectTemplate.Execute(&buf, o); err != nil {
		panic(err)
	}
	return dot.HTML(buf.String())
}

var objectTemplate = template.Must(template.New("obj").Funcs(template.FuncMap{
	"typeOf": func(v interface{}) string {
		return fmt.Sprintf("%T", v)
	},
	"toMap": func(v interface{}) (interface{}, error) {
		marshaled, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		var m map[string]interface{}
		if err := json.Unmarshal(marshaled, &m); err != nil {
			return nil, err
		}
		// Flatten objects up to one level to deal with embedded index/column
		// descriptors for now.
		flattenedMap := make(map[string]interface{})
		for k, v := range m {
			if obj, ok := v.(map[string]interface{}); ok {
				for objk, objv := range obj {
					flattenedMap[k+"."+objk] = objv
				}
			} else {
				flattenedMap[k] = v
			}
		}
		return flattenedMap, nil
	},
}).Parse(`
<table CELLSPACING="0" CELLBORDER="1" CELLPADDING="0" WIDTH="0">
  <tr>
    <td ALIGN="CENTER" COLSPAN="2" CELLSPACING="0" CELLPADDING="0">
    {{- typeOf . -}}
    </td>
  </tr>
{{ range $k, $v := (toMap .) -}}
  <tr>
    <td ALIGN="LEFT"> {{- $k -}} </td>
    <td> {{- $v -}} </td>
  </tr>
{{- end -}}
</table>
`))
