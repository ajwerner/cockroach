package compiler

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/ops"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// TODO(ajwerner): There are some ordering requirements between ops in
// the same stage. Make sure to deal with that. In particular, we need
// to move public things out before we can move non-public things in.
// There's some hackery but it's just that.

type Stage struct {
	Ops         []ops.Op
	NextTargets []targets.TargetState
}

type ExecutionPhase int

const (
	PostStatementPhase ExecutionPhase = iota
	PreCommitPhase
	PostCommitPhase
)

type CompileFlags struct {
	ExecutionPhase       ExecutionPhase
	CreatedDescriptorIDs descIDSet
}

type descIDSet struct {
	set util.FastIntSet
}

func makeDescIDSet(ids ...descpb.ID) descIDSet {
	s := descIDSet{}
	for _, id := range ids {
		s.add(id)
	}
	return s
}

func (d *descIDSet) add(id descpb.ID) {
	d.set.Add(int(id))
}

func (d *descIDSet) contains(id descpb.ID) bool {
	return d.set.Contains(int(id))
}

type opEdge struct {
	from, to *targets.TargetState
	op       ops.Op
}

func (o *opEdge) start() *targets.TargetState {
	return o.from
}

func (o *opEdge) end() *targets.TargetState {
	return o.to
}

var _ edge = (*opEdge)(nil)
var _ edge = (*depEdge)(nil)

// depEdge represents a dependency of a TargetState to another.
// A dependency implies that the from TargetState must be reached on or
// before the to TargetState.
//
// It is illegal for from and to to refer to the same target.
type depEdge struct {
	from, to *targets.TargetState
}

func (d depEdge) start() *targets.TargetState {
	return d.from
}

func (d depEdge) end() *targets.TargetState {
	return d.to
}

type edge interface {
	start() *targets.TargetState
	end() *targets.TargetState
}

type SchemaChange struct {
	flags               CompileFlags
	initialTargetStates []*targets.TargetState

	targets      []targets.Target
	targetStates []map[targets.State]*targets.TargetState
	targetIdxMap map[targets.Target]int

	// targetStateOpEdges maps a TargetState to an opEdge that proceeds
	// from it. A targetState may have at most one opEdge from it.
	targetStateOpEdges map[*targets.TargetState]*opEdge

	// targetStateDepEdges maps a TargetState to its dependencies.
	// A targetState dependency is another target state which must be
	// reached before or concurrently with this targetState.
	targetStateDepEdges map[*targets.TargetState][]*depEdge

	edges []edge

	stages []stage
}

func (g *SchemaChange) getOrCreateTargetState(
	t targets.Target, s targets.State,
) *targets.TargetState {
	targetStates := g.getTargetStatesMap(t)
	if ts, ok := targetStates[s]; ok {
		return ts
	}
	ts := &targets.TargetState{
		Target: t,
		State:  s,
	}
	targetStates[s] = ts
	return ts
}

func (g *SchemaChange) getTargetStatesMap(t targets.Target) map[targets.State]*targets.TargetState {
	return g.targetStates[g.targetIdxMap[t]]
}

// DrawStageGraph returns a graphviz string of the stages of the compiled
// SchemaChange.
func (g *SchemaChange) DrawStageGraph() (string, error) {
	gv, err := g.drawStages()
	if err != nil {
		return "", err
	}
	return gv.String(), nil
}

// D returns a graphviz string of the stages of the compiled
// SchemaChange.
func (g *SchemaChange) DrawDepGraph() (string, error) {
	gv, err := g.drawDeps()
	if err != nil {
		return "", err
	}
	return gv.String(), nil
}

func (g *SchemaChange) containsTarget(target targets.Target) bool {
	_, exists := g.targetIdxMap[target]
	return exists
}

// addOpEdge adds an opEdge for the given target with the provided op.
// Returns the next state (for convenience).
func (g *SchemaChange) addOpEdge(
	t targets.Target, cur, next targets.State, op ops.Op,
) targets.State {
	oe := &opEdge{
		from: g.getOrCreateTargetState(t, cur),
		to:   g.getOrCreateTargetState(t, next),
		op:   op,
	}
	if existing, exists := g.targetStateOpEdges[oe.from]; exists {
		panic(errors.Errorf("duplicate outbound op edge %v and %v",
			oe, existing))
	}
	g.edges = append(g.edges, oe)
	g.targetStateOpEdges[oe.from] = oe
	return next
}

func (g *SchemaChange) addDepEdge(
	fromTarget targets.Target,
	fromState targets.State,
	toTarget targets.Target,
	toState targets.State,
) {
	de := &depEdge{
		from: g.getOrCreateTargetState(fromTarget, fromState),
		to:   g.getOrCreateTargetState(toTarget, toState),
	}
	g.edges = append(g.edges, de)
	g.targetStateDepEdges[de.from] = append(g.targetStateDepEdges[de.from], de)
}

func buildGraph(initialStates []targets.TargetState, flags CompileFlags) (*SchemaChange, error) {
	g := SchemaChange{
		targetIdxMap:        map[targets.Target]int{},
		targetStateOpEdges:  map[*targets.TargetState]*opEdge{},
		targetStateDepEdges: map[*targets.TargetState][]*depEdge{},
	}

	// TODO: add validation of targets to ensure no two overlap in their
	// referenced elements.
	for _, ts := range initialStates {
		if existing, ok := g.targetIdxMap[ts.Target]; ok {
			return nil, errors.Errorf("invalid initial states contains duplicate target: %v and %v", ts, initialStates[existing])
		}
		idx := len(g.targets)
		g.targetIdxMap[ts.Target] = idx
		g.targets = append(g.targets, ts.Target)
		g.targetStates = append(g.targetStates, map[targets.State]*targets.TargetState{})
		g.initialTargetStates = append(g.initialTargetStates,
			g.getOrCreateTargetState(ts.Target, ts.State))
	}
	for _, ts := range initialStates {
		if err := generateOpEdges(&g, ts.Target, ts.State, flags); err != nil {
			return nil, err
		}
	}
	if err := generateDepEdges(&g); err != nil {
		return nil, err
	}
	if err := buildStages(&g, flags); err != nil {
		return nil, err
	}
	return &g, nil
}

type stage struct {
	ops  []ops.Op
	next []*targets.TargetState
}

func buildStages(g *SchemaChange, flags CompileFlags) error {
	// TODO(ajwerner): deal with the case where the target state was
	// fulfilled by something that preceded the initial state.
	fulfilled := map[*targets.TargetState]struct{}{}
	cur := g.initialTargetStates
	for {
		for _, ts := range cur {
			fulfilled[ts] = struct{}{}
		}
		// candidates is a map of ends of opEdges to their opEdge.
		var opEdges []*opEdge
		for _, t := range cur {
			// TODO(ajwerner): improve the efficiency of this lookup.
			// Look for an opEdge from this node. Then, for the other side
			// of the opEdge, look for dependencies.
			if oe := g.targetStateOpEdges[t]; oe != nil {
				opEdges = append(opEdges, oe)
			}
		}
		// Find out what opEdges we have on a per-type basis and then
		// figure out if we have the dependencies fulfilled.
		opTypes := make(map[ops.Type][]*opEdge)
		for _, oe := range opEdges {
			opTypes[oe.op.Type()] = append(opTypes[oe.op.Type()], oe)
		}

		// It's not clear that greedy is going to do it here but let's
		// assume that it will and press on. The reason it's valid is
		// that so long as we make progress, everything is okay.
		var didSomething bool
		for _, typ := range []ops.Type{
			ops.DescriptorMutationType,
			ops.BackfillType,
			ops.ValidationType,
		} {
			edges := opTypes[typ]
			for len(edges) > 0 {
				candidates := make(map[*targets.TargetState]struct{})
				for _, e := range edges {
					candidates[e.to] = struct{}{}
				}
				// See if we can apply all of them. Otherwise what? remove all
				// that cannot be applied.
				failed := map[*opEdge]struct{}{}
				for _, e := range edges {
					for _, d := range g.targetStateDepEdges[e.to] {
						if _, ok := fulfilled[d.to]; ok {
							continue
						}
						if _, ok := candidates[d.to]; ok {
							continue
						}
						failed[e] = struct{}{}
						break
					}
				}
				if len(failed) == 0 {
					break
				}
				truncated := edges[:0]
				for _, e := range edges {
					if _, found := failed[e]; !found {
						truncated = append(truncated, e)
					}
				}
				edges = truncated
			}
			if len(edges) == 0 {
				continue
			}
			next := append(cur[:0:0], cur...)
			var s stage
			for i, ts := range cur {
				for _, e := range edges {
					if e.from == ts {
						next[i] = e.to
						s.ops = append(s.ops, e.op)
						break
					}
				}
			}
			// TODO(ajwerner): Make this way better
			sort.Slice(s.ops, func(i, j int) bool {
				ii, iOk := s.ops[i].(ops.IndexDescriptorStateChange)
				jj, jOk := s.ops[j].(ops.IndexDescriptorStateChange)
				if iOk && !jOk {
					return true
				}
				if jOk && !iOk {
					return false
				}
				if !iOk && !jOk {
					return false
				}
				return ii.State > jj.State
			})
			s.next = next
			g.stages = append(g.stages, s)
			cur = next
			didSomething = true
			break
			// For each edge, we want to see if the
		}
		if !didSomething {
			break
		}
	}
	return nil
}

type CompiledSchemaChange struct {
}

func Compile(t []targets.TargetState, flags CompileFlags) (*SchemaChange, error) {
	// We want to create a sequence of TargetStates and ops along the edges.

	// We'll start with a process of producing a graph of edges.
	// We'll also want to create a dependency graph.
	// Then we'll fill it down.

	// We want to walk the states and add edges to the current stage
	// so long as they have their dependencies met or can have all of
	// their dependencies met.
	return buildGraph(t, flags)
}

func (g *SchemaChange) Stages() []Stage {
	ret := make([]Stage, 0, len(g.stages))
	for i := range g.stages {
		ret = append(ret, Stage{
			Ops: g.stages[i].ops,
			NextTargets: func() (next []targets.TargetState) {
				next = make([]targets.TargetState, len(g.stages[i].next))
				for i, ts := range g.stages[i].next {
					next[i] = *ts
				}
				return next
			}(),
		})
	}
	return ret
}

func generateDepEdges(g *SchemaChange) error {
	// We want to generate the dependencies between target states.

	// TODO(ajwerner): refactor, this initial pass is incredibly imperative.
	// We want to iterate over the set of nodes and then iterate over the set of
	// targets which might be associated with those nodes and then add relevant
	// dep edges (we may need to synthesize nodes).
	for idx, ts := range g.targetStates {
		for s := range ts {
			if err := generateTargetStateDepEdges(g, g.targets[idx], s); err != nil {
				return err
			}
		}
	}
	return nil
}

// Now we need a way to talk about dependencies.
// * A column cannot be made public until all of the indexes using it are backfilled.
// * A column cannot be made public until all column constraints are public
// * A primary index cannot be made DeleteAndWriteOnly until another primary index
//   is in DeleteAndWriteOnly.
// *

func columnsContainsID(haystack []descpb.ColumnID, needle descpb.ColumnID) bool {
	for _, id := range haystack {
		if id == needle {
			return true
		}
	}
	return false
}
