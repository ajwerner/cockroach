package compiler

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/ops"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
	"github.com/cockroachdb/errors"
)

// TODO(ajwerner): Deal with figuring out when we need to issue a GC job and for
// what.

// TODO(ajwerner): Deal with the flags to indicate whether things should be done
// in the current transaction.

func generateOpEdges(
	g *targetStateGraph, t targets.Target, s targets.State, flags CompileFlags,
) error {
	switch t := t.(type) {
	case *targets.AddColumn:
		return generateAddColumnOpEdges(g, t, s, flags)
	case *targets.AddCheckConstraint:
		return generateAddCheckConstraintOpEdges(g, t, s, flags)
	case *targets.DropIndex:
		return generateDropIndexOpEdges(g, t, s, flags)
	case *targets.AddIndex:
		return generateAddIndexOpEdges(g, t, s, flags)
	case *targets.DropColumn:
		return generateDropColumnOpEdges(g, t, s, flags)
	default:
		return errors.AssertionFailedf("generateOpEdges not implemented for %T", t)
	}
}

func generateDropColumnOpEdges(
	g *targetStateGraph, t *targets.DropColumn, s targets.State, flags CompileFlags,
) error {
	for {
		switch s {
		case targets.StatePublic:
			s = g.addOpEdge(t, s,
				targets.StateDeleteAndWriteOnly,
				ops.ColumnDescriptorStateChange{
					TableID:   t.TableID,
					ColumnID:  t.ColumnID,
					State:     s,
					NextState: targets.StateDeleteAndWriteOnly,
				})
		case targets.StateDeleteAndWriteOnly:
			if !flags.CreatedDescriptorIDs.contains(t.TableID) &&
				(flags.ExecutionPhase == PostStatementPhase ||
					flags.ExecutionPhase == PreCommitPhase) {
				return nil
			}
			s = g.addOpEdge(t, s,
				targets.StateDeleteOnly,
				ops.ColumnDescriptorStateChange{
					TableID:   t.TableID,
					ColumnID:  t.ColumnID,
					State:     s,
					NextState: targets.StateDeleteOnly,
				})
		case targets.StateDeleteOnly:
			s = g.addOpEdge(t, s,
				targets.StateAbsent,
				ops.ColumnDescriptorStateChange{
					TableID:   t.TableID,
					ColumnID:  t.ColumnID,
					State:     s,
					NextState: targets.StateAbsent,
				})
		case targets.StateAbsent:
			return nil
		default:
			return errors.AssertionFailedf("unexpected state %s for %T", s, t)
		}
	}
}

func generateAddIndexOpEdges(
	g *targetStateGraph, t *targets.AddIndex, s targets.State, flags CompileFlags,
) error {
	for {
		switch s {
		case targets.StateAbsent:
			if !flags.CreatedDescriptorIDs.contains(t.TableID) &&
				flags.ExecutionPhase == PostStatementPhase {
				return nil
			}
			s = g.addOpEdge(t, s,
				targets.StateDeleteOnly,
				ops.AddIndexDescriptor{
					TableID: t.TableID,
					Index:   t.Index,
					Primary: t.Primary,
				})
		case targets.StateDeleteOnly:
			if !flags.CreatedDescriptorIDs.contains(t.TableID) &&
				flags.ExecutionPhase == PreCommitPhase {
				return nil
			}
			s = g.addOpEdge(t, s,
				targets.StateDeleteAndWriteOnly,
				ops.IndexDescriptorStateChange{
					TableID:   t.TableID,
					IndexID:   t.Index.ID,
					IsPrimary: t.Primary,
					State:     s,
					NextState: targets.StateDeleteAndWriteOnly,
				})
		case targets.StateDeleteAndWriteOnly:
			// TODO(ajwerner): In the case of a primary index swap, we only need to
			// validate if the columns being used did not previously contain a unique
			// and NOT NULL constraints.
			var next targets.State
			if !t.Index.Unique {
				next = targets.StateValidated
			} else {
				next = targets.StateBackfilled
			}
			s = g.addOpEdge(t, s, next, ops.IndexBackfill{
				TableID: t.TableID,
				IndexID: t.Index.ID,
			})
		case targets.StateBackfilled:
			s = g.addOpEdge(t, s,
				targets.StateValidated,
				ops.UniqueIndexValidation{
					TableID:        t.TableID,
					PrimaryIndexID: t.PrimaryIndex,
					IndexID:        t.Index.ID,
				})
		case targets.StateValidated:
			s = g.addOpEdge(t, s,
				targets.StatePublic,
				ops.IndexDescriptorStateChange{
					TableID:   t.TableID,
					IndexID:   t.Index.ID,
					IsPrimary: t.Primary,
					State:     s,
					NextState: targets.StatePublic,
				})
		case targets.StatePublic:
			return nil
		default:
			return errors.AssertionFailedf("unexpected state %s for %T", s, t)
		}
	}
}

func generateDropIndexOpEdges(
	g *targetStateGraph, t *targets.DropIndex, s targets.State, flags CompileFlags,
) error {
	for {
		switch s {
		case targets.StatePublic:
			if !flags.CreatedDescriptorIDs.contains(t.TableID) &&
				flags.ExecutionPhase == PostStatementPhase {
				return nil
			}
			s = g.addOpEdge(t, s,
				targets.StateDeleteAndWriteOnly,
				ops.IndexDescriptorStateChange{
					TableID:   t.TableID,
					IndexID:   t.IndexID,
					IsPrimary: t.ReplacedBy != 0,
					State:     s,
					NextState: targets.StateDeleteAndWriteOnly,
				})
		case targets.StateDeleteAndWriteOnly:
			if !flags.CreatedDescriptorIDs.contains(t.TableID) &&
				flags.ExecutionPhase == PreCommitPhase {
				return nil
			}
			s = g.addOpEdge(t, s,
				targets.StateDeleteOnly,
				ops.IndexDescriptorStateChange{
					TableID:   t.TableID,
					IndexID:   t.IndexID,
					IsPrimary: t.ReplacedBy != 0,
					State:     s,
					NextState: targets.StateDeleteOnly,
				})
		case targets.StateDeleteOnly:
			s = g.addOpEdge(t, s,
				targets.StateAbsent,
				ops.IndexDescriptorStateChange{
					TableID:   t.TableID,
					IndexID:   t.IndexID,
					IsPrimary: t.ReplacedBy != 0,
					State:     s,
					NextState: targets.StateAbsent,
				})
		case targets.StateAbsent:
			return nil
		default:
			return errors.AssertionFailedf("unexpected state %s for %T", s, t)
		}
	}
}

func generateAddCheckConstraintOpEdges(
	g *targetStateGraph, t *targets.AddCheckConstraint, s targets.State, flags CompileFlags,
) error {
	panic("unimplemented")
}

func generateAddColumnOpEdges(
	g *targetStateGraph, t *targets.AddColumn, s targets.State, flags CompileFlags,
) (_ error) {
	for {
		switch s {
		case targets.StateAbsent:
			if !flags.CreatedDescriptorIDs.contains(t.TableID) &&
				flags.ExecutionPhase == PostStatementPhase {
				return nil
			}
			s = g.addOpEdge(t, s,
				targets.StateDeleteOnly,
				ops.AddColumnDescriptor{
					TableID: t.TableID,
					Column:  t.Column,
				})
		case targets.StateDeleteOnly:
			if !flags.CreatedDescriptorIDs.contains(t.TableID) &&
				flags.ExecutionPhase == PreCommitPhase {
				return nil
			}
			s = g.addOpEdge(t, s,
				targets.StateDeleteAndWriteOnly,
				ops.ColumnDescriptorStateChange{
					TableID:   t.TableID,
					ColumnID:  t.Column.ID,
					State:     s,
					NextState: targets.StateDeleteAndWriteOnly,
				})
		case targets.StateDeleteAndWriteOnly:
			s = g.addOpEdge(t, s,
				targets.StatePublic,
				ops.ColumnDescriptorStateChange{
					TableID:   t.TableID,
					ColumnID:  t.Column.ID,
					State:     s,
					NextState: targets.StatePublic,
				})
		case targets.StatePublic:
			return
		default:
			return errors.AssertionFailedf("unexpected state %s for %T", s, t)
		}
	}
}
