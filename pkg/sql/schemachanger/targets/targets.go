package targets

import "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"

type ID int64

type TargetState struct {
	Target Target
	State  State
}

func (s TargetState) Transition(to State) TargetState {
	return TargetState{Target: s.Target, State: to}
}

type Target interface {
	ID() ID
}

type target struct {
	id ID
}

func (t target) ID() ID { return t.id }

type AddIndex struct {
	target
	TableID descpb.ID
	Index   descpb.IndexDescriptor

	PrimaryIndex   descpb.IndexID // primary index from which to backfill this index
	ReplacementFor descpb.IndexID
	Primary        bool
}

type DropIndex struct {
	target
	TableID descpb.ID
	IndexID descpb.IndexID

	ReplacedBy descpb.IndexID
	ColumnIDs  []descpb.ColumnID
}

type AddColumn struct {
	target
	TableID      descpb.ID
	ColumnFamily descpb.FamilyID
	Column       descpb.ColumnDescriptor
}

type DropColumn struct {
	target
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

type AddUniqueConstraint struct {
	target
	TableID   descpb.ID
	IndexID   descpb.IndexID
	ColumnIDs descpb.ColumnIDs
}

type DropUniqueConstraint struct {
	target
	TableID   descpb.ID
	IndexID   descpb.IndexID
	ColumnIDs descpb.ColumnIDs
}

type AddCheckConstraint struct {
	target
	TableID   descpb.ID
	Name      string
	Expr      string
	ColumnIDs descpb.ColumnIDs
}

type AddCheckConstraintUnvalidated struct {
	target
	TableID   descpb.ID
	Name      string
	Expr      string
	ColumnIDs descpb.ColumnIDs
}

type DropCheckConstraint struct {
	target
	TableID descpb.ID
	Name    string
}

// TODO: move this to some lower-level package
// TODO (lucy): I think we will want to make a distinction between internal
// schema changer states and states written to the descriptor (i.e., relevant
// for execution, which excludes the backfilled and validated states). Maybe
// we want different types.
type State int

//go:generate stringer --type State

const (
	StateAbsent State = iota
	StateDeleteOnly
	StateDeleteAndWriteOnly
	StateBackfilled
	StateValidated
	StatePublic
)
