package scpb

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

// NumStates is the number of values which State may take on.
var NumStates = len(State_name)

// Node represents a Target in a given state.
type Node struct {
	Target *Target
	State  State
}

// Element returns the target's element.
func (n *Node) Element() Element {
	return n.Target.Element()
}

type Element interface {
	proto.Message
	DescriptorID() descpb.ID
}

func (e *ElementProto) Element() Element {
	return e.GetValue().(Element)
}

// NewElement constructs a new Element. The passed elem must be one of the
// oneOf members of Element. If not, this call will panic.
func NewTarget(dir Target_Direction, elem Element) *Target {
	t := Target{
		Direction: dir,
	}
	if !t.SetValue(elem) {
		panic(errors.Errorf("unknown element type %T", elem))
	}
	return &t
}

func (e *Column) DescriptorID() descpb.ID { return e.TableID }

func (e *PrimaryIndex) DescriptorID() descpb.ID { return e.TableID }

func (e *SecondaryIndex) DescriptorID() descpb.ID { return e.TableID }

func (e *SequenceDependency) DescriptorID() descpb.ID { return e.SequenceID }

func (e *UniqueConstraint) DescriptorID() descpb.ID { return e.TableID }

func (e *CheckConstraint) DescriptorID() descpb.ID { return e.TableID }
