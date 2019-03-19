package replication

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"go.etcd.io/etcd/raft/raftpb"
)

type RaftTruncatedState interface {
	SetIndex(uint64)
	GetIndex() uint64
	SetTerm(uint64)
	GetTerm() uint64
}

type StateLoader interface {
	LoadRaftTruncatedState(context.Context, engine.Reader) (_ RaftTruncatedState, isLegacy bool, _ error)
	SetRaftTruncatedState(context.Context, engine.ReadWriter, RaftTruncatedState) error
	LoadHardState(context.Context, engine.Reader) (raftpb.HardState, error)
	SetHardState(context.Context, engine.ReadWriter, raftpb.HardState) error
	SynthesizeRaftState(context.Context, engine.ReadWriter) error
	LoadLastIndex(ctx context.Context, reader engine.Reader) (uint64, error)
}
