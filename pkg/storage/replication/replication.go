// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package replication

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachlabs/acidlib/v1/connect"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// GroupID is the basic unit of replication.
// Each peer corresponds to a single ID.
type GroupID int64

// PeerID is a replica id :/
type PeerID int64

// ProposalMessage is used to propose commands to the system.
type ProposalMessage struct {
	ID      storagebase.CmdIDKey
	Command *storagepb.RaftCommand
}

type RaftMessage struct {
	GroupID GroupID
	Msg     raftpb.Message
}

type TransportFactory interface {
	NewClient() connect.Conn
}

// PeerFactory creates Peers and schedules their interactions between io and
// network resources.
type PeerFactory struct {
	stopper   *stop.Stopper
	transport TransportFactory
	storage   engine.Engine

	// requestQueues stores the current requests
	// we're going to have a goroutine waiting to receive messages and add them
	// to the queues. Maybe these queues should live on the peer objects
	// TODO(ajwerner): revisit whether this is the right place for the queues
	requestQueues requestQueues

	mu struct {
		syncutil.RWMutex
		peers map[GroupID]*Peer
	}
}

// Random thoughts:

// Merges and splits don't really happen here at all right?
// Like sure there will need to be some initialization stuff but for the most
// part that gets orchestrated above.
// In splits a new peer gets added with the new GroupID
// In merges a peer gets destroyed.
// All good.

// Peer represents local replication state for a replica group.
type Peer struct {

	// does this need to be protected by the mutex for lazy initialization?
	*raft.RawNode

	// Do we need to think about quiescence?
	// Seems like yes for sure.
	// TODO: sprinkle some state for that
	quiescent bool

	// StateLoader?
	// RaftTransport?
	mu struct {
		syncutil.RWMutex

		proposals map[storagebase.CmdIDKey]*proposal
	}
}

func (f *PeerFactory) Destroy(p *Peer) {
	panic("not implemented")
}

func (f *PeerFactory) NewPeer(id GroupID) (*Peer, error) {
	panic("not implemented")
}

func (f *PeerFactory) LoadPeer(id GroupID) (*Peer, error) {
	panic("not implemented")
}

var _ peerIface = (*Peer)(nil)

// peerIface exists merely to clarify the peer's interface and is not expected
// to be used in any way.
type peerIface interface {
	NewClient(sync.Locker) *PeerClient
	Progress() Progress
	Snapshot()      // ???
	ApplySnapshot() // ???
}

// PeerClient is a connect.Conn that implements proposals.
type PeerClient struct {

	// Callbacks are hooks into the proposal lifecycle.
	// Expect one callback per message send.
	// TODO(ajwerner): Is it the case that they'll be called in order of the
	// messages which are sent? Is there enforcement that a client only ever
	// sends one message? Do we need to decorate the signatures?

	Callbacks struct {
		// DoCommit is a callback which occurs to allow the command to validate that
		// a commit can proceed given the current state. If an error is returned it
		// propagate back to the client through a message send. An error prevents
		// application of the command.
		Commit func() error

		// DoApply is a callback which the client can set to do the work of applying a
		// command. An error from DoApply
		Apply func(engine.Writer) error

		// OnApplied is called when the application of the command has been written
		// to storage.
		Applied func()
	}

	mu struct {
		syncutil.RWMutex
		p *peer
	}
}

// Send accepts ProposalMessages.
func (pc *PeerClient) Send(ctx context.Context, msg connect.Message) {
	m, ok := msg.(*ProposalMessage)
	if !ok {
		panic(fmt.Errorf("got %T, expected %T", msg, (*ProposalMessage)(nil)))
	}
	pc.p.addProposal(&proposal{
		ctx: ctx,
		m:   m,
		pc:  pc,
	})
}

type proposal struct {
	ctx             context.Context
	pc              *PeerClient
	msg             *ProposalMessage
	proposedAtTicks int
}

// Progress represents the local view of state of replication for the replica
// group.
type Progress interface {
	AppliedIndex() uint64
	LeaseSequence() int64
	LeaseAppliedIndex() int64
}
