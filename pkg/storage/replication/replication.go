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
	"time"

	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachlabs/acidlib/v1/connect"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// Question: does it matter that we've been aggressively focusing on having

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

type RaftMessage interface {
	connect.Message
	SetGroup(GroupID)
	SetRaftMessage(raftpb.Message)
}

// PeerFactory creates Peers and schedules their interactions between io and
// network resources.
type PeerFactory struct {
	config
	stopper   *stop.Stopper
	transport connect.Conn
	storage   engine.Engine

	scheduler scheduler

	liveness           func(GroupID) bool
	raftMessageFactory func() RaftMessage

	tickInterval time.Duration

	mu struct {
		syncutil.RWMutex
		peers      map[GroupID]*Peer
		unquiesced map[GroupID]struct{}
	}
}

func (f *PeerFactory) recvLoop() {
	for {
		msg := f.transport.Recv()
		switch msg := msg.(type) {
		case RaftMessage:
			// Enqueue the message for the peer if it exists

			// TODO(ajwerner): how do we deal with the case where the peer does not yet exist?
			// I think what we'll do is have the contract be that messages sent to peers which don't exist will just be dropped
			p := f.getPeer(msg.GroupID)
			if p == nil {
				// TODO(ajwerner): record a metric or something?
				continue
			}
			p.enqueueMessage(msg.Msg)
			f.scheduler.EnqueueRaftReady(msg.ID)
		case nil:
			return
		default:
			panic(fmt.Errorf("unexpected message type %T", msg))
		}
	}
}

func (f *PeerFactory) tickLoop() {
	ticker := time.NewTicker(s.cfg.RaftTickInterval)
	defer ticker.Stop()
	var groupsToTick []GroupID
	for {
		select {
		case <-ticker.C:
			// TODO(ajwerner): need a call to update the liveness map here because
			// it gets referenced when ticking the peers.
			groupsToTick = f.getUnquiescedGroups(groupsToTick[:0])
			f.scheduler.EnqueueRaftTick(groupsToTick...)
		case <-ticker.ShouldQuiesce():
			return
		}
	}
}

func (f *PeerFactory) getUnquiescedGroups(buf []GroupID) []GroupID {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for g := range f.mu.unquiesced {
		buf = append(buf, g)
	}
	return buf
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

	toProcess requestQueue

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
	MarkUnreachable(PeerID)
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

	syn sync.Locker
	p   *peer
}

// Send accepts ProposalMessages.
func (pc *PeerClient) Send(ctx context.Context, msg connect.Message) {
	m, ok := msg.(*ProposalMessage)
	if !ok {
		panic(fmt.Errorf("got %T, expected %T", msg, (*ProposalMessage)(nil)))
	}
	pc.syn.Unlock()
	defer pc.syn.Lock()
	pc.p.addProposal(ctx, pc, sync, pc, &proposal{
		ctx: ctx,
		m:   m,
		pc:  pc,
	})
}

type proposal struct {
	ctx             context.Context
	syn             sync.Locker
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
