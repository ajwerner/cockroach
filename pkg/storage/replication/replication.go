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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/connect"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/raftentry"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

// RaftMessage is a message used to send raft messages on the wire.
type RaftMessage interface {
	connect.Message
	GetGroupID() GroupID
	SetGroupID(GroupID)
	GetRaftMessage() raftpb.Message
	SetRaftMessage(raftpb.Message)
}

func NewPeerFactory(
	ctx context.Context,
	cfg base.RaftConfig,
	stopper *stop.Stopper,
	raftTransport connect.Conn,
	storage engine.Engine,
	raftMessageFactory func(GroupID) RaftMessage,
	c *raftentry.Cache,
	options ...Option,
) (*PeerFactory, error) {
	pf := PeerFactory{
		cfg:           cfg,
		stopper:       stopper,
		raftTransport: raftTransport,
		storage:       storage,
		entryCache:    c,
	}
	pf.mu.peers = make(map[GroupID]*Peer)
	pf.mu.unquiesced = make(map[GroupID]struct{})
	if err := pf.init(options...); err != nil {
		return nil, err
	}
	initScheduler(&pf.scheduler, &pf, pf.numWorkers)
	pf.scheduler.Start(ctx, pf.stopper)
	pf.stopper.RunAsyncTask(ctx, "replication.PeerFactory.tickLoop", pf.tickLoop)
	pf.stopper.RunAsyncTask(ctx, "replication.PeerFactory.recvLoop", pf.recvLoop)
	return &pf, nil
}

// PeerFactory creates Peers and schedules their interactions between io and
// network resources.
type PeerFactory struct {
	config
	cfg           base.RaftConfig
	stopper       *stop.Stopper
	raftTransport connect.Conn
	storage       engine.Engine
	entryCache    *raftentry.Cache

	scheduler scheduler

	liveness           func(GroupID, PeerID) bool
	raftMessageFactory func() RaftMessage
	onUnquiesce        func(g GroupID)
	entries            func()

	tickInterval time.Duration

	mu struct {
		syncutil.RWMutex
		peers      map[GroupID]*Peer
		unquiesced map[GroupID]struct{}
	}
}

func (pf *PeerFactory) processReady(ctx context.Context, id GroupID) {
	if log.V(2) {
		log.Infof(ctx, "processing ready for %d", id)
	}
	pf.mu.RLock()
	p, ok := pf.mu.peers[id]
	pf.mu.RUnlock()
	if !ok {
		return
	}
	ctx = p.logCtx.AnnotateCtx(ctx)
	p.handleRaftReady(ctx)
}

func (pf *PeerFactory) processRequestQueue(ctx context.Context, id GroupID) bool {
	pf.mu.RLock()
	p, ok := pf.mu.peers[id]
	pf.mu.RUnlock()
	if !ok {
		return false
	}
	exists, err := p.tick(ctx, func(peerID PeerID) bool {
		return pf.liveness(id, peerID)
	})
	if err != nil {
		log.Error(ctx, err)
	}
	return exists
}

// Process a raft tick for the specified range. Return true if the range
// should be queued for ready processing.
func (p *PeerFactory) processTick(context.Context, GroupID) bool {
	panic("not implemented")
}

func (f *PeerFactory) getPeer(id GroupID) *Peer {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mu.peers[id]
}

func (f *PeerFactory) recvLoop(ctx context.Context) {
	for {
		msg := f.raftTransport.Recv()
		switch msg := msg.(type) {
		case RaftMessage:
			// Enqueue the message for the peer if it exists

			// TODO(ajwerner): how do we deal with the case where the peer does not yet exist?
			// I think what we'll do is have the contract be that messages sent to peers which don't exist will just be dropped
			groupID := msg.GetGroupID()
			if groupID == 0 {
				panic("There shouldn't ever be a zero group ID")
			}
			p := f.getPeer(groupID)
			if p == nil {
				// TODO(ajwerner): record a metric or something?
				continue
			}
			raftMsg := msg.GetRaftMessage()
			p.enqueueRaftMessage(raftMsg)
			f.scheduler.EnqueueRaftReady(groupID)
		case nil:
			return
		default:
			panic(fmt.Errorf("unexpected message type %T", msg))
		}
	}
}

func (f *PeerFactory) tickLoop(ctx context.Context) {
	ticker := time.NewTicker(f.cfg.RaftTickInterval)
	defer f.raftTransport.Close(ctx, true)
	defer ticker.Stop()
	var groupsToTick []GroupID
	for {
		select {
		case <-ticker.C:
			// TODO(ajwerner): need a call to update the liveness map here because
			// it gets referenced when ticking the peers.
			groupsToTick = f.getUnquiescedGroups(groupsToTick[:0])
			f.scheduler.EnqueueRaftTick(groupsToTick...)
		case <-f.stopper.ShouldQuiesce():
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

func (f *PeerFactory) Destroy(p *Peer) {
	panic("not implemented")
}

func (f *PeerFactory) NewPeer(logCtx log.AmbientContext, id GroupID) (*Peer, error) {
	// TODO: actually implement
	// We start out without a peer ID.
	// We'll get one when we start receiving raft messages hopefully.
	f.mu.RLock()
	p, exists := f.mu.peers[id]
	f.mu.RUnlock()
	if exists {
		return p, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if p, exists = f.mu.peers[id]; exists {
		return p, nil
	}
	p = &Peer{
		pf:      f,
		groupID: id,
		logCtx:  logCtx,
		raftMessageFactory: func() RaftMessage {
			m := f.raftMessageFactory()
			m.SetGroupID(id)
			return m
		},
		onUnquiesce: func() { f.onUnquiesce(id) },
	}
	f.mu.peers[id] = p
	f.mu.unquiesced[id] = struct{}{}
	return p, nil
}

func (f *PeerFactory) LoadPeer(logCtx log.AmbientContext, id GroupID) (*Peer, error) {
	panic("not implemented")
}

var _ peerIface = (*Peer)(nil)

// peerIface exists merely to clarify the peer's interface and is not expected
// to be used in any way.
type peerIface interface {
	NewClient(sync.Locker) *PeerClient
	// MarkUnreachable(PeerID)
	Progress() Progress
	// Snapshot()      // ???
	// ApplySnapshot() // ???
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
		// command. An error from DoApply is fatal.
		Apply func(engine.Writer) error

		// OnApplied is called when the application of the command has been written
		// to storage.
		Applied func()
	}

	commitErr error
	cond      sync.Cond
	syn       sync.Locker
	peer      *Peer
}

// Send accepts ProposalMessages.
func (pc *PeerClient) Send(ctx context.Context, msg connect.Message) {
	m, ok := msg.(*ProposalMessage)
	if !ok {
		panic(fmt.Errorf("got %T, expected %T", msg, (*ProposalMessage)(nil)))
	}
	pc.peer.addProposal(&proposal{
		ctx: ctx,
		syn: pc.syn,
		msg: m,
		pc:  pc,
	})
}

// TODO(ajwerner): consider cancelling the proposal if possible.
func (pc *PeerClient) Close(ctx context.Context, drain bool) {

}

type CommittedMessage struct {
}

type ErrorMessage struct {
	Err error
}

func (pc *PeerClient) Recv() connect.Message {
	pc.cond.Wait()
	if pc.commitErr != nil {
		return &ErrorMessage{Err: pc.commitErr}
	}
	return CommittedMessage{}
}

var _ connect.Conn = (*PeerClient)(nil)

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
