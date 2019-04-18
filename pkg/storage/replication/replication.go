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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/connect"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// Open Question: where does raft hard state and truncated state belong?
// With the log or with the rest of the data? The truncated state likely
// belongs with the rest of the data in the primary storage engine and is
// unreplicated starting in 19.2. The HardState likely belongs with the
// raft log.

// RaftStorage is used to store on-disk state for raft.
type RaftStorage interface {
	raft.Storage

	Append(
		ctx context.Context, batch engine.ReadWriter, entries []raftpb.Entry,
	) (onCommit func() error, err error)

	SetHardState(ctx context.Context, batch engine.ReadWriter, hs raftpb.HardState) error

	LogSize() (size int64, trusted bool)
}

// GroupID is the basic unit of replication.
// Each peer corresponds to a single ID.
type GroupID = uint64

// PeerID is a ReplicaID.
// TODO(ajwerner): I am on the fence as to whether the mixed terminology
// helps or hurts.
type PeerID = uint64

// TODO(ajwerner): should these process functions take an engine?
// My sense is no. A ReadWriter doesn't work because we already assume all over
// the place that we can create batches whenever we want. Maybe we assume this
// function is going to have access to storage on its own.

// ProcessCommandFunc is used to handle committed raft commands.
type ProcessCommandFunc func(
	ctx context.Context,
	term, index uint64,
	id storagebase.CmdIDKey,
	prop ProposalMessage,
)

// ProcessConfChangeFunc is used to handle committed raft config change commands.
type ProcessConfChangeFunc func(
	ctx context.Context,
	term, index uint64,
	id storagebase.CmdIDKey,
	confChange ConfChangeMessage,
) (confChanged bool)

// RaftMessage is a message used to send raft messages on the wire.
// RaftMessage is created by a client provided factory function and the concrete
// type is left up to the client.
type RaftMessage interface {
	connect.Message
	GetGroupID() GroupID
	GetRaftMessage() raftpb.Message
}

// TestingKnobs are used for testing.
type TestingKnobs struct {
	// DisableRefreshReasonNewLeader disables refreshing pending commands when a new
	// leader is discovered.
	DisableRefreshReasonNewLeader bool
	// DisableRefreshReasonNewLeaderOrConfigChange disables refreshing pending
	// commands when a new leader is discovered or when a config change is
	// dropped.
	DisableRefreshReasonNewLeaderOrConfigChange bool
	// DisableRefreshReasonTicks disables refreshing pending commands when a
	// snapshot is applied.
	DisableRefreshReasonSnapshotApplied bool
	// DisableRefreshReasonTicks disables refreshing pending commands
	// periodically.
	DisableRefreshReasonTicks bool
}

// FactoryConfig contains the for a Factory.
type FactoryConfig struct {

	// TODO(ajwerner): should there be a start method that takes a stopper?
	Stopper *stop.Stopper

	// Storage for all of the peers.
	Storage engine.Engine

	// Settings are the cluster settings.
	Settings *cluster.Settings

	// NumWorkers is the number of worker goroutines which will be used to handle
	// events on behalf of peers.
	NumWorkers int

	// RaftConfig configures etcd raft.
	RaftConfig base.RaftConfig

	// RaftTransport is the message bus on which RaftMessages are sent and
	// received.
	RaftTransport connect.Conn

	TestingKnobs TestingKnobs
}

func NewFactory(ctx context.Context, cfg FactoryConfig) (*Factory, error) {
	pf := Factory{
		cfg: cfg,
	}
	pf.mu.peers = make(map[GroupID]*Peer)
	pf.mu.unquiesced = make(map[GroupID]struct{})
	initScheduler(&pf.scheduler, &pf, cfg.NumWorkers)
	pf.scheduler.Start(ctx, pf.cfg.Stopper)
	if err := pf.cfg.Stopper.RunAsyncTask(
		ctx, "replication.Factory.tickLoop", pf.tickLoop,
	); err != nil {
		panic(errors.Wrap(err, "failed to start tick loop"))
	}
	if err := pf.cfg.Stopper.RunAsyncTask(
		ctx, "replication.Factory.recvLoop", pf.recvLoop,
	); err != nil {
		panic(errors.Wrap(err, "failed to start recv loop"))
	}
	if err := pf.cfg.Stopper.RunAsyncTask(
		ctx, "shutdown waiter", func(ctx context.Context) {
			<-pf.cfg.Stopper.ShouldQuiesce()
			pf.cfg.RaftTransport.Close(ctx, false)
		}); err != nil {
		panic(errors.Wrap(err, "failed to start shutdown waiter"))
	}
	return &pf, nil
}

// Factory creates Peers and schedules their interactions between io and
// network resources.
type Factory struct {
	cfg FactoryConfig

	scheduler scheduler

	liveness    func(GroupID, PeerID) bool
	onUnquiesce func(g GroupID)

	mu struct {
		syncutil.RWMutex
		peers      map[GroupID]*Peer
		unquiesced map[GroupID]struct{}
	}
}

func (pf *Factory) processReady(ctx context.Context, id GroupID) {
	if log.V(2) {
		log.Infof(ctx, "processing ready for %d", id)
	}
	pf.mu.RLock()
	p, ok := pf.mu.peers[id]
	pf.mu.RUnlock()
	if !ok {
		return
	}
	ctx = p.AnnotateCtx(ctx)
	p.handleRaftReady(ctx)
}

func (pf *Factory) processRequestQueue(ctx context.Context, id GroupID) bool {
	if log.V(2) {
		log.Infof(ctx, "processing request queue %v", id)
	}
	pf.mu.RLock()
	p, ok := pf.mu.peers[id]
	pf.mu.RUnlock()
	// TODO(ajwerner): think harder about this locking
	if !ok {
		return false
	}
	p.msgQueueMu.Lock()
	msgs := p.msgQueueMu.msgQueue
	p.msgQueueMu.msgQueue = msgs[len(msgs)-1:]
	p.msgQueueMu.Unlock()

	// TODO(ajwerner): deal with error handling
	// TODO(ajwerner): deal with life cycle weirdness
	for _, msg := range msgs {
		if err := p.withRaftGroup(false, func(raftGroup *raft.RawNode) (bool, error) {
			// We're processing a message from another replica which means that the
			// other replica is not quiesced, so we don't need to wake the leader.
			// Note that we avoid campaigning when receiving raft messages, because
			// we expect the originator to campaign instead.
			err := raftGroup.Step(msg)
			if err == raft.ErrProposalDropped {
				// A proposal was forwarded to this replica but we couldn't propose it.
				// Swallow the error since we don't have an effective way of signaling
				// this to the sender.
				// TODO(bdarnell): Handle ErrProposalDropped better.
				// https://github.com/cockroachdb/cockroach/issues/21849
				err = nil
			}
			return false /* unquiesceAndWakeLeader */, err
		}); err != nil {
			panic(errors.Wrap(err, "failed to step my messages"))
		}
	}

	if _, expl, err := p.handleRaftReady(ctx); err != nil {
		fatalOnRaftReadyErr(ctx, expl, err)
	}
	return true
}

func fatalOnRaftReadyErr(ctx context.Context, expl string, err error) {
	// Mimic the behavior in processRaft.
	log.Fatalf(ctx, "%s: %s", log.Safe(expl), err) // TODO(bdarnell)
}

// Process a raft tick for the specified range. Return true if the range
// should be queued for ready processing.
func (f *Factory) processTick(ctx context.Context, gid GroupID) bool {
	p := f.getPeer(gid)
	if p == nil {
		return false
	}
	// TODO(ajwerner): add metrics on tick processing
	exists, err := p.tick(ctx, nil)
	if err != nil {
		log.Error(ctx, err)
	}
	return exists
}

func (f *Factory) getPeer(id GroupID) *Peer {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mu.peers[id]
}

func (f *Factory) recvLoop(ctx context.Context) {
	for {
		msg := f.cfg.RaftTransport.Recv()
		switch msg := msg.(type) {
		case RaftMessage:
			// Enqueue the message for the peer if it exists

			// TODO(ajwerner): how do we deal with the case where the peer does not yet exist?
			// I think what we'll do is have the contract be that messages sent to peers which don't exist will just be dropped
			groupID := msg.GetGroupID()
			if groupID == 0 {
				panic(errors.Errorf("There shouldn't ever be a zero group ID: %v", msg))
			}
			p := f.getPeer(groupID)
			if p == nil {
				// TODO(ajwerner): record a metric or something?
				continue
			}
			raftMsg := msg.GetRaftMessage()
			p.enqueueRaftMessage(raftMsg)
			f.scheduler.EnqueueRaftRequest(groupID)
		case nil:
			return
		default:
			panic(fmt.Errorf("unexpected message type %T", msg))
		}
	}
}

func (f *Factory) tickLoop(ctx context.Context) {
	ticker := time.NewTicker(f.cfg.RaftConfig.RaftTickInterval)
	defer f.cfg.RaftTransport.Close(ctx, true)
	defer ticker.Stop()
	var groupsToTick []GroupID
	for {
		select {
		case <-ticker.C:
			// TODO(ajwerner): need a call to update the liveness map here because
			// it gets referenced when ticking the peers.
			groupsToTick = f.getUnquiescedGroups(groupsToTick[:0])
			f.scheduler.EnqueueRaftTick(groupsToTick...)
		case <-f.cfg.Stopper.ShouldQuiesce():
			return
		}
	}
}

func (f *Factory) getUnquiescedGroups(buf []GroupID) []GroupID {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for g := range f.mu.unquiesced {
		buf = append(buf, g)
	}
	return buf
}

// enqueueRaftUpdateCheck asynchronously registers the given range ID to be
// checked for raft updates when the processRaft goroutine is idle.
func (s *Factory) enqueueRaftUpdateCheck(groupID GroupID) {
	s.scheduler.EnqueueRaftReady(groupID)
}

// Random thoughts:

// Merges and splits don't really happen here at all right?
// Like sure there will need to be some initialization stuff but for the most
// part that gets orchestrated above.
// In splits a new peer gets added with the new GroupID
// In merges a peer gets destroyed.
// All good.

func (f *Factory) Destroy(p *Peer) {
	panic("not implemented")
}

// RaftTransport is used to connect a peer to its other peers.
type RaftTransport interface {
	connect.Conn

	NewRaftMessage(raftpb.Message) RaftMessage

	// I really want to install some notion of node-liveness here.
	IsLive(PeerID)
}

// Okay so we define a peer conn which

// NewPeer creates a new peer.
// It assumes that there is a peer ID and awareness of all of the other peers.
func (f *Factory) NewPeer(cfg PeerConfig) (*Peer, error) {
	// We start out without a peer ID.
	// We'll get one when we start receiving raft messages hopefully.
	f.mu.Lock()
	p, exists := f.mu.peers[cfg.GroupID]
	if exists {
		f.mu.Unlock()
		return p, nil
	}
	groupID := cfg.GroupID // goes on the heap because it's captured
	p = &Peer{
		AmbientContext:     cfg.AmbientContext,
		settings:           &f.cfg.Settings.SV,
		testingKnobs:       &f.cfg.TestingKnobs,
		groupID:            cfg.GroupID,
		raftConfig:         &f.cfg.RaftConfig,
		raftStorage:        cfg.RaftStorage,
		storage:            f.cfg.Storage,
		raftMessageFactory: cfg.RaftMessageFactory,
		processCommand:     cfg.ProcessCommand,
		processConfChange:  cfg.ProcessConfChanged,
	}
	p.onUnquiesce = func() { f.onUnquiesce(groupID) }
	p.onRaftReady = func() { f.scheduler.EnqueueRaftReady(groupID) }
	p.raftTransport = f.cfg.RaftTransport
	p.mu.peerID = cfg.PeerID
	p.mu.peers = cfg.Peers
	p.mu.proposals = make(map[storagebase.CmdIDKey]*proposal)
	f.mu.peers[cfg.GroupID] = p
	f.mu.unquiesced[cfg.GroupID] = struct{}{}
	f.mu.Unlock()
	p.withRaftGroupLocked(false, nil)
	return p, nil
}

// Progress represents the local view of state of replication for the replica
// group.
type Progress interface {
	AppliedIndex() uint64
	// TODO(ajwerner): something about log position
	// this is going to be needed to drive the proposal quota.
}
