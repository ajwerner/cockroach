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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

// Question: does it matter that we've been aggressively focusing on having

// GroupID is the basic unit of replication.
// Each peer corresponds to a single ID.
type GroupID int64

// PeerID is a replica id :/
type PeerID int64

type ProposalMessage interface {
	ID() storagebase.CmdIDKey
	Encoded() []byte
	Size() int
}

type ConfChangeMessage interface {
	ProposalMessage
	ChangeType() raftpb.ConfChangeType
	PeerID() PeerID
}

var _ ProposalMessage = EncodedProposalMessage(nil)

func (epm EncodedProposalMessage) Size() int { return len(epm) }
func (epm EncodedProposalMessage) ID() storagebase.CmdIDKey {
	return EncodedCommand(epm).ID()
}
func (epm EncodedProposalMessage) Encoded() []byte { return []byte(epm) }

var _ ConfChangeMessage = (*ConfChange)(nil)

func (ccm *ConfChange) Size() int { return len(ccm.Command) }
func (ccm *ConfChange) ID() storagebase.CmdIDKey {
	return ccm.Command.ID()
}

func (ccm *ConfChange) PeerID() PeerID {
	return ccm.Peer
}

func (ccm *ConfChange) ChangeType() raftpb.ConfChangeType {
	return ccm.Type
}

func (ccm *ConfChange) Encoded() []byte {
	return []byte(ccm.Command)
}

type EncodedCommand []byte

func (ec EncodedCommand) ID() storagebase.CmdIDKey {
	id, _ := ec.Decode()
	return id
}

func (ec EncodedCommand) Decode() (storagebase.CmdIDKey, []byte) {
	v := raftCommandEncodingVersion(ec[0] & raftCommandNoSplitMask)
	if v != raftVersionStandard && v != raftVersionSideloaded {
		panic(fmt.Sprintf("unknown command encoding version %v", ec[0]))
	}
	return storagebase.CmdIDKey(ec[1 : 1+raftCommandIDLen]), []byte(ec[1+raftCommandIDLen:])
}

// ProposalMessage is used to propose commands to the system.
type EncodedProposalMessage EncodedCommand

type ConfChange struct {
	Type    raftpb.ConfChangeType
	Peer    PeerID
	Command EncodedCommand
}

// RaftMessage is a message used to send raft messages on the wire.
type RaftMessage interface {
	connect.Message
	GetGroupID() GroupID
	// SetGroupID(GroupID)
	GetRaftMessage() raftpb.Message
	// SetRaftMessage(raftpb.Message)
}

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

// SideloadStorage is the interface used for Raft SSTable sideloading.
// Implementations do not need to be thread safe.
type SideloadStorage interface {
	// The directory in which the sideloaded files are stored. May or may not
	// exist.
	Dir() string
	// Writes the given contents to the file specified by the given index and
	// term. Overwrites the file if it already exists.
	Put(_ context.Context, index, term uint64, contents []byte) error
	// Load the file at the given index and term. Return errSideloadedFileNotFound when no
	// such file is present.
	Get(_ context.Context, index, term uint64) ([]byte, error)
	// Purge removes the file at the given index and term. It may also
	// remove any leftover files at the same index and earlier terms, but
	// is not required to do so. When no file at the given index and term
	// exists, returns errSideloadedFileNotFound.
	//
	// Returns the total size of the purged payloads.
	Purge(_ context.Context, index, term uint64) (int64, error)
	// Clear files that may have been written by this SideloadStorage.
	Clear(context.Context) error
	// TruncateTo removes all files belonging to an index strictly smaller than
	// the given one. Returns the number of bytes freed, the number of bytes in
	// files that remain, or an error.
	TruncateTo(_ context.Context, index uint64) (freed, retained int64, _ error)
	// Returns an absolute path to the file that Get() would return the contents
	// of. Does not check whether the file actually exists.
	Filename(_ context.Context, index, term uint64) (string, error)
}

type StateLoader interface {
	LoadRaftTruncatedState(context.Context, engine.Reader) (_ roachpb.RaftTruncatedState, isLegacy bool, _ error)
	SetRaftTruncatedState(context.Context, engine.ReadWriter, *roachpb.RaftTruncatedState) error

	LoadHardState(context.Context, engine.Reader) (raftpb.HardState, error)
	SetHardState(context.Context, engine.ReadWriter, raftpb.HardState) error

	SynthesizeRaftState(context.Context, engine.ReadWriter) error
	LoadLastIndex(ctx context.Context, reader engine.Reader) (uint64, error)

	RaftLastIndexKey() roachpb.Key
	RaftLogPrefix() roachpb.Key
	RaftLogKey(logIndex uint64) roachpb.Key
	RangeLastReplicaGCTimestampKey() roachpb.Key
}

type EntryCache interface {
	Add(_ []raftpb.Entry, truncate bool)
	Clear(hi uint64)
	Get(idx uint64) (raftpb.Entry, bool)
	Scan(buf []raftpb.Entry, lo, hi, maxBytes uint64) (_ []raftpb.Entry, bytes, nextIdx uint64, exceededMaxBytes bool)
}

type FactoryConfig struct {
	base.RaftConfig

	// RaftTransport is the message bus on which RaftMessages are sent and
	// received.
	RaftTransport connect.Conn

	// TODO(ajwerner): should there be a start method that takes a stopper?
	Stopper *stop.Stopper

	// Storage for all of the peers.
	Storage engine.Engine

	NumWorkers int

	TestingKnobs TestingKnobs

	Settings *cluster.Settings

	//RaftMessageFactory     func() RaftMessage
	// 	SideloadStorageFactory func(GroupID) SideloadStorage
	StateLoaderFactory  func(GroupID) StateLoader
	EntryCacheFactory   func(GroupID) EntryCache
	EntryScannerFactory func(GroupID) EntryReader
}

type EntryReader func(_ context.Context, _ engine.Reader, lo, hi uint64, f func(raftpb.Entry) (wantMore bool, err error)) error

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
	log.Infof(ctx, "processing request queue %v", id)
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
	ticker := time.NewTicker(f.cfg.RaftTickInterval)
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

type ProcessCommandFunc func(ctx context.Context, eng engine.ReadWriter, term, index uint64, command []byte)
type ProcessConfChangeFunc func(ctx context.Context, eng engine.ReadWriter, term, index uint64, command []byte) (confChanged bool)

type PeerConfig struct {
	log.AmbientContext
	GroupID            GroupID
	PeerID             PeerID
	Peers              []PeerID
	ProcessCommand     ProcessCommandFunc
	ProcessConfChanged ProcessConfChangeFunc
	RaftMessageFactory func(raftpb.Message) RaftMessage
	SideloadStorage    SideloadStorage
}

func (f *Factory) NewPeer(cfg PeerConfig) (*Peer, error) {
	// TODO: actually implement
	// We start out without a peer ID.
	// We'll get one when we start receiving raft messages hopefully.
	f.mu.Lock()
	p, exists := f.mu.peers[cfg.GroupID]
	if exists {
		f.mu.Unlock()
		return p, nil
	}
	p = &Peer{
		AmbientContext:     cfg.AmbientContext,
		settings:           &f.cfg.Settings.SV,
		testingKnobs:       &f.cfg.TestingKnobs,
		groupID:            cfg.GroupID,
		raftConfig:         &f.cfg.RaftConfig,
		raftMessageFactory: cfg.RaftMessageFactory,
		storage:            f.cfg.Storage,
		onUnquiesce:        func() { f.onUnquiesce(cfg.GroupID) },
		processCommand:     cfg.ProcessCommand,
		processConfChange:  cfg.ProcessConfChanged,
	}

	p.mu.peerID = cfg.PeerID
	p.mu.peers = cfg.Peers
	p.entryCache = f.cfg.EntryCacheFactory(cfg.GroupID)
	p.raftTransport = f.cfg.RaftTransport
	p.entryReader = f.cfg.EntryScannerFactory(cfg.GroupID)
	p.mu.stateLoader = f.cfg.StateLoaderFactory(cfg.GroupID)
	p.raftMu.stateLoader = f.cfg.StateLoaderFactory(cfg.GroupID)
	p.raftMu.sideloaded = cfg.SideloadStorage
	f.mu.peers[cfg.GroupID] = p
	f.mu.unquiesced[cfg.GroupID] = struct{}{}
	p.mu.proposals = make(map[storagebase.CmdIDKey]*proposal)
	f.mu.Unlock()
	p.withRaftGroupLocked(false, nil)
	return p, nil
}

func (f *Factory) LoadPeer(logCtx log.AmbientContext, id GroupID) (*Peer, error) {
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
	m, ok := msg.(ProposalMessage)
	if !ok {
		panic(fmt.Errorf("got %T, expected %T", msg, (ProposalMessage)(nil)))
	}
	pc.syn.Unlock()
	defer pc.syn.Lock()
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
	msg             ProposalMessage
	proposedAtTicks int
}

// Progress represents the local view of state of replication for the replica
// group.
type Progress interface {
	AppliedIndex() uint64
	LeaseSequence() int64
	LeaseAppliedIndex() int64
}
