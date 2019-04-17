package kvtoy

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/connect"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/raftentry"
	"github.com/cockroachdb/cockroach/pkg/storage/replication"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/raftstorage"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/rafttransport"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/sideload"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/raftpb"
)

// Config configures the Store.
type Config struct {
	Ambient  log.AmbientContext
	Settings *cluster.Settings
	Stopper  *stop.Stopper

	// NodeID and StoreID are manually assigned for simplicity.
	// DO NOT DUPLICATE THEM!
	NodeID  roachpb.NodeID
	StoreID roachpb.StoreID

	Engine        engine.Engine
	Clock         *hlc.Clock
	EntryCache    *raftentry.Cache
	RaftTransport connect.Conn
	RaftConfig    base.RaftConfig
}

// Store stores key-value data.
type Store struct {
	cfg         Config
	peerFactory *replication.Factory
	peer        *replication.Peer

	// TODO(ajwerner): this used to be protected by a mutex, why does it change?
	stateLoader stateloader.StateLoader
	mu          struct {
		syncutil.RWMutex

		replDesc  roachpb.ReplicaDescriptor
		state     storagepb.ReplicaState
		proposals map[storagebase.CmdIDKey]*proposal

		// pending reads is a queue waiting for a snapshot in order to serve reads
		pendingReads *readSnapshotWaiter
	}
}

// NewStore creates a new Store.
func NewStore(ctx context.Context, cfg Config) (s *Store, err error) {
	s = &Store{cfg: cfg}
	replConfig := s.makeReplicationFactoryConfig()
	if s.peerFactory, err = replication.NewFactory(ctx, replConfig); err != nil {
		return nil, errors.Wrap(err, "failed to make replication factory")
	}
	var desc roachpb.RangeDescriptor
	found, err := engine.MVCCGetProto(ctx, cfg.Engine, keys.RangeDescriptorKey(roachpb.RKeyMin), hlc.Timestamp{}, &desc,
		engine.MVCCGetOptions{})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.Errorf("no state found")
	}
	s.mu.proposals = make(map[storagebase.CmdIDKey]*proposal)
	s.stateLoader = stateloader.Make(desc.RangeID)
	s.mu.state, err = s.stateLoader.Load(ctx, cfg.Engine, &desc)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to load state for %v", desc)
	}
	if s.mu.replDesc, found = s.mu.state.Desc.GetReplicaDescriptor(cfg.StoreID); !found {
		return nil, errors.Errorf("state does not contain an entry for store %d", cfg.StoreID)
	}
	s.mu.state.Desc = &desc
	sideloadStorage := sideload.MustNewInMemSideloadStorage(desc.RangeID, s.mu.replDesc.ReplicaID, "")
	peers := make([]replication.PeerID, 0, len(desc.Replicas))
	for _, replica := range desc.Replicas {
		peers = append(peers, replication.PeerID(replica.ReplicaID))
	}
	rs, err := raftstorage.NewRaftStorage(ctx, raftstorage.Config{
		Ambient:         s.cfg.Ambient,
		Engine:          s.cfg.Engine,
		StateLoader:     stateloader.Make(desc.RangeID),
		EntryCache:      groupCache{c: s.cfg.EntryCache, id: uint64(desc.RangeID)},
		SideloadStorage: sideloadStorage,
		GetConfState: func() (cs raftpb.ConfState) {
			cs.Nodes = peers
			return cs
		},
	})
	if err != nil {
		return nil, err
	}
	s.peer, err = s.peerFactory.NewPeer(replication.PeerConfig{
		GroupID:        1,
		PeerID:         replication.PeerID(s.mu.replDesc.ReplicaID),
		Peers:          peers,
		ProcessCommand: s.processRaftCommand,
		RaftMessageFactory: func(msg raftpb.Message) replication.RaftMessage {
			var m rafttransport.RaftMessageRequest
			m.Message = msg
			m.RangeID = desc.RangeID
			s.mu.RLock()
			defer s.mu.RUnlock()
			from := roachpb.ReplicaID(m.Message.From)
			me := s.mu.replDesc.ReplicaID
			if from != me {
				panic(errors.Errorf("not from me: %v != %v", from, me))
			}
			var ok bool
			m.FromReplica, ok = s.mu.state.Desc.GetReplicaDescriptorByID(me)
			if !ok {
				panic(errors.Errorf("failed to find my descriptor which I know to exist?!"))
			}
			m.ToReplica, ok = s.mu.state.Desc.GetReplicaDescriptorByID(roachpb.ReplicaID(m.Message.To))
			if !ok {
				log.Errorf(ctx, "Unknown outbound replica %v", m.Message.To)
				return nil
			}
			return &m
		},
		RaftStorage: rs,
	})
	if err != nil {
		return nil, err
	}
	return s, nil
}

// makeReplicationFactoryConfig sets up the configuration for the replication
// factory.
func (s *Store) makeReplicationFactoryConfig() replication.FactoryConfig {
	var cfg replication.FactoryConfig
	cfg.Settings = s.cfg.Settings
	cfg.Storage = s.cfg.Engine
	cfg.Stopper = s.cfg.Stopper
	cfg.RaftConfig = s.cfg.RaftConfig
	cfg.RaftTransport = s.cfg.RaftTransport
	cfg.NumWorkers = 1
	return cfg
}

type proposal struct {
	ctx context.Context
	mu  sync.Mutex
	id  storagebase.CmdIDKey
	ba  *roachpb.BatchRequest
	br  *roachpb.BatchResponse
	err error
}

func (p *proposal) ID() storagebase.CmdIDKey { return p.id }
func (p *proposal) Encoded() []byte {
	data, err := p.ba.Marshal()
	if err != nil {
		panic(err)
	}
	return data
}
func (p *proposal) Size() int { return p.ba.Size() }

// Batch implements the roachpb.Internal interface.
func (s *Store) Batch(
	ctx context.Context, ba *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	if ba.IsReadOnly() {
		return s.handleReadOnlyBatch(ctx, ba)
	}
	return s.handleReadWriteBatch(ctx, ba)
	// if ba.IsReadOnly() {
	// 	return s.handleReadOnlyBatch(ctx, ba)
	// }
	// return s.handleReadWriteBatch(ctx, ba)
}

const rangeID roachpb.RangeID = 1
const markKeyPrefix = "mark"

func makeMarkerKey(h *roachpb.Header) roachpb.Key {
	buf := bytes.Buffer{}
	buf.Write([]byte(keys.MakeRangeIDReplicatedPrefix(h.RangeID)))
	buf.WriteString(markKeyPrefix)
	binary.Write(&buf, binary.BigEndian, h.Timestamp.WallTime)
	binary.Write(&buf, binary.BigEndian, h.Timestamp.Logical)
	binary.Write(&buf, binary.BigEndian, int64(h.Replica.NodeID))
	return roachpb.Key(buf.Bytes())
}

// getOrCreateMarker protects writes from being applied more than once due to
// reproposals. All writes do a read to see if they have already been written
// and then if they have been not write a marker.
func (s *Store) getOrCreateMarker(
	ctx context.Context, h *roachpb.Header, eng engine.ReadWriter,
) (created bool) {
	k := makeMarkerKey(h)
	val, _, err := engine.MVCCGet(ctx, eng, k, hlc.Timestamp{}, engine.MVCCGetOptions{})
	if err != nil {
		panic(err)
	}
	if val != nil {
		if _, err := val.GetBytes(); err != nil {
			panic(err)
		}
		return false
	}
	var newVal roachpb.Value
	newVal.SetBytes(nil)
	err = engine.MVCCPut(ctx, eng, nil, k, hlc.Timestamp{}, newVal, nil)
	if err != nil {
		panic(err)
	}
	return true
}

func (s *Store) servePendingReadsLocked() {
	if s.mu.pendingReads != nil {
		s.mu.pendingReads.setSnapshot(s.cfg.Engine.NewSnapshot())
		s.mu.pendingReads = nil
	}
}

func (s *Store) processRaftCommand(
	ctx context.Context,
	term, raftIndex uint64,
	id storagebase.CmdIDKey,
	msg replication.ProposalMessage,
) {
	var ba *roachpb.BatchRequest
	s.mu.Lock()
	// TODO(ajwerner): think about what this actually does with regards to
	// consistency.
	s.servePendingReadsLocked() // this call serves to sequence reads
	prop, exists := s.mu.proposals[id]
	if exists {
		ctx = prop.ctx
		ba = prop.ba
		delete(s.mu.proposals, id)
	}
	s.mu.Unlock()

	if !exists {
		// This gets gnarly because we need some information about the
		// "encoding version".
		ba = new(roachpb.BatchRequest)
		if err := proto.Unmarshal(msg.Encoded(), ba); err != nil {
			panic(err)
		}
	}

	// Check if the marker thing exists and, if so assert prop is nil and return
	proposalNotApplied := s.getOrCreateMarker(ctx, &ba.Header, s.cfg.Engine)
	if !proposalNotApplied && log.V(1) {
		log.Infof(ctx, "request %v already applied", &ba.Header)
	}
	batch := s.cfg.Engine.NewBatch()
	defer batch.Close()

	if proposalNotApplied {
		br, err := s.handleReadWriteBatchLocked(ctx, ba, batch)
		if prop != nil {
			prop.br, prop.err = br, err
		}
		// We need to disregard this batch if an error occurs.
		if err != nil {
			batch.Close()
			batch = s.cfg.Engine.NewBatch()
			// Also need to mark the proposal as having been applied in the new batch.
			s.getOrCreateMarker(ctx, &ba.Header, batch)
		}
	}
	// TODO(ajwerner): stop using fake stats
	var stats enginepb.MVCCStats
	if err := s.stateLoader.SetRangeAppliedState(ctx, batch, raftIndex, 0, &stats); err != nil {
		panic(err)
	}
	if err := batch.Commit(false); err != nil {
		panic(err)
	}
	s.mu.Lock()
	s.mu.state.RaftAppliedIndex = raftIndex
	s.mu.Unlock()
}

func (s *Store) RangeFeed(*roachpb.RangeFeedRequest, roachpb.Internal_RangeFeedServer) error {
	panic("not implemented")
}

func newReadSnapshotWaiter() *readSnapshotWaiter {
	sw := readSnapshotWaiter{}
	sw.cond.L = sw.mu.RLocker()
	return &sw
}

type readSnapshotWaiter struct {
	mu   sync.RWMutex
	cond sync.Cond
	snap engine.Reader
}

func (w *readSnapshotWaiter) getSnapshot() engine.Reader {
	w.cond.L.Lock()
	defer w.cond.L.Unlock()
	for w.snap == nil {
		w.cond.Wait()
	}
	return w.snap
}

func (w *readSnapshotWaiter) setSnapshot(snap engine.Reader) {
	defer w.cond.Broadcast()
	w.mu.Lock()
	defer w.mu.Unlock()
	w.snap = snap
}

func (s *Store) getReadSnapshot() engine.Reader {
	s.mu.RLock()
	sw := s.mu.pendingReads
	s.mu.RUnlock()
	if sw != nil {
		return sw.getSnapshot()
	}
	// There wasn't already a waiter, create one.
	s.mu.Lock()
	sw = s.mu.pendingReads
	if sw == nil { // nobody beat us in a race
		s.mu.pendingReads = newReadSnapshotWaiter()
		if len(s.mu.proposals) == 0 {
			s.submitEmptyBatchLocked(context.TODO())
		}
		sw = s.mu.pendingReads
	}
	s.mu.Unlock()
	return sw.getSnapshot()
}

func (s *Store) handleReadOnlyBatch(
	ctx context.Context, ba *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	snap := s.getReadSnapshot()
	br := ba.CreateReply()
	for i, req := range ba.Requests {
		var resp roachpb.Response
		var err error
		switch req := req.GetInner().(type) {
		case *roachpb.GetRequest:
			resp, err = s.handleGet(ctx, req, snap)
		default:
			return nil, errors.Errorf("unknown request type %T", req)
		}
		if err != nil {
			return nil, err
		}
		br.Responses[i].SetInner(resp)
	}
	return br, nil
}

func (s *Store) submitEmptyBatchLocked(ctx context.Context) {
	idKey := replication.MakeIDKey()
	ba := roachpb.BatchRequest{}
	ba.Header.Timestamp = s.cfg.Clock.Now()
	prop := &proposal{
		ctx: ctx,
		id:  idKey,
		ba:  &ba,
	}
	prop.mu.Lock()
	defer prop.mu.Unlock()
	pc := s.peer.NewClient(&prop.mu)
	// Always repropose because we don't have LeaseAppliedIndex or anything like
	// that.
	pc.Callbacks.ShouldRepropose = func(_ replication.ReproposalReason) error {
		return nil
	}
	pc.Send(ctx, prop)
}

func (s *Store) handleReadWriteBatch(
	ctx context.Context, ba *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	idKey := replication.MakeIDKey()
	ts := s.cfg.Clock.Now()
	ba.Header.Timestamp = ts
	prop := &proposal{
		ctx: ctx,
		id:  idKey,
		ba:  ba,
	}
	s.mu.Lock()
	ba.Header.Replica = s.mu.replDesc
	s.mu.proposals[idKey] = prop
	s.mu.Unlock()
	pc := s.peer.NewClient(&prop.mu)
	prop.mu.Lock()
	// Always repropose because we don't have LeaseAppliedIndex or anything like
	// that.
	pc.Callbacks.ShouldRepropose = func(_ replication.ReproposalReason) error {
		return nil
	}
	pc.Send(ctx, prop)
	msg := pc.Recv()
	switch msg := msg.(type) {
	case *replication.ErrorMessage:
		return nil, msg.Err
	case replication.CommittedMessage:
		// realistically we need to wait for applied
		return prop.br, prop.err
	default:
		panic(errors.Errorf("unexpected response type %T", msg))
	}
}

func (s *Store) handleReadWriteBatchLocked(
	ctx context.Context, ba *roachpb.BatchRequest, batch engine.ReadWriter,
) (*roachpb.BatchResponse, error) {
	br := ba.CreateReply()
	for i, req := range ba.Requests {
		var resp roachpb.Response
		var err error
		switch req := req.GetInner().(type) {
		case *roachpb.PutRequest:
			resp, err = s.handlePut(ctx, req, batch)
		case *roachpb.ConditionalPutRequest:
			resp, err = s.handleConditionalPut(ctx, req, batch)
		case *roachpb.DeleteRequest:
			resp, err = s.handleDelete(ctx, req, batch)
		case *roachpb.GetRequest:
			resp, err = s.handleGet(ctx, req, batch)
		default:
			// The type system should prevent this case.
			panic(errors.Errorf("unknown request type %T", req))
		}
		if err != nil {
			return nil, err
		}
		br.Responses[i].SetInner(resp)
	}
	return br, nil
}

func (s *Store) handleGet(
	ctx context.Context, req *roachpb.GetRequest, eng engine.Reader,
) (roachpb.Response, error) {
	val, _, err := engine.MVCCGet(ctx, eng, req.Key, hlc.Timestamp{}, engine.MVCCGetOptions{})
	if err != nil {
		return nil, err
	}
	return &roachpb.GetResponse{Value: val}, nil
}

func (s *Store) handlePut(
	ctx context.Context, req *roachpb.PutRequest, eng engine.ReadWriter,
) (roachpb.Response, error) {
	err := engine.MVCCPut(ctx, eng, nil, req.Key, hlc.Timestamp{}, req.Value, nil)
	if err != nil {
		return nil, err
	}
	return &roachpb.PutResponse{}, nil
}

func (s *Store) handleDelete(
	ctx context.Context, req *roachpb.DeleteRequest, eng engine.ReadWriter,
) (roachpb.Response, error) {
	err := engine.MVCCDelete(ctx, eng, nil, req.Key, hlc.Timestamp{}, nil)
	if err != nil {
		return nil, err
	}
	return &roachpb.DeleteResponse{}, nil
}

func (s *Store) handleConditionalPut(
	ctx context.Context, req *roachpb.ConditionalPutRequest, eng engine.ReadWriter,
) (roachpb.Response, error) {
	val, _, err := engine.MVCCGet(ctx, eng, req.Key, hlc.Timestamp{}, engine.MVCCGetOptions{})
	if err != nil {
		return nil, err
	}
	if !val.Equal(req.Value) {
		return nil, errors.Errorf("conditional put: expectation failed for key %v: %v != %v",
			req.Key, val, req.Value)
	}
	err = engine.MVCCPut(ctx, eng, nil, req.Key, hlc.Timestamp{}, req.Value, nil)
	if err != nil {
		return nil, err
	}
	return &roachpb.ConditionalPutResponse{}, nil
}
