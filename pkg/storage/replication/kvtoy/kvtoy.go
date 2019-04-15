// Package kvtoy is a toy implementation of a replicated key-value store built
// on top of the replication library.
//
// It strives to simplify interaction with replication enough to be much smaller
// than the kv but to be sophisticated enough to enable tests and benchmarks.
package kvtoy

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/connect"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/raftentry"
	"github.com/cockroachdb/cockroach/pkg/storage/replication"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/kvtoy/kvtoypb"
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

// I should clarify the goals of this package.
// I want to protoype the basics of replication in a mulit-raft set up
// This implementation will be simpler than the main implementation in a couple
// of important ways.
//
// Firstly, it will not do anything with leasing initially.
// Next it won't deal with transactionally managing replica descriptors.
//
// We'll assume that the above things can happen out of band.
//
// Next it's not going to eagerly create the replica when it receives raft
// messages. We need a good story here, but I don't have one yet. As soon as the
// node is part of the raft group from the other nodes perspectives, it can
// vote. That means that if we don't participate until after we receive snapshot
// data then we're sort of underreplicated. This is a major TODO.
//
// Instead (for now) we'll require that the ReplicaState be resident in the
// engine when the replica is created. That being said, we'll support
// pre-emptive snapshot and allow a replica to be created without having a
// ReplicaID.

// Ideally we'll be able to cover over the implementation of the entry storage
// and general raft state storage a little bit better but that's not for right
// right now.

type Store struct {
	// For now we're going to assume that there's just a single range.
	cfg         StoreConfig
	peerFactory *replication.Factory

	// This guy on launch then needs to scan some special key space and initialize
	// its replicas.

	mu struct {
		syncutil.RWMutex
		// Singleton for now
		replica *Replica
	}
}

func (s *Store) Batch(
	ctx context.Context, ba *kvtoypb.BatchRequest,
) (*kvtoypb.BatchResponse, error) {
	resp, err := s.mu.replica.Handle(ctx, ba)
	if err != nil {
		return nil, err.GoError()
	}
	return resp, nil
}

func (r *Replica) handleRequest(
	ctx context.Context, eng engine.ReadWriter, req *kvtoypb.RequestUnion,
) (*kvtoypb.ResponseUnion, error) {
	if get := req.GetGet(); get != nil {
		return r.handleGet(ctx, eng, get)
	}
	if put := req.GetPut(); put != nil {
		return r.handlePut(ctx, eng, put)
	}
	panic("not implemented")
}

// getOrCreateMarker protects writes from being applied more than once due to
// reproposals. All writes do a read to see if they have already been written
// and then if they have been not write a marker.
func (r *Replica) getOrCreateMarker(
	ctx context.Context, h *kvtoypb.Header, eng engine.ReadWriter,
) (created bool) {
	k := makeMarkerKey(r.rangeID, h)
	val, _, err := engine.MVCCGet(ctx, eng, k, hlc.Timestamp{}, engine.MVCCGetOptions{})
	if err != nil {
		panic(err)
	}
	if val != nil {
		if _, err := val.GetBytes(); err != nil {
			panic(err)
		}
		log.Infof(ctx, "marker exists for %q %v", k, h)
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

func makeMarkerKey(rangeID roachpb.RangeID, h *kvtoypb.Header) roachpb.Key {
	// TODO(ajwerner): fewer allocations here.
	buf := bytes.Buffer{}
	buf.Write([]byte(keys.MakeRangeIDReplicatedPrefix(rangeID)))
	buf.WriteString("mark")
	binary.Write(&buf, binary.BigEndian, h.Timestamp.WallTime)
	binary.Write(&buf, binary.BigEndian, h.Timestamp.Logical)
	binary.Write(&buf, binary.BigEndian, int64(h.Server))
	return roachpb.Key(buf.Bytes())
}

func (r *Replica) processRaftCommand(
	ctx context.Context, eng engine.ReadWriter, term, raftIndex uint64, command []byte,
) {

	id, op := replication.DecodeRaftCommand(command)
	var ba *kvtoypb.BatchRequest
	r.mu.Lock()
	prop, exists := r.mu.proposals[id]
	if exists {
		ctx = prop.ctx
		ba = prop.ba
		delete(r.mu.proposals, id)
	}
	r.mu.Unlock()
	// Now we're going to evaluate and apply the op
	if !exists {
		ba = new(kvtoypb.BatchRequest)
		if err := proto.Unmarshal(op, ba); err != nil {
			panic(err)
		}
	}
	if !ba.IsReadOnly() {
		// Check if the marker thing exists and, if so assert prop is nil and return
		if !r.getOrCreateMarker(ctx, &ba.Header, eng) {
			log.Infof(ctx, "request %v already applied", &ba.Header)
			return
		}
	} else if prop == nil {
		// Could grab a snapshot and go serve reads asynchronously
		return
	}
	// Could probably look at the spans being written and run non-overlapping
	// replicated commands in parallel.
	for i := range ba.Requests {
		resp, err := r.handleRequest(ctx, eng, &ba.Requests[i])
		if err != nil {
			panic(errors.Wrap(err, "we made an error"))
		}
		if prop != nil {
			prop.setResponse(i, resp)
		}
	}
	r.mu.RLock()
	stateLoader := r.mu.stateLoader
	r.mu.RUnlock()
	stats := enginepb.MVCCStats{}
	if err := stateLoader.SetRangeAppliedState(ctx, eng, raftIndex, 0, &stats); err != nil {
		panic(err)
	}
}

func (r *Replica) Handle(
	ctx context.Context, ba *kvtoypb.BatchRequest,
) (*kvtoypb.BatchResponse, *roachpb.Error) {
	idKey := replication.MakeIDKey()
	if !ba.IsReadOnly() {
		ts := r.store.cfg.Clock.Now()
		ba.Header.Timestamp = &ts
		ba.Header.Server = r.store.cfg.NodeID
	}
	prop := &proposal{
		ctx: ctx,
		id:  idKey,
		ba:  ba,
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.proposals[idKey] = prop
	pc := r.mu.peer.NewClient(&r.mu)
	pc.Callbacks.Apply = func(eng engine.Writer) error {
		log.Infof(ctx, "here in apply")
		panic("here")
	}
	pc.Send(ctx, prop)
	msg := pc.Recv()
	log.Infof(ctx, "got a response! %v %v, %v", msg, prop.br, len(prop.br.Responses))
	// realistically we need to wait for applied
	return prop.br, prop.err
}

func (r *Replica) handleGet(
	ctx context.Context, eng engine.ReadWriter, req *kvtoypb.GetRequest,
) (*kvtoypb.ResponseUnion, error) {
	val, _, err := engine.MVCCGet(ctx, eng, req.Key, hlc.Timestamp{}, engine.MVCCGetOptions{})
	if err != nil {
		return nil, err
	}
	return &kvtoypb.ResponseUnion{
		Value: &kvtoypb.ResponseUnion_Get{&kvtoypb.GetResponse{
			Value: val,
		}},
	}, nil
}

func (r *Replica) handlePut(
	ctx context.Context, eng engine.ReadWriter, req *kvtoypb.PutRequest,
) (*kvtoypb.ResponseUnion, error) {
	log.Infof(ctx, "put %v %v", req.Key, req.Value)
	err := engine.MVCCPut(ctx, eng, nil, req.Key, hlc.Timestamp{}, req.Value, nil)
	if err != nil {
		return nil, err
	}
	return &kvtoypb.ResponseUnion{
		Value: &kvtoypb.ResponseUnion_Put{&kvtoypb.PutResponse{}},
	}, nil
}

func (r *Replica) handleDelete(
	ctx context.Context, req *kvtoypb.DeleteRequest,
) (*kvtoypb.ResponseUnion, error) {
	panic("not implemented")
}

func (r *Replica) handleConditionalPut(
	ctx context.Context, req *kvtoypb.ConditionalPutRequest,
) (*kvtoypb.ResponseUnion, error) {
	panic("not implemented")
}

type Replica struct {
	rangeID roachpb.RangeID
	store   *Store
	mu      struct {
		syncutil.RWMutex

		// state is used on disk to represent the replica's state but only
		// Desc, RaftAppliedIndex, and TruncatedState are used.
		state storagepb.ReplicaState

		stateLoader stateloader.StateLoader

		// replicaID is zero until the replica is part of the raft group.
		replicaID roachpb.ReplicaID

		peer      *replication.Peer
		proposals map[storagebase.CmdIDKey]*proposal
	}
}

type proposal struct {
	ctx context.Context
	id  storagebase.CmdIDKey
	ba  *kvtoypb.BatchRequest
	br  *kvtoypb.BatchResponse
	err *roachpb.Error
}

func (p *proposal) ID() storagebase.CmdIDKey { return p.id }
func (p *proposal) Encoded() []byte {
	data, err := p.ba.Marshal()
	if err != nil {
		panic(err)
	}
	return []byte(replication.EncodeRaftCommandV1(p.id, data))
}
func (p *proposal) Size() int { return p.ba.Size() }

func (p *proposal) setResponse(i int, resp *kvtoypb.ResponseUnion) {
	fmt.Println("setting response", i, resp)
	if p.br == nil {
		p.br = &kvtoypb.BatchResponse{
			Responses: make([]kvtoypb.ResponseUnion, len(p.ba.Requests)),
		}
	}
	if resp != nil {
		p.br.Responses[i] = *resp
	}
}

// The store is going to track for which replicas it might have state a little
// bit more tightly. It's going to track who it has heard from for which ranges
// addressed how, it's going to own a quota pool that will be "orthogonal" to
// the replication layer.

// newReplica is going to create a new replica
// Unlike in storage, it assumes that there exists a committed range descriptor
// on disk, though we may still not be part of the group.
func newReplica(
	ctx context.Context, desc *roachpb.RangeDescriptor, store *Store, storage engine.Engine,
) (r *Replica, err error) {
	r = &Replica{
		store: store,
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.stateLoader = stateloader.Make(desc.RangeID)
	r.mu.state, err = r.mu.stateLoader.Load(ctx, storage, desc)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to load state for %v", desc)
	}

	// now we want to create the peer which should also have hard state and yell
	// if it doesn't
	replDesc, ok := r.mu.state.Desc.GetReplicaDescriptor(store.cfg.StoreID)
	if !ok {
		return nil, errors.Errorf("failed to find replica for store %v in %v",
			store.cfg.StoreID, r.mu.state.Desc)
	}
	r.mu.replicaID = replDesc.ReplicaID
	r.mu.proposals = make(map[storagebase.CmdIDKey]*proposal)
	peers := make([]replication.PeerID, 0, len(r.mu.state.Desc.Replicas))
	for _, r := range r.mu.state.Desc.Replicas {
		peers = append(peers, replication.PeerID(r.ReplicaID))
	}
	r.mu.peer, err = store.peerFactory.NewPeer(replication.PeerConfig{
		GroupID:        1,
		PeerID:         replication.PeerID(replDesc.ReplicaID),
		Peers:          peers,
		ProcessCommand: r.processRaftCommand,
		RaftMessageFactory: func(msg raftpb.Message) replication.RaftMessage {
			var m rafttransport.RaftMessageRequest
			m.Message = msg
			m.RangeID = desc.RangeID
			r.mu.RLock()
			defer r.mu.RUnlock()

			if roachpb.ReplicaID(m.Message.From) != r.mu.replicaID {
				panic(errors.Errorf("not from me: %v != %v", roachpb.ReplicaID(m.Message.From), r.mu.replicaID))
			}
			var ok bool
			m.FromReplica, ok = r.mu.state.Desc.GetReplicaDescriptorByID(r.mu.replicaID)
			if !ok {
				panic(errors.Errorf("failed to find my descriptor which I know to exist?!"))
			}
			m.ToReplica, ok = r.mu.state.Desc.GetReplicaDescriptorByID(roachpb.ReplicaID(m.Message.To))
			if !ok {
				log.Errorf(ctx, "Unknown outbound replica %v", m.Message.To)
				return nil
			}
			return &m
		},
		SideloadStorage: sideload.MustNewInMemSideloadStorage(r.mu.state.Desc.RangeID, r.mu.replicaID, ""),
	})
	if err != nil {
		return nil, err
	}

	// The thing I'm struggling with right now is when should we initialize the
	// raft peer? We can't do it unless we know we're a replica. We know we're a
	// replica if our store exists in our state that is on disk or if we're received
	// a message. But this is all sort of lame because of expectations around
	// the raft mu and some amount of locking that might happen when the storage
	// reaches up in to the replica which is a bummer.

	// When the snapshot happens we'll

	// I guess the issue is that the replica needs to know hard far it has applied.
	// The coupling seems to happen around the snapshots. I think I need to keep
	// making progress before I all the way understand.

	// We need to load the state and it needs to exist, otherwise it's an error
	// then we use our state to create the peer.

	// Let's think about who we want loading what.
	// Ideally the peer will not need to load anything from disk except the hard
	// state.

	// Separately we'll keep at the replica level the applied index

	// This means that it is going to be an error if we load empty state
	return r, nil
}

func NewStore(ctx context.Context, cfg StoreConfig) (s *Store, err error) {
	s = &Store{
		cfg: cfg,
	}
	replConfig := s.makeReplicationFactoryConfig()
	if s.peerFactory, err = replication.NewFactory(ctx, replConfig); err != nil {
		return nil, err
	}
	var rd roachpb.RangeDescriptor
	found, err := engine.MVCCGetProto(ctx, cfg.Engine, keys.RangeDescriptorKey(roachpb.RKeyMin), hlc.Timestamp{}, &rd,
		engine.MVCCGetOptions{})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.Errorf("no state found")
	}
	s.mu.replica, err = newReplica(ctx, &rd, s, cfg.Engine)
	if err != nil {
		return nil, err
	}
	// TODO(ajwerner): read the range descriptors and load the ranges

	//

	return s, nil
}

type StoreConfig struct {
	// NodeID and StoreID are manually assigned for simplicity.
	// DO NOT DUPLICATE THEM!

	NodeID  roachpb.NodeID
	StoreID roachpb.StoreID

	Settings      *cluster.Settings
	Stopper       *stop.Stopper
	RaftConfig    base.RaftConfig
	EntryCache    *raftentry.Cache
	Engine        engine.Engine
	RaftTransport connect.Conn
	Clock         *hlc.Clock
	Ambient       log.AmbientContext
	NodeDialer    *nodedialer.Dialer
}

func TestingStoreConfig(nodeID roachpb.NodeID) StoreConfig {
	cfg := StoreConfig{
		NodeID:     nodeID,
		StoreID:    roachpb.StoreID(nodeID),
		Settings:   cluster.MakeTestingClusterSettings(),
		Clock:      hlc.NewClock(hlc.UnixNano, 0),
		Stopper:    stop.NewStopper(),
		EntryCache: raftentry.NewCache(1 << 16),
		Engine:     engine.NewInMem(roachpb.Attributes{}, 1<<26 /* 64 MB */),
		Ambient:    log.AmbientContext{},
	}
	cfg.Ambient.AddLogTag("s", cfg.StoreID)
	cfg.RaftConfig.SetDefaults()
	cfg.RaftConfig.RaftHeartbeatIntervalTicks = 1
	cfg.RaftConfig.RaftElectionTimeoutTicks = 3
	cfg.RaftConfig.RaftTickInterval = 100 * time.Millisecond
	return cfg
}

// Somewhere we need to plumb the information into the raft message about which
// node a raft request is destined for.

// I guess the replica knows and needs to dictate the node ID for a given replica
// and then pass it along

func (s *Store) makeReplicationFactoryConfig() replication.FactoryConfig {
	var cfg replication.FactoryConfig
	cfg.Settings = s.cfg.Settings
	cfg.Storage = s.cfg.Engine
	cfg.Stopper = s.cfg.Stopper
	cfg.RaftConfig = s.cfg.RaftConfig
	cfg.RaftTransport = s.cfg.RaftTransport
	cfg.EntryCacheFactory = func(id replication.GroupID) replication.EntryCache {
		return groupCache{c: s.cfg.EntryCache, id: id}
	}
	cfg.StateLoaderFactory = func(id replication.GroupID) replication.StateLoader {
		return stateloader.Make(roachpb.RangeID(id))
	}
	cfg.NumWorkers = 16
	return cfg
}

func WriteInitialClusterData(ctx context.Context, eng engine.Engine) error {
	rsl := stateloader.Make(roachpb.RangeID(1))
	b := eng.NewBatch()
	defer b.Close()
	// TODO(ajwerner): initialize with a single replica and up replicate to the
	// others.
	replicas := []roachpb.ReplicaDescriptor{
		{
			NodeID:    1,
			StoreID:   1,
			ReplicaID: 1,
		},
		{
			NodeID:    2,
			StoreID:   2,
			ReplicaID: 2,
		},
		{
			NodeID:    3,
			StoreID:   3,
			ReplicaID: 3,
		},
	}
	if err := engine.MVCCPutProto(ctx, b, nil, keys.RangeDescriptorKey(roachpb.RKeyMin), hlc.Timestamp{},
		nil, &roachpb.RangeDescriptor{
			StartKey: roachpb.RKeyMin,
			RangeID:  1,
			Replicas: replicas,
		}); err != nil {
		return err
	}
	if _, err := rsl.Save(ctx, b, storagepb.ReplicaState{
		Lease: &roachpb.Lease{
			Replica: replicas[0],
		},
		TruncatedState: &roachpb.RaftTruncatedState{
			Term: 1,
		},
		GCThreshold:        &hlc.Timestamp{},
		Stats:              &enginepb.MVCCStats{},
		TxnSpanGCThreshold: &hlc.Timestamp{},
	}, stateloader.TruncatedStateUnreplicated); err != nil {
		return err
	}
	if err := rsl.SynthesizeRaftState(ctx, b); err != nil {
		return err
	}
	return b.Commit(true)
}
