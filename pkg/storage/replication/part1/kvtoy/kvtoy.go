package kvtoy

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/raftentry"
	"github.com/cockroachdb/cockroach/pkg/storage/replication"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachlabs/acidlib/v1/connect"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/raftpb"
)

// Config configures the Store.
type Config struct {
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

// Store stores key-value data.
type Store struct {
	cfg         Config
	peerFactory *replication.Factory

	mu struct {
		syncutil.RWMutex
	}
}

// NewStore creates a new Store.
func NewStore(cfg Config) *Store {
	return &Store{
		cfg: cfg,
	}
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
	cfg.EntryCacheFactory = func(id replication.GroupID) replication.EntryCache {
		return groupCache{c: s.cfg.EntryCache, id: id}
	}
	cfg.StateLoaderFactory = func(id replication.GroupID) replication.StateLoader {
		return stateloader.Make(roachpb.RangeID(id))
	}
	cfg.EntryScannerFactory = func(id replication.GroupID) replication.EntryReader {
		return func(
			ctx context.Context,
			eng engine.Reader,
			lo, hi uint64,
			f func(raftpb.Entry) (wantMore bool, err error),
		) error {
			var ent raftpb.Entry
			scanFunc := func(kv roachpb.KeyValue) (bool, error) {
				if err := kv.Value.GetProto(&ent); err != nil {
					return false, err
				}
				return f(ent)
			}
			_, err := engine.MVCCIterate(
				ctx, eng,
				keys.RaftLogKey(roachpb.RangeID(id), lo),
				keys.RaftLogKey(roachpb.RangeID(id), hi),
				hlc.Timestamp{},
				engine.MVCCScanOptions{},
				scanFunc,
			)
			return err
		}
	}
	cfg.NumWorkers = 16
	return cfg
}

// Batch implements the roachpb.Internal interface.
func (s *Store) Batch(
	ctx context.Context, ba *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	if ba.IsReadOnly() {
		return s.handleReadOnlyBatch(ctx, ba)
	}
	return s.handleReadWriteBatch(ctx, ba)
}

func (s *Store) RangeFeed(*roachpb.RangeFeedRequest, roachpb.Internal_RangeFeedServer) error {
	panic("not implemented")
}

func (s *Store) handleReadOnlyBatch(
	ctx context.Context, ba *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	s.mu.RLock()
	snap := s.engine.NewSnapshot()
	s.mu.RUnlock()
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

func (s *Store) handleReadWriteBatch(
	ctx context.Context, ba *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	br := ba.CreateReply()
	s.mu.Lock()
	defer s.mu.Unlock()
	batch := s.engine.NewBatch()
	defer batch.Close()
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
	if err := batch.Commit(true); err != nil {
		return nil, err
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
