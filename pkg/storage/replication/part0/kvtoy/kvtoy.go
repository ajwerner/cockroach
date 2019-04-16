package kvtoy

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
)

// Config configures the Store.
type Config struct {
	// Engine is the storage engine used by the store.
	Engine engine.Engine
}

// Store stores key-value data.
type Store struct {
	engine engine.Engine

	mu struct {
		syncutil.RWMutex
	}
}

// NewStore creates a new Store.
func NewStore(cfg Config) *Store {
	return &Store{
		engine: cfg.Engine,
	}
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
