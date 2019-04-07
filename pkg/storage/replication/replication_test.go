package replication_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/kvtoy"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/kvtoy/kvtoypb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func put(key roachpb.Key, value float64) *kvtoypb.BatchRequest {
	put := &kvtoypb.PutRequest{}
	put.Key = key
	put.Value.SetFloat(value)
	return &kvtoypb.BatchRequest{
		Requests: []kvtoypb.RequestUnion{
			{Value: &kvtoypb.RequestUnion_Put{Put: put}},
		},
	}
}

func get(key roachpb.Key) *kvtoypb.BatchRequest {
	get := &kvtoypb.GetRequest{}
	get.Key = key
	return &kvtoypb.BatchRequest{
		Requests: []kvtoypb.RequestUnion{
			{Value: &kvtoypb.RequestUnion_Get{Get: get}},
		},
	}
}

func TestReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// We want to construct a replication layer and then use it to replicate
	// some data.
	n := kvtoy.NewNetwork()
	ctx := context.Background()
	cfg1 := kvtoy.TestingStoreConfig()
	cfg1.StoreID = 1
	cfg1.RaftTransport = n.NewConn(cfg1.StoreID)
	cfg2 := kvtoy.TestingStoreConfig()
	cfg2.StoreID = 2
	cfg2.RaftTransport = n.NewConn(cfg2.StoreID)
	cfg3 := kvtoy.TestingStoreConfig()
	cfg3.StoreID = 3
	cfg3.RaftTransport = n.NewConn(cfg3.StoreID)

	// We want to put a routing conn in place
	require.Nil(t, kvtoy.WriteInitialClusterData(ctx, cfg1.Engine))
	require.Nil(t, kvtoy.WriteInitialClusterData(ctx, cfg2.Engine))
	require.Nil(t, kvtoy.WriteInitialClusterData(ctx, cfg3.Engine))
	s1, err := kvtoy.NewStore(ctx, cfg1)
	require.Nil(t, err)
	s2, err := kvtoy.NewStore(ctx, cfg2)
	require.Nil(t, err)
	_, err = kvtoy.NewStore(ctx, cfg3)
	require.Nil(t, err)

	time.Sleep(500 * time.Millisecond)
	resp, err := s1.Batch(ctx, put(roachpb.Key("asdf"), 1))
	assert.Nil(t, err)
	resp, err = s1.Batch(ctx, get(roachpb.Key("asdf")))
	assert.Nil(t, err)
	respVal, err := resp.Responses[0].GetGet().Value.GetFloat()
	assert.Nil(t, err)
	assert.Equal(t, respVal, 1.0)
	resp, err = s2.Batch(ctx, put(roachpb.Key("asdf"), 2))
	assert.Nil(t, err)
	resp, err = s2.Batch(ctx, get(roachpb.Key("asdf")))
	assert.Nil(t, err)
	respVal, err = resp.Responses[0].GetGet().Value.GetFloat()
	assert.Nil(t, err)
	assert.Equal(t, respVal, 2.0)

	//s.Put("asdf", []byte("asdf"))

	defer func() {
		var wg sync.WaitGroup
		wg.Add(3)
		go func() { cfg1.Stopper.Stop(ctx); wg.Done() }()
		go func() { cfg2.Stopper.Stop(ctx); wg.Done() }()
		go func() { cfg3.Stopper.Stop(ctx); wg.Done() }()
		wg.Wait()
		cfg1.Engine.Close()
		cfg2.Engine.Close()
		cfg3.Engine.Close()
	}()

}
