package replication_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/kvtoy"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/kvtoy/kvtoypb"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/rafttransport"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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

func getAddr() net.Addr {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	addr := l.Addr()
	if err := l.Close(); err != nil {
		panic(err)
	}
	return addr
}

// TODO(ajwerner): finish this to replace the in-memory network with grpc.
// Still need to map the raft transport abstraction on to the connect.Conn
// abstraction.
func setUpToyStoresWithRealRPCs(
	ctx context.Context, t *testing.T, numNodes int,
) (stores []*kvtoy.Store, cleanup func()) {

	cfgs := make([]kvtoy.StoreConfig, 0, numNodes)
	stores = make([]*kvtoy.Store, 0, numNodes)
	addrs := make([]net.Addr, 0, numNodes)
	rpcContexts := make([]*rpc.Context, 0, numNodes)
	servers := make([]*grpc.Server, 0, numNodes)
	raftTransports := make([]*rafttransport.RaftTransport, 0, numNodes)
	resolver := func(nid roachpb.NodeID) (net.Addr, error) {
		if i := int(nid) - 1; i >= 0 && i < len(addrs) {
			return addrs[i], nil
		}
		return nil, fmt.Errorf("no known addr for %v", nid)
	}
	for i := 1; i <= numNodes; i++ {
		cfg := kvtoy.TestingStoreConfig(roachpb.NodeID(i))
		addr := getAddr()
		rpcCtx := rpc.NewContext(cfg.Ambient,
			&base.Config{
				Insecure: true,
				Addr:     addr.String(),
			},
			cfg.Clock,
			cfg.Stopper,
			&cfg.Settings.Version)
		cfg.NodeDialer = nodedialer.New(rpcCtx, resolver)
		server := rpc.NewServer(rpcCtx)
		raftTransport := rafttransport.NewRaftTransport(cfg.Ambient, cfg.Settings,
			cfg.NodeDialer, server, cfg.Stopper)
		require.Nil(t, kvtoy.WriteInitialClusterData(ctx, cfg.Engine))
		s, err := kvtoy.NewStore(ctx, cfg)
		require.Nil(t, err)
		rpcContexts = append(rpcContexts, rpcCtx)
		addrs = append(addrs, addr)
		cfgs = append(cfgs, cfg)
		stores = append(stores, s)
		servers = append(servers, server)
		raftTransports = append(raftTransports, raftTransport)
	}
	return stores, func() {
		var wg sync.WaitGroup
		wg.Add(3)
		for i := range cfgs {
			go func(i int) {
				cfgs[i].Stopper.Stop(ctx)
				wg.Done()
			}(i)
		}
		wg.Wait()
		for i := range cfgs {
			cfgs[i].Engine.Close()
		}
	}
}

func setUpToyStores(
	ctx context.Context, t *testing.T, numNodes int,
) (stores []*kvtoy.Store, cleanup func()) {
	n := kvtoy.NewNetwork()
	cfgs := make([]kvtoy.StoreConfig, 0, numNodes)
	stores = make([]*kvtoy.Store, 0, numNodes)
	for i := 1; i <= numNodes; i++ {
		cfg := kvtoy.TestingStoreConfig(roachpb.NodeID(i))
		cfg.RaftTransport = n.NewConn(cfg.StoreID)
		require.Nil(t, kvtoy.WriteInitialClusterData(ctx, cfg.Engine))
		s, err := kvtoy.NewStore(ctx, cfg)
		require.Nil(t, err)

		cfgs = append(cfgs, cfg)
		stores = append(stores, s)
	}

	return stores, func() {
		var wg sync.WaitGroup
		wg.Add(3)
		for i := range cfgs {
			go func(i int) {
				cfgs[i].Stopper.Stop(ctx)
				wg.Done()
			}(i)
		}
		wg.Wait()
		for i := range cfgs {
			cfgs[i].Engine.Close()
		}
	}
}

func TestReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// We want to construct a replication layer and then use it to replicate
	// some data.
	ctx := context.Background()
	const numNodes = 3
	stores, cleanup := setUpToyStores(ctx, t, numNodes)
	defer cleanup()
	s1 := stores[0]
	s2 := stores[1]

	// This is a terrible hack to deal with the fact that initial proposals will
	// be dropped and I haven't handled reproposals yet.
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
}
