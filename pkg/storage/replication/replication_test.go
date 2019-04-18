package replication_test

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/part1/kvtoy"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/rafttransport"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var k = roachpb.Key("k")
	var cases = []testCase{
		{
			name: "basic",
			ops: []op{
				{0, getOp{key: k, expNil: true}},
				{0, putOp{key: k, value: 1}},
				{0, getOp{key: k, expValue: 1}},
			},
		},
		{
			name: "propose from anywhere, no stale reads",
			ops: []op{
				{0, getOp{key: k, expNil: true}},
				{0, putOp{key: k, value: 1}},
				{2, getOp{key: k, expValue: 1}},
				{1, putOp{key: k, value: 2}},
				{0, getOp{key: k, expValue: 2}},
				{2, cputOp{key: k, expValue: 2, value: 3}},
				{0, getOp{key: k, expValue: 3}},
				{1, cputOp{key: k, expValue: 2, expFail: true}},
				{2, cputOp{key: k, expValue: 3, value: 4}},
				{0, getOp{key: k, expValue: 4}},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, tc.run)
	}
}

type testCase struct {
	name string
	ops  []op
}

func (tc testCase) run(t *testing.T) {
	ctx := context.Background()
	const numNodes = 3
	stores, cleanup := setUpToyStores(ctx, t, numNodes)
	defer cleanup()
	for i, op := range tc.ops {
		s := stores[op.store]
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			op.op.run(ctx, t, s)
		})
	}
}

type op struct {
	store int
	op    testOp
}

type testOp interface {
	run(ctx context.Context, t *testing.T, s *kvtoy.Store)
}

type putOp struct {
	key   roachpb.Key
	value float64
}

func (o putOp) run(ctx context.Context, t *testing.T, s *kvtoy.Store) {
	t.Helper()
	_, err := s.Batch(ctx, putReq(o.key, o.value))
	require.Nil(t, err)
}

func putReq(key roachpb.Key, value float64) *roachpb.BatchRequest {
	var br roachpb.BatchRequest
	pr := roachpb.PutRequest{}
	pr.Key = key
	pr.Value.SetFloat(value)
	br.Add(&pr)
	return &br
}

type getOp struct {
	key      roachpb.Key
	expValue float64
	expNil   bool
}

func (o getOp) run(ctx context.Context, t *testing.T, s *kvtoy.Store) {
	t.Helper()
	resp, err := s.Batch(ctx, getReq(o.key))
	assert.Nil(t, err)
	if o.expNil {
		assert.Nil(t, resp.Responses[0].GetGet().Value)
		return
	}
	require.NotNil(t, resp.Responses[0].GetGet().Value)
	respVal, err := resp.Responses[0].GetGet().Value.GetFloat()
	assert.Nil(t, err)
	assert.Equal(t, respVal, o.expValue)
}

func getReq(key roachpb.Key) *roachpb.BatchRequest {
	var br roachpb.BatchRequest
	pr := roachpb.GetRequest{}
	pr.Key = key
	br.Add(&pr)
	return &br
}

type cputOp struct {
	key      roachpb.Key
	expValue float64
	value    float64
	expFail  bool
}

func (o cputOp) run(ctx context.Context, t *testing.T, s *kvtoy.Store) {
	t.Helper()
	_, err := s.Batch(ctx, cputReq(o.key, o.expValue, o.value))
	if o.expFail {
		assert.NotNil(t, err)
	} else {
		assert.Nil(t, err)
	}
}

func cputReq(key roachpb.Key, exp, value float64) *roachpb.BatchRequest {
	var br roachpb.BatchRequest
	pr := roachpb.ConditionalPutRequest{}
	pr.Key = key
	pr.Value.SetFloat(value)
	var expValue roachpb.Value
	expValue.SetFloat(exp)
	pr.ExpValue = &expValue
	br.Add(&pr)
	return &br
}

func setUpToyStores(
	ctx context.Context, t *testing.T, numNodes int,
) (stores []*kvtoy.Store, cleanup func()) {
	cfgs := make([]kvtoy.Config, 0, numNodes)
	stores = make([]*kvtoy.Store, 0, numNodes)
	listeners := make([]net.Listener, 0, numNodes)
	rpcContexts := make([]*rpc.Context, 0, numNodes)
	servers := make([]*grpc.Server, 0, numNodes)
	resolver := func(nid roachpb.NodeID) (net.Addr, error) {
		if i := int(nid) - 1; i >= 0 && i < len(listeners) {
			return listeners[i].Addr(), nil
		}
		return nil, fmt.Errorf("no known addr for %v", nid)
	}
	nodeIDs := func() (nodeIDs []int) {
		for i := 1; i <= numNodes; i++ {
			nodeIDs = append(nodeIDs, i)
		}
		return nodeIDs
	}()
	for i := 1; i <= numNodes; i++ {
		cfg := kvtoy.TestingStoreConfig(roachpb.NodeID(i))
		l, err := net.Listen("tcp", ":0")
		require.Nil(t, err)
		rpcCtx := rpc.NewContext(cfg.Ambient,
			&base.Config{
				Insecure:          true,
				Addr:              l.Addr().String(),
				HeartbeatInterval: 2 * time.Second,
			},
			cfg.Clock,
			cfg.Stopper,
			&cfg.Settings.Version)
		nodeDialer := nodedialer.New(rpcCtx, resolver)
		server := rpc.NewServer(rpcCtx)
		cfg.RaftTransport = rafttransport.NewRaftTransport(cfg.Ambient, cfg.Settings,
			nodeDialer, server, cfg.Stopper)
		cfg.Stopper.RunAsyncTask(ctx, "server "+strconv.Itoa(i), func(ctx context.Context) {
			server.Serve(l)
		})
		cfg.Stopper.RunAsyncTask(ctx, "server stopper "+strconv.Itoa(i), func(ctx context.Context) {
			<-cfg.Stopper.ShouldQuiesce()
			server.Stop()
		})
		require.Nil(t, kvtoy.WriteInitialClusterData(ctx, cfg.Engine, 1, nodeIDs...))
		s, err := kvtoy.NewStore(ctx, cfg)
		require.Nil(t, err)
		rpcContexts = append(rpcContexts, rpcCtx)
		listeners = append(listeners, l)
		cfgs = append(cfgs, cfg)
		stores = append(stores, s)
		servers = append(servers, server)
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
