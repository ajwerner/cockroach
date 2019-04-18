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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/part2/kvtoy"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/rafttransport"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
			name:         "basic",
			numNodes:     3,
			initialNodes: 3,
			ops: []op{
				reqOp{0, getOp{key: k, expNil: true}},
				reqOp{0, putOp{key: k, value: 1}},
				reqOp{0, getOp{key: k, expValue: 1}},
			},
		},
		{
			name:         "propose from anywhere, no stale reads",
			numNodes:     3,
			initialNodes: 3,
			ops: []op{
				reqOp{0, getOp{key: k, expNil: true}},
				reqOp{0, putOp{key: k, value: 1}},
				reqOp{2, getOp{key: k, expValue: 1}},
				reqOp{1, putOp{key: k, value: 2}},
				reqOp{0, getOp{key: k, expValue: 2}},
				reqOp{2, cputOp{key: k, expValue: 2, value: 3}},
				reqOp{0, getOp{key: k, expValue: 3}},
				reqOp{1, cputOp{key: k, expValue: 2, expFail: true}},
				reqOp{2, cputOp{key: k, expValue: 3, value: 4}},
				reqOp{0, getOp{key: k, expValue: 4}},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, tc.run)
	}
}

func TestConfChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var k = roachpb.Key("k")
	var cases = []testCase{
		{
			name:         "conf change",
			numNodes:     4,
			initialNodes: 3,
			ops: []op{
				reqOp{0, getOp{key: k, expNil: true}},
				reqOp{0, putOp{key: k, value: 1}},
				reqOp{2, getOp{key: k, expValue: 1}},
				// Let's put a whole lot of data over there so that a snapshot is important.
				// Also I'm curious about truncating the log.
				funcOp(func(ctx context.Context, t *testing.T, c *testCluster) {
					sem := make(chan struct{}, 500)
					// Note that with a large value here this test fails due to the need
					// to send a snapshot.
					const N = 10000
					var wg sync.WaitGroup
					wg.Add(N)
					for i := 0; i < N; i++ {
						sem <- struct{}{}
						go func(i int) {
							defer func() { <-sem }()
							defer wg.Done()
							k := roachpb.Key(strconv.Itoa(i))
							reqOp{i % 3, putOp{k, float64(i)}}.run(ctx, t, c)
						}(i)
					}
					wg.Wait()

				}),
				// Perform the Config Change
				funcOp(func(ctx context.Context, t *testing.T, c *testCluster) {
					var br roachpb.BatchRequest
					acrr := roachpb.AdminChangeReplicasRequest{
						ChangeType: roachpb.ADD_REPLICA,
						Targets: []roachpb.ReplicationTarget{
							{NodeID: 4, StoreID: 4},
						},
					}
					br.Add(&acrr)
					_, err := c.stores[0].Batch(ctx, &br)
					require.Nil(t, err)
				}),
				// Create the new store.
				funcOp(func(ctx context.Context, t *testing.T, c *testCluster) {
					var br roachpb.BatchRequest
					gr := roachpb.GetRequest{}
					descKey := keys.RangeDescriptorKey(roachpb.RKeyMin)
					gr.Key = descKey
					br.Add(&gr)
					resp, err := c.stores[2].Batch(ctx, &br)
					require.Nil(t, err)
					var desc roachpb.RangeDescriptor
					err = resp.Responses[0].GetInner().(*roachpb.GetResponse).Value.GetProto(&desc)
					require.Nil(t, err)
					err = engine.MVCCPutProto(ctx, c.cfgs[3].Engine, nil,
						descKey, hlc.Timestamp{}, nil, &desc)
					require.Nil(t, err)
					s4, err := kvtoy.NewStore(ctx, c.cfgs[3])
					require.Nil(t, err)
					c.stores = append(c.stores, s4)
				}),
				reqOp{3, getOp{key: k, expValue: 1}},
				reqOp{3, putOp{key: k, value: 2}},
				reqOp{2, getOp{key: k, expValue: 2}},
				reqOp{3, getOp{key: roachpb.Key(strconv.Itoa(1)), expValue: 1}},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, tc.run)
	}
}

type testCase struct {
	name         string
	numNodes     int
	initialNodes int
	ops          []op
}

func (tc testCase) run(t *testing.T) {
	ctx := context.Background()
	c := newTestCluster(ctx, t, tc.numNodes, tc.initialNodes)
	defer c.cleanup()
	for _, op := range tc.ops {
		op.run(ctx, t, c)
	}
}

type funcOp func(ctx context.Context, t *testing.T, tc *testCluster)

func (fo funcOp) run(ctx context.Context, t *testing.T, tc *testCluster) {
	t.Helper()
	fo(ctx, t, tc)
}

type op interface {
	run(ctx context.Context, t *testing.T, tc *testCluster)
}

type reqOp struct {
	store int
	op    storeOp
}

func (ro reqOp) run(ctx context.Context, t *testing.T, tc *testCluster) {
	t.Helper()
	ro.op.run(ctx, t, tc.stores[ro.store])
}

type storeOp interface {
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
	require.NotNilf(t, resp.Responses[0].GetGet().Value, "%v", o)
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

func newTestCluster(
	ctx context.Context, t *testing.T, numNodes int, initialNodes int,
) *testCluster {
	cfgs := make([]kvtoy.Config, 0, numNodes)
	stores := make([]*kvtoy.Store, 0, numNodes)
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
		require.Nil(t, kvtoy.WriteInitialClusterData(ctx, cfg.Engine, 1, nodeIDs[:initialNodes]...))
		if i <= initialNodes {
			s, err := kvtoy.NewStore(ctx, cfg)
			require.Nil(t, err)
			stores = append(stores, s)
		}
		rpcContexts = append(rpcContexts, rpcCtx)
		listeners = append(listeners, l)
		cfgs = append(cfgs, cfg)
		servers = append(servers, server)
	}
	return &testCluster{
		stores: stores,
		cfgs:   cfgs,
	}
}

type testCluster struct {
	stores []*kvtoy.Store
	cfgs   []kvtoy.Config
}

func (tc *testCluster) cleanup() {
	var wg sync.WaitGroup
	wg.Add(len(tc.cfgs))
	for i := range tc.cfgs {
		go func(i int) {
			tc.cfgs[i].Stopper.Stop(context.TODO())
			wg.Done()
		}(i)
	}
	wg.Wait()
	for i := range tc.cfgs {
		tc.cfgs[i].Engine.Close()
	}
}
