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
	"github.com/cockroachdb/cockroach/pkg/connect"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/part1/kvtoy"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/rafttransport"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	const numNodes = 3
	stores, cleanup := setUpToyStoresWithRealRPCs(ctx, t, numNodes)
	defer cleanup()
	s1 := stores[0]
	s2 := stores[1]

	resp, err := s1.Batch(ctx, put(roachpb.Key("asdf"), 1))
	require.Nil(t, err)
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
	resp, err = s2.Batch(ctx, cput(roachpb.Key("asdf"), 2.0, 3.0))
	assert.Nil(t, err)
	resp, err = s2.Batch(ctx, get(roachpb.Key("asdf")))
	assert.Nil(t, err)
	respVal, err = resp.Responses[0].GetGet().Value.GetFloat()
	assert.Nil(t, err)
	assert.Equal(t, respVal, 3.0)
}

func put(key roachpb.Key, value float64) *roachpb.BatchRequest {
	var br roachpb.BatchRequest
	pr := roachpb.PutRequest{}
	pr.Key = key
	pr.Value.SetFloat(value)
	br.Add(&pr)
	return &br
}

func cput(key roachpb.Key, exp, value float64) *roachpb.BatchRequest {
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

func get(key roachpb.Key) *roachpb.BatchRequest {
	var br roachpb.BatchRequest
	pr := roachpb.GetRequest{}
	pr.Key = key
	br.Add(&pr)
	return &br
}

type raftTransport struct {
	stopper *stop.Stopper
	send    *rafttransport.RaftTransport
	recv    chan connect.Message
}

func (rt *raftTransport) Recv() connect.Message {
	select {
	case msg := <-rt.recv:
		return msg
	case <-rt.stopper.ShouldQuiesce():
		return nil
	}
}

func (rt *raftTransport) Send(ctx context.Context, msg connect.Message) {
	switch msg := msg.(type) {
	case *rafttransport.RaftMessageRequest:
		rt.send.SendAsync(msg)
	default:
		panic(errors.Errorf("unknown message type %T", msg))
	}
}

func (rt *raftTransport) Close(ctx context.Context, drain bool) {
	log.Errorf(ctx, "Got unhandled close")
}

func newRaftTransport(
	storeID roachpb.StoreID, stopper *stop.Stopper, rt *rafttransport.RaftTransport,
) *raftTransport {
	t := &raftTransport{
		send: rt,
		// TODO(ajwerner): give this a buffer when I figure it out.
		recv:    make(chan connect.Message),
		stopper: stopper,
	}
	rt.Listen(storeID, t)
	return t
}

func (rt *raftTransport) HandleRaftRequest(
	ctx context.Context,
	req *rafttransport.RaftMessageRequest,
	respStream rafttransport.RaftMessageResponseStream,
) *roachpb.Error {
	select {
	case rt.recv <- req:
		return nil
	case <-rt.stopper.ShouldQuiesce():
		return roachpb.NewError(stop.ErrUnavailable)
	}
}

func (rt *raftTransport) HandleRaftResponse(
	ctx context.Context, resp *rafttransport.RaftMessageResponse,
) error {
	log.Errorf(ctx, "Got unhandled RaftResponse %v", resp)
	return nil
}

func (rt *raftTransport) HandleSnapshot(
	header *rafttransport.SnapshotRequest_Header, respStream rafttransport.SnapshotResponseStream,
) error {
	panic("not implemented")
}

func setUpToyStores(
	ctx context.Context, t *testing.T, numNodes int,
) (stores []*kvtoy.Store, cleanup func()) {

	cfgs := make([]kvtoy.Config, 0, numNodes)
	stores = make([]*kvtoy.Store, 0, numNodes)
	listeners := make([]net.Listener, 0, numNodes)
	rpcContexts := make([]*rpc.Context, 0, numNodes)
	servers := make([]*grpc.Server, 0, numNodes)
	raftTransports := make([]*rafttransport.RaftTransport, 0, numNodes)
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
		raftTransport := rafttransport.NewRaftTransport(cfg.Ambient, cfg.Settings,
			nodeDialer, server, cfg.Stopper)
		cfg.RaftTransport = newRaftTransport(cfg.StoreID, cfg.Stopper, raftTransport)
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
