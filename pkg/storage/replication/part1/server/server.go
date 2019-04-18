package server

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/raftentry"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/part1/kvtoy"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/rafttransport"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type Config struct {
	Ambient log.AmbientContext

	// Addr is the address of the server.
	Addr string

	NodeID roachpb.NodeID

	AddrMap AddrMap

	Stopper *stop.Stopper

	Engine engine.Engine
}

type AddrMap map[roachpb.NodeID]net.Addr

func (am AddrMap) LookupAddr(id roachpb.NodeID) (net.Addr, error) {
	if addr, ok := am[id]; ok {
		return addr, nil
	}
	return nil, fmt.Errorf("failed to resolve node %d to an address", id)
}

type Server struct {
	cfg Config

	rpcCtx     *rpc.Context
	grpcServer *grpc.Server
	store      *kvtoy.Store
}

// NewServer creates a new server with in-memory storage.
func NewServer(ctx context.Context, cfg Config) (*Server, error) {
	// Make an engine and a server
	clock := hlc.NewClock(hlc.UnixNano, 0)
	baseConfig := base.Config{
		Insecure:          true,
		Addr:              cfg.Addr,
		HeartbeatInterval: 2 * time.Second,
	}
	// NodeID and StoreID are always the same for this part.
	storeID := roachpb.StoreID(cfg.NodeID)
	cfg.Ambient.AddLogTag("s", storeID)
	settings := cluster.MakeTestingClusterSettings()
	rpcCtx := rpc.NewContext(
		cfg.Ambient,
		&baseConfig,
		clock,
		cfg.Stopper,
		&settings.Version,
	)
	nodeDialer := nodedialer.New(rpcCtx, cfg.AddrMap.LookupAddr)
	grpcServer := rpc.NewServer(rpcCtx)
	raftTransport := rafttransport.NewRaftTransport(
		cfg.Ambient,
		settings,
		nodeDialer,
		grpcServer,
		cfg.Stopper,
	)

	storeCfg := kvtoy.Config{
		Ambient:  cfg.Ambient,
		Settings: settings,
		Stopper:  cfg.Stopper,

		NodeID:  cfg.NodeID,
		StoreID: storeID,

		Engine:        cfg.Engine,
		Clock:         clock,
		EntryCache:    raftentry.NewCache(1 << 20 /* 1MB */),
		RaftTransport: raftTransport,
	}
	storeCfg.RaftConfig.SetDefaults()
	storeCfg.RaftConfig.RaftHeartbeatIntervalTicks = 1
	storeCfg.RaftConfig.RaftElectionTimeoutTicks = 3
	storeCfg.RaftConfig.RaftTickInterval = 100 * time.Millisecond
	store, err := kvtoy.NewStore(ctx, storeCfg)
	if err != nil {
		return nil, err
	}
	roachpb.RegisterInternalServer(grpcServer, store)
	listener, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to listen on %v", cfg.Addr)
	}
	if err := cfg.Stopper.RunAsyncTask(ctx, "server.Serve", func(ctx context.Context) {
		err := grpcServer.Serve(listener)
		if err != nil {
			log.Errorf(ctx, "server exiting with error: %v", err)
		} else {
			log.Infof(ctx, "server exiting")
		}
	}); err != nil {
		panic(errors.Wrap(err, "failed to run async task"))
	}
	if err := cfg.Stopper.RunAsyncTask(ctx, "server.closeOnQuiesce", func(ctx context.Context) {
		<-cfg.Stopper.ShouldQuiesce()
		grpcServer.Stop()
	}); err != nil {
		panic(errors.Wrap(err, "failed to run async task"))
	}
	return &Server{
		cfg:        cfg,
		rpcCtx:     rpcCtx,
		grpcServer: grpcServer,
		store:      store,
	}, nil
}
