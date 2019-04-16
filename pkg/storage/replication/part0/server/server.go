package server

import (
	"context"
	"net"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/part0/kvtoy"
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

	Stopper *stop.Stopper
}

type Server struct {
	cfg Config

	rpcCtx     *rpc.Context
	grpcServer *grpc.Server
	store      *kvtoy.Store
}

// NewServer creates a new server with in-memory storage.
func NewServer(ctx context.Context, cfg Config) (*Server, error) {
	// Make an engine and a server.
	eng := engine.NewInMem(roachpb.Attributes{}, 1<<26 /* 64 MB */)
	clock := hlc.NewClock(hlc.UnixNano, 0)
	baseConfig := base.Config{
		Insecure: true,
		Addr:     cfg.Addr,
	}
	settings := cluster.MakeTestingClusterSettings()
	rpcCtx := rpc.NewContext(
		cfg.Ambient,
		&baseConfig,
		clock,
		cfg.Stopper,
		&settings.Version,
	)
	grpcServer := rpc.NewServer(rpcCtx)
	storeCfg := kvtoy.Config{
		Engine: eng,
	}
	store := kvtoy.NewStore(storeCfg)
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
