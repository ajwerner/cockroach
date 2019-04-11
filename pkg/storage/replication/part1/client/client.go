// Package client provides a simple client the kvtoy.
package client

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/kvtoy/kvtoypb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

type Config struct {
	Ambient log.AmbientContext
	Stopper *stop.Stopper
	Addr    string
}

type Client struct {
	Client kvtoypb.InternalClient
}

// NewClient creates a new Client.
func NewClient(cfg Config) (*Client, error) {
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
	rpcConn := rpcCtx.GRPCDial(cfg.Addr)
	conn, err := rpcConn.Connect(context.TODO())
	if err != nil {
		return nil, err
	}
	return &Client{Client: kvtoypb.NewInternalClient(conn)}, nil
}
