package cli

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

var cfg = config{
	ambient: log.AmbientContext{
		Tracer: tracing.NewTracer(),
	},
	stopper: stop.NewStopper(),
}

type config struct {
	ambient log.AmbientContext
	stopper *stop.Stopper
	addr    string
}

// newClient creates a new Client.
func newClient() (*client.DB, error) {
	clock := hlc.NewClock(hlc.UnixNano, 0)
	baseConfig := base.Config{
		Insecure: true,
		Addr:     cfg.addr,
	}
	settings := cluster.MakeTestingClusterSettings()
	rpcCtx := rpc.NewContext(
		cfg.ambient,
		&baseConfig,
		clock,
		cfg.stopper,
		&settings.Version,
	)
	rpcConn := rpcCtx.GRPCDial(cfg.addr)
	conn, err := rpcConn.Connect(context.TODO())
	if err != nil {
		return nil, err
	}
	rpcClient := roachpb.NewInternalClient(conn)
	sender := client.MakeMockTxnSenderFactory(
		func(ctx context.Context, _ *roachpb.Transaction, ba roachpb.BatchRequest,
		) (*roachpb.BatchResponse, *roachpb.Error) {
			br, err := rpcClient.Batch(ctx, &ba)
			if err != nil {
				return nil, roachpb.NewError(err)
			}
			return br, nil
		})
	return client.NewDB(cfg.ambient, sender, clock), nil
}
