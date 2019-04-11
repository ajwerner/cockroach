package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/kvtoy/kvtoypb"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/part1/client"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/part1/server"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// Main is the entry point for the cli, with a single line calling it intended
// to be the body of an action package main `main` func elsewhere. It is
// abstracted for reuse by duplicated `main` funcs in different distributions.
func Main() {
	Run(os.Args[1:])
}

func Run(args []string) {
	kvtoyCmd.SetArgs(args)
	kvtoyCmd.Execute()
}

var kvtoyCmd = &cobra.Command{
	Use:          "kvtoy [command] (flags)",
	Short:        "kvtoy command-line interface and server",
	SilenceUsage: true,
}

var startCmd = func() *cobra.Command {
	var cfg server.Config
	cmd := &cobra.Command{
		Use: "start",
		Run: wrapRun(func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			cfg.Stopper = stop.NewStopper()
			_, err := server.NewServer(ctx, cfg)
			if err != nil {
				return err
			}
			// TODO(ajwerner): deal with signals
			<-cfg.Stopper.IsStopped()
			return nil
		}),
		Args: cobra.NoArgs,
	}
	flags := cmd.Flags()
	flags.StringVar(&cfg.Addr, "addr", ":0", "Address on which the server will listen")
	return cmd
}()

// batchCmd is going to create a client and issue a batch to the server.
// This is critical for testing.
var putCmd = func() *cobra.Command {
	var cfg client.Config
	cmd := &cobra.Command{
		Use: "put",
		Run: wrapRun(func(cmd *cobra.Command, args []string) error {
			cfg.Stopper = stop.NewStopper()
			client, err := client.NewClient(cfg)
			if err != nil {
				return err
			}
			var ba kvtoypb.BatchRequest
			ba.Requests = make([]kvtoypb.RequestUnion, 1)
			pr := kvtoypb.PutRequest{}
			pr.Key = roachpb.Key(args[0])
			pr.Value.SetString(args[1])
			ba.Requests[0].SetInner(&pr)
			_, err = client.Client.Batch(context.TODO(), &ba)
			if err != nil {
				return err
			}
			fmt.Printf("put %v = %v\n", pr.Key, pr.Value)
			return nil
		}),
		Args: cobra.ExactArgs(2),
	}
	flags := cmd.Flags()
	flags.StringVar(&cfg.Addr, "addr", "", "Address for client to connect to.")
	return cmd
}()

// batchCmd is going to create a client and issue a batch to the server.
// This is critical for testing.
var getCmd = func() *cobra.Command {
	var cfg client.Config
	cmd := &cobra.Command{
		Use: "get",
		Run: wrapRun(func(cmd *cobra.Command, args []string) error {
			cfg.Stopper = stop.NewStopper()
			client, err := client.NewClient(cfg)
			if err != nil {
				return err
			}
			var ba kvtoypb.BatchRequest
			ba.Requests = make([]kvtoypb.RequestUnion, 1)
			gr := kvtoypb.GetRequest{}
			gr.Key = roachpb.Key(args[0])
			ba.Requests[0].SetInner(&gr)
			resp, err := client.Client.Batch(context.TODO(), &ba)
			if err != nil {
				return err
			}
			getResp := resp.Responses[0].GetInner().(*kvtoypb.GetResponse)
			respBytes, err := getResp.Value.GetBytes()
			if err != nil {
				return errors.Wrapf(err, "failed to decode value %v as bytes", getResp.Value.RawBytes)
			}
			fmt.Printf("get %v = %v\n", gr.Key, string(respBytes))
			return nil
		}),
		Args: cobra.ExactArgs(1),
	}
	flags := cmd.Flags()
	flags.StringVar(&cfg.Addr, "addr", "", "Address for client to connect to.")
	return cmd
}()

func init() {
	kvtoyCmd.AddCommand(
		startCmd,
		putCmd,
		getCmd,
	)
}

func wrapRun(
	f func(cmd *cobra.Command, args []string) error,
) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		if err := f(cmd, args); err != nil {
			fmt.Fprintf(os.Stderr, "%v: %v\n", cmd.Name(), err)
			os.Exit(1)
		}
	}
}
