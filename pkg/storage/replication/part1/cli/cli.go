package cli

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/part1/kvtoy"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/part1/server"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

const DefaultAddr = ":10103"

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

type addrMapFlag server.AddrMap

func (amf addrMapFlag) String() string { return fmt.Sprintf("%v", server.AddrMap(amf)) }
func (amf addrMapFlag) Set(val string) error {
	// we want to split on @ where the first is the node id and the second is the
	// address.
	atIdx := strings.Index(val, "@")
	if atIdx == -1 {
		return errors.Errorf("malformed node address mapping, no @ found")
	}
	idInt, err := strconv.Atoi(val[:atIdx])
	if err != nil {
		return errors.Wrap(err, "failed to parse node ID from address mapping")
	}
	id := roachpb.NodeID(idInt)
	addr, err := net.ResolveTCPAddr("tcp", val[atIdx+1:])
	if err != nil {
		return errors.Wrap(err, "failed to parse addr from address mapping")
	}
	if existing, exists := amf[id]; exists {
		return errors.Errorf("duplicate node address mapping for %d: %v and %v", id, existing, addr)
	}
	amf[id] = addr
	return nil
}

func (amf addrMapFlag) Type() string { return "address map" }

var startCmd = func() *cobra.Command {
	cfg := server.Config{
		AddrMap: server.AddrMap{},
	}
	var myNodeID int
	var init bool
	cmd := &cobra.Command{
		Use: "start",
		Run: wrapRun(func(cmd *cobra.Command, args []string) error {
			if myNodeID <= 0 {
				return errors.Errorf("Invalid negative node id")
			}
			cfg.NodeID = roachpb.NodeID(myNodeID)
			if addr, exists := cfg.AddrMap[cfg.NodeID]; exists {
				cfg.Addr = addr.String()
			} else {
				return errors.Errorf("No address for own node id %v found in addresses", cfg.NodeID)
			}
			ctx := context.Background()
			cfg.Engine = engine.NewInMem(roachpb.Attributes{}, 1<<26 /* 64 MB */)
			if init {
				nodes := make([]int, 0, len(cfg.AddrMap))
				for id := range cfg.AddrMap {
					nodes = append(nodes, int(id))
				}
				rsl := stateloader.Make(1)
				if err := kvtoy.WriteInitialClusterData(ctx, cfg.Engine, rsl, nodes...); err != nil {
					return err
				}
			}
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
	flags.IntVar(&myNodeID, "node-id", 1, "Node ID for this node to use")
	flags.BoolVar(&init, "init", false, "Initialize the cluster state")
	flags.Var(addrMapFlag(cfg.AddrMap), "addresses", "Addresses of the other nodes, should be repeated values of <nodeID>@<addr>")
	// flags.StringVar(&cfg.Addr, "addr", DefaultAddr, "Address on which the server will listen")
	return cmd
}()

// batchCmd is going to create a client and issue a batch to the server.
// This is critical for testing.
var putCmd = func() *cobra.Command {
	cmd := &cobra.Command{
		Use: "put",
		Run: wrapRun(func(cmd *cobra.Command, args []string) error {
			db, err := newClient()
			if err != nil {
				return err
			}
			txn := client.NewTxn(context.TODO(), db, 0, client.RootTxn)
			b := txn.NewBatch()
			b.Put(args[0], args[1])
			if txn.Run(context.TODO(), b); err != nil {
				return err
			}
			fmt.Printf("put %v = %v\n", args[0], args[1])
			return nil
		}),
		Args: cobra.ExactArgs(2),
	}
	flags := cmd.Flags()
	flags.StringVar(&cfg.addr, "addr", DefaultAddr, "Address for client to connect to.")
	return cmd
}()

// batchCmd is going to create a client and issue a batch to the server.
// This is critical for testing.
var getCmd = func() *cobra.Command {
	cmd := &cobra.Command{
		Use: "get",
		Run: wrapRun(func(cmd *cobra.Command, args []string) error {
			db, err := newClient()
			if err != nil {
				return err
			}
			txn := client.NewTxn(context.TODO(), db, 0, client.RootTxn)
			b := txn.NewBatch()
			b.Get(args[0])
			if txn.Run(context.TODO(), b); err != nil {
				return err
			}
			if len(b.Results) < 1 || len(b.Results[0].Rows) < 1 {
				return errors.Errorf("unexpected empty results: %v", b.Results)
			}
			if b.Results[0].Rows[0].Value == nil {
				fmt.Printf("get: %v not found\n", args[0])
			} else {
				data, err := b.Results[0].Rows[0].Value.GetBytes()
				if err != nil {
					return errors.Wrapf(err, "failed to decode value at key %q as bytes", args[0])
				}
				fmt.Printf("get: %v = %v\n", args[0], string(data))
			}
			return nil
		}),
		Args: cobra.ExactArgs(1),
	}
	flags := cmd.Flags()
	flags.StringVar(&cfg.addr, "addr", DefaultAddr, "Address for client to connect to.")
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
