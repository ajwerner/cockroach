package cli

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/part1/kvtoy"
	"github.com/cockroachdb/cockroach/pkg/storage/replication/part1/server"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
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

func init() {
	runtime.SetMutexProfileFraction(1000)
	runtime.SetBlockProfileRate(1000)
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
	cmd := &cobra.Command{
		Use:  "start",
		Args: cobra.NoArgs,
	}
	var (
		addrMap     = server.AddrMap{}
		storagePath string
		myNodeID    int
		init        bool
		pprofAddr   string
		vmodule     string
	)
	flags := cmd.Flags()
	flags.Var(addrMapFlag(addrMap), "addresses", "Addresses of the other nodes, should be repeated values of <nodeID>@<addr>")
	flags.IntVar(&myNodeID, "node-id", 1, "Node ID for this node to use")
	flags.BoolVar(&init, "init", false, "Initialize the cluster state")
	flags.StringVar(&storagePath, "storage", "", "Directory to store rocksdb state, in-memory if empty")
	flags.StringVar(&pprofAddr, "pprof", "", "Address on which to serve pprof endpoints")
	flags.StringVar(&vmodule, "vmodule", "", "vmodule level")
	var cfg server.Config
	cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		if pprofAddr != "" {
			go func() {
				log.Infof(context.TODO(), "failed to serve pprof at %v: %v",
					pprofAddr, http.ListenAndServe(pprofAddr, nil))
			}()
		}

		if myNodeID <= 0 {
			return errors.Errorf("Invalid negative node id")
		}
		cfg.NodeID = roachpb.NodeID(myNodeID)
		if addr, exists := addrMap[cfg.NodeID]; exists {
			cfg.Addr = addr.String()
		} else {
			return errors.Errorf("No address for own node id %v found in addresses", cfg.NodeID)
		}
		cfg.AddrMap = addrMap
		if storagePath == "" {
			cfg.Engine = engine.NewInMem(roachpb.Attributes{}, 1<<26 /* 64 MB */)
		} else {
			cache := engine.NewRocksDBCache(1 << 20)
			eng, err := engine.NewRocksDB(engine.RocksDBConfig{Dir: storagePath}, cache)
			if err != nil {
				return errors.Wrap(err, "failed to create rocks db engine")
			}
			cfg.Engine = eng
		}
		if vmodule != "" {
			log.SetVModule(vmodule)
		}
		return nil
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		if init {
			nodes := make([]int, 0, len(cfg.AddrMap))
			for id := range cfg.AddrMap {
				nodes = append(nodes, int(id))
			}
			if err := kvtoy.WriteInitialClusterData(ctx, cfg.Engine, 1, nodes...); err != nil {
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
	}

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
			start := time.Now()
			if txn.Run(context.TODO(), b); err != nil {
				return err
			}
			fmt.Printf("took %v\n", time.Since(start))
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

// batchCmd is going to create a client and issue a batch to the server.
// This is critical for testing.
var workloadCmd = func() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "wl",
		Args: cobra.ExactArgs(1),
	}
	var (
		readPercent float64
		duration    time.Duration
		concurrency int
	)
	flags := cmd.Flags()
	flags.StringVar(&cfg.addr, "addr", DefaultAddr, "Address for client to connect to.")
	flags.DurationVar(&duration, "duration", 10*time.Second, "test duration")
	flags.Float64Var(&readPercent, "read-percent", 0, "read pecentage")
	flags.IntVar(&concurrency, "concurrency", 1, "test concurrency")
	var reads, writes int64
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		deadline := time.Now().Add(duration)
		g, gCtx := errgroup.WithContext(context.Background())
		runOne := func() error {
			db, err := newClient()
			if err != nil {
				return err
			}
			for time.Now().Before(deadline) {
				if choice := rand.Float64(); choice < readPercent {
					if _, err := get(gCtx, db, "asdf"); err != nil {
						return err
					}
					atomic.AddInt64(&reads, 1)
				} else {
					if err := put(gCtx, db, "asdf", rand.Float64()); err != nil {
						return err
					}
					atomic.AddInt64(&writes, 1)
				}
			}
			return nil
		}

		for i := 0; i < concurrency; i++ {
			g.Go(runOne)
		}
		if err := g.Wait(); err != nil {
			return err
		}
		fmt.Printf("reads: %v (%v qps); writes: %v (%v qps)\n", reads, float64(reads)/duration.Seconds(),
			writes, float64(writes)/duration.Seconds())
		return nil
	}

	return cmd
}()

func get(ctx context.Context, db *client.DB, key string) (*roachpb.Value, error) {
	txn := client.NewTxn(ctx, db, 0, client.RootTxn)
	b := txn.NewBatch()
	b.Get(key)
	if err := txn.Run(ctx, b); err != nil {
		return nil, err
	}
	return b.Results[0].Rows[0].Value, nil
}

func put(ctx context.Context, db *client.DB, key string, val interface{}) error {
	txn := client.NewTxn(ctx, db, 0, client.RootTxn)
	b := txn.NewBatch()
	b.Put(key, val)
	return txn.Run(ctx, b)
}

func init() {
	kvtoyCmd.AddCommand(
		startCmd,
		workloadCmd,
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
