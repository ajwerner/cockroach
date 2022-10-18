package multiregionccl

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils/regionlatency"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestColdStartLatency attempts to capture the cold start latency for
// sql pods given different cluster topologies.
//
// The test ought to allow us to run many experiments to understand what the
// cold start ought to be. In the first pass, we'll actually incur the latency
// penalty. In future passes, we may want to find ways to not actually inject
// the latency but to figure out how long the critical path might have taken.
func TestColdStartLatency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We'll need to make some per-node args to assign the different
	// KV nodes to different regions and AZs. We'll want to do it to
	// look somewhat like the real cluster topologies we have in mind.
	//
	// Initially we'll want 18 nodes, 6 per region in 3 regions with
	// 2 per AZ. We can tune the various latencies between these regions.

	regionLatencies := regionlatency.RoundTripPairs{
		{A: "us-east1", B: "us-west1"}:     66 * time.Millisecond,
		{A: "us-east1", B: "europe-west1"}: 64 * time.Millisecond,
		{A: "us-west1", B: "europe-west1"}: 146 * time.Millisecond,
	}.ToLatencyMap()
	const (
		numNodes        = 9
		numAZsPerRegion = 3
	)
	localities := makeLocalities(regionLatencies, numNodes, numAZsPerRegion)
	perServerArgs := make(map[int]base.TestServerArgs, numNodes)
	pauseAfter := make(chan struct{})
	signalAfter := make([]chan struct{}, numNodes)
	var enabled syncutil.AtomicBool
	for i := 0; i < numNodes; i++ {
		args := base.TestServerArgs{
			DisableDefaultTestTenant: true,
			Locality:                 localities[i],
		}
		signalAfter[i] = make(chan struct{})
		args.Knobs.Server = &server.TestingKnobs{
			PauseAfterGettingRPCAddress:  pauseAfter,
			SignalAfterGettingRPCAddress: signalAfter[i],
			ContextTestingKnobs: rpc.ContextTestingKnobs{
				InjectedLatencyOracle:  rpc.InjectedLatencyMap{},
				InjectedLatencyEnabled: &enabled,
			},
		}
		perServerArgs[i] = args
	}
	tc := testcluster.NewTestCluster(t, numNodes, base.TestClusterArgs{
		ParallelStart:     true,
		ServerArgsPerNode: perServerArgs,
	})
	go func() {
		fmt.Println("waiting")
		for i, c := range signalAfter {
			fmt.Println("waited for ", i)
			<-c
		}
		fmt.Println("applying latencies")
		regionLatencies.Apply(tc)
		close(pauseAfter)
	}()
	tc.Start(t)
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	for i := 0; i < numNodes; i++ {
		fmt.Println(i, tc.Server(i).SQLAddr())
	}

	tenantServerKnobs := func(i int) *server.TestingKnobs {
		return &server.TestingKnobs{
			ContextTestingKnobs: rpc.ContextTestingKnobs{
				InjectedLatencyOracle: tc.Server(i).TestingKnobs().
					Server.(*server.TestingKnobs).ContextTestingKnobs.
					InjectedLatencyOracle,
				InjectedLatencyEnabled: &enabled,
			},
		}
	}
	const password = "asdf"
	{
		tenant, tenantDB := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{
			TenantID: serverutils.TestTenantID(),
			TestingKnobs: base.TestingKnobs{
				Server: tenantServerKnobs(0),
			},
			Locality: localities[0],
		})
		tdb := sqlutils.MakeSQLRunner(tenantDB)
		tdb.Exec(t, "CREATE USER foo PASSWORD $1 LOGIN", password)
		tdb.Exec(t, "GRANT admin TO foo")
		tenant.Stopper().Stop(ctx)
	}
	doTest := func(wg *sync.WaitGroup, i int, duration *time.Duration) {
		defer wg.Done()
		start := timeutil.Now()
		sn := tenantServerKnobs(i)
		tenant, err := tc.Server(i).StartTenant(ctx, base.TestTenantArgs{
			TenantID:            serverutils.TestTenantID(),
			Existing:            true,
			DisableCreateTenant: true,
			TestingKnobs: base.TestingKnobs{
				Server: sn,
			},
			Locality: localities[i],
		})
		require.NoError(t, err)
		pgURL, cleanup, err := sqlutils.PGUrlWithOptionalClientCertsE(
			tenant.SQLAddr(), "tenantdata", url.UserPassword("foo", password),
			false, // withClientCerts
		)
		if !assert.NoError(t, err) {
			return
		}
		defer cleanup()
		pgURL.Path = "defaultdb"
		conn, err := pgx.Connect(ctx, pgURL.String())
		if !assert.NoError(t, err) {
			return
		}
		var one int
		assert.NoError(t, conn.QueryRow(ctx, "SELECT 1").Scan(&one))
		*duration = timeutil.Since(start)
	}
	runAllTests := func() []time.Duration {
		latencyResults := make([]time.Duration, numNodes)
		var wg sync.WaitGroup
		for i := 0; i < numNodes; i++ {
			wg.Add(1)
			go doTest(&wg, i, &latencyResults[i])
		}
		wg.Wait()
		return latencyResults
	}

	for i := 0; i < 2; i++ {
		res := runAllTests()
		for i, l := range res {
			t.Log(localities[i].String(), l)
		}
		enabled.Set(true)
	}
}

func makeLocalities(
	lm regionlatency.LatencyMap, numNodes, azsPerRegion int,
) (ret []roachpb.Locality) {
	regions := lm.GetRegions()
	for regionIdx, nodesInRegion := range distribute(numNodes, len(regions)) {
		for azIdx, nodesInAZ := range distribute(nodesInRegion, azsPerRegion) {
			for i := 0; i < nodesInAZ; i++ {
				ret = append(ret, roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "region", Value: regions[regionIdx]},
						{Key: "az", Value: string(rune('a' + azIdx))},
					},
				})
			}
		}
	}
	return ret
}

func distribute(total, num int) []int {
	res := make([]int, num)
	for i := range res {
		// Use the average number of remaining connections.
		div := len(res) - i
		res[i] = (total + div/2) / div
		total -= res[i]
	}
	return res
}

type concurrentLatencyMap struct {
	mu struct {
		syncutil.RWMutex
		m rpc.InjectedLatencyMap
	}
}

func (c *concurrentLatencyMap) GetLatency(addr string) time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.m.GetLatency(addr)
}

func (c *concurrentLatencyMap) SetLatency(addr string, l time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.m.SetLatency(addr, l)
}

func newConcurrentLatencyMap() *concurrentLatencyMap {
	m := concurrentLatencyMap{}
	m.mu.m = make(rpc.InjectedLatencyMap)
	return &m
}

var _ rpc.InjectedLatencyOracle = (*concurrentLatencyMap)(nil)
