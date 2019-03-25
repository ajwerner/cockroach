package quota

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

func TestQuota(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	s := cluster.MakeTestingClusterSettings()
	defer stopper.Stop(ctx)

	qp := NewReadQuota(ctx, stopper, Config{Settings: s})
	const N = 1000
	quotas := make([]Quota, N)
	for i := 0; i < N; i++ {
		q, err := qp.Acquire(ctx, false)
		if err != nil {
			panic(err)
		}
		quotas[i] = q
	}
	time.Sleep(time.Second)
	m := qp.Metrics()
	fmt.Printf("%+#v %v\n", qp, m.InUse.Value())
	for i := 0; i < N; i++ {
		qp.Release(quotas[i], 128*1<<10)
	}
}
