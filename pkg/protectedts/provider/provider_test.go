package provider_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/protectedts"
	"github.com/cockroachdb/cockroach/pkg/protectedts/provider"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestBuildProtectQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 5, base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
	})
	defer tc.Stopper().Stop(ctx)

	p := provider.New(tc.Server(0).InternalExecutor().(*sql.InternalExecutor))

	ts, err := p.Protect(ctx, nil, tc.Server(0).Clock().Now(), "", nil, []roachpb.Span{
		{[]byte("asdf"), []byte("asdf")},
	})
	require.NoError(t, err)
	t.Log(ts)
	got, err := p.List(ctx)
	require.NoError(t, err)
	require.EqualValues(t, []protectedts.ProtectedTS{*ts}, got)
}
