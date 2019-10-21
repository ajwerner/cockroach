package provider_test

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/protectedts"
	"github.com/cockroachdb/cockroach/pkg/protectedts/provider"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestProtectQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
	})
	defer tc.Stopper().Stop(ctx)

	s0 := tc.Server(0)
	ex := s0.InternalExecutor().(*sql.InternalExecutor)
	// TODO(ajwerner): get rid of this bootstrapping
	_, err := ex.Exec(ctx, "bootstap", nil, "INSERT INTO system.protected_ts_meta (rows, spans, version) VALUES (0, 0, 0)")
	require.NoError(t, err)
	p := provider.New(ex, s0.DB())

	// Add one protected ts with 2 spans
	ts, err := p.Protect(ctx, nil, s0.Clock().Now(), "", nil, makeTableSpan(42), makeTableSpan(43))
	require.NoError(t, err)
	got, err := p.List(ctx)
	require.NoError(t, err)

	row, err := ex.QueryRow(ctx, "bootstap", nil, "SELECT * FROM system.protected_ts_meta")
	require.NoError(t, err)
	require.Nil(t, row, fmt.Sprint(row))

	// Add 9 more (which should be allowed).
	list := []protectedts.ProtectedTS{*ts}
	require.EqualValues(t, list, got.Timestamps)
	require.EqualValues(t, 1, got.Version)
	for i := 0; i < 9; i++ {
		ts, err := p.Protect(ctx, nil, s0.Clock().Now(), "", nil, makeTableSpan(uint32(i)), makeTableSpan(uint32(i+1)))
		require.NoError(t, err)
		list = append(list, *ts)
	}
	sort.Slice(list, func(a, b int) bool {
		return bytes.Compare(list[a].ID[:], list[b].ID[:]) < 0
	})
	got, err = p.List(ctx)
	require.NoError(t, err)
	require.EqualValues(t, list, got.Timestamps)
	require.EqualValues(t, 10, got.Version)

	// Try to add another row which should fail.
	ts, err = p.Protect(ctx, nil, s0.Clock().Now(), "", nil, makeTableSpan(0))
	require.Nil(t, ts)
	require.True(t, testutils.IsError(err, "limits exceeded: 10\\+1 > 10 rows"), err.Error())

	// Release one, this should increment the version and remove the first
	// of the list from the set.
	err = p.Release(ctx, nil, list[0].ID)
	require.NoError(t, err)
	got, err = p.List(ctx)
	require.NoError(t, err)
	require.EqualValues(t, list[1:], got.Timestamps)
	require.EqualValues(t, 11, got.Version)

	// Release another one, this should remove
	err = p.Release(ctx, nil, list[1].ID)
	require.NoError(t, err)
	got, err = p.List(ctx)
	require.NoError(t, err)
	require.EqualValues(t, list[2:], got.Timestamps)
	require.EqualValues(t, 12, got.Version)

	// Insert a set of spans that will take us right up to our span limit
	// but not over.
	spans := func() (ret []roachpb.Span) {
		for i := uint32(0); i < 100; i++ {
			ret = append(ret, makeTableSpan(i))
		}
		return ret
	}()
	// Try to add another row which should fail.
	ts, err = p.Protect(ctx, nil, s0.Clock().Now(), "", nil, spans...)
	require.Nil(t, ts)
	require.True(t, testutils.IsError(err, "limits exceeded: 8\\+100 > 100 spans"), err)

	// Now insert another row which would exceed the span limit.
}

func makeTableSpan(tableID uint32) roachpb.Span {
	k := keys.MakeTablePrefix(tableID)
	return roachpb.Span{
		Key:    k,
		EndKey: roachpb.Key(roachpb.RKey(k).PrefixEnd()),
	}
}
