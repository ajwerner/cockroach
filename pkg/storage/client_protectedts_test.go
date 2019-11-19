// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage_test

import (
	"context"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptstorage"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptverifier"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// TestProtectedTimestamps is an end-to-end test for protected timestamps.
// It works by writing a lot of data and waiting for the GC heuristic to allow
// for GC. Because of this, it's very slow and expensive. It should
// potentially be made cheaper by injecting hooks to force GC.
//
// Probably this test should always be skipped until it is made cheaper,
// nevertheless it's a useful test.
//
// TODO(ajwerner): Make this test cheaper or skip it.
func TestProtectedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	if util.RaceEnabled {
		t.Skip("this test is too slow to run with race")
	}

	args := base.TestClusterArgs{}
	tc := testcluster.StartTestCluster(t, 3, args)
	defer tc.Stopper().Stop(ctx)
	s0 := tc.Server(0)

	conn := tc.ServerConn(0)
	_, err := conn.Exec("CREATE TABLE foo (k INT PRIMARY KEY, v BYTES)")
	require.NoError(t, err)

	_, err = conn.Exec("SET CLUSTER SETTING kv.protectedts.poll_interval = '10ms';")
	require.NoError(t, err)

	_, err = conn.Exec("ALTER TABLE foo CONFIGURE ZONE USING gc.ttlseconds = 1;")
	require.NoError(t, err)

	rRand, _ := randutil.NewPseudoRand()
	upsertUntilBackpressure := func() {
		for {
			_, err := conn.Exec("UPSERT INTO foo VALUES (1, $1)", randutil.RandBytes(rRand, 1<<19))
			if testutils.IsError(err, "backpressure") {
				break
			}
			require.NoError(t, err)
		}
	}
	const processedPattern = "(?s)shouldQueue=true.*processing replica.*GC score after GC"
	processedRegexp := regexp.MustCompile(processedPattern)

	getTableStartKey := func(table string) roachpb.Key {
		row := conn.QueryRow(
			"SELECT start_key "+
				"FROM crdb_internal.ranges_no_leases "+
				"WHERE table_name = $1 "+
				"AND database_name = current_database() "+
				"ORDER BY start_key ASC "+
				"LIMIT 1",
			"foo")

		var startKey roachpb.Key
		require.NoError(t, row.Scan(&startKey))
		return startKey
	}

	getStoreAndReplica := func() (*storage.Store, *storage.Replica) {
		startKey := getTableStartKey("foo")
		// Okay great now we have a key and can go find replicas and stores and what not.
		r := tc.LookupRangeOrFatal(t, startKey)
		l, _, err := tc.FindRangeLease(r, nil)
		require.NoError(t, err)

		lhServer := tc.Server(int(l.Replica.NodeID) - 1)
		return getFirstStoreReplica(t, lhServer, startKey)
	}

	gcSoon := func() {
		testutils.SucceedsSoon(t, func() error {
			s, repl := getStoreAndReplica()
			trace, _, err := s.ManuallyEnqueue(ctx, "gc", repl, false)
			require.NoError(t, err)
			if !processedRegexp.MatchString(trace.String()) {
				return errors.Errorf("%q does not match %q", trace.String(), processedRegexp)
			}
			return nil
		})
	}

	beforeWrites := s0.Clock().Now()
	upsertUntilBackpressure()
	gcSoon()

	pts := ptstorage.New(s0.ClusterSettings(), s0.InternalExecutor().(*sql.InternalExecutor))
	p := protectedts.WithDatabase(pts, s0.DB())
	startKey := getTableStartKey("foo")
	ptsRec := ptpb.NewRecord(s0.Clock().Now(), ptpb.PROTECT_AT, "", nil, roachpb.Span{
		Key:    startKey,
		EndKey: startKey.PrefixEnd(),
	})
	require.NoError(t, p.Protect(ctx, nil /* txn */, &ptsRec))

	upsertUntilBackpressure()
	s, repl := getStoreAndReplica()
	trace, _, err := s.ManuallyEnqueue(ctx, "gc", repl, false)
	require.NoError(t, err)
	require.Regexp(t, "(?s)not gc'ing replica.*due to protected timestamps.*"+ptsRec.ID.String(), trace.String())

	ptv := ptverifier.New(s0.DB(), pts)
	require.NoError(t, ptv.Verify(ctx, ptsRec.ID))
	ptsRecVerified, err := p.GetRecord(ctx, nil /* txn */, ptsRec.ID)
	require.NoError(t, err)
	require.True(t, ptsRecVerified.Verified)

	// Make a new record that is doomed to fail.
	failedRec := ptsRec
	failedRec.ID = uuid.MakeV4()
	failedRec.Timestamp = beforeWrites
	failedRec.Timestamp.Logical = 0
	require.NoError(t, p.Protect(ctx, nil /* txn */, &failedRec))
	_, err = p.GetRecord(ctx, nil /* txn */, failedRec.ID)
	require.NoError(t, err)

	// Verify that it indeed did fail.
	verifyErr := ptv.Verify(ctx, failedRec.ID)
	require.True(t, testutils.IsError(verifyErr, "failed to verify protection"),
		"%v", verifyErr)

	// Release the record that had succeeded and ensure that GC eventually
	// happens.
	require.NoError(t, pts.Release(ctx, nil, ptsRec.ID))
	testutils.SucceedsSoon(t, func() error {
		trace, _, err = s.ManuallyEnqueue(ctx, "gc", repl, false)
		require.NoError(t, err)
		if !processedRegexp.MatchString(trace.String()) {
			return errors.Errorf("%q does not match %q", trace.String(), processedRegexp)
		}
		return nil
	})

	// Release the failed record.
	require.NoError(t, pts.Release(ctx, nil, failedRec.ID))
}
