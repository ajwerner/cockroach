// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrationmanager_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// TestImproperlyVersionedSnapshotsDisallowed verifies that a snapshot generated
// from a replica running a certain version is disallowed on a store with an
// active cluster version that's higher than it. Instead of creating the
// specific setup by hand, we simulate it in the way we expect to see it happen:
// - A cluster version upgrade from vX to vX+1 is in process
// - The migration manager bumps the cluster version on all stores, to vX+1
// - The migration manager begins migrating individual ranges to vX+1
// - Concurrently a range still at vX generates a learner snapshot
// - We expect this snapshot to be disallowed by the target store
func TestImproperlyVersionedSnapshotsDisallowed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We're going to be migrating from startCV to endCV.
	startCV := clusterversion.ClusterVersion{Version: roachpb.Version{Major: 41}}
	endCV := clusterversion.ClusterVersion{Version: roachpb.Version{Major: 42}}

	blockSnapshotsCh := make(chan struct{})

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: cluster.MakeTestingClusterSettingsWithVersions(endCV.Version, startCV.Version, false),
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					BinaryVersionOverride:          startCV.Version,
					DisableAutomaticVersionUpgrade: 1,
				},
				Store: &kvserver.StoreTestingKnobs{
					// We'll control the snapshot queue by hand.
					DisableRaftSnapshotQueue: true,
					// We'll want to temporarily stall incoming snapshots in
					// order to create the situation where an older inflight
					// snapshot is being sent to a store with a higher active
					// cluster version.
					ReceiveSnapshot: func(header *kvserver.SnapshotRequest_Header) error {
						select {
						case <-blockSnapshotsCh:
						case <-time.After(10 * time.Second):
							return errors.New(`test timed out`)
						}
						return nil
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	// We'll take a specific range, still running at startCV, generate an
	// outgoing snapshot and then suspend it temporarily. We'll then bump the
	// cluster version on all the stores, as part of the migration process, and
	// then resume the snapshot process. Seeing as how the snapshot was
	// generated pre-version bump, off of a version of the range that hadn't
	// observed the migration corresponding to the latest cluster version, we
	// expect the store to reject it.

	key := tc.ScratchRange(t)
	require.NoError(t, tc.WaitForSplitAndInitialization(key))
	desc, err := tc.LookupRange(key)
	require.NoError(t, err)
	rangeID := desc.RangeID

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		// Add a learner replica. This will be the target for the outgoing
		// snapshot.
		_, err := tc.AddVoters(key, tc.Target(1))
		return err
	})

	// Enqueue the replica in the raftsnapshot queue. We use SucceedsSoon
	// because it may take a bit for raft to figure out that we need to be
	// generating a snapshot.
	store := tc.GetFirstStoreFromServer(t, 0)
	repl, err := store.GetReplica(rangeID)
	require.NoError(t, err)

	testutils.SucceedsSoon(t, func() error {
		trace, processErr, err := store.ManuallyEnqueue(ctx, "raftsnapshot", repl, true /* skipShouldQueue */)
		if err != nil {
			return err
		}
		if processErr != nil {
			return processErr
		}
		const msg = `skipping snapshot; replica is likely a learner in the process of being added: (n2,s2):2LEARNER`
		formattedTrace := trace.String()
		if !strings.Contains(formattedTrace, msg) {
			return errors.Errorf(`expected "%s" in trace got:\n%s`, msg, formattedTrace)
		}
		return nil
	})

	// Register the below raft migration.
	unregisterKVMigration := batcheval.TestingRegisterMigrationInterceptor(endCV.Version, func() {})
	defer unregisterKVMigration()

	// Register the top-level migration.
	migrated := false
	unregister := migration.TestingRegisterMigrationInterceptor(endCV, func(
		ctx context.Context, cv clusterversion.ClusterVersion, h migration.Cluster,
	) error {
		// By the time we're here, all the stores will have had their cluster
		// versions bumped. It's now safe to unblock the snapshot sending
		// process.
		close(blockSnapshotsCh)

		migrated = true
		return nil
	})
	defer unregister()

	// Kick off the migration process.
	_, _ = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`, endCV.String())
	if !migrated {
		t.Fatalf("expected migration interceptor to have been called")
	}

	if err := g.Wait(); !testutils.IsError(err, fmt.Sprintf("snapshot generated at %s, only accepting versions >= %s", startCV, endCV)) {
		t.Fatal(err)
	}
}

func TestMigrateUpdatesReplicaVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We're going to be migrating from startCV to endCV.
	startCV := clusterversion.ClusterVersion{Version: roachpb.Version{Major: 41}}
	endCV := clusterversion.ClusterVersion{Version: roachpb.Version{Major: 42}}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: cluster.MakeTestingClusterSettingsWithVersions(endCV.Version, startCV.Version, false),
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					BinaryVersionOverride:          startCV.Version,
					DisableAutomaticVersionUpgrade: 1,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	// We'll take a specific range, still running at startCV, generate an
	// outgoing snapshot and then suspend it temporarily. We'll then bump the
	// cluster version on all the stores, as part of the migration process, and
	// then resume the snapshot process. Seeing as how the snapshot was
	// generated pre-version bump, off of a version of the range that hadn't
	// observed the migration corresponding to the latest cluster version, we
	// expect the store to reject it.

	key := tc.ScratchRange(t)
	require.NoError(t, tc.WaitForSplitAndInitialization(key))
	desc, err := tc.LookupRange(key)
	require.NoError(t, err)
	rangeID := desc.RangeID

	// Enqueue the replica in the raftsnapshot queue. We use SucceedsSoon
	// because it may take a bit for raft to figure out that we need to be
	// generating a snapshot.
	store := tc.GetFirstStoreFromServer(t, 0)
	repl, err := store.GetReplica(rangeID)
	require.NoError(t, err)

	if got := repl.Version(); got != startCV.Version {
		t.Fatalf("got replica version %s, expected %s", got, startCV.Version)
	}

	// Register the below raft migration.
	unregisterKVMigration := batcheval.TestingRegisterMigrationInterceptor(endCV.Version, func() {})
	defer unregisterKVMigration()

	// Register the top-level migration.
	unregister := migration.TestingRegisterMigrationInterceptor(endCV, func(
		ctx context.Context, cv clusterversion.ClusterVersion, c migration.Cluster,
	) error {
		return c.DB().Migrate(ctx, desc.StartKey, desc.EndKey, cv.Version)
	})
	defer unregister()

	// Kick off the migration process.
	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`, endCV.String())
	require.NoError(t, err)

	if got := repl.Version(); got != endCV.Version {
		t.Fatalf("got replica version %s, expected %s", got, endCV.Version)
	}
}
