// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package migrationmanager provides an implementation of migration.Manager
// for use on kv nodes.
package migrationmanager

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// Manager is the instance responsible for executing migrations across the
// cluster.
type Manager struct {
	c  migration.Cluster
	ie sqlutil.InternalExecutor
	jr *jobs.Registry
}

// NewManager constructs a new Manager.
//
// TODO(irfansharif): We'll need to eventually plumb in on a lease manager here.
func NewManager(c migration.Cluster, ie sqlutil.InternalExecutor, jr *jobs.Registry) *Manager {
	return &Manager{
		c:  c,
		ie: ie,
		jr: jr,
	}
}

// Migrate runs the set of migrations required to upgrade the cluster version
// from the current version to the target one.
func (m *Manager) Migrate(
	ctx context.Context, user security.SQLUsername, from, to clusterversion.ClusterVersion,
) error {
	// TODO(irfansharif): Should we inject every ctx here with specific labels
	// for each migration, so they log distinctly?
	ctx = logtags.AddTag(ctx, "migration-mgr", nil)
	if from == to {
		// Nothing to do here.
		log.Infof(ctx, "no need to migrate, cluster already at newest version")
		return nil
	}

	// TODO(irfansharif): We'll need to acquire a lease here and refresh it
	// throughout during the migration to ensure mutual exclusion.

	// TODO(irfansharif): We'll need to create a system table to store
	// in-progress state of long running migrations, for introspection.

	clusterVersions := clusterversion.ListBetween(from, to)
	if len(clusterVersions) == 0 {
		// We're attempt to migrate to something that's not defined in cluster
		// versions. This only happens in tests, when we're exercising version
		// upgrades over non-existent versions (like in the cluster_version
		// logictest). These tests explicitly override the
		// binary{,MinSupportedVersion} in order to work. End-user attempts to
		// do something similar would be caught at the sql layer (also tested in
		// the same logictest). We'll just explicitly append the target version
		// here instead, so that we're able to actually migrate into it.
		clusterVersions = append(clusterVersions, to)
	}
	log.Infof(ctx, "migrating cluster from %s to %s (stepping through %s)", from, to, clusterVersions)

	for _, clusterVersion := range clusterVersions {
		log.Infof(ctx, "stepping through %s", from, to, clusterVersions)
		// First, run the actual migration if any.
		if err := m.runMigration(ctx, user, clusterVersion); err != nil {
			return err
		}

		// Next we'll push out the version gate to every node in the cluster.
		// Each node will persist the version, bump the local version gates, and
		// then return. The migration associated with the specific version is
		// executed before every node in the cluster has the corresponding
		// version activated. Migrations that depend on a certain version
		// already being activated will need to registered using a cluster
		// version greater than it.
		//
		// For each intermediate version, we'll need to first bump the fence
		// version before bumping the "real" one. Doing so allows us to provide
		// the invariant that whenever a cluster version is active, all Nodes in
		// the cluster (including ones added concurrently during version
		// upgrades) are running binaries that know about the version.

		// Bumping the version gates only after running the migration itself is
		// important for below-raft migrations. Below-raft migrations mutate
		// replica state, making use of the Migrate(version=V) primitive when
		// issuing it against the entire keyspace. As part of this process,
		// migrations want to rely on the invariant that there are no extant
		// replicas in the system that haven't seen the specific Migrate command.
		// Consider the the following:
		//
		// - r420 is running version=87
		// - The leaseholder for r420 generates a snapshot to add a new replica
		//   elsewhere
		// - While the snapshot is inflight, all existing replicas of r420 are
		//   migrated to version=88
		// - The snapshot is received by the store, instantiating a replica with
		//   the pre-migrated state
		//
		// To guard against this, we gate any incoming snapshots that were
		// generated from a replica that (at the time) had a version[1] less
		// than our store's current active version[2]. After executing
		// Migrate(version=x), the infrastructure bumps the corresponding
		// cluster version on each store, preventing future snapshots with
		// versions < V. We should note that the reason we bump the cluster
		// version after the Migrate request, and not before, is because the
		// migration process itself can be arbitrarily long running. It'd be
		// undesirable to pause rebalancing/repair mechanisms from functioning
		// during this time.
		//
		// Coming back to the invariant above ("there should be no extant
		// replicas in the system that haven't seen the corresponding Migrate
		// command"), this is partly achieved through the implementation of the
		// Migrate command itself, which waits until it's applied on all
		// followers[3] before returning. That still leaves the possibility for
		// replicas in the replica GC queue to evade detection. To address this,
		// below-raft migrations typically take a two-phrase approach (the
		// TruncatedAndRangeAppliedStateMigration being one example of this),
		// where after having migrated the entire keyspace to version V, and
		// after having prevented subsequent snapshots originating from replicas
		// with versions < V, the migration sets out to purge outdated replicas
		// in the system[4]. Specifically it processes all replicas in
		// the GC queue with a version < V (which are not accessible during the
		// application of the Migrate command).
		//
		// [1]: See ReplicaState.Version.
		// [2]: See the snapshot version checks Store.canApplySnapshotLocked.
		// [3]: See Replica.executeWriteBatch, specifically how proposals with the
		//      Migrate request are handled downstream of raft.
		// [4]: See PurgeOutdatedReplicas from the Migration service.

		{
			// The migrations infrastructure makes use of internal fence
			// versions when stepping through consecutive versions. It's
			// instructive to walk through how we expect a version migration
			// from v21.1 to v21.2 to take place, and how we behave in the
			// presence of new v21.1 or v21.2 Nodes being added to the cluster.
			//
			//   - All Nodes are running v21.1
			//   - All Nodes are rolled into v21.2 binaries, but with active
			//     cluster version still as v21.1
			//   - The first version bump will be into v21.2-1(fence), see the
			//     migration manager above for where that happens
			//
			// Then concurrently:
			//
			//   - A new node is added to the cluster, but running binary v21.1
			//   - We try bumping the cluster gates to v21.2-1(fence)
			//
			// If the v21.1 Nodes manages to sneak in before the version bump,
			// it's fine as the version bump is a no-op one (all fence versions
			// are). Any subsequent bumps (including the "actual" one bumping to
			// v21.2) will fail during the validation step where we'll first
			// check to see that all Nodes are running v21.2 binaries.
			//
			// If the v21.1 node is only added after v21.2-1(fence) is active,
			// it won't be able to actually join the cluster (it'll be prevented
			// by the join RPC).
			//
			// All of which is to say that once we've seen the node list
			// stabilize (as EveryNode enforces), any new Nodes that can join
			// the cluster will run a release that support the fence version,
			// and by design also supports the actual version (which is the
			// direct successor of the fence).
			fenceVersion := migration.FenceVersionFor(ctx, clusterVersion)
			req := &serverpb.BumpClusterVersionRequest{ClusterVersion: &fenceVersion}
			op := fmt.Sprintf("bump-cluster-version=%s", req.ClusterVersion.PrettyPrint())
			err := m.c.EveryNode(ctx, op, func(ctx context.Context, client serverpb.MigrationClient) error {
				_, err := client.BumpClusterVersion(ctx, req)
				return err
			})
			if err != nil {
				return err
			}
		}
		{
			// Now sanity check that we'll actually be able to perform the real
			// cluster version bump, cluster-wide.
			req := &serverpb.ValidateTargetClusterVersionRequest{ClusterVersion: &clusterVersion}
			op := fmt.Sprintf("validate-cluster-version=%s", req.ClusterVersion.PrettyPrint())
			err := m.c.EveryNode(ctx, op, func(ctx context.Context, client serverpb.MigrationClient) error {
				_, err := client.ValidateTargetClusterVersion(ctx, req)
				return err
			})
			if err != nil {
				return err
			}
		}
		{
			// Finally, bump the real version cluster-wide.
			req := &serverpb.BumpClusterVersionRequest{ClusterVersion: &clusterVersion}
			op := fmt.Sprintf("bump-cluster-version=%s", req.ClusterVersion.PrettyPrint())
			if err := m.c.EveryNode(ctx, op, func(ctx context.Context, client serverpb.MigrationClient) error {
				_, err := client.BumpClusterVersion(ctx, req)
				return err
			}); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *Manager) runMigration(
	ctx context.Context, user security.SQLUsername, version clusterversion.ClusterVersion,
) error {
	if _, exists := migration.GetMigration(version); !exists {
		return nil
	}
	id, err := m.getOrCreateMigrationJob(ctx, user, version)
	if err != nil {
		return err
	}
	return m.jr.Run(ctx, m.ie, []int64{id})
}

func (m *Manager) getOrCreateMigrationJob(
	ctx context.Context, user security.SQLUsername, version clusterversion.ClusterVersion,
) (jobID int64, _ error) {

	if err := m.c.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		var found bool
		found, jobID, err = m.getRunningMigrationJob(ctx, txn, version)
		if err != nil {
			return err
		}
		if found {
			return nil
		}
		var j *jobs.Job
		j, err = m.jr.CreateJobWithTxn(ctx, jobs.Record{
			Description: "Long running migration",
			Details: jobspb.LongRunningMigrationDetails{
				ClusterVersion: &version,
			},
			Username:      user,
			Progress:      jobspb.LongRunningMigrationProgress{},
			NonCancelable: true,
		}, txn)
		if err != nil {
			return err
		}
		jobID = *j.ID()
		return nil
	}); err != nil {
		return 0, err
	}
	return jobID, nil
}

func (m *Manager) getRunningMigrationJob(
	ctx context.Context, txn *kv.Txn, version clusterversion.ClusterVersion,
) (found bool, jobID int64, _ error) {
	const query = `
SELECT id, status
	FROM (
		SELECT id,
		status,
		crdb_internal.pb_to_json(
			'cockroach.sql.jobs.jobspb.Payload',
			payload
		) AS pl
	FROM system.jobs
  WHERE status IN ` + jobs.NonTerminalStatusTupleString + `
	)
	WHERE pl->'longRunningMigration'->'clusterVersion' = $1::JSON;`
	// TODO(ajwerner): Flip the emitDefaults flag once this is rebased on master.
	jsonMsg, err := protoreflect.MessageToJSON(&version, true /* emitDefaults */)
	if err != nil {
		return false, 0, errors.Wrap(err, "failed to marshal version to JSON")
	}
	rows, err := m.ie.Query(ctx, "migration-manager-find-jobs", txn, query, jsonMsg.String())
	if err != nil {
		return false, 0, err
	}
	parseRow := func(row tree.Datums) (id int64, status jobs.Status) {
		return int64(*row[0].(*tree.DInt)), jobs.Status(*row[1].(*tree.DString))
	}
	switch len(rows) {
	case 0:
		return false, 0, nil
	case 1:
		id, status := parseRow(rows[0])
		log.Infof(ctx, "found existing migration job %d for version %v in status %s, waiting",
			id, &version, status)
		return true, id, nil
	default:
		format := "found multiple non-terminal jobs for version %v: [" +
			strings.Repeat("(%d, %s), ", len(rows)-1) + "(%d, %s)]"
		args := make([]interface{}, 1+len(rows)*2)
		args[0] = &version
		for i := range rows {
			args[2*i+1], args[2*i+2] = parseRow(rows[i])
		}
		log.Errorf(ctx, format, args...)
		return false, 0, errors.AssertionFailedf(format, args...)
	}
}
