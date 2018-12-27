// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package followerreadsccl

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan/leaseholder"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// FollowerReadMultiple is the multiple of kv.closed_timestmap.target_duration
// which the implementation of the follower read capable replica policy ought
// to use to determine if a request can be used for reading.
var FollowerReadMultiple = settings.RegisterValidatedFloatSetting(
	"kv.follower_read.target_multiple",
	"if above 1, encourages the distsender to perform a read against the closest replica if a request is older than this multiple of kv.closed_timestamp.target_duration less a clock uncertainty interval. This value is also used to create RECENT.",
	3,
	func(v float64) error {
		if v < 1 {
			return fmt.Errorf("multiple of %v is not >= 1", v)
		}
		return nil
	},
)

// recentDuration returns the duration which should be used to as the value
// of RECENT. The same value less clock uncertainty, then is used to determine
// if a query can use follower reads.
func getRecentDuration(st *cluster.Settings) time.Duration {
	return -1 * time.Duration(FollowerReadMultiple.Get(&st.SV)*
		float64(closedts.TargetDuration.Get(&st.SV)))
}

// canUseFollowerRead determines if a query can be sent to a follower
func canUseFollowerRead(clusterID uuid.UUID, st *cluster.Settings, ts hlc.Timestamp) bool {
	if !storage.FollowerReadsEnabled.Get(&st.SV) {
		return false
	}
	threshold := getRecentDuration(st) - base.DefaultMaxClockOffset
	if time.Since(ts.GoTime()) < threshold {
		return false
	}
	org := sql.ClusterOrganization.Get(&st.SV)
	return utilccl.CheckEnterpriseEnabled(st, clusterID, org, "follower reads") == nil
}

// canSendToFollower implements the logic for checking whether a batch request
// may be sent to a follower.
func canSendToFollower(clusterID uuid.UUID, st *cluster.Settings, ba roachpb.BatchRequest) bool {
	return ba.IsReadOnly() && ba.Txn != nil &&
		canUseFollowerRead(clusterID, st, ba.Txn.OrigTimestamp)
}

type oracleFactory struct {
	clusterID *base.ClusterIDContainer
	st        *cluster.Settings

	binPacking leaseholder.OracleFactory
	closest    leaseholder.OracleFactory
}

func newOracleFactory(cfg leaseholder.OracleConfig) leaseholder.OracleFactory {
	return &oracleFactory{
		clusterID:  &cfg.RPCContext.ClusterID,
		st:         cfg.Settings,
		binPacking: leaseholder.NewOracleFactory(leaseholder.BinPackingChoice, cfg),
		closest:    leaseholder.NewOracleFactory(leaseholder.ClosestChoice, cfg),
	}
}

func (f oracleFactory) Oracle(txn *client.Txn) leaseholder.Oracle {
	if txn != nil && canUseFollowerRead(f.clusterID.Get(), f.st, txn.OrigTimestamp()) {
		return f.closest.Oracle(txn)
	}
	return f.binPacking.Oracle(txn)
}

// followerReadAwareChoice is a leaseholder choosing policy that detects
// whether a query can be used with a follower read.
var followerReadAwareChoice = leaseholder.RegisterChoosingPolicy(newOracleFactory)

func init() {
	sql.DefaultResolverPolicy = followerReadAwareChoice
	sql.RecentDuration = getRecentDuration
	kv.CanSendToFollower = canSendToFollower
}
