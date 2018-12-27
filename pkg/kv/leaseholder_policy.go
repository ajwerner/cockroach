package kv

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// CanSendToFollower is used by the followerreadsccl code to inject logic
// to check if follower reads are enabled.
var CanSendToFollower = func(clusterID uuid.UUID, st *cluster.Settings, ba roachpb.BatchRequest) bool {
	return false
}
