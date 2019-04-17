package kvtoy

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/pkg/errors"
)

func makeRangeDescriptor(nodes []int) *roachpb.RangeDescriptor {
	replicas := make([]roachpb.ReplicaDescriptor, 0, len(nodes))
	for _, i := range nodes {
		replicas = append(replicas, roachpb.ReplicaDescriptor{
			NodeID:    roachpb.NodeID(i),
			StoreID:   roachpb.StoreID(i),
			ReplicaID: roachpb.ReplicaID(i),
		})
	}
	return &roachpb.RangeDescriptor{
		StartKey: roachpb.RKeyMin,
		RangeID:  1,
		Replicas: replicas,
	}
}

func WriteInitialClusterData(
	ctx context.Context, eng engine.Engine, rsl stateloader.StateLoader, nodes ...int,
) error {
	if len(nodes) < 1 {
		return errors.Errorf("failed to write initial cluster data: must provide at least 1 node")
	}
	b := eng.NewBatch()
	defer b.Close()
	rangeDesc := makeRangeDescriptor(nodes)
	err := engine.MVCCPutProto(ctx, b, nil,
		keys.RangeDescriptorKey(roachpb.RKeyMin), hlc.Timestamp{},
		nil, rangeDesc)
	if err != nil {
		return err
	}
	if _, err := rsl.Save(ctx, b, storagepb.ReplicaState{
		Lease: &roachpb.Lease{
			Replica: rangeDesc.Replicas[0],
		},
		TruncatedState: &roachpb.RaftTruncatedState{
			Term: 1,
		},
		GCThreshold:        &hlc.Timestamp{},
		Stats:              &enginepb.MVCCStats{},
		TxnSpanGCThreshold: &hlc.Timestamp{},
	}, stateloader.TruncatedStateUnreplicated); err != nil {
		return err
	}
	if err := rsl.SynthesizeRaftState(ctx, b); err != nil {
		return err
	}
	return b.Commit(true)
}
