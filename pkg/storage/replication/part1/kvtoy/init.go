package kvtoy

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachlabs/acidlib/v1/hlc"
)

func makeRangeDescriptor(numNodes int) roachpb.RangeDescriptor {
	replicas := make([]roachpb.ReplicaDescriptor, 0, numNodes)
	for i := 0; i < numNodes; i++ {
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

func WriteInitialClusterData(ctx context.Context, eng engine.Engine) error {
	rangeDesc := makeRangeDescriptor(3)
	err := engine.MVCCPutProto(ctx, b, nil,
		keys.RangeDescriptorKey(roachpb.RKeyMin), hlc.Timestamp{},
		nil, rangeDesc)
	if err != nil {
		return err
	}
	if _, err := rsl.Save(ctx, b, storagepb.ReplicaState{
		Lease: &roachpb.Lease{
			Replica: replicas[0],
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
