package leaseholder

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// Test that the binPackingOracle is consistent in its choices: once a range has
// been assigned to one node, that choice is reused.
func TestBinPackingOracleIsConsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng := roachpb.RangeDescriptor{RangeID: 99}

	queryState := MakeOracleQueryState()
	expRepl := kv.ReplicaInfo{
		ReplicaDescriptor: roachpb.ReplicaDescriptor{
			NodeID: 99, StoreID: 99, ReplicaID: 99}}
	queryState.AssignedRanges[rng.RangeID] = expRepl
	// For our purposes, an uninitialized binPackingOracle will do.
	bp := binPackingOracle{
		leaseHolderCache: kv.NewLeaseHolderCache(func() int64 { return 1 }),
	}
	repl, err := bp.ChoosePreferredLeaseHolder(context.TODO(), rng, queryState)
	if err != nil {
		t.Fatal(err)
	}
	if repl != expRepl {
		t.Fatalf("expected replica %+v, got: %+v", expRepl, repl)
	}
}
