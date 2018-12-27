// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// Package leaseholder provides functionality for distsqlplan to choose a node
// for a range.
package leaseholder

import (
	"context"
	"math"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// ChoosingPolicy determines how an Oracle should select a replica.
type ChoosingPolicy byte

var (
	// RandomChoice chooses lease replicas randomly.
	RandomChoice = RegisterChoosingPolicy(newRandomOracleFactory)
	// BinPackingChoice bin-packs the choices.
	BinPackingChoice = RegisterChoosingPolicy(newBinPackingOracleFactory)
	// ClosestChoice chooses the node closest to the current node.
	ClosestChoice = RegisterChoosingPolicy(newClosestOracleFactory)
)

// OracleConfig is used to construct an OracleFactory.
type OracleConfig struct {
	NodeDesc         roachpb.NodeDescriptor
	Settings         *cluster.Settings
	Gossip           *gossip.Gossip
	RPCContext       *rpc.Context
	LeaseHolderCache *kv.LeaseHolderCache
}

// Oracle is used to choose the lease holder for ranges. This
// interface was extracted so we can experiment with different choosing
// policies.
// Note that choices that start out random can act as self-fulfilling prophecies
// - if there's no active lease, the node that will be asked to execute part of
// the query (the chosen node) will acquire a new lease.
type Oracle interface {
	// ChoosePreferredLeaseHolder returns a choice for one range. Implementors are free to
	// use the queryState param, which has info about the number of
	// ranges already handled by each node for the current SQL query. The state is
	// not updated with the result of this method; the caller is in charge of
	// that.
	//
	// A RangeUnavailableError can be returned if there's no information in gossip
	// about any of the nodes that might be tried.
	ChoosePreferredLeaseHolder(
		context.Context, roachpb.RangeDescriptor, OracleQueryState,
	) (kv.ReplicaInfo, error)
}

// OracleFactory creates an oracle for a Txn.
type OracleFactory interface {
	Oracle(*client.Txn) Oracle
}

// OracleFactoryFunc creates an OracleFactory from an OracleConfig.
type OracleFactoryFunc func(OracleConfig) OracleFactory

// NewOracleFactory creates an oracle with the given policy.
func NewOracleFactory(policy ChoosingPolicy, cfg OracleConfig) OracleFactory {
	ff, ok := oracleFactoryFuncs[policy]
	if !ok {
		panic(errors.Errorf("unknown ChoosingPolicy %v", policy))
	}
	return ff(cfg)
}

// Register is intended to be called only during init.
// It is not safe for concurrent use.
func RegisterChoosingPolicy(f OracleFactoryFunc) ChoosingPolicy {
	if len(oracleFactoryFuncs) == 255 {
		panic("Can only register 255 ChoosingPolicy instances")
	}
	r := ChoosingPolicy(len(oracleFactoryFuncs))
	oracleFactoryFuncs[r] = f
	return r
}

var oracleFactoryFuncs = map[ChoosingPolicy]OracleFactoryFunc{}

// OracleQueryState encapsulates the history of assignments of ranges to nodes
// done by an oracle on behalf of one particular query.
type OracleQueryState struct {
	RangesPerNode  map[roachpb.NodeID]int
	AssignedRanges map[roachpb.RangeID]kv.ReplicaInfo
}

// MakeOracleQueryState creates an initialized OracleQueryState.
func MakeOracleQueryState() OracleQueryState {
	return OracleQueryState{
		RangesPerNode:  make(map[roachpb.NodeID]int),
		AssignedRanges: make(map[roachpb.RangeID]kv.ReplicaInfo),
	}
}

// randomOracle is a Oracle that chooses the lease holder randomly
// among the replicas in a range descriptor.
type randomOracle struct {
	gossip *gossip.Gossip
}

var _ OracleFactory = &randomOracle{}

func newRandomOracleFactory(cfg OracleConfig) OracleFactory {
	return &randomOracle{gossip: cfg.Gossip}
}

func (o *randomOracle) Oracle(_ *client.Txn) Oracle {
	return o
}

func (o *randomOracle) ChoosePreferredLeaseHolder(
	ctx context.Context, desc roachpb.RangeDescriptor, _ OracleQueryState,
) (kv.ReplicaInfo, error) {
	replicas, err := replicaSliceOrErr(desc, o.gossip)
	if err != nil {
		return kv.ReplicaInfo{}, err
	}
	return replicas[rand.Intn(len(replicas))], nil
}

type closestOracle struct {
	gossip *gossip.Gossip
	rpcCtx *rpc.Context
	// nodeDesc is the descriptor of the current node. It will be used to give
	// preference to the current node and others "close" to it.
	nodeDesc roachpb.NodeDescriptor
}

func newClosestOracleFactory(cfg OracleConfig) OracleFactory {
	return &closestOracle{
		gossip:   cfg.Gossip,
		nodeDesc: cfg.NodeDesc,
	}
}

func (o *closestOracle) Oracle(_ *client.Txn) Oracle {
	return o
}

func (o *closestOracle) ChoosePreferredLeaseHolder(
	ctx context.Context, desc roachpb.RangeDescriptor, queryState OracleQueryState,
) (kv.ReplicaInfo, error) {
	replicas, err := replicaSliceOrErr(desc, o.gossip)
	if err != nil {
		return kv.ReplicaInfo{}, err
	}
	replicas.OptimizeReplicaOrder(&o.nodeDesc, latencyFunc(o.rpcCtx))
	return replicas[0], nil
}

// maxPreferredRangesPerLeaseHolder applies to the binPackingOracle.
// When choosing lease holders, we try to choose the same node for all the
// ranges applicable, until we hit this limit. The rationale is that maybe a
// bunch of those ranges don't have an active lease, so our choice is going to
// be self-fulfilling. If so, we want to collocate the lease holders. But above
// some limit, we prefer to take the parallelism and distribute to multiple
// nodes. The actual number used is based on nothing.
const maxPreferredRangesPerLeaseHolder = 10

// binPackingOracle coalesces choices together, so it gives preference to
// replicas on nodes that are already assumed to be lease holders for some other
// ranges that are going to be part of a single query.
// Secondarily, it gives preference to replicas that are "close" to the current
// node.
// Finally, it tries not to overload any node.
type binPackingOracle struct {
	leaseHolderCache                 *kv.LeaseHolderCache
	maxPreferredRangesPerLeaseHolder int
	gossip                           *gossip.Gossip
	rpcCtx                           *rpc.Context
	// nodeDesc is the descriptor of the current node. It will be used to give
	// preference to the current node and others "close" to it.
	nodeDesc roachpb.NodeDescriptor
}

func newBinPackingOracleFactory(cfg OracleConfig) OracleFactory {
	return &binPackingOracle{
		maxPreferredRangesPerLeaseHolder: maxPreferredRangesPerLeaseHolder,
		gossip:           cfg.Gossip,
		nodeDesc:         cfg.NodeDesc,
		leaseHolderCache: cfg.LeaseHolderCache,
	}
}

var _ OracleFactory = &binPackingOracle{}

func (o *binPackingOracle) Oracle(_ *client.Txn) Oracle {
	return o
}

func (o *binPackingOracle) ChoosePreferredLeaseHolder(
	ctx context.Context, desc roachpb.RangeDescriptor, queryState OracleQueryState,
) (kv.ReplicaInfo, error) {
	// Attempt to find a cached lease holder and use it if found.
	// If an error occurs, ignore it and proceed to choose a replica below.
	if storeID, ok := o.leaseHolderCache.Lookup(ctx, desc.RangeID); ok {
		var repl kv.ReplicaInfo
		repl.ReplicaDescriptor = roachpb.ReplicaDescriptor{StoreID: storeID}
		// Fill in the node descriptor.
		nodeID, err := o.gossip.GetNodeIDForStoreID(storeID)
		if err != nil {
			log.VEventf(ctx, 2, "failed to lookup store %d: %s", storeID, err)
		} else if nd, err := o.gossip.GetNodeDescriptor(nodeID); err != nil {
			log.VEventf(ctx, 2, "failed to resolve node %d: %s", nodeID, err)
		} else {
			repl.ReplicaDescriptor.NodeID = nodeID
			repl.NodeDesc = nd
			return repl, nil
		}
	}

	// If we've assigned the range before, return that assignment.
	if repl, ok := queryState.AssignedRanges[desc.RangeID]; ok {
		return repl, nil
	}

	replicas, err := replicaSliceOrErr(desc, o.gossip)
	if err != nil {
		return kv.ReplicaInfo{}, err
	}
	replicas.OptimizeReplicaOrder(&o.nodeDesc, latencyFunc(o.rpcCtx))

	// Look for a replica that has been assigned some ranges, but it's not yet full.
	minLoad := int(math.MaxInt32)
	var leastLoadedIdx int
	for i, repl := range replicas {
		assignedRanges := queryState.RangesPerNode[repl.NodeID]
		if assignedRanges != 0 && assignedRanges < o.maxPreferredRangesPerLeaseHolder {
			return repl, nil
		}
		if assignedRanges < minLoad {
			leastLoadedIdx = i
			minLoad = assignedRanges
		}
	}
	// Either no replica was assigned any previous ranges, or all replicas are
	// full. Use the least-loaded one (if all the load is 0, then the closest
	// replica is returned).
	return replicas[leastLoadedIdx], nil
}

// replicaSliceOrErr returns a ReplicaSlice for the given range descriptor.
// ReplicaSlices are restricted to replicas on nodes for which a NodeDescriptor
// is available in gossip. If no nodes are available, a RangeUnavailableError is
// returned.
func replicaSliceOrErr(desc roachpb.RangeDescriptor, gsp *gossip.Gossip) (kv.ReplicaSlice, error) {
	replicas := kv.NewReplicaSlice(gsp, &desc)
	if len(replicas) == 0 {
		// We couldn't get node descriptors for any replicas.
		var nodeIDs []roachpb.NodeID
		for _, r := range desc.Replicas {
			nodeIDs = append(nodeIDs, r.NodeID)
		}
		return kv.ReplicaSlice{}, sqlbase.NewRangeUnavailableError(
			desc.RangeID, errors.Errorf("node info not available in gossip"), nodeIDs...)
	}
	return replicas, nil
}

// latencyFunc returns a kv.LatencyFunc for use with
// Replicas.OptimizeReplicaOrder.
func latencyFunc(rpcCtx *rpc.Context) kv.LatencyFunc {
	if rpcCtx != nil {
		return rpcCtx.RemoteClocks.Latency
	}
	return nil
}
