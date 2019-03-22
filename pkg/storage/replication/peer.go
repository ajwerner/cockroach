// Copyright 2019 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package replication

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/connect"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/gofrs/uuid"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// Peer represents local replication state for a replica group.
type Peer struct {
	log.AmbientContext
	raftConfig *base.RaftConfig
	groupID    GroupID // Does a Peer need to know this?

	// raftTransport is exclusively for outbound traffic.
	// inbound traffic will come on the message queue.
	raftTransport      connect.Conn
	raftMessageFactory func() RaftMessage
	shouldCampaign     func(ctx context.Context, status *raft.Status) bool
	onUnquiesce        func()
	storage            engine.Engine
	entryReader        EntryReader
	entryCache         EntryCache

	// mu < msgQueueMu < unreachablesMu
	mu struct {
		syncutil.RWMutex

		truncatedState *roachpb.RaftTruncatedState

		appliedIndex uint64 // same as Replica.mu.state.RaftAppliedIndex?

		// PeerID is the current peer's ID.
		// It is zero when the Peer was created from a snapshot.
		peerID              PeerID
		leaderID            PeerID
		ticks               uint64
		lastIndex, lastTerm uint64
		quiescent           bool
		destroyed           bool
		stateLoader         StateLoader
		peers               []PeerID // ugh this is sort of from the replica descriptor?
		rawNode             *raft.RawNode

		proposals map[storagebase.CmdIDKey]*proposal

		// For command size based allocations we keep track of the sizes of all
		// in-flight commands.
		commandSizes map[storagebase.CmdIDKey]int

		dontUseQuotaPool bool
		proposalQuota    *quotaPool

		proposalQuotaBaseIndex uint64

		// TODO(ajwerner): Figure out what I'm doing here.
		// The most recently updated time for each follower of this range. This is updated
		// every time a Raft message is received from a peer.
		// Note that superficially it seems that similar information is contained in the
		// Progress of a RaftStatus, which has a RecentActive field. However, that field
		// is always true unless CheckQuorum is active, which at the time of writing in
		// CockroachDB is not the case.
		//
		// The lastUpdateTimes map is also updated when a leaseholder steps up
		// (making the assumption that all followers are live at that point),
		// and when the range unquiesces (marking all replicating followers as
		// live).
		//
		// TODO(tschottdorf): keeping a map on each replica seems to be
		// overdoing it. We should map the replicaID to a NodeID and then use
		// node liveness (or any sensible measure of the peer being around).
		// The danger in doing so is that a single stuck replica on an otherwise
		// functioning node could fill up the quota pool. We are already taking
		// this kind of risk though: a replica that gets stuck on an otherwise
		// live node will not lose leaseholdership.
		lastUpdateTimes lastUpdateTimesMap

		// Once the leader observes a proposal come 'out of Raft', we consult
		// the 'commandSizes' map to determine the size of the associated
		// command and add it to a queue of quotas we have yet to release back
		// to the quota pool. We only do so when all replicas have persisted
		// the corresponding entry into their logs.
		quotaReleaseQueue []int
	}
	// TODO(ajwerner): justify and understand this
	raftMu struct {
		syncutil.Mutex
		// Note that there are two StateLoaders, in raftMu and mu,
		// depending on which lock is being held.
		stateLoader StateLoader
		// on-disk storage for sideloaded SSTables. nil when there's no ReplicaID.
		sideloaded SideloadStorage
	}
	msgQueueMu struct {
		syncutil.RWMutex
		msgQueue        []raftpb.Message
		droppedMessages int
	}
	unreachablesMu struct {
		syncutil.Mutex
		remotes map[GroupID]struct{}
	}
}

// TODO: see cleanupFailedProposalsLocked
func (p *Peer) Progress() Progress {
	panic("not implemented")
}

func (p *Peer) SetUseQuotaPool(useQuotaPool bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.dontUseQuotaPool = !useQuotaPool
}

type handleRaftReadyStats struct {
	processed int
}

// TODO: something with snapshots
// TODO: decide if I need a "raftMu"
func (p *Peer) handleRaftReady(ctx context.Context) (handleRaftReadyStats, string, error) {
	var stats handleRaftReadyStats
	var hasReady bool
	var rd raft.Ready
	p.mu.Lock()
	leaderID := p.mu.leaderID
	lastLeaderID := leaderID

	defer p.updateProposalQuota(ctx, lastLeaderID)
	err := p.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
		if hasReady = raftGroup.HasReady(); hasReady {
			rd = raftGroup.Ready()
		}
		return hasReady /* unquiesceAndWakeLeader */, nil
	})
	log.Infof(ctx, "%+#v", rd)
	p.mu.Unlock()
	if err != nil {
		const expl = "while checking raft group for Ready"
		return stats, expl, errors.Wrap(err, expl)
	}

	if !hasReady {
		return stats, "", nil
	}

	logRaftReady(ctx, rd)
	if rd.SoftState != nil && leaderID != PeerID(rd.SoftState.Lead) {
		// TODO(ajwerner): deal with leadership changes?
		log.Infof(ctx, "Leader changed? %v %v", rd.SoftState, leaderID)
		if log.V(3) {
			log.Infof(ctx, "raft leader changed: %d -> %d", leaderID, rd.SoftState.Lead)
		}
		leaderID = PeerID(rd.SoftState.Lead)
	}

	if !raft.IsEmptySnap(rd.Snapshot) {
		snapUUID, err := uuid.FromBytes(rd.Snapshot.Data)
		log.Infof(ctx, "got a snapshot? %v %v", snapUUID, err)
	}
	// Use a more efficient write-only batch because we don't need to do any
	// reads from the batch. Any reads are performed via the "distinct" batch
	// which passes the reads through to the underlying DB.
	batch := p.storage.NewWriteOnlyBatch()
	defer batch.Close()

	return stats, "", nil

}

//go:generate stringer -type refreshRaftReason
type refreshRaftReason int

const (
	noReason refreshRaftReason = iota
	reasonNewLeader
	reasonNewLeaderOrConfigChange
	// A snapshot was just applied and so it may have contained commands that we
	// proposed whose proposal we still consider to be inflight. These commands
	// will never receive a response through the regular channel.
	reasonSnapshotApplied
	reasonReplicaIDChanged
	reasonTicks
)

func (p *Peer) withRaftGroupLocked(
	mayCampaignOnWake bool, f func(r *raft.RawNode) (unquiesceAndWakeLeader bool, _ error),
) error {
	if p.mu.destroyed {
		// Silently ignore all operations on destroyed replicas. We can't return an
		// error here as all errors returned from this method are considered fatal.
		return nil
	}

	if p.mu.peerID == 0 {
		// The replica's raft group has not yet been configured (i.e. the replica
		// was created from a preemptive snapshot).
		return nil
	}
	// Should we create the raft raw node lazily? I feel like probably not.
	if p.mu.rawNode == nil {
		ctx := p.AnnotateCtx(context.TODO())
		raftGroup, err := raft.NewRawNode(newRaftConfig(
			raft.Storage((*peerStorage)(p)),
			uint64(p.mu.peerID),
			p.mu.appliedIndex, // TODO(ajwerner): think about this
			*p.raftConfig,
			&raftLogger{ctx: ctx},
		), nil)
		if err != nil {
			return err
		}
		p.mu.rawNode = raftGroup

		if mayCampaignOnWake {
			p.maybeCampaignOnWakeLocked(ctx)
		}
	}

	// This wrapper function is a hack to add range IDs to stack traces
	// using the same pattern as Replica.sendWithRangeID.
	unquiesce, err := func(groupID GroupID, raftGroup *raft.RawNode) (bool, error) {
		if f == nil {
			return false, nil
		}
		return f(raftGroup)
	}(p.groupID, p.mu.rawNode)
	if unquiesce {
		p.unquiesceAndWakeLeaderLocked()
	}
	return err
}

func (p *Peer) maybeCampaignOnWakeLocked(ctx context.Context) {
	panic("TODO(ajwerner): not implemented")
}

// TODO: consider pooling
func (p *Peer) NewClient(syn sync.Locker) *PeerClient {
	pc := &PeerClient{
		syn:  syn,
		peer: p,
	}
	pc.cond.L = syn
	return pc
}

func (p *Peer) enqueueRaftMessage(m raftpb.Message) {
	p.msgQueueMu.Lock()
	defer p.msgQueueMu.Unlock()
	p.msgQueueMu.msgQueue = append(p.msgQueueMu.msgQueue, m)
}

func (p *Peer) addProposal(prop *proposal) {
	p.mu.Lock()
	defer p.mu.Unlock()

}

func (p *Peer) updateProposalQuota(ctx context.Context, lastLeaderID PeerID) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.mu.peerID == 0 {
		// The replica was created from preemptive snapshot and has not been
		// added to the Raft group.
		return
	}

	if p.mu.destroyed {
		if p.mu.proposalQuota != nil {
			p.mu.proposalQuota.close()
		}
		p.mu.proposalQuota = nil
		p.mu.lastUpdateTimes = nil
		p.mu.quotaReleaseQueue = nil
		p.mu.commandSizes = nil
	}
	if p.mu.leaderID != lastLeaderID {
		if p.mu.peerID == p.mu.leaderID {
			// We're becoming the leader.
			p.mu.proposalQuotaBaseIndex = p.mu.lastIndex

			if p.mu.proposalQuota != nil {
				log.Fatal(ctx, "proposalQuota was not nil before becoming the leader")
			}
			if releaseQueueLen := len(p.mu.quotaReleaseQueue); releaseQueueLen != 0 {
				log.Fatalf(ctx, "len(p.mu.quotaReleaseQueue) = %d, expected 0", releaseQueueLen)
			}
			if commandSizesLen := len(p.mu.commandSizes); commandSizesLen != 0 {
				log.Fatalf(ctx, "len(p.mu.commandSizes) = %d, expected 0", commandSizesLen)
			}

			// Raft may propose commands itself (specifically the empty
			// commands when leadership changes), and these commands don't go
			// through the code paths where we acquire quota from the pool. To
			// offset this we reset the quota pool whenever leadership changes
			// hands.
			p.mu.proposalQuota = newQuotaPool(p.raftConfig.RaftProposalQuota)
			// TODO(ajwerner): figure out last update
			p.mu.commandSizes = make(map[storagebase.CmdIDKey]int)
		} else if p.mu.proposalQuota != nil {
			// We're becoming a follower.

			// We unblock all ongoing and subsequent quota acquisition
			// goroutines (if any).
			p.mu.proposalQuota.close()
			p.mu.proposalQuota = nil
			p.mu.lastUpdateTimes = nil
			p.mu.quotaReleaseQueue = nil
			p.mu.commandSizes = nil
		}
		return
	} else if p.mu.proposalQuota == nil {
		if p.mu.peerID == p.mu.leaderID {
			log.Fatal(ctx, "leader has uninitialized proposalQuota pool")
		}
		// We're a follower.
		return
	}

	// We're still the leader.

	// TODO(ajwerner): figure out how to make the quota pool work.

	// // TODO(peter): Can we avoid retrieving the Raft status on every invocation
	// // in order to avoid the associated allocation? Tracking the progress
	// // ourselves via looking at MsgAppResp messages would be overkill. Perhaps
	// // another accessor on RawNode.
	// status := p.raftStatusRLocked()
	// if status == nil {
	// 	log.Fatal(ctx, "leader with nil RaftStatus")
	// }

	// // Find the minimum index that active followers have acknowledged.
	// now := timeutil.Now()
	// minIndex := status.Commit
	// for _, rep := range p.mu.state.Desc.Replicas {
	// 	// Only consider followers that that have "healthy" RPC connections.

	// 	if err := r.store.cfg.NodeDialer.ConnHealth(rep.NodeID); err != nil {
	// 		continue
	// 	}

	// 	// Only consider followers that are active.
	// 	if !p.mu.lastUpdateTimes.isFollowerActive(ctx, rep.ReplicaID, now) {
	// 		continue
	// 	}
	// 	if progress, ok := status.Progress[uint64(rep.ReplicaID)]; ok {
	// 		// Note that the Match field has different semantics depending on
	// 		// the State.
	// 		//
	// 		// In state ProgressStateReplicate, the Match index is optimistically
	// 		// updated whenever a message is *sent* (not received). Due to Raft
	// 		// flow control, only a reasonably small amount of data can be en
	// 		// route to a given follower at any point in time.
	// 		//
	// 		// In state ProgressStateProbe, the Match index equals Next-1, and
	// 		// it tells us the leader's optimistic best guess for the right log
	// 		// index (and will try once per heartbeat interval to update its
	// 		// estimate). In the usual case, the follower responds with a hint
	// 		// when it rejects the first probe and the leader replicates or
	// 		// sends a snapshot. In the case in which the follower does not
	// 		// respond, the leader reduces Match by one each heartbeat interval.
	// 		// But if the follower does not respond, we've already filtered it
	// 		// out above. We use the Match index as is, even though the follower
	// 		// likely isn't there yet because that index won't go up unless the
	// 		// follower is actually catching up, so it won't cause it to fall
	// 		// behind arbitrarily.
	// 		//
	// 		// Another interesting tidbit about this state is that the Paused
	// 		// field is usually true as it is used to limit the number of probes
	// 		// (i.e. appends) sent to this follower to one per heartbeat
	// 		// interval.
	// 		//
	// 		// In state ProgressStateSnapshot, the Match index is the last known
	// 		// (possibly optimistic, depending on previous state) index before
	// 		// the snapshot went out. Once the snapshot applies, the follower
	// 		// will enter ProgressStateReplicate again. So here the Match index
	// 		// works as advertised too.

	// 		// Only consider followers who are in advance of the quota base
	// 		// index. This prevents a follower from coming back online and
	// 		// preventing throughput to the range until it has caught up.
	// 		if progress.Match < p.mu.proposalQuotaBaseIndex {
	// 			continue
	// 		}
	// 		if progress.Match > 0 && progress.Match < minIndex {
	// 			minIndex = progress.Match
	// 		}
	// 		// If this is the most recently added replica and it has caught up, clear
	// 		// our state that was tracking it. This is unrelated to managing proposal
	// 		// quota, but this is a convenient place to do so.
	// 		if rep.ReplicaID == p.mu.lastReplicaAdded && progress.Match >= status.Commit {
	// 			p.mu.lastReplicaAdded = 0
	// 			p.mu.lastReplicaAddedTime = time.Time{}
	// 		}
	// 	}
	// }

	// if p.mu.proposalQuotaBaseIndex < minIndex {
	// 	// We've persisted minIndex - p.mu.proposalQuotaBaseIndex entries to
	// 	// the raft log on all 'active' replicas since last we checked,
	// 	// we 'should' be able to release the difference back to
	// 	// the quota pool. But consider the scenario where we have a single
	// 	// replica that we're writing to, we only construct the
	// 	// quotaReleaseQueue when entries 'come out' of Raft via
	// 	// raft.Ready.CommittedEntries. The minIndex computed above uses the
	// 	// replica's commit index which is independent of whether or we've
	// 	// iterated over the entirety of raft.Ready.CommittedEntries and
	// 	// therefore may not have all minIndex - p.mu.proposalQuotaBaseIndex
	// 	// command sizes in our quotaReleaseQueue.  Hence we only process
	// 	// min(minIndex - p.mu.proposalQuotaBaseIndex, len(p.mu.quotaReleaseQueue))
	// 	// quota releases.
	// 	numReleases := minIndex - p.mu.proposalQuotaBaseIndex
	// 	if qLen := uint64(len(p.mu.quotaReleaseQueue)); qLen < numReleases {
	// 		numReleases = qLen
	// 	}
	// 	sum := 0
	// 	for _, rel := range p.mu.quotaReleaseQueue[:numReleases] {
	// 		sum += rel
	// 	}
	// 	p.mu.proposalQuotaBaseIndex += numReleases
	// 	p.mu.quotaReleaseQueue = p.mu.quotaReleaseQueue[numReleases:]

	// 	p.mu.proposalQuota.add(int64(sum))
	// }
}

func (p *Peer) raftStatusRLocked() *raft.Status {
	if rg := p.mu.rawNode; rg != nil {
		return rg.Status()
	}
	return nil
}

func (p *Peer) tick(
	ctx context.Context, isAlive func(peerID PeerID) bool,
) (exists bool, err error) {
	ctx = p.AnnotateCtx(ctx)
	if log.V(2) {
		log.Infof(ctx, "ticking")
	}

	p.unreachablesMu.Lock()
	remotes := p.unreachablesMu.remotes
	p.unreachablesMu.remotes = nil
	p.unreachablesMu.Unlock()

	p.raftMu.Lock()
	defer p.raftMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()

	// If the raft group is uninitialized, do not initialize on tick.
	if p.mu.rawNode == nil {
		return false, nil
	}

	for remoteReplica := range remotes {
		p.mu.rawNode.ReportUnreachable(uint64(remoteReplica))
	}

	if p.mu.quiescent {
		return false, nil
	}
	if p.maybeQuiesceLocked(ctx, isAlive) {
		return false, nil
	}

	p.maybeTransferRaftLeadershipLocked(ctx)

	p.mu.ticks++
	p.mu.rawNode.Tick()

	// refreshAtDelta := p.pf.cfg.RaftElectionTimeoutTicks
	// TODO(ajwerner): add back testing knobs
	// if knob := p.store.TestingKnobs().RefreshReasonTicksPeriod; knob > 0 {
	// 	refreshAtDelta = knob
	// }
	// if !p.store.TestingKnobs().DisableRefreshReasonTicks && p.mu.ticks%refreshAtDelta == 0 {
	// 	// RaftElectionTimeoutTicks is a reasonable approximation of how long we
	// 	// should wait before deciding that our previous proposal didn't go
	// 	// through. Note that the combination of the above condition and passing
	// 	// RaftElectionTimeoutTicks to refreshProposalsLocked means that commands
	// 	// will be refreshed when they have been pending for 1 to 2 election
	// 	// cycles.
	// 	p.refreshProposalsLocked(refreshAtDelta, reasonTicks)
	// }
	return true, nil
}

// maybeTransferRaftLeadershipLocked attempts to transfer the leadership away
// from this node to the leaseholder, if this node is the current raft leader
// but not the leaseholder. We don't attempt to transfer leadership if the
// leaseholder is behind on applying the log.
//
// We like it when leases and raft leadership are collocated because that
// facilitates quick command application (requests generally need to make it to
// both the lease holder and the raft leader before being applied by other
// replicas).
// TODO(fix this)
func (p *Peer) maybeTransferRaftLeadershipLocked(ctx context.Context) {
	// TODO(ajwerner): add back testing knobs
	// if r.store.TestingKnobs().DisableLeaderFollowsLeaseholder {
	// 	return
	// }
	// lease := *r.mu.state.Lease
	// if lease.OwnedBy(r.StoreID()) || !r.isLeaseValidRLocked(lease, r.Clock().Now()) {
	// 	return
	// }
	// raftStatus := r.raftStatusRLocked()
	// if raftStatus == nil || raftStatus.RaftState != raft.StateLeader {
	// 	return
	// }
	// lhReplicaID := uint64(lease.Replica.ReplicaID)
	// lhProgress, ok := raftStatus.Progress[lhReplicaID]
	// if (ok && lhProgress.Match >= raftStatus.Commit) || r.mu.draining {
	// 	log.VEventf(ctx, 1, "transferring raft leadership to replica ID %v", lhReplicaID)
	// 	r.store.metrics.RangeRaftLeaderTransfers.Inc(1)
	// 	r.mu.internalRaftGroup.TransferLeader(lhReplicaID)
	// }
}

func (p *Peer) maybeQuiesceLocked(ctx context.Context, isAlive func(peerID PeerID) bool) bool {
	// TODO(ajwerner): implement
	return false

}

func (p *Peer) maybeAcquireProposalQuota(prop *proposal) error {
	p.mu.RLock()
	quotaPool := p.mu.proposalQuota
	dontUseQuotaPool := p.mu.dontUseQuotaPool
	p.mu.RUnlock()

	if quotaPool == nil || dontUseQuotaPool {
		return nil
	}

	// Trace if we're running low on available proposal quota; it might explain
	// why we're taking so long.
	if log.HasSpanOrEvent(prop.ctx) {
		if q := quotaPool.approximateQuota(); q < quotaPool.maxQuota()/10 {
			log.Eventf(prop.ctx, "quota running low, currently available ~%d", q)
		}
	}

	return quotaPool.acquire(prop)
}

// a lastUpdateTimesMap is maintained on the Raft leader to keep track of the
// last communication received from followers, which in turn informs the quota
// pool and log truncations.
type lastUpdateTimesMap map[roachpb.ReplicaID]time.Time

// // func (m lastUpdateTimesMap) update(replicaID roachpb.ReplicaID, now time.Time) {
// // 	if m == nil {
// // 		return
// // 	}
// // 	m[replicaID] = now
// // }

// // // updateOnUnquiesce is called when the leader unquiesces. In that case, we
// // // don't want live followers to appear as dead before their next message reaches
// // // us; to achieve that, we optimistically mark all followers that are in
// // // ProgressStateReplicate (or rather, were in that state when the group

// // quiesced) as live as of `now`. We don't want to mark other followers as
// // live as they may be down and could artificially seem alive forever assuming
// // a suitable pattern of quiesce and unquiesce operations (and this in turn
// // can interfere with Raft log truncations).
// func (m lastUpdateTimesMap) updateOnUnquiesce(
// 	descs []roachpb.ReplicaDescriptor, prs map[uint64]raft.Progress, now time.Time,
// ) {
// 	for _, desc := range descs {
// 		if prs[uint64(desc.ReplicaID)].State == raft.ProgressStateReplicate {
// 			m.update(desc.ReplicaID, now)
// 		}
// 	}
// }

// // updateOnBecomeLeader is similar to updateOnUnquiesce, but is called when the
// // replica becomes the Raft leader. It updates all followers irrespective of
// // their Raft state, for the Raft state is not yet populated by the time this
// // callback is invoked. Raft leadership is usually stable, so there is no danger
// // of artificially keeping down followers alive, though if it started
// // flip-flopping at a <10s cadence there would be a risk of that happening.
// func (m lastUpdateTimesMap) updateOnBecomeLeader(descs []roachpb.ReplicaDescriptor, now time.Time) {
// 	for _, desc := range descs {
// 		m.update(desc.ReplicaID, now)
// 	}
// }

// // isFollowerActive returns whether the specified follower has made
// // communication with the leader in the last MaxQuotaReplicaLivenessDuration.
// func (m lastUpdateTimesMap) isFollowerActive(
// 	ctx context.Context, replicaID roachpb.ReplicaID, now time.Time,
// ) bool {
// 	lastUpdateTime, ok := m[replicaID]
// 	if !ok {
// 		// If the follower has no entry in lastUpdateTimes, it has not been
// 		// updated since r became the leader (at which point all then-existing
// 		// replicas were updated).
// 		return false
// 	}
// 	return now.Sub(lastUpdateTime) <= MaxQuotaReplicaLivenessDuration
// }

func makeIDKey() storagebase.CmdIDKey {
	idKeyBuf := make([]byte, 0, raftCommandIDLen)
	idKeyBuf = encoding.EncodeUint64Ascending(idKeyBuf, uint64(rand.Int63()))
	return storagebase.CmdIDKey(idKeyBuf)
}

func (p *Peer) unquiesceAndWakeLeaderLocked() {
	if p.mu.quiescent && p.mu.rawNode != nil {
		ctx := p.AnnotateCtx(context.TODO())
		if log.V(3) {
			log.Infof(ctx, "unquiescing %d: waking leader", p.groupID)
		}
		p.mu.quiescent = false
		p.onUnquiesce()
		if p.shouldCampaign(ctx, p.mu.rawNode.Status()) {
			log.VEventf(ctx, 3, "campaigning")
			if err := p.mu.rawNode.Campaign(); err != nil {
				log.VEventf(ctx, 1, "failed to campaign: %s", err)
			}
		}
		// Propose an empty command which will wake the leader.
		_ = p.mu.rawNode.Propose(encodeRaftCommandV1(makeIDKey(), nil))
	}
}

type raftCommandEncodingVersion byte

// Raft commands are encoded with a 1-byte version (currently 0 or 1), an 8-byte
// ID, followed by the payload. This inflexible encoding is used so we can
// efficiently parse the command id while processing the logs.
//
// TODO(bdarnell): is this commandID still appropriate for our needs?
const (
	// The prescribed length for each command ID.
	raftCommandIDLen = 8
	// The initial Raft command version, used for all regular Raft traffic.
	raftVersionStandard raftCommandEncodingVersion = 0
	// A proposal containing an SSTable which preferably should be sideloaded
	// (i.e. not stored in the Raft log wholesale). Can be treated as a regular
	// proposal when arriving on the wire, but when retrieved from the local
	// Raft log it necessary to inline the payload first as it has usually
	// been sideloaded.
	raftVersionSideloaded raftCommandEncodingVersion = 1
	// The no-split bit is now unused, but we still apply the mask to the first
	// byte of the command for backward compatibility.
	//
	// TODO(tschottdorf): predates v1.0 by a significant margin. Remove.
	raftCommandNoSplitBit  = 1 << 7
	raftCommandNoSplitMask = raftCommandNoSplitBit - 1
)

func encodeRaftCommandV1(commandID storagebase.CmdIDKey, command []byte) []byte {
	return encodeRaftCommand(raftVersionStandard, commandID, command)
}

func encodeRaftCommandV2(commandID storagebase.CmdIDKey, command []byte) []byte {
	return encodeRaftCommand(raftVersionSideloaded, commandID, command)
}

// encode a command ID, an encoded storagebase.RaftCommand, and
// whether the command contains a split.
func encodeRaftCommand(
	version raftCommandEncodingVersion, commandID storagebase.CmdIDKey, command []byte,
) []byte {
	if len(commandID) != raftCommandIDLen {
		panic(fmt.Sprintf("invalid command ID length; %d != %d", len(commandID), raftCommandIDLen))
	}
	x := make([]byte, 1, 1+raftCommandIDLen+len(command))
	x[0] = byte(version)
	x = append(x, []byte(commandID)...)
	x = append(x, command...)
	return x
}

// DecodeRaftCommand splits a raftpb.Entry.Data into its commandID and
// command portions. The caller is responsible for checking that the data
// is not empty (which indicates a dummy entry generated by raft rather
// than a real command). Usage is mostly internal to the storage package
// but is exported for use by debugging tools.
func DecodeRaftCommand(data []byte) (storagebase.CmdIDKey, []byte) {
	v := raftCommandEncodingVersion(data[0] & raftCommandNoSplitMask)
	if v != raftVersionStandard && v != raftVersionSideloaded {
		panic(fmt.Sprintf("unknown command encoding version %v", data[0]))
	}
	return storagebase.CmdIDKey(data[1 : 1+raftCommandIDLen]), data[1+raftCommandIDLen:]
}

func newRaftConfig(
	strg raft.Storage, id uint64, appliedIndex uint64, storeCfg base.RaftConfig, logger raft.Logger,
) *raft.Config {
	return &raft.Config{
		ID:                        id,
		Applied:                   appliedIndex,
		ElectionTick:              storeCfg.RaftElectionTimeoutTicks,
		HeartbeatTick:             storeCfg.RaftHeartbeatIntervalTicks,
		MaxUncommittedEntriesSize: storeCfg.RaftMaxUncommittedEntriesSize,
		MaxCommittedSizePerReady:  storeCfg.RaftMaxCommittedSizePerReady,
		MaxSizePerMsg:             storeCfg.RaftMaxSizePerMsg,
		MaxInflightMsgs:           storeCfg.RaftMaxInflightMsgs,
		Storage:                   strg,
		Logger:                    logger,

		PreVote: true,
	}
}
