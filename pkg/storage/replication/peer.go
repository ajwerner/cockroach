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
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// TODO(ajwerner): unify this setting.

var disableSyncRaftLog = settings.RegisterBoolSetting(
	"kv.replication_raft_log.disable_synchronization_unsafe",
	"set to true to disable synchronization on Raft log writes to persistent storage. "+
		"Setting to true risks data loss or data corruption on server crashes. "+
		"The setting is meant for internal testing only and SHOULD NOT be used in production.",
	false,
)

// MaxCommandSizeFloor is the minimum allowed value for the MaxCommandSize
// cluster setting.
const MaxCommandSizeFloor = 4 << 20 // 4MB

// MaxCommandSize wraps "kv.raft.command.max_size".
var MaxCommandSize = settings.RegisterValidatedByteSizeSetting(
	"kv.replication_raft.command.max_size",
	"maximum size of a raft command",
	64<<20,
	func(size int64) error {
		if size < MaxCommandSizeFloor {
			return fmt.Errorf("max_size must be greater than %s", humanizeutil.IBytes(MaxCommandSizeFloor))
		}
		return nil
	},
)

// Peer represents local replication state for a replica group.
type Peer struct {
	log.AmbientContext
	// TODO(ajwerner): consider just storing a peer factory pointer here
	// and accessing fields either directly or through an interface.
	settings     *settings.Values
	raftConfig   *base.RaftConfig
	testingKnobs *TestingKnobs

	groupID GroupID // Does a Peer need to know this?

	// raftTransport is exclusively for outbound traffic.
	// inbound traffic will come on the message queue.
	raftTransport connect.Conn

	shouldCampaign func(ctx context.Context, status *raft.Status) bool
	onUnquiesce    func()
	storage        engine.Engine
	entryReader    EntryReader
	entryCache     EntryCache

	processCommand     ProcessCommandFunc
	processConfChange  ProcessConfChangeFunc
	raftMessageFactory func(raftpb.Message) RaftMessage

	// mu < msgQueueMu < unreachablesMu
	mu struct {
		syncutil.RWMutex

		truncatedState *roachpb.RaftTruncatedState

		appliedIndex uint64 // same as Replica.mu.state.RaftAppliedIndex?

		// PeerID is the current peer's ID.
		peerID PeerID

		leaderID            PeerID
		ticks               uint64
		lastIndex, lastTerm uint64
		raftLogSize         int64
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
	lastIndex := p.mu.lastIndex // used for append below
	lastTerm := p.mu.lastTerm
	raftLogSize := p.mu.raftLogSize
	leaderID := p.mu.leaderID
	lastLeaderID := leaderID

	// We defer the check to Replica.updateProposalQuotaRaftMuLocked because we need
	// to check it in both cases, if hasReady is false and otherwise.
	// If hasReady == false:
	//     Consider the case when our quota is of size 1 and two out of three
	//     replicas have committed one log entry while the third is lagging
	//     behind. When the third replica finally does catch up and sends
	//     along a MsgAppResp, since the entry is already committed on the
	//     leader replica, no Ready is emitted. But given that the third
	//     replica has caught up, we can release
	//     some quota back to the pool.
	// Otherwise:
	//     Consider the case where there are two replicas and we have a quota
	//     of size 1. We acquire the quota when the write gets proposed on the
	//     leader and expect it to be released when the follower commits it
	//     locally. In order to do so we need to have the entry 'come out of
	//     raft' and in the case of a two node raft group, this only happens if
	//     hasReady == true.
	//     If we don't release quota back at the end of
	//     handleRaftReadyRaftMuLocked, the next write will get blocked.
	defer p.updateProposalQuota(ctx, lastLeaderID)
	err := p.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
		if hasReady = raftGroup.HasReady(); hasReady {
			rd = raftGroup.Ready()
		}
		return hasReady /* unquiesceAndWakeLeader */, nil
	})
	p.mu.Unlock()
	if err != nil {
		const expl = "while checking raft group for Ready"
		return stats, expl, errors.Wrap(err, expl)
	}

	if !hasReady {
		return stats, "", nil
	}

	logRaftReady(ctx, rd)
	refreshReason := noReason
	if rd.SoftState != nil && leaderID != PeerID(rd.SoftState.Lead) {
		// TODO(ajwerner): deal with leadership changes?
		log.Infof(ctx, "Leader changed? %v %v", rd.SoftState, leaderID)
		if log.V(3) {
			log.Infof(ctx, "raft leader changed: %d -> %d", leaderID, rd.SoftState.Lead)
		}
		if !p.testingKnobs.DisableRefreshReasonNewLeader {
			refreshReason = reasonNewLeader
		}
		leaderID = PeerID(rd.SoftState.Lead)
	}

	if !raft.IsEmptySnap(rd.Snapshot) {
		snapUUID, err := uuid.FromBytes(rd.Snapshot.Data)
		log.Infof(ctx, "got a snapshot? %v %v", snapUUID, err)
		panic("snapshots not implemented")
	}

	// Separate the MsgApp messages from all other Raft message types so that we
	// can take advantage of the optimization discussed in the Raft thesis under
	// the section: `10.2.1 Writing to the leader’s disk in parallel`. The
	// optimization suggests that instead of a leader writing new log entries to
	// disk before replicating them to its followers, the leader can instead
	// write the entries to disk in parallel with replicating to its followers
	// and them writing to their disks.
	//
	// Here, we invoke this optimization by:
	// 1. sending all MsgApps.
	// 2. syncing all entries and Raft state to disk.
	// 3. sending all other messages.
	//
	// Since this is all handled in handleRaftReadyRaftMuLocked, we're assured
	// that even though we may sync new entries to disk after sending them in
	// MsgApps to followers, we'll always have them synced to disk before we
	// process followers' MsgAppResps for the corresponding entries because this
	// entire method requires RaftMu to be locked. This is a requirement because
	// etcd/raft does not support commit quorums that do not include the leader,
	// even though the Raft thesis states that this would technically be safe:
	// > The leader may even commit an entry before it has been written to its
	// > own disk, if a majority of followers have written it to their disks;
	// > this is still safe.
	//
	// However, MsgApps are also used to inform followers of committed entries
	// through the Commit index that they contains. Because the optimization
	// sends all MsgApps before syncing to disc, we may send out a commit index
	// in a MsgApp that we have not ourselves written in HardState.Commit. This
	// is ok, because the Commit index can be treated as volatile state, as is
	// supported by raft.MustSync. The Raft thesis corroborates this, stating in
	// section: `3.8 Persisted state and server restarts` that:
	// > Other state variables are safe to lose on a restart, as they can all be
	// > recreated. The most interesting example is the commit index, which can
	// > safely be reinitialized to zero on a restart.
	msgApps, otherMsgs := splitMsgApps(rd.Messages)
	p.traceMessageSends(msgApps, "sending msgApp")
	p.sendRaftMessages(ctx, msgApps)

	// Use a more efficient write-only batch because we don't need to do any
	// reads from the batch. Any reads are performed via the "distinct" batch
	// which passes the reads through to the underlying DB.
	batch := p.storage.NewWriteOnlyBatch()
	defer batch.Close()

	// We know that all of the writes from here forward will be to distinct keys.
	writer := batch.Distinct()
	prevLastIndex := lastIndex
	if len(rd.Entries) > 0 {
		// All of the entries are appended to distinct keys, returning a new
		// last index.
		thinEntries, sideLoadedEntriesSize, err := p.maybeSideloadEntriesRaftMuLocked(ctx, rd.Entries)
		if err != nil {
			const expl = "during sideloading"
			return stats, expl, errors.Wrap(err, expl)
		}
		raftLogSize += sideLoadedEntriesSize
		if lastIndex, lastTerm, raftLogSize, err = p.append(
			ctx, writer, lastIndex, lastTerm, raftLogSize, thinEntries,
		); err != nil {
			const expl = "during append"
			return stats, expl, errors.Wrap(err, expl)
		}
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		log.Infof(ctx, "setting that hard state %v %v", p.raftMu.stateLoader, rd.HardState)
		if err := p.raftMu.stateLoader.SetHardState(ctx, writer, rd.HardState); err != nil {
			const expl = "during setHardState"
			return stats, expl, errors.Wrap(err, expl)
		}
	}
	writer.Close()
	// Synchronously commit the batch with the Raft log entries and Raft hard
	// state as we're promising not to lose this data.
	//
	// Note that the data is visible to other goroutines before it is synced to
	// disk. This is fine. The important constraints are that these syncs happen
	// before Raft messages are sent and before the call to RawNode.Advance. Our
	// regular locking is sufficient for this and if other goroutines can see the
	// data early, that's fine. In particular, snapshots are not a problem (I
	// think they're the only thing that might access log entries or HardState
	// from other goroutines). Snapshots do not include either the HardState or
	// uncommitted log entries, and even if they did include log entries that
	// were not persisted to disk, it wouldn't be a problem because raft does not
	// infer the that entries are persisted on the node that sends a snapshot.
	//commitStart := timeutil.Now()
	mustSync := rd.MustSync && !disableSyncRaftLog.Get(p.settings)
	if err := batch.Commit(mustSync); err != nil {
		const expl = "while committing batch"
		return stats, expl, errors.Wrap(err, expl)
	}
	//elapsed := timeutil.Since(commitStart)
	// TODO(ajwerner): metrics
	// r.metrics.RaftLogCommitLatency.RecordValue(elapsed.Nanoseconds())

	if len(rd.Entries) > 0 {
		// We may have just overwritten parts of the log which contain
		// sideloaded SSTables from a previous term (and perhaps discarded some
		// entries that we didn't overwrite). Remove any such leftover on-disk
		// payloads (we can do that now because we've committed the deletion
		// just above).
		firstPurge := rd.Entries[0].Index // first new entry written
		purgeTerm := rd.Entries[0].Term - 1
		lastPurge := prevLastIndex // old end of the log, include in deletion
		purgedSize, err := maybePurgeSideloaded(ctx, p.raftMu.sideloaded, firstPurge, lastPurge, purgeTerm)
		if err != nil {
			const expl = "while purging sideloaded storage"
			return stats, expl, err
		}
		raftLogSize -= purgedSize
		if raftLogSize < 0 {
			// Might have gone negative if node was recently restarted.
			raftLogSize = 0
		}

	}

	// Update protected state - last index, last term, raft log size, and raft
	// leader ID.
	p.mu.Lock()
	p.mu.lastIndex = lastIndex
	p.mu.lastTerm = lastTerm
	p.mu.raftLogSize = raftLogSize
	var becameLeader bool
	if p.mu.leaderID != leaderID {
		p.mu.leaderID = leaderID
		// Clear the remote proposal set. Would have been nil already if not
		// previously the leader.
		becameLeader = p.mu.leaderID == p.mu.peerID
	}
	// When becoming the leader, proactively add the replica to the replicate
	// queue. We might have been handed leadership by a remote node which wanted
	// to remove itself from the range.
	if becameLeader /* && r.store.replicateQueue != nil */ {
		// TODO(ajwerner): deal with this
		log.Infof(ctx, "I became leader and should do something about it with the replicate queue: %v",
			becameLeader)
	}
	p.mu.Unlock()

	// Update raft log entry cache. We clear any older, uncommitted log entries
	// and cache the latest ones.
	p.entryCache.Add(rd.Entries, true /* truncate */)
	p.sendRaftMessages(ctx, otherMsgs)
	p.traceEntries(rd.CommittedEntries, "committed, before applying any entries")
	// applicationStart := timeutil.Now()
	var commitBatch engine.Batch
	if len(rd.CommittedEntries) > 0 {
		commitBatch = p.storage.NewBatch()
		defer commitBatch.Close()
	}
	for _, e := range rd.CommittedEntries {
		switch e.Type {
		case raftpb.EntryNormal:
			// NB: Committed entries are handed to us by Raft. Raft does not
			// know about sideloading. Consequently the entries here are all
			// already inlined.

			var commandID storagebase.CmdIDKey

			// Process committed entries. etcd raft occasionally adds a nil entry
			// (our own commands are never empty). This happens in two situations:
			// When a new leader is elected, and when a config change is dropped due
			// to the "one at a time" rule. In both cases we may need to resubmit our
			// pending proposals (In the former case we resubmit everything because
			// we proposed them to a former leader that is no longer able to commit
			// them. In the latter case we only need to resubmit pending config
			// changes, but it's hard to distinguish so we resubmit everything
			// anyway). We delay resubmission until after we have processed the
			// entire batch of entries.
			if len(e.Data) == 0 {
				// Overwrite unconditionally since this is the most aggressive
				// reproposal mode.
				if !p.testingKnobs.DisableRefreshReasonNewLeaderOrConfigChange {
					refreshReason = reasonNewLeaderOrConfigChange
				}
				commandID = "" // special-cased value, command isn't used
			} else {

				// // commandID, encodedCommand = DecodeRaftCommand(e.Data)
				// // // An empty command is used to unquiesce a range and wake the
				// // // leader. Clear commandID so it's ignored for processing.
				// // if len(encodedCommand) == 0 {
				// // 	commandID = ""
				// // } else if err := protoutil.Unmarshal(encodedCommand, &command); err != nil {
				// // 	const expl = "while unmarshalling entry"
				// // 	return stats, expl, errors.Wrap(err, expl)
				// // }
			}

			p.processRaftCommand(ctx, commitBatch, e.Term, e.Index, e.Data)
			// r.store.metrics.RaftCommandsApplied.Inc(1)
			stats.processed++

			p.mu.Lock()
			if p.mu.peerID == p.mu.leaderID {
				// At this point we're not guaranteed to have proposalQuota
				// initialized, the same is true for quotaReleaseQueue and
				// commandSizes. By checking if the specified commandID is
				// present in commandSizes, we'll only queue the cmdSize if
				// they're all initialized.
				if cmdSize, ok := p.mu.commandSizes[commandID]; ok {
					p.mu.quotaReleaseQueue = append(p.mu.quotaReleaseQueue, cmdSize)
					delete(p.mu.commandSizes, commandID)
				}
			}
			p.mu.Unlock()

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := protoutil.Unmarshal(e.Data, &cc); err != nil {
				const expl = "while unmarshaling ConfChange"
				return stats, expl, errors.Wrap(err, expl)
			}
			// var ccCtx ConfChangeContext
			// if err := protoutil.Unmarshal(cc.Context, &ccCtx); err != nil {
			// 	const expl = "while unmarshaling ConfChangeContext"
			// 	return stats, expl, errors.Wrap(err, expl)
			// }
			var commandID storagebase.CmdIDKey
			var command storagepb.RaftCommand
			var encodedCommand []byte
			commandID, encodedCommand = DecodeRaftCommand(cc.Context)
			// An empty command is used to unquiesce a range and wake the
			// leader. Clear commandID so it's ignored for processing.
			if len(encodedCommand) == 0 {
				commandID = ""
			} else if err := protoutil.Unmarshal(encodedCommand, &command); err != nil {
				const expl = "while unmarshalling entry"
				return stats, expl, errors.Wrap(err, expl)
			}
			if changedRepl := p.processRaftConfChange(ctx, commitBatch, e.Term, e.Index, e.Data); !changedRepl {
				// If we did not apply the config change, tell raft that the config change was aborted.
				cc = raftpb.ConfChange{}
			}
			stats.processed++

			p.mu.Lock()
			if p.mu.peerID == p.mu.leaderID {
				if cmdSize, ok := p.mu.commandSizes[commandID]; ok {
					p.mu.quotaReleaseQueue = append(p.mu.quotaReleaseQueue, cmdSize)
					delete(p.mu.commandSizes, commandID)
				}
			}
			p.mu.Unlock()

			if err := p.withRaftGroup(true, func(raftGroup *raft.RawNode) (bool, error) {
				raftGroup.ApplyConfChange(cc)
				return true, nil
			}); err != nil {
				const expl = "during ApplyConfChange"
				return stats, expl, errors.Wrap(err, expl)
			}
		default:
			log.Fatalf(ctx, "unexpected Raft entry: %v", e)
		}
	}
	if commitBatch != nil {
		if err := commitBatch.Commit(false); err != nil {
			panic(err)
		}
	}

	// applicationElapsed := timeutil.Since(applicationStart).Nanoseconds()
	// r.store.metrics.RaftApplyCommittedLatency.RecordValue(applicationElapsed)
	if refreshReason != noReason {
		p.mu.Lock()
		//p.refreshProposalsLocked(0, refreshReason)
		p.mu.Unlock()
		log.Warningf(ctx, "TODO(ajwerner): not sure how to handle refresh reason: %v", refreshReason)
	}

	// TODO(bdarnell): need to check replica id and not Advance if it
	// has changed. Or do we need more locking to guarantee that replica
	// ID cannot change during handleRaftReady?
	const expl = "during advance"
	if err := p.withRaftGroup(true, func(raftGroup *raft.RawNode) (bool, error) {
		raftGroup.Advance(rd)

		// If the Raft group still has more to process then we immediately
		// re-enqueue it for another round of processing. This is possible if
		// the group's committed entries were paginated due to size limitations
		// and we didn't apply all of them in this pass.
		if raftGroup.HasReady() {
			// TODO(ajwerner): deal with this case
			log.Warningf(ctx, "need to reenqueue this peer for raftUpdateCheck")
			// p.store.enqueueRaftUpdateCheck(r.RangeID)
		}
		return true, nil
	}); err != nil {
		return stats, expl, errors.Wrap(err, expl)
	}
	return stats, "", nil

}

func (p *Peer) sendRaftMessages(ctx context.Context, messages []raftpb.Message) {
	var lastAppResp raftpb.Message
	for _, message := range messages {
		drop := false
		switch message.Type {
		case raftpb.MsgApp:
			if util.RaceEnabled {
				// Iterate over the entries to assert that all sideloaded commands
				// are already inlined. replicaRaftStorage.Entries already performs
				// the sideload inlining for stable entries and raft.unstable always
				// contain fat entries. Since these are the only two sources that
				// raft.sendAppend gathers entries from to populate MsgApps, we
				// should never see thin entries here.
				for j := range message.Entries {
					assertSideloadedRaftCommandInlined(ctx, &message.Entries[j])
				}
			}

		case raftpb.MsgAppResp:
			// A successful (non-reject) MsgAppResp contains one piece of
			// information: the highest log index. Raft currently queues up
			// one MsgAppResp per incoming MsgApp, and we may process
			// multiple messages in one handleRaftReady call (because
			// multiple messages may arrive while we're blocked syncing to
			// disk). If we get redundant MsgAppResps, drop all but the
			// last (we've seen that too many MsgAppResps can overflow
			// message queues on the receiving side).
			//
			// Note that this reorders the chosen MsgAppResp relative to
			// other messages (including any MsgAppResps with the Reject flag),
			// but raft is fine with this reordering.
			//
			// TODO(bdarnell): Consider pushing this optimization into etcd/raft.
			// Similar optimizations may be possible for other message types,
			// although MsgAppResp is the only one that has been seen as a
			// problem in practice.
			if !message.Reject && message.Index > lastAppResp.Index {
				lastAppResp = message
				drop = true
			}
		}

		if !drop {
			p.sendRaftMessage(ctx, message)
		}
	}
	if lastAppResp.Index > 0 {
		p.sendRaftMessage(ctx, lastAppResp)
	}
}

// sendRaftMessage sends a Raft message.
func (p *Peer) sendRaftMessage(ctx context.Context, msg raftpb.Message) {
	p.mu.Lock()
	if msg.Type == raftpb.MsgHeartbeat {
		if p.mu.peerID == 0 {
			log.Fatalf(ctx, "preemptive snapshot attempted to send a heartbeat: %+v", msg)
		}
		// For followers, we update lastUpdateTimes when we step a message from
		// them into the local Raft group. The leader won't hit that path, so we
		// update it whenever it sends a heartbeat. In effect, this makes sure
		// it always sees itself as alive.
		p.mu.lastUpdateTimes.update(p.mu.peerID, timeutil.Now())
	}
	p.mu.Unlock()

	// TODO(ajwerner): deal with snapshots, probably do this above inside the
	// conn. Also coalesced heartbeats.
	raftMsg := p.raftMessageFactory(msg)
	if raftMsg != nil {
		p.raftTransport.Send(ctx, raftMsg)
	}

	// TODO(ajwerner): need to figure out how much the peer needs to know about
	// where remote replicas live. Who tracks this? It probably can't be the
	// transport exactly. We want to just
}

func (p *Peer) processRaftCommand(
	ctx context.Context, eng engine.ReadWriter, term, raftIndex uint64, command []byte,
) {
	log.Infof(ctx, "processRaftCommand %v %v %v", term, raftIndex, command)
	if len(command) == 0 {
		return
	}
	id, _ := DecodeRaftCommand(command)

	p.processCommand(ctx, eng, term, raftIndex, command)
	p.mu.Lock()

	prop, exists := p.mu.proposals[id]
	if exists {
		delete(p.mu.proposals, id)
		defer prop.pc.cond.Signal()
	}
	p.mu.Unlock()
}

func (p *Peer) processRaftConfChange(
	ctx context.Context, eng engine.ReadWriter, term, raftIndex uint64, command []byte,
) (changeRepl bool) {
	return p.processConfChange(ctx, eng, term, raftIndex, command)
}

// TODO(ajwerner): add feedback for failed message sends

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

func (p *Peer) withRaftGroup(
	mayCampaignOnWake bool, f func(r *raft.RawNode) (unquiesceAndWakeLeader bool, _ error),
) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.withRaftGroupLocked(mayCampaignOnWake, f)
}

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
		log.Infof(ctx, "raft group: %v %v", raftGroup, raftGroup.Status())

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
	// TODO(ajwerner): deal with returning things?
	proposalSize := prop.msg.Size()
	if proposalSize > int(MaxCommandSize.Get(p.settings)) {
		// Once a command is written to the raft log, it must be loaded
		// into memory and replayed on all replicas. If a command is
		// too big, stop it here.
		panic(roachpb.NewError(errors.Errorf(
			"command is too large: %d bytes (max: %d)",
			proposalSize, MaxCommandSize.Get(p.settings),
		)))
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.mu.rawNode == nil {
		// Unlock first before locking in {raft,replica}mu order.
		p.mu.Unlock()
		p.raftMu.Lock()
		defer p.raftMu.Unlock()
		p.mu.Lock()
		log.Event(prop.ctx, "acquired {raft,replica}mu")
	} else {
		log.Event(prop.ctx, "acquired replica mu")
	}
	id := prop.msg.ID()
	// Add size of proposal to commandSizes map.
	if p.mu.commandSizes != nil {
		p.mu.commandSizes[id] = proposalSize
	}
	p.mu.proposals[id] = prop
	// TODO(ajwerner): deal with replica state here?
	// Some sort of callback magic for dealing with grabbing lease index?
	switch msg := prop.msg.(type) {
	case ProposalMessage:
		if log.V(4) {
			log.Infof(prop.ctx, "proposing command %x", id)
		}
		if err := p.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
			// We're proposing a command so there is no need to wake the leader if
			// we're quiesced.
			// TODO(ajwerner): deal with unquiesce
			// p.unquiesceLocked()
			return false /* unquiesceAndWakeLeader */, raftGroup.Propose(msg.Encoded())
		}); err != nil {
			if err == raft.ErrProposalDropped {
				log.Infof(prop.ctx, "failed to submit proposal %q: %v", msg.ID(), err)
			} else {
				panic(err)
			}
		}
		// So then at the end we need to go and ack all of the proposals right?
	case ConfChangeMessage:
		// EndTransactionRequest with a ChangeReplicasTrigger is special
		// because raft needs to understand it; it cannot simply be an
		// opaque command.
		log.Infof(prop.ctx, "proposing %s", msg)

		// Ensure that we aren't trying to remove ourselves from the range without
		// having previously given up our lease, since the range won't be able
		// to make progress while the lease is owned by a removed replica (and
		// leases can stay in such a state for a very long time when using epoch-
		// based range leases). This shouldn't happen often, but has been seen
		// before (#12591).

		if msg.ChangeType() == raftpb.ConfChangeRemoveNode && msg.PeerID() == p.mu.peerID {
			log.Errorf(prop.ctx, "received invalid ChangeReplicasTrigger %s to remove self (leaseholder)", msg)
			panic(errors.Errorf("%v: received invalid ChangeReplicasTrigger %v to remove self (leaseholder)", p, p.mu.peerID))
		}
		if err := p.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
			// We're proposing a command here so there is no need to wake the
			// leader if we were quiesced.
			// p.unquiesceLocked()
			return false, /* unquiesceAndWakeLeader */
				raftGroup.ProposeConfChange(raftpb.ConfChange{
					Type:    msg.ChangeType(),
					NodeID:  uint64(msg.PeerID()),
					Context: []byte(msg.Encoded()),
				})
		}); err != nil {
			panic(err)
		}
	}
}

func (p *Peer) updateProposalQuota(ctx context.Context, lastLeaderID PeerID) {
	if r := recover(); r != nil {
		panic(r)
	}
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
type lastUpdateTimesMap map[PeerID]time.Time

func (m lastUpdateTimesMap) update(peerID PeerID, now time.Time) {
	if m == nil {
		return
	}
	m[peerID] = now
}

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
			log.VEventf(ctx, 3, "campaigning ")
			if err := p.mu.rawNode.Campaign(); err != nil {
				log.VEventf(ctx, 1, "failed to campaign: %s", err)
			}
		}
		// Propose an empty command which will wake the leader.
		_ = p.mu.rawNode.Propose(EncodeRaftCommandV1(makeIDKey(), nil))
	}
}

type raftCommandEncodingVersion byte

// Raft commands are encoded with a 1-byte version (currently 0 or 1), an 8-byte
// ID, followed by the payload. This inflexible encoding is used so we can
// efficiently parse the command id while processing the logs.
//
// TODO(bdarnell): is this commandID still appropriate for our needs?
const (
	// The initial Raft command version, used for all regular Raft traffic.
	raftVersionStandard raftCommandEncodingVersion = 0
	// A proposal containing an SSTable which preferably should be sideloaded
	// (i.e. not stored in the Raft log wholesale). Can be treated as a regular
	// proposal when arriving on the wire, but when retrieved from the local
	// Raft log it necessary to inline the payload first as it has usually
	// been sideloaded.
	raftVersionSideloaded raftCommandEncodingVersion = 1
	// The prescribed length for each command ID.
	raftCommandIDLen = 8
	// The prescribed length of each encoded command's prefix.
	raftCommandPrefixLen = 1 + raftCommandIDLen
	// The no-split bit is now unused, but we still apply the mask to the first
	// byte of the command for backward compatibility.
	//
	// TODO(tschottdorf): predates v1.0 by a significant margin. Remove.
	raftCommandNoSplitBit  = 1 << 7
	raftCommandNoSplitMask = raftCommandNoSplitBit - 1
)

func MakeIDKey() storagebase.CmdIDKey {
	idKeyBuf := make([]byte, 0, raftCommandIDLen)
	idKeyBuf = encoding.EncodeUint64Ascending(idKeyBuf, uint64(rand.Int63()))
	return storagebase.CmdIDKey(idKeyBuf)
}

func EncodeRaftCommandV1(commandID storagebase.CmdIDKey, command []byte) EncodedCommand {
	return encodeRaftCommand(raftVersionStandard, commandID, command)
}

func EncodeRaftCommandV2(commandID storagebase.CmdIDKey, command []byte) EncodedCommand {
	return encodeRaftCommand(raftVersionSideloaded, commandID, command)
}

func encodeRaftCommand(
	version raftCommandEncodingVersion, commandID storagebase.CmdIDKey, command []byte,
) []byte {
	b := make([]byte, raftCommandPrefixLen+len(command))
	encodeRaftCommandPrefix(b[:raftCommandPrefixLen], version, commandID)
	copy(b[raftCommandPrefixLen:], command)
	return b
}

func encodeRaftCommandPrefix(
	b []byte, version raftCommandEncodingVersion, commandID storagebase.CmdIDKey,
) {
	if len(commandID) != raftCommandIDLen {
		panic(fmt.Sprintf("invalid command ID length; %d != %d", len(commandID), raftCommandIDLen))
	}
	if len(b) != raftCommandPrefixLen {
		panic(fmt.Sprintf("invalid command prefix length; %d != %d", len(b), raftCommandPrefixLen))
	}
	b[0] = byte(version)
	copy(b[1:], []byte(commandID))
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

// splitMsgApps splits the Raft message slice into two slices, one containing
// MsgApps and one containing all other message types. Each slice retains the
// relative ordering between messages in the original slice.
func splitMsgApps(msgs []raftpb.Message) (msgApps, otherMsgs []raftpb.Message) {
	splitIdx := 0
	for i, msg := range msgs {
		if msg.Type == raftpb.MsgApp {
			msgs[i], msgs[splitIdx] = msgs[splitIdx], msgs[i]
			splitIdx++
		}
	}
	return msgs[:splitIdx], msgs[splitIdx:]
}
