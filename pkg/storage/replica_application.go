package storage

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// entryApplicationState stores the state required to apply an entry to a
// replica. The state is accumulated over a number of steps as a part of an
// entryApplicationBatch.
//
// RaftCommands are classified as either being "trivial" or "non-trivial" based
// on their implied change to the Replica state machine (see isTrivial). Trivial
// commands are safe to
type entryApplicationState struct {
	ctx              context.Context
	e                *raftpb.Entry
	idKey            storagebase.CmdIDKey
	raftCmd          storagepb.RaftCommand
	proposalRetry    proposalReevaluationReason
	forcedErr        *roachpb.Error
	proposedLocally  bool
	proposal         *ProposalData
	leaseIndex       uint64
	lResult          *result.LocalResult
	response         proposalResult
	cc               raftpb.ConfChange
	ccCtx            ConfChangeContext
	splitMergeUnlock func(*storagepb.ReplicatedEvalResult)
}

func isTrivial(usingStateAppliedKey bool, r *storagepb.ReplicatedEvalResult) bool {
	return !r.IsLeaseRequest &&
		!r.BlockReads &&
		r.Split == nil &&
		r.Merge == nil &&
		r.ComputeChecksum == nil &&
		r.ChangeReplicas == nil &&
		r.AddSSTable == nil &&
		(r.State == nil ||
			(r.State.GCThreshold == nil &&
				r.State.TxnSpanGCThreshold == nil &&
				r.State.Lease == nil &&
				r.State.Desc == nil &&
				(usingStateAppliedKey || !r.State.UsingAppliedStateKey)))
}

func (s *entryApplicationState) initialize(ctx context.Context, e *raftpb.Entry) {
	*s = entryApplicationState{
		ctx: ctx,
		e:   e,
	}
}

func (s *entryApplicationState) decode() (expl string, err error) {
	switch s.e.Type {
	case raftpb.EntryNormal:
		return s.decodeNormalEntry(s.e)
	case raftpb.EntryConfChange:
		return s.decodeConfChangeEntry(s.e)
	default:
		log.Fatalf(s.ctx, "unexpected Raft entry: %v", s.e)
		return "", nil // unreachable
	}
}

func (s *entryApplicationState) decodeNormalEntry(e *raftpb.Entry) (expl string, err error) {
	var encodedCommand []byte
	s.idKey, encodedCommand = DecodeRaftCommand(e.Data)
	// An empty command is used to unquiesce a range and wake the
	// leader. Clear commandID so it's ignored for processing.
	if len(encodedCommand) == 0 {
		s.idKey = ""
	} else if err := protoutil.Unmarshal(encodedCommand, &s.raftCmd); err != nil {
		const expl = "while unmarshalling entry"
		return expl, errors.Wrap(err, expl)
	}
	return "", nil
}

func (s *entryApplicationState) decodeConfChangeEntry(e *raftpb.Entry) (expl string, err error) {
	if err := protoutil.Unmarshal(e.Data, &s.cc); err != nil {
		const expl = "while unmarshaling ConfChange"
		return expl, errors.Wrap(err, expl)
	}
	if err := protoutil.Unmarshal(s.cc.Context, &s.ccCtx); err != nil {
		const expl = "while unmarshaling ConfChangeContext"
		return expl, errors.Wrap(err, expl)
	}
	if err := protoutil.Unmarshal(s.ccCtx.Payload, &s.raftCmd); err != nil {
		const expl = "while unmarshaling RaftCommand"
		return expl, errors.Wrap(err, expl)
	}
	s.idKey = storagebase.CmdIDKey(s.ccCtx.CommandID)
	return "", nil
}

// entryApplicationBatch accumulates state due to the application of raft
// commands. Committed raft commands are applied to the batch in a multi-stage
// process whereby individual commands are decoded, prepared for application
// relative to the current view of replicaState.
type entryApplicationBatch struct {
	batch                 engine.Batch
	refreshReason         *refreshRaftReason
	stats                 enginepb.MVCCStats
	replicaState          storagepb.ReplicaState
	updatedTruncatedState bool
	appStates             entryApplicationStateBuf
}

var entryApplicationBatchSyncPool = sync.Pool{
	New: func() interface{} { return new(entryApplicationBatch) },
}

func getEntryApplicationBatch() *entryApplicationBatch {
	return entryApplicationBatchSyncPool.Get().(*entryApplicationBatch)
}

func putEntryApplicationBatch(b *entryApplicationBatch) {
	b.appStates.destroy()
	*b = entryApplicationBatch{}
	entryApplicationBatchSyncPool.Put(b)
}

func clearTrivialReplicatedEvalResultFields(
	usingAppliedStateKey bool, rResult *storagepb.ReplicatedEvalResult,
) {
	// Fields for which no action is taken in this method are zeroed so that
	// they don't trigger an assertion at the end of the method (which checks
	// that all fields were handled).
	rResult.IsLeaseRequest = false
	rResult.Timestamp = hlc.Timestamp{}
	rResult.PrevLeaseProposal = nil
	// The state fields cleared here were already applied to the in-memory view of
	// replica state for this batch.
	if haveState := rResult.State != nil; haveState {
		rResult.State.Stats = nil
		rResult.State.TruncatedState = nil

		// If we're already using the AppliedStateKey then there's nothing
		// to do. This flag is idempotent so it's ok that we see this flag
		// multiple times, but we want to make sure it doesn't cause us to
		// perform repeated state assertions, so clear it before the
		// shouldAssert determination.
		// A reader might wonder if using the value of usingAppliedState key from
		// after applying an entire batch is valid to determine whether this command
		// implied a transition, but if it had implied a transition then the batch
		// would not have been considered trivial and the current view of will still
		// be false as complex state transitions are handled after this call.
		if usingAppliedStateKey {
			rResult.State.UsingAppliedStateKey = false
		}
		// ReplicaState.Stats was previously non-nullable which caused nodes to
		// send a zero-value MVCCStats structure. If the proposal was generated by
		// an old node, we'll have decoded that zero-value structure setting
		// ReplicaState.Stats to a non-nil value which would trigger the "unhandled
		// field in ReplicatedEvalResult" assertion to fire if we didn't clear it.
		if rResult.State.Stats != nil && (*rResult.State.Stats == enginepb.MVCCStats{}) {
			rResult.State.Stats = nil
		}
		if *rResult.State == (storagepb.ReplicaState{}) {
			rResult.State = nil
		}
	}
	rResult.Delta = enginepb.MVCCStatsDelta{}
}

func (b *entryApplicationBatch) isNonTrivial() bool {
	return b.appStates.len == 1 &&
		!isTrivial(b.replicaState.UsingAppliedStateKey,
			&b.appStates.first().raftCmd.ReplicatedEvalResult)
}

// applyBatch handles the logic of applying an accumulated entryApplication
// batch to the Replica state machine.
func (r *Replica) applyBatch(
	ctx context.Context, b *entryApplicationBatch, stats *handleRaftReadyStats,
) (expl string, err error) {
	// TODO(ajwerner): clean up this sanity check
	if log.V(2) {
		log.Infof(ctx, "flushing batch %v %v", b.replicaState, b.appStates.len)
	}
	if b.batch == nil || b.appStates.len == 0 {
		panic("invalid empty batch")
	}

	if err := b.batch.Commit(false); err != nil {
		log.Fatalf(ctx, "failed to commit Raft entry batch: %v", err)
	}
	b.batch.Close()
	b.batch = nil
	isNonTrivial := b.isNonTrivial()
	if isNonTrivial {
		s := b.appStates.first()
		if unlock := s.splitMergeUnlock; unlock != nil {
			defer unlock(&s.raftCmd.ReplicatedEvalResult)
		}
		if s.raftCmd.ReplicatedEvalResult.BlockReads {
			r.readOnlyCmdMu.Lock()
			defer r.readOnlyCmdMu.Unlock()
			s.raftCmd.ReplicatedEvalResult.BlockReads = false
		}
	}
	// Once it's committed we can go about applying the truncated state
	var truncationDelta int64
	if b.updatedTruncatedState {
		truncState := b.replicaState.TruncatedState
		r.store.raftEntryCache.Clear(r.RangeID, truncState.Index+1)
		// Truncate the sideloaded storage. Note that this is safe only if the new truncated state
		// is durably on disk (i.e.) synced. This is true at the time of writing but unfortunately
		// could rot.
		{
			log.VEventf(ctx, 1, "truncating sideloaded storage up to (and including) index %d", truncState.Index)
			if truncationDelta, _, err = r.raftMu.sideloaded.TruncateTo(ctx, truncState.Index+1); err != nil {
				// We don't *have* to remove these entries for correctness. Log a
				// loud error, but keep humming along.
				log.Errorf(ctx, "while removing sideloaded files during log truncation: %s", err)
			}
		}
	}
	applyRaftLogDelta := func(raftLogDelta int64) {
		if raftLogDelta == 0 {
			return
		}
		r.mu.raftLogSize += raftLogDelta
		if r.mu.raftLogSize < 0 {
			r.mu.raftLogSize = 0
		}
		r.mu.raftLogLastCheckSize += raftLogDelta
		if r.mu.raftLogLastCheckSize < 0 {
			r.mu.raftLogLastCheckSize = 0
		}
	}
	// deltaStats will store the delta from the current state to the new state
	// which will be used to update the metrics.
	deltaStats := *b.replicaState.Stats
	r.mu.Lock()
	r.mu.state.RaftAppliedIndex = b.replicaState.RaftAppliedIndex
	r.mu.state.LeaseAppliedIndex = b.replicaState.LeaseAppliedIndex
	deltaStats.Subtract(*r.mu.state.Stats)
	*r.mu.state.Stats = *b.replicaState.Stats
	// Iterate through the commands and their replicated eval results to apply
	// their raft log deltas.
	// Finally apply the truncation delta.
	// Store the queuing conditions
	var it entryApplicationStateBufIterator
	for ok := it.init(&b.appStates); ok; ok = it.next() {
		s := it.state()
		applyRaftLogDelta(s.raftCmd.ReplicatedEvalResult.RaftLogDelta)
		s.raftCmd.ReplicatedEvalResult.RaftLogDelta = 0
	}
	applyRaftLogDelta(-1 * truncationDelta)
	checkRaftLog := r.mu.raftLogSize-r.mu.raftLogLastCheckSize >= RaftLogQueueStaleSize
	needsSplitBySize := r.needsSplitBySizeRLocked()
	needsMergeBySize := r.needsMergeBySizeRLocked()
	if b.updatedTruncatedState {
		r.mu.state.TruncatedState = b.replicaState.TruncatedState
	}
	r.mu.Unlock()
	r.store.metrics.addMVCCStats(deltaStats)
	// NB: the bootstrap store has a nil split queue.
	// TODO(tbg): the above is probably a lie now.
	if r.store.splitQueue != nil && needsSplitBySize && r.splitQueueThrottle.ShouldProcess(timeutil.Now()) {
		r.store.splitQueue.MaybeAddAsync(ctx, r, r.store.Clock().Now())
	}
	// The bootstrap store has a nil merge queue.
	// TODO(tbg): the above is probably a lie now.
	if r.store.mergeQueue != nil && needsMergeBySize && r.mergeQueueThrottle.ShouldProcess(timeutil.Now()) {
		// TODO(tbg): for ranges which are small but protected from merges by
		// other means (zone configs etc), this is called on every command, and
		// fires off a goroutine each time. Make this trigger (and potentially
		// the split one above, though it hasn't been observed to be as
		// bothersome) less aggressive.
		r.store.mergeQueue.MaybeAddAsync(ctx, r, r.store.Clock().Now())
	}
	if checkRaftLog {
		r.store.raftLogQueue.MaybeAddAsync(ctx, r, r.store.Clock().Now())
	}
	for ok := it.init(&b.appStates); ok; ok = it.next() {
		s := it.state()
		rResult := &s.raftCmd.ReplicatedEvalResult
		for _, sc := range rResult.SuggestedCompactions {
			r.store.compactor.Suggest(s.ctx, sc)
		}
		rResult.SuggestedCompactions = nil
	}

	for ok := it.init(&b.appStates); ok; ok = it.next() {
		s := it.state()
		// Set up the local result prior to handling the ReplicatedEvalResult to
		// give testing knobs an opportunity to inspect it.
		r.prepareLocalResult(s.ctx, s)
		rResult := &s.raftCmd.ReplicatedEvalResult
		// Handle the ReplicatedEvalResult, executing any side effects of the last
		// state machine transition.
		//
		// Note that this must happen after committing (the engine.Batch), but
		// before notifying a potentially waiting client.
		clearTrivialReplicatedEvalResultFields(b.replicaState.UsingAppliedStateKey, rResult)
		if isNonTrivial && !r.handleComplexReplicatedEvalResult(s.ctx, *rResult) {
			panic("non-trivial batch did not require state assertion")
		} else if !isNonTrivial && !rResult.Equal(storagepb.ReplicatedEvalResult{}) {
			panic("non-trival ReplicatedEvalResult for multi-command batch")
		}

		// NB: Perform state assertion before acknowledging the client.
		// Some tests (TestRangeStatsInit) assumes that once the store has started
		// and the first range has a lease that there will not be a later hard-state.
		if isNonTrivial {
			// Assert that the on-disk state doesn't diverge from the in-memory
			// state as a result of the side effects.
			r.mu.Lock()
			r.assertStateLocked(ctx, r.store.Engine())
			r.mu.Unlock()
		}

		if s.lResult != nil {
			r.handleLocalEvalResult(s.ctx, *s.lResult)
		}
		confChanged := r.finishRaftCommand(s.ctx, s)
		stats.processed++
		switch s.e.Type {
		case raftpb.EntryConfChange:
			if !confChanged {
				s.cc = raftpb.ConfChange{}
			}
		case raftpb.EntryNormal:
			if confChanged {
				log.Fatalf(s.ctx, "unexpected replication change from command %s", &s.raftCmd)
			}
		}
		if err := r.withRaftGroup(true, func(raftGroup *raft.RawNode) (bool, error) {
			raftGroup.ApplyConfChange(s.cc)
			return true, nil
		}); err != nil {
			const expl = "during ApplyConfChange"
			return expl, errors.Wrap(err, expl)
		}
	}

	b.appStates.truncate()
	b.updatedTruncatedState = false
	return "", nil
}

func (r *Replica) handleCommittedEntriesRaftMuLocked(
	ctx context.Context,
	committedEntries []raftpb.Entry,
	stats *handleRaftReadyStats,
	refreshReason *refreshRaftReason,
) (expl string, err error) {

	// NB: We want to batch application of commands which have a "trivial"
	// impact on replicated range state.
	//
	// To deal with this we create a batch up front. For each command we check its
	// type. If it's a conf change we know it'll be handled on its own.
	// If it's a regular entry with an interesting ReplicatedEvalResult then we
	// also want to handle it on its own.
	//
	// For things which we do not want to handle on their own we call
	// processRaftCommand with the current batch and the current view of in-memory
	// state.
	b := getEntryApplicationBatch()
	defer putEntryApplicationBatch(b)
	maybeInitializeBatch := func() {
		if b.batch != nil {
			return
		}
		// TODO(ajwerner): eliminate this allocation.
		r.mu.RLock()
		b.replicaState = r.mu.state
		b.stats = *r.mu.state.Stats
		r.mu.RUnlock()
		b.replicaState.Stats = &b.stats
		b.batch = r.store.engine.NewBatch()
	}
	// Decode commands into the batch until we find a non-trivial one.
	// When we find a non-trivial one, unpush it, set committed entries to be that one next.
	// Go process the batch.
	decodeNextBatch := func(
		ctx context.Context, toProcess []raftpb.Entry,
	) (remaining []raftpb.Entry, expl string, err error) {
		maybeInitializeBatch()
		for i := range toProcess {
			// Allocate a *entryApplicationState for this entry.
			// If it turns out that this entry is non-trivial and we already have
			// entries in the current batch then we'll unpush it, apply the batch and
			// then push it back.
			s := b.appStates.pushBack()
			e := &toProcess[i]
			// If we already decoded this message and didn't process it because it
			// was non-trivial, then break out of this loop and go process it.
			if alreadyDecoded := s.e == e; alreadyDecoded {
				if i != 0 {
					panic("this doesn't make sense if i != 0")
				}
				return toProcess[1:], "", nil
			}
			s.initialize(ctx, e)
			// etcd raft occasionally adds a nil entry (our own commands are never
			// empty). This happens in two situations:
			// When a new leader is elected, and when a config change is dropped due
			// to the "one at a time" rule. In both cases we may need to resubmit our
			// pending proposals (In the former case we resubmit everything because
			// we proposed them to a former leader that is no longer able to commit
			// them. In the latter case we only need to resubmit pending config
			// changes, but it's hard to distinguish so we resubmit everything
			// anyway). We delay resubmission until after we have processed the
			// entire batch of entries.
			if len(s.e.Data) == 0 {
				// Overwrite unconditionally since this is the most aggressive
				// reproposal mode.
				if !r.store.TestingKnobs().DisableRefreshReasonNewLeaderOrConfigChange {
					*refreshReason = reasonNewLeaderOrConfigChange
				}
				s.idKey = "" // special-cased value, command isn't used
			} else if expl, err := s.decode(); err != nil {
				return nil, expl, err
			}
			// This is a non-trivial entry which needs to be processed in its own
			// batch.
			if !isTrivial(b.replicaState.UsingAppliedStateKey, &s.raftCmd.ReplicatedEvalResult) {
				// If there are no other entries already in the batch then we pop this
				// one off the front and return. Otherwise, we have to unpush it from
				// the batch so that it'll be processed on its own in the next batch.
				if b.appStates.len == 1 {
					return toProcess[1:], "", nil
				}
				b.appStates.unpush()
				return toProcess[i:], "", nil
			}
		}
		return nil, "", nil
	}
	var it entryApplicationStateBufIterator
	for len(committedEntries) > 0 {
		if committedEntries, expl, err = decodeNextBatch(ctx, committedEntries); err != nil {
			return expl, err
		}
		ok := it.init(&b.appStates)
		r.mu.Lock()
		for ; ok; ok = it.next() {
			s := it.state()
			s.proposal, s.proposedLocally = r.mu.proposals[s.idKey]
			if s.proposedLocally {
				// We initiated this command, so use the caller-supplied context.
				ctx = s.proposal.ctx
				s.ctx = ctx
				delete(r.mu.proposals, s.idKey)
				// At this point we're not guaranteed to have proposalQuota initialized,
				// the same is true for quotaReleaseQueues. Only queue the proposal's
				// quota for release if the proposalQuota is initialized.
				if r.mu.proposalQuota != nil {
					r.mu.quotaReleaseQueue = append(r.mu.quotaReleaseQueue, s.proposal.quotaSize)
				}
			}
		}
		r.mu.Unlock()
		// We want to process the command which gives us some state.
		// We then want to apply the "trivial" portion of the update to
		// the current view of the state.
		// We also need to accumulate the side-effects such as
		for ok = it.init(&b.appStates); ok; ok = it.next() {
			s := it.state()
			r.stageRaftCommand(ctx, s, b.batch, &b.replicaState, it.idx+1 == it.buf.len)
			didUpdate := applyTrivialReplicatedEvalResult(ctx,
				&s.raftCmd.ReplicatedEvalResult, s.e.Index, s.leaseIndex, &b.replicaState)
			b.updatedTruncatedState = b.updatedTruncatedState || didUpdate
		}
		if expl, err := r.applyBatch(ctx, b, stats); err != nil {
			return expl, err
		}
	}
	// if b.batch != nil {
	// 	return r.applyBatch(ctx, b, stats)
	// }
	return "", nil
}

// stageRaftCommand handles the first phase of applying a command to the replica
// state machine. It  the complexities involved in moving the Raft
// state of a Replica forward. At a high level, it receives a proposal, which
// contains the evaluation of a batch (at its heart a WriteBatch, to be applied
// to the underlying storage engine), which it applies and for which it signals
// the client waiting for it (if it's waiting on this Replica).
//
// The proposal also contains auxiliary data which needs to be verified in order
// to decide whether the proposal should be applied: the command's MaxLeaseIndex
// must move the state machine's LeaseAppliedIndex forward, and the proposer's
// lease (or rather its sequence number) must match that of the state machine.
// Furthermore, the GCThreshold is validated and it is checked whether the
// request's key span is contained in the Replica's (it is unclear whether all
// of these checks are necessary). If any of the checks fail, the proposal's
// content is wiped and we apply an empty log entry instead, returning an error
// to the caller to handle. The two typical cases are the lease mismatch (in
// which case the caller tries to send the command to the actual leaseholder)
// and violations of the LeaseAppliedIndex (in which the caller tries again).
//
// Assuming all checks were passed, the command should be applied to the engine,
// which is done by the aptly named applyRaftCommand.
//
// For simple proposals this is the whole story, but some commands trigger
// additional code in this method. The standard way in which this is triggered
// is via a side effect communicated in the proposal's ReplicatedEvalResult and,
// for local proposals, the LocalEvalResult. These might, for example, trigger
// an update of the Replica's in-memory state to match updates to the on-disk
// state, or pass intents to the intent resolver. Some commands don't fit this
// simple schema and need to hook deeper into the code. Notably splits and merges
// need to acquire locks on their right-hand side Replicas and may need to add
// data to the WriteBatch before it is applied; similarly, changes to the disk
// layout of internal state typically require a migration which shows up here.
func (r *Replica) stageRaftCommand(
	ctx context.Context,
	s *entryApplicationState,
	batch engine.Batch,
	replicaState *storagepb.ReplicaState,
	writeAppliedState bool,
) {
	if s.e.Index == 0 {
		log.Fatalf(ctx, "processRaftCommand requires a non-zero index")
	}
	if log.V(4) {
		log.Infof(ctx, "processing command %x: maxLeaseIndex=%d", s.idKey, s.raftCmd.MaxLeaseIndex)
	}

	var ts hlc.Timestamp
	if s.idKey != "" {
		ts = s.raftCmd.ReplicatedEvalResult.Timestamp
	}

	s.leaseIndex, s.proposalRetry, s.forcedErr = checkForcedErr(ctx,
		s.idKey, s.raftCmd, s.proposal, s.proposedLocally, replicaState)
	if s.forcedErr == nil {
		// Verify that the batch timestamp is after the GC threshold. This is
		// necessary because not all commands declare read access on the GC
		// threshold key, even though they implicitly depend on it. This means
		// that access to this state will not be serialized by latching,
		// so we must perform this check upstream and downstream of raft.
		// See #14833.
		//
		// We provide an empty key span because we already know that the Raft
		// command is allowed to apply within its key range. This is guaranteed
		// by checks upstream of Raft, which perform the same validation, and by
		// span latches, which assure that any modifications to the range's
		// boundaries will be serialized with this command. Finally, the
		// leaseAppliedIndex check in checkForcedErrLocked ensures that replays
		// outside of the spanlatch manager's control which break this
		// serialization ordering will already by caught and an error will be
		// thrown.
		s.forcedErr = roachpb.NewError(r.requestCanProceed(roachpb.RSpan{}, ts))
	}

	// applyRaftCommand will return "expected" errors, but may also indicate
	// replica corruption (as of now, signaled by a replicaCorruptionError).
	// We feed its return through maybeSetCorrupt to act when that happens.
	if s.forcedErr != nil {
		log.VEventf(ctx, 1, "applying command with forced error: %s", s.forcedErr)
	} else {
		log.Event(ctx, "applying command")

		if splitMergeUnlock, err := r.maybeAcquireSplitMergeLock(ctx, s.raftCmd); err != nil {
			log.Eventf(ctx, "unable to acquire split lock: %s", err)
			// Send a crash report because a former bug in the error handling might have
			// been the root cause of #19172.
			_ = r.store.stopper.RunAsyncTask(ctx, "crash report", func(ctx context.Context) {
				log.SendCrashReport(
					ctx,
					&r.store.cfg.Settings.SV,
					0, // depth
					"while acquiring split lock: %s",
					[]interface{}{err},
					log.ReportTypeError,
				)
			})

			s.forcedErr = roachpb.NewError(err)
		} else if splitMergeUnlock != nil {
			// Close over raftCmd to capture its value at execution time; we clear
			// ReplicatedEvalResult on certain errors.
			s.splitMergeUnlock = splitMergeUnlock
		}
	}

	var writeBatch *storagepb.WriteBatch
	{
		if filter := r.store.cfg.TestingKnobs.TestingApplyFilter; s.forcedErr == nil && filter != nil {
			var newPropRetry int
			newPropRetry, s.forcedErr = filter(storagebase.ApplyFilterArgs{
				CmdID:                s.idKey,
				ReplicatedEvalResult: s.raftCmd.ReplicatedEvalResult,
				StoreID:              r.store.StoreID(),
				RangeID:              r.RangeID,
			})
			if s.proposalRetry == 0 {
				s.proposalRetry = proposalReevaluationReason(newPropRetry)
			}
		}

		if s.forcedErr != nil {
			// Apply an empty entry.
			s.raftCmd.ReplicatedEvalResult = storagepb.ReplicatedEvalResult{}
			s.raftCmd.WriteBatch = nil
			s.raftCmd.LogicalOpLog = nil
		}

		// Update the node clock with the serviced request. This maintains
		// a high water mark for all ops serviced, so that received ops without
		// a timestamp specified are guaranteed one higher than any op already
		// executed for overlapping keys.
		r.store.Clock().Update(ts)

		if s.raftCmd.WriteBatch != nil {
			writeBatch = s.raftCmd.WriteBatch
		}

		if deprecatedDelta := s.raftCmd.ReplicatedEvalResult.DeprecatedDelta; deprecatedDelta != nil {
			s.raftCmd.ReplicatedEvalResult.Delta = deprecatedDelta.ToStatsDelta()
			s.raftCmd.ReplicatedEvalResult.DeprecatedDelta = nil
		}

		// AddSSTable ingestions run before the actual batch. This makes sure
		// that when the Raft command is applied, the ingestion has definitely
		// succeeded. Note that we have taken precautions during command
		// evaluation to avoid having mutations in the WriteBatch that affect
		// the SSTable. Not doing so could result in order reversal (and missing
		// values) here. If the key range we are ingesting into isn't empty,
		// we're not using AddSSTable but a plain WriteBatch.
		if s.raftCmd.ReplicatedEvalResult.AddSSTable != nil {
			copied := addSSTablePreApply(
				ctx,
				r.store.cfg.Settings,
				r.store.engine,
				r.raftMu.sideloaded,
				s.e.Term,
				s.e.Index,
				*s.raftCmd.ReplicatedEvalResult.AddSSTable,
				r.store.limiters.BulkIOWriteRate,
			)
			r.store.metrics.AddSSTableApplications.Inc(1)
			if copied {
				r.store.metrics.AddSSTableApplicationCopies.Inc(1)
			}
			s.raftCmd.ReplicatedEvalResult.AddSSTable = nil
		}

		if s.raftCmd.ReplicatedEvalResult.Split != nil {
			// Splits require a new HardState to be written to the new RHS
			// range (and this needs to be atomic with the main batch). This
			// cannot be constructed at evaluation time because it differs
			// on each replica (votes may have already been cast on the
			// uninitialized replica). Transform the write batch to add the
			// updated HardState.
			// See https://github.com/cockroachdb/cockroach/issues/20629
			//
			// This is not the most efficient, but it only happens on splits,
			// which are relatively infrequent and don't write much data.
			tmpBatch := r.store.engine.NewBatch()
			if err := tmpBatch.ApplyBatchRepr(writeBatch.Data, false); err != nil {
				log.Fatal(ctx, err)
			}
			splitPreApply(ctx, tmpBatch, s.raftCmd.ReplicatedEvalResult.Split.SplitTrigger)
			writeBatch.Data = tmpBatch.Repr()
			tmpBatch.Close()
		}

		if merge := s.raftCmd.ReplicatedEvalResult.Merge; merge != nil {
			// Merges require the subsumed range to be atomically deleted when the
			// merge transaction commits.
			//
			// This is not the most efficient, but it only happens on merges,
			// which are relatively infrequent and don't write much data.
			tmpBatch := r.store.engine.NewBatch()
			if err := tmpBatch.ApplyBatchRepr(writeBatch.Data, false); err != nil {
				log.Fatal(ctx, err)
			}
			rhsRepl, err := r.store.GetReplica(merge.RightDesc.RangeID)
			if err != nil {
				log.Fatal(ctx, err)
			}
			const destroyData = false
			err = rhsRepl.preDestroyRaftMuLocked(ctx, tmpBatch, tmpBatch, merge.RightDesc.NextReplicaID, destroyData)
			if err != nil {
				log.Fatal(ctx, err)
			}
			writeBatch.Data = tmpBatch.Repr()
			tmpBatch.Close()
		}

		{
			var err error
			s.raftCmd.ReplicatedEvalResult, err = r.applyRaftCommand(
				s.ctx, s.idKey, s.raftCmd.ReplicatedEvalResult, s.e.Index, s.leaseIndex, writeBatch, replicaState, batch, writeAppliedState)

			// applyRaftCommand returned an error, which usually indicates
			// either a serious logic bug in CockroachDB or a disk
			// corruption/out-of-space issue. Make sure that these fail with
			// descriptive message so that we can differentiate the root causes.
			if err != nil {
				log.Errorf(ctx, "unable to update the state machine: %s", err)
				// Report the fatal error separately and only with the error, as that
				// triggers an optimization for which we directly report the error to
				// sentry (which in turn allows sentry to distinguish different error
				// types).
				log.Fatal(ctx, err)
			}
		}
	}
}

// prepareLocalResult provides is performed after the command has been committed
// to the engine but before its side-effects have been applied to the Replica's
// in-memory state. This method gives the command an opportunity to interact
// with testing knobs and to set up its local result if it was proposed locally.
// This is performed prior to handling the command's ReplicatedEvalResult
// because the process of handling the replicated eval result will zero-out the
// struct to ensure that is has properly performed all of the implied
// side-effects.
func (r *Replica) prepareLocalResult(ctx context.Context, s *entryApplicationState) {
	var pErr *roachpb.Error
	if filter := r.store.cfg.TestingKnobs.TestingPostApplyFilter; filter != nil {
		var newPropRetry int
		newPropRetry, pErr = filter(storagebase.ApplyFilterArgs{
			CmdID:                s.idKey,
			ReplicatedEvalResult: s.raftCmd.ReplicatedEvalResult,
			StoreID:              r.store.StoreID(),
			RangeID:              r.RangeID,
		})
		if s.proposalRetry == 0 {
			s.proposalRetry = proposalReevaluationReason(newPropRetry)
		}
		// calling maybeSetCorrupt here is mostly for tests and looks. The
		// interesting errors originate in applyRaftCommand, and they are
		// already handled above.
		pErr = r.maybeSetCorrupt(ctx, pErr)
	}
	if pErr == nil {
		pErr = s.forcedErr
	}

	if s.proposedLocally {
		if s.proposalRetry != proposalNoReevaluation && pErr == nil {
			log.Fatalf(ctx, "proposal with nontrivial retry behavior, but no error: %+v", s.proposal)
		}
		if pErr != nil {
			// A forced error was set (i.e. we did not apply the proposal,
			// for instance due to its log position) or the Replica is now
			// corrupted.
			// If proposalRetry is set, we don't also return an error, as per the
			// proposalResult contract.
			if s.proposalRetry == proposalNoReevaluation {
				s.response.Err = pErr
			}
		} else if s.proposal.Local.Reply != nil {
			s.response.Reply = s.proposal.Local.Reply
		} else {
			log.Fatalf(ctx, "proposal must return either a reply or an error: %+v", s.proposal)
		}
		s.response.Intents = s.proposal.Local.DetachIntents()
		s.response.EndTxns = s.proposal.Local.DetachEndTxns(pErr != nil)
		if pErr == nil {
			s.lResult = s.proposal.Local
		}
	}
	if pErr != nil && s.lResult != nil {
		log.Fatalf(ctx, "shouldn't have a local result if command processing failed. pErr: %s", pErr)
	}
	if log.ExpensiveLogEnabled(ctx, 2) {
		log.VEvent(ctx, 2, s.lResult.String())
	}
}

func (r *Replica) finishRaftCommand(
	ctx context.Context, s *entryApplicationState,
) (changedReplicas bool) {

	// Provide the command's corresponding logical operations to the
	// Replica's rangefeed. Only do so if the WriteBatch is nonnil,
	// otherwise it's valid for the logical op log to be nil, which
	// would shut down all rangefeeds. If no rangefeed is running,
	// this call will be a noop.
	if s.raftCmd.WriteBatch != nil {
		r.handleLogicalOpLogRaftMuLocked(ctx, s.raftCmd.LogicalOpLog)
	} else if s.raftCmd.LogicalOpLog != nil {
		log.Fatalf(ctx, "nonnil logical op log with nil write batch: %v", s.raftCmd)
	}

	// When set to true, recomputes the stats for the LHS and RHS of splits and
	// makes sure that they agree with the state's range stats.
	const expensiveSplitAssertion = false

	if expensiveSplitAssertion && s.raftCmd.ReplicatedEvalResult.Split != nil {
		split := s.raftCmd.ReplicatedEvalResult.Split
		lhsStatsMS := r.GetMVCCStats()
		lhsComputedMS, err := rditer.ComputeStatsForRange(&split.LeftDesc, r.store.Engine(), lhsStatsMS.LastUpdateNanos)
		if err != nil {
			log.Fatal(ctx, err)
		}

		rightReplica, err := r.store.GetReplica(split.RightDesc.RangeID)
		if err != nil {
			log.Fatal(ctx, err)
		}

		rhsStatsMS := rightReplica.GetMVCCStats()
		rhsComputedMS, err := rditer.ComputeStatsForRange(&split.RightDesc, r.store.Engine(), rhsStatsMS.LastUpdateNanos)
		if err != nil {
			log.Fatal(ctx, err)
		}

		if diff := pretty.Diff(lhsStatsMS, lhsComputedMS); len(diff) > 0 {
			log.Fatalf(ctx, "LHS split stats divergence: diff(claimed, computed) = %s", pretty.Diff(lhsStatsMS, lhsComputedMS))
		}
		if diff := pretty.Diff(rhsStatsMS, rhsComputedMS); len(diff) > 0 {
			log.Fatalf(ctx, "RHS split stats divergence diff(claimed, computed) = %s", pretty.Diff(rhsStatsMS, rhsComputedMS))
		}
	}

	if s.proposedLocally {
		// If we failed to apply at the right lease index, try again with
		// a new one. This is important for pipelined writes, since they
		// don't have a client watching to retry, so a failure to
		// eventually apply the proposal would be a uservisible error.
		// TODO(nvanbenschoten): This reproposal is not tracked by the
		// quota pool. We should fix that.
		if s.proposalRetry == proposalIllegalLeaseIndex && r.tryReproposeWithNewLeaseIndex(s.proposal) {
			return false
		}
		// Otherwise, signal the command's status to the client.
		s.proposal.finishApplication(s.response)
	} else if s.response.Err != nil {
		log.VEventf(ctx, 1, "applying raft command resulted in error: %s", s.response.Err)
	}

	return s.raftCmd.ReplicatedEvalResult.ChangeReplicas != nil
}

// applyRaftCommand applies a raft command from the replicated log to the
// current batch's view of the underlying state machine. When the state machine
// cannot be updated, an error (which is likely fatal!) is returned and must be
// handled by the caller.
// The returned ReplicatedEvalResult replaces the caller's.
func (r *Replica) applyRaftCommand(
	ctx context.Context,
	idKey storagebase.CmdIDKey,
	rResult storagepb.ReplicatedEvalResult,
	raftAppliedIndex, leaseAppliedIndex uint64,
	writeBatch *storagepb.WriteBatch,
	replicaState *storagepb.ReplicaState,
	batch engine.Batch,
	writeAppliedState bool,
) (storagepb.ReplicatedEvalResult, error) {
	if writeBatch != nil && len(writeBatch.Data) > 0 {
		// Record the write activity, passing a 0 nodeID because replica.writeStats
		// intentionally doesn't track the origin of the writes.
		mutationCount, err := engine.RocksDBBatchCount(writeBatch.Data)
		if err != nil {
			log.Errorf(ctx, "unable to read header of committed WriteBatch: %s", err)
		} else {
			r.writeStats.recordCount(float64(mutationCount), 0 /* nodeID */)
		}
	}

	// Exploit the fact that a split will result in a full stats
	// recomputation to reset the ContainsEstimates flag.
	//
	// TODO(tschottdorf): We want to let the usual MVCCStats-delta
	// machinery update our stats for the left-hand side. But there is no
	// way to pass up an MVCCStats object that will clear out the
	// ContainsEstimates flag. We should introduce one, but the migration
	// makes this worth a separate effort (ContainsEstimates would need to
	// have three possible values, 'UNCHANGED', 'NO', and 'YES').
	// Until then, we're left with this rather crude hack.
	if rResult.Split != nil {
		replicaState.Stats.ContainsEstimates = false
	}
	ms := replicaState.Stats

	// TODO(ajwerner): revisit this, We should have some sort of sanity check.
	if raftAppliedIndex != replicaState.RaftAppliedIndex+1 {
		// If we have an out of order index, there's corruption. No sense in
		// trying to update anything or running the command. Simply return
		// a corruption error.
		return storagepb.ReplicatedEvalResult{}, errors.Errorf("applied index jumped from %d to %d",
			replicaState.RaftAppliedIndex, raftAppliedIndex)
	}

	haveTruncatedState := rResult.State != nil && rResult.State.TruncatedState != nil

	if writeBatch != nil {
		if err := batch.ApplyBatchRepr(writeBatch.Data, false); err != nil {
			return storagepb.ReplicatedEvalResult{}, errors.Wrap(err, "unable to apply WriteBatch")
		}
	}

	// The only remaining use of the batch is for range-local keys which we know
	// have not been previously written within this batch.
	writer := batch.Distinct()

	// Special-cased MVCC stats handling to exploit commutativity of stats delta
	// upgrades. Thanks to commutativity, the spanlatch manager does not have to
	// serialize on the stats key.
	deltaStats := rResult.Delta.ToStats()
	usingAppliedStateKey := replicaState.UsingAppliedStateKey
	if !usingAppliedStateKey && rResult.State != nil && rResult.State.UsingAppliedStateKey {
		// The Raft command wants us to begin using the RangeAppliedState key
		// and we haven't performed the migration yet. Delete the old keys
		// that this new key is replacing.
		err := r.raftMu.stateLoader.MigrateToRangeAppliedStateKey(ctx, writer, &deltaStats)
		if err != nil {
			return storagepb.ReplicatedEvalResult{}, errors.Wrap(err, "unable to migrate to range applied state")
		}
		usingAppliedStateKey = true
	}
	if writeAppliedState {
		if usingAppliedStateKey {
			// Note that calling ms.Add will never result in ms.LastUpdateNanos
			// decreasing (and thus LastUpdateNanos tracks the maximum LastUpdateNanos
			// across all deltaStats).
			ms.Add(deltaStats)

			// Set the range applied state, which includes the last applied raft and
			// lease index along with the mvcc stats, all in one key.
			if err := r.raftMu.stateLoader.SetRangeAppliedState(ctx, writer,
				raftAppliedIndex, leaseAppliedIndex, ms); err != nil {
				return storagepb.ReplicatedEvalResult{}, errors.Wrap(err, "unable to set range applied state")
			}
			ms.Subtract(deltaStats)
		} else {
			// Advance the last applied index. We use a blind write in order to avoid
			// reading the previous applied index keys on every write operation. This
			// requires a little additional work in order maintain the MVCC stats.
			var appliedIndexNewMS enginepb.MVCCStats
			if err := r.raftMu.stateLoader.SetLegacyAppliedIndexBlind(ctx, writer, &appliedIndexNewMS,
				raftAppliedIndex, leaseAppliedIndex); err != nil {
				return storagepb.ReplicatedEvalResult{}, errors.Wrap(err, "unable to set applied index")
			}
			deltaStats.SysBytes += appliedIndexNewMS.SysBytes -
				r.raftMu.stateLoader.CalcAppliedIndexSysBytes(replicaState.RaftAppliedIndex, replicaState.LeaseAppliedIndex)

			// Note that calling ms.Add will never result in ms.LastUpdateNanos
			// decreasing (and thus LastUpdateNanos tracks the maximum LastUpdateNanos
			// across all deltaStats).
			ms.Add(deltaStats)
			if err := r.raftMu.stateLoader.SetMVCCStats(ctx, writer, ms); err != nil {
				return storagepb.ReplicatedEvalResult{}, errors.Wrap(err, "unable to update MVCCStats")
			}
			ms.Subtract(deltaStats)
		}
	}

	if haveTruncatedState {
		apply, err := handleTruncatedStateBelowRaft(ctx, replicaState.TruncatedState, rResult.State.TruncatedState, r.raftMu.stateLoader, writer)
		if err != nil {
			return storagepb.ReplicatedEvalResult{}, err
		}
		if !apply {
			// The truncated state was discarded, so make sure we don't apply
			// it to our in-memory state.
			rResult.State.TruncatedState = nil
			rResult.RaftLogDelta = 0
			// TODO(ajwerner): move this code
			// We received a truncation that doesn't apply to us, so we know that
			// there's a leaseholder out there with a log that has earlier entries
			// than ours. That leader also guided our log size computations by
			// giving us RaftLogDeltas for past truncations, and this was likely
			// off. Mark our Raft log size is not trustworthy so that, assuming
			// we step up as leader at some point in the future, we recompute
			// our numbers.
			r.mu.Lock()
			r.mu.raftLogSizeTrusted = false
			r.mu.Unlock()
		}
	}

	// TODO(peter): We did not close the writer in an earlier version of
	// the code, which went undetected even though we used the batch after
	// (though only to commit it). We should add an assertion to prevent that in
	// the future.
	writer.Close()

	start := timeutil.Now()

	var assertHS *raftpb.HardState
	if util.RaceEnabled && rResult.Split != nil {
		rsl := stateloader.Make(rResult.Split.RightDesc.RangeID)
		oldHS, err := rsl.LoadHardState(ctx, batch)
		if err != nil {
			return storagepb.ReplicatedEvalResult{}, errors.Wrap(err, "unable to load HardState")
		}
		assertHS = &oldHS
	}

	if assertHS != nil {
		// Load the HardState that was just committed (if any).
		rsl := stateloader.Make(rResult.Split.RightDesc.RangeID)
		newHS, err := rsl.LoadHardState(ctx, batch)
		if err != nil {
			return storagepb.ReplicatedEvalResult{}, errors.Wrap(err, "unable to load HardState")
		}
		// Assert that nothing moved "backwards".
		if newHS.Term < assertHS.Term || (newHS.Term == assertHS.Term && newHS.Commit < assertHS.Commit) {
			log.Fatalf(ctx, "clobbered HardState: %s\n\npreviously: %s\noverwritten with: %s",
				pretty.Diff(newHS, *assertHS), pretty.Sprint(*assertHS), pretty.Sprint(newHS))
		}
	}

	elapsed := timeutil.Since(start)
	r.store.metrics.RaftCommandCommitLatency.RecordValue(elapsed.Nanoseconds())
	rResult.Delta = deltaStats.ToStatsDelta()
	return rResult, nil
}
