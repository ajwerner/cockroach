// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// replica_application_*.go files provide concrete implementations of
// the interfaces defined in the storage/apply package:
//
// replica_application_state_machine.go  ->  apply.StateMachine
// replica_application_decoder.go        ->  apply.Decoder
// replica_application_cmd.go            ->  apply.Command         (and variants)
// replica_application_cmd_buf.go        ->  apply.CommandIterator (and variants)
// replica_application_cmd_buf.go        ->  apply.CommandList     (and variants)
//
// These allow Replica to interface with the storage/apply package.

// isTrivial determines whether the side-effects of a ReplicatedEvalResult are
// "trivial". A result is fundamentally considered "trivial" if it does not have
// side effects which rely on the written state of the replica exactly matching
// the in-memory state of the replica at the corresponding log position.
// Non-trivial commands must be applied in their own batch so that after
// the batch is applied the replica's written and in-memory state correspond
// to that log index.
//
// At the time of writing it is possible that the current conditions are too
// strict but they are certainly sufficient.
func isTrivial(r *storagepb.ReplicatedEvalResult) bool {
	// Check if there are any non-trivial State updates.
	if r.State != nil {
		stateWhitelist := *r.State
		// ReplicaState.Stats was previously non-nullable which caused nodes to
		// send a zero-value MVCCStats structure. If the proposal was generated by
		// an old node, we'll have decoded that zero-value structure setting
		// ReplicaState.Stats to a non-nil value which would trigger the "unhandled
		// field in ReplicatedEvalResult" assertion to fire if we didn't clear it.
		// TODO(ajwerner): eliminate this case that likely can no longer occur as of
		// at least 19.1.
		if stateWhitelist.Stats != nil && (*stateWhitelist.Stats == enginepb.MVCCStats{}) {
			stateWhitelist.Stats = nil
		}
		if stateWhitelist.DeprecatedTxnSpanGCThreshold != nil {
			stateWhitelist.DeprecatedTxnSpanGCThreshold = nil
		}
		if stateWhitelist != (storagepb.ReplicaState{}) {
			return false
		}
	}
	// Set whitelist to the value of r and clear the whitelisted fields.
	// If whitelist is zero-valued after clearing the whitelisted fields then
	// it is trivial.
	whitelist := *r
	whitelist.Delta = enginepb.MVCCStatsDelta{}
	whitelist.Timestamp = hlc.Timestamp{}
	whitelist.DeprecatedDelta = nil
	whitelist.PrevLeaseProposal = nil
	whitelist.State = nil
	return whitelist.Equal(storagepb.ReplicatedEvalResult{})
}

// clearTrivialReplicatedEvalResultFields is used to zero out the fields of a
// ReplicatedEvalResult that have already been consumed when staging the
// corresponding command and applying it to the current batch's view of the
// ReplicaState. This function is called after a batch has been written to the
// storage engine. For trivial commands this function should result in a zero
// value replicatedResult.
func clearTrivialReplicatedEvalResultFields(r *storagepb.ReplicatedEvalResult) {
	// Fields for which no action is taken in this method are zeroed so that
	// they don't trigger an assertion at the end of the application process
	// (which checks that all fields were handled).
	r.IsLeaseRequest = false
	r.Timestamp = hlc.Timestamp{}
	r.PrevLeaseProposal = nil
	// The state fields cleared here were already applied to the in-memory view of
	// replica state for this batch.
	if haveState := r.State != nil; haveState {
		r.State.Stats = nil

		// Strip the DeprecatedTxnSpanGCThreshold. We don't care about it.
		// TODO(nvanbenschoten): Remove in 20.1.
		r.State.DeprecatedTxnSpanGCThreshold = nil
		if *r.State == (storagepb.ReplicaState{}) {
			r.State = nil
		}
	}
	r.Delta = enginepb.MVCCStatsDelta{}
}

// prepareLocalResult is performed after the command has been committed to the
// engine but before its side-effects have been applied to the Replica's
// in-memory state. This method gives the command an opportunity to interact
// with testing knobs and to set up its local result if it was proposed
// locally. This is performed prior to handling the command's
// ReplicatedEvalResult because the process of handling the replicated eval
// result will zero-out the struct to ensure that is has properly performed all
// of the implied side-effects.
func (r *Replica) prepareLocalResult(ctx context.Context, cmd *replicatedCmd) {
	if !cmd.IsLocal() {
		return
	}

	var pErr *roachpb.Error
	if filter := r.store.cfg.TestingKnobs.TestingPostApplyFilter; filter != nil {
		var newPropRetry int
		newPropRetry, pErr = filter(storagebase.ApplyFilterArgs{
			CmdID:                cmd.idKey,
			ReplicatedEvalResult: *cmd.replicatedResult(),
			StoreID:              r.store.StoreID(),
			RangeID:              r.RangeID,
		})
		if cmd.proposalRetry == 0 {
			cmd.proposalRetry = proposalReevaluationReason(newPropRetry)
		}
		// calling maybeSetCorrupt here is mostly for tests and looks. The
		// interesting errors originate in applyRaftCommandToBatch, and they are
		// already handled above.
		pErr = r.maybeSetCorrupt(ctx, pErr)
	}
	if pErr == nil {
		pErr = cmd.forcedErr
	}

	if cmd.proposalRetry != proposalNoReevaluation && pErr == nil {
		log.Fatalf(ctx, "proposal with nontrivial retry behavior, but no error: %+v", cmd.proposal)
	}
	if pErr != nil {
		// A forced error was set (i.e. we did not apply the proposal,
		// for instance due to its log position) or the Replica is now
		// corrupted.
		switch cmd.proposalRetry {
		case proposalNoReevaluation:
			cmd.response.Err = pErr
		case proposalIllegalLeaseIndex:
			// If we failed to apply at the right lease index, try again with a
			// new one. This is important for pipelined writes, since they don't
			// have a client watching to retry, so a failure to eventually apply
			// the proposal would be a user-visible error.
			pErr = r.tryReproposeWithNewLeaseIndex(ctx, cmd)
			if pErr != nil {
				cmd.response.Err = pErr
			} else {
				// Unbind the entry's local proposal because we just succeeded
				// in reproposing it and we don't want to acknowledge the client
				// yet.
				cmd.proposal = nil
				return
			}
		default:
			panic("unexpected")
		}
	} else if cmd.proposal.Local.Reply != nil {
		cmd.response.Reply = cmd.proposal.Local.Reply
	} else {
		log.Fatalf(ctx, "proposal must return either a reply or an error: %+v", cmd.proposal)
	}
	cmd.response.Intents = cmd.proposal.Local.DetachIntents()
	cmd.response.EndTxns = cmd.proposal.Local.DetachEndTxns(pErr != nil)
	if pErr == nil {
		cmd.localResult = cmd.proposal.Local
	} else if cmd.localResult != nil {
		log.Fatalf(ctx, "shouldn't have a local result if command processing failed. pErr: %s", pErr)
	}
}

// tryReproposeWithNewLeaseIndex is used by prepareLocalResult to repropose
// commands that have gotten an illegal lease index error, and that we know
// could not have applied while their lease index was valid (that is, we
// observed all applied entries between proposal and the lease index becoming
// invalid, as opposed to skipping some of them by applying a snapshot).
//
// It is not intended for use elsewhere and is only a top-level function so that
// it can avoid the below_raft_protos check. Returns a nil error if the command
// has already been successfully applied or has been reproposed here or by a
// different entry for the same proposal that hit an illegal lease index error.
func (r *Replica) tryReproposeWithNewLeaseIndex(
	ctx context.Context, cmd *replicatedCmd,
) *roachpb.Error {
	// Note that we don't need to validate anything about the proposal's
	// lease here - if we got this far, we know that everything but the
	// index is valid at this point in the log.
	p := cmd.proposal
	if p.applied || cmd.raftCmd.MaxLeaseIndex != p.command.MaxLeaseIndex {
		// If the command associated with this rejected raft entry already
		// applied then we don't want to repropose it. Doing so could lead
		// to duplicate application of the same proposal.
		//
		// Similarly, if the command associated with this rejected raft
		// entry has a different (larger) MaxLeaseIndex than the one we
		// decoded from the entry itself, the command must have already
		// been reproposed (this can happen if there are multiple copies
		// of the command in the logs; see TestReplicaRefreshMultiple).
		// We must not create multiple copies with multiple lease indexes,
		// so don't repropose it again. This ensures that at any time,
		// there is only up to a single lease index that has a chance of
		// succeeding in the Raft log for a given command.
		return nil
	}
	// Some tests check for this log message in the trace.
	log.VEventf(ctx, 2, "retry: proposalIllegalLeaseIndex")
	maxLeaseIndex, pErr := r.propose(ctx, p)
	if pErr != nil {
		log.Warningf(ctx, "failed to repropose with new lease index: %s", pErr)
		return pErr
	}
	log.VEventf(ctx, 2, "reproposed command %x at maxLeaseIndex=%d", cmd.idKey, maxLeaseIndex)
	return nil
}

// The following Replica.handleXYZResult methods are called when applying
// non-trivial side effects in replicaStateMachine.ApplySideEffects. As a
// general rule, there is a method for each of the non-trivial fields in
// ReplicatedEvalResult. Most methods are simple enough that they will be
// inlined.

func (r *Replica) handleSplitResult(ctx context.Context, split *storagepb.Split) {
	splitPostApply(ctx, split.RHSDelta, &split.SplitTrigger, r)
}

func (r *Replica) handleMergeResult(ctx context.Context, merge *storagepb.Merge) {
	if err := r.store.MergeRange(
		ctx, r, merge.LeftDesc, merge.RightDesc, merge.FreezeStart,
	); err != nil {
		// Our in-memory state has diverged from the on-disk state.
		log.Fatalf(ctx, "failed to update store after merging range: %s", err)
	}
}

func (r *Replica) handleDescResult(ctx context.Context, desc *roachpb.RangeDescriptor) {
	r.setDesc(ctx, desc)
}

func (r *Replica) handleLeaseResult(ctx context.Context, lease *roachpb.Lease) {
	r.leasePostApply(ctx, *lease, false /* permitJump */)
}

func (r *Replica) handleTruncatedStateResult(
	ctx context.Context, t *roachpb.RaftTruncatedState,
) (raftLogDelta int64) {
	r.mu.Lock()
	r.mu.state.TruncatedState = t
	r.mu.Unlock()

	// Clear any entries in the Raft log entry cache for this range up
	// to and including the most recently truncated index.
	r.store.raftEntryCache.Clear(r.RangeID, t.Index+1)

	// Truncate the sideloaded storage. Note that this is safe only if the new truncated state
	// is durably on disk (i.e.) synced. This is true at the time of writing but unfortunately
	// could rot.
	log.Eventf(ctx, "truncating sideloaded storage up to (and including) index %d", t.Index)
	size, _, err := r.raftMu.sideloaded.TruncateTo(ctx, t.Index+1)
	if err != nil {
		// We don't *have* to remove these entries for correctness. Log a
		// loud error, but keep humming along.
		log.Errorf(ctx, "while removing sideloaded files during log truncation: %+v", err)
	}
	return -size
}

func (r *Replica) handleGCThresholdResult(ctx context.Context, thresh *hlc.Timestamp) {
	if thresh.IsEmpty() {
		return
	}
	r.mu.Lock()
	r.mu.state.GCThreshold = thresh
	r.mu.Unlock()
}

func (r *Replica) handleUsingAppliedStateKeyResult(ctx context.Context) {
	r.mu.Lock()
	r.mu.state.UsingAppliedStateKey = true
	r.mu.Unlock()
}

func (r *Replica) handleComputeChecksumResult(ctx context.Context, cc *storagepb.ComputeChecksum) {
	r.computeChecksumPostApply(ctx, *cc)
}

func (r *Replica) handleChangeReplicasResult(
	ctx context.Context, removed bool, chng *storagepb.ChangeReplicas,
) {
	// If this command removes us then we need to go through the process of
	// removing our replica from the store. After this method returns the code
	// should roughly return all the way up to whoever called handleRaftReady
	// and this Replica should never be heard from again.
	if !removed {
		return
	}
	if log.V(1) {
		log.Infof(ctx, "removing replica due to ChangeReplicasTrigger: %v", chng)
	}
	// TODO(ajwerner): the data should already be destroyed at this point.
	// destroying the data again seems crazy.
	if err := r.store.removeReplicaImpl(ctx, r, chng.Desc.NextReplicaID, RemoveOptions{
		DestroyData: true, // We already destroyed the data when the batch committed.
	}); err != nil {
		log.Fatalf(ctx, "failed to remove replica: %v", err)
	}
}

func (r *Replica) handleRaftLogDeltaResult(ctx context.Context, delta int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.raftLogSize += delta
	r.mu.raftLogLastCheckSize += delta
	// Ensure raftLog{,LastCheck}Size is not negative since it isn't persisted
	// between server restarts.
	if r.mu.raftLogSize < 0 {
		r.mu.raftLogSize = 0
	}
	if r.mu.raftLogLastCheckSize < 0 {
		r.mu.raftLogLastCheckSize = 0
	}
}

func (r *Replica) handleNoRaftLogDeltaResult(ctx context.Context) {
	// Check for whether to queue the range for Raft log truncation if this is
	// not a Raft log truncation command itself. We don't want to check the
	// Raft log for truncation on every write operation or even every operation
	// which occurs after the Raft log exceeds RaftLogQueueStaleSize. The logic
	// below queues the replica for possible Raft log truncation whenever an
	// additional RaftLogQueueStaleSize bytes have been written to the Raft
	// log.
	r.mu.Lock()
	checkRaftLog := r.mu.raftLogSize-r.mu.raftLogLastCheckSize >= RaftLogQueueStaleSize
	if checkRaftLog {
		r.mu.raftLogLastCheckSize = r.mu.raftLogSize
	}
	r.mu.Unlock()
	if checkRaftLog {
		r.store.raftLogQueue.MaybeAddAsync(ctx, r, r.store.Clock().Now())
	}
}

func (r *Replica) handleSuggestedCompactionsResult(
	ctx context.Context, scs []storagepb.SuggestedCompaction,
) {
	for _, sc := range scs {
		r.store.compactor.Suggest(ctx, sc)
	}
}
