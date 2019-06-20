// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/admission"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var disableReadQuota = settings.RegisterBoolSetting(
	"kv.read_quota.disabled",
	"set to true to disable the read quota.",
	false,
)

// TODO(ajwerner): explore other cases where read only requests should not be
// blocked. Internal requests should already receive a high priority.
func requiresReadQuota(r *Replica, ba *roachpb.BatchRequest) bool {
	if ba.Txn != nil && ba.Txn.Key != nil {
		return false
	}
	if disableReadQuota.Get(&r.store.ClusterSettings().SV) {
		return false
	}
	if _, isQueryTxnRequest := ba.GetArg(roachpb.QueryTxn); isQueryTxnRequest {
		return false
	}
	if _, isQueryIntent := ba.GetArg(roachpb.QueryIntent); isQueryIntent {
		return false
	}
	return true
}

// executeReadOnlyBatch updates the read timestamp cache and waits for any
// overlapping writes currently processing through Raft ahead of us to
// clear via the latches.
func (r *Replica) executeReadOnlyBatch(
	ctx context.Context, ba roachpb.BatchRequest,
) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	// If the read is not inconsistent, the read requires the range lease or
	// permission to serve via follower reads.
	var status storagepb.LeaseStatus
	if ba.ReadConsistency.RequiresReadLease() {
		if status, pErr = r.redirectOnOrAcquireLease(ctx); pErr != nil {
			if nErr := r.canServeFollowerRead(ctx, ba, pErr); nErr != nil {
				return nil, nErr
			}
			r.store.metrics.FollowerReadsCount.Inc(1)
		}
	}
	respSize := -1
	if requiresReadQuota(r, &ba) {
		// Let's check on the admission controller level
		priority := admission.PriorityFromContext(ctx)
		if err := r.store.admissionController.Admit(ctx, priority); err != nil {
			if err == admission.ErrRejected {
				return nil, roachpb.NewError(&roachpb.ReadRejectedError{})
			}
			return nil, roachpb.NewError(err)
		}
		alloc, err := r.store.readQuota.acquire(ctx, priority)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		defer func() {
			if respSize > 0 {
				if log.V(1) {
					log.Infof(ctx, "acquired %v, used %v", respSize, alloc.Acquired())
				}
				r.store.metrics.ReadQuotaBytesGuessed.Inc(alloc.Acquired())
				r.store.metrics.ReadQuotaBytesRead.Inc(int64(respSize))
				switch priority.Level {
				case admission.MaxLevel:
					r.store.metrics.ReadResponseSizeMaxLevel.Add(float64(respSize))
				case admission.DefaultLevel:
					r.store.metrics.ReadResponseSizeDefLevel.Add(float64(respSize))
				case admission.MinLevel:
					r.store.metrics.ReadResponseSizeMinLevel.Add(float64(respSize))
				}
			}
			alloc.Release()
		}()
	}

	r.limitTxnMaxTimestamp(ctx, &ba, status)

	spans, err := r.collectSpans(&ba)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	// Acquire latches to prevent overlapping commands from executing
	// until this command completes.
	log.Event(ctx, "acquire latches")
	endCmds, err := r.beginCmds(ctx, &ba, spans)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	log.Event(ctx, "waiting for read lock")
	r.readOnlyCmdMu.RLock()
	defer r.readOnlyCmdMu.RUnlock()

	// Guarantee we release the latches that we just acquired. It is
	// important that this is inside the readOnlyCmdMu lock so that the
	// timestamp cache update is synchronized. This is wrapped to delay
	// pErr evaluation to its value when returning.
	defer func() {
		endCmds.done(br, pErr)
	}()

	// TODO(nvanbenschoten): Can this be moved into Replica.requestCanProceed?
	if _, err := r.IsDestroyed(); err != nil {
		return nil, roachpb.NewError(err)
	}

	rSpan, err := keys.Range(ba)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	if err := r.requestCanProceed(rSpan, ba.Timestamp); err != nil {
		return nil, roachpb.NewError(err)
	}

	// Evaluate read-only batch command. It checks for matching key range; note
	// that holding readOnlyCmdMu throughout is important to avoid reads from the
	// "wrong" key range being served after the range has been split.
	var result result.Result
	rec := NewReplicaEvalContext(r, spans)
	readOnly := r.store.Engine().NewReadOnly()
	if util.RaceEnabled {
		readOnly = spanset.NewReadWriter(readOnly, spans)
	}
	defer readOnly.Close()
	br, result, pErr = evaluateBatch(ctx, storagebase.CmdIDKey(""), readOnly, rec, nil, ba, true /* readOnly */)

	// A merge is (likely) about to be carried out, and this replica
	// needs to block all traffic until the merge either commits or
	// aborts. See docs/tech-notes/range-merges.md.
	if result.Local.DetachMaybeWatchForMerge() {
		if err := r.maybeWatchForMerge(ctx); err != nil {
			return nil, roachpb.NewError(err)
		}
	}

	if intents := result.Local.DetachIntents(); len(intents) > 0 {
		log.Eventf(ctx, "submitting %d intents to asynchronous processing", len(intents))
		// We only allow synchronous intent resolution for consistent requests.
		// Intent resolution is async/best-effort for inconsistent requests.
		//
		// An important case where this logic is necessary is for RangeLookup
		// requests. In their case, synchronous intent resolution can deadlock
		// if the request originated from the local node which means the local
		// range descriptor cache has an in-flight RangeLookup request which
		// prohibits any concurrent requests for the same range. See #17760.
		allowSyncProcessing := ba.ReadConsistency == roachpb.CONSISTENT
		if err := r.store.intentResolver.CleanupIntentsAsync(ctx, intents, allowSyncProcessing); err != nil {
			log.Warning(ctx, err)
		}
	}
	if pErr != nil {
		log.VErrEvent(ctx, 3, pErr.String())
	} else {
		log.Event(ctx, "read completed")
		respSize = br.Size()
		r.store.metrics.ReadResponseSizeSummary1m.Add(float64(respSize))
	}
	return br, pErr
}
