package ptverifier

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

func Verify(ctx context.Context, db *client.DB, s protectedts.Storage, id uuid.UUID) error {
	// For all of the spans in the record we need to go reach out to all of the ranges which cover that span.
	// We're going to do this in an iterative way by finding the ranges, sending them
	//
	// AdminVerifyProtectedTimestampRequests then waiting for responses which will include range info.

	// We want to make the responses combinable

	// We want to create a single batch which will verify the record

	// First we want to read the record, then we want to run the batch
	txn := db.NewTxn(ctx, "verify")
	r, err := s.GetRecord(ctx, txn, id)
	ts := txn.Serialize().Timestamp
	_ = txn.Rollback(ctx) // We don't care.
	if err != nil {
		return errors.Wrapf(err, "failed to fetch record %s", id)
	}

	mergedSpans, _ := roachpb.MergeSpans(r.Spans)
	var b client.Batch
	for _, s := range mergedSpans {
		var req roachpb.AdminVerifyProtectedTimestampRequest
		req.RecordAliveAt = ts
		req.Protected = r.Timestamp
		req.RecordID = r.ID
		req.Key = s.Key
		req.EndKey = s.EndKey
		b.AddRawRequest(&req)
	}
	if err := db.Run(ctx, &b); err != nil {
		return err
	}
	// Now we should go check the responses and synthesize an error.
	rawResponse := b.RawResponse()
	var failed []roachpb.RangeDescriptor
	for _, r := range rawResponse.Responses {
		resp := r.GetInner().(*roachpb.AdminVerifyProtectedTimestampResponse)
		if len(resp.FailedRanges) == 0 {
			continue
		}
		if len(failed) == 0 {
			failed = resp.FailedRanges
		} else {
			failed = append(failed, resp.FailedRanges...)
		}
	}
	if len(failed) > 0 {
		return errors.Errorf("failed to verify protection %v on %v", r, failed)
	}
	return nil
}
