// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package protectedts offers abstractions to prevent spans from having old
// MVCC values live at a given timestamp from being GC'd.
package protectedts

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

type ProtectedTS struct {
	ID        uuid.UUID
	Timestamp hlc.Timestamp
	Spans     []roachpb.Span

	MetaType string
	Meta     []byte
}

type Iterator func(*ProtectedTS) (wantMore bool)

type Store interface {
	// Protect will durably create a protected timestamp, if no error is returned
	// then no data in the specified spans at which are live at the specified
	// timestamp can be garbage collected until the this ProtectedTimestamp is
	// released.
	Protect(
		_ context.Context,
		txn *client.Txn,
		ts hlc.Timestamp,
		metaType string, meta []byte,
		spans []roachpb.Span,
	) (*ProtectedTS, error)

	// Release allows spans which were previously protected to now be garbage
	// collected.
	Release(_ context.Context, txn *client.Txn, id uuid.UUID) error

	// Iterate will call the callback for each protected timestamp which is
	// currently stored.
	List(context.Context) ([]ProtectedTS, error)
}

type Reader interface {
	Protected(ctx context.Context, overlaps roachpb.Span, asOf hlc.Timestamp) (bool, error)
}
