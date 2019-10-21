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

// Store provides clients with a mechanism to transactionally protect and
// release protected timestamps for a set of spans.
//
// Clients may provide a txn object which will allow them to write the id
// of this new protection transactionally into their own state. If the caller
// provides a txn object then it is the caller's responsibility to call
// CleanupOnError if an error is returned. If an error is returned and the
// transaction is subsequently committed, the resulting state of the system
// is undefined.
//
//
// The above comment about an undefined state occurs because this transaction
// may determine that the limits have been reached for the maximum number of
// rows or spans but it will already have laid down an intent to update those
// values.
//
// It is the caller's responsibility to ensure that a timestamp is ultimately
// released.
type Store interface {

	// Protect will durably create a protected timestamp, if no error is returned
	// then no data in the specified spans at which are live at the specified
	// timestamp can be garbage collected until the this ProtectedTimestamp is
	// released.
	//
	// Protect ensures when this returns that the intent for the protected timestamp
	// is laid down within the minimum GC interval thus ensuring that any
	// successful read of the table during which could possibly attempt to
	// GC this timestamp would fail.
	//
	// TODO(ajwerner): should this just return a UUID?
	Protect(
		_ context.Context,
		txn *client.Txn,
		ts hlc.Timestamp,
		metaType string, meta []byte,
		spans ...roachpb.Span,
	) (*ProtectedTS, error)

	// Release allows spans which were previously protected to now be garbage
	// collected.
	Release(_ context.Context, txn *client.Txn, id uuid.UUID) error
}

type Catalog interface {
	Version(context.Context) (version int, asOf hlc.Timestamp, _ error)
	// Iterate will call the callback for each protected timestamp which is
	// currently stored.
	List(context.Context) (ProtectedTimestamps, error)
}

type ProtectedTimestamps struct {
	Version    int
	AsOf       hlc.Timestamp
	Timestamps []ProtectedTS
}

type Reader interface {
	Protected(ctx context.Context, overlaps roachpb.Span, asOf hlc.Timestamp) (bool, error)
}
