// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package provider

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

func (p *Provider) Release(ctx context.Context, txn *client.Txn, id uuid.UUID) error {
	if txn == nil {
		return p.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			return p.Release(ctx, txn, id)
		})
	}

	// First we want to check that this row exists and how many spans it has.
	row, err := p.ex.QueryRow(ctx, "protectedts-release", txn,
		"SELECT COUNT(*) FROM system.protected_ts_spans WHERE id = $1", id[:])
	if err != nil {
		return err
	}
	numSpans := int(*row[0].(*tree.DInt))
	if numSpans == 0 {
		// TODO(ajwerner): can this happen with query row? Isn't it an error if I
		// don't get a row?
		return errors.Errorf("expected non-zero number of spans")
	}
	_, err = p.ex.Exec(ctx, "protectedts-release", txn,
		"UPDATE system.protected_ts_meta SET version = version +1, rows = rows - 1, spans = spans - $1;", numSpans)
	if err != nil {
		return errors.Wrap(err, "failed to update protected_ts_meta")
	}
	// NB: This will CASCADE to delete all of the associated spans.
	_, err = p.ex.Exec(ctx, "protectedts-release", txn,
		"DELETE FROM system.protected_ts_timestamps WHERE id = $1", id[:])
	if err != nil {
		return errors.Wrap(err, "failed to delete row")
	}
	return nil
}
