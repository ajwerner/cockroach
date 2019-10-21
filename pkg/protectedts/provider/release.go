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
	return releaseImpl(ctx, p, txn, id)
}

func releaseImpl(ctx context.Context, p *Provider, txn *client.Txn, id uuid.UUID) error {
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
	n, err := p.ex.Exec(ctx, "protectedts-release", txn,
		"DELETE FROM system.protected_ts_timestamps WHERE id = $1", id[:])
	if err != nil {
		return errors.Wrap(err, "failed to delete row")
	}
	if n != 1 {
		panic("I really needed to delete a row")
	}
	return nil
}
