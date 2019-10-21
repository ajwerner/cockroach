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
	"html/template"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// Protect protects a timestamp. The txn may be nil in which case a new
// transaction from the Provider's underlying DB is used.
func (p *Provider) Protect(
	ctx context.Context,
	txn *client.Txn,
	ts hlc.Timestamp,
	metaType string,
	meta []byte,
	spans ...roachpb.Span,
) (ret *protectedts.ProtectedTS, err error) {
	if ts == (hlc.Timestamp{}) {
		return nil, errors.Errorf("cannot protect zero value timestamp")
	}
	if len(spans) == 0 {
		return nil, errors.Errorf("cannot protect empty set of spans")
	}
	if txn == nil {
		err = p.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			ret, err = p.Protect(ctx, txn, ts, metaType, meta, spans...)
			return err
		})
		return ret, err
	}
	return protectImpl(ctx, p, txn, ts, metaType, meta, spans)
}

var protectQueryTmpl = template.Must(template.New("protect").Funcs(template.FuncMap{
	"arg": func(start, by, offset int) int {
		return start + (by * offset)
	},
}).Parse(
	`WITH
    read_meta AS (SELECT rows, spans, version FROM system.protected_ts_meta),` +
		// TODO(ajwerner): consider installing a migration to ensure that this row is initialized
		`
    current_meta
        AS (
            SELECT * FROM read_meta
            UNION ALL
                SELECT
                    0 AS rows, 0 AS spans, 0 AS version
                WHERE
                    NOT EXISTS (SELECT * FROM read_meta)
        ),
    checks
        AS (
            SELECT
                new_rows, new_spans, new_version, new_rows > $1 OR new_spans > $2 AS failed
            FROM
                (
                    SELECT
                        rows + 1 AS new_rows, spans + $3 AS new_spans, version +1 AS new_version
                    FROM
                        current_meta
                )
            LIMIT 1
        ),
    updated_meta
        AS (
            UPSERT
            INTO
                system.protected_ts_meta
            (rows, spans, version)
            (
                SELECT
                    new_rows, new_spans, new_version
                FROM
                    checks
                WHERE
                    NOT failed
            )
            RETURNING
                rows, spans, version
        ),
    new_row
        AS (
            INSERT
            INTO
                system.protected_ts_timestamps (id, ts, meta_type, meta)
            (
                SELECT
                    id, ts, meta_type, meta
                FROM
                    (VALUES ($4::UUID, $5::DECIMAL, $6, $7))
                        AS _ (id, ts, meta_type, meta),
                    checks
                WHERE
                    NOT failed
            )
            RETURNING
                id
        ),
    spans_to_insert
        AS (
            SELECT
                id, key, end_key
            FROM
                new_row,
                (
                    SELECT
                        *
                    FROM
                        (
                            VALUES
                            {{- range $index, $span := . }}{{ if $index }},{{end}}
                                ( ${{ arg 8 2 $index }}, ${{ arg 9 2 $index }} )
                            {{- end }}
                        )
                            AS _ (key, end_key)
                ),
                checks
            WHERE
                NOT failed
        ),
    span_inserts
        AS (
            INSERT
            INTO
                system.protected_ts_spans
            (SELECT * FROM spans_to_insert)
            RETURNING
                NULL
        )
SELECT
    failed, rows AS prev_rows, spans AS prev_spans, version AS prev_version
FROM
    checks, current_meta;`))

func protectImpl(
	ctx context.Context,
	p *Provider,
	txn *client.Txn,
	ts hlc.Timestamp,
	metaType string,
	meta []byte,
	spans []roachpb.Span,
) (*protectedts.ProtectedTS, error) {

	// TODO(ajwerner): We should maybe cache the string used to install common
	// numbers of spans like 1. Given the lack of prepared statement with the
	// internal executor the parsing and planning probably dominates the rendering
	// of the template anyway.
	var buf strings.Builder
	if err := protectQueryTmpl.Execute(&buf, spans); err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(err, "failed to generate sql")
	}
	id := uuid.MakeV4()
	args := make([]interface{}, 0, 7+len(spans)*2)
	args = append(args, maxRows, maxSpans, len(spans),
		[]byte(id[:]), ts.AsOfSystemTime(), metaType, meta)
	args = spansToValues(args, spans)
	row, err := p.ex.QueryRow(ctx, "protect-ts", txn, buf.String(), args...)
	if err != nil {
		return nil, err
	}
	if *row[0].(*tree.DBool) {
		if rows := int(*row[1].(*tree.DInt)); rows >= maxRows {
			return nil, errors.Errorf("limits exceeded: %d+1 > %d rows",
				rows, maxRows)
		}
		curNumSpans := *row[2].(*tree.DInt)
		return nil, errors.Errorf("limits exceeded: %d+%d > %d spans",
			curNumSpans, len(spans), maxSpans)
	}
	deadline := ts.Add(minGCInterval.Nanoseconds(), 0)
	// This behavior is sort of terrible. I imagine generally callers want to protect "now"
	// where now is just some time which precedes the commit of this transaction. If it took
	// us too long to lay down this intent then there's probably something else bad going on
	// but in principle we'd like for the client to be able to retry this with a higher
	// timestamp.
	//
	// Perhaps this implies that the interface is wrong and we that we should not allow the
	// client to specify the timestamp which they want to protect.
	//
	// This feels like a natural place for a ROLLBACK TO SAVEPOINT if we had set up a
	// savepoint just before the above write.
	//
	// TODO(ajwerner): this doesn't work at all. There is no minimum gcttl and well
	// there is no guarantee that this doesn't get pushed. We need to figure out a
	// real deadline for this transaction and set it. Ideally that deadline should
	// be the timestamp plus some multiple of the minimum gcttl for the spans we're
	// protecting. Then we need to transactionally ensure that we have the right
	// system config.
	if !txn.Serialize().Timestamp.Less(deadline) {
		return nil, errors.Errorf("failed to lay down intents to protect %v before %v, the minimum timestamp at which a read on protected_ts_timestamps table or the protected_ts_meta table",
			ts, deadline)
	}
	return &protectedts.ProtectedTS{
		ID:        id,
		Timestamp: ts,
		Spans:     spans,
		Meta:      meta,
		MetaType:  metaType,
	}, nil
}

func spansToValues(args []interface{}, spans []roachpb.Span) []interface{} {
	for _, s := range spans {
		args = append(args, s.Key, s.EndKey)
	}
	return args
}
