// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ptstorage

import (
	"bytes"
	"context"
	"encoding/json"
	"html/template"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// Storage interacts with the durable state of the protectedts subsystem.
type Storage struct {
	settings *cluster.Settings
	ex       *sql.InternalExecutor
}

var _ protectedts.Storage = (*Storage)(nil)

// New creates a new Storage.
func New(settings *cluster.Settings, ex *sql.InternalExecutor) *Storage {
	return &Storage{settings: settings, ex: ex}
}

// Protect protects a timestamp. The txn may be nil in which case a new
// transaction from the Storage's underlying DB is used.
func (p *Storage) Protect(ctx context.Context, txn *client.Txn, r *ptpb.Record) error {
	if err := validateRecord(r); err != nil {
		return err
	}

	// TODO(ajwerner): We should maybe cache the string used to install common
	// numbers of spans like 1. Given the lack of prepared statement with the
	// internal executor the parsing and planning probably dominates the rendering
	// of the template anyway.
	var buf strings.Builder
	if err := protectQueryTmpl.Execute(&buf, r.Spans); err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "failed to generate sql")
	}

	maxRecords := protectedts.MaxRecords.Get(&p.settings.SV)
	maxSpans := protectedts.MaxSpans.Get(&p.settings.SV)
	args := make([]interface{}, 0, 7+len(r.Spans)*2)
	args = append(args,
		maxRecords,
		maxSpans, len(r.Spans),
		[]byte(r.ID[:]), r.Timestamp.AsOfSystemTime(), r.MetaType, r.Meta)
	args = spansToValues(args, r.Spans)
	row, err := p.ex.QueryRow(ctx, "protect-ts", txn, buf.String(), args...)
	if err != nil {
		return err
	}
	if failed := *row[0].(*tree.DBool); failed {
		records := int64(*row[1].(*tree.DInt))
		if records >= maxRecords {
			return errors.Errorf("limits exceeded: %d+1 > %d rows",
				records, maxRecords)
		}
		curNumSpans := int64(*row[2].(*tree.DInt))
		if curNumSpans >= maxSpans {
			return errors.Errorf("limits exceeded: %d+%d > %d spans",
				curNumSpans, len(r.Spans), maxSpans)
		}
		return protectedts.ErrExists
	}
	return nil
}

func validateRecord(r *ptpb.Record) error {
	if r.Timestamp == (hlc.Timestamp{}) {
		return errors.Errorf("cannot protect zero value timestamp")
	}
	if len(r.Spans) == 0 {
		return errors.Errorf("cannot protect empty set of spans")
	}
	return nil
}

// GetRecord returns a record with the provided id.
func (p *Storage) GetRecord(
	ctx context.Context, txn *client.Txn, id uuid.UUID,
) (*ptpb.Record, error) {
	row, err := p.ex.QueryRow(ctx, "iterate-protected-ts", txn, getRecord, id.String())
	if err != nil {
		return nil, err
	}
	if len(row) == 0 {
		return nil, protectedts.ErrNotFound
	}
	var buf bytes.Buffer
	var v recordUnmarshaler
	row[0].(*tree.DJSON).JSON.Format(&buf)
	if err := json.Unmarshal(buf.Bytes(), &v); err != nil {
		return nil, err
	}
	rec := v.toRecord()
	return &rec, nil
}

// Release releases a record with a provided ID.
func (p *Storage) Release(ctx context.Context, txn *client.Txn, id uuid.UUID) error {
	// Check if the record exists
	// If the record exists then delete it and update the meta row
	row, err := p.ex.QueryRow(ctx, "protected-ts-release", txn, releaseQuery, id[:])
	if err != nil {
		return err
	}
	if *row[0].(*tree.DInt) == 0 {
		return protectedts.ErrNotFound
	}
	return nil
}

func (p *Storage) GetMetadata(ctx context.Context, txn *client.Txn) (ptpb.Metadata, error) {
	row, err := p.ex.QueryRow(ctx, "protected-ts-get-metadata", txn, `WITH 
`+currentMetaCTEs+`
    SELECT num_records, num_spans, version FROM current_meta`)
	if err != nil {
		return ptpb.Metadata{}, errors.Wrap(err, "failed to fetch metadata")
	}
	return ptpb.Metadata{
		Version:    uint64(*row[2].(*tree.DInt)),
		NumRecords: uint64(*row[0].(*tree.DInt)),
		NumSpans:   uint64(*row[1].(*tree.DInt)),
	}, nil
}

// GetState returns the current set of protected timestamps.
func (p *Storage) GetState(ctx context.Context, txn *client.Txn) (ptpb.State, error) {
	row, err := p.ex.QueryRow(ctx, "iterate-protected-ts", txn, listQuery)
	if err != nil {
		return ptpb.State{}, err
	}
	var buf bytes.Buffer
	var v ptpb.State
	row[0].(*tree.DJSON).JSON.Format(&buf)
	if err := json.Unmarshal(buf.Bytes(), (*stateUnmarshaler)(&v)); err != nil {
		return ptpb.State{}, err
	}
	return v, nil
}

const (
	currentMetaCTEs = `
read_meta AS (
    SELECT num_records, num_spans, version FROM system.protected_ts_meta
),` +
		// TODO(ajwerner): consider installing a migration to ensure that this row is initialized
		`
current_meta AS (
     SELECT num_records, num_spans, version FROM read_meta
     UNION ALL
     SELECT
         0 AS num_records, 0 AS num_spans, 0 AS version
     WHERE
         NOT EXISTS (SELECT * FROM read_meta)
)`

	// Collect the number of spans for the record identified by $1.
	releaseQuerySelectRecordAndNumSpans = `
SELECT
    r.id AS id, count(*) AS spans
FROM
    system.protected_ts_records AS r
    RIGHT JOIN system.protected_ts_spans AS s ON r.id = s.id
WHERE
    r.id = $1
GROUP BY
    (r.id, r.meta, r.ts)`

	// Updates the meta row if there was a record.
	releaseQueryUpsertMeta = `
UPSERT INTO
    system.protected_ts_meta (num_records, num_spans, version)
(
    SELECT
        num_records, num_spans, version
    FROM
        (
            SELECT
                num_records - 1 AS num_records,
                num_spans - spans AS num_spans,
                version + 1 AS version
            FROM
                current_meta, record
        ), record
    WHERE
        EXISTS(SELECT * FROM record)
)
RETURNING 1`

	// Delete the record if it existed.
	releaseQueryDeletedRow = `
DELETE FROM
    system.protected_ts_records AS r
WHERE
    EXISTS(SELECT NULL FROM record WHERE record.id = r.id)
RETURNING
    NULL`

	// Put it all together.
	releaseQuery = `WITH
    record AS (` + releaseQuerySelectRecordAndNumSpans + `),` +
		currentMetaCTEs + `,
    updated_meta AS ( ` + releaseQueryUpsertMeta + ` ),
    deleted_row AS ( ` + releaseQueryDeletedRow + ` )
SELECT count(*) FROM deleted_row`

	getRecordQueryBase = `
SELECT
    json_build_object(
        'id',
        m.id,
        'ts',
        ts::STRING,
        'meta',
        encode(meta, 'base64'),
        'spans',
        json_agg(sub.span)
    ) AS record
FROM
    system.protected_ts_records AS m
RIGHT JOIN (
    SELECT
        id,
        json_build_array(encode(key, 'base64'), encode(end_key, 'base64'))
            AS span
    FROM
        system.protected_ts_spans
) AS sub ON sub.id = m.id`
	getRecordQueryGroupBy = `
GROUP BY (m.id, m.meta, m.ts)`
	getAllRecords = getRecordQueryBase + getRecordQueryGroupBy
	getRecord     = getRecordQueryBase + " WHERE m.id = $1 " + getRecordQueryGroupBy

	listQuery = `WITH` + currentMetaCTEs + `
SELECT
    jsonb_build_object(
        'version', version,
        'num_records', num_records,
        'num_spans', num_spans,
        'records', records
    ) AS state
FROM
    current_meta,
    (
        SELECT json_agg(record) AS records
        FROM (` + getAllRecords + `)
    );`
)

var protectQueryTmpl = template.Must(template.New("protect").Funcs(template.FuncMap{
	"arg": func(start, by, offset int) int {
		return start + (by * offset)
	},
}).Parse(
	`WITH
` + currentMetaCTEs + `,
    old_record
        AS (
            SELECT NULL FROM system.protected_ts_records WHERE id = $4
        ),
    checks
        AS (
            SELECT
                new_records, new_spans, new_version, new_records > $1 OR new_spans > $2 OR EXISTS(SELECT * FROM old_record) AS failed
            FROM
                (
                    SELECT
                        num_records + 1 AS new_records, num_spans + $3 AS new_spans, version +1 AS new_version
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
            (num_records, num_spans, version)
            (
                SELECT
                    new_records, new_spans, new_version
                FROM
                    checks
                WHERE
                    NOT failed
            )
            RETURNING
                num_records, num_spans, version
        ),
    new_record
        AS (
            INSERT
            INTO
                system.protected_ts_records (id, ts, meta_type, meta)
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
                new_record,
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
    failed, num_records AS prev_records, num_spans AS prev_spans, version AS prev_version
FROM
    checks, current_meta;`))

func spansToValues(args []interface{}, spans []roachpb.Span) []interface{} {
	for _, s := range spans {
		args = append(args, s.Key, s.EndKey)
	}
	return args
}

type stateUnmarshaler ptpb.State

func (pt *stateUnmarshaler) UnmarshalJSON(data []byte) error {
	var raw struct {
		metadataUnmarshaler
		Records []recordUnmarshaler `json:"records"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*pt = stateUnmarshaler{
		Metadata: raw.metadataUnmarshaler.toMetadata(),
		Records:  toRecords(raw.Records),
	}
	return nil
}

func toRecords(unmarshaled []recordUnmarshaler) []ptpb.Record {
	if len(unmarshaled) == 0 {
		return nil
	}
	ret := make([]ptpb.Record, len(unmarshaled))
	for i := range unmarshaled {
		ret[i] = unmarshaled[i].toRecord()
	}
	return ret
}

type metadataUnmarshaler struct {
	Version    int `json:"version"`
	NumRecords int `json:"num_records"`
	NumSpans   int `json:"num_spans"`
}

func (md *metadataUnmarshaler) toMetadata() ptpb.Metadata {
	return ptpb.Metadata{
		Version:    uint64(md.Version),
		NumRecords: uint64(md.NumRecords),
		NumSpans:   uint64(md.NumSpans),
	}
}

type recordUnmarshaler struct {
	ID       uuid.UUID               `json:"id"`
	TS       hlcTimestampUnmarshaler `json:"ts"`
	Spans    [][2][]byte             `json:"spans"`
	MetaType string                  `json:"meta_type"`
	Meta     []byte                  `json:"meta"`
}

func (ts *recordUnmarshaler) toRecord() ptpb.Record {
	var spans []roachpb.Span
	if len(ts.Spans) > 0 {
		spans = make([]roachpb.Span, len(ts.Spans))
		for i, pair := range ts.Spans {
			spans[i] = roachpb.Span{
				Key:    roachpb.Key(pair[0]),
				EndKey: roachpb.Key(pair[1]),
			}
		}
	}
	ret := ptpb.Record{
		ID:        ts.ID,
		Timestamp: hlc.Timestamp(ts.TS),
		Spans:     spans,
		MetaType:  ts.MetaType,
	}
	if len(ts.Meta) > 0 {
		ret.Meta = ts.Meta
	}
	return ret
}

type hlcTimestampUnmarshaler hlc.Timestamp

func (ts *hlcTimestampUnmarshaler) UnmarshalText(text []byte) error {
	decimal := bytes.IndexRune(text, '.')
	wallTime, err := strconv.ParseInt(string(text[:decimal]), 10, 64)
	if err != nil {
		return err
	}
	logical, err := strconv.ParseInt(string(text[decimal+1:]), 10, 32)
	if err != nil {
		return err
	}
	ts.WallTime, ts.Logical = wallTime, int32(logical)
	return nil
}
