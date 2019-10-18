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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// Provider interacts with the durable state of the protectedts subsystem.
type Provider struct {
	ex *sql.InternalExecutor
}

var _ protectedts.Store = (*Provider)(nil)

func New(ex *sql.InternalExecutor) *Provider {
	return &Provider{ex: ex}
}

const protectQueryTemplate = `
WITH
    meta
        AS (
            INSERT
            INTO system.protected_ts_meta (id, ts, meta_type, meta)
            VALUES ($1, $2, $3, $4)
            RETURNING id
        ),
    spans
        AS (
            SELECT * FROM
            {{- if (not (len .)) }} (SELECT NULL::STRING foo, NULL::STRING bar LIMIT 0)
            {{- else -}}
                (
                    VALUES
                        {{- range $index, $span := . }}{{ if $index }},{{end}}
                        ( ${{ arg 5 $index }}, ${{ arg 6 $index }} )
                        {{- end }}
                )
                    AS _ (key, end_key)
            {{- end}}
        )
INSERT
INTO
    system.protected_ts_spans
(SELECT id, key, end_key FROM meta, spans);
`

// Protect protects a timestamp. The txn may be nil.
func (p *Provider) Protect(
	ctx context.Context,
	txn *client.Txn,
	ts hlc.Timestamp,
	metaType string,
	meta []byte,
	spans []roachpb.Span,
) (*protectedts.ProtectedTS, error) {
	var buf strings.Builder
	if err := protectQueryTmpl.Execute(&buf, spans); err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(err, "failed to generate sql")
	}
	id := uuid.MakeV4()
	args := append(make([]interface{}, 0, 4+len(spans)*2),
		[]byte(id[:]), ts.AsOfSystemTime(), metaType, meta)
	_, err := p.ex.Exec(ctx, "protect-ts", txn, buf.String(),
		spansToValues(args, spans)...)
	if err != nil {
		return nil, err
	}
	return &protectedts.ProtectedTS{
		ID:        id,
		Timestamp: ts,
		Spans:     spans,
		Meta:      meta,
		MetaType:  metaType,
	}, nil
}

var protectQueryTmpl = template.Must(template.New("protect").Funcs(template.FuncMap{
	"arg": func(start, offset int) int {
		return start + offset
	},
}).Parse(protectQueryTemplate))

const iterateQuery = `
SELECT 
    json_build_object('id', m.id, 'ts', ts::string, 'meta', encode(meta, 'base64'), 'spans', json_agg(sub.span)) AS ts
FROM
    system.protected_ts_meta AS m
    FULL JOIN (SELECT id, json_build_array(encode(key, 'base64'), encode(end_key, 'base64')) AS span FROM system.protected_ts_spans)
            AS sub ON sub.id = m.id
GROUP BY
    (m.id, m.meta, m.ts);
`

type protectedTSUnmarshaler struct {
	ID       uuid.UUID               `json:"id"`
	TS       hlcTimestampUnmarshaler `json:"ts"`
	Spans    [][2][]byte             `json:"spans"`
	MetaType string                  `json:"meta_type"`
	Meta     []byte                  `json:"meta"`
}

func (ts *protectedTSUnmarshaler) toTS() protectedts.ProtectedTS {
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
	ret := protectedts.ProtectedTS{
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

func (p *Provider) List(ctx context.Context) ([]protectedts.ProtectedTS, error) {
	rows, err := p.ex.Query(ctx, "iterate-protected-ts", nil, iterateQuery)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	out := make([]protectedts.ProtectedTS, len(rows))
	var v protectedTSUnmarshaler
	for i, row := range rows {
		row[0].(*tree.DJSON).JSON.Format(&buf)
		fmt.Println(buf.String())
		if err := json.Unmarshal(buf.Bytes(), &v); err != nil {
			return nil, err
		}
		out[i] = v.toTS()
	}
	return out, nil
}

func (p *Provider) Release(ctx context.Context, txn *client.Txn, id uuid.UUID) error {
	panic("not implemented")
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

func spansToValues(args []interface{}, spans []roachpb.Span) []interface{} {
	for _, s := range spans {
		args = append(args, s.Key, s.EndKey)
	}
	return args
}
