package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const listQuery = `
SELECT
    json_build_object(
        'version',
        version,
        'as_of',
        cluster_logical_timestamp()::STRING,
        'timestamps',
        timestamps
    )
        AS protected_timestamps
FROM
    system.protected_ts_meta,
    (
        SELECT
            json_agg(ts) AS timestamps
        FROM
            (
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
                    )
                        AS ts
                FROM
                    system.protected_ts_timestamps AS m
                    RIGHT JOIN (
                            SELECT
                                id,
                                json_build_array(encode(key, 'base64'), encode(end_key, 'base64'))
                                    AS span
                            FROM
                                system.protected_ts_spans
                        )
                            AS sub ON sub.id = m.id
                GROUP BY
                    (m.id, m.meta, m.ts)
            )
    );`

type protectedTimestampsUnmarshaler protectedts.ProtectedTimestamps

func (pt *protectedTimestampsUnmarshaler) UnmarshalJSON(data []byte) error {
	var raw struct {
		Version    int                      `json:"version"`
		AsOf       hlcTimestampUnmarshaler  `json:"as_of"`
		Timestamps []protectedTSUnmarshaler `json:"timestamps"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*pt = protectedTimestampsUnmarshaler{
		Version:    raw.Version,
		AsOf:       hlc.Timestamp(raw.AsOf),
		Timestamps: toTimestamps(raw.Timestamps),
	}
	return nil
}

func toTimestamps(unmarshaled []protectedTSUnmarshaler) []protectedts.ProtectedTS {
	ret := make([]protectedts.ProtectedTS, len(unmarshaled))
	for i := range unmarshaled {
		ret[i] = unmarshaled[i].toTS()
	}
	return ret
}

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

func (p *Provider) List(ctx context.Context) (*protectedts.ProtectedTimestamps, error) {
	row, err := p.ex.QueryRow(ctx, "iterate-protected-ts", nil, listQuery)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	var v protectedts.ProtectedTimestamps
	row[0].(*tree.DJSON).JSON.Format(&buf)
	if err := json.Unmarshal(buf.Bytes(), (*protectedTimestampsUnmarshaler)(&v)); err != nil {
		return nil, err
	}
	return &v, nil
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
