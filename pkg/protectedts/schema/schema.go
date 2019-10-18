// Package schema contains the SQL schema for the protectedts subsystem.
package schema

const createTableProtectedTSMeta = `
CREATE TABLE protected_ts_meta (
    id UUID,
    ts DECIMAL,
    meta BYTES,
    PRIMARY KEY(id)
);`

const createTableProtectedTSSpans = `
CREATE TABLE protected_ts_spans (
    id UUID,
    key BYTES,
    end_key BYTES,
    CONSTRAINT fk_protected_ts_meta FOREIGN KEY (id) REFERENCES protected_ts_meta (id),
    PRIMARY KEY (id, key, end_key)
) INTERLEAVE IN PARENT protected_ts_meta (id);`
