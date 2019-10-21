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
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/protectedts"
	"github.com/cockroachdb/cockroach/pkg/sql"
)

// Provider interacts with the durable state of the protectedts subsystem.
type Provider struct {
	ex *sql.InternalExecutor
	db *client.DB
}

// TODO(ajwerner): move these to a hidden setting or something.
const maxRows = 10
const maxSpans = 100

// TODO(ajwerner): codify this to something and ensure that it's correct.
const minGCInterval = 2 * time.Minute

var _ protectedts.Store = (*Provider)(nil)

func New(ex *sql.InternalExecutor, db *client.DB) *Provider {
	return &Provider{ex: ex, db: db}
}
