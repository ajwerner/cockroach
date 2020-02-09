// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvfeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/tablefeed"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/span"
)

type Config struct {
	Settings         *cluster.Settings
	DB               *client.DB
	Clock            *hlc.Clock
	Gossip           *gossip.Gossip
	Spans            []roachpb.Span
	Details          jobspb.ChangefeedDetails
	Sink             *Buffer
	LeaseMgr         *sql.LeaseManager
	Metrics          *Metrics
	MM               *mon.BytesMonitor
	InitialHighWater hlc.Timestamp

	needsInitialScan bool

	// These dependencies are made available for test injection.
	scanner      scanner
	tableFeed    tableFeed
	physicalFeed physicalFeedFactory
}

type tableFeed interface {
	Peek(ctx context.Context, atOrBefore hlc.Timestamp) (events []tablefeed.Event, err error)
	Pop(ctx context.Context, atOrBefore hlc.Timestamp) (events []tablefeed.Event, err error)
}

// Run will run the kvfeed. The feed runs synchronously and returns an
// error when it finishes.
//
// TODO(ajwerner): Better document.
func Run(ctx context.Context, cfg Config) error {
	// TODO(ajwerner): It'd be nice to support an initial scan from a cursor position
	// Doing so would not be hard, we'd just need an option.
	if cfg.needsInitialScan = cfg.InitialHighWater == (hlc.Timestamp{}); cfg.needsInitialScan {
		// If no highWater is specified, set the highwater to the statement time
		// and add a scanBoundary at the statement time to trigger an immediate output
		// of the full table.
		cfg.InitialHighWater = cfg.Details.StatementTime
	}
	if cfg.physicalFeed == nil {
		sender := cfg.DB.NonTransactionalSender()
		distSender := sender.(*client.CrossRangeTxnWrapperSender).Wrapped().(*kv.DistSender)
		cfg.physicalFeed = &rangefeedFactory{ds: distSender}
	}
	if cfg.scanner == nil {
		cfg.scanner = &scanRequestScanner{
			settings: cfg.Settings,
			gossip:   cfg.Gossip,
			db:       cfg.DB,
		}
	}
	g := ctxgroup.WithContext(ctx)
	if cfg.tableFeed == nil {
		tf := tablefeed.New(makeTablefeedConfig(cfg))
		// Start polling tablehistory, which must be done concurrently with
		// the individual rangefeed routines.
		if err := tf.Start(ctx, g); err != nil {
			return err
		}
		cfg.tableFeed = tf
	}
	g.GoCtx(func(ctx context.Context) error { return run(ctx, cfg) })
	return g.Wait()
}

func run(ctx context.Context, cfg Config) (err error) {
	// highWater represents the point in time at or before which we know
	// we've seen all events or is the initial starting time of the feed.
	highWater := cfg.InitialHighWater
	for i := 0; ; i++ {
		initialScan := i == 0
		if err = scanIfShould(ctx, initialScan, highWater, cfg); err != nil {
			return err
		}
		highWater, err = runUntilTableEvent(ctx, highWater, cfg)
		if err != nil {
			return err
		}
		// TODO(ajwerner): At this point we could check the configuration and
		// potentially exit if there were a schema change event and we're configured
		// to exit on schema changes.
	}
}

func scanIfShould(
	ctx context.Context, initialScan bool, highWater hlc.Timestamp, cfg Config,
) error {
	scanTime := highWater.Next()
	events, err := cfg.tableFeed.Peek(ctx, scanTime)
	if err != nil {
		return err
	}
	// This off-by-one is a little weird. It says that if you create a changefeed
	// at some statement time then you're going to get the table as of that statement
	// time with an initial backfill but if you use a cursor then you will get the
	// updates after that timestamp.
	if initialScan && cfg.needsInitialScan {
		scanTime = highWater
	} else if len(events) > 0 {
		// TODO(ajwerner): In this case we should only backfill for the tables
		// which have events which may not be all of the targets.
		for _, ev := range events {
			if !scanTime.Equal(ev.After.ModificationTime) {
				log.Fatalf(ctx, "found event in shouldScan which did not occur at the scan time %v: %v",
					scanTime, ev)
			}
		}
	} else {
		return nil
	}
	_, withDiff := cfg.Details.Opts[changefeedbase.OptDiff]
	cfg.scanner.Scan(ctx, cfg.Sink, physicalConfig{
		Spans:     cfg.Spans,
		Timestamp: scanTime,
		WithDiff:  !(initialScan && cfg.needsInitialScan) && withDiff,
	})
	// Consume the events up to scanTime.
	if _, err := cfg.tableFeed.Pop(ctx, scanTime); err != nil {
		return err
	}

	// NB: We don't update the highwater even though we've technically seen all
	// events for all spans at the previous highwater.Next(). We choose not to
	// because doing so would be wrong once we only backfill some tables.
	return nil
}

func runUntilTableEvent(
	ctx context.Context, startFrom hlc.Timestamp, cfg Config,
) (resolvedUpTo hlc.Timestamp, err error) {
	// Determine whether to request the previous value of each update from
	// RangeFeed based on whether the `diff` option is specified.
	_, withDiff := cfg.Details.Opts[changefeedbase.OptDiff]
	if _, err := cfg.tableFeed.Peek(ctx, startFrom); err != nil {
		return hlc.Timestamp{}, err
	}

	memBuf := makeMemBuffer(cfg.MM.MakeBoundAccount(), cfg.Metrics)
	defer memBuf.Close(ctx)

	g := ctxgroup.WithContext(ctx)
	physicalCfg := physicalConfig{Spans: cfg.Spans, Timestamp: startFrom, WithDiff: withDiff}
	g.GoCtx(func(ctx context.Context) error {
		return copyFromSourceToSinkUntilTableEvent(ctx, cfg.Sink, memBuf, physicalCfg, cfg.tableFeed)
	})
	cfg.physicalFeed.Start(&g, memBuf, physicalCfg)

	// TODO(mrtracy): We are currently tearing down the entire rangefeed set in
	// order to perform a scan; however, given that we have an intermediate
	// buffer, its seems that we could do this without having to destroy and
	// recreate the rangefeeds.
	err = g.Wait()
	switch err := err.(type) {
	case nil:
		log.Fatalf(ctx, "feed exited with no error and no scan boundary")
		return hlc.Timestamp{}, nil // unreachable
	case *errBoundaryReached:
		// TODO(ajwerner): iterate the spans and add a Resolved timestamp.
		// We'll need to do this to ensure that a resolved timestamp propagates
		// when we're trying to exit.
		return err.Timestamp().Prev(), nil
	default:
		return hlc.Timestamp{}, err
	}
}

type errBoundaryReached struct {
	tablefeed.Event
}

func (e *errBoundaryReached) Error() string {
	return "scan boundary reached: " + e.String()
}

// copyFromSourceToSinkUntilTableEvents will pull read entries from source and
// publish them to sink if there is no table event from the tableFeed. If a
// tableEvent occurs then the function will return once all of the spans have
// been resolved up to the event. The first such event will be returned as
// *errBoundaryReached. A nil error will never be returned.
//
// TODO(ajwerner): unify the sink and source buffers through an interface.
func copyFromSourceToSinkUntilTableEvent(
	ctx context.Context, sink *Buffer, source *memBuffer, cfg physicalConfig, tables tableFeed,
) error {
	// Maintain a local spanfrontier to tell when all the component rangefeeds
	// being watched have reached the Scan boundary.
	frontier := span.MakeFrontier(cfg.Spans...)
	for _, span := range cfg.Spans {
		frontier.Forward(span, cfg.Timestamp)
	}
	var (
		scanBoundary         *errBoundaryReached
		checkForScanBoundary = func(ts hlc.Timestamp) error {
			if scanBoundary != nil {
				return nil
			}
			nextEvents, err := tables.Peek(ctx, ts)
			if err != nil {
				return err
			}
			// TODO(ajwerner): check the policy for table events
			if len(nextEvents) > 0 {
				scanBoundary = &errBoundaryReached{nextEvents[0]}
			}
			return nil
		}
		applyScanBoundary = func(e BufferEntry) (skipEvent, reachedBoundary bool) {
			if scanBoundary == nil {
				return false, false
			}
			if bufferEntryTimestamp(e).Less(scanBoundary.Timestamp()) {
				return false, false
			}
			if isKV := e.KV.Key != nil; isKV {
				return true, false
			}
			boundaryResolvedTimestamp := scanBoundary.Timestamp().Prev()
			if e.Resolved.Timestamp.LessEq(boundaryResolvedTimestamp) {
				return false, false
			}
			frontier.Forward(e.Resolved.Span, boundaryResolvedTimestamp)
			return true, frontier.Frontier() == boundaryResolvedTimestamp
		}
		addEntry = func(e BufferEntry) error {
			if isKV := e.KV.Key != nil; isKV {
				return sink.AddKV(ctx, e.KV, e.PrevVal, e.BackfillTimestamp)
			}
			// TODO(ajwerner): technically this doesn't need to happen for most
			// events - we just need to make sure we forward for events which are
			// at scanBoundary.Prev(). The logic currently doesn't make this clean.
			frontier.Forward(e.Resolved.Span, e.Resolved.Timestamp)
			return sink.AddResolved(ctx, e.Resolved.Span, e.Resolved.Timestamp)
		}
	)
	for {
		e, err := source.Get(ctx)
		if err != nil {
			return err
		}
		if err := checkForScanBoundary(bufferEntryTimestamp(e)); err != nil {
			return err
		}
		skipEntry, scanBoundaryReached := applyScanBoundary(e)
		if scanBoundaryReached {
			// All component rangefeeds are now at the boundary.
			// Break out of the ctxgroup by returning the sentinel error.
			return scanBoundary
		}
		if skipEntry {
			continue
		}
		if err := addEntry(e); err != nil {
			return err
		}
	}
}

func makeTablefeedConfig(cfg Config) tablefeed.Config {
	return tablefeed.Config{
		DB:               cfg.DB,
		Clock:            cfg.Clock,
		Settings:         cfg.Settings,
		Targets:          cfg.Details.Targets,
		LeaseManager:     cfg.LeaseMgr,
		FilterFunc:       defaultBackfillPolicy.ShouldFilter,
		InitialHighWater: cfg.InitialHighWater,
	}
}
