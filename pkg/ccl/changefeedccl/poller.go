// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/tablefeed"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// poller uses ExportRequest with the `ReturnSST` to repeatedly fetch every kv
// that changed between a set of timestamps and insert them into a buffer.
//
// Each poll (ie set of ExportRequests) are rate limited to be no more often
// than the `changefeed.experimental_poll_interval` setting.
type poller struct {
	settings         *cluster.Settings
	db               *client.DB
	clock            *hlc.Clock
	gossip           *gossip.Gossip
	spans            []roachpb.Span
	details          jobspb.ChangefeedDetails
	buf              *buffer
	tableFeed        *tablefeed.TableFeed
	leaseMgr         *sql.LeaseManager
	metrics          *Metrics
	mm               *mon.BytesMonitor
	needsInitialScan bool

	mu struct {
		syncutil.Mutex
		// highWater timestamp for exports processed by this poller so far.
		highWater hlc.Timestamp
	}
}

func makePoller(
	settings *cluster.Settings,
	db *client.DB,
	clock *hlc.Clock,
	gossip *gossip.Gossip,
	spans []roachpb.Span,
	details jobspb.ChangefeedDetails,
	highWater hlc.Timestamp,
	buf *buffer,
	leaseMgr *sql.LeaseManager,
	metrics *Metrics,
	mm *mon.BytesMonitor,
) *poller {
	p := &poller{
		settings: settings,
		db:       db,
		clock:    clock,
		gossip:   gossip,

		spans:    spans,
		details:  details,
		buf:      buf,
		leaseMgr: leaseMgr,
		metrics:  metrics,
		mm:       mm,
	}
	// TODO(ajwerner): It'd be nice to support an initial scan from a cursor position
	// Doing so would not be hard, we'd just need an option.
	if p.needsInitialScan = highWater == (hlc.Timestamp{}); p.needsInitialScan {
		// If no highWater is specified, set the highwater to the statement time
		// and add a scanBoundary at the statement time to trigger an immediate output
		// of the full table.
		p.mu.highWater = details.StatementTime
	} else {
		p.mu.highWater = highWater
	}
	tableEventsConfig := tablefeed.Config{
		DB:               db,
		Clock:            clock,
		Settings:         settings,
		Targets:          details.Targets,
		LeaseManager:     leaseMgr,
		FilterFunc:       defaultBackfillPolicy.ShouldFilter,
		InitialHighWater: p.mu.highWater,
	}
	p.tableFeed = tablefeed.New(tableEventsConfig)
	return p
}

var defaultBackfillPolicy = BackfillPolicy{
	AddColumn:  true,
	DropColumn: true,
}

type BackfillPolicy struct {
	AddColumn  bool
	DropColumn bool
	// ColumnRename bool
}

func (b BackfillPolicy) ShouldFilter(ctx context.Context, e tablefeed.Event) (bool, error) {
	interestingEvent := (b.AddColumn && newColumnBackfillComplete(e)) ||
		(b.DropColumn && hasNewColumnDropBackfillMutation(e))
	return !interestingEvent, nil
}

// runUsingRangeFeeds performs the same task as the normal Run method, but uses
// the experimental Rangefeed system to capture changes rather than the
// poll-and-export method.  Note
func (p *poller) runUsingRangefeeds(ctx context.Context) error {

	// Start polling tablehistory, which must be done concurrently with
	// the individual rangefeed routines.
	g := ctxgroup.WithContext(ctx)

	// We want to initialize the table history which will pull the initial version
	// and then begin polling.
	//
	// TODO(ajwerner): As written the polling will add table events forever.
	// If there are a ton of table events we'll buffer them all in RAM. There are
	// cases where this might be problematic. It could be mitigated with some
	// memory monitoring. Probably better is to not poll eagerly but only poll if
	// we don't have an event.
	//
	// After we add some sort of locking to prevent schema changes we should also
	// only poll if we don't have a lease.
	if err := p.tableFeed.Start(ctx, g); err != nil {
		return err
	}
	g.GoCtx(p.rangefeedImpl)
	return g.Wait()
}

var errBoundaryReached = errors.New("scan boundary reached")

// TODO(ajwerner): this should return the set of tables to scan.
func (p *poller) needsScan(
	ctx context.Context, isInitialScan bool,
) (needsScan bool, scanTime hlc.Timestamp, _ error) {
	p.mu.Lock()
	highWater := p.mu.highWater
	p.mu.Unlock()
	scanTime = highWater.Next()

	// We should check if there are events for us to process at our high water.
	// We used to miss these

	// This will only happen if we don't have a cursor.
	// TODO(ajwerner): This should be instead based on a policy about whether we should do an
	// initial scan and a boolean under the mutex as to whether we've done it.

	// Another weird thing is that if this isn't the initial scan we really want
	// to check highWater.Next().

	// If we have a high water it means that we've seen all rows up to that
	// we've seen all rows up to this one inclusive. If it's an initial scan
	// we haven't seen any rows and we're saying we want to start at the current
	// time.
	events, err := p.tableFeed.Peek(ctx, scanTime)
	defer func() {
		log.Infof(ctx, "needsScan highWater %v, scanTime %v, initialScan %v, needsScan %v scanTime %v events %v", highWater, scanTime, isInitialScan && p.needsInitialScan, needsScan, scanTime, len(events))
	}()

	if err != nil {
		return false, hlc.Timestamp{}, err
	}
	if isInitialScan && p.needsInitialScan {
		return true, p.mu.highWater, nil
	}
	if len(events) > 0 {
		// We should return true (and then the set of relevant targets) when we
		// have an event. Given
		for _, ev := range events {
			if !scanTime.Equal(ev.After.ModificationTime) {
				log.Fatalf(ctx, "found event in needsScan which did not occur at the scan time %v: %v",
					scanTime, ev)
			}
		}
		return true, scanTime, err
	}
	return false, hlc.Timestamp{}, nil
}

func (p *poller) rangefeedImpl(ctx context.Context) error {
	// We might need to perform an initial scan - let's check
	// Check if we need to do an initial scan.
	_, withDiff := p.details.Opts[optDiff]

	for i := 0; ; i++ {
		initialScan := i == 0
		needsScan, scanTime, err := p.needsScan(ctx, initialScan)
		if err != nil {
			return err
		}
		if needsScan {
			backfillWithDiff := !(initialScan && p.needsInitialScan) && withDiff
			if err := p.performScan(ctx, scanTime, backfillWithDiff); err != nil {
				return err
			}
		}

		// I think this should do something like process it's scans as necessary
		// and then run the rangefeed until we hit an event or error.

		// We need to do an initial scan

		// When we first start up we want to generally either backfill all the
		// tables from the beginning of time or not at all.

		if err := p.rangefeedImplIter(ctx, i); err != nil {
			return err
		}
	}
}

// TODO(ajwerner): This should take a set of targets so that we can backfill
// just some tables.
func (p *poller) performScan(
	ctx context.Context, scanTime hlc.Timestamp, backfillWithDiff bool,
) error {
	spans, err := getSpansToProcess(ctx, p.db, p.spans)
	if err != nil {
		return err
	}
	if err := p.exportSpansParallel(ctx, spans, scanTime, backfillWithDiff); err != nil {
		return err
	}
	if _, err := p.tableFeed.Pop(ctx, scanTime); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.highWater = scanTime
	return nil
}

func (p *poller) rangefeedImplIter(ctx context.Context, i int) error {
	// Determine whether to request the previous value of each update from
	// RangeFeed based on whether the `diff` option is specified.
	_, withDiff := p.details.Opts[optDiff]
	p.mu.Lock()
	lastHighwater := p.mu.highWater
	p.mu.Unlock()
	if _, err := p.tableFeed.Peek(ctx, lastHighwater); err != nil {
		return err
	}

	// NB: this might have just happened for the scan but it seems
	// okay to do it again.
	spans, err := getSpansToProcess(ctx, p.db, p.spans)
	if err != nil {
		return err
	}

	// Start rangefeeds, exit polling if we hit a resolved timestamp beyond
	// the next scan boundary.

	// TODO(nvanbenschoten): This is horrible.
	sender := p.db.NonTransactionalSender()
	ds := sender.(*client.CrossRangeTxnWrapperSender).Wrapped().(*kv.DistSender)
	g := ctxgroup.WithContext(ctx)
	eventC := make(chan *roachpb.RangeFeedEvent, 128)

	// To avoid blocking raft, RangeFeed puts all entries in a server side
	// buffer. But to keep things simple, it's a small fixed-sized buffer. This
	// means we need to ingest everything we get back as quickly as possible, so
	// we throw it in a buffer here to pick up the slack between RangeFeed and
	// the sink.
	//
	// TODO(dan): Right now, there are two buffers in the changefeed flow when
	// using RangeFeeds, one here and the usual one between the poller and the
	// rest of the changefeed (he latter of which is implemented with an
	// unbuffered channel, and so doesn't actually buffer). Ideally, we'd have
	// one, but the structure of the poller code right now makes this hard.
	// Specifically, when a schema change happens, we need a barrier where we
	// flush out every change before the schema change timestamp before we start
	// emitting any changes from after the schema change. The poller's
	// `tableFeed` is responsible for detecting and enforcing these (they queue
	// up in `p.scanBoundaries`), but the after-poller buffer doesn't have
	// access to any of this state. A cleanup is in order.
	memBuf := makeMemBuffer(p.mm.MakeBoundAccount(), p.metrics)
	defer memBuf.Close(ctx)

	// Maintain a local spanfrontier to tell when all the component rangefeeds
	// being watched have reached the Scan boundary.
	// TODO(mrtracy): The alternative to this would be to maintain two
	// goroutines for each span (the current arrangement is one goroutine per
	// span and one multiplexing goroutine that outputs to the buffer). This
	// alternative would allow us to stop the individual rangefeeds earlier and
	// avoid the need for a span frontier, but would also introduce a different
	// contention pattern and use additional goroutines. it's not clear which
	// solution is best without targeted performance testing, so we're choosing
	// the faster-to-implement solution for now.
	frontier := span.MakeFrontier(spans...)

	rangeFeedStartTS := lastHighwater
	for _, span := range p.spans {
		span := span
		frontier.Forward(span, rangeFeedStartTS)
		_, withDiff := p.details.Opts[optDiff]
		g.GoCtx(func(ctx context.Context) error {
			return ds.RangeFeed(ctx, span, rangeFeedStartTS, withDiff, eventC)
		})
	}
	g.GoCtx(func(ctx context.Context) error {
		for {
			select {
			case e := <-eventC:
				switch t := e.GetValue().(type) {
				case *roachpb.RangeFeedValue:
					kv := roachpb.KeyValue{Key: t.Key, Value: t.Value}
					var prevVal roachpb.Value
					if withDiff {
						prevVal = t.PrevValue
					}
					if err := memBuf.AddKV(ctx, kv, prevVal); err != nil {
						return err
					}
				case *roachpb.RangeFeedCheckpoint:
					if !t.ResolvedTS.IsEmpty() && t.ResolvedTS.Less(rangeFeedStartTS) {
						// RangeFeed happily forwards any closed timestamps it receives as
						// soon as there are no outstanding intents under them.
						// Changefeeds don't care about these at all, so throw them out.
						continue
					}
					if err := memBuf.AddResolved(ctx, t.Span, t.ResolvedTS); err != nil {
						return err
					}
				default:
					log.Fatalf(ctx, "unexpected RangeFeedEvent variant %v", t)
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	// scanBoundary represents an event on which the rangefeeds should be stopped
	// for a backfill or due to failure.
	var scanBoundary *tablefeed.Event
	g.GoCtx(func(ctx context.Context) error {
		var (
			checkForScanBoundary = func(ts hlc.Timestamp) error {
				if scanBoundary != nil {
					return nil
				}
				nextEvents, err := p.tableFeed.Peek(ctx, ts)
				if err != nil {
					return err
				}
				// TODO(ajwerner): check the policy for table events
				if len(nextEvents) > 0 {
					scanBoundary = &nextEvents[0]
				}
				return nil
			}
			applyScanBoundary = func(e bufferEntry) (skipEvent, reachedBoundary bool) {
				if scanBoundary == nil {
					return false, false
				}
				if bufferEntryTimestamp(e).Less(scanBoundary.Timestamp()) {
					return false, false
				}
				if isKV := e.kv.Key != nil; isKV {
					return true, false
				}
				boundaryResolvedTimestamp := scanBoundary.Timestamp().Prev()
				if e.resolved.Timestamp.LessEq(boundaryResolvedTimestamp) {
					return false, false
				}
				frontier.Forward(e.resolved.Span, boundaryResolvedTimestamp)
				return true, frontier.Frontier() == boundaryResolvedTimestamp
			}
			addEntry = func(e bufferEntry) error {
				if isKV := e.kv.Key != nil; isKV {
					return p.buf.AddKV(ctx, e.kv, e.prevVal, e.backfillTimestamp)
				}
				// TODO(ajwerner): technically this doesn't need to happen for most
				// events - we just need to make sure we forward for events which are
				// at scanBoundary.Prev(). The logic currently doesn't make this clean.
				frontier.Forward(e.resolved.Span, e.resolved.Timestamp)
				return p.buf.AddResolved(ctx, e.resolved.Span, e.resolved.Timestamp)
			}
		)
		for {
			e, err := memBuf.Get(ctx)
			if err != nil {
				return err
			}
			// Check for scanBoundary
			if err := checkForScanBoundary(bufferEntryTimestamp(e)); err != nil {
				return err
			}
			skipEntry, scanBoundaryReached := applyScanBoundary(e)
			if scanBoundaryReached {
				// All component rangefeeds are now at the boundary.
				// Break out of the ctxgroup by returning a sentinel error.
				return errBoundaryReached
			}
			if skipEntry {
				continue
			}
			if err := addEntry(e); err != nil {
				return err
			}
		}
	})
	// TODO(mrtracy): We are currently tearing down the entire rangefeed set in
	// order to perform a scan; however, given that we have an intermediate
	// buffer, its seems that we could do this without having to destroy and
	// recreate the rangefeeds.
	if err := g.Wait(); err != nil && err != errBoundaryReached {
		return err
	}
	newHighWater := scanBoundary.Timestamp().Prev()
	for _, sp := range spans {
		frontier.Forward(sp, newHighWater)
	}

	p.mu.Lock()
	p.mu.highWater = newHighWater
	p.mu.Unlock()

	// TODO(ajwerner): iterate the spans and add a resolved timestamp at
	// spanBoundary.Prev().

	// Then, check the setting to return early.
	// Need to fix the spanBoundaries to detect column renames.
	// Need to also break on IMPORT INTO
	// Essentially this scanBoundary needs some better abstraction.

	return nil
}

func bufferEntryTimestamp(e bufferEntry) hlc.Timestamp {
	if e.kv.Key != nil {
		return e.kv.Value.Timestamp
	}
	return e.resolved.Timestamp
}

func getSpansToProcess(
	ctx context.Context, db *client.DB, targetSpans []roachpb.Span,
) ([]roachpb.Span, error) {
	var ranges []roachpb.RangeDescriptor
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		ranges, err = allRangeDescriptors(ctx, txn)
		return err
	}); err != nil {
		return nil, errors.Wrapf(err, "fetching range descriptors")
	}

	type spanMarker struct{}
	type rangeMarker struct{}

	var spanCovering covering.Covering
	for _, span := range targetSpans {
		spanCovering = append(spanCovering, covering.Range{
			Start:   []byte(span.Key),
			End:     []byte(span.EndKey),
			Payload: spanMarker{},
		})
	}

	var rangeCovering covering.Covering
	for _, rangeDesc := range ranges {
		rangeCovering = append(rangeCovering, covering.Range{
			Start:   []byte(rangeDesc.StartKey),
			End:     []byte(rangeDesc.EndKey),
			Payload: rangeMarker{},
		})
	}

	chunks := covering.OverlapCoveringMerge(
		[]covering.Covering{spanCovering, rangeCovering},
	)

	var requests []roachpb.Span
	for _, chunk := range chunks {
		if _, ok := chunk.Payload.([]interface{})[0].(spanMarker); !ok {
			continue
		}
		requests = append(requests, roachpb.Span{Key: chunk.Start, EndKey: chunk.End})
	}
	return requests, nil
}

func (p *poller) exportSpansParallel(
	ctx context.Context, spans []roachpb.Span, ts hlc.Timestamp, withDiff bool,
) error {
	//if log.V(2) {
	log.Infof(ctx, "performing scan on %v at %v withDiff %v", spans, ts, withDiff)
	//}

	// Export requests for the various watched spans are executed in parallel,
	// with a semaphore-enforced limit based on a cluster setting.
	// The spans here generally correspond with range boundaries.
	maxConcurrentExports := clusterNodeCount(p.gossip) *
		int(storage.ExportRequestsLimit.Get(&p.settings.SV))
	exportsSem := make(chan struct{}, maxConcurrentExports)
	g := ctxgroup.WithContext(ctx)

	// atomicFinished is used only to enhance debugging messages.
	var atomicFinished int64

	for _, span := range spans {
		span := span

		// Wait for our semaphore.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case exportsSem <- struct{}{}:
		}

		g.GoCtx(func(ctx context.Context) error {
			defer func() { <-exportsSem }()

			err := p.exportSpan(ctx, span, ts, withDiff)
			finished := atomic.AddInt64(&atomicFinished, 1)
			if log.V(2) {
				log.Infof(ctx, `exported %d of %d: %v`, finished, len(spans), err)
			}
			if err != nil {
				return err
			}
			return nil
		})
	}
	return g.Wait()
}

func (p *poller) exportSpan(
	ctx context.Context, span roachpb.Span, ts hlc.Timestamp, withDiff bool,
) error {
	txn := p.db.NewTxn(ctx, "changefeed backfill")
	if log.V(2) {
		log.Infof(ctx, `sending ScanRequest %s at %s`, span, ts)
	}
	txn.SetFixedTimestamp(ctx, ts)
	stopwatchStart := timeutil.Now()
	var scanDuration, bufferDuration time.Duration
	// TODO(ajwerner): Adopt the byte limit here as soon as it merges. This must
	// happen in 20.1!
	const maxKeysPerScan = 1 << 18
	for remaining := span; ; {
		start := timeutil.Now()
		b := txn.NewBatch()
		r := roachpb.NewScan(remaining.Key, remaining.EndKey).(*roachpb.ScanRequest)
		r.ScanFormat = roachpb.BATCH_RESPONSE
		b.Header.MaxSpanRequestKeys = maxKeysPerScan
		// NB: We use a raw request rather than the Scan() method because we want
		// the MVCC timestamps which are encoded in the response but are filtered
		// during result parsing.
		b.AddRawRequest(r)
		if err := txn.Run(ctx, b); err != nil {
			return errors.Wrapf(err, `fetching changes for %s`, span)
		}
		afterScan := timeutil.Now()
		res := b.RawResponse().Responses[0].GetScan()
		if err := p.slurpScanResponse(ctx, remaining, res, ts, withDiff); err != nil {
			return err
		}
		afterBuffer := timeutil.Now()
		scanDuration += afterScan.Sub(start)
		bufferDuration += afterBuffer.Sub(afterScan)
		if res.ResumeSpan != nil {
			remaining = *res.ResumeSpan
		} else {
			break
		}
	}
	p.metrics.PollRequestNanosHist.RecordValue(scanDuration.Nanoseconds())
	if err := p.buf.AddResolved(ctx, span, ts); err != nil {
		return err
	}
	if log.V(2) {
		log.Infof(ctx, `finished Scan of %s at %s took %s`,
			span, ts.AsOfSystemTime(), timeutil.Since(stopwatchStart))
	}
	return nil
}

// slurpScanResponse iterates the ScanResponse and inserts the contained kvs into
// the poller's buffer.
func (p *poller) slurpScanResponse(
	ctx context.Context,
	span roachpb.Span,
	res *roachpb.ScanResponse,
	ts hlc.Timestamp,
	withDiff bool,
) error {
	for _, br := range res.BatchResponses {
		for len(br) > 0 {
			var kv roachpb.KeyValue
			var err error
			kv.Key, kv.Value.Timestamp, kv.Value.RawBytes, br, err = enginepb.ScanDecodeKeyValue(br)
			if err != nil {
				return errors.Wrapf(err, `decoding changes for %s`, span)
			}
			var prevVal roachpb.Value
			if withDiff {
				// Include the same value for the "before" and "after" KV, but
				// interpret them at different timestamp. Specifically, interpret
				// the "before" KV at the timestamp immediately before the schema
				// change. This is handled in kvsToRows.
				prevVal = kv.Value
			}
			if err = p.buf.AddKV(ctx, kv, prevVal, ts); err != nil {
				return errors.Wrapf(err, `buffering changes for %s`, span)
			}
		}
	}
	return nil
}

func allRangeDescriptors(ctx context.Context, txn *client.Txn) ([]roachpb.RangeDescriptor, error) {
	rows, err := txn.Scan(ctx, keys.Meta2Prefix, keys.MetaMax, 0)
	if err != nil {
		return nil, err
	}

	rangeDescs := make([]roachpb.RangeDescriptor, len(rows))
	for i, row := range rows {
		if err := row.ValueProto(&rangeDescs[i]); err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err,
				"%s: unable to unmarshal range descriptor", row.Key)
		}
	}
	return rangeDescs, nil
}

// clusterNodeCount returns the approximate number of nodes in the cluster.
func clusterNodeCount(g *gossip.Gossip) int {
	var nodes int
	_ = g.IterateInfos(gossip.KeyNodeIDPrefix, func(_ string, _ gossip.Info) error {
		nodes++
		return nil
	})
	return nodes
}

func shouldAddScanBoundary(e tablefeed.Event) (res bool) {
	return newColumnBackfillComplete(e) ||
		hasNewColumnDropBackfillMutation(e)
}

func hasNewColumnDropBackfillMutation(e tablefeed.Event) (res bool) {
	dropMutationExists := func(desc *sqlbase.TableDescriptor) bool {
		for _, m := range desc.Mutations {
			if m.Direction == sqlbase.DescriptorMutation_DROP &&
				m.State == sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY {
				return true
			}
		}
		return false
	}
	// Make sure that the old descriptor *doesn't* have the same mutation to avoid adding
	// the same scan boundary more than once.
	return !dropMutationExists(e.Before) && dropMutationExists(e.After)
}

func newColumnBackfillComplete(e tablefeed.Event) (res bool) {
	return len(e.Before.Columns) < len(e.After.Columns) &&
		e.Before.HasColumnBackfillMutation() && !e.After.HasColumnBackfillMutation()
}

func fetchSpansForTargets(
	ctx context.Context, db *client.DB, targets jobspb.ChangefeedTargets, ts hlc.Timestamp,
) ([]roachpb.Span, error) {
	var spans []roachpb.Span
	err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		spans = nil
		txn.SetFixedTimestamp(ctx, ts)
		// Note that all targets are currently guaranteed to be tables.
		for tableID := range targets {
			tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, tableID)
			if err != nil {
				return err
			}
			spans = append(spans, tableDesc.PrimaryIndexSpan())
		}
		return nil
	})
	return spans, err
}
