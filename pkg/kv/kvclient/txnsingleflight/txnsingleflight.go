package txnsingleflight

import (
	"context"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

type Config struct {
	OpName  string
	TagName string
	DB      isql.DB
	Time    timeutil.TimeSource
	Stopper *stop.Stopper
}

func NewGroup(cfg Config) *Group {
	g := &Group{
		cfg:          cfg,
		g:            singleflight.NewGroup(cfg.OpName, cfg.TagName),
		pushTaskName: cfg.OpName + " push",
	}
	g.mu.pending = make(map[string]*pending)
	return g
}

type Group struct {
	cfg Config
	g   *singleflight.Group
	mu  struct {
		syncutil.Mutex
		pending map[string]*pending
	}
	pushTaskName string
}

// pending corresponds to an in-flight group.
type pending struct {
	key roachpb.Key
	mu  struct {
		syncutil.Mutex

		// txn is the current transaction of the singleflight leader.
		// This transaction will always have an anchor key corresponding to the
		// key of the group. The txn will never be nil, but it may change over
		// time.
		txn     *kv.Txn
		waiters map[*kv.Txn]struct{}
	}
}

type DoFn func(context.Context, isql.Txn) (any, error)

// TODO(ajwerner): Move this constant to be a cluster setting.
const pushTimeout = 100 * time.Millisecond

type Future struct {
	singleflight.Future

	exited     chan struct{}
	cancelPush context.CancelFunc
}

func (f Future) Reset() {
	f.cancelPush()
	<-f.exited
	f.Future.Reset()
}

func (g *Group) DoChan(
	ctx context.Context, key roachpb.Key, blockedTxn *kv.Txn, fn DoFn, options ...isql.Option,
) (_ Future, leader bool) {

	// The basic design is that we're going to wait some time interval,
	// and if the transaction has not returned, we're going to push that
	// transaction as a way of installing an edge in the dependency graph.
	keyStr := key.String()
	g.mu.Lock()
	p := g.getOrCreatePendingLocked(key)
	var f singleflight.Future
	f, leader = g.g.DoChan(ctx, keyStr, singleflight.DoOpts{
		Stop:               g.cfg.Stopper,
		InheritCancelation: true,
	}, func(
		ctx context.Context,
	) (any, error) {
		defer func() { g.removePending(p) }()
		var unlocked bool
		unlock := func() {
			if !unlocked {
				unlocked = true
				g.mu.Unlock()
			}
		}
		setGroupTxn := func(txn *kv.Txn) {
			defer unlock()
			if !unlocked {
				g.setGroupTxnLocked(keyStr, txn)
			} else { // deal with restarts
				g.setGroupTxn(keyStr, txn)
			}
		}
		defer unlock()
		var ret any
		if err := g.cfg.DB.Txn(ctx, func(ctx context.Context, inner isql.Txn) (err error) {
			ret = nil
			if err = inner.KV().Sender().SetAnchor(ctx, key); err != nil {
				return err
			}
			log.Infof(ctx, "anchor set to asdf %v %v", inner, key)
			setGroupTxn(inner.KV())
			ret, err = fn(ctx, inner)
			// We want to deal with a case where we get a restart error.
			// Namely, we want to make sure that we've ratcheted our priority above
			// that of our waiters. If we don't, and there's a cycle, and this
			// transaction is not the maximum priority, it can get caught in a loop.
			// The primary hazard is that there's no guarantee that the transaction
			// will have a transaction record, so it can get aborted via the timestamp
			// cache. When the deadlock is broken via the timestamp cache, we don't
			// observe any ratcheting of the priority. Even if we did, we assume that
			// breaking the lock will remove the edge. In practice, these deadlocks
			// are very rare.
			//
			// As such, we're going to always ratchet above all waiters only on
			// restarts.
			return err
		}); err != nil {
			return nil, err
		}
		return ret, nil
	})
	if !leader {
		g.mu.Unlock()
	}
	pushCtx, cancelPush := context.WithCancel(ctx)
	exited := make(chan struct{})
	if err := g.cfg.Stopper.RunAsyncTask(pushCtx, "pusher", func(ctx context.Context) {
		defer close(exited)
		timer := g.cfg.Time.NewTimer()
		defer timer.Stop()
		timer.Reset(pushTimeout)
		pushDoneChan := make(chan error, 1)
		for {
			select {
			case <-f.C():
				return
			case <-ctx.Done():
				return
			case err := <-pushDoneChan:
				// TODO(ajwerner): Decide what to do if there's a non-nil error.
				// If there's a nil error, it doesn't mean we've succeeded; it could
				// be that the transaction internally got aborted. In any case, kick
				// off another timer.

				// This might inject more delay than one might hope for, but it shouldn't
				// be wrong.
				if err != nil && log.ExpensiveLogEnabled(ctx, 1) {
					log.VEventf(ctx, 1, "failed to push singleflight txn: %v", err)
				} else if log.ExpensiveLogEnabled(ctx, 2) {
					log.VEventf(ctx, 1, "successfully pushed singleflight txn")
				}
				timer.Reset(pushTimeout)
			case <-timer.Ch():
				timer.MarkRead()
				// Ignore the error. If we're shutting down, fine, we don't care
				// about holding off a deadlock.
				toPush := g.getGroupTxn(keyStr)
				// The only case there can be a nil transaction is if the task
				// we had joined finished.
				if toPush == nil {
					timer.Reset(pushTimeout)
					continue
				}
				// TODO(ajwerner): Is there a hazard whereby the lease transaction will
				// get aborted for some reason, but we'll not notice it because it was
				// a read-only transaction and then it restarts and we have to wait for
				// the transaction to expire and get aborted? Is that hazard so bad? It
				// would mean a transaction expiration duration of waiting in the async
				// function. In an extreme edge case, it could also mean that we don't
				// push again or detect a deadlock for an expiration duration.
				blockOnCtx, span := tracing.ForkSpan(ctx, "BlockOn")
				if err := g.cfg.Stopper.RunAsyncTask(ctx, g.pushTaskName, func(ctx context.Context) {
					// This cannot block; there's only one running DBPusher goroutine
					// at a time, and the channel has a buffer of size 1.
					defer span.Finish()
					// The core challenge here is that there's nothing to ratchet the priority
					// of the
					pushDoneChan <- blockedTxn.Sender().BlockOn(blockOnCtx, toPush.Sender())
				}); err != nil {
					span.Finish()
				}
			}
		}
	}); err != nil {
		close(exited)
	}
	return Future{
		Future:     f,
		exited:     exited,
		cancelPush: cancelPush,
	}, leader
}

func (g *Group) unsetGroupTxn(key string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.mu.txns, key)
}

func (g *Group) setGroupTxn(key string, txn *kv.Txn) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.setGroupTxnLocked(key, txn)
}

func (g *Group) setGroupTxnLocked(key string, txn *kv.Txn) {
	g.mu.AssertHeld()
	g.mu.txns[key] = txn
}

func (g *Group) getGroupTxn(key string) *kv.Txn {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.mu.txns[key]
}

func (g *Group) getOrCreateLockedPending(key roachpb.Key) *pending {
	g.mu.Lock()
	defer g.mu.Unlock()
	keyStr := *(*string)(unsafe.Pointer(&key))
	p, ok := g.mu.pending[keyStr]
	if !ok {
		p = &pending{}
		p.key = key
		p.mu.waiters = make(map[*kv.Txn]struct{})
		g.mu.pending[keyStr] = p
	}
	p.mu.Lock()
	return p
}
