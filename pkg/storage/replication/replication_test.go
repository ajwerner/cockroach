package replication_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/connect"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/raftentry"
	"github.com/cockroachdb/cockroach/pkg/storage/replication"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"go.etcd.io/etcd/raft/raftpb"
)

type manualTransport struct {
	mu            syncutil.Mutex
	cond          sync.Cond
	waitingToRecv bool
	closed        bool
	buf           connect.Message
}

func (t *manualTransport) Send(ctx context.Context, msg connect.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for t.buf != nil || t.closed {
		t.cond.Wait()
	}
	if !t.closed {
		t.buf = msg
	}
}

func (t *manualTransport) Close(ctx context.Context, drain bool) {
	// TODO(ajwerner): implement drain
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	t.closed = true
	defer t.cond.Broadcast()
}

func (t *manualTransport) Recv() (msg connect.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for t.buf == nil || t.closed {
		t.cond.Wait()
	}
	if t.closed {
		return nil
	}
	defer t.cond.Broadcast()
	msg, t.buf = t.buf, nil
	return msg
}

func newManualTransport() *manualTransport {
	mt := &manualTransport{}
	mt.cond.L = &mt.mu
	return mt
}

type groupCache struct {
	c  *raftentry.Cache
	id replication.GroupID
}

func (c groupCache) Add(ents []raftpb.Entry) { c.c.Add(int64(c.id), ents) }
func (c groupCache) Clear(hi uint64)         { c.c.Clear(int64(c.id), hi) }
func (c groupCache) Get(idx uint64) (e raftpb.Entry, ok bool) {
	return c.c.Get(int64(c.id), idx)
}
func (c groupCache) Scan(
	buf []raftpb.Entry, lo, hi, maxBytes uint64,
) (_ []raftpb.Entry, bytes, nextIdx uint64, exceededMaxBytes bool) {
	return c.c.Scan(buf, int64(c.id), lo, hi, maxBytes)
}

func newTestConfig() replication.FactoryConfig {
	var cfg replication.FactoryConfig
	cfg.RaftConfig.SetDefaults()
	cfg.RaftHeartbeatIntervalTicks = 1
	cfg.RaftElectionTimeoutTicks = 3
	cfg.RaftTickInterval = 100 * time.Millisecond
	cfg.Stopper = stop.NewStopper()
	cfg.RaftTransport = newManualTransport()
	entryCache := raftentry.NewCache(1 << 16)
	cfg.EntryCacheFactory = func(id replication.GroupID) replication.EntryCache {
		return groupCache{c: entryCache, id: id}
	}
	cfg.RaftMessageFactory = func() replication.RaftMessage {
		return &raftMessage{}
	}
	cfg.StateLoaderFactory = func(id replication.GroupID) replication.StateLoader {
		return stateloader.Make(roachpb.RangeID(id))
	}
	cfg.EntryScannerFactory = func(id replication.GroupID) replication.EntryReader {
		return func(
			ctx context.Context,
			eng engine.Reader,
			lo, hi uint64,
			f func(raftpb.Entry) (wantMore bool, err error),
		) error {
			var ent raftpb.Entry
			scanFunc := func(kv roachpb.KeyValue) (bool, error) {
				if err := kv.Value.GetProto(&ent); err != nil {
					return false, err
				}
				return f(ent)
			}
			_, err := engine.MVCCIterate(
				ctx, eng,
				keys.RaftLogKey(roachpb.RangeID(id), lo),
				keys.RaftLogKey(roachpb.RangeID(id), hi),
				hlc.Timestamp{},
				engine.MVCCScanOptions{},
				scanFunc,
			)
			return err
		}
	}
	cfg.Storage = engine.NewInMem(roachpb.Attributes{}, 1<<26 /* 64 MB */)
	cfg.NumWorkers = 16
	return cfg
}

type raftMessage struct {
	ID  replication.GroupID
	Msg raftpb.Message
}

func (m *raftMessage) GetGroupID() replication.GroupID   { return m.ID }
func (m *raftMessage) SetGroupID(id replication.GroupID) { m.ID = id }
func (m *raftMessage) GetRaftMessage() raftpb.Message    { return m.Msg }
func (m *raftMessage) SetRaftMessage(msg raftpb.Message) { m.Msg = msg }

func TestReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// We want to construct a replication layer and then use it to replicate
	// some data.
	ctx := context.Background()
	cfg := newTestConfig()
	defer cfg.Stopper.Stop(ctx)
	pf, err := replication.NewFactory(ctx, cfg)
	if err != nil {
		panic(err)
	}
	p, err := pf.NewPeer(replication.PeerConfig{
		GroupID: 1,
		PeerID:  1,
		Peers:   []replication.PeerID{2, 3},
	})
	if err != nil {
		panic(err)
	}
	var l sync.Mutex
	l.Lock()
	pc := p.NewClient(&l)
	pc.Callbacks.Commit = func() error {
		log.Infof(ctx, "in commit callback")
		return nil
	}
	pc.Callbacks.Apply = func(w engine.Writer) error {
		log.Infof(ctx, "in apply callback")
		return nil
	}
	pc.Callbacks.Applied = func() {
		log.Infof(ctx, "in applied callback")
	}
	pc.Send(ctx, &replication.ProposalMessage{
		ID:      "asdfasdf",
		Command: &storagepb.RaftCommand{},
	})
	fmt.Println(pc.Recv())
}
