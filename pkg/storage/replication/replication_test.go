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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func (c groupCache) Add(ents []raftpb.Entry, truncate bool) {
	c.c.Add(int64(c.id), ents, truncate)
}
func (c groupCache) Clear(hi uint64) { c.c.Clear(int64(c.id), hi) }
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
	cfg.Settings = cluster.MakeTestingClusterSettings()
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
	cfg2 := newTestConfig()
	defer cfg.Stopper.Stop(ctx)

	// Okay I guess what I should do is erm uh create a few of these factories
	// and then get them all hooked up and try to replicate some data	between
	// those for a single group. This is going to be a whole thing.
	// Open questions about how much bootstrap is needed and how to mirror the
	// replica tracking on a per group basis. Like who actually intercepts things
	// and adds replicas and what not. Seems like there's a bunch of complexity
	// in that.

	// Also, snapshots :facepalm: will need to figure those out too.
	// This feels like a bunch of work. I think I will rejoice when I have a
	// single group doing that replication and then see where I'm at.
	// That sounds laudable.

	pf, err := replication.NewFactory(ctx, cfg)
	if err != nil {
		panic(err)
	}
	pf2, err := replication.NewFactory(ctx, cfg2)
	p, err := pf.NewPeer(replication.PeerConfig{
		GroupID: 1,
		PeerID:  1,
		Peers:   []replication.PeerID{},
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
	time.Sleep(500 * time.Millisecond)
	cmd := &storagepb.RaftCommand{}
	data, err := proto.Marshal(cmd)
	require.Nil(t, err)
	id := replication.MakeIDKey()
	msg := replication.EncodeRaftCommandV2(id, data)
	pc.Send(ctx, replication.ProposalMessage(msg))
	// we'll need to receive on the raft transport from 1 and send it to two
	raftMsg := cfg.RaftTransport.Recv()
	log.Infof(ctx, "%v", raftMsg)
	cfg2.RaftTransport.Send(ctx, raftMsg)
	_, err = pf2.LoadPeer(log.AmbientContext{}, 1)
	assert.Nil(t, err)
	fmt.Println(pc.Recv())
}
