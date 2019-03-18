package replication_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/connect"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/replication"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

func TestReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// We want to construct a replication layer and then use it to replicate
	// some data.
	ctx := context.Background()
	cfg := base.RaftConfig{}
	cfg.SetDefaults()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	mt := newManualTransport()
	pf, err := replication.NewPeerFactory(ctx, cfg, stopper, mt, nil, nil)
	if err != nil {
		panic(err)
	}
	p, err := pf.NewPeer(log.AmbientContext{}, 1)
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
