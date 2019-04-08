package kvtoy

import (
	"github.com/cockroachdb/cockroach/pkg/storage/raftentry"
	"github.com/cockroachdb/cockroach/pkg/storage/replication"
	"go.etcd.io/etcd/raft/raftpb"
)

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

// // type Network struct {
// // 	conns sync.Map
// // }

// // func NewNetwork() *Network {
// // 	return &Network{}
// // }

// // func (n *Network) NewConn(id interface{}) connect.Conn {
// // 	if c, exists := n.conns.Load(id); exists {
// // 		return c.(connect.Conn)
// // 	}
// // 	c := &NetworkedConn{
// // 		id:         id,
// // 		underlying: newManualTransport(),
// // 		network:    n,
// // 	}
// // 	got, _ := n.conns.LoadOrStore(id, c)
// // 	return got.(connect.Conn)
// // }

// // type NetworkMessage interface {
// // 	Addr() interface{}
// // }

// // type NetworkedConn struct {
// // 	id         interface{}
// // 	underlying connect.Conn
// // 	network    *Network
// // }

// // func (s *NetworkedConn) Recv() connect.Message { return s.underlying.Recv() }

// // func (s *NetworkedConn) Close(ctx context.Context, drain bool) {
// // 	s.network.conns.Delete(s.id)
// // 	s.underlying.Close(ctx, drain)
// // }

// // func (s *NetworkedConn) Send(ctx context.Context, msg connect.Message) {
// // 	netMsg, ok := msg.(NetworkMessage)
// // 	if !ok {
// // 		panic(errors.Errorf("message %T is not a %T", msg, NetworkMessage(nil)))
// // 	}
// // 	to := netMsg.Addr()
// // 	if to == s.id {
// // 		s.underlying.Send(ctx, msg)
// // 	}
// // 	toConnValue, ok := s.network.conns.Load(to)
// // 	if !ok {
// // 		log.Warningf(ctx, "message is to an unknown network %v", to)
// // 		return
// // 	}
// // 	toConn, ok := toConnValue.(*NetworkedConn)
// // 	if !ok {
// // 		panic(errors.Errorf("connection is of the wrong type %T", toConnValue))
// // 	}
// // 	toConn.underlying.Send(ctx, msg)
// // }

// // type NetworkConn struct {
// // 	conns []connect.Conn
// // }

// // func (nc NetworkConn) Send(ctx context.Context, msg connect.Message) {
// // 	m := msg.(*kvtoypb.RaftMessageRequest)
// // 	nc.conns[int(m.ToReplica.StoreID-1)].Send(ctx, msg)
// // }

// // type DuplexManualTransport struct {
// // 	SendConn connect.Conn
// // 	RecvConn connect.Conn
// // }

// // func (dmt DuplexManualTransport) Send(ctx context.Context, msg connect.Message) {
// // 	dmt.SendConn.Send(ctx, msg)
// // }

// // func (dmt DuplexManualTransport) Recv() connect.Message {
// // 	return dmt.RecvConn.Recv()
// // }

// // func (dmt DuplexManualTransport) Close(ctx context.Context, drain bool) {
// // 	dmt.SendConn.Close(ctx, drain)
// // 	dmt.RecvConn.Close(ctx, drain)
// // }

// // func newDuplexManualTransport() *DuplexManualTransport {
// // 	return &DuplexManualTransport{
// // 		SendConn: newManualTransport(),
// // 		RecvConn: newManualTransport(),
// // 	}
// // }

// // type manualTransport struct {
// // 	mu            syncutil.Mutex
// // 	sendCond      sync.Cond
// // 	recvCond      sync.Cond
// // 	waitingToRecv bool
// // 	closed        bool
// // 	buf           connect.Message
// // }

// // func (t *manualTransport) Send(ctx context.Context, msg connect.Message) {
// // 	t.mu.Lock()
// // 	defer t.mu.Unlock()
// // 	for t.buf != nil && !t.closed {
// // 		t.sendCond.Wait()
// // 	}
// // 	if t.closed {
// // 		return
// // 	}
// // 	t.buf = msg
// // 	t.recvCond.Signal()
// // }

// // func (t *manualTransport) Close(ctx context.Context, drain bool) {
// // 	// TODO(ajwerner): implement drain
// // 	t.mu.Lock()
// // 	defer t.mu.Unlock()
// // 	if t.closed {
// // 		return
// // 	}
// // 	t.closed = true
// // 	t.sendCond.Broadcast()
// // 	t.recvCond.Broadcast()
// // }

// // func (t *manualTransport) Recv() (msg connect.Message) {
// // 	t.mu.Lock()
// // 	defer t.mu.Unlock()
// // 	for t.buf == nil && !t.closed {
// // 		t.recvCond.Wait()
// // 	}
// // 	if t.closed {
// // 		return nil
// // 	}
// // 	defer t.sendCond.Signal()
// // 	msg, t.buf = t.buf, nil
// // 	return msg
// // }

// // func newManualTransport() *manualTransport {
// // 	mt := &manualTransport{}
// // 	mt.sendCond.L = &mt.mu
// // 	mt.recvCond.L = &mt.mu
// // 	return mt
// // }
