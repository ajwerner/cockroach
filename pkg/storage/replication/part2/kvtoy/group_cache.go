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
