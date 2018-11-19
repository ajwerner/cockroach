package raftentry

import (
	"container/list"
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/andy-kimball/arenaskl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/raftpb"
)

const pageSize = 128 << 10 // 128 kB

var arenaPool = &sync.Pool{
	New: func() interface{} {
		return arenaskl.NewArena(pageSize)
	},
}

var pagePool = &sync.Pool{
	New: func() interface{} {
		return &page{}
	},
}

// let's have a cache that has a global
type Cache struct {
	size     uint64
	bytes    uint64
	maxBytes uint64

	mu struct {
		syncutil.RWMutex
		parts map[roachpb.RangeID]partition
		lru   list.List
	}
	// TODO: metrics
}

func NewCache(maxBytes uint64) *Cache {
	var c Cache
	c.mu.parts = map[roachpb.RangeID]partition{}
	c.maxBytes = maxBytes
	return &c
}

func (c *Cache) Add(r roachpb.RangeID, ents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}
	totalSize := entriesSize(ents)
	if totalSize > c.maxBytes {
		panic(errors.Errorf("Cannot cache %d bytes, greater than max cache size of %d",
			totalSize, c.maxBytes))
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if atomic.AddUint64(&c.bytes, totalSize) > c.maxBytes {
		c.mu.RUnlock()
		c.rotatePages()
		c.mu.RLock()
	}
	for len(ents) > 0 {
		if i, done := c.add(r, ents); done {
			break
		} else {
			ents = ents[i+1:]
			c.mu.RUnlock()
			c.allocPage(r)
			c.mu.RLock()
		}
	}
}

// add adds ent to the cache
// If done is false, i represents the next index in ents which needs to be added.
func (c *Cache) add(r roachpb.RangeID, ents []raftpb.Entry) (next int, done bool) {
	part, ok := c.mu.parts[r]
	if !ok {
		return 0, false
	}
	p := part[len(part)-1]
	for i, e := range ents {
		if !p.add(&e) {
			return i, false
		}
	}
	return len(ents), true
}

const metaInternedData = 1
const dataThreshold = pageSize / 64
const encodedEntrySize = 8 /* Term */ + 4 /* Type */ + 4 /* Offset */ + 4 /* Size */

func (p *page) add(ent *raftpb.Entry) (ok bool) {
	var key [8]byte /* Index */
	binary.BigEndian.PutUint64(key[:], ent.Index)
	val, meta := makeVal(ent)
	var it arenaskl.Iterator
	it.Init(p.skl)
	if meta&metaInternedData != 0 {
		p.dataSet.mu.Lock()
		defer p.dataSet.mu.Unlock()
	}
	err := it.Add(key[:], val, meta); err != nil {
		return false
	}
	for err != nil {	
		switch err {
		case arenaskl.ErrArenaFull:
			return false
		case arenaskl.ErrRecordExists:
			delta := len(it.Value()) - len(val)
			
		}
	}
	
	if p.dataSet.mu.data == nil {
		p.dataSet.mu.data = map[termIndex][]byte{
			termIndex{term: ent.Term, index: ent.Index}: ent.Data,
		}
	} else {
		p.dataSet.mu.data[termIndex{term: ent.Term, index: ent.Index}] = ent.Data
	}
	return true
}

func makeVal(ent *raftpb.Entry) (val []byte, meta uint16) {
	const headerSize = 8 /* Term */ + 4 /* Type */
	valSize := headerSize
	aboveThreshold := len(ent.Data) > dataThreshold
	if !aboveThreshold {
		valSize += len(ent.Data)
	}
	val = make([]byte, valSize)
	if !aboveThreshold {
		copy(val[12:], ent.Data)
	}
	binary.LittleEndian.PutUint64(val[0:8], ent.Term)
	binary.LittleEndian.PutUint32(val[8:12], uint32(ent.Type))
	return
}

func newPage(id roachpb.RangeID) *page {
	arena := arenaPool.Get().(*arenaskl.Arena)
	arena.Reset()
	p := pagePool.Get().(*page)
	*p = page{
		id:    id,
		arena: arena,
		skl:   arenaskl.NewSkiplist(arena),
	}
	return p
}

func (c *Cache) allocPage(id roachpb.RangeID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := newPage(id)
	c.mu.lru.PushFront(p)
	c.mu.parts[id] = append(c.mu.parts[id], p)
}

func (c *Cache) rotatePages() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for atomic.LoadUint64(&c.bytes) > c.maxBytes {
		c.rotatePageLocked()
	}
}

func (c *Cache) rotatePageLocked() {
	p := c.mu.lru.Remove(c.mu.lru.Back()).(*page)
	c.removePageLocked(p)
	atomic.AddUint64(&p.bytes, ^uint64(int64(p.bytes)-1))
	arenaPool.Put(p.arena)
	p.id, p.arena, p.skl, p.bytes = 0, nil, nil, 0
	pagePool.Put(p)
}

func (c *Cache) removePageLocked(p *page) {
	// TODO: do we always just remove the first page?
	part := c.mu.parts[p.id]
	for i, pi := range part {
		if pi == p {
			c.mu.parts[p.id] = append(part[:i], part[i+1:]...)
			if len(c.mu.parts[p.id]) == 0 {
				delete(c.mu.parts, p.id)
			}
			return
		}
	}
	panic("didn't find page")
}

// partition corresponds to a single range ID
// it has potentially multiple pages.
// TODO: are these pages strictly non-overlapping?
// if not, does it dramatically overlap the data structure?
type partition []*page

type page struct {
	id roachpb.RangeID
	dataSet
	arena *arenaskl.Arena
	skl   *arenaskl.Skiplist
	bytes uint64
}

type termIndex struct {
	term  uint64
	index uint64
}

type dataSet struct {
	mu struct {
		sync.RWMutex
		data map[termIndex][]byte
	}
}

// computes the total byte size of ents
func entriesSize(ents []raftpb.Entry) (size uint64) {
	for _, e := range ents {
		size += uint64(e.Size())
	}
	return
}
