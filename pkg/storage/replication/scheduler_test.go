// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package replication

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
)

func newScheduler(processor eventProcessor, numWorkers int) *scheduler {
	var s scheduler
	initScheduler(&s, processor, numWorkers)
	return &s
}

func TestRangeIDChunk(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var c groupIDChunk
	if c.Len() != 0 {
		t.Fatalf("expected empty chunk, but found %d", c.Len())
	}
	if c.WriteCap() != groupIDChunkSize {
		t.Fatalf("expected %d, but found %d", groupIDChunkSize, c.WriteCap())
	}
	if _, ok := c.PopFront(); ok {
		t.Fatalf("successfully popped from empty chunk")
	}

	for i := 1; i <= groupIDChunkSize; i++ {
		if !c.PushBack(GroupID(i)) {
			t.Fatalf("%d: failed to push", i)
		}
		if e := i; e != c.Len() {
			t.Fatalf("expected %d, but found %d", e, c.Len())
		}
		if e := groupIDChunkSize - i; e != c.WriteCap() {
			t.Fatalf("expected %d, but found %d", e, c.WriteCap())
		}
	}
	if c.PushBack(0) {
		t.Fatalf("successfully pushed to full chunk")
	}

	for i := 1; i <= groupIDChunkSize; i++ {
		id, ok := c.PopFront()
		if !ok {
			t.Fatalf("%d: failed to pop", i)
		}
		if GroupID(i) != id {
			t.Fatalf("expected %d, but found %d", i, id)
		}
		if e := groupIDChunkSize - i; e != c.Len() {
			t.Fatalf("expected %d, but found %d", e, c.Len())
		}
		if c.WriteCap() != 0 {
			t.Fatalf("expected full chunk, but found %d", c.WriteCap())
		}
	}
	if c.Len() != 0 {
		t.Fatalf("expected empty chunk, but found %d", c.Len())
	}
	if c.WriteCap() != 0 {
		t.Fatalf("expected full chunk, but found %d", c.WriteCap())
	}
	if _, ok := c.PopFront(); ok {
		t.Fatalf("successfully popped from empty chunk")
	}
}

func TestRangeIDQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var q groupIDQueue
	if q.Len() != 0 {
		t.Fatalf("expected empty queue, but found %d", q.Len())
	}
	if _, ok := q.PopFront(); ok {
		t.Fatalf("successfully popped from empty queue")
	}

	const count = 3 * groupIDChunkSize
	for i := 1; i <= count; i++ {
		q.PushBack(GroupID(i))
		if e := i; e != q.Len() {
			t.Fatalf("expected %d, but found %d", e, q.Len())
		}
	}

	for i := 1; i <= count; i++ {
		id, ok := q.PopFront()
		if !ok {
			t.Fatalf("%d: failed to pop", i)
		}
		if GroupID(i) != id {
			t.Fatalf("expected %d, but found %d", i, id)
		}
		if e := count - i; e != q.Len() {
			t.Fatalf("expected %d, but found %d", e, q.Len())
		}
	}
	if q.Len() != 0 {
		t.Fatalf("expected empty queue, but found %d", q.Len())
	}
	if _, ok := q.PopFront(); ok {
		t.Fatalf("successfully popped from empty queue")
	}
}

type testProcessor struct {
	mu struct {
		syncutil.Mutex
		raftReady   map[GroupID]int
		raftRequest map[GroupID]int
		raftTick    map[GroupID]int
	}
}

func newTestProcessor() *testProcessor {
	p := &testProcessor{}
	p.mu.raftReady = make(map[GroupID]int)
	p.mu.raftRequest = make(map[GroupID]int)
	p.mu.raftTick = make(map[GroupID]int)
	return p
}

func (p *testProcessor) processReady(_ context.Context, groupID GroupID) {
	p.mu.Lock()
	p.mu.raftReady[groupID]++
	p.mu.Unlock()
}

func (p *testProcessor) processRequestQueue(_ context.Context, groupID GroupID) bool {
	p.mu.Lock()
	p.mu.raftRequest[groupID]++
	p.mu.Unlock()
	return true
}

func (p *testProcessor) processTick(_ context.Context, groupID GroupID) bool {
	p.mu.Lock()
	p.mu.raftTick[groupID]++
	p.mu.Unlock()
	return false
}

// GroupIDSlice implements sort.Interface.
type GroupIDSlice []GroupID

func (r GroupIDSlice) Len() int           { return len(r) }
func (r GroupIDSlice) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r GroupIDSlice) Less(i, j int) bool { return r[i] < r[j] }

func (p *testProcessor) countsLocked(m map[GroupID]int) string {
	var ids GroupIDSlice
	for id := range m {
		ids = append(ids, id)
	}
	sort.Sort(ids)
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "[")
	for i, id := range ids {
		if i > 0 {
			fmt.Fprintf(&buf, ",")
		}
		fmt.Fprintf(&buf, "%d:%d", id, m[id])
	}
	fmt.Fprintf(&buf, "]")
	return buf.String()
}

func (p *testProcessor) String() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return fmt.Sprintf("ready=%s request=%s tick=%s",
		p.countsLocked(p.mu.raftReady),
		p.countsLocked(p.mu.raftRequest),
		p.countsLocked(p.mu.raftTick))
}

// Verify that enqueuing more ranges than the number of workers correctly
// processes all of the ranges. This exercises a code path that was buggy
// during development.
func TestSchedulerLoop(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := newTestProcessor()
	s := newScheduler(p, 1)
	stopper := stop.NewStopper()
	ctx := context.TODO()
	defer stopper.Stop(ctx)
	s.Start(ctx, stopper)
	s.EnqueueRaftTick(1, 2, 3)

	testutils.SucceedsSoon(t, func() error {
		const expected = "ready=[] request=[] tick=[1:1,2:1,3:1]"
		if s := p.String(); expected != s {
			return errors.Errorf("expected %s, but got %s", expected, s)
		}
		return nil
	})
}

// Verify that when we enqueue the same range multiple times for the same
// reason, it is only processed once.
func TestSchedulerBuffering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := newTestProcessor()
	s := newScheduler(p, 1)
	stopper := stop.NewStopper()
	ctx := context.TODO()
	defer stopper.Stop(ctx)
	s.Start(ctx, stopper)

	testCases := []struct {
		state    scheduleState
		expected string
	}{
		{stateRaftReady, "ready=[1:1] request=[] tick=[]"},
		{stateRaftRequest, "ready=[1:1] request=[1:1] tick=[]"},
		{stateRaftTick, "ready=[1:1] request=[1:1] tick=[1:1]"},
		{stateRaftReady | stateRaftRequest | stateRaftTick, "ready=[1:2] request=[1:2] tick=[1:2]"},
	}

	for _, c := range testCases {
		s.signal(s.enqueueN(c.state, 1, 1, 1, 1, 1))

		testutils.SucceedsSoon(t, func() error {
			if s := p.String(); c.expected != s {
				return errors.Errorf("expected %s, but got %s", c.expected, s)
			}
			return nil
		})
	}
}
