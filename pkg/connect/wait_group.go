// Copyright 2019 The Cockroach Authors.
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

package connect

import "sync"

// WaitGroup works like the sync.WaitGroup struct, except that it synchronizes
// using a syn lock rather than using atomic operations. Callers must acquire
// the syn lock before calling methods. The WaitGroup will release the syn lock
// before blocking. See the Synchronization section in the connect package
// documentation for more details.
type WaitGroup struct {
	counter int
	waiters int
	wait    sync.Cond
}

// Init provides the syn lock that must be released before waiting, and then re-
// acquired before proceeding.
func (w *WaitGroup) Init(syn sync.Locker) {
	w.wait.L = syn
}

// Add adds delta, which may be negative, to the WaitGroup counter. If the
// counter becomes zero, all goroutines blocked on Wait are released. If the
// counter goes negative, Add panics.
func (w *WaitGroup) Add(delta int) {
	if w.counter < 0 {
		panic("WaitGroup counter cannot go negative")
	}
	w.counter += delta
}

// Done decrements the WaitGroup counter by one. If the counter becomes zero,
// then any goroutines blocked on Wait will be signaled.
func (w *WaitGroup) Done() {
	if w.counter <= 0 {
		panic("Monitor exited too many times")
	}
	w.counter--
	if w.counter == 0 && w.waiters != 0 {
		// Signal waiters that counter hit zero.
		w.wait.Broadcast()
	}
}

// Wait blocks until the WaitGroup counter is zero.
func (w *WaitGroup) Wait() {
	for w.counter > 0 {
		w.waiters++
		w.wait.Wait()
		w.waiters--
	}
}
