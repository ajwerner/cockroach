// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

// Package notifyqueue provides an allocation efficient FIFO queue for
// chan struct{}.
//
// Internally efficiency is achieved using a linked-list of ring-buffers
// which utilize a sync.Pool.
package notifyqueue

import "sync"

// bufferSize is the size of the ringBuf buf served from the default pool.
//
// Each node is 32+8*bufferSize bytes so at 28 a node is 256 bytes which
// feels like a nice number.
const bufferSize = 28

// NotifyQueue provides an allocation efficient FIFO queue for chan struct{}.
//
// NotifyQueue is not safe for concurrent use.
type NotifyQueue struct {
	len  int
	pool *pool
	head *node
}

// Initialize initializes a NotifyQueue.
// NotifyQueue values should not be used until they have been initialized.
// It is illegal to initialize a NotifyQueue more than once and this function
// will panic if called with an already initialized NotifyQueue.
func Initialize(q *NotifyQueue) {
	if q.pool != nil {
		panic("cannot re-initialize a NotifyQueue")
	}
	defaultPool.initialize(q)
}

var defaultPool = newPool()

// Enqueue adds c to the end of the queue.
func (q *NotifyQueue) Enqueue(c chan struct{}) {
	if q.head == nil {
		q.head = q.pool.pool.Get().(*node)
		q.head.prev = q.head
		q.head.next = q.head
	}
	tail := q.head.prev
	if !tail.enqueue(c) {
		newTail := q.pool.pool.Get().(*node)
		tail.next = newTail
		q.head.prev = newTail
		newTail.prev = tail
		newTail.next = q.head
		if !newTail.enqueue(c) {
			panic("failed to enqueue into a fresh buffer")
		}
	}
	q.len++
}

// Len returns the current length of the queue.
func (q *NotifyQueue) Len() int {
	return q.len
}

// Dequeue removes the head of the queue and returns it and nil if the queue is
// empty.
func (q *NotifyQueue) Dequeue() (c chan struct{}) {
	if q.head == nil {
		return nil
	}
	ret := q.head.dequeue()
	if q.head.len == 0 {
		oldHead := q.head
		if oldHead.next == oldHead {
			q.head = nil
		} else {
			q.head = oldHead.next
			q.head.prev = oldHead.prev
			q.head.prev.next = q.head
		}
		*oldHead = node{}
		q.pool.pool.Put(oldHead)
	}
	q.len--
	return ret
}

// Peek returns the current head of the queue or nil if the queue is empty.
// It does not modify the queue.
func (q *NotifyQueue) Peek() chan struct{} {
	if q.head == nil {
		return nil
	}
	return q.head.buf[q.head.head]
}

// pool constructs NotifyQueue objects which internally pool their buffers.
type pool struct {
	pool sync.Pool
}

// newPool returns a new Pool which can be used to construct NotifyQueues which
// internally pool their buffers.
func newPool() *pool {
	return &pool{
		pool: sync.Pool{
			New: func() interface{} { return &node{} },
		},
	}
}

// initialize initializes a which will share a sync.Pool of nodes with the
// other NotifyQueue instances initialized with this pool.
func (p *pool) initialize(q *NotifyQueue) {
	*q = NotifyQueue{pool: p}
}

type node struct {
	ringBuf
	prev, next *node
}

type ringBuf struct {
	buf  [bufferSize]chan struct{}
	head int
	len  int
}

func (rb *ringBuf) enqueue(c chan struct{}) (enqueued bool) {
	if rb.len == bufferSize {
		return false
	}
	rb.buf[(rb.head+rb.len)%bufferSize] = c
	rb.len++
	return true
}

func (rb *ringBuf) dequeue() chan struct{} {
	// NB: the NotifyQueue never contains an empty ringBuf.
	if rb.len == 0 {
		panic("cannot dequeue from an empty buffer")
	}
	ret := rb.buf[rb.head]
	rb.buf[rb.head] = nil
	rb.head++
	rb.head %= bufferSize
	rb.len--
	return ret
}
