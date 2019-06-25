package storage

import (
	"fmt"
	"sync"
)

const appStateBufSize = 8

type entryApplicationStateBuf struct {
	len        int32
	unpushed   bool
	head, tail *entryApplicationStateBufNode
}

var entryApplicationStateBufNodeSyncPool = sync.Pool{
	New: func() interface{} { return new(entryApplicationStateBufNode) },
}

type entryApplicationStateBufIterator struct {
	idx    int32
	offset int32
	buf    *entryApplicationStateBuf
	node   *entryApplicationStateBufNode
}

func (it *entryApplicationStateBufIterator) reset() {
	it.offset = 0
	it.idx = 0
	it.node = it.buf.head
}

func (it *entryApplicationStateBufIterator) init(buf *entryApplicationStateBuf) bool {
	*it = entryApplicationStateBufIterator{
		buf:  buf,
		node: buf.head,
	}
	return it.buf.len > 0
}

func (it *entryApplicationStateBufIterator) next() bool {
	if it.idx+1 == it.buf.len {
		return false
	}
	it.idx++
	it.offset++
	if it.offset == appStateBufSize {
		it.node = it.node.next
		it.offset = 0
	}
	return true
}

func (it *entryApplicationStateBufIterator) state() *entryApplicationState {
	return it.node.at(it.offset)
}

type entryApplicationStateBufNode struct {
	ringBuf
	next *entryApplicationStateBufNode
}

func (buf *entryApplicationStateBuf) initialize() {
	n := entryApplicationStateBufNodeSyncPool.Get().(*entryApplicationStateBufNode)
	buf.head, buf.tail = n, n
}

// unpush allows the caller to keep the tail element of the buffer allocated but
// remove it from iteration. After calling unpush, the next call to pushBack
// will return the value from the previous call to pushBack.
// This function may not be called more than once between calls to pushBack and
// may not be called on an empty buf.
func (buf *entryApplicationStateBuf) unpush() {
	//defer buf.debug("unpush")()
	if buf.unpushed {
		panic("already unpushed")
	}
	buf.len--
	buf.unpushed = true
}

func (buf *entryApplicationStateBuf) first() *entryApplicationState {
	return buf.head.at(0)
}

func (rb *ringBuf) last() *entryApplicationState {
	return rb.at(rb.len - 1)
}

// func (buf *entryApplicationStateBuf) String() string {
// 	var b bytes.Buffer
// 	fmt.Fprintf(&b, "debug-%p len %d, unpushed %v, head %p, tail %p [ ", buf, buf.len, buf.unpushed, buf.head, buf.tail)
// 	for cur := buf.head; cur != nil; cur = cur.next {
// 		fmt.Fprintf(&b, "{%p %v %v}", cur, cur.len, cur.head)
// 	}
// 	fmt.Fprintf(&b, "]")
// 	return b.String()
// }

// func (buf *entryApplicationStateBuf) debug(m string) func() {
// 	fmt.Println(buf.String(), "before", m)
// 	return func() {
// 		fmt.Println(buf.String(), "after", m)
// 	}
// }

func (buf *entryApplicationStateBuf) pushBack() *entryApplicationState {
	// defer buf.debug("pushBack")()
	if buf.tail == nil {
		buf.initialize()
	}
	buf.len++
	if buf.unpushed {
		buf.unpushed = false
		return buf.tail.last()
	}
	if buf.tail.len == appStateBufSize {
		newTail := entryApplicationStateBufNodeSyncPool.Get().(*entryApplicationStateBufNode)
		buf.tail.next = newTail
		buf.tail = newTail
	}
	return buf.tail.pushBack()
}

func (buf *entryApplicationStateBuf) destroy() {
	// defer buf.debug("destroy")()
	for cur := buf.head; cur != nil; {
		next := cur.next
		*cur = entryApplicationStateBufNode{}
		entryApplicationStateBufNodeSyncPool.Put(cur)
		cur, buf.head = next, next
	}
	*buf = entryApplicationStateBuf{}
}

func (buf *entryApplicationStateBuf) truncate() {
	// defer buf.debug("truncate")()
	for buf.len > 0 {
		// The first case here occurs when an entry has been unpushed
		if headLen := buf.head.len; headLen >= buf.len {
			buf.head.truncate(buf.len)
			if headLen == buf.len && buf.unpushed {
				oldHead := buf.head
				newHead := oldHead.next
				buf.head = newHead
				*oldHead = entryApplicationStateBufNode{}
				entryApplicationStateBufNodeSyncPool.Put(oldHead)
			}
			buf.len = 0
			break
		}
		buf.len -= buf.head.len
		buf.head.truncate(buf.head.len)
		// newHead cannot be nil here because we know that buf has more entries.
		oldHead := buf.head
		newHead := oldHead.next
		buf.head = newHead
		*oldHead = entryApplicationStateBufNode{}
		entryApplicationStateBufNodeSyncPool.Put(oldHead)
	}
}

type ringBuf struct {
	head int32
	len  int32
	buf  [appStateBufSize]entryApplicationState
}

func (rb *ringBuf) at(idx int32) *entryApplicationState {
	if idx >= rb.len {
		panic(fmt.Sprintf("index out of range %v, %v", idx, rb.len))
	}
	return &rb.buf[(rb.head+idx)%appStateBufSize]
}

func (rb *ringBuf) pushBack() *entryApplicationState {
	if rb.len == appStateBufSize {
		panic("cannot push onto a full ringBuf")
	}
	ret := &rb.buf[(rb.head+rb.len)%appStateBufSize]
	rb.len++
	return ret
}

func (rb *ringBuf) truncate(n int32) {
	if n > rb.len {
		panic("cannot truncate more than have")
	}
	for i := int32(0); i < n; i++ {
		*rb.at(i) = entryApplicationState{}
	}
	rb.len -= n
	rb.head += n
	rb.head %= appStateBufSize
}
