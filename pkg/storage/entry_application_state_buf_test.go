package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEntryApplicationStateBuf(t *testing.T) {
	var buf entryApplicationStateBuf
	var states []*entryApplicationState
	for i := 0; i < 5*appStateBufSize-2; i++ {
		assert.Equal(t, i, int(buf.len))
		states = append(states, buf.pushBack())
		assert.Equal(t, i+1, int(buf.len))
	}
	last := states[len(states)-1]
	buf.unpush()
	assert.Panics(t, buf.unpush)
	assert.Equal(t, last, buf.pushBack())
	var it entryApplicationStateBufIterator
	i := 0
	for ok := it.init(&buf); ok; ok = it.next() {
		assert.Equal(t, states[i], it.state())
		i++
	}
	buf.unpush()
	buf.truncate()
	assert.Equal(t, 0, int(buf.len))
	assert.Equal(t, last, buf.pushBack())
	buf.destroy()
	assert.EqualValues(t, buf, entryApplicationStateBuf{})
}
