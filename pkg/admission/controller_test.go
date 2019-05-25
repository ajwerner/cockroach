package admission

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAdmission(t *testing.T) {
	var overloaded atomic.Value
	overloaded.Store(false)
	c := NewController("test",
		func(Priority) bool { return overloaded.Load().(bool) },
		500*time.Millisecond, .05, .01)
	assert.Equal(t, minPriority, c.Level())
	t100 := time.Unix(0, 100e6)
	p := Priority{DefaultLevel, 0}
	assert.True(t, c.AdmitAt(p, t100))
	assert.True(t, c.AdmitAt(Priority{DefaultLevel, maxShard}, t100))
	overloaded.Store(true)
	t601 := time.Unix(0, 601e6)
	assert.False(t, c.AdmitAt(p, t601))
	p.Level = MaxLevel
	assert.True(t, c.AdmitAt(p, t601))

	assert.Equal(t, Priority{DefaultLevel, maxShard}, c.Level())
	overloaded.Store(false)
	assert.True(t, c.AdmitAt(maxPriority, t601))
	t1101 := time.Unix(0, 1101e6)
	assert.True(t, c.AdmitAt(maxPriority.dec(), t1101))
}
