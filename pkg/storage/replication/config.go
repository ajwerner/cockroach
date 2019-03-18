package replication

import (
	"runtime"
	"time"
)

type Option interface {
	apply(*config) error
}

type config struct {
	tickInterval       time.Duration
	numWorkers         int
	liveness           func(GroupID) bool
	raftMessageFactory func() RaftMessage
}

func mustOptionFunc(f func(c *config)) optionFunc {
	return func(c *config) error { f(c); return nil }
}

var defaultNumWorkers = 8 * runtime.NumCPU()
var defaultTickInterval = time.Millisecond

type optionFunc func(*config) error

func (f optionFunc) apply(c *config) error { return f(c) }

func (c *config) init(options ...Option) error {
	*c = config{
		tickInterval: defaultTickInterval,
		numWorkers:   defaultNumWorkers,
	}
	for _, o := range options {
		if err := o.apply(c); err != nil {
			return err
		}
	}
	return nil
}

// TickInterval returns an option to set the tick interval.
func TickInterval(interval time.Duration) Option {
	return mustOptionFunc(func(c *config) {
		c.tickInterval = interval
	})
}

func NumWorkers(numWorkers int) Option {
	return mustOptionFunc(func(c *config) { c.numWorkers = numWorkers })
}
