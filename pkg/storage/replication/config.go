package replication

import "time"

type Option interface {
	apply(*config)
}

type config struct {
	tickInterval time.Duration
	numWorkers   int
}

type optionFunc func(*config)

func (f optionFunc) apply(c *config) { f(c) }

func init(c *config, options ...Option) {
	*c = config{
		tickInterval: defaultTickInterval,
		numWorkers:   defaultNumWorkers,
	}
	for _, o := range options {
		o.apply(c)
	}
}

// TickInterval returns an option to set the tick interval.
func TickInterval(interval time.Duration) Option {
	return optionFunc(func(c *config) {
		c.tickInterval = interval
	})
}
