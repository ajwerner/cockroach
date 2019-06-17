package admission

import (
	"context"
	"fmt"
	"math"
	"math/rand"
)

type Priority struct {
	Level uint8
	Shard uint8
}

type contextKey uint8

const (
	priorityContextKey contextKey = iota
)

func ContextWithPriority(ctx context.Context, priority Priority) context.Context {
	return context.WithValue(ctx, priorityContextKey, priority)
}

func PriorityFromContext(ctx context.Context) Priority {
	if prio, ok := ctx.Value(priorityContextKey).(Priority); ok {
		return prio
	}
	return Priority{
		Level: DefaultLevel,
		Shard: uint8(rand.Intn(math.MaxUint8)),
	}
}

func (p Priority) Encode() uint16 {
	return uint16(p.Level)<<8 | uint16(p.Shard)
}

func Decode(p uint16) Priority {
	return Priority{
		Level: uint8(p >> 8),
		Shard: uint8(p),
	}
}

func (p Priority) dec() (r Priority) {
	if p == minPriority {
		return p
	}
	if p.Shard == minShard {
		return Priority{
			Level: levelFromBucket(bucketFromLevel(p.Level) - 1),
			Shard: maxShard,
		}
	}
	return Priority{
		Level: p.Level,
		Shard: shardFromBucket(bucketFromShard(p.Shard) - 1),
	}
}

func (p Priority) inc() Priority {
	if p == maxPriority {
		return p
	}
	if p.Shard == maxShard {
		return Priority{
			Level: levelFromBucket(bucketFromLevel(p.Level) + 1),
			Shard: minShard,
		}
	}
	return Priority{
		Level: p.Level,
		Shard: shardFromBucket(bucketFromShard(p.Shard) + 1),
	}
}

// MakePriority constructs a Priority with a provided level and shard.
func MakePriority(level uint8, shard uint8) Priority {
	return Priority{
		Level: level,
		Shard: shard,
	}
}

func (p Priority) less(other Priority) bool {
	pl, ol := bucketFromLevel(p.Level), bucketFromLevel(other.Level)
	if pl < ol {
		return true
	} else if pl > ol {
		return false
	}
	ps, os := bucketFromShard(p.Shard), bucketFromShard(other.Shard)
	return ps < os
}

const (
	numLevels = 3
	levelStep = math.MaxUint8 / (numLevels - 1)
)

const (
	MaxLevel uint8 = math.MaxUint8 - (iota * levelStep)
	DefaultLevel
	MinLevel
	verifyNumLevels = iota
)

var levelStrings = map[uint8]string{
	MaxLevel:     "max",
	DefaultLevel: "def",
	MinLevel:     "min",
}

func (p Priority) String() string {
	return fmt.Sprintf("%s:%d",
		levelStrings[levelFromBucket(bucketFromLevel(p.Level))],
		bucketFromShard(p.Shard))
}

func init() {
	if numLevels != verifyNumLevels {
		panic("PriorityLevel definitions are messed up")
	}
}

const (
	numShards = 128
	shardStep = math.MaxUint8 / (numShards - 1)
	maxShard  = math.MaxUint8
	minShard  = math.MaxUint8 - ((numShards - 1) * shardStep)
)

var levels = func() (levels [numLevels]uint8) {
	for i, l := 0, uint8(MaxLevel); i < numLevels; i++ {
		levels[i] = l
		l -= levelStep
	}
	return levels
}()

var shards = func() (shards [numShards]uint8) {
	for i, l := 0, uint8(maxShard); i < numShards; i++ {
		shards[i] = l
		l -= shardStep
	}
	return shards
}()

func init() {

}

func levelFromBucket(bucket int) uint8 {
	if bucket > numLevels {
		return MaxLevel
	}
	return uint8(levelStep*bucket) + MinLevel
}

func bucketFromLevel(level uint8) int {
	return int(level / levelStep)
}

func shardFromBucket(bucket int) uint8 {
	if bucket > numShards {
		return math.MaxUint8
	}
	return uint8(shardStep*bucket) + minShard
}

func bucketFromShard(shard uint8) int {
	return int(shard / shardStep)
}
