package admission

import (
	"context"
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
