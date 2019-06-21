package admission

import (
	"fmt"
	"math"
	"strconv"
)

// TODO(ajwerner): Consider renaming from Priority to Level and
// Level to Class.

// Priority represents a logical priority level.
// While its fields are uint8, the values are dense in the number of shards and
// levels rather than being spread out over the uint8 space as is the case for
// EncodedPriority values.
type Priority struct {

	// Level indicates the level associated with this priority.
	// For a valid Priority its value lies in (NumLevels, 0].
	Level Level

	// Shard indicates the priority shard within this level.
	// For a valid Priority its value lies in (NumShards, 0].
	Shard Shard
}

// Level indicates traffic of a certain quality.
type Level uint8

// Shard indicates a shard within a Level.
type Shard uint8

// Decode decodes a priority from a uint32.
// The high 16 bits are ignored.
func Decode(p uint32) Priority {
	return Priority{
		Level: decodeLevel(p),
		Shard: decodeShard(p),
	}
}

// IsValid return true if p has a valid value.
func (p Priority) IsValid() bool {
	return p.Level.IsValid() && p.Shard.IsValid()
}

// IsValid is true if Level is in (NumLevels, 0].
func (l Level) IsValid() bool {
	return l < NumLevels
}

// IsValid is true if Shard is in (NumShards, 0].
func (s Shard) IsValid() bool {
	return s < NumShards
}

// Encode encodes a priority to a uint32.
// Encoded priorities can be compared using normal comparison operators.
func (p Priority) Encode() uint32 {
	return encodeLevel(p.Level) | encodeShard(p.Shard)
}

const (
	levelMask = 0x0000FF00
	shardMask = 0x000000FF
)

func encodeLevel(l Level) uint32 {
	if !l.IsValid() {
		panic(fmt.Errorf("cannot encode invalid level %d", l))
	}
	return uint32(255-((NumLevels-1-l)*levelStep)) << 8
}

func encodeShard(s Shard) uint32 {
	if !s.IsValid() {
		panic(fmt.Errorf("cannot encode invalid shard %d", s))
	}
	return uint32(255 - ((NumShards - 1 - s) * shardStep))
}

func decodeShard(e uint32) Shard {
	return Shard(e) / shardStep
}

func decodeLevel(e uint32) Level {
	return Level(e>>8) / levelStep
}

// Dec returns the next lower priority value unless p is the minimum value
// in which case p is returned.
func (p Priority) Dec() Priority {
	if p.Shard > 0 {
		p.Shard--
	} else if p.Level > 0 {
		p.Level--
		p.Shard = NumShards - 1
	}
	return p
}

// Inc returns the next higher priority value unless p is the maximum value
// in which case p is returned.
func (p Priority) Inc() Priority {
	if p.Shard < NumShards-1 {
		p.Shard++
	} else if p.Level < NumLevels-1 {
		p.Level++
		p.Shard = 0
	}
	return p
}

// Less returns true if p is less than other.
func (p Priority) Less(other Priority) bool {
	if p.Level < other.Level {
		return true
	}
	return p.Shard < other.Shard
}

const (
	// NumLevels is the total number of levels.
	NumLevels = 3
	// NumShards is tje total number of logical shards.
	NumShards = 128

	levelStep = math.MaxUint8 / (NumLevels - 1)
	shardStep = math.MaxUint8 / (NumShards - 1)
)

const (
	MaxLevel Level = NumLevels - 1 - iota
	DefaultLevel
	MinLevel
)

var levelStrings = [NumLevels]string{
	MaxLevel:     "max",
	DefaultLevel: "def",
	MinLevel:     "min",
}

func (p Priority) String() string {
	p = p.makeValid()
	return levelStrings[p.Level] + ":" + strconv.Itoa(int(p.Shard))
}

func (p Priority) makeValid() Priority {
	if p.Level >= NumLevels {
		p.Level = NumLevels - 1
	}
	if p.Shard >= NumShards {
		p.Shard = NumShards - 1
	}
	return p
}

// These arrays contain all of the levels and shards in reserve order
// where the highest value is the 0th element.
// These are for loops because it's confusing to check equality when iterating
// a uint8 in reverse.
var (
	levels [NumLevels]Level
	shards [NumShards]Shard
)

func init() {
	for i := 0; i < NumLevels; i++ {
		levels[i] = NumLevels - 1 - Level(i)
	}
	for i := 0; i < NumShards; i++ {
		shards[i] = NumShards - 1 - Shard(i)
	}
}
