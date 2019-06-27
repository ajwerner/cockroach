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

package qos

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// Level represents a quality of service level.
// The quality of service space is broken up into explicit classes which are
// subdivided into shards. This tuple space allows nodes to make indepedent
// admission decisions which will cooperate to backpressure traffic in an
// orderly manner without explicit coordination.
//
// While its fields are uint8, the values are dense in (NumClasses, 0] and
// (NumShards, 0] rather than being spread out over the uint8 space (like when
// encoded). A Level which contains a Class or Shard outside of that range is
// not valid. Levels which are not valid will cause a panic during encoding.
// Encoded values however are spread over the uint8 space to allow for future
// versions to use different numbers of classes and shards in a forward and
// backward compatible way.
type Level struct {

	// Class indicates the level associated with this priority.
	// For a valid Level its value lies in (NumClasses, 0].
	Class Class

	// Shard indicates the priority shard within this level.
	// For a valid Level its value lies in (NumShards, 0].
	Shard Shard
}

// Class indicates traffic of a certain quality.
type Class uint8

// Shard indicates a shard within a Class.
type Shard uint8

// Decode decodes a priority from a uint32.
// The high 16 bits are ignored.
func Decode(p uint32) Level {
	return Level{
		Class: decodeClass(p),
		Shard: decodeShard(p),
	}
}

// IsValid return true if p has a valid value.
func (l Level) IsValid() bool {
	return l.Class.IsValid() && l.Shard.IsValid()
}

// IsValid is true if Class is in (NumClasses, 0].
func (l Class) IsValid() bool {
	return l < NumClasses
}

// IsValid is true if Shard is in (NumShards, 0].
func (s Shard) IsValid() bool {
	return s < NumShards
}

// Encode encodes a priority to a uint32.
// Encoded priorities can be compared using normal comparison operators.
// Encode will panic if l is not valid.
func (l Level) Encode() uint32 {
	return uint32(encodeClass(l.Class))<<8 | uint32(encodeShard(l.Shard))
}

// EncodeString encodes a priority to a string.
// The returned string will be a 4 character lower-case hex encoded value.
// String encoded priorities can be compared using normal comparison operators.
// EncodeString will panic if l is not valid.
func (l Level) EncodeString() string {
	if !l.IsValid() {
		panic(fmt.Errorf("cannot encode invalid Level %v", l))
	}
	return marshaledString[l.Class][l.Shard]
}

// DecodeString decodes a priority from a string.
// The string should be a 4 character hex encoded value as returned from
// EncodeString.
func DecodeString(s string) (Level, error) {
	var l Level
	if err := l.UnmarshalText(encoding.UnsafeConvertStringToBytes(s)); err != nil {
		return Level{}, err
	}
	return l, nil
}

func encodeClass(l Class) uint8 {
	if !l.IsValid() {
		panic(fmt.Errorf("cannot encode invalid level %d", l))
	}
	const minEncodedClass = math.MaxUint8 % classStep
	return minEncodedClass + uint8(l*classStep)
}

func encodeShard(s Shard) uint8 {
	if !s.IsValid() {
		panic(fmt.Errorf("cannot encode invalid shard %d", s))
	}
	const minEncodedShard = math.MaxUint8 % shardStep
	return minEncodedShard + uint8(s*shardStep)
}

func decodeShard(e uint32) Shard {
	return Shard(e) / shardStep
}

func decodeClass(e uint32) Class {
	return Class(e>>8) / classStep
}

// Dec returns the next lower priority value unless p is the minimum value
// in which case p is returned.
func (l Level) Dec() Level {
	if l.Shard > 0 {
		l.Shard--
	} else if l.Class > 0 {
		l.Class--
		l.Shard = NumShards - 1
	}
	return l
}

// Inc returns the next higher priority value unless p is the maximum value
// in which case p is returned.
func (l Level) Inc() Level {
	if l.Shard < NumShards-1 {
		l.Shard++
	} else if l.Class < NumClasses-1 {
		l.Class++
		l.Shard = 0
	}
	return l
}

// Less returns true if p is less than other.
func (l Level) Less(other Level) bool {
	if l.Class == other.Class {
		return l.Shard < other.Shard
	}
	return l.Class < other.Class
}

const (
	// NumClasses is the total number of levels.
	NumClasses = 3

	// NumShards is the total number of logical shards.
	NumShards = 128

	classStep = math.MaxUint8 / (NumClasses - 1)
	shardStep = math.MaxUint8 / (NumShards - 1)
)

const (
	// ClassHigh is the highest quality of service class.
	ClassHigh Class = NumClasses - 1 - iota
	// ClassDefault is the default quality of service class.
	ClassDefault
	// ClassLow is the lowest quality of service class.
	ClassLow
)

var classStrings = [NumClasses]string{
	ClassHigh:    "h",
	ClassDefault: "d",
	ClassLow:     "l",
}

var marshaledText [NumClasses][NumShards][textLen]byte
var marshaledString [NumClasses][NumShards]string

// textLen is the length of a hex-encoded Level.
const textLen = 4 // 2 * 2 bytes = 4

func init() {
	for c := Class(0); c < NumClasses; c++ {
		for s := Shard(0); s < NumShards; s++ {
			text := levelToText(Level{c, s})
			marshaledText[c][s] = text
			var str string
			strHdr := (*reflect.StringHeader)(unsafe.Pointer(&str))
			strHdr.Data = uintptr(unsafe.Pointer(&text[0]))
			strHdr.Len = len(text)
			marshaledString[c][s] = str
		}
	}
}

func levelToText(l Level) [textLen]byte {
	var data [2]byte
	var out [4]byte
	data[0] = encodeClass(l.Class)
	data[1] = encodeShard(l.Shard)
	if n := hex.Encode(out[:], data[:]); n != len(out) {
		panic(fmt.Errorf("expected to encode %d bytes, got %d", n, len(out)))
	}
	return out
}

var (
	classes = [NumClasses]Class{ClassHigh, ClassDefault, ClassLow}
	shards  = func() (shards [NumShards]Shard) {
		for i := 0; i < NumShards; i++ {
			shards[NumShards-1-i] = Shard(i)
		}
		return shards
	}()
)

// Classes returns an array of classes ordered from highest to lowest.
// This function exists to ease code which iterates over classes in descending
// order.
func Classes() [NumClasses]Class {
	return classes
}

// Shards return an array of Shard values ordered from highest (NumShards-1) to
// lowest (0). This function exists to ease code which iterates over classes in
// descending order.
func Shards() [NumShards]Shard {
	return shards
}

// String returns a shorthand string for the Class if it is valid or a decimal
// integer representation if it is not.
func (c Class) String() string {
	if c.IsValid() {
		return classStrings[c]
	}
	return strconv.Itoa(int(c))
}

// String returns a string formatted as Class:Shard where Shard is always an
// integer and Class uses a shorthand string if it is valid or an intger if not.
func (l Level) String() string {
	return l.Class.String() + ":" + strconv.Itoa(int(l.Shard))
}

// MarshalText implements encoding.TextMarshaler.
func (l Level) MarshalText() ([]byte, error) {
	if !l.IsValid() {
		return nil, fmt.Errorf("cannot marshal invalid Level (%d, %d) to text", l.Class, l.Shard)
	}
	return marshaledText[l.Class][l.Shard][:], nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (l *Level) UnmarshalText(data []byte) error {
	if len(data) != textLen {
		return fmt.Errorf("invalid data length %d, expected %d", len(data), textLen)
	}
	var decoded [2]byte
	if _, err := hex.Decode(decoded[:], data); err != nil {
		return err
	}
	*l = Decode(uint32(binary.BigEndian.Uint16(decoded[:])))
	return nil
}
