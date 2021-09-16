package entitynodetest

import "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"

type TestAttr int8

var _ rel.Attribute = TestAttr(0)

//go:generate stringer --type TestAttr  --tags test
const (
	I8 TestAttr = iota
	PI8
	I16
	PI16
	I32
	PI32
	I64
	PI64
	UI8
	PUI8
	UI16
	PUI16
	UI32
	PUI32
	UI64
	PUI64
	String
	PS
	Uintptr
	PUintptr
	E
	L
	R
	N
)
