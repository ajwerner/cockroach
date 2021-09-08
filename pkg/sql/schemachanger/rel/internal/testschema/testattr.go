package testschema

import "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"

type TestAttr int8

func (i TestAttr) Ordinal() rel.Ordinal { return rel.Ordinal(i) }

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
	S
	PS
	Uintptr
	PUintptr
	A
	B
)
