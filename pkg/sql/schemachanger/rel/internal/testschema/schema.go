package testschema

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
)

type Entity struct {
	I8       int8
	PI8      *int8
	I16      int16
	PI16     *int16
	I32      int32
	PI32     *int32
	I64      int64
	PI64     *int64
	UI8      uint8
	PUI8     *uint8
	UI16     uint16
	PUI16    *uint16
	UI32     uint32
	PUI32    *uint32
	UI64     uint64
	PUI64    *uint64
	S        string
	PS       *string
	Uintptr  uintptr
	PUintptr *uintptr
}

type Pair struct {
	A, B *Entity
}

var Schema = rel.MustSchema("testschema", rel.Mappings{
	TypeMappings: map[reflect.Type]map[string]rel.Attribute{
		reflect.TypeOf((*Entity)(nil)): {
			"I8":       I8,
			"PI8":      PI8,
			"I16":      I16,
			"PI16":     PI16,
			"I32":      I32,
			"PI32":     PI32,
			"I64":      I64,
			"PI64":     PI64,
			"UI8":      UI8,
			"PUI8":     PUI8,
			"UI16":     UI16,
			"PUI16":    PUI16,
			"UI32":     UI32,
			"PUI32":    PUI32,
			"UI64":     UI64,
			"PUI64":    PUI64,
			"S":        S,
			"PS":       PS,
			"Uintptr":  Uintptr,
			"PUintptr": PUintptr,
		},
		reflect.TypeOf((*Pair)(nil)): {
			"A": A,
			"B": B,
		},
	},
})
