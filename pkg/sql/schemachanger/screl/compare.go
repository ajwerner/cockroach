package screl

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

var equalityAttrs = []rel.Attribute{
	DescID,
	ReferencedDescID,
	ColumnID,
	Name,
	IndexID,
	Direction,
	Status,
}

func Equal(a, b scpb.Element) bool {
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		return false
	}
	return Schema.EqualOn(equalityAttrs, a, b)
}
