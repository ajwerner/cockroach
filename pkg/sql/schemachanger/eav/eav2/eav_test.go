package eav2_test

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav/eav2"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func TestEav(t *testing.T) {
	sc := eav2.NewSchema(eav2.Mappings{
		AttributeTypes: map[eav2.Attribute]reflect.Type{
			scpb.AttrElement: reflect.TypeOf((*protoutil.Message)(nil)).Elem(),
		},
		TypeMappings: map[reflect.Type]map[string]eav2.Attribute{
			reflect.TypeOf((*scpb.Database)(nil)): {
				"DatabaseID": scpb.AttrDescID,
			},
			reflect.TypeOf((*scpb.Table)(nil)): {
				"TableID": scpb.AttrDescID,
			},
			reflect.TypeOf((*scpb.Column)(nil)): {
				"TableID":   scpb.AttrDescID,
				"Column.ID": scpb.AttrColumnID,
			},
			reflect.TypeOf((*scpb.Target)(nil)): {
				"Direction":            scpb.AttrDirection,
				"Column":               scpb.AttrElement,
				"PrimaryIndex":         scpb.AttrElement,
				"SecondaryIndex":       scpb.AttrElement,
				"SequenceDependency":   scpb.AttrElement,
				"UniqueConstraint":     scpb.AttrElement,
				"CheckConstraint":      scpb.AttrElement,
				"Sequence":             scpb.AttrElement,
				"DefaultExpression":    scpb.AttrElement,
				"View":                 scpb.AttrElement,
				"TypeRef":              scpb.AttrElement,
				"Table":                scpb.AttrElement,
				"OutForeignKey":        scpb.AttrElement,
				"InForeignKey":         scpb.AttrElement,
				"RelationDependedOnBy": scpb.AttrElement,
				"SequenceOwner":        scpb.AttrElement,
				"Type":                 scpb.AttrElement,
				"Schema":               scpb.AttrElement,
				"Database":             scpb.AttrElement,
			},
			reflect.TypeOf((*scpb.Node)(nil)): {
				"Status": scpb.AttrStatus,
				"Target": scpb.AttrTarget,
			},
		},
	})

	n := &scpb.Node{
		Target: scpb.NewTarget(scpb.Target_DROP, &scpb.Column{
			TableID:    1,
			FamilyID:   1,
			FamilyName: "asdf",
			Column: descpb.ColumnDescriptor{
				ID: 1,
			},
		}),
		Status: scpb.Status_ABSENT,
	}
	tr := eav2.NewTree(sc, nil)
	tr.Insert(n)
	tr.Iterate(eav2.Values{}, eav2.EntityIteratorFunc(func(entity eav2.Entity) error {
		t.Logf("here %T %v", entity.Interface(), entity.Interface())
		return nil
	}))
}
