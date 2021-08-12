package eav2_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav/eav2"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav/eav2"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestEav(t *testing.T) {
	sc := NewSchema(Mappings{
		AttributeTypes: map[Attribute]reflect.Type{
			scpb.AttrElement: reflect.TypeOf((*protoutil.Message)(nil)).Elem(),
		},
		TypeMappings: map[reflect.Type]map[string]Attribute{
			reflect.TypeOf((*scpb.Database)(nil)): {
				"DatabaseID": scpb.AttrDescID,
			},
			reflect.TypeOf((*scpb.Table)(nil)): {
				"TableID":        scpb.AttrDescID,
				"ParentID":       scpb.AttrParentID,
				"ParentSchemaID": scpb.AttrParentSchemaID,
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
	db := NewTree(sc, nil)

	data := []*scpb.Node{
		{
			Target: scpb.NewTarget(scpb.Target_DROP, &scpb.Column{
				TableID:    42,
				FamilyID:   1,
				FamilyName: "asdf",
				Column: descpb.ColumnDescriptor{
					ID: 1,
				},
			}),
			Status: scpb.Status_ABSENT,
		},
		{
			Target: scpb.NewTarget(scpb.Target_DROP, &scpb.Table{
				TableID:        42,
				ParentSchemaID: 29,
				ParentID:       1,
			}),
			Status: scpb.Status_ABSENT,
		},
		{
			Target: scpb.NewTarget(scpb.Target_DROP, &scpb.Database{
				DatabaseID: 1,
			}),
			Status: scpb.Status_ABSENT,
		},
	}
	for _, v := range data {
		require.NoError(t, db.Insert(v))
	}

	typ := reflect.TypeOf
	d := Datom
	var (
		table, tableID, parent, parentID, column, parentTarget, parentNode Var = "table",
			"tableID", "parent", "parentID", "column", "parentTarget", "parentNode"
	)
	q := Prepare(sc,
		d(table, TypeAttribute, typ((*scpb.Table)(nil))),
		d(table, scpb.AttrDescID, tableID),
		d(table, scpb.AttrParentID, parentID),
		d(parent, scpb.AttrDescID, parentID),
		d(column, scpb.AttrDescID, tableID),
		d(column, TypeAttribute, typ((*scpb.Column)(nil))),
		d(parentTarget, TypeAttribute, typ((*scpb.Target)(nil))),
		d(parentTarget, scpb.AttrElement, table),
		d(parentNode, TypeAttribute, typ((*scpb.Node)(nil))),
		d(parentNode, scpb.AttrTarget, parentTarget),
		d(parentTarget, scpb.AttrDirection, scpb.Target_DROP),
		d(parentNode, scpb.AttrStatus, scpb.Status_ABSENT),
	)
	_ = q.Evaluate(db, func(r Result) error {
		fmt.Printf("%T %v", r.Var(table), r.Var(table))
		return nil
	})
	type v = eav2.Var
	Prepare(sc,
		d(v("parent"), TypeAttribute, typ((*scpb.Database)(nil))),
		d(v("parent"), scpb.AttrDescID, v("parentID")),
		d(v("parentNode"), scpb.AttrElement, v("parent")),
		d(v("parentNode"), scpb.AttrDirection, scpb.Target_DROP),
		d(v("parentNode"), scpb.AttrStatus, v("status")),
		d(v("parentNode"), scpb.AttrStatus, Any(
			scpb.Status_DELETE_AND_WRITE_ONLY,
			scpb.Status_DELETE_ONLY,
		)),

		/*
			// would be nice to be able to use an or clause or something like that.
				d(v("parent"), TypeAttribute, typ((*scpb.Schema)(nil))),
				d(v("other"), scpb.AttrParentSchemaID, v("parentID")),
				d(v("other"), TypeAttribute, Any(
					typ((*scpb.Table)(nil)),
					typ((*scpb.View)(nil)),
					typ((*scpb.Sequence)(nil)),
					typ((*scpb.Type)(nil)),
				)),
		*/
		d(v("other"), scpb.AttrParentID, v("parentID")),
		d(v("other"), TypeAttribute, Any(
			typ((*scpb.Table)(nil)),
			typ((*scpb.View)(nil)),
			typ((*scpb.Sequence)(nil)),
			typ((*scpb.Type)(nil)),
			typ((*scpb.Schema)(nil)),
		)),

		d(v("otherNode"), scpb.AttrElement, v("other")),
		d(v("otherNode"), scpb.AttrDirection, scpb.Target_DROP),
		d(v("otherNode"), scpb.AttrStatus, v("status")),
	)
}
