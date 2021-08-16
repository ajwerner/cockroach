package eav_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/stretchr/testify/require"
)

func TestEav(t *testing.T) {
	db := NewDatabase(scpb.AttrSchema, nil)
	data := []*scpb.Node{
		{
			Status: scpb.Status_ABSENT,
			Target: scpb.NewTarget(
				scpb.Target_DROP,
				&scpb.Column{
					TableID:    42,
					FamilyID:   1,
					FamilyName: "bar",
					Column: descpb.ColumnDescriptor{
						Name: "baz",
						ID:   1,
					},
				}),
		},
		{
			Status: scpb.Status_ABSENT,
			Target: scpb.NewTarget(
				scpb.Target_DROP,
				&scpb.Table{
					TableID:        42,
					ParentSchemaID: 29,
					ParentID:       1,
				}),
		},
		{
			Status: scpb.Status_ABSENT,
			Target: scpb.NewTarget(
				scpb.Target_DROP,
				&scpb.Database{
					DatabaseID: 1,
				}),
		},
	}
	for _, v := range data {
		require.NoError(t, db.Insert(v))
	}

	typ := reflect.TypeOf
	d := Datom
	var table, tableID, parent, parentID, column Var = "table",
		"table-id", "parent", "parent-id", "column"
	q, err := NewQuery(scpb.AttrSchema,
		EntityType(table, (*scpb.Table)(nil)),
		EntityType(parent, (*scpb.Database)(nil)),
		EntityType(column, (*scpb.Column)(nil)),
		d(table, scpb.AttrDescID, tableID),
		d(table, scpb.AttrParentID, parentID),
		d(parent, scpb.AttrDescID, parentID),
		d(column, scpb.AttrDescID, tableID),
		scpb.NodeRule(parent, scpb.Target_DROP, scpb.Status_ABSENT),
	)
	require.NoError(t, err)
	_ = q.Prepare().Iterate(db, func(r Result) error {
		fmt.Printf("%T %v\n", r.Var(table), r.Var(table))
		return nil
	})
	type v = Var
	NewQuery(scpb.AttrSchema,
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
