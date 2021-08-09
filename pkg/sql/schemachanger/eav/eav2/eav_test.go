package eav2_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav/eav2"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/stretchr/testify/require"
)

func TestEav(t *testing.T) {
	sc := eav2.NewSchema(eav2.Mappings{
		TypeFieldMappings: map[interface{}]eav2.FieldMappings{
			(*scpb.Database)(nil): {
				"DatabaseID": scpb.AttrDescID,
			},
			(*scpb.Table)(nil): {
				"TableID": scpb.AttrDescID,
			},
			(*scpb.Column)(nil): {
				"TableID":   scpb.AttrDescID,
				"Column.ID": scpb.AttrColumnID,
			},
			(*scpb.Target)(nil): {
				"Direction": scpb.AttrDirection,
			},
			(*scpb.Node)(nil): {
				"Status": scpb.AttrStatus,
			},
		},
		TypeChildMappings: map[interface{}]eav2.ChildMappings{
			(*scpb.Target)(nil): {
				scpb.AttrElement: (*scpb.Target).GetElement,
			},
			(*scpb.Node)(nil): {
				scpb.AttrTarget: func(n *scpb.Node) *scpb.Target { return n.Target },
			},
		},
	})

	require.Equal(t, descpb.ColumnID(1), *sc.GetAttribute(scpb.AttrColumnID, &scpb.Column{
		TableID:    1,
		FamilyID:   1,
		FamilyName: "asdf",
		Column: descpb.ColumnDescriptor{
			ID: 1,
		},
	}).(*descpb.ColumnID))

}
