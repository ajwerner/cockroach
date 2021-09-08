package deprules

import "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"

func indexContainsColumn(idx *descpb.IndexDescriptor, colID descpb.ColumnID) bool {
	return columnsContainsID(idx.KeyColumnIDs, colID) ||
		columnsContainsID(idx.StoreColumnIDs, colID) ||
		columnsContainsID(idx.KeySuffixColumnIDs, colID)
}

func columnsContainsID(haystack []descpb.ColumnID, needle descpb.ColumnID) bool {
	for _, id := range haystack {
		if id == needle {
			return true
		}
	}
	return false
}

func idInIDs(objects []descpb.ID, id descpb.ID) bool {
	for _, other := range objects {
		if other == id {
			return true
		}
	}
	return false
}
