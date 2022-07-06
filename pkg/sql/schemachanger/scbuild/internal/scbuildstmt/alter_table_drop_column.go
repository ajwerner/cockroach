package scbuildstmt

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func alterTableDropColumn(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, n *tree.AlterTableDropColumn,
) {
	checkSafeUpdatesForDropColumn(b)
	checkRowLevelTTLColumn(b, tn, tbl, n)
	checkRegionalByRowColumnConflict(b, tbl, n)

	b.IncrementSchemaChangeAlterCounter("table", "drop_column")

	// Check column existence.
	col, elts, done := resolveColumnForDropColumn(b, tn, tbl, n)
	if done {
		return
	}

	checkColumnNotInaccessible(col, n)
	_, _, de := scpb.FindColumnDefaultExpression(elts)
	handleDropColumnDefaultExpression(b, de, tbl, n)

	dropIndexesForDropColumn(b, tn, tbl.TableID, n.DropBehavior, col)

	// Now for the primary indexes -- at the time of writing, there are at most
	// two
	existing, freshlyAdded := getPrimaryIndexes(b, tbl.TableID)
	if freshlyAdded != nil {
		panic(scerrors.NotImplementedErrorf(n, "multi-statement drop not yet implemented"))
	}
	createNewPrimaryIndex(b, tbl, existing, func(
		b BuildCtx, newIndex *scpb.PrimaryIndex, existingColumns []*scpb.IndexColumn,
	) (newColumns []*scpb.IndexColumn) {
		var ic *scpb.IndexColumn
		for _, c := range existingColumns {
			if c.ColumnID == col.ColumnID {
				ic = c
				break
			}
		}
		if ic == nil {
			panic(errors.AssertionFailedf("failed to find column"))
		}
		if ic.Kind != scpb.IndexColumn_STORED {
			panic(errors.AssertionFailedf("can only drop columns which are stored in the primary index, this one is %v ",
				ic.Kind))
		}
		for _, ec := range existingColumns {
			sameKind := ec.Kind == ic.Kind
			if sameKind && ec.OrdinalInKind == ic.OrdinalInKind {
				continue
			}
			cloned := protoutil.Clone(ec).(*scpb.IndexColumn)
			if sameKind && ec.OrdinalInKind > ic.OrdinalInKind {
				cloned.OrdinalInKind--
			}
			cloned.IndexID = newIndex.IndexID
			newColumns = append(newColumns, cloned)
			b.Add(cloned)
		}
		return newColumns
	})

	elts.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		b.Drop(e)
	})
}

func dropIndexesForDropColumn(
	b BuildCtx,
	tn *tree.TableName,
	tableID catid.DescID,
	dropBehavior tree.DropBehavior,
	col *scpb.Column,
) {
	// TODO(ajwerner): Deal with status
	tableElts := b.QueryByID(tableID)
	var indexIDs catalog.IndexIDSet
	scpb.ForEachIndexColumn(tableElts, func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.IndexColumn) {
		if e.ColumnID == col.ColumnID {
			indexIDs.Add(e.IndexID)
		}
	})
	var secondaryIndexIDs catalog.IndexIDSet
	var indexes []*scpb.SecondaryIndex
	var indexNames []*scpb.IndexName
	scpb.ForEachSecondaryIndex(tableElts, func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.SecondaryIndex,
	) {
		if indexIDs.Contains(e.IndexID) {
			secondaryIndexIDs.Add(e.IndexID)
			indexes = append(indexes, e)
		}
	})
	scpb.ForEachIndexName(tableElts, func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.IndexName,
	) {
		if secondaryIndexIDs.Contains(e.IndexID) {
			indexNames = append(indexNames, e)
		}
	})
	if len(indexNames) != len(indexes) {
		panic(errors.AssertionFailedf("indexes %v does not match indexNames %v",
			indexes, indexNames))
	}
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i].IndexID < indexes[j].IndexID
	})
	sort.Slice(indexNames, func(i, j int) bool {
		return indexNames[i].IndexID < indexNames[j].IndexID
	})
	for i, idx := range indexes {
		name := tree.TableIndexName{
			Table: *tn,
			Index: tree.UnrestrictedName(indexNames[i].Name),
		}
		dropSecondaryIndex(b, &name, dropBehavior, idx, tableElts.Filter(func(
			current scpb.Status, target scpb.TargetStatus, e scpb.Element,
		) bool {
			idI, _ := screl.Schema.GetAttribute(screl.IndexID, e)
			return idI != nil && idI.(catid.IndexID) == idx.IndexID
		}))
	}
}

func handleDropColumnDefaultExpression(
	b BuildCtx, de *scpb.ColumnDefaultExpression, tbl *scpb.Table, n *tree.AlterTableDropColumn,
) (elementsToDrop []scpb.Element) {
	if de == nil {
		return
	}
	if de != nil {
		panic(scerrors.NotImplementedError(n))
	}
	{
		var isReferencedByOtherColumn bool
		scpb.ForEachSequenceOwner(b.QueryByID(tbl.TableID), func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.SequenceOwner) {
			if isReferencedByOtherColumn {
				return
			}
			// TODO(ajwerner): Check status etc.
			backrefs := b.BackReferences(e.SequenceID)
			scpb.ForEachColumnType(backrefs, func(_ scpb.Status, _ scpb.TargetStatus, ct *scpb.ColumnType) {
				if isReferencedByOtherColumn {
					return
				}
				if ct.TableID != tbl.TableID || ct.ColumnID != de.ColumnID {
					isReferencedByOtherColumn = true
				}
			})
			scpb.ForEachColumnDefaultExpression(backrefs, func(_ scpb.Status, _ scpb.TargetStatus, cd *scpb.ColumnDefaultExpression) {
				if isReferencedByOtherColumn {
					return
				}
				if cd.TableID != tbl.TableID || cd.ColumnID != de.ColumnID {
					isReferencedByOtherColumn = true
				}
			})
			scpb.ForEachColumnOnUpdateExpression(backrefs, func(_ scpb.Status, _ scpb.TargetStatus, cu *scpb.ColumnOnUpdateExpression) {
				if isReferencedByOtherColumn {
					return
				}
				if cu.TableID != tbl.TableID || cu.ColumnID != de.ColumnID {
					isReferencedByOtherColumn = true
				}
			})
			scpb.ForEachCheckConstraint(backrefs, func(_ scpb.Status, _ scpb.TargetStatus, cu *scpb.CheckConstraint) {
				if isReferencedByOtherColumn {
					return
				}
				// TODO(ajwerner): Is there more to this?
				isReferencedByOtherColumn = true
			})
		})
		if isReferencedByOtherColumn {
			panic("TODO")
		}
	}
	return nil
}

// I want to query for all sequences owned by the current column for all references
// to the column such that that reference is not this column.

func checkColumnNotInaccessible(col *scpb.Column, n *tree.AlterTableDropColumn) {
	if col.IsInaccessible {
		panic(pgerror.Newf(
			pgcode.InvalidColumnReference,
			"cannot drop inaccessible column %q",
			n.Column,
		))
	}
}

func checkRegionalByRowColumnConflict(b BuildCtx, tbl *scpb.Table, n *tree.AlterTableDropColumn) {
	var regionalByRow *scpb.TableLocalityRegionalByRow
	// TODO(ajwerner): Does this need to look at status or target status?
	scpb.ForEachTableLocalityRegionalByRow(b.QueryByID(tbl.TableID), func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.TableLocalityRegionalByRow) {
		regionalByRow = e
	})
	if regionalByRow != nil {
		rbrColName := tree.RegionalByRowRegionDefaultColName
		if regionalByRow.As != "" {
			rbrColName = tree.Name(regionalByRow.As)
		}
		if rbrColName == n.Column {
			panic(errors.WithHintf(
				pgerror.Newf(
					pgcode.InvalidColumnReference,
					"cannot drop column %s as it is used to store the region in a REGIONAL BY ROW table",
					n.Column,
				),
				"You must change the table locality before dropping this table or alter the table to use a different column to use for the region.",
			))
		}
	}
}

func checkRowLevelTTLColumn(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, n *tree.AlterTableDropColumn,
) {
	var rowLevelTTL *scpb.RowLevelTTL
	// TODO(ajwerner): Does this need to look at status or target status?
	scpb.ForEachRowLevelTTL(b.QueryByID(tbl.TableID), func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.RowLevelTTL) {
		rowLevelTTL = e
	})
	if n.Column == colinfo.TTLDefaultExpirationColumnName && rowLevelTTL != nil {
		panic(errors.WithHintf(
			pgerror.Newf(
				pgcode.InvalidTableDefinition,
				`cannot drop column %s while row-level TTL is active`,
				n.Column,
			),
			"use ALTER TABLE %s RESET (ttl) instead",
			tn,
		))
	}
}

func resolveColumnForDropColumn(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, n *tree.AlterTableDropColumn,
) (col *scpb.Column, elts ElementResultSet, done bool) {
	elts = b.ResolveColumn(tbl.TableID, n.Column, ResolveParams{
		IsExistenceOptional: n.IfExists,
		RequiredPrivilege:   privilege.CREATE,
	})
	// TODO(ajwerner): Do we need to check the status of the column?
	// if the column is already being dropped, we should probably
	// error out, no?
	_, _, col = scpb.FindColumn(elts)
	if col == nil {
		if !n.IfExists {
			panic(errors.AssertionFailedf("failed to find column %v in %v which was already resolved",
				n.Column, tn))
		}
		return nil, nil, true
	}
	return col, elts, false
}

func checkSafeUpdatesForDropColumn(b BuildCtx) {
	if !b.SessionData().SafeUpdates {
		return
	}
	err := pgerror.DangerousStatementf("ALTER TABLE DROP COLUMN will " +
		"remove all data in that column")
	if !b.EvalCtx().TxnIsSingleStmt {
		err = errors.WithIssueLink(err, errors.IssueLink{
			IssueURL: "https://github.com/cockroachdb/cockroach/issues/46541",
			Detail: "when used in an explicit transaction combined with other " +
				"schema changes to the same table, DROP COLUMN can result in data " +
				"loss if one of the other schema change fails or is canceled",
		})
	}
	panic(err)
}
