// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/sequence"
	"github.com/cockroachdb/errors"
)

type Builder struct {
	// TODO(ajwerner): Inject a better interface than this.
	res     resolver.SchemaResolver
	semaCtx *tree.SemaContext
	evalCtx *tree.EvalContext

	nodes []*scpb.Node
}

func NewBuilder(
	res resolver.SchemaResolver, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext,
) *Builder {
	return &Builder{
		res:     res,
		semaCtx: semaCtx,
		evalCtx: evalCtx,
	}
}

func (b *Builder) AlterTable(
	ctx context.Context, nodes []*scpb.Node, n *tree.AlterTable,
) ([]*scpb.Node, error) {
	// TODO (lucy): Clean this up.
	b.nodes = nodes
	defer func() {
		b.nodes = nil
	}()

	// Resolve the table.
	tn := n.Table.ToTableName()
	table, err := resolver.ResolveExistingTableObject(ctx, b.res, &tn,
		tree.ObjectLookupFlagsWithRequired())
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) && n.IfExists {
			return nodes, nil
		}
		return nil, err
	}
	for _, cmd := range n.Cmds {
		if err := b.alterTableCmd(ctx, table, cmd, &tn); err != nil {
			return nil, err
		}
	}

	result := make([]*scpb.Node, len(b.nodes))
	for i := range b.nodes {
		result[i] = b.nodes[i]
	}
	return result, nil
}

func (b *Builder) alterTableCmd(
	ctx context.Context, table *tabledesc.Immutable, cmd tree.AlterTableCmd, tn *tree.TableName,
) error {
	switch t := cmd.(type) {
	case *tree.AlterTableAddColumn:
		return b.alterTableAddColumn(ctx, table, t, tn)
	case *tree.AlterTableAddConstraint:

	case *tree.AlterTableAlterPrimaryKey:

	case *tree.AlterTableDropColumn:
		return b.alterTableDropColumn(ctx, table, t)
	case *tree.AlterTableDropConstraint:

	case *tree.AlterTableValidateConstraint:

	case tree.ColumnMutationCmd:

	case *tree.AlterTablePartitionByTable:

	case *tree.AlterTableSetAudit:

	case *tree.AlterTableRenameColumn:

	case *tree.AlterTableOwner:

	default:
		return errors.AssertionFailedf("unsupported alter command: %T", cmd)
	}
	panic("not implemented")
}

func (b *Builder) alterTableAddColumn(
	ctx context.Context, table *tabledesc.Immutable, t *tree.AlterTableAddColumn, tn *tree.TableName,
) error {
	d := t.ColumnDef

	version := b.evalCtx.Settings.Version.ActiveVersionOrEmpty(ctx)
	toType, err := tree.ResolveType(ctx, d.Type, b.semaCtx.GetTypeResolver())
	if err != nil {
		return err
	}
	if supported, err := isTypeSupportedInVersion(version, toType); err != nil {
		return err
	} else if !supported {
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"type %s is not supported until version upgrade is finalized",
			toType.SQLString(),
		)
	}

	if d.IsSerial {
		panic("not implemented")
	}
	col, idx, defaultExpr, err := tabledesc.MakeColumnDefDescs(ctx, d, b.semaCtx, b.evalCtx)
	if err != nil {
		return err
	}
	colID := b.nextColumnID(table)
	col.ID = colID

	// If the new column has a DEFAULT expression that uses a sequence, add
	// references between its descriptor and this column descriptor.
	if d.HasDefaultExpr() {
		if err := b.maybeAddSequenceReferenceDependencies(
			ctx, table.ID, col, defaultExpr,
		); err != nil {
			return err
		}
	}

	if err := b.validateColumnName(table, d, col, t.IfNotExists); err != nil {
		return err
	}

	familyID := descpb.FamilyID(0)
	familyName := string(d.Family.Name)
	// TODO(ajwerner,lucy-zhang): Figure out how to compute the default column ID
	// for the family.
	if d.HasColumnFamily() {
		if familyID, err = b.findOrAddColumnFamily(
			table, familyName, d.Family.Create, d.Family.IfNotExists,
		); err != nil {
			return err
		}
	} else {
		// TODO(ajwerner,lucy-zhang): Deal with adding the first column to the
		// table.
		fam := table.GetFamilies()[0]
		familyID = fam.ID
		familyName = fam.Name
	}

	if d.IsComputed() {
		if d.IsVirtual() {
			return unimplemented.NewWithIssue(57608, "virtual computed columns")
		}

		// TODO (lucy): This is not going to work when the computed column
		// references columns created in the same transaction.
		computedColValidator := schemaexpr.MakeComputedColumnValidator(
			ctx,
			table,
			b.semaCtx,
			tn,
		)
		serializedExpr, err := computedColValidator.Validate(d)
		if err != nil {
			return err
		}
		col.ComputeExpr = &serializedExpr
	}

	b.addNode(scpb.Target_ADD, &scpb.Column{
		TableID:    table.GetID(),
		Column:     *col,
		FamilyID:   familyID,
		FamilyName: familyName,
	})
	newPrimaryIdxID := b.addOrUpdatePrimaryIndexTargetsForAddColumn(table, colID, col.Name)

	if idx != nil {
		idxID := b.nextIndexID(table)
		idx.ID = idxID
		b.addNode(scpb.Target_ADD, &scpb.SecondaryIndex{
			TableID:      table.GetID(),
			Index:        *idx,
			PrimaryIndex: newPrimaryIdxID,
		})
	}
	return nil
}

func (b *Builder) validateColumnName(
	table *tabledesc.Immutable,
	d *tree.ColumnTableDef,
	col *descpb.ColumnDescriptor,
	ifNotExists bool,
) error {
	_, err := table.FindActiveColumnByName(string(d.Name))
	if err == nil {
		if ifNotExists {
			return nil
		}
		return sqlerrors.NewColumnAlreadyExistsError(string(d.Name), table.Name)
	}
	for _, n := range b.nodes {
		switch t := n.Element().(type) {
		case *scpb.Column:
			if t.TableID != table.GetID() || t.Column.Name != string(d.Name) {
				continue
			}
			switch dir := n.Target.Direction; dir {
			case scpb.Target_ADD:
				return pgerror.Newf(pgcode.DuplicateColumn,
					"duplicate: column %q in the middle of being added, not yet public",
					col.Name)
			case scpb.Target_DROP:
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"column %q being dropped, try again later", col.Name)
			default:
				return errors.AssertionFailedf("unknown direction %v in %v", dir, n.Target)
			}
		}
	}
	return nil
}

func (b *Builder) findOrAddColumnFamily(
	table *tabledesc.Immutable, family string, create bool, ifNotExists bool,
) (descpb.FamilyID, error) {
	if len(family) > 0 {
		for i := range table.Families {
			if table.Families[i].Name == family {
				if create && !ifNotExists {
					return 0, errors.Errorf("family %q already exists", family)
				}
				return table.Families[i].ID, nil
			}
		}
	}
	// See if we're in the process of adding a column or dropping a column in this
	// family.
	//
	// TODO(ajwerner): Decide what to do if the only column in a family of this
	// name is being dropped and then if there is or isn't a create directive.
	nextFamilyID := table.NextFamilyID
	for _, n := range b.nodes {
		switch col := n.Element().(type) {
		case *scpb.Column:
			if col.TableID != table.GetID() {
				continue
			}
			if col.FamilyName == family {
				if create && !ifNotExists {
					return 0, errors.Errorf("family %q already exists", family)
				}
				return col.FamilyID, nil
			}
			if col.FamilyID >= nextFamilyID {
				nextFamilyID = col.FamilyID + 1
			}
		}
	}
	if !create {
		return 0, errors.Errorf("unknown family %q", family)
	}
	return nextFamilyID, nil
}

func (b *Builder) alterTableDropColumn(
	ctx context.Context, table *tabledesc.Immutable, t *tree.AlterTableDropColumn,
) error {
	if b.evalCtx.SessionData.SafeUpdates {
		return pgerror.DangerousStatementf("ALTER TABLE DROP COLUMN will " +
			"remove all data in that column")
	}

	// TODO(ajwerner): Deal with drop column for columns which are being added
	// currently.
	colToDrop, _, err := table.FindColumnByName(t.Column)
	if err != nil {
		if t.IfExists {
			// Noop.
			return nil
		}
		return err
	}
	// Check whether the column is being dropped.
	for _, n := range b.nodes {
		switch col := n.Element().(type) {
		case *scpb.Column:
			if col.TableID != table.GetID() ||
				n.Target.Direction != scpb.Target_DROP ||
				col.Column.ColName() != t.Column {
				continue
			}
			// Column drops are, while the column is in the process of being dropped,
			// for whatever reason, idempotent. Return silently here.
			return nil
		}
	}

	// TODO:
	// remove sequence dependencies
	// drop sequences owned by column (if not referenced by other columns)
	// drop view (if cascade specified)
	// check that no computed columns reference this column
	// check that column is not in the PK
	// drop secondary indexes
	// drop all indexes that index/store the column or use it as a partial index predicate
	// drop check constraints
	// remove comments
	// drop foreign keys

	// TODO(ajwerner): Add family information to the column.
	b.addNode(scpb.Target_DROP, &scpb.Column{
		TableID: table.GetID(),
		Column:  *colToDrop,
	})

	b.addOrUpdatePrimaryIndexTargetsForDropColumn(table, colToDrop.ID)
	return nil
}

func (b *Builder) CreateIndex(ctx context.Context, n *tree.CreateIndex) error {
	// TODO: currently indexes are created in sql.MakeIndexDescriptor, but
	// populating the index with IDs, etc. happens in AllocateIDs.
	panic("unimplemented")
}

func (b *Builder) maybeAddSequenceReferenceDependencies(
	ctx context.Context, tableID descpb.ID, col *descpb.ColumnDescriptor, defaultExpr tree.TypedExpr,
) error {
	seqNames, err := sequence.GetUsedSequenceNames(defaultExpr)
	if err != nil {
		return err
	}
	for _, seqName := range seqNames {
		parsedSeqName, err := parser.ParseTableName(seqName)
		if err != nil {
			return err
		}
		tn := parsedSeqName.ToTableName()
		seqDesc, err := resolver.ResolveExistingTableObject(ctx, b.res, &tn,
			tree.ObjectLookupFlagsWithRequired())
		if err != nil {
			return err
		}

		col.UsesSequenceIds = append(col.UsesSequenceIds, seqDesc.ID)
		b.addNode(scpb.Target_ADD, &scpb.SequenceDependency{
			SequenceID: seqDesc.GetID(),
			TableID:    tableID,
			ColumnID:   col.ID,
		})
	}
	return nil
}

func (b *Builder) addOrUpdatePrimaryIndexTargetsForAddColumn(
	table *tabledesc.Immutable, colID descpb.ColumnID, colName string,
) (idxID descpb.IndexID) {
	// Check whether a target to add a PK already exists. If so, update its
	// storing columns.
	for i, n := range b.nodes {
		if t, ok := n.Element().(*scpb.PrimaryIndex); ok &&
			b.nodes[i].Target.Direction == scpb.Target_ADD &&
			t.TableID == table.GetID() {
			t.StoreColumnIDs = append(t.StoreColumnIDs, colID)
			t.StoreColumnNames = append(t.StoreColumnNames, colName)
			return t.Index.ID
		}
	}

	// Create a new primary index, identical to the existing one except for its
	// ID and name.
	idxID = b.nextIndexID(table)
	newIdx := protoutil.Clone(&table.PrimaryIndex).(*descpb.IndexDescriptor)
	newIdx.Name = tabledesc.GenerateUniqueConstraintName(
		"new_primary_key",
		func(name string) bool {
			// TODO (lucy): Also check the new indexes specified in the targets.
			_, err := table.FindIndexWithName(name)
			return err == nil
		},
	)
	newIdx.ID = idxID

	var storeColIDs []descpb.ColumnID
	var storeColNames []string
	for _, col := range table.Columns {
		containsCol := false
		for _, id := range newIdx.ColumnIDs {
			if id == col.ID {
				containsCol = true
				break
			}
		}
		if !containsCol {
			storeColIDs = append(storeColIDs, col.ID)
			storeColNames = append(storeColNames, col.Name)
		}
	}

	b.addNode(scpb.Target_ADD, &scpb.PrimaryIndex{
		TableID:           table.GetID(),
		Index:             *newIdx,
		OtherPrimaryIndex: table.GetPrimaryIndexID(),
		StoreColumnIDs:    append(storeColIDs, colID),
		StoreColumnNames:  append(storeColNames, colName),
	})

	// Drop the existing primary index.
	b.addNode(scpb.Target_DROP, &scpb.PrimaryIndex{
		TableID:           table.GetID(),
		Index:             *(protoutil.Clone(&table.PrimaryIndex).(*descpb.IndexDescriptor)),
		OtherPrimaryIndex: newIdx.ID,
		StoreColumnIDs:    storeColIDs,
		StoreColumnNames:  storeColNames,
	})

	return idxID
}

// TODO (lucy): refactor this to share with the add column case.
func (b *Builder) addOrUpdatePrimaryIndexTargetsForDropColumn(
	table *tabledesc.Immutable, colID descpb.ColumnID,
) (idxID descpb.IndexID) {
	// Check whether a target to add a PK already exists. If so, update its
	// storing columns.
	for _, n := range b.nodes {
		if t, ok := n.Element().(*scpb.PrimaryIndex); ok &&
			n.Target.Direction == scpb.Target_ADD &&
			t.TableID == table.GetID() {
			for j := range t.StoreColumnIDs {
				if t.StoreColumnIDs[j] == colID {
					t.StoreColumnIDs = append(t.StoreColumnIDs[:j], t.StoreColumnIDs[j+1:]...)
					t.StoreColumnNames = append(t.StoreColumnNames[:j], t.StoreColumnNames[j+1:]...)
					return t.Index.ID
				}
				panic("index not found")
			}
		}
	}

	// Create a new primary index, identical to the existing one except for its
	// ID and name.
	idxID = b.nextIndexID(table)
	newIdx := protoutil.Clone(&table.PrimaryIndex).(*descpb.IndexDescriptor)
	newIdx.Name = tabledesc.GenerateUniqueConstraintName(
		"new_primary_key",
		func(name string) bool {
			// TODO (lucy): Also check the new indexes specified in the targets.
			_, err := table.FindIndexWithName(name)
			return err == nil
		},
	)
	newIdx.ID = idxID

	var addStoreColIDs []descpb.ColumnID
	var addStoreColNames []string
	var dropStoreColIDs []descpb.ColumnID
	var dropStoreColNames []string
	for _, col := range table.Columns {
		containsCol := false
		for _, id := range newIdx.ColumnIDs {
			if id == col.ID {
				containsCol = true
				break
			}
		}
		if !containsCol {
			if colID != col.ID {
				addStoreColIDs = append(addStoreColIDs, col.ID)
				addStoreColNames = append(addStoreColNames, col.Name)
			}
			dropStoreColIDs = append(dropStoreColIDs, col.ID)
			dropStoreColNames = append(dropStoreColNames, col.Name)
		}
	}

	b.addNode(scpb.Target_ADD, &scpb.PrimaryIndex{
		TableID:           table.GetID(),
		Index:             *newIdx,
		OtherPrimaryIndex: table.GetPrimaryIndexID(),
		StoreColumnIDs:    addStoreColIDs,
		StoreColumnNames:  addStoreColNames,
	})

	// Drop the existing primary index.
	b.addNode(scpb.Target_DROP, &scpb.PrimaryIndex{
		TableID:           table.GetID(),
		Index:             *(protoutil.Clone(&table.PrimaryIndex).(*descpb.IndexDescriptor)),
		OtherPrimaryIndex: idxID,
		StoreColumnIDs:    dropStoreColIDs,
		StoreColumnNames:  dropStoreColNames,
	})
	return idxID
}

func (b *Builder) nextColumnID(table *tabledesc.Immutable) descpb.ColumnID {
	nextColID := table.GetNextColumnID()
	var maxColID descpb.ColumnID

	for _, n := range b.nodes {
		if n.Target.Direction != scpb.Target_ADD || n.Element().DescriptorID() != table.GetID() {
			continue
		}
		if ac, ok := n.Element().(*scpb.Column); ok {
			if ac.Column.ID > maxColID {
				maxColID = ac.Column.ID
			}
		}
	}
	if maxColID != 0 {
		nextColID = maxColID + 1
	}
	return nextColID
}

func (b *Builder) nextIndexID(table *tabledesc.Immutable) descpb.IndexID {
	nextMaxID := table.GetNextIndexID()
	var maxIdxID descpb.IndexID
	for _, n := range b.nodes {
		if n.Target.Direction != scpb.Target_ADD || n.Element().DescriptorID() != table.GetID() {
			continue
		}
		if ai, ok := n.Element().(*scpb.SecondaryIndex); ok {
			if ai.Index.ID > maxIdxID {
				maxIdxID = ai.Index.ID
			}
		} else if ai, ok := n.Element().(*scpb.PrimaryIndex); ok {
			if ai.Index.ID > maxIdxID {
				maxIdxID = ai.Index.ID
			}
		}
	}
	if maxIdxID != 0 {
		nextMaxID = maxIdxID + 1
	}
	return nextMaxID
}

func (b *Builder) addNode(dir scpb.Target_Direction, elem scpb.Element) {
	var s scpb.State
	switch dir {
	case scpb.Target_ADD:
		s = scpb.State_ABSENT
	case scpb.Target_DROP:
		s = scpb.State_PUBLIC
	default:
		panic(errors.Errorf("unknown direction %s", dir))
	}
	b.nodes = append(b.nodes, &scpb.Node{
		Target: scpb.NewTarget(dir, elem),
		State:  s,
	})
}

// minimumTypeUsageVersions defines the minimum version needed for a new
// data type.
var minimumTypeUsageVersions = map[types.Family]clusterversion.Key{
	types.GeographyFamily: clusterversion.GeospatialType,
	types.GeometryFamily:  clusterversion.GeospatialType,
	types.Box2DFamily:     clusterversion.Box2DType,
}

// isTypeSupportedInVersion returns whether a given type is supported in the given version.
// This is copied straight from the sql package.
func isTypeSupportedInVersion(v clusterversion.ClusterVersion, t *types.T) (bool, error) {
	// For these checks, if we have an array, we only want to find whether
	// we support the array contents.
	if t.Family() == types.ArrayFamily {
		t = t.ArrayContents()
	}

	minVersion, ok := minimumTypeUsageVersions[t.Family()]
	if !ok {
		return true, nil
	}
	return v.IsActive(minVersion), nil
}
