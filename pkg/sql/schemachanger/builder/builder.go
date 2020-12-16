package builder

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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
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

	targetStates []targets.TargetState
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
	ctx context.Context, ts []targets.TargetState, n *tree.AlterTable,
) ([]targets.TargetState, error) {
	// TODO (lucy): Clean this up.
	b.targetStates = ts
	defer func() {
		b.targetStates = nil
	}()

	// Resolve the table.
	tn := n.Table.ToTableName()
	table, err := resolver.ResolveExistingTableObject(ctx, b.res, &tn,
		tree.ObjectLookupFlagsWithRequired())
	if errors.Is(err, catalog.ErrDescriptorNotFound) && n.IfExists {
		return nil, err
	}
	for _, cmd := range n.Cmds {
		if err := b.alterTableCmd(ctx, table, cmd, &tn); err != nil {
			return nil, err
		}
	}

	result := make([]targets.TargetState, len(b.targetStates))
	for i := range b.targetStates {
		result[i] = b.targetStates[i]
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
		if err := b.maybeAddSequenceDependencies(ctx, table.ID, col, defaultExpr); err != nil {
			return err
		}
	}

	if err := b.validateColumnName(table, d, col, t.IfNotExists); err != nil {
		return err
	}

	b.addTargetState(
		&targets.AddColumn{
			TableID: table.GetID(),
			Column:  *col,
		},
		targets.State_ABSENT,
	)

	newPrimaryIdxID := b.addOrUpdatePrimaryIndexTargetsForColumnChange(table, colID, col.Name)

	if idx != nil {
		idxID := b.nextIndexID(table)
		idx.ID = idxID
		b.addTargetState(
			&targets.AddIndex{
				TableID:      table.GetID(),
				Index:        *idx,
				PrimaryIndex: newPrimaryIdxID,
			},
			targets.State_ABSENT,
		)
	}

	if d.HasColumnFamily() {
		// TODO (lucy): Also assign the column ID on the column descriptor?
		if err := b.maybeAddColumnFamily(
			table, string(d.Family.Name), d.Family.Create, d.Family.IfNotExists,
		); err != nil {
			return err
		}
	}

	if d.IsComputed() {
		if d.IsVirtual() {
			return unimplemented.NewWithIssue(57608, "virtual computed columns")
		}
		computedColValidator := schemaexpr.MakeComputedColumnValidator(
			ctx,
			table,
			b.semaCtx,
			tn,
		)
		if err := computedColValidator.Validate(d); err != nil {
			return err
		}
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
	for _, ts := range b.targetStates {
		switch t := ts.Target.(type) {
		case *targets.AddColumn:
			if t.TableID == table.GetID() && t.Column.Name == string(d.Name) {
				return pgerror.Newf(pgcode.DuplicateColumn,
					"duplicate: column %q in the middle of being added, not yet public",
					col.Name)
			}
		case *targets.DropColumn:
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"column %q being dropped, try again later", col.Name)
		}
	}
	return nil
}

func (b *Builder) maybeAddColumnFamily(
	table *tabledesc.Immutable, family string, create bool, ifNotExists bool,
) error {
	idx := -1
	if len(family) > 0 {
		for i := range table.Families {
			if table.Families[i].Name == family {
				idx = i
				break
			}
		}
	}

	if idx == -1 {
		if !create {
			return errors.Errorf("unknown family %q", family)
		}
		b.addTargetState(&targets.AddColumnFamily{
			TableID: table.GetID(),
			Family: descpb.ColumnFamilyDescriptor{
				Name:            family,
				ID:              b.nextFamilyID(table),
				ColumnNames:     []string{},
				ColumnIDs:       []descpb.ColumnID{},
				DefaultColumnID: 0,
			},
		}, targets.State_ABSENT)
		return nil
	}
	if create && !ifNotExists {
		return errors.Errorf("family %q already exists", family)
	}
	return nil
}

func (b *Builder) alterTableDropColumn(
	ctx context.Context, table *tabledesc.Immutable, t *tree.AlterTableDropColumn,
) error {
	panic("unimplemented")

	// colToDrop, dropped, err := table.FindColumnByName(t.Column)
	// if err != nil {
	// 	if t.IfExists {
	// 		// Noop.
	// 		return nil
	// 	}
	// 	return err
	// }
	// // TODO: this check should be coming from the targets, not the descriptor.
	// if dropped {
	// 	return err
	// }
	//
	// // TODO: validation, cascades, etc.
	//
	// Assume we've validated that the column isn't part of the PK.
	// b.addOrUpdatePrimaryIndexTargetsForColumnChange(table)
	// b.addTargetState(
	// 	&targets.DropColumn{
	// 		TableID:  table.GetID(),
	// 		ColumnID: colToDrop.ID,
	// 	},
	// 	targets.State_PUBLIC,
	// )
	return nil
}

func (b *Builder) CreateIndex(ctx context.Context, n *tree.CreateIndex) error {
	// TODO: currently indexes are created in sql.MakeIndexDescriptor, but
	// populating the index with IDs, etc. happens in AllocateIDs.
	panic("unimplemented")
}

func (b *Builder) maybeAddSequenceDependencies(
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
		b.addTargetState(
			&targets.AddSequenceDependency{
				TableID:    tableID,
				ColumnID:   col.ID,
				SequenceID: seqDesc.GetID(),
			},
			targets.State_ABSENT,
		)
	}
	return nil
}

func (b *Builder) addOrUpdatePrimaryIndexTargetsForColumnChange(
	table *tabledesc.Immutable, colID descpb.ColumnID, colName string,
) (idxID descpb.IndexID) {
	// Check whether a target to add a PK already exists. If so, update its
	// storing columns.
	for i := range b.targetStates {
		if t, ok := b.targetStates[i].Target.(*targets.AddPrimaryIndex); ok &&
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
		for _, colID := range newIdx.ColumnIDs {
			if colID == col.ID {
				containsCol = true
				break
			}
		}
		if !containsCol {
			storeColIDs = append(storeColIDs, col.ID)
			storeColNames = append(storeColNames, col.Name)
		}
	}

	b.addTargetState(
		&targets.AddPrimaryIndex{
			TableID:          table.GetID(),
			Index:            *newIdx,
			PrimaryIndex:     table.GetPrimaryIndexID(),
			ReplacementFor:   table.GetPrimaryIndexID(),
			StoreColumnIDs:   append(storeColIDs, colID),
			StoreColumnNames: append(storeColNames, colName),
		},
		targets.State_ABSENT,
	)

	// Drop the existing primary index.
	b.addTargetState(
		&targets.DropPrimaryIndex{
			TableID:          table.GetID(),
			Index:            *(protoutil.Clone(&table.PrimaryIndex).(*descpb.IndexDescriptor)),
			ReplacedBy:       idxID,
			StoreColumnIDs:   storeColIDs,
			StoreColumnNames: storeColNames,
		},
		targets.State_PUBLIC,
	)

	return idxID
}

func (b *Builder) nextColumnID(table *tabledesc.Immutable) descpb.ColumnID {
	nextColID := table.GetNextColumnID()
	var maxColID descpb.ColumnID
	for _, ts := range b.targetStates {
		if ac, ok := ts.Target.(*targets.AddColumn); ok && ac.TableID == table.GetID() {
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
	for _, ts := range b.targetStates {
		if ai, ok := ts.Target.(*targets.AddIndex); ok && ai.TableID == table.GetID() {
			if ai.Index.ID > maxIdxID {
				maxIdxID = ai.Index.ID
			}
		} else if ai, ok := ts.Target.(*targets.AddPrimaryIndex); ok && ai.TableID == table.GetID() {
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

func (b *Builder) nextFamilyID(table *tabledesc.Immutable) descpb.FamilyID {
	nextMaxID := table.GetNextFamilyID()
	var maxFamilyID descpb.FamilyID
	for _, ts := range b.targetStates {
		if af, ok := ts.Target.(*targets.AddColumnFamily); ok &&
			af.TableID == table.GetID() {
			if af.Family.ID > maxFamilyID {
				maxFamilyID = af.Family.ID
			}
		}
	}
	if maxFamilyID != 0 {
		nextMaxID = maxFamilyID + 1
	}
	return nextMaxID
}

func (b *Builder) addTargetState(t targets.Target, s targets.State) {
	b.targetStates = append(b.targetStates, targets.TargetState{
		Target: t,
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
