package deprules

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/errors"
)

func targetNodeVars(el rel.Var) (element, target, node rel.Var) {
	return el, el + "-target", el + "-node"
}

func init() {
	var (
		parent, parentTarget, parentNode         = targetNodeVars("parent")
		other, otherTarget, otherNode            = targetNodeVars("other")
		status, direction                rel.Var = "status", "direction"
	)
	register(
		"database dependencies",
		parentNode, otherNode,
		rel.MustQuery(screl.Schema,
			direction.Eq(rel.Value(Target_DROP)),
			status.Eq(rel.Any(Status_DELETE_ONLY, Status_DELETE_AND_WRITE_ONLY)),

			parent.Type((*Database)(nil)),
			other.Type(
				(*Type)(nil), (*Table)(nil), (*View)(nil), (*Sequence)(nil),
				(*Schema)(nil),
			),

			rel.Filter(
				"depends-on", parent, other,
			)(func(db *Database, other Element) bool {
				return idInIDs(db.DependentObjects, screl.GetDescID(other))
			}),

			direction.Entities(screl.Direction, parentTarget, otherTarget),
			status.Entities(screl.Status, parentNode, otherNode),

			screl.JoinTargetNode(parent, parentTarget, parentNode),
			screl.JoinTargetNode(other, otherTarget, otherNode),
		),
	)

	register(
		"schema dependencies",
		parentNode, otherNode,
		rel.MustQuery(screl.Schema,
			direction.Eq(rel.Value(Target_DROP)),
			status.Eq(rel.Any(Status_DELETE_ONLY, Status_DELETE_AND_WRITE_ONLY)),

			parent.Type((*Schema)(nil)),
			other.Type(
				(*Type)(nil), (*Table)(nil), (*View)(nil), (*Sequence)(nil),
			),

			rel.Filter(
				"schema-depends-on", parent, other,
			)(func(
				sc *Schema, other Element,
			) bool {
				return idInIDs(sc.DependentObjects, screl.GetDescID(other))
			}),

			direction.Entities(screl.Direction, parentTarget, otherTarget),
			status.Entities(screl.Status, parentNode, otherNode),

			screl.JoinTargetNode(parent, parentTarget, parentNode),
			screl.JoinTargetNode(other, otherTarget, otherNode),
		),
	)
}

func init() {
	ownedBy, ownedByTarget, ownedByNode := targetNodeVars("owned-by")
	seq, seqTarget, seqNode := targetNodeVars("seq")
	var id, direction rel.Var = "id", "direction"
	register(
		"sequence owned by being dropped relies on sequence entering delete only",
		ownedByNode, seqNode,
		rel.MustQuery(screl.Schema,
			direction.Eq(rel.Value(Target_DROP)),

			ownedBy.Type((*SequenceOwnedBy)(nil)),
			seq.Type((*Sequence)(nil)),

			id.Entities(screl.DescID, ownedBy, seq),

			direction.Entities(screl.Direction, ownedByTarget, seqTarget),
			seqNode.Attr(screl.Status, rel.Value(Status_ABSENT)),
			ownedByNode.Attr(screl.Status, rel.Value(Status_DELETE_ONLY)),

			screl.JoinTargetNode(ownedBy, ownedByTarget, ownedByNode),
			screl.JoinTargetNode(seq, seqTarget, seqNode),
		))
}

func init() {

	typ, typTarget, typNode := targetNodeVars("type")
	typRef, typRefTarget, typRefNode := targetNodeVars("type-ref")
	var id, direction rel.Var = "id", "direction"
	register(
		"type reference something",
		typNode, typRefNode,
		rel.MustQuery(screl.Schema,
			direction.Eq(rel.Value(Target_DROP)),

			typ.Type((*Type)(nil)),
			typRef.Type((*TypeReference)(nil)),

			typ.Attr(screl.DescID, id),
			typRef.Attr(screl.ReferencedDescID, id),

			direction.Entities(screl.Direction, typTarget, typRefTarget),

			typNode.Attr(screl.Status, rel.Value(Status_PUBLIC)),
			typRefNode.Attr(screl.Status, rel.Value(Status_DELETE_ONLY)),

			screl.JoinTargetNode(typ, typTarget, typNode),
			screl.JoinTargetNode(typRef, typRefTarget, typRefNode),
		),
	)
}

func init() {
	from, fromTarget, fromNode := targetNodeVars("from")
	to, toTarget, toNode := targetNodeVars("to")
	var id, status, direction rel.Var = "id", "status", "direction"
	register(
		"view depends on view",
		fromNode, toNode,
		rel.MustQuery(screl.Schema,
			direction.Eq(rel.Value(Target_DROP)),
			status.Eq(rel.Value(Status_ABSENT)),

			from.Type((*View)(nil)),
			to.Type((*View)(nil)),
			rel.Filter(
				"depended-on-by",
				from, to,
			)(func(from, to *View) bool {
				return from != to && idInIDs(from.DependedOnBy, to.TableID)
			}),

			direction.Entities(screl.Direction, fromTarget, toTarget),
			status.Entities(screl.Status, fromNode, toNode),

			screl.JoinTargetNode(from, fromTarget, fromNode),
			screl.JoinTargetNode(to, toTarget, toNode),
		),
	)

	register(
		"view depends on type",
		fromNode, toNode,
		rel.MustQuery(screl.Schema,
			direction.Eq(rel.Value(Target_DROP)),
			status.Eq(rel.Value(Status_ABSENT)),

			from.Type((*View)(nil)),
			to.Type((*TypeReference)(nil)),

			from.Attr(screl.DescID, id),
			to.Attr(screl.DescID, id),

			direction.Entities(screl.Direction, fromTarget, toTarget),
			status.Entities(screl.Status, fromNode, toNode),

			screl.JoinTargetNode(from, fromTarget, fromNode),
			screl.JoinTargetNode(to, toTarget, toNode),
		),
	)
}

func init() {

	column, columnTarget, columnNode := targetNodeVars("column")
	index, indexTarget, indexNode := targetNodeVars("index")
	var id, status, direction rel.Var = "id", "status", "direction"
	register(
		"column depends on indexes",
		columnNode, indexNode,
		rel.MustQuery(screl.Schema,

			status.Eq(rel.Any(Status_DELETE_AND_WRITE_ONLY, Status_PUBLIC)),
			direction.Eq(rel.Value(Target_ADD)),

			column.Type((*Column)(nil)),
			index.Type((*PrimaryIndex)(nil), (*SecondaryIndex)(nil)),

			id.Entities(screl.DescID, column, index),

			direction.Entities(screl.Direction, columnTarget, indexTarget),
			status.Entities(screl.Status, columnNode, indexNode),

			rel.Filter(
				"column-in-index", column, index,
			)(func(from *Column, to Entity) bool {
				var idx *descpb.IndexDescriptor
				switch to := to.GetElement().(type) {
				case *PrimaryIndex:
					idx = &to.Index
				case *SecondaryIndex:
					idx = &to.Index
				default:
					panic(errors.AssertionFailedf("unexpected type %T", to))
				}
				return indexContainsColumn(idx, from.Column.ID)
			}),

			screl.JoinTargetNode(column, columnTarget, columnNode),
			screl.JoinTargetNode(index, indexTarget, indexNode),
		),
	)
}

func init() {

	add, addTarget, addNode := targetNodeVars("add")
	drop, dropTarget, dropNode := targetNodeVars("drop")
	var id rel.Var = "id"
	primaryIndexReferenceEachOther := rel.MustQuery(screl.Schema,
		add.Type((*PrimaryIndex)(nil)),
		drop.Type((*PrimaryIndex)(nil)),
		id.Entities(screl.DescID, add, drop),

		rel.Filter(
			"reference-each-other", "add", "drop",
		)(func(add, drop *PrimaryIndex) bool {
			return add.OtherPrimaryIndexID == drop.Index.ID
		}),

		addTarget.Attr(screl.Direction, rel.Value(Target_ADD)),
		dropTarget.Attr(screl.Direction, rel.Value(Target_DROP)),
		addNode.Attr(screl.Status, rel.Value(Status_PUBLIC)),
		dropNode.Attr(screl.Status, rel.Value(Status_DELETE_AND_WRITE_ONLY)),

		screl.JoinTargetNode(add, addTarget, addNode),
		screl.JoinTargetNode(drop, dropTarget, dropNode),
	)

	register(
		"primary index add depends on drop",
		addNode, dropNode,
		primaryIndexReferenceEachOther,
	)
	register(
		"primary index drop depends on add",
		dropNode, addNode,
		primaryIndexReferenceEachOther,
	)
}

/*
	// TODO(ajwerner): What does this even mean? The sequence starts in
	// public.
	register(
		"sequence default expr",
		"seq", "def_expr",
		q.MustBuild(func(b q.Builder) {
			q.Constrain(b, "seq", []q.AttributeValue{
				{Status, PublicStatus},
				{Direction, DropDirection},
				{AttrElementType, SequenceElement},
			})
			q.Constrain(b, "def_expr", []q.AttributeValue{
				{Status, AbsentStatus},
				{Direction, DropDirection},
				{AttrElementType, DefaultExpressionElement},
			})
			b.Filter(makeFilter(b, []string{
				"seq", "def_expr",
			}, func(seq *Sequence, defExpr *DefaultExpression) bool {
				return defaultExprReferencesColumn(seq, defExpr)
			}))
		}))
*/
