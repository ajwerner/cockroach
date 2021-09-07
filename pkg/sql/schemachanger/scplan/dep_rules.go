package scplan

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/errors"
)

type depRegistry struct {
	rules []depRule
}

type depRule struct {
	name     string
	from, to rel.Var
	q        *rel.Query
}

func (r *depRegistry) Register(ruleName string, from, to rel.Var, query *rel.Query) {
	r.rules = append(r.rules, depRule{
		name: ruleName,
		from: from,
		to:   to,
		q:    query,
	})
}

var depRules depRegistry

func init() {
	targetNodeVars := func(el rel.Var) (element, target, node rel.Var) {
		return el, el + "-target", el + "-node"
	}
	var (
		parent, parentTarget, parentNode         = targetNodeVars("parent")
		other, otherTarget, otherNode            = targetNodeVars("other")
		status, direction                rel.Var = "status", "direction"
	)
	depRules.Register(
		"database dependencies",
		parentNode, otherNode,
		rel.MustQuery(screl.Schema,
			direction.Eq(Target_DROP),
			status.EqAny(Status_DELETE_ONLY, Status_DELETE_AND_WRITE_ONLY),

			parent.Type((*Database)(nil)),
			other.Type(
				(*Type)(nil), (*Table)(nil), (*View)(nil), (*Sequence)(nil),
				(*Schema)(nil),
			),
			screl.JoinTargetNode(parent, parentTarget, parentNode),
			screl.JoinTargetNode(other, otherTarget, otherNode),

			direction.Entities(screl.Direction, parentTarget, otherTarget),
			status.Entities(screl.Status, parentNode, otherNode),

			rel.Filter(parent, other)(func(db *Database, other Element) bool {
				return idInIDs(db.DependentObjects, screl.GetDescID(other))
			}),
		),
	)

	depRules.Register(
		"schema dependencies",
		parentNode, otherNode,
		rel.MustQuery(screl.Schema,
			parent.Type((*Schema)(nil)),
			other.Type(
				(*Type)(nil), (*Table)(nil), (*View)(nil), (*Sequence)(nil),
			),

			screl.JoinTargetNode(parent, parentTarget, parentNode),
			screl.JoinTargetNode(other, otherTarget, otherNode),

			direction.Eq(Target_DROP),
			direction.Entities(screl.Direction, parentTarget, otherTarget),

			status.EqAny(Status_DELETE_ONLY, Status_DELETE_AND_WRITE_ONLY),
			status.Entities(screl.Status, parentNode, otherNode),

			rel.Filter(parent, other)(func(sc *Schema, other Element) bool {
				return idInIDs(sc.DependentObjects, screl.GetDescID(other))
			}),
		),
	)

	var (
		ownedBy, ownedByTarget, ownedByNode = targetNodeVars("owned-by")
		seq, seqTarget, seqNode             = targetNodeVars("seq")
		id                                  = rel.Var("id")
	)
	depRules.Register(
		"sequence owned by being dropped relies on sequence entering delete only",
		ownedByNode, seqNode,
		rel.MustQuery(screl.Schema,
			ownedBy.Type((*SequenceOwnedBy)(nil)),
			seq.Type((*Sequence)(nil)),

			id.Entities(screl.DescID, ownedBy, seq),

			screl.JoinTargetNode(ownedBy, ownedByTarget, ownedByNode),
			screl.JoinTargetNode(seq, seqTarget, seqNode),

			direction.Eq(Target_DROP),
			direction.Entities(screl.Direction, ownedByTarget, seqTarget),

			status.Eq(Status_ABSENT),
			status.Entities(screl.Status, ownedByNode, seqNode),
		))

	var (
		typ, typTarget, typNode          = targetNodeVars("type")
		typRef, typRefTarget, typRefNode = targetNodeVars("type-ref")
	)
	depRules.Register(
		"type reference something",
		typNode, typRefNode,
		rel.MustQuery(screl.Schema,
			typ.Type((*Type)(nil)),
			typRef.Type((*TypeReference)(nil)),

			typ.Attr(screl.DescID, id),
			typRef.Attr(screl.ReferencedDescID, id),

			screl.JoinTargetNode(typ, typTarget, typNode),
			screl.JoinTargetNode(typRef, typRefTarget, typRefNode),

			direction.Eq(Target_DROP),
			direction.Entities(screl.Direction, typTarget, typRefTarget),

			typNode.Attr(screl.Status, rel.Value(Status_PUBLIC)),
			typRefNode.Attr(screl.Status, rel.Value(Status_DELETE_ONLY))),
	)

	/*
		// TODO(ajwerner): What does this even mean? The sequence starts in
		// public.
		depRules.Register(
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

	var (
		from, fromTarget, fromNode = targetNodeVars("from")
		to, toTarget, toNode       = targetNodeVars("to")
	)
	depRules.Register(
		"view depends on view",
		fromNode, toNode,
		rel.MustQuery(screl.Schema,
			direction.Eq(Target_DROP),
			status.Eq(Status_ABSENT),

			from.Type((*View)(nil)),
			screl.JoinTargetNode(from, fromTarget, fromNode),

			to.Type((*View)(nil)),
			screl.JoinTargetNode(to, toTarget, toNode),

			direction.Entities(screl.Direction, fromTarget, toTarget),
			status.Entities(screl.Status, fromNode, toNode),

			rel.Filter(from, to)(func(from, to *View) bool {
				return from != to && idInIDs(from.DependedOnBy, to.TableID)
			}),
		),
	)

	depRules.Register(
		"view depends on type",
		fromNode, toNode,
		rel.MustQuery(screl.Schema,
			direction.Eq(Target_DROP),
			status.Eq(Status_ABSENT),

			from.Type((*View)(nil)),
			to.Type((*TypeReference)(nil)),

			from.Attr(screl.DescID, id),
			to.Attr(screl.ReferencedDescID, id),

			screl.JoinTargetNode(from, fromTarget, fromNode),
			screl.JoinTargetNode(to, toTarget, toNode),

			direction.Entities(screl.Direction, fromTarget, toTarget),
			status.Entities(screl.Status, fromNode, toNode),
		),
	)

	var (
		column, columnTarget, columnNode = targetNodeVars("column")
		index, indexTarget, indexNode    = targetNodeVars("index")
	)
	depRules.Register(
		"column depends on indexes",
		columnNode, indexNode,
		rel.MustQuery(screl.Schema,

			status.EqAny(Status_DELETE_AND_WRITE_ONLY, Status_PUBLIC),
			direction.Eq(Target_ADD),

			column.Type((*Column)(nil)),
			index.Type((*PrimaryIndex)(nil), (*SecondaryIndex)(nil)),

			id.Entities(screl.DescID, column, index),

			direction.Entities(screl.Direction, columnTarget, indexTarget),
			status.Entities(screl.Status, columnNode, indexNode),

			rel.Filter(column, index)(func(from *Column, to Entity) bool {
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
		),
	)

	var (
		add, addTarget, addNode    = targetNodeVars("add")
		drop, dropTarget, dropNode = targetNodeVars("drop")
	)
	primaryIndexReferenceEachOther := rel.MustQuery(screl.Schema,
		add.Type((*PrimaryIndex)(nil)),
		drop.Type((*PrimaryIndex)(nil)),
		id.Entities(screl.DescID, add, drop),

		screl.JoinTargetNode(add, addTarget, addNode),
		addTarget.Attr(screl.Direction, rel.Value(Target_ADD)),
		addNode.Attr(screl.Status, rel.Value(Status_PUBLIC)),

		screl.JoinTargetNode(drop, dropTarget, dropNode),
		dropTarget.Attr(screl.Direction, rel.Value(Target_DROP)),
		dropNode.Attr(screl.Status, rel.Value(Status_DELETE_AND_WRITE_ONLY)),

		id.Entities(screl.DescID, column, index),

		direction.Entities(screl.Direction, columnTarget, indexTarget),
		status.Entities(screl.Status, columnNode, indexNode),

		rel.Filter("add", "drop")(func(add, drop *PrimaryIndex) bool {
			return add.OtherPrimaryIndexID == drop.Index.ID
		}),
	)

	depRules.Register(
		"primary index add depends on drop",
		addNode, dropNode,
		primaryIndexReferenceEachOther,
	)
	depRules.Register(
		"primary index drop depends on add",
		dropNode, addNode,
		primaryIndexReferenceEachOther,
	)
}
