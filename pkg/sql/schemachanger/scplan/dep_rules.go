package scplan

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav"
	q "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav/eavquery"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/errors"
)

type depRegistry struct {
	rules []depRule
}

type depRule struct {
	name     string
	from, to string
	q        *q.Query
}

func (r *depRegistry) Register(ruleName, from, to string, query *q.Query) {
	r.rules = append(r.rules, depRule{
		name: ruleName,
		from: from,
		to:   to,
		q:    query,
	})
}

var depRules depRegistry

func init() {
	depRules.Register(
		"database dependencies",
		"db", "other",
		q.MustBuild(func(b q.Builder) {
			q.Constrain(b, "db", []q.AttributeValue{
				{AttrStatus, DeleteOnlyStatus},
				{AttrDirection, DropDirection},
				{AttrElementType, DatabaseElement},
			})
			q.Constrain(b, "other", []q.AttributeValue{
				{AttrStatus, AbsentStatus},
				{AttrDirection, DropDirection},
				{AttrElementType, q.Any(
					TypeElement,
					TableElement,
					ViewElement,
					SequenceElement,
					SchemaElement,
				)},
			})
			b.Filter(makeFilter(b, []string{
				"db", "other",
			}, func(db *Database, other Entity) bool {
				return idInIDs(db.DependentObjects, GetDescID(other))
			}))
		}))

	depRules.Register(
		"schema dependencies",
		"schema", "other",
		q.MustBuild(func(b q.Builder) {
			q.Constrain(b, "schema", []q.AttributeValue{
				{AttrStatus, DeleteOnlyStatus},
				{AttrDirection, DropDirection},
				{AttrElementType, SchemaElement},
			})
			q.Constrain(b, "other", []q.AttributeValue{
				{AttrStatus, AbsentStatus},
				{AttrDirection, DropDirection},
				{AttrElementType, q.Any(
					TypeElement,
					TableElement,
					ViewElement,
					SequenceElement,
				)},
			})
			b.Filter(makeFilter(b, []string{
				"schema", "other",
			}, func(schema *Schema, other Entity) bool {
				return idInIDs(schema.DependentObjects, GetDescID(other))
			}))
		}))

	depRules.Register(
		"sequence owned by being dropped relies on sequence entering delete only",
		"owned_by", "seq",
		q.MustBuild(func(b q.Builder) {
			ownedBy := q.Constrain(b, "owned_by", []q.AttributeValue{
				{AttrStatus, AbsentStatus},
				{AttrDirection, DropDirection},
				{AttrElementType, SequenceOwnerElement},
			})
			q.Constrain(b, "seq", []q.AttributeValue{
				{AttrStatus, DeleteOnlyStatus},
				{AttrDirection, DropDirection},
				{AttrElementType, SequenceElement},
				{AttrDescID, ownedBy.Reference(AttrDescID)},
			})
		}))

	// TODO(ajwerner): What does this even mean?
	depRules.Register(
		"type reference something",
		"type", "type_ref",
		q.MustBuild(func(b q.Builder) {
			typ := q.Constrain(b, "type", []q.AttributeValue{
				{AttrStatus, PublicStatus},
				{AttrDirection, DropDirection},
			})
			q.Constrain(b, "type_ref", []q.AttributeValue{
				{AttrStatus, DeleteOnlyStatus},
				{AttrDirection, DropDirection},
				{AttrReferencedDescID, typ.Reference(AttrDescID)},
			})
		}))

	// TODO(ajwerner): What does this even mean? The sequence starts in
	// public.
	depRules.Register(
		"sequence default expr",
		"seq", "def_expr",
		q.MustBuild(func(b q.Builder) {
			q.Constrain(b, "seq", []q.AttributeValue{
				{AttrStatus, PublicStatus},
				{AttrDirection, DropDirection},
				{AttrElementType, SequenceElement},
			})
			q.Constrain(b, "def_expr", []q.AttributeValue{
				{AttrStatus, AbsentStatus},
				{AttrDirection, DropDirection},
				{AttrElementType, DefaultExpressionElement},
			})
			b.Filter(makeFilter(b, []string{
				"seq", "def_expr",
			}, func(seq *Sequence, defExpr *DefaultExpression) bool {
				return defaultExprReferencesColumn(seq, defExpr)
			}))
		}))

	dropViewAbsent := []q.AttributeValue{
		{AttrElementType, ViewElement},
		{AttrDirection, DropDirection},
		{AttrStatus, AbsentStatus},
	}
	depRules.Register(
		"view depends on view",
		"from", "to",
		q.MustBuild(func(b q.Builder) {
			q.Constrain(b, "from", dropViewAbsent)
			q.Constrain(b, "to", dropViewAbsent)
			b.Filter(makeFilter(b, []string{
				"from", "to",
			}, func(from *View, to Entity) bool {
				toID := GetDescID(to)
				return GetDescID(from) != toID && idInIDs(from.DependedOnBy, toID)
			}))
		}),
	)
	depRules.Register(
		"view depends on type",
		"from", "to",
		q.MustBuild(func(b q.Builder) {
			from := q.Constrain(b, "from", dropViewAbsent)
			q.Constrain(b, "to", []q.AttributeValue{
				{AttrElementType, TypeRefElement},
				{AttrDirection, DropDirection},
				{AttrStatus, AbsentStatus},
				{AttrDescID, from.Reference(AttrDescID)},
			})
		}))

	depRules.Register(
		"column depends on indexes",
		"from", "to",
		q.MustBuild(func(b q.Builder) {
			from := q.Constrain(b, "from", []q.AttributeValue{
				{AttrElementType, ColumnElement},
				{AttrDirection, AddDirection},
				{AttrStatus, q.Any(DeleteAndWriteOnlyStatus, PublicStatus)},
			})
			q.Constrain(b, "to", []q.AttributeValue{
				{AttrDescID, from.Reference(AttrDescID)},
				{AttrDirection, AddDirection},
				{AttrStatus, from.Reference(AttrStatus)},
				{AttrElementType, q.Any(PrimaryIndexElement, SecondaryIndexElement)},
			})
			b.Filter(makeFilter(b, []string{
				"from", "to",
			}, func(from *Column, to Entity) bool {
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
			}))
		}))

	primaryIndexReferenceEachOther := q.MustBuild(func(b q.Builder) {
		add := q.Constrain(b, "add", []q.AttributeValue{
			{AttrElementType, PrimaryIndexElement},
			{AttrDirection, AddDirection},
			{AttrStatus, PublicStatus},
		})
		q.Constrain(b, "drop", []q.AttributeValue{
			{AttrElementType, PrimaryIndexElement},
			{AttrDirection, DropDirection},
			{AttrStatus, DeleteAndWriteOnlyStatus},
			{AttrDescID, add.Reference(AttrDescID)},
		})
		b.Filter(makeFilter(b, []string{
			"add", "drop",
		}, func(add, drop *PrimaryIndex) bool {
			return add.OtherPrimaryIndexID == drop.Index.ID
		}))
	})
	depRules.Register(
		"primary index add depends on drop",
		"add", "drop",
		primaryIndexReferenceEachOther,
	)
	depRules.Register(
		"primary index drop depends on add",
		"drop", "add",
		primaryIndexReferenceEachOther,
	)

}

var (
	boolType    = reflect.TypeOf((*bool)(nil)).Elem()
	elementType = reflect.TypeOf((*Element)(nil)).Elem()
	entityType  = reflect.TypeOf((*Entity)(nil)).Elem()
)

func makeFilter(b q.Builder, nodeNames []string, fn interface{}) q.Filter {
	fv := reflect.ValueOf(fn)
	ft := fv.Type()
	if ft.Kind() != reflect.Func {
		panic(errors.AssertionFailedf("expected %v to be a func, %s", ft))
	}
	if ft.NumIn() != len(nodeNames) {
		panic(errors.AssertionFailedf(
			"expected %v to have %d arguments corresponding to %q",
			ft, len(nodeNames), nodeNames))
	}
	if ft.NumOut() != 1 || ft.Out(0) != boolType {
		panic(errors.AssertionFailedf(
			"expected %v to have one bool return value",
			ft))
	}
	nodes := make([]q.Entity, len(nodeNames))
	for i, name := range nodeNames {
		nodes[i] = b.Entity(name)
	}
	// We want to then make sure that we do the proper conversions.
	convertFuncs := make([]func(n eav.Entity) reflect.Value, ft.NumIn())
	for i := 0; i < ft.NumIn(); i++ {
		i := i // for closure
		arg := ft.In(i)
		switch {
		case arg == elementType:
			convertFuncs[i] = func(n eav.Entity) reflect.Value {
				return reflect.ValueOf(n.(Entity).GetElement()).Convert(elementType)
			}
		case arg == entityType:
			convertFuncs[i] = func(n eav.Entity) reflect.Value {
				return reflect.ValueOf(n.(Entity))
			}
		case arg.Implements(elementType):
			nodes[i].Constrain(
				AttrElementType,
				GetElementType(reflect.Zero(arg).Interface().(Element)),
			)
			convertFuncs[i] = func(n eav.Entity) reflect.Value {
				v := reflect.ValueOf(n.(Entity).GetElement())
				if v.Type() != arg {
					panic(errors.AssertionFailedf("expected %v, got type %v for entity %q",
						arg, v.Type(), nodeNames[i]))
				}
				return v
			}
		default:
			panic(errors.AssertionFailedf(
				"unsupported filter argument type %v for entity %s",
				arg, nodeNames[i]))
		}
	}
	return func(result q.Result) bool {
		resContainers := make([]reflect.Value, len(nodes))
		for i, conv := range convertFuncs {
			resContainers[i] = conv(result.Entity(nodeNames[i]))
		}
		out := fv.Call(resContainers)
		return out[0].Interface().(bool)
	}
}
