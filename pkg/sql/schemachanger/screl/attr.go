// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package screl

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// Attr are keys used for finger prints of objects
// for comparing uniqueness
type Attr int

var _ rel.Attribute = Attr(0)

//go:generate stringer -type=Attr -trimprefix=Attr
const (
	_ Attr = iota // reserve 0 for rel.Type
	// DescID is the descriptor ID to which this element belongs.
	DescID
	// ReferencedDescID is the descriptor ID to which this element refers.
	ReferencedDescID
	//ColumnID is the column ID to which this element corresponds.
	ColumnID
	// Name is the name of the element.
	Name
	// IndexID is the index ID to which this element corresponds.
	IndexID
	// Direction is the direction of a Target.
	Direction
	// Status is the Status of a Node.
	Status
	// Element references an element.
	Element
	// Target is the reference from a node to a target.
	Target

	NumAttrs int = iota
)

var t = reflect.TypeOf

var Schema = rel.MustSchema("", rel.Mappings{
	AttributeTypes: map[rel.Attribute]reflect.Type{
		Element: t((*protoutil.Message)(nil)).Elem(),
	},
	TypeMappings: map[reflect.Type]map[string]rel.Attribute{
		t((*scpb.Node)(nil)): {
			"Status": Status,
			"Target": Target,
		},
		t((*scpb.Target)(nil)): {
			"Direction":            Direction,
			"Column":               Element,
			"PrimaryIndex":         Element,
			"SecondaryIndex":       Element,
			"SequenceDependency":   Element,
			"UniqueConstraint":     Element,
			"CheckConstraint":      Element,
			"Sequence":             Element,
			"DefaultExpression":    Element,
			"View":                 Element,
			"TypeRef":              Element,
			"Table":                Element,
			"OutForeignKey":        Element,
			"InForeignKey":         Element,
			"RelationDependedOnBy": Element,
			"SequenceOwner":        Element,
			"Type":                 Element,
			"Schema":               Element,
			"Database":             Element,
		},
		t((*scpb.Column)(nil)): {
			"TableID":     DescID,
			"Column.ID":   ColumnID,
			"Column.Name": Name,
		},
		t((*scpb.PrimaryIndex)(nil)): {
			"TableID":    DescID,
			"Index.ID":   IndexID,
			"Index.Name": Name,
		},
		t((*scpb.SecondaryIndex)(nil)): {
			"TableID":    DescID,
			"Index.ID":   IndexID,
			"Index.Name": Name,
		},
		t((*scpb.SequenceDependency)(nil)): {
			"SequenceID": DescID,
			"TableID":    ReferencedDescID,
			"ColumnID":   ColumnID,
		},
		t((*scpb.UniqueConstraint)(nil)): {
			"TableID": DescID,
			"IndexID": IndexID,
		},
		t((*scpb.Sequence)(nil)): {
			"SequenceID": DescID,
		},
		t((*scpb.DefaultExpression)(nil)): {
			"TableID":  DescID,
			"ColumnID": ColumnID,
		},
		t((*scpb.View)(nil)): {
			"TableID": DescID,
		},
		t((*scpb.TypeReference)(nil)): {
			"DescID": DescID,
			"TypeID": ReferencedDescID,
		},
		t((*scpb.Table)(nil)): {
			"TableID": DescID,
		},
		t((*scpb.InboundForeignKey)(nil)): {
			"OriginID":    DescID,
			"ReferenceID": ReferencedDescID,
			"Name":        Name,
		},
		t((*scpb.OutboundForeignKey)(nil)): {
			"OriginID":    DescID,
			"ReferenceID": ReferencedDescID,
			"Name":        Name,
		},
		t((*scpb.RelationDependedOnBy)(nil)): {
			"TableID":      DescID,
			"DependedOnBy": ReferencedDescID,
		},
		t((*scpb.SequenceOwnedBy)(nil)): {
			"SequenceID":   DescID,
			"OwnerTableID": ReferencedDescID,
		},
		t((*scpb.Type)(nil)): {
			"TypeID": DescID,
		},
		t((*scpb.Schema)(nil)): {
			"SchemaID": DescID,
		},
		t((*scpb.Database)(nil)): {
			"DatabaseID": DescID,
		},
	},
})

// JoinTargetNode generates a clause that joins the target and node vars
// to the corresponding element.
func JoinTargetNode(element, target, node rel.Var) rel.Clause {
	return rel.And(
		target.Type((*scpb.Target)(nil)),
		target.Attr(Element, element),
		node.Type((*scpb.Node)(nil)),
		node.Attr(Target, target),
	)
}
