// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scpb

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav2"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// Attr are keys used for finger prints of objects
// for comparing uniqueness
type Attr int

// Ordinal is part of the eavasdf.Attribute interface.
func (i Attr) Ordinal() eav2.Ordinal { return eav2.Ordinal(i) }

var _ eav2.Attribute = Attr(0)

//go:generate stringer -type=Attr -trimprefix=Attr
const (
	_ Attr = iota // reserve 0 for eav2.TypeAttribute
	// AttrElementType type id of the element.
	AttrElementType
	// AttrDescID is the descriptor ID to which this element belongs.
	AttrDescID
	// AttrReferencedDescID is the descriptor ID to which this element refers.
	AttrReferencedDescID
	//AttrColumnID is the column ID to which this element corresponds.
	AttrColumnID
	// AttrName is the name of the element.
	AttrName
	// AttrIndexID is the index ID to which this element corresponds.
	AttrIndexID
	// AttrDirection is the direction of a Target.
	AttrDirection
	// AttrStatus is the Status of a Node.
	AttrStatus
	// AttrElement references an element.
	AttrElement
	// AttrTarget is the reference from a node to a target.
	AttrTarget
	// AttrParentID is the parent of this descriptor.
	AttrParentID
	AttrParentSchemaID

	NumAttrs int = iota
)

var AttrSchema = eav2.NewSchema(eav2.Mappings{
	AttributeTypes: map[eav2.Attribute]reflect.Type{
		AttrElement: reflect.TypeOf((*protoutil.Message)(nil)).Elem(),
	},
	TypeMappings: map[reflect.Type]map[string]eav2.Attribute{
		reflect.TypeOf((*Node)(nil)): {
			"Status": AttrStatus,
			"Target": AttrTarget,
		},
		reflect.TypeOf((*Target)(nil)): {
			"Direction":            AttrDirection,
			"Column":               AttrElement,
			"PrimaryIndex":         AttrElement,
			"SecondaryIndex":       AttrElement,
			"SequenceDependency":   AttrElement,
			"UniqueConstraint":     AttrElement,
			"CheckConstraint":      AttrElement,
			"Sequence":             AttrElement,
			"DefaultExpression":    AttrElement,
			"View":                 AttrElement,
			"TypeRef":              AttrElement,
			"Table":                AttrElement,
			"OutForeignKey":        AttrElement,
			"InForeignKey":         AttrElement,
			"RelationDependedOnBy": AttrElement,
			"SequenceOwner":        AttrElement,
			"Type":                 AttrElement,
			"Schema":               AttrElement,
			"Database":             AttrElement,
		},
		reflect.TypeOf((*Database)(nil)): {
			"DatabaseID": AttrDescID,
		},
		reflect.TypeOf((*Table)(nil)): {
			"TableID":        AttrDescID,
			"ParentID":       AttrParentID,
			"ParentSchemaID": AttrParentSchemaID,
		},
		reflect.TypeOf((*Column)(nil)): {
			"TableID":   AttrDescID,
			"Column.ID": AttrColumnID,
		},
	},
})
