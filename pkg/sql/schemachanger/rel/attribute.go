// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rel

// Attribute is used to define a property of entities. Attributes in a given
// schema have a type.
type Attribute interface {

	// String is used when formatting the attribute.
	String() string

	// Ordinal is used to cheaply identify the attribute.
	Ordinal() Ordinal
}

// Ordinal is used to correlate attributes in a schema.
// It enables use of the ordinalSet.
type Ordinal uint64

func attrLess(a, b Attribute) bool {
	switch {
	case a != nil && b != nil:
		return a.Ordinal() < b.Ordinal()
	case a != nil:
		return false
	case b != nil:
		return true
	default:
		return false
	}
}
