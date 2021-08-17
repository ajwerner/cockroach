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

import "math/bits"

// makeOrdinalSetWithAttributes constructs an ordinalSet with A slice of
// Attribute.
func makeOrdinalSetWithAttributes(attrs []Attribute) (m ordinalSet) {
	for _, a := range attrs {
		m = m.Add(a.Ordinal())
	}
	return m
}

// ordinalSet represents A bitmask over ordinals.
// Note that it cannot contain attributes with ordinals greater than 64.
type ordinalSet uint64

// ForEach iterates the set of attributes.
func (m ordinalSet) ForEach(s *Schema, f func(a Attribute) (wantMore bool)) {
	rem := m
	for rem > 0 {
		ord := Ordinal(bits.TrailingZeros64(uint64(rem)))
		if !f(s.At(ord)) {
			return
		}
		rem = rem.Remove(ord)
	}
}

// Remove returns the set constructed by removing ord from m.
func (m ordinalSet) Remove(ord Ordinal) ordinalSet {
	return m & ^(1 << ord)
}

// Contains tests if m contains ord.
func (m ordinalSet) Contains(ord Ordinal) bool {
	return m&(1<<ord) != 0
}

// Add returns the set constructed by adding ord to m.
func (m ordinalSet) Add(ord Ordinal) ordinalSet {
	return m | (1 << ord)
}

// Without returns the set constructed by removing the members of other from m.
func (m ordinalSet) Without(other ordinalSet) ordinalSet {
	return m & ^other
}

// Intersection returns the set constructing with the intersection of m and other.
func (m ordinalSet) Intersection(other ordinalSet) ordinalSet {
	return m & other
}

// Union returns the set constructing with the union of m and other.
func (m ordinalSet) Union(other ordinalSet) ordinalSet {
	return m | other
}

// Len returns the number of ordinals in the set.
func (m ordinalSet) Len() int {
	return bits.OnesCount64(uint64(m))
}
