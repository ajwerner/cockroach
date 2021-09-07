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

import (
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
	"github.com/google/btree"
)

// Database is a data structure for indexing entities.
type Database struct {
	schema *Schema

	// indexes are store the entities, ordered by a specified set of attributes.
	// When an entity is inserted, it is inserted into each of the indexes. The
	// first entry in the list is the "primary index" which compares entities
	// based on all attributes.
	indexes []index
	// entities stores all the entities keyed on its pointer value.
	entities map[uintptr]*entity
}

// Schema returns the schema associated with the tree.
func (t *Database) Schema() *Schema {
	return t.schema
}

// NewDatabase constructs A new Database with the specified indexes.
// Note that the schema must not contain more than 64 attributes.
func NewDatabase(sc *Schema, indexes [][]Attribute) *Database {
	t := &Database{
		schema:   sc,
		indexes:  make([]index, len(indexes)+1),
		entities: make(map[uintptr]*entity),
	}
	// Index everything by all of the attributes. This serves as the primary
	// index.
	const degree = 8
	fl := btree.NewFreeList(len(indexes) + 1)
	{
		var primaryIndex index
		primaryIndex.s = sc
		primaryIndex.tree = btree.NewWithFreeList(8, fl)
		t.indexes[0] = primaryIndex
	}
	secondaryIndexes := t.indexes[1:]
	for i, attrs := range indexes {
		m := makeOrdinalSetWithAttributes(attrs)
		spec := indexSpec{mask: m, attrs: attrs, s: sc}
		secondaryIndexes[i] = index{
			indexSpec: spec,
			tree:      btree.NewWithFreeList(degree, fl),
		}
	}
	return t
}

// Insert inserts an variable.
//
// TODO(ajwerner): Figure out what to do if the variable already
// exists. We need to nail down what existence means: is it
// intentional, as in, does the unique pointer exist, or is it
// extensional, as in, does some variable exist with the same attributes
// ignoring pointer value? Either way, what we have here does not fly.
func (t *Database) Insert(e interface{}) error {
	return asEntities(t.schema, allOrdinals, e, func(entity entity) error {
		return t.insert(&entity)
	})
}

// TODO(ajwerner): Deal with already inserted data.
func (t *Database) insert(e *entity) error {
	t.entities[e.ptr] = e
	removedItem := t.indexes[0].tree.ReplaceOrInsert(&containerItem{
		entity:    e,
		indexSpec: &t.indexes[0].indexSpec,
	})
	if removedItem != nil && !equal(t.schema, removedItem.(*containerItem).entity, e) {
		return errors.AssertionFailedf(
			"expected to remove the item each time: %v", removedItem,
		)
	}
	secondaryIndexes := t.indexes[1:]
	for i := range secondaryIndexes {
		idx := &secondaryIndexes[i]
		if g := idx.tree.ReplaceOrInsert(&containerItem{
			entity:    e,
			indexSpec: &idx.indexSpec,
		}); (removedItem == nil) != (g == nil) {
			return errors.AssertionFailedf(
				"expected to remove the item each time: %v %v", removedItem, g,
			)
		}
	}
	return nil
}

type index struct {
	indexSpec
	tree *btree.BTree
}

type indexSpec struct {
	s     *Schema
	mask  ordinalSet
	attrs []Attribute
}

// Iterate will iterate the containers which match the specified valuesMap.
func (t *Database) iterate(where *valuesMap, f entityIterator) (err error) {
	var all, nils, nonNils ordinalSet
	{
		all = where.attrs
		all.ForEach(t.schema, func(a Attribute) (wantMore bool) {
			if where.get(a) == nil {
				nils = nils.Add(a.Ordinal())
			}
			return true
		})
		nonNils = all.Without(nils)
	}

	idx, toCheck := t.chooseIndex(all)
	from, to := getValuesItems(&idx.indexSpec, where, all)
	defer putValuesItems(from, to)
	idx.tree.AscendRange(from, to, func(i btree.Item) (wantMore bool) {
		c := i.(*containerItem)
		// We want to skip items which do not have valuesMap set for
		// all members of the where clause or which have valuesMap set
		// for attributes where we explicitly do not want them.
		if cAttrs := c.entity.attrs; nonNils.Without(cAttrs) != 0 ||
			nils.Intersection(cAttrs) != 0 {
			return true
		}
		var failed bool
		toCheck.ForEach(t.schema, func(a Attribute) (wantMore bool) {
			_, eq := compareOn(a, &c.valuesMap, where)
			failed = !eq
			return !failed
		})
		if !failed {
			err = f.visit(c.entity)
		}
		return err == nil
	})
	if iterutil.Done(err) {
		err = nil
	}
	return err
}

// chooseIndex chooses an index which has A prefix with the highest number of
// attributes which overlap with m. It also returns the ordinals of the
// attributes which are not covered by the index prefix.
//
// TODO(ajwerner): Consider something about selectivity by tracking
// the number of entries under each index (i.variable. which have non-NULL valuesMap)
// for the given dimension.
func (t *Database) chooseIndex(m ordinalSet) (_ *index, toCheck ordinalSet) {
	// Default to the "primary" index.
	best, bestOverlap := 0, ordinalSet(0)
	dims := t.indexes[1:]
	for i := range dims {
		if overlap := dims[i].overlap(m); overlap.Len() > bestOverlap.Len() {
			best, bestOverlap = i+1, overlap
		}
	}
	return &t.indexes[best], m.Without(bestOverlap)
}

// overlap returns the ordinals from m which overlap with a prefix of
// attributes in s.
func (s *indexSpec) overlap(m ordinalSet) ordinalSet {
	var overlap ordinalSet
	for _, a := range s.attrs {
		if m.Contains(a.Ordinal()) {
			overlap = overlap.Add(a.Ordinal())
		} else {
			break
		}
	}
	return overlap
}
