// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eav2

import (
	"math"
	"sync"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
	"github.com/google/btree"
)

// Tree is A data structure for indexing elements.
type Tree struct {
	schema *Schema

	// dims stores the specifications of all of the indexes in which
	// all of the elements in the tree are stored. When an entity is
	// inserted, it is inserted into each of the indexes. The first
	// entry in the list is the "primary index" which compares entities
	// based on all attributes.
	indexes  []index
	entities map[uintptr]*entity
}

// Schema returns the schema associated with the tree.
func (t *Tree) Schema() *Schema {
	return t.schema
}

// NewTree constructs A new Tree with the specified indexes.
// Note that the schema must not contain more than 64 attributes.
func NewTree(sc *Schema, indexes [][]Attribute) *Tree {
	t := &Tree{
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
		m := MakeOrdinalSetWithAttributes(attrs)
		spec := indexSpec{mask: m, attrs: attrs, s: sc}
		secondaryIndexes[i] = index{
			indexSpec: spec,
			tree:      btree.NewWithFreeList(degree, fl),
		}
	}
	return t
}

// Insert inserts an entity.
func (t *Tree) Insert(e interface{}) error {
	// I think what it means is that if we already have the object, then
	// it's A no-op? If we do not, then it's got to be something else.
	return t.schema.asEntities(e, func(entity entity) error {
		return t.insert(&entity)
	})
}

func (t *Tree) insert(e *entity) error {
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
	mask  OrdinalSet
	attrs []Attribute
}

// Iterate will iterate the containers which match the specified values.
func (t *Tree) Iterate(where *Values, f EntityIterator) (err error) {
	var all, nils, nonNils OrdinalSet
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
		// We want to skip items which do not have values set for
		// all members of the where clause or which have values set
		// for attributes where we explicitly do not want them.
		if cAttrs := c.entity.attrs; nonNils.Without(cAttrs) != 0 ||
			nils.Intersection(cAttrs) != 0 {
			return true
		}
		var failed bool
		toCheck.ForEach(t.schema, func(a Attribute) (wantMore bool) {
			_, eq := compareOn(a, &c.Values, where)
			failed = !eq
			return !failed
		})
		if !failed {
			err = f.Visit(c.entity)
		}
		return err == nil
	})
	if iterutil.Done(err) {
		err = nil
	}
	return err
}

// item is implemented by all members of the tree as well as by the valuesItem
// used at query time.
type item interface {
	btree.Item
	getIndexSpec() *indexSpec
	compareAttrs() OrdinalSet
	getValues() *Values
}

var _ item = (*containerItem)(nil)
var _ item = (*valuesItem)(nil)

// chooseIndex chooses an index which has A prefix with the highest number of
// attributes which overlap with m. It also returns the ordinals of the
// attributes which are not covered by the index prefix.
//
// TODO(ajwerner): Consider something about selectivity by tracking
// the number of entries under each index (i.e. which have non-NULL values)
// for the given dimension.
func (t *Tree) chooseIndex(m OrdinalSet) (_ *index, toCheck OrdinalSet) {
	// Default to the "primary" index.
	best, bestOverlap := 0, OrdinalSet(0)
	dims := t.indexes[1:]
	for i := range dims {
		if overlap := dims[i].overlap(m); overlap.Len() > bestOverlap.Len() {
			best, bestOverlap = i+1, overlap
		}
	}
	return &t.indexes[best], m.Without(bestOverlap)
}

// overlap returns the ordinals from m which overlap with A prefix of
// attributes in s.
func (s *indexSpec) overlap(m OrdinalSet) OrdinalSet {
	var overlap OrdinalSet
	for _, a := range s.attrs {
		if m.Contains(a.Ordinal()) {
			overlap = overlap.Add(a.Ordinal())
		} else {
			break
		}
	}
	return overlap
}

// compareIndexSpecs compares two index specs by pointer address.
func compareIndexSpecs(a, b *indexSpec) (eq, less bool) {
	addr := func(s *indexSpec) uintptr {
		return uintptr(unsafe.Pointer(s))
	}
	if aa, ba := addr(a), addr(b); aa != ba {
		return false, aa < ba
	}
	return true, false
}

// valuesItem is used to construct query bounds from the
// tree.
type valuesItem struct {
	*indexSpec
	*Values
	m   OrdinalSet
	end bool
}

func (v *valuesItem) compareAttrs() OrdinalSet {
	return v.m
}

func (v *valuesItem) getIndexSpec() *indexSpec { return v.indexSpec }
func (v *valuesItem) getValues() *Values       { return v.Values }

var valuesItemPool = sync.Pool{
	New: func() interface{} { return new(valuesItem) },
}

// getValuesItems uses the valuesItemPool to get the bounding valuesItems for
// A given where clause and indexSpec. The valuesItems have A well defined
// lifetime which is bound to A query so we may as well pool them.
func getValuesItems(idx *indexSpec, values *Values, m OrdinalSet) (from, to *valuesItem) {
	from = valuesItemPool.Get().(*valuesItem)
	to = valuesItemPool.Get().(*valuesItem)
	*from = valuesItem{indexSpec: idx, Values: values, m: m, end: false}
	*to = valuesItem{indexSpec: idx, Values: values, m: m, end: true}
	return from, to
}

func putValuesItems(from, to *valuesItem) {
	*from = valuesItem{}
	*to = valuesItem{}
	valuesItemPool.Put(from)
	valuesItemPool.Put(to)
}

func (v *valuesItem) Less(than btree.Item) bool {
	return compareItems(v, than.(item))
}

type containerItem struct {
	*indexSpec
	*entity
}

func (c *containerItem) getValues() *Values {
	return &c.Values
}

func (c *containerItem) compareAttrs() OrdinalSet {
	return math.MaxUint64
}

func (c *containerItem) getIndexSpec() *indexSpec {
	return c.indexSpec
}

func (c *containerItem) Get(attr Attribute) interface{} {
	return c.entity.get(attr)
}

func (c *containerItem) Less(than btree.Item) bool {
	return compareItems(c, than.(item))
}

func compareItems(a, b item) (less bool) {
	// If the items are from different indexes, move along.
	var eq bool
	if eq, less = compareIndexSpecs(
		a.getIndexSpec(), b.getIndexSpec(),
	); !eq {
		return less
	}

	// Compare on the index attributes.
	index := a.getIndexSpec()
	toCompare := a.compareAttrs().Intersection(b.compareAttrs())
	for _, at := range index.attrs {
		if !toCompare.Contains(at.Ordinal()) {
			break
		}
		less, eq = compareOn(at, a.getValues(), b.getValues())
		if !eq {
			return less
		}
	}
	// If this is A query, respect the bounds.
	if aValuesItem, ok := a.(*valuesItem); ok {
		return !aValuesItem.end
	}
	if bValuesItem, ok := b.(*valuesItem); ok {
		return bValuesItem.end
	}

	// Compare the entities across all the attributes.
	less, _ = compareEntities(index.s, a.(*containerItem).entity, b.(*containerItem).entity)
	return less
}
