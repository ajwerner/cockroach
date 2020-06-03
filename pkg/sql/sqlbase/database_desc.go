// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// DatabaseDescriptorInterface will eventually be called dbdesc.Descriptor.
// It is implemented by ImmutableDatabaseDescriptor.
type DatabaseDescriptorInterface interface {
	BaseDescriptorInterface
	DatabaseDesc() *DatabaseDescriptor
}

var _ DatabaseDescriptorInterface = (*ImmutableDatabaseDescriptor)(nil)

// ImmutableDatabaseDescriptor wraps a database descriptor and provides methods
// on it.
type ImmutableDatabaseDescriptor struct {
	meta DescriptorMeta

	DatabaseDescriptor
}

// MutableDatabaseDescriptor is a mutable reference to a DatabaseDescriptor.
//
// TODO(ajwerner): Today this isn't actually ever mutated but rather exists for
// a future where we anticipate having a mutable copy of database descriptors.
// There's a large amount of space to question this `Mutable|Immutable` version
// of each descriptor type. Maybe it makes no sense but we're running with it
// for the moment.
type MutableDatabaseDescriptor struct {
	ImmutableDatabaseDescriptor

	ClusterVersion *ImmutableDatabaseDescriptor
}

// NewImmutableDatabaseDescriptor makes a new database descriptor.
func NewImmutableDatabaseDescriptor(desc *Descriptor) *ImmutableDatabaseDescriptor {
	// TODO(ajwerner): Upgrade the meta. This will mean copying from the underlying
	// desc as necessary.
	ret := newImmutableDatabaseDesc(desc.DescriptorMeta, *desc.GetDatabase())
	return ret
}

// NewMutableDatabaseDescriptor creates a new MutableDatabaseDescriptor. The
// version of the returned descriptor will be the successor of the descriptor
// from which it was constructed.
func NewMutableDatabaseDescriptor(
	mutationOf *ImmutableDatabaseDescriptor,
) *MutableDatabaseDescriptor {
	mut := &MutableDatabaseDescriptor{
		ImmutableDatabaseDescriptor: *mutationOf,
		ClusterVersion:              mutationOf,
	}
	mut.meta.Version++
	return mut
}

// NewInitialDatabaseDescriptor constructs a new DatabaseDescriptor for an
// initial version from an id and name.
func NewInitialDatabaseDescriptor(id ID, name string) *ImmutableDatabaseDescriptor {
	return newImmutableDatabaseDesc(
		MakeInitialDescriptorMeta(id, name),
		DatabaseDescriptor{
			Privileges: NewDefaultPrivilegeDescriptor(),
		})
}

func newImmutableDatabaseDesc(
	meta DescriptorMeta, desc DatabaseDescriptor,
) *ImmutableDatabaseDescriptor {
	// TODO(ajwerner): At a certain point we shouldn't need to write to the
	// deprecated fields and we can just clear them out. We'll always need to
	// read from them to deal with backups.
	//
	// TODO(ajwerner): Rename the DescriptorMeta fields on the DatabaseDescriptor
	// to Deprecated*.
	if meta.Name == "" && desc.Name == "" {
		panic(errors.Errorf("cannot construct a DatabaseDescriptor without an ID"))
	} else if meta.Name == "" {
		meta.Name = desc.Name
	} else if desc.Name == "" {
		desc.Name = meta.Name
	} else if desc.Name != meta.Name {
		panic(errors.Errorf("cannot construct a DatabaseDescriptor with mismatched names: %s in meta != %s in desc",
			meta.Name, desc.Name))
	}

	if meta.ID == 0 && desc.ID == 0 {
		// NB: The only database we allow to have a zero ID is the system database.
		if meta.Name != SystemDatabaseName {
			panic(errors.Errorf("cannot construct a DatabaseDescriptor without an ID"))
		}
	} else if meta.ID == 0 {
		meta.ID = desc.ID
	} else if desc.ID == 0 {
		desc.ID = meta.ID
	} else if desc.ID != meta.ID {
		panic(errors.Errorf("cannot construct a DatabaseDescriptor with mismatched IDs: %d in meta != %d in desc",
			log.Safe(meta.ID), log.Safe(desc.ID)))
	}

	// TODO(ajwerner): rinse and repeat for all of the fields in meta.
	// Let's seriously hope we can get away with not ever putting those fields
	// into the SchemaDescriptor and TypeDescriptor so we don't have to deal with
	// the very long (possibly forever) backup migration.
	return &ImmutableDatabaseDescriptor{meta: meta, DatabaseDescriptor: desc}
}

// TypeName returns the plain type of this descriptor.
func (desc *DatabaseDescriptor) TypeName() string {
	return "database"
}

// DatabaseDesc implements the ObjectDescriptor interface.
func (desc *DatabaseDescriptor) DatabaseDesc() *DatabaseDescriptor {
	return desc
}

// SchemaDesc implements the ObjectDescriptor interface.
func (desc *DatabaseDescriptor) SchemaDesc() *SchemaDescriptor {
	return nil
}

// TableDesc implements the ObjectDescriptor interface.
func (desc *DatabaseDescriptor) TableDesc() *TableDescriptor {
	return nil
}

// TypeDesc implements the ObjectDescriptor interface.
func (desc *DatabaseDescriptor) TypeDesc() *TypeDescriptor {
	return nil
}

// NameResolutionResult implements the ObjectDescriptor interface.
func (desc *ImmutableDatabaseDescriptor) NameResolutionResult() {}

// DescriptorProto wraps a DatabaseDescriptor in a Descriptor.
func (desc *ImmutableDatabaseDescriptor) DescriptorProto() *Descriptor {
	return &Descriptor{
		DescriptorMeta: desc.meta,
		Union: &Descriptor_Database{
			Database: &desc.DatabaseDescriptor,
		},
	}
}

// SetName sets the name on the descriptor.
func (desc *MutableDatabaseDescriptor) SetName(name string) {
	desc.Name = name
	desc.meta.Name = name
}

// SetID sets the id on the descriptor.
func (desc *MutableDatabaseDescriptor) SetID(id ID) {
	desc.ID = id
	desc.meta.ID = id
}

// Validate validates that the database descriptor is well formed.
// Checks include validate the database name, and verifying that there
// is at least one read and write user.
func (desc *DatabaseDescriptor) Validate() error {
	if err := validateName(desc.Name, "descriptor"); err != nil {
		return err
	}
	if desc.ID == 0 {
		return fmt.Errorf("invalid database ID %d", desc.ID)
	}

	// Fill in any incorrect privileges that may have been missed due to mixed-versions.
	// TODO(mberhault): remove this in 2.1 (maybe 2.2) when privilege-fixing migrations have been
	// run again and mixed-version clusters always write "good" descriptors.
	desc.Privileges.MaybeFixPrivileges(desc.GetID())

	// Validate the privilege descriptor.
	return desc.Privileges.Validate(desc.GetID())
}

// GetAuditMode is part of the DescriptorProto interface.
// This is a stub until per-database auditing is enabled.
func (desc *DatabaseDescriptor) GetAuditMode() TableDescriptor_AuditMode {
	return TableDescriptor_DISABLED
}
