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
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// GetTypeDescFromID retrieves the type descriptor for the type ID passed
// in using an existing proto getter. It returns an error if the descriptor
// doesn't exist or if it exists and is not a type descriptor.
//
// TODO(ajwerner): Move this to catalogkv or something like it.
func GetTypeDescFromID(
	ctx context.Context, protoGetter protoGetter, codec keys.SQLCodec, id ID,
) (*TypeDescriptor, error) {
	descKey := MakeDescMetadataKey(codec, id)
	desc := &Descriptor{}
	_, err := protoGetter.GetProtoTs(ctx, descKey, desc)
	if err != nil {
		return nil, err
	}
	typ := desc.GetType()
	if typ == nil {
		return nil, ErrDescriptorNotFound
	}
	return typ, nil
}

// TypeDescriptorInterface will eventually be called typedesc.Descriptor.
// It is implemented by (Imm|M)utableTypeDescriptor.
type TypeDescriptorInterface interface {
	BaseDescriptorInterface
	TypeDesc() *TypeDescriptor
	HydrateTypeInfoWithName(typ *types.T, name *tree.TypeName) error
}

var _ TypeDescriptorInterface = (*ImmutableTypeDescriptor)(nil)
var _ TypeDescriptorInterface = (*MutableTypeDescriptor)(nil)

// MakeSimpleAliasTypeDescriptor creates a type descriptor that is an alias
// for the input type. It is intended to be used as an intermediate for name
// resolution, and should not be serialized and stored on disk.
func MakeSimpleAliasTypeDescriptor(typ *types.T) *ImmutableTypeDescriptor {
	return NewImmutableTypeDescriptor(&Descriptor{
		// TODO(ajwerner): Populate DescriptorMeta fields.
		Union: &Descriptor_Type{
			Type: &TypeDescriptor{
				ParentID:       InvalidID,
				ParentSchemaID: InvalidID,
				Name:           typ.Name(),
				ID:             InvalidID,
				Kind:           TypeDescriptor_ALIAS,
				Alias:          typ,
			},
		},
	})
}

// NameResolutionResult implements the NameResolutionResult interface.
func (desc *TypeDescriptor) NameResolutionResult() {}

// typeDescriptor allows (Imm|M)utableTypeDescriptor to embed a
// method set on the TypeDescriptor without exposing its fields.
//
// We prefer to attach the methods here rather than on the proto struct itself
// to enable package boundaries.
type typeDescriptor struct {
	*TypeDescriptor
}

// MutableTypeDescriptor is a custom type for TypeDescriptors undergoing
// any types of modifications.
type MutableTypeDescriptor struct {

	// TODO(ajwerner): Decide whether we're okay embedding the
	// ImmutableTypeDescriptor or whether we should be embedding some other base
	// struct that implements the various methods. For now we have the trap that
	// the code really wants direct field access and moving all access to
	// getters on an interface is a bigger task.
	ImmutableTypeDescriptor

	// ClusterVersion represents the version of the type descriptor read
	// from the store.
	ClusterVersion ImmutableTypeDescriptor
}

// ImmutableTypeDescriptor is a custom type for wrapping TypeDescriptors
// when used in a read only way.
type ImmutableTypeDescriptor struct {
	meta DescriptorMeta

	TypeDescriptor
}

// Avoid linter unused warnings.
var _ = NewMutableCreatedTypeDescriptor

// NewMutableCreatedTypeDescriptor returns a MutableTypeDescriptor from the
// given type descriptor with the cluster version being the zero type. This
// is for a type that is created in the same transaction.
func NewMutableCreatedTypeDescriptor(desc *Descriptor) *MutableTypeDescriptor {
	return &MutableTypeDescriptor{
		ImmutableTypeDescriptor: makeImmutableTypeDescriptor(desc),
	}
}

// NewMutableExistingTypeDescriptor returns a MutableTypeDescriptor from the
// given type descriptor with the cluster version also set to the descriptor.
// This is for types that already exist.
func NewMutableExistingTypeDescriptor(desc *Descriptor) *MutableTypeDescriptor {
	return &MutableTypeDescriptor{
		ImmutableTypeDescriptor: makeImmutableTypeDescriptor(desc),
		ClusterVersion:          makeImmutableTypeDescriptor(desc),
	}
}

// NewImmutableTypeDescriptor returns an ImmutableTypeDescriptor from the
// given TypeDescriptor.
func NewImmutableTypeDescriptor(desc *Descriptor) *ImmutableTypeDescriptor {
	m := makeImmutableTypeDescriptor(desc)
	return &m
}

func makeImmutableTypeDescriptor(desc *Descriptor) ImmutableTypeDescriptor {
	m := ImmutableTypeDescriptor{}
	m.meta = desc.DescriptorMeta
	m.TypeDescriptor = *desc.GetType()
	return m
}

// DescriptorProto returns a Descriptor for serialization.
func (desc *TypeDescriptor) DescriptorProto() *Descriptor {

	// TODO(ajwerner): Copy over the metadata fields.
	// TODO(ajwerner): This ultimately should be cleaner.
	return wrapDescriptor(desc)
}

// TODO(ajwerner): Change the receiver of these methods to the
// ImmutableTypeDescriptor.
//
// In many ways it seems better if these methods were just the getters from
// the descriptor union and we had each of the descriptor types just embed
// that union.

// DatabaseDesc implements the ObjectDescriptor interface.
func (desc *TypeDescriptor) DatabaseDesc() *DatabaseDescriptor {
	return nil
}

// SchemaDesc implements the ObjectDescriptor interface.
func (desc *TypeDescriptor) SchemaDesc() *SchemaDescriptor {
	return nil
}

// TableDesc implements the ObjectDescriptor interface.
func (desc *TypeDescriptor) TableDesc() *TableDescriptor {
	return nil
}

// TypeDesc implements the ObjectDescriptor interface.
func (desc *TypeDescriptor) TypeDesc() *TypeDescriptor {
	return desc
}

// GetAuditMode implements the DescriptorProto interface.
func (desc *TypeDescriptor) GetAuditMode() TableDescriptor_AuditMode {
	return TableDescriptor_DISABLED
}

// GetPrivileges implements the DescriptorProto interface.
func (desc *TypeDescriptor) GetPrivileges() *PrivilegeDescriptor {
	return nil
}

// TypeName implements the DescriptorProto interface.
func (desc *TypeDescriptor) TypeName() string {
	return "type"
}

// SetName implements the DescriptorProto interface.
func (desc *TypeDescriptor) SetName(name string) {
	desc.Name = name
}

// HydrateTypeInfo fills in user defined type metadata for a type.
// TODO (rohany): This method should eventually be defined on an
//  ImmutableTypeDescriptor so that pointers to the cached info
//  can be shared among callers.
func (desc *TypeDescriptor) HydrateTypeInfo(typ *types.T) error {
	return desc.HydrateTypeInfoWithName(typ, tree.NewUnqualifiedTypeName(tree.Name(desc.Name)))
}

// HydrateTypeInfoWithName fills in user defined type metadata for
// a type and also sets the name in the metadata to the passed in name.
// This is used when hydrating a type with a known qualified name.
func (desc *TypeDescriptor) HydrateTypeInfoWithName(typ *types.T, name *tree.TypeName) error {
	typ.TypeMeta.Name = name
	switch desc.Kind {
	case TypeDescriptor_ENUM:
		if typ.Family() != types.EnumFamily {
			return errors.New("cannot hydrate a non-enum type with an enum type descriptor")
		}
		logical := make([]string, len(desc.EnumMembers))
		physical := make([][]byte, len(desc.EnumMembers))
		for i := range desc.EnumMembers {
			member := &desc.EnumMembers[i]
			logical[i] = member.LogicalRepresentation
			physical[i] = member.PhysicalRepresentation
		}
		typ.TypeMeta.EnumData = &types.EnumMetadata{
			LogicalRepresentations:  logical,
			PhysicalRepresentations: physical,
		}
		return nil
	case TypeDescriptor_ALIAS:
		// This is a noop until we possibly allow aliases to user defined types.
		return nil
	default:
		return errors.AssertionFailedf("unknown type descriptor kind %s", desc.Kind)
	}
}
