package replication

import (
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"go.etcd.io/etcd/raft/raftpb"
)

// TODO(ajwerner): consider some unsafe hackery to not allocate the string for
// ID on every call to ID().

// ProposalMessage is used to propose data to raft.
type ProposalMessage interface {
	ID() storagebase.CmdIDKey
	Encoded() []byte
	Size() int
}

type encodedProposalMessage []byte

func (ec encodedProposalMessage) ID() storagebase.CmdIDKey {
	if len(ec) == 0 {
		return ""
	}
	id, _ := DecodeRaftCommand([]byte(ec))
	return id
}

func (ec encodedProposalMessage) Encoded() []byte {
	if len(ec) == 0 {
		return []byte(ec)
	}
	_, data := DecodeRaftCommand([]byte(ec))
	return data
}

func (ec encodedProposalMessage) Size() int {
	return len(ec.Encoded())
}

// ConfChangeMessage is used to propose a configuration change to raft.
type ConfChangeMessage interface {
	ProposalMessage
	ChangeType() raftpb.ConfChangeType
	PeerID() PeerID
}

type encodedConfChangeMessage struct {
	cc *raftpb.ConfChange
}

var _ ConfChangeMessage = (*encodedConfChangeMessage)(nil)

func (ecc encodedConfChangeMessage) ID() storagebase.CmdIDKey {
	return encodedProposalMessage(ecc.cc.Context).ID()
}

func (ecc encodedConfChangeMessage) Size() int {
	return encodedProposalMessage(ecc.cc.Context).Size()
}

func (ecc encodedConfChangeMessage) Encoded() []byte {
	return encodedProposalMessage(ecc.cc.Context).Encoded()
}

func (ecc encodedConfChangeMessage) ChangeType() raftpb.ConfChangeType {
	return ecc.cc.Type
}

func (ecc encodedConfChangeMessage) PeerID() PeerID {
	return ecc.cc.NodeID
}
