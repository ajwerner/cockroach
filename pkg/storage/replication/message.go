package replication

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/connect"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"go.etcd.io/etcd/raft/raftpb"
)

// PeerClient is a connect.Conn that implements proposals.
type PeerClient struct {

	// Callbacks are hooks into the proposal lifecycle.
	// Expect one callback per message send.
	// TODO(ajwerner): Is it the case that they'll be called in order of the
	// messages which are sent? Is there enforcement that a client only ever
	// sends one message? Do we need to decorate the signatures?
	Callbacks struct {

		// ShouldRepropose takes a reason and erturns an error that gets send back.
		ShouldRepropose func(ReproposalReason) error

		// DoCommit is a callback which occurs to allow the command to validate that
		// a commit can proceed given the current state. If an error is returned it
		// propagate back to the client through a message send. An error prevents
		// application of the command.
		Commit func() error

		// DoApply is a callback which the client can set to do the work of applying a
		// command. An error from DoApply is fatal.
		Apply func(engine.Writer) error

		// OnApplied is called when the application of the command has been written
		// to storage.
		Applied func()
	}

	done bool
	err  error
	cond sync.Cond
	syn  sync.Locker
	peer *Peer
}

// Send accepts ProposalMessages.
func (pc *PeerClient) Send(ctx context.Context, msg connect.Message) {
	m, ok := msg.(ProposalMessage)
	if !ok {
		panic(fmt.Errorf("got %T, expected %T", msg, (ProposalMessage)(nil)))
	}
	pc.syn.Unlock()
	defer pc.syn.Lock()
	pc.peer.addProposal(&proposal{
		ctx: ctx,
		syn: pc.syn,
		msg: m,
		pc:  pc,
	})
}

// TODO(ajwerner): consider cancelling the proposal if possible.
func (pc *PeerClient) Close(ctx context.Context, drain bool) {
	panic("not implemented")
}

// CommittedMessage is send when a proposal has been comitted.
type CommittedMessage struct {
}

// ErrorMessage is send when a proposal has not been comitted.
type ErrorMessage struct {
	Err error
}

// Recv will return a CommittedMessage or an ErrorMessage
func (pc *PeerClient) Recv() connect.Message {
	for !pc.done {
		pc.cond.Wait()
	}
	if pc.err != nil {
		return &ErrorMessage{Err: pc.err}
	}
	return CommittedMessage{}
}

func (pc *PeerClient) finishWithError(err error) {
	pc.syn.Lock()
	defer pc.cond.Signal()
	defer pc.syn.Unlock()
	if pc.done {
		panic("double finish on peer client")
	}
	pc.err = err
	pc.done = true
}

var _ connect.Conn = (*PeerClient)(nil)

type proposal struct {
	ctx             context.Context
	syn             sync.Locker
	pc              *PeerClient
	msg             ProposalMessage
	proposedAtTicks int
}

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
