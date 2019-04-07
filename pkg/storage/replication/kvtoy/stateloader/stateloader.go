// Package stateloader provides logic for interacting with the storage engine
// to access local state for a replica.
package stateloader

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft/raftpb"
)

type StateLoader struct {
	keys.RangeIDPrefixBuf
}

// Make creates a a StateLoader.
func Make(rangeID roachpb.RangeID) StateLoader {
	rsl := StateLoader{
		RangeIDPrefixBuf: keys.MakeRangeIDPrefixBuf(rangeID),
	}
	return rsl
}

// LoadRangeAppliedState loads the Range applied state. The returned pointer
// will be nil if the applied state key is not found.
func (rsl StateLoader) LoadRangeAppliedState(
	ctx context.Context, reader engine.Reader,
) (*enginepb.RangeAppliedState, error) {
	var as enginepb.RangeAppliedState
	found, err := engine.MVCCGetProto(ctx, reader, rsl.RangeAppliedStateKey(), hlc.Timestamp{}, &as,
		engine.MVCCGetOptions{})
	if !found {
		return nil, err
	}
	return &as, err
}

// AssertNoRangeAppliedState asserts that no Range applied state key is present.

func (rsl StateLoader) AssertNoRangeAppliedState(ctx context.Context, reader engine.Reader) error {
	if as, err := rsl.LoadRangeAppliedState(ctx, reader); err != nil {
		return err
	} else if as != nil {
		log.Fatalf(ctx, "unexpected RangeAppliedState present: %v", as)
	}

	return nil
}

// LoadAppliedIndex returns the Raft applied index and the lease applied index.

func (rsl StateLoader) LoadAppliedIndex(
	ctx context.Context, reader engine.Reader,
) (raftAppliedIndex uint64, leaseAppliedIndex uint64, err error) {

	// Check the applied state key.
	if as, err := rsl.LoadRangeAppliedState(ctx, reader); err != nil {
		return 0, 0, err
	} else if as != nil {
		return as.RaftAppliedIndex, as.LeaseAppliedIndex, nil
	}
	return 0, 0, errors.Errorf("failed to find lease applied state")
}

// LoadLastIndex loads the last index.

func (rsl StateLoader) LoadLastIndex(ctx context.Context, reader engine.Reader) (uint64, error) {
	prefix := rsl.RaftLogPrefix()
	iter := reader.NewIterator(engine.IterOptions{LowerBound: prefix})
	defer iter.Close()
	var lastIndex uint64
	iter.SeekReverse(engine.MakeMVCCMetadataKey(rsl.RaftLogKey(math.MaxUint64)))
	if ok, _ := iter.Valid(); ok {
		key := iter.Key()
		var err error
		_, lastIndex, err = encoding.DecodeUint64Ascending(key.Key[len(prefix):])
		if err != nil {
			log.Fatalf(ctx, "unable to decode Raft log index key: %s", key)
		}
	}

	if lastIndex == 0 {
		// The log is empty, which means we are either starting from scratch
		// or the entire log has been truncated away.
		lastEnt, _, err := rsl.LoadRaftTruncatedState(ctx, reader)
		if err != nil {
			return 0, err
		}
		lastIndex = lastEnt.Index
	}
	return lastIndex, nil
}

// Load a ReplicaState from disk. The exception is the Desc field, which is
// updated transactionally, and is populated from the supplied RangeDescriptor
// under the convention that that is the latest committed version.
func (rsl StateLoader) Load(
	ctx context.Context, reader engine.Reader, desc *roachpb.RangeDescriptor,
) (storagepb.ReplicaState, error) {
	var s storagepb.ReplicaState
	// TODO(tschottdorf): figure out whether this is always synchronous with
	// on-disk state (likely iffy during Split/ChangeReplica triggers).
	s.Desc = protoutil.Clone(desc).(*roachpb.RangeDescriptor)

	if as, err := rsl.LoadRangeAppliedState(ctx, reader); err != nil {
		return storagepb.ReplicaState{}, err
	} else if as != nil {
		s.UsingAppliedStateKey = true

		s.RaftAppliedIndex = as.RaftAppliedIndex
		s.LeaseAppliedIndex = as.LeaseAppliedIndex

		ms := as.RangeStats.ToStats()
		s.Stats = &ms
	}
	// The truncated state should not be optional (i.e. the pointer is
	// pointless), but it is and the migration is not worth it.
	truncState, _, err := rsl.LoadRaftTruncatedState(ctx, reader)
	if err != nil {
		return storagepb.ReplicaState{}, err
	}
	s.TruncatedState = &truncState

	return s, nil
}

// Save persists the given ReplicaState to disk. It assumes that the contained
// Stats are up-to-date and returns the stats which result from writing the
// updated State.
//
// As an exception to the rule, the Desc field (whose on-disk state is special
// in that it's a full MVCC value and updated transactionally) is only used for
// its RangeID.
//
// TODO(tschottdorf): test and assert that none of the optional values are
// missing whenever save is called. Optional values should be reserved
// strictly for use in Result. Do before merge.
func (rsl StateLoader) Save(
	ctx context.Context, eng engine.ReadWriter, state storagepb.ReplicaState,
) (enginepb.MVCCStats, error) {

	if err := rsl.SetRaftTruncatedState(ctx, eng, state.TruncatedState); err != nil {
		return enginepb.MVCCStats{}, err
	}

	rai, lai := state.RaftAppliedIndex, state.LeaseAppliedIndex
	if err := rsl.SetRangeAppliedState(ctx, eng, rai, lai, ms); err != nil {
		return enginepb.MVCCStats{}, err
	}

	return *ms, nil
}

// LoadRaftTruncatedState loads the truncated state. The returned boolean returns
// whether the result was read from the TruncatedStateLegacyKey. If both keys
// are missing, it is false which is used to migrate into the unreplicated key.
//
// See VersionUnreplicatedRaftTruncatedState.
func (rsl StateLoader) LoadRaftTruncatedState(
	ctx context.Context, reader engine.Reader,
) (_ roachpb.RaftTruncatedState, isLegacy bool, _ error) {
	var truncState roachpb.RaftTruncatedState
	if found, err := engine.MVCCGetProto(
		ctx, reader, rsl.RaftTruncatedStateKey(), hlc.Timestamp{}, &truncState, engine.MVCCGetOptions{},
	); err != nil {
		return roachpb.RaftTruncatedState{}, false, err
	} else if found {
		return truncState, false, nil
	}

	// If the "new" truncated state isn't there (yet), fall back to the legacy
	// truncated state. The next log truncation will atomically rewrite them
	// assuming the cluster version has advanced sufficiently.
	//
	// See VersionUnreplicatedRaftTruncatedState.

	legacyFound, err := engine.MVCCGetProto(
		ctx, reader, rsl.RaftTruncatedStateLegacyKey(), hlc.Timestamp{}, &truncState, engine.MVCCGetOptions{},
	)

	if err != nil {
		return roachpb.RaftTruncatedState{}, false, err
	}

	return truncState, legacyFound, nil
}

// SetRaftTruncatedState overwrites the truncated state.

func (rsl StateLoader) SetRaftTruncatedState(
	ctx context.Context, eng engine.ReadWriter, truncState *roachpb.RaftTruncatedState,
) error {
	if (*truncState == roachpb.RaftTruncatedState{}) {
		return errors.New("cannot persist empty RaftTruncatedState")
	}

	return engine.MVCCPutProto(ctx, eng, nil, /* ms */
		rsl.RaftTruncatedStateKey(), hlc.Timestamp{}, nil, truncState)

}

// LoadHardState loads the HardState.

func (rsl StateLoader) LoadHardState(
	ctx context.Context, reader engine.Reader,
) (raftpb.HardState, error) {

	var hs raftpb.HardState
	found, err := engine.MVCCGetProto(ctx, reader, rsl.RaftHardStateKey(),
		hlc.Timestamp{}, &hs, engine.MVCCGetOptions{})
	if !found || err != nil {
		return raftpb.HardState{}, err
	}
	return hs, nil
}

// SetHardState overwrites the HardState.
func (rsl StateLoader) SetHardState(
	ctx context.Context, batch engine.ReadWriter, st raftpb.HardState,
) error {
	return engine.MVCCPutProto(ctx, batch, nil,
		rsl.RaftHardStateKey(), hlc.Timestamp{}, nil, &st)
}

// SynthesizeRaftState creates a Raft state which synthesizes both a HardState
// and a lastIndex from pre-seeded data in the engine (typically created via
// writeInitialReplicaState and, on a split, perhaps the activity of an
// uninitialized Raft group)
func (rsl StateLoader) SynthesizeRaftState(ctx context.Context, eng engine.ReadWriter) error {
	hs, err := rsl.LoadHardState(ctx, eng)
	if err != nil {
		return err
	}

	truncState, _, err := rsl.LoadRaftTruncatedState(ctx, eng)
	if err != nil {
		return err
	}

	raftAppliedIndex, _, err := rsl.LoadAppliedIndex(ctx, eng)
	if err != nil {
		return err
	}
	return rsl.SynthesizeHardState(ctx, eng, hs, truncState, raftAppliedIndex)
}

// SynthesizeHardState synthesizes an on-disk HardState from the given input,

// taking care that a HardState compatible with the existing data is written.

func (rsl StateLoader) SynthesizeHardState(
	ctx context.Context,
	eng engine.ReadWriter,
	oldHS raftpb.HardState,
	truncState roachpb.RaftTruncatedState,
	raftAppliedIndex uint64,
) error {

	newHS := raftpb.HardState{
		Term: truncState.Term,

		// Note that when applying a Raft snapshot, the applied index is
		// equal to the Commit index represented by the snapshot.
		Commit: raftAppliedIndex,
	}

	if oldHS.Commit > newHS.Commit {
		return log.Safe(errors.Errorf("can't decrease HardState.Commit from %d to %d",
			oldHS.Commit, newHS.Commit))
	}

	if oldHS.Term > newHS.Term {

		// The existing HardState is allowed to be ahead of us, which is
		// relevant in practice for the split trigger. We already checked above
		// that we're not rewinding the acknowledged index, and we haven't
		// updated votes yet.
		newHS.Term = oldHS.Term
	}

	// If the existing HardState voted in this term, remember that.
	if oldHS.Term == newHS.Term {
		newHS.Vote = oldHS.Vote
	}
	err := rsl.SetHardState(ctx, eng, newHS)
	return errors.Wrapf(err, "writing HardState %+v", &newHS)
}
