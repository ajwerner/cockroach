package replication

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/raftentry"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// TODO(ajwerner): move RaftTruncatedState out into its own proto

type peerStorage Peer

var _ raft.Storage = (*peerStorage)(nil)

// InitialState implements the raft.Storage interface.
// InitialState requires that r.mu is held.
func (p *peerStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	ctx := p.logCtx.AnnotateCtx(context.TODO())
	hs, err := p.mu.stateLoader.LoadHardState(ctx, p.pf.storage)
	// For uninitialized ranges, membership is unknown at this point.
	if raft.IsEmptyHardState(hs) || err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, err
	}
	var cs raftpb.ConfState
	for _, peerID := range p.mu.peers {
		cs.Nodes = append(cs.Nodes, uint64(peerID))
	}
	return hs, cs, nil
}

// FirstIndex implements the raft.Storage interface.
func (p *peerStorage) FirstIndex() (uint64, error) {
	ctx := p.logCtx.AnnotateCtx(context.TODO())
	ts, err := (*Peer)(p).raftTruncatedStateLocked(ctx)
	if err != nil {
		return 0, err
	}
	return ts.GetIndex() + 1, nil
}

// raftTruncatedStateLocked returns metadata about the log that preceded the
// first current entry. This includes both entries that have been compacted away
// and the dummy entries that make up the starting point of an empty log.
// raftTruncatedStateLocked requires that r.mu is held.
func (p *Peer) raftTruncatedStateLocked(ctx context.Context) (RaftTruncatedState, error) {
	if p.mu.truncatedState != nil {
		return p.mu.truncatedState, nil
	}
	ts, _, err := p.mu.stateLoader.LoadRaftTruncatedState(ctx, p.pf.storage)
	if err != nil {
		return ts, err
	}
	if ts.GetIndex() != 0 {
		p.mu.truncatedState = ts
	}
	return ts, nil
}

// Entries implements the raft.Storage interface. Note that maxBytes is advisory
// and this method will always return at least one entry even if it exceeds
// maxBytes. Sideloaded proposals count towards maxBytes with their payloads inlined.
func (p *peerStorage) Entries(lo, hi, maxBytes uint64) ([]raftpb.Entry, error) {
	readonly := p.pf.storage.NewReadOnly()
	defer readonly.Close()
	ctx := p.logCtx.AnnotateCtx(context.TODO())
	if p.raftMu.sideloaded == nil {
		return nil, errors.New("sideloaded storage is uninitialized")
	}
	return entries(ctx, p.mu.stateLoader, readonly, p.groupID, p.pf.entryCache,
		p.raftMu.sideloaded, lo, hi, maxBytes)
}

// LastIndex implements the raft.Storage interface.
func (p *peerStorage) LastIndex() (uint64, error) {
	return p.mu.lastIndex, nil
}

// invalidLastTerm is an out-of-band value for r.mu.lastTerm that
// invalidates lastTerm caching and forces retrieval of Term(lastTerm)
// from the raftEntryCache/RocksDB.
const invalidLastTerm = 0

// Term implements the raft.Storage interface.
func (p *peerStorage) Term(i uint64) (uint64, error) {
	// TODO(nvanbenschoten): should we set r.mu.lastTerm when
	//   r.mu.lastIndex == i && r.mu.lastTerm == invalidLastTerm?
	if p.mu.lastIndex == i && p.mu.lastTerm != invalidLastTerm {
		return p.mu.lastTerm, nil
	}
	// Try to retrieve the term for the desired entry from the entry cache.
	if e, ok := p.pf.entryCache.Get(int64(p.groupID), i); ok {
		return e.Term, nil
	}
	readonly := p.pf.storage.NewReadOnly()
	defer readonly.Close()
	ctx := p.logCtx.AnnotateCtx(context.TODO())
	return term(ctx, p.mu.stateLoader, readonly, p.groupID, p.pf.entryCache, i)
}

func term(
	ctx context.Context,
	rsl StateLoader,
	eng engine.Reader,
	rangeID GroupID,
	eCache *raftentry.Cache,
	i uint64,
) (uint64, error) {
	// entries() accepts a `nil` sideloaded storage and will skip inlining of
	// sideloaded entries. We only need the term, so this is what we do.
	ents, err := entries(ctx, rsl, eng, rangeID, eCache, nil /* sideloaded */, i, i+1, math.MaxUint64 /* maxBytes */)
	if err == raft.ErrCompacted {
		ts, _, err := rsl.LoadRaftTruncatedState(ctx, eng)
		if err != nil {
			return 0, err
		}
		if i == ts.GetIndex() {
			return ts.GetTerm(), nil
		}
		return 0, raft.ErrCompacted
	} else if err != nil {
		return 0, err
	}
	if len(ents) == 0 {
		return 0, nil
	}
	return ents[0].Term, nil
}

// Snapshot implements the raft.Storage interface. Snapshot requires that
// r.mu is held. Note that the returned snapshot is a placeholder and
// does not contain any of the replica data. The snapshot is actually generated
// (and sent) by the Raft snapshot queue.
func (p *peerStorage) Snapshot() (raftpb.Snapshot, error) {
	p.mu.AssertHeld()
	appliedIndex := p.mu.appliedIndex
	term, err := p.Term(appliedIndex)
	if err != nil {
		return raftpb.Snapshot{}, err
	}
	return raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: appliedIndex,
			Term:  term,
		},
	}, nil
}

// TODO(ajwerner): refactor this to be cleaner
// entries retrieves entries from the engine. To accommodate loading the term,
// `sideloaded` can be supplied as nil, in which case sideloaded entries will
// not be inlined, the raft entry cache will not be populated with *any* of the
// loaded entries, and maxBytes will not be applied to the payloads.
func entries(
	ctx context.Context,
	rsl StateLoader,
	e engine.Reader,
	gid GroupID,
	eCache *raftentry.Cache,
	sideloaded SideloadStorage,
	lo, hi, maxBytes uint64,
) ([]raftpb.Entry, error) {
	if lo > hi {
		return nil, errors.Errorf("lo:%d is greater than hi:%d", lo, hi)
	}

	n := hi - lo
	if n > 100 {
		n = 100
	}
	ents := make([]raftpb.Entry, 0, n)

	ents, size, hitIndex, exceededMaxBytes := eCache.Scan(ents, int64(gid), lo, hi, maxBytes)

	// Return results if the correct number of results came back or if
	// we ran into the max bytes limit.
	if uint64(len(ents)) == hi-lo || exceededMaxBytes {
		return ents, nil
	}

	// Scan over the log to find the requested entries in the range [lo, hi),
	// stopping once we have enough.
	expectedIndex := hitIndex

	// Whether we can populate the Raft entries cache. False if we found a
	// sideloaded proposal, but the caller didn't give us a sideloaded storage.
	canCache := true

	var ent raftpb.Entry
	scanFunc := func(kv roachpb.KeyValue) (bool, error) {
		if err := kv.Value.GetProto(&ent); err != nil {
			return false, err
		}
		// Exit early if we have any gaps or it has been compacted.
		if ent.Index != expectedIndex {
			return true, nil
		}
		expectedIndex++

		if sniffSideloadedRaftCommand(ent.Data) {
			canCache = canCache && sideloaded != nil
			if sideloaded != nil {
				newEnt, err := maybeInlineSideloadedRaftCommand(
					ctx, gid, ent, sideloaded, eCache,
				)
				if err != nil {
					return true, err
				}
				if newEnt != nil {
					ent = *newEnt
				}
			}
		}

		// Note that we track the size of proposals with payloads inlined.
		size += uint64(ent.Size())
		if size > maxBytes {
			exceededMaxBytes = true
			if len(ents) > 0 {
				return exceededMaxBytes, nil
			}
		}
		ents = append(ents, ent)
		return exceededMaxBytes, nil
	}

	if err := iterateEntries(ctx, e, gid, expectedIndex, hi, scanFunc); err != nil {
		return nil, err
	}

	// Cache the fetched entries, if we may.
	if canCache {
		eCache.Add(int64(gid), ents)
	}

	// Did the correct number of results come back? If so, we're all good.
	if uint64(len(ents)) == hi-lo {
		return ents, nil
	}

	// Did we hit the size limit? If so, return what we have.
	if exceededMaxBytes {
		return ents, nil
	}

	// Did we get any results at all? Because something went wrong.
	if len(ents) > 0 {
		// Was the lo already truncated?
		if ents[0].Index > lo {
			return nil, raft.ErrCompacted
		}

		// Was the missing index after the last index?
		lastIndex, err := rsl.LoadLastIndex(ctx, e)
		if err != nil {
			return nil, err
		}
		if lastIndex <= expectedIndex {
			return nil, raft.ErrUnavailable
		}

		// We have a gap in the record, if so, return a nasty error.
		return nil, errors.Errorf("there is a gap in the index record between lo:%d and hi:%d at index:%d", lo, hi, expectedIndex)
	}

	// No results, was it due to unavailability or truncation?
	ts, _, err := rsl.LoadRaftTruncatedState(ctx, e)
	if err != nil {
		return nil, err
	}
	if ts.GetIndex() >= lo {
		// The requested lo index has already been truncated.
		return nil, raft.ErrCompacted
	}
	// The requested lo index does not yet exist.
	return nil, raft.ErrUnavailable
}

func iterateEntries(
	ctx context.Context,
	e engine.Reader,
	gid GroupID,
	lo, hi uint64,
	scanFunc func(roachpb.KeyValue) (bool, error),
) error {
	_, err := engine.MVCCIterate(
		ctx, e,
		keys.RaftLogKey(roachpb.RangeID(gid), lo),
		keys.RaftLogKey(roachpb.RangeID(gid), hi),
		hlc.Timestamp{},
		engine.MVCCScanOptions{},
		scanFunc,
	)
	return err
}

func sniffSideloadedRaftCommand(data []byte) (sideloaded bool) {
	return len(data) > 0 && data[0] == byte(raftVersionSideloaded)
}

// maybeInlineSideloadedRaftCommand takes an entry and inspects it. If its
// command encoding version indicates a sideloaded entry, it uses the entryCache
// or SideloadStorage to inline the payload, returning a new entry (which must
// be treated as immutable by the caller) or nil (if inlining does not apply)
//
// If a payload is missing, returns an error whose Cause() is
// errSideloadedFileNotFound.
func maybeInlineSideloadedRaftCommand(
	ctx context.Context,
	gid GroupID,
	ent raftpb.Entry,
	sideloaded SideloadStorage,
	entryCache *raftentry.Cache,
) (*raftpb.Entry, error) {
	if !sniffSideloadedRaftCommand(ent.Data) {
		return nil, nil
	}
	log.Event(ctx, "inlining sideloaded SSTable")
	// We could unmarshal this yet again, but if it's committed we
	// are very likely to have appended it recently, in which case
	// we can save work.
	cachedSingleton, _, _, _ := entryCache.Scan(
		nil, int64(gid), ent.Index, ent.Index+1, 1<<20,
	)

	if len(cachedSingleton) > 0 {
		log.Event(ctx, "using cache hit")
		return &cachedSingleton[0], nil
	}

	// Make a shallow copy.
	entCpy := ent
	ent = entCpy

	log.Event(ctx, "inlined entry not cached")
	// Out of luck, for whatever reason the inlined proposal isn't in the cache.
	cmdID, data := DecodeRaftCommand(ent.Data)

	var command storagepb.RaftCommand
	if err := protoutil.Unmarshal(data, &command); err != nil {
		return nil, err
	}

	if len(command.ReplicatedEvalResult.AddSSTable.Data) > 0 {
		// The entry we started out with was already "fat". This happens when
		// the entry reached us through a preemptive snapshot (when we didn't
		// have a ReplicaID yet).
		log.Event(ctx, "entry already inlined")
		return &ent, nil
	}

	sideloadedData, err := sideloaded.Get(ctx, ent.Index, ent.Term)
	if err != nil {
		return nil, errors.Wrap(err, "loading sideloaded data")
	}
	command.ReplicatedEvalResult.AddSSTable.Data = sideloadedData
	{
		data, err := protoutil.Marshal(&command)
		if err != nil {
			return nil, err
		}
		ent.Data = encodeRaftCommandV2(cmdID, data)
	}
	return &ent, nil
}

// assertSideloadedRaftCommandInlined asserts that if the provided entry is a
// sideloaded entry, then its payload has already been inlined. Doing so
// requires unmarshalling the raft command, so this assertion should be kept out
// of performance critical paths.
func assertSideloadedRaftCommandInlined(ctx context.Context, ent *raftpb.Entry) {
	if !sniffSideloadedRaftCommand(ent.Data) {
		return
	}

	var command storagepb.RaftCommand
	_, data := DecodeRaftCommand(ent.Data)
	if err := protoutil.Unmarshal(data, &command); err != nil {
		log.Fatal(ctx, err)
	}

	if len(command.ReplicatedEvalResult.AddSSTable.Data) == 0 {
		// The entry is "thin", which is what this assertion is checking for.
		log.Fatalf(ctx, "found thin sideloaded raft command: %+v", command)
	}
}

// maybePurgeSideloaded removes [firstIndex, ..., lastIndex] at the given term
// and returns the total number of bytes removed. Nonexistent entries are
// silently skipped over.
func maybePurgeSideloaded(
	ctx context.Context, ss SideloadStorage, firstIndex, lastIndex uint64, term uint64,
) (int64, error) {
	var totalSize int64
	for i := firstIndex; i <= lastIndex; i++ {
		size, err := ss.Purge(ctx, i, term)
		if err != nil && errors.Cause(err) != errSideloadedFileNotFound {
			return totalSize, err
		}
		totalSize += size
	}
	return totalSize, nil
}

var errSideloadedFileNotFound = errors.New("sideloaded file not found")
