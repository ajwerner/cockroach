package raftstorage

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// Notes(ajwerner): It seems like this object should track things in memory like
// raft log size, last term, etc. Given the general need (or at least desire)
// for atomicity when committing data inside a raft ready we probably should
// create some object like an append batch with a mechanism to commit changes.

// The goal of this package is to encapsulate logic related to storing raft
// state. I could see an argument in favor of having it exclusively deal with
// log entries. For now it also deals with hard state.

// Additionally it deals with truncation state. Maybe that's crazy, maybe it's
// not.

// We definitely deal with sideloading here. This package is going to exist
// orthogonally to replication. Replication will be passed this object and will
// interact with it through interfaces. In particular, this pacakge will know
// about encoding (this is still poorly defined in terms of its interface) as
// well as the entry cache.

// The below two interfaces are the dependencies for this package.

type StateLoader interface {
	// TODO(ajwerner): figure out the dynamics of this call
	LoadRaftTruncatedState(context.Context, engine.Reader) (_ roachpb.RaftTruncatedState, isLegacy bool, _ error)
	SetRaftTruncatedState(context.Context, engine.ReadWriter, *roachpb.RaftTruncatedState) error

	LoadHardState(context.Context, engine.Reader) (raftpb.HardState, error)
	SetHardState(context.Context, engine.ReadWriter, raftpb.HardState) error

	// TODO(ajwerner): figure out the dynamics of this call. Do we need it?
	SynthesizeRaftState(context.Context, engine.ReadWriter) error

	LoadLastIndex(ctx context.Context, reader engine.Reader) (uint64, error)

	RaftLogKey(logIndex uint64) roachpb.Key
}

type EntryCache interface {
	Add(_ []raftpb.Entry, truncate bool)
	Clear(hi uint64)
	Get(idx uint64) (raftpb.Entry, bool)
	Scan(buf []raftpb.Entry, lo, hi, maxBytes uint64) (_ []raftpb.Entry, bytes, nextIdx uint64, exceededMaxBytes bool)
}

// SideloadStorage is the interface used for Raft SSTable sideloading.
// Implementations do not need to be thread safe.
type SideloadStorage interface {
	// The directory in which the sideloaded files are stored. May or may not
	// exist.
	Dir() string
	// Writes the given contents to the file specified by the given index and
	// term. Overwrites the file if it already exists.
	Put(_ context.Context, index, term uint64, contents []byte) error
	// Load the file at the given index and term. Return errSideloadedFileNotFound when no
	// such file is present.
	Get(_ context.Context, index, term uint64) ([]byte, error)
	// Purge removes the file at the given index and term. It may also
	// remove any leftover files at the same index and earlier terms, but
	// is not required to do so. When no file at the given index and term
	// exists, returns errSideloadedFileNotFound.
	//
	// Returns the total size of the purged payloads.
	Purge(_ context.Context, index, term uint64) (int64, error)
	// Clear files that may have been written by this SideloadStorage.
	Clear(context.Context) error
	// TruncateTo removes all files belonging to an index strictly smaller than
	// the given one. Returns the number of bytes freed, the number of bytes in
	// files that remain, or an error.
	TruncateTo(_ context.Context, index uint64) (freed, retained int64, _ error)
	// Returns an absolute path to the file that Get() would return the contents
	// of. Does not check whether the file actually exists.
	Filename(_ context.Context, index, term uint64) (string, error)
}

// TODO(ajwerner): need a constructor to load the last index from the stateloader
// set the term to something invalid.

type Config struct {
	Ambient         log.AmbientContext
	Engine          engine.Engine
	StateLoader     StateLoader
	EntryCache      EntryCache
	SideloadStorage SideloadStorage

	// MaybeGetCommandByID can be used to look up an already deserialized
	// RaftCommand by its ID. This function is used in a best-effort fashion
	// and can always return zero values or be nil.
	MaybeGetCommandByID func(storagebase.CmdIDKey) (storagepb.RaftCommand, bool)

	// GetConfState is used to provide a value for InitialState.
	// It is sort of a bummer. On the one hand it's sort of problematic because of
	// locking. You might imagine that you want this call to grab a lock but what
	// if you're already holding that lock when this call is made? Well maybe it's
	// the case that you are already always holding that lock when this is called
	// what sort of contract is that? I guess this might just push the client to
	// use an atomic.Value. Would that be alright? Probably not. :/
	GetConfState func() raftpb.ConfState
}

type RaftStorage struct {
	ambient             log.AmbientContext
	engine              engine.Engine
	stateLoader         StateLoader
	entryCache          EntryCache
	sideloadStorage     SideloadStorage
	maybeGetRaftCommand func(id storagebase.CmdIDKey) (storagepb.RaftCommand, bool)
	getConfState        func() raftpb.ConfState
	mu                  struct {
		syncutil.RWMutex
		// TODO(ajwerner): think about peers and locking
		truncatedState *roachpb.RaftTruncatedState

		lastTerm           uint64
		lastIndex          uint64
		raftLogSize        int64
		raftLogSizeTrusted bool
	}
}

// invalidLastTerm is an out-of-band value for r.mu.lastTerm that
// invalidates lastTerm caching and forces retrieval of Term(lastTerm)
// from the raftEntryCache/RocksDB.
const invalidLastTerm = 0

func NewRaftStorage(ctx context.Context, cfg Config) (*RaftStorage, error) {
	rs := &RaftStorage{
		ambient:         cfg.Ambient,
		stateLoader:     cfg.StateLoader,
		entryCache:      cfg.EntryCache,
		engine:          cfg.Engine,
		sideloadStorage: cfg.SideloadStorage,
		getConfState:    cfg.GetConfState,
	}
	// Now we need to load some state.
	var err error
	rs.mu.lastIndex, err = rs.stateLoader.LoadLastIndex(ctx, rs.engine)
	if err != nil {
		return nil, err
	}
	rs.mu.lastTerm = invalidLastTerm
	return rs, nil
}

func (rs *RaftStorage) LogSize() (size int64, trusted bool) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.mu.raftLogSize, rs.mu.raftLogSizeTrusted
}

// NewEntryBatch is how clients add new log entries to RaftStorage.
func (rs *RaftStorage) Append(
	ctx context.Context, writer engine.ReadWriter, entries []raftpb.Entry,
) (onCommit func() error, _ error) {
	// We start by grabbing the mutex and recording the current value for the
	// lastTerm, lastIndex, and raftLogSize.
	// Then we attempt to sideload entries
	// Then we do the append which writes
	// the entries into the writeBatch, then finally
	rs.mu.RLock()
	lastIndex := rs.mu.lastIndex // used for append below
	lastTerm := rs.mu.lastTerm
	raftLogSize := rs.mu.raftLogSize
	rs.mu.RUnlock()
	prevLastIndex := lastIndex

	// TODO(ajwerner): eliminate some of the allocations here.
	// All of the entries are appended to distinct keys, returning a new
	// last index.
	thinEntries, sideLoadedEntriesSize, err := rs.maybeSideloadEntriesRaftMuLocked(ctx, entries)
	if err != nil {
		const expl = "during sideloading"
		return nil, errors.Wrap(err, expl)
	}
	raftLogSize += sideLoadedEntriesSize
	if lastIndex, lastTerm, raftLogSize, err = rs.append(
		ctx, writer, lastIndex, lastTerm, raftLogSize, thinEntries,
	); err != nil {
		const expl = "during append"
		return nil, errors.Wrap(err, expl)
	}
	return func() (err error) {
		// We may have just overwritten parts of the log which contain
		// sideloaded SSTables from a previous term (and perhaps discarded some
		// entries that we didn't overwrite). Remove any such leftover on-disk
		// payloads (we can do that now because we've committed the deletion
		// just above).

		firstPurge := entries[0].Index // first new entry written
		purgeTerm := entries[0].Term - 1
		lastPurge := prevLastIndex // old end of the log, include in deletion
		purgedSize, err := maybePurgeSideloaded(ctx, rs.sideloadStorage, firstPurge, lastPurge, purgeTerm)
		if err != nil {
			const expl = "while purging sideloaded storage"
			return errors.Wrap(err, expl)
		}
		raftLogSize -= purgedSize
		if raftLogSize < 0 {
			// Might have gone negative if node was recently restarted.
			raftLogSize = 0
		}
		rs.mu.Lock()
		rs.mu.lastIndex = lastIndex
		rs.mu.lastTerm = lastTerm
		rs.mu.raftLogSize = raftLogSize
		rs.mu.Unlock()
		rs.entryCache.Add(entries, true /* truncate */)
		return nil
	}, nil
}

// append the given entries to the raft log. Takes the previous values of
// p.mu.lastIndex, p.mu.lastTerm, and r.mu.raftLogSize, and returns new values.
// We do this rather than modifying them directly because these modifications
// need to be atomic with the commit of the batch. This method requires that
// r.raftMu is held.
//
// append is intentionally oblivious to the existence of sideloaded proposals.
// They are managed by the caller, including cleaning up obsolete on-disk
// payloads in case the log tail is replaced.
func (rs *RaftStorage) append(
	ctx context.Context,
	batch engine.ReadWriter,
	prevLastIndex uint64,
	prevLastTerm uint64,
	prevRaftLogSize int64,
	entries []raftpb.Entry,
) (uint64, uint64, int64, error) {
	if len(entries) == 0 {
		return prevLastIndex, prevLastTerm, prevRaftLogSize, nil
	}
	var diff enginepb.MVCCStats
	var value roachpb.Value
	for i := range entries {
		ent := &entries[i]
		key := rs.stateLoader.RaftLogKey(ent.Index)

		if err := value.SetProto(ent); err != nil {
			return 0, 0, 0, err
		}
		value.InitChecksum(key)
		var err error
		if ent.Index > prevLastIndex {
			err = engine.MVCCBlindPut(ctx, batch, &diff, key, hlc.Timestamp{}, value, nil /* txn */)
		} else {
			err = engine.MVCCPut(ctx, batch, &diff, key, hlc.Timestamp{}, value, nil /* txn */)
		}
		if err != nil {
			return 0, 0, 0, err
		}
	}

	// Delete any previously appended log entries which never committed.
	lastIndex := entries[len(entries)-1].Index
	lastTerm := entries[len(entries)-1].Term
	for i := lastIndex + 1; i <= prevLastIndex; i++ {
		// Note that the caller is in charge of deleting any sideloaded payloads
		// (which they must only do *after* the batch has committed).
		err := engine.MVCCDelete(ctx, batch, &diff, rs.stateLoader.RaftLogKey(i),
			hlc.Timestamp{}, nil /* txn */)
		if err != nil {
			return 0, 0, 0, err
		}
	}

	raftLogSize := prevRaftLogSize + diff.SysBytes

	return lastIndex, lastTerm, raftLogSize, nil
}

func (rs *RaftStorage) SetHardState(
	ctx context.Context, writeBatch engine.ReadWriter, hs raftpb.HardState,
) error {
	return rs.stateLoader.SetHardState(ctx, writeBatch, hs)
}

var _ raft.Storage = (*RaftStorage)(nil)

// InitialState returns the saved HardState and ConfState information.
// InitialState implements the raft.Storage interface.
func (rs *RaftStorage) InitialState() (hs raftpb.HardState, cs raftpb.ConfState, err error) {
	ctx := rs.ambient.AnnotateCtx(context.TODO())
	hs, err = rs.stateLoader.LoadHardState(ctx, rs.engine)
	// For uninitialized ranges, membership is unknown at this point.
	if raft.IsEmptyHardState(hs) || err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, err
	}
	return hs, rs.getConfState(), nil
}

// Entries returns a slice of log entries in the range [lo,hi).
// MaxSize limits the total size of the log entries returned, but Entries
// returns at least one entry if any.
// Entries implements the raft.Storage interface. Note that maxBytes is advisory
// and this method will always return at least one entry even if it exceeds
// maxBytes. Sideloaded proposals count towards maxBytes with their payloads inlined.
func (rs *RaftStorage) Entries(lo, hi, maxBytes uint64) ([]raftpb.Entry, error) {
	readonly := rs.engine.NewReadOnly()
	defer readonly.Close()
	ctx := rs.ambient.AnnotateCtx(context.TODO())
	if rs.sideloadStorage == nil {
		return nil, errors.New("sideloaded storage is uninitialized")
	}
	return entries(ctx, rs.stateLoader, readonly, rs.entryCache,
		rs.sideloadStorage, lo, hi, maxBytes)
}

// Term returns the term of entry i, which must be in the range
// [FirstIndex()-1, LastIndex()]. The term of the entry before
// FirstIndex is retained for matching purposes even though the
// rest of that entry may not be available.
// Term implements the raft.Storage interface.
func (rs *RaftStorage) Term(i uint64) (uint64, error) {
	// TODO(nvanbenschoten): should we set r.mu.lastTerm when
	//   r.mu.lastIndex == i && r.mu.lastTerm == invalidLastTerm?
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	if rs.mu.lastIndex == i && rs.mu.lastTerm != invalidLastTerm {
		return rs.mu.lastTerm, nil
	}
	// Try to retrieve the term for the desired entry from the entry cache.
	if e, ok := rs.entryCache.Get(i); ok {
		return e.Term, nil
	}
	readonly := rs.engine.NewReadOnly()
	defer readonly.Close()
	ctx := rs.ambient.AnnotateCtx(context.TODO())
	return term(ctx, rs.stateLoader, readonly, rs.entryCache, i)
}

func term(
	ctx context.Context, rsl StateLoader, eng engine.Reader, eCache EntryCache, i uint64,
) (uint64, error) {
	// entries() accepts a `nil` sideloaded storage and will skip inlining of
	// sideloaded entries. We only need the term, so this is what we do.
	ents, err := entries(ctx, rsl, eng, eCache, nil /* sideloaded */, i, i+1, math.MaxUint64 /* maxBytes */)
	if err == raft.ErrCompacted {
		ts, _, err := rsl.LoadRaftTruncatedState(ctx, eng)
		if err != nil {
			return 0, err
		}
		if i == ts.Index {
			return ts.Term, nil
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

// LastIndex returns the index of the last entry in the log.
func (rs *RaftStorage) LastIndex() (uint64, error) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.mu.lastIndex, nil
}

func (rs *RaftStorage) FirstIndex() (uint64, error) {
	ctx := rs.ambient.AnnotateCtx(context.TODO())
	rs.mu.Lock()
	defer rs.mu.Unlock()
	ts, err := rs.raftTruncatedStateLocked(ctx)
	if err != nil {
		return 0, err
	}
	return ts.Index + 1, nil
}

// raftTruncatedStateLocked returns metadata about the log that preceded the
// first current entry. This includes both entries that have been compacted away
// and the dummy entries that make up the starting point of an empty log.
// raftTruncatedStateLocked requires that r.mu is held.
func (rs *RaftStorage) raftTruncatedStateLocked(
	ctx context.Context,
) (roachpb.RaftTruncatedState, error) {
	if rs.mu.truncatedState != nil {
		return *rs.mu.truncatedState, nil
	}
	ts, _, err := rs.stateLoader.LoadRaftTruncatedState(ctx, rs.engine)
	if err != nil {
		return ts, err
	}
	if ts.Index != 0 {
		rs.mu.truncatedState = &ts
	}
	return ts, nil
}

// Snapshot returns the most recent snapshot.
// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
// so raft state machine could know that Storage needs some time to prepare
// snapshot and call Snapshot later.
func (rs *RaftStorage) Snapshot() (raftpb.Snapshot, error) {
	panic("not implemented")
}

// TODO(ajwerner): refactor this to be cleaner
// entries retrieves entries from the engine. To accommodate loading the term,
// `sideloaded` can be supplied as nil, in which case sideloaded entries will
// not be inlined, the raft entry cache will not be populated with *any* of the
// loaded entries, and maxBytes will not be applied to the payloads.
func entries(
	ctx context.Context,
	rsl StateLoader,
	eng engine.Reader,
	eCache EntryCache,
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

	ents, size, hitIndex, exceededMaxBytes := eCache.Scan(ents, lo, hi, maxBytes)

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

	scanFunc := func(ent raftpb.Entry) (bool, error) {
		// Exit early if we have any gaps or it has been compacted.
		if ent.Index != expectedIndex {
			return true, nil
		}
		expectedIndex++

		if sniffSideloadedRaftCommand(ent.Data) {
			canCache = canCache && sideloaded != nil
			if sideloaded != nil {
				newEnt, err := maybeInlineSideloadedRaftCommand(ctx, ent, sideloaded, eCache)
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

	var ent raftpb.Entry
	_, err := engine.MVCCIterate(
		ctx, eng,
		rsl.RaftLogKey(lo),
		rsl.RaftLogKey(hi),
		hlc.Timestamp{},
		engine.MVCCScanOptions{},
		func(kv roachpb.KeyValue) (bool, error) {
			if err := kv.Value.GetProto(&ent); err != nil {
				return false, err
			}
			return scanFunc(ent)
		})
	if err != nil {
		return nil, err
	}

	// Cache the fetched entries, if we may.
	if canCache {
		eCache.Add(ents, false)
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
		lastIndex, err := rsl.LoadLastIndex(ctx, eng)
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
	ts, _, err := rsl.LoadRaftTruncatedState(ctx, eng)
	if err != nil {
		return nil, err
	}
	if ts.Index >= lo {
		// The requested lo index has already been truncated.
		return nil, raft.ErrCompacted
	}
	// The requested lo index does not yet exist.
	return nil, raft.ErrUnavailable
}

// maybeSideloadEntriesRaftMuLocked should be called with a slice of "fat"
// entries before appending them to the Raft log. For those entries which are
// sideloadable, this is where the actual sideloading happens: in come fat
// proposals, out go thin proposals. Note that this method is to be called
// before modifications are persisted to the log. The other way around is
// incorrect since an ill-timed crash gives you thin proposals and no files.
//
// The passed-in slice is not mutated.
func (rs *RaftStorage) maybeSideloadEntriesRaftMuLocked(
	ctx context.Context, entriesToAppend []raftpb.Entry,
) (_ []raftpb.Entry, sideloadedEntriesSize int64, _ error) {
	return maybeSideloadEntriesImpl(ctx, entriesToAppend, rs.sideloadStorage,
		rs.maybeGetRaftCommand)
}

// maybeSideloadEntriesImpl iterates through the provided slice of entries. If
// no sideloadable entries are found, it returns the same slice. Otherwise, it
// returns a new slice in which all applicable entries have been sideloaded to
// the specified SideloadStorage. maybeRaftCommand is called when sideloading is
// necessary and can optionally supply a pre-Unmarshaled RaftCommand (which
// usually is provided by the Replica in-flight proposal map.
func maybeSideloadEntriesImpl(
	ctx context.Context,
	entriesToAppend []raftpb.Entry,
	sideloaded SideloadStorage,
	maybeRaftCommand func(storagebase.CmdIDKey) (storagepb.RaftCommand, bool),
) (_ []raftpb.Entry, sideloadedEntriesSize int64, _ error) {

	cow := false
	for i := range entriesToAppend {
		var err error
		if sniffSideloadedRaftCommand(entriesToAppend[i].Data) {
			log.Event(ctx, "sideloading command in append")
			if !cow {
				// Avoid mutating the passed-in entries directly. The caller
				// wants them to remain "fat".
				log.Eventf(ctx, "copying entries slice of length %d", len(entriesToAppend))
				cow = true
				entriesToAppend = append([]raftpb.Entry(nil), entriesToAppend...)
			}

			ent := &entriesToAppend[i]
			cmdID, data := DecodeRaftCommand(ent.Data) // cheap
			strippedCmd, ok := maybeRaftCommand(cmdID)
			if ok {
				// Happy case: we have this proposal locally (i.e. we proposed
				// it). In this case, we can save unmarshalling the fat proposal
				// because it's already in-memory.
				if strippedCmd.ReplicatedEvalResult.AddSSTable == nil {
					log.Fatalf(ctx, "encountered sideloaded non-AddSSTable command: %+v", strippedCmd)
				}
				log.Eventf(ctx, "command already in memory")
				// The raft proposal is immutable. To respect that, shallow-copy
				// the (nullable) AddSSTable struct which we intend to modify.
				addSSTableCopy := *strippedCmd.ReplicatedEvalResult.AddSSTable
				strippedCmd.ReplicatedEvalResult.AddSSTable = &addSSTableCopy
			} else {
				// Bad luck: we didn't have the proposal in-memory, so we'll
				// have to unmarshal it.
				log.Event(ctx, "proposal not already in memory; unmarshaling")
				if err := protoutil.Unmarshal(data, &strippedCmd); err != nil {
					return nil, 0, err
				}
			}

			if strippedCmd.ReplicatedEvalResult.AddSSTable == nil {
				// Still no AddSSTable; someone must've proposed a v2 command
				// but not becaused it contains an inlined SSTable. Strange, but
				// let's be future proof.
				log.Warning(ctx, "encountered sideloaded Raft command without inlined payload")
				continue
			}

			// Actually strip the command.
			dataToSideload := strippedCmd.ReplicatedEvalResult.AddSSTable.Data
			strippedCmd.ReplicatedEvalResult.AddSSTable.Data = nil

			{
				data = make([]byte, raftCommandPrefixLen+strippedCmd.Size())
				encodeRaftCommandPrefix(data[:raftCommandPrefixLen], raftVersionSideloaded, cmdID)
				_, err := protoutil.MarshalToWithoutFuzzing(&strippedCmd, data[raftCommandPrefixLen:])
				if err != nil {
					return nil, 0, errors.Wrap(err, "while marshaling stripped sideloaded command")
				}
				ent.Data = data
			}

			log.Eventf(ctx, "writing payload at index=%d term=%d", ent.Index, ent.Term)
			if err = sideloaded.Put(ctx, ent.Index, ent.Term, dataToSideload); err != nil {
				return nil, 0, err
			}
			sideloadedEntriesSize += int64(len(dataToSideload))
		}
	}
	return entriesToAppend, sideloadedEntriesSize, nil
}

// maybeInlineSideloadedRaftCommand takes an entry and inspects it. If its
// command encoding version indicates a sideloaded entry, it uses the entryCache
// or SideloadStorage to inline the payload, returning a new entry (which must
// be treated as immutable by the caller) or nil (if inlining does not apply)
//
// If a payload is missing, returns an error whose Cause() is
// errSideloadedFileNotFound.
func maybeInlineSideloadedRaftCommand(
	ctx context.Context, ent raftpb.Entry, sideloaded SideloadStorage, entryCache EntryCache,
) (*raftpb.Entry, error) {
	if !sniffSideloadedRaftCommand(ent.Data) {
		return nil, nil
	}
	log.Event(ctx, "inlining sideloaded SSTable")
	// We could unmarshal this yet again, but if it's committed we
	// are very likely to have appended it recently, in which case
	// we can save work.
	cachedSingleton, _, _, _ := entryCache.Scan(
		nil, ent.Index, ent.Index+1, 1<<20,
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
		ent.Data = EncodeRaftCommandV2(cmdID, data)
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

func sniffSideloadedRaftCommand(data []byte) (sideloaded bool) {
	return len(data) > 0 && data[0] == byte(raftVersionSideloaded)
}
