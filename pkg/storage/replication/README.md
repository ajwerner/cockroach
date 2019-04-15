# Storage from the bottom up

This document will hopefully evolve in to a blog post about my experience trying
to refactor the replication path of the storage package.

In the process I learned that I needed to understand not just what the
mechanisms which existed in the storage package were but I also needed to
better understand why they existed in order to properly abstract the interface
for data replication.

At the beginning I started with relatively vague notions of what replication
entails:

1) Have some piece of data you want to replicate
1) Have some nodes to which you are replicating it
1) Replicate the data using concensus and a log
1) Get some callbacks along the way
1) Sometimes change those nodes
1) Sometimes look at the progress
1) Sometimes send some snapshots either for adding nodes or catching them up

Complexity arises when considering how clients interact with the proposal
lifecycle as well as how the local replication state evolves relative to
on disk state as well as side effects of applying commands.
   * This paragraph is unfortunately unclear
   * The replication package needs to maintain on-disk state
   * That on-disk state will include the hard state for votes and commits as
     well as the actual log.
   * One interesting comment is that raft log truncation happens out of band
     but is something that the replication storage layer is likely going to
     need to be aware of.

This post will proceed by introducing a replication abstraction and then
layering functionality on top of and into this replication abstraction to
create a functional storage system. This process will force us to confront
the challenges adopters of this library will face and to design an abstraction
to anticipate and work with those challenges.

The initial approach was to start from roughly basic principles looking at the
primary API functionality. An ancillary goal to adopt design patterns
from the `acidlib/v2/connect` library. This post will breifly introduce the
`connect` design pattern, will present the initial attempt at a replication
library and then will work through the process of adopting this library to
built an at first simple and then slowly more complex distributed storage
system.

### The `connect` package

The connect package is worth a longer discussion to justify its architecture
but I'll give a brief overview here to motivate its use. The connect pattern
defines the scaffolding for a design pattern that primarily revolves around
clients issuing requests to servers over connections. The pattern provides
guidance on how synchronization and state management between clients and servers
ought to be structured.

#### Why `connect`?

One should consider `connect` as a style or design pattern rather than a design
choice. In part it is motivated by the unfortunate cost of stack switching and
channel-based synchronization in go. Building high-performance, highly
concurrent go applications requires minimizing synchronization overhead. In
particular, yielding execution to the runtime as happens in `select` statements
and channel operations is generally unacceptable in the common case.

Go's pitch is that a channel send should be thought of as costing on the same
order as a function call but unfortunately this is not the case. Maybe we'll be
there one day.

#### What is `connect`?

The main workhorse of the connect model is the `Connection`. A connection is a
logical bus for clients to send messages and receive responses. 


## High level initial API


### Replication 

I did roughly this, I came up with what felt like a nice API for the primary
case of replication. The main thing you want to replicate is data. It seems
useful to provide a mechanism to identify commands so we leave the ID.
Furthermore we leave encoding to the replication package. This may imply a
need for a hook to describe the encoding type but I suspect this can all be
handled in an orthogonal package that deals with storage. This is yet to be
actively explored.

```go
type ProposalMessage interface {
    ID() storagebase.CmdIDKey
    Data() []byte
}
```

This interface allows actively proposing clients to associate whatever
in-memory state with a command that they'd like but is also an interface that
the system will implement with just raw encoded replicated data so that
non-proposing replicas can have a uniform mechanism to deal with the command.

Note:
  
  We could imagine just passing the originally proposed value during application
  if one exists. Maybe we should do that. Would that make the contract clearer?
  Would it obviate the need for the ID? Maybe sideloading prevents us from
  removing the ID?

For now the replication package exposes an implementation of ProposalMessage
called EncodedProposalMessage

```go
// Should this expose the fact that it's just bytes?
// Would it be better as struct { data []byte } ?
type EncodedProposalMessage []byte
```

## Storage

Raft requires that we feed it an API to access its state.
This means accessing log entries and hard state.

...

## Transport

Raft requires that messages be sent between nodes to step the state machine.

The way that the 

## The initial storage system

Before we talk about replication, let's talk about a storage system.

The basic abstraction we're initially going to be discussing is a transactional
key-value store. The basic client abstractions are the `BatchRequest` and
`BatchResponse`. A batch request is a batch of `Request`s. Initially we'll
have a small number of requests:

 * Put
 * Get
 * Delete
 * ConditionalPut

But we can talk about others later.

The storage system provides the contract that batches will be atomically applied
exactly once if they succeed and zero times if they fail. Later we will need to
expose errors which imply ambiguity about whether a batch was applied which
will mean that a batch was applied zero or one times.

In the very first pass we handle write requests serially by grabbing a lock on
the single server. Such a system is not fault-tolerant, unreplicated and has
very low operational concurrency but it's a starting point to discuss the
system. Over time we'll layer in replication, multi-range parallelism,
intra-range parallelism, a multi-range transactional protocol, leases,
lease-holder isolation and batch evaluation.

This system which we'll call `kvtoy` begins with a couple of basic components.

The basic business logic is driven through the `Store` which handles 
`BatchRequest`s via the `Internal` GRPC service defined in `roachpb`.

```protobuf
service Internal {
  rpc Batch (BatchRequest) returns (BatchResponse) {}
}
```

In order to save ourselves some complexity later, we're going to reuse some rich
types from the `roachpb` protocol but leave some fields with zero value until we
need them. This will ensure that we are true to the existing API as
functionality is layered on. Additionally we'll be using the
`pkg/storage/engine` package to store data. This storage interface contains
logic relating to MVCC which we will not be using for likely quite some time
but it would be a bad idea to duplicate this package.

Let's look at the logic for the most basic store:

```go
package kvtoy

type Config struct {
    // Engine is the storage engine used by the store.
    Engine engine.Engine
}

type Store struct {
    engine engine.Engine

    mu struct {
        syncutil.RWMutex
    }
}

func NewStore(cfg Config) *Store {
    return &Store{
        engine: cfg.Engine,
    }
}

func (s *Store) Batch(
	ctx context.Context, ba *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
    if ba.IsReadOnly() {
        return s.handleReadOnlyBatch(ctx, ba)
    }
    return s.handleReadWriteBatch(ctx, ba)
}

func (s *Store) handleReadOnlyBatch(
	ctx context.Context, ba *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
    s.mu.RLock()
    snap := s.engine.NewSnapshot
    s.mu.RUnlock()
    br := ba.CreateReply()
    for i, req := range ba.Requests {
       var resp roachpb.Response
       var err error
       switch req := req.GetInner().(type) {
       case *roachpb.ReadRequest:
           resp, err = s.handleGet(ctx, req, snap)
       default:
           return nil, errors.Errorf("unknown request type %T", req)
       }
       if err != nil {
           return nil, err
       }
       br.Responses[i].SetInner(resp)
    }
    return br, nil
}

func (s *Store) handleReadWriteBatch(
	ctx context.Context, ba *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
    br := ba.CreateReply()
    s.mu.Lock()
    defer s.mu.Unlock()
    batch := s.engine.NewBatch()
    defer batch.Close()
    for i, req := range ba.Requests {
       var resp roachpb.Response
       var err error 
       switch req := req.GetInner().(type) {
       case *roachpb.PutRequest:
           resp, err = s.handleGet(ctx, req, batch)
       case *roachpb.ConditionalPutRequest:
           resp, err = s.handleConditionalPut(ctx, req, batch)
       case *roachpb.DeleteRequest:
           resp, err = s.handleDelete(ctx, req, batch)
       case *roachpb.GetRequest:
           resp, err = s.handleGet(ctx, req, batch)
       default:
           // The type system should prevent this case.
           panic(errors.Errorf("unknown request type %T", req))
       }
       if err != nil {
           return nil, err
       }
       br.Responses[i].SetInner(resp)
    }
    if err := batch.Commit(true); err != nil {
        return nil, err
    }
    return resp, nil
}
```

This is sort of all we need. We need to be able to store data on disk and we
need to do some synchronization between requests. The glaringly missing problem
here is the lack of a server but we're going to set that up separately.

```go
package server

type Config struct {

    // StoragePath is the path to use for the storage.
    // An empty string implies in-memory storage.
    StoragePath string

    // Addr is the address of the server.
    Addr string
}

type Server struct {
    stopper *stop.Stopper
    rpcCtx  *rpc.Context
    server  *grpc.Server
    store   *kvtoy.Store
}

func NewServer(cfg Config) {
    // Make an engine and a server
    eng := engine.NewInMem(roachpb.Attributes{}, 1<<26 /* 64 MB */),

    // Make a store
    // Make a listener
    // Make an rpc context
    // Make a 
}
```

The amazing this is this works. Sure the thing we made isn't distributed but it
is a durable key-value store with batch semantics that can actually perform
admirably for workloads dominated by reads.

Now we're going to more on to the problem of replicating this this data.

## Part II - A replicated KV

Now that we saw how easy it is to build a non-replicated key-value store we're
going to take the existing logic and replicate across a variety of servers in
a fault-tolerant way. This next architecture is not going to be a panacea for
the following reason:

1) Node identity is static and determined out-of-band via pre-configuration

The storage layer in cockroachdb uses a leasing mechanism that gives it a number
of advantages over the part 2 architecture.

1) Allows for simpler distributed command semantics. We can just replicate a
   write batch as opposed to logical commands which will evolve over time.
1) Is more efficient because command logic only needs to be evaluated on one
   replica instead of all.

We hope to build up to this lease-based architecture but we're not going to
start there. Instead we'll start by taking the batched which previously were
evaluated under the single-server mutex and replicate them using a raft-based
concensus mechanism.

We also begin to sow seeds for an important concept, multiple distinct sets
of replicated data. We do this primarily due to our goal to create a replication
library to serve the later stages of this project. We create a new abstraction
called a replica which together with other replicas form a range. For now all
stores will house a single replica. Additionally we have a notion of node and
store where a store corresponds to a single disk and a node is a single machine
which may have several stores. Again, for simplicity, in part 2 each store
corresponds to a single node and a single replica for range id 1.

In order to kick-start a replicated storage system we need to provide each
node with information about the state of the other nodes. We set this up in
`init.go` where we provide a function to write initial state to the storage
engine which the store will use upon construction to learn about the replication
configuration.

Up front we decide on the number of nodes which will be participating in
replication and we create a `RangeDescriptor` which describes all of the nodes.
This concept of a `Range` will become more important later as we deal with
architectures where not all data is on all nodes in the cluster. A range is a
set of data which can be atomically modified by a `BatchRequest`. In the
cockroachdb architecture this range stores user addressible data that in a
contiguous key range together a set of ranges forms the entire keyspace.

We'll dive in to some of those details later as we deal in part 4 with multiple
ranges existing and then in part 5 where we add the logic to implement
cross-range transactions.

For now we'll assume that there is just one range which carries the ID 1 and
that all of our nodes replicate it.

```go

func makeRangeDescriptor(numNodes int) roachpb.RangeDescriptor {
    replicas := make([]roachpb.ReplicaDescriptor, 0, numNodes)
    for i := 0; i < numNodes; i++ {
        replicas = append(replicas, roachpb.ReplicaDescriptor{
            NodeID:    roachpb.NodeID(i),
            StoreID:   roachpb.StoreID(i),
            ReplicaID: roachpb.ReplicaID(i),
        })
    }
    return &roachpb.RangeDescriptor{
        StartKey: roachpb.RKeyMin,
        RangeID: 1,
        Replicas: replicas,
    }
}

func WriteInitialClusterData(ctx context.Context, eng engine.Engine) error {
     rangeDesc := makeRangeDescriptor(3)
     err := engine.MVCCPutProto(ctx, b, nil, 
         keys.RangeDescriptorKey(roachpb.RKeyMin), hlc.Timestamp{}, 
         nil, rangeDesc)
     if err != nil {
         return err
     }
     if _, err := rsl.Save(ctx, b, storagepb.ReplicaState{
         Lease: &roachpb.Lease{
             Replica: replicas[0],
          },
          TruncatedState: &roachpb.RaftTruncatedState{
              Term: 1,
          },
          GCThreshold:        &hlc.Timestamp{},
          Stats:              &enginepb.MVCCStats{},
          TxnSpanGCThreshold: &hlc.Timestamp{},
     }, stateloader.TruncatedStateUnreplicated); err != nil {
         return err
     }
     if err := rsl.SynthesizeRaftState(ctx, b); err != nil {
         return err
     }
     return b.Commit(true)
}
```

There are some things in there which should be a little bit surprising because
they haven't yet been introduced but we'll get there. The particularly
unfortunate bits are the `ReplicaState` 