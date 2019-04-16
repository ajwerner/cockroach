# Storage from the bottom up

This document will hopefully evolve in to a post of sorts about my experience
refactoring the replication path of the storage package.

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
   * The replication package needs to maintain on-disk state
   * That on-disk state will include the hard state for votes and commits as
     well as the actual log.
   * One interesting comment is that raft log truncation happens out of band
     but is something that the replication storage layer is likely going to
     need to be aware of.

This post will proceed by first introducing the user to a simplified single-host
key-value store and then layering on a replication abstraction to meet and then
layering functionality on top of and into this replication abstraction to
create a functional storage system. This process will force us to confront
the challenges adopters of this library will face and to design an abstraction
to anticipate and work with those challenges.

In part 0 we introduce a completely non-replicated storage abstraction. This
preliminary exercise is a valuable framing for the rest of the work to come.
From there, in part 1, we dive in to adding inflexible replication which does
not permit configuration changes or deal with snapshots. This section forces us
to engage with the meat of the client-facing replication interface as well as
the underlying dependencies to interact with on disk state and network
communication. In part 2 we tackle configuration changes which forces use to
deal with snapshots and proposals which reconfigure the state.

### The `connect` package

An ancillary goal to adopt design patterns from the `acidlib/v2/connect`
library. The connect package is worth a longer discussion to justify its
architecture but I'll give a brief overview here to motivate its use. The
connect pattern defines the scaffolding for a design pattern that primarily
revolves around clients issuing requests to servers over connections. The
pattern provides guidance on how synchronization and state management between
clients and servers ought to be structured. Note that while a major goal is to
adopt this pattern for the replication package, we will not make use of it until
part 1. Don't be alarmed when it does not appear in the preliminary part 0.

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
logical bus for clients to send messages and receive responses. I personally am
struggling between the terms `Client` and `Connection` but the takeaway should
be that when a piece of code needs to leverage functionality provided by a
service it should create a client object on which it can send messages. This
client object should be seen as the workhorse which provides hooks for callbacks
and injections as appropriate for the API. Messages on the other hand ought to
be pieces of data which generally have a clear serialization.

## Part 0 - Unreplicated storage

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
functionality is layered on. We'll also be using the `pkg/storage/engine`
package to store data. This storage interface contains logic relating to MVCC
which we will not be using for likely quite some time but it would be a bad idea
to duplicate this package. Another thing to note is that the `Internal`
interface contains a `RangeFeed` method which we will not be implementing.

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
```

The separation of read and write batches is an optimization that will greatly
improve the performance of the server for read-heavy workloads as `engine`
offers cheap snapshots which we can use to serve reads. Let's look at these
two workhorse methods now.

```go
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

Now let's look at the implementation of the individual requests.

```go

func (s *Store) handleGet(
	ctx context.Context, req *roachpb.GetRequest, eng engine.Reader,
) (roachpb.Response, error) {
	val, _, err := engine.MVCCGet(ctx, eng, req.Key, hlc.Timestamp{}, engine.MVCCGetOptions{})
	if err != nil {
		return nil, err
	}
	return &roachpb.GetResponse{Value: val}, nil
}

func (s *Store) handlePut(
	ctx context.Context, req *roachpb.PutRequest, eng engine.ReadWriter,
) (roachpb.Response, error) {
	err := engine.MVCCPut(ctx, eng, nil, req.Key, hlc.Timestamp{}, req.Value, nil)
	if err != nil {
		return nil, err
	}
	return &roachpb.PutResponse{}, nil
}

func (s *Store) handleDelete(
	ctx context.Context, req *roachpb.DeleteRequest, eng engine.ReadWriter,
) (roachpb.Response, error) {
	err := engine.MVCCDelete(ctx, eng, nil, req.Key, hlc.Timestamp{}, nil)
	if err != nil {
		return nil, err
	}
	return &roachpb.DeleteResponse{}, nil
}

func (s *Store) handleConditionalPut(
	ctx context.Context, req *roachpb.ConditionalPutRequest, eng engine.ReadWriter,
) (roachpb.Response, error) {
	val, _, err := engine.MVCCGet(ctx, eng, req.Key, hlc.Timestamp{}, engine.MVCCGetOptions{})
	if err != nil {
		return nil, err
	}
	if !val.Equal(req.Value) {
		return nil, errors.Errorf("conditional put: expectation failed for key %v: %v != %v",
			req.Key, val, req.Value)
	}
	err = engine.MVCCPut(ctx, eng, nil, req.Key, hlc.Timestamp{}, req.Value, nil)
	if err != nil {
		return nil, err
	}
	return &roachpb.ConditionalPutResponse{}, nil
}
```

This is sort of all we need to produce a working implementation of the Internal
interface. We need to be able to store data on disk and we need to do some 
synchronization between requests. Now we need to test that this all work. 
The glaringly missing problem here is the lack of a server so let's go ahead
and set that up separately in a `server` package.

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
    // Make a grpc server
    // Register the store with the server
    // Serve the server on the listener
}
```

The amazing this is this works. Sure the thing we made isn't distributed but it
is a durable key-value store with batch semantics that can actually perform
admirably for workloads dominated by reads.

In order to test all of this we need to build a binary that can run this server
and then we should write some tests. 

TODO(ajwerner): come back and fill this in, it exists in the `cli`, and
`cmd/kvtoy` packages.

Now we're going to more on to the problem of replicating this this data.

## Part 1 - A basic replicated KV

Let's sketch out an overview of the goals of this section before spending a
while discussing the replication package, its interfaces, dependencies and
their initial implementation. Unlike the previous section, we now want to build
an implementation of the above storage interface which operates across multiple
servers in a fault-tolerant way. Furthermore we're going to require that this
implementation provides linearizable consistency. In order to do this we're
going to leverage a concensus-based replication protocol whereby all batches are
replicated to a quorum of nodes before they are evaluated. It's worth keeping in
mind that this isn't exactly how cockroachdb works but is a useful stepping
stone. Before we can talk about adopting replication we need to introduce the
concept and discuss the dependencies.

### Replication

The goal of a concensus-backed replication is to enable a group of coordinating
servers safetly distribute and store data. We'll go further and say that the
goal of this specific replication package is to enable "state-machine
replication" whereby the members of this coordinating group of nodes
deterministically change state based on evaluation of shared log of commands.
In our case, for this section, these commands will be serialized `BatchRequest`
messages.

Let's begin by looking at the client-facing interface to replicate data.
As discussed in the introduction, we'd like to adopt the connect model.
Central to the `connect` model is having a well-defined set of messages
which will be sent on a client `Conn`. For replication the primary message
will be a `ProposalMessage` as defined below.

```go
type ProposalMessage interface {
    ID() storagebase.CmdIDKey
    Data() []byte
}
```

Historically it has been useful to provide a mechanism to identify commands
so we leave the ID. It is worth noting that this interface leaves details of
proposal encoding to the replication package. This may imply a need for a hook
to describe the encoding type but I suspect this can all be handled in an
orthogonal package that deals with storage to be discussed below.

This interface allows actively proposing clients to associate whatever
in-memory state with a command that they'd like but is also an interface that
the system will implement with just raw encoded replicated data so that
non-proposing replicas can have a uniform mechanism to deal with the command.

Note:

```  
  We could imagine just passing the originally proposed value during application
  if one exists. Maybe we should do that. Would that make the contract clearer?
  Would it obviate the need for the ID? Maybe sideloading prevents us from
  removing the ID?
```

The replication package exposes an implementation of ProposalMessage called
`EncodedProposalMessage` which represents an encoded proposal on replicas which
did not send the original `ProposalMessage`.

```go
// Should this expose the fact that it's just bytes?
// Would it be better as struct { data []byte } ?
type EncodedProposalMessage []byte
```

Now that we've looked at the high level concept of how users will interact with
replication, we need to discuss the design of the system which will coordinate
this replication. Some of the considerations of the replication package fall
out of scope for the system we're attempting to build in this section but
please bear with me as they'll come back up. Let's introduce the key terminology
that will be used by the replication package.

The basic unit for replicated data is a `Group`. A group is identified by an
`int64` value called a `GroupID`. Each group corresponds to a replicated state
machine and consists of a number of `Peer`s. Each peer similarly is identified
by an `int64` value called a `PeerID`. In an attempt to mitigate confusion, in
the context of CRDB a group corresponds to a Range, and a Peer corresponds to a
Replica. Throughout this section there will only be a single Group. Because
later on we'll build a system which must simultaneously deal with `Peer`s from
different groups we make some API considerations for that now.

It is imperative that the replication library be capable of managing peers from
many groups without requiring goroutines for each group. A client interacts with
a group via a handle to a `Peer` struct. A Peer is a local replica which
corresponds to a single group. A `Peer` is created via a `Factory`. The `Factory`
orchestrates lifecycle for all of the `Peer`s it creates, managing a thread pool,
dispatching messages, ticking, etc.

The `Factory` has a number of important dependencies which we'll dig in to
momentarily. Furthermore the `Peer` has some of its own dependencies. Let's
take a peek at the `FactoryConfig`. 

```go
// FactoryConfig contains the necessary dependencies to create a Factory.
type FactoryConfig struct {

	// TODO(ajwerner): should there be a start method that takes a stopper?
	Stopper *stop.Stopper

	// Storage for all of the peers.
	Storage engine.Engine

	// Settings are the cluster settings.
	Settings *cluster.Settings

	// NumWorkers is the number of worker goroutines which will be used to handle
	// events on behalf of peers.
	NumWorkers int

	// RaftConfig configures etcd raft.
	RaftConfig base.RaftConfig

	// RaftTransport is the message bus on which RaftMessages are sent and
	// received.
	RaftTransport connect.Conn

	TestingKnobs TestingKnobs
}
```

The only really interesting piece worth digging in to here is that
RaftTransport. Notice that it is a connect.Conn, my hope is that this
is not an abuse. The `Factory` will receive messages from this Conn and
enqueue them for processing by a peer. A RaftMessage must implement the
following interface:

```
// RaftMessage is a message used to send raft messages on the wire.
// RaftMessage is created by a client provided factory function and the concrete
// type is left up to the client.
type RaftMessage interface {
	connect.Message
	GetGroupID() GroupID
	GetRaftMessage() raftpb.Message
}
```

We will see soon that each peer is configured with a function to create
instances of these `RaftMessage`s. The GroupID enables the replication package
to route messages internally. The client of the replication package is
responsible for routing messages to the intended `RaftTransport` on the other
side. In a below section we'll explore the `rafttransport` package which
provides a concrete implementation of the `RaftTransport` that we will adopt and
use in our KV.

Now let's explore what it takes to create a `Peer`.

```
// PeerConfig is used to create a new Peer.
type PeerConfig struct {
	log.AmbientContext
	GroupID            GroupID
	PeerID             PeerID
	Peers              []PeerID
	ProcessCommand     ProcessCommandFunc
	ProcessConfChanged ProcessConfChangeFunc
	RaftMessageFactory func(raftpb.Message) RaftMessage
	RaftStorage        RaftStorage
}
```

Unlike the `FactoryConfig` the `PeerConfig` has a lot to unpack.


#### RaftStorage

Raft requires that we feed it an API to access its state.
This means accessing log entries and hard state.

...

#### RaftTransport

Raft requires that messages be sent between nodes to step the state machine.

...

### The replicated Store

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