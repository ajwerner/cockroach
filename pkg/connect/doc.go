// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

/*
Package connect contains interfaces that enable components to easily locate and
communicate with one another. Objects that implement these interfaces can be
reused and plugged into one another in a variety of interesting ways. For
example, a replication component uses a connect.Router to establish
connections with target servers. Depending on the router implementation, this
might use TCP to connect to servers across the world. Or it may directly connect
to servers in the same process in testing scenarios.

Messages

Systems using the connect package asynchronously send and receive messages.
Messages can represent a piece of data, a command, or anything else; to the
connect package, they are all just messages to be sent from place to place. Any
kind of object can be passed a message (it is defined as interface{}), though
some components may only accept messages that implement additional interface(s).
Some messages may be batches of data or commands from the application's point of
view, but to the connect package, it's still just another message.

Messages are sent and received using a familiar request/reply protocol. Each
request must have a corresponding reply, except in the case where a connection
terminates early due to calling Close, or due to an error that terminates the
connection. However, unlike a protocol like traditional HTTP, requests and
replies can be pipelined, meaning the next request can be sent before receiving
the previous reply. This allows many requests to be outstanding at once. In
addition, large requests and replies can be broken into multiple message chunks
and fully streamed using the Chunkable interface.

Connections

Connections are established between components using the Server and Router
interfaces. A Server offers a shared service that can be used concurrently by
multiple clients. A Listener associates a Server with a universal resource
identifier (URI) that is unique on the network. A Router locates and connects to
a Server using its URI.

Note that a Server does not need to be associated with a URI to be usable. For
example, a data store might implement the Server interface. Multiple clients can
connect to it directly, without being routed over a network:

  client := dataStore.NewClient(syn)
  client.Write(ctx, &KVReadMessage{Key: key})
  reply := client.Read().(*KVReadResultMessage)

Using a common message-passing abstraction for all communication between major
components of the system makes it much easier to build a complex, asynchronous,
location-transparent, distributed system. Components that implement the Server
and Router interfaces typically form an interlocking pipeline, with each
component acting as a server for the previous component, and as a client of the
next component. Messages travel along the pipeline, and are inspected, updated,
and routed by each component in turn.

A connection pipeline employs at least two goroutines in order to ensure
progress. One goroutine handles writes and the other handles reads. If the
writer goroutine is blocked writing a request, then the reader goroutine is
still able to read any replies. This is important, because otherwise it would be
possible to deadlock, with a single goroutine blocked writing a request that
can't be processed until a previous reply has been read.

Synchronization

Each connection pipeline is synchronized using a shared sync.Locker primitive
that is propagated to each of the components in the pipeline. This is called the
"syn lock". By sharing the same syn lock, the components ensure that they have
exclusive access to their own state, as well as to the messages that flow
through the pipeline. If there is a reader and writer goroutine, then the lock
causes them to "take turns" writing requests and reading replies.

Before a message can be written to a connection, the caller must ensure that the
syn lock has been acquired. Assuming that all components in the pipeline share
the same syn lock (which is typical), then none of them need to acquire or
release a syn lock, since they know their own caller will take care of that.

Before blocking on any potentially long-running operation (like writing to
network or disk, sleeping, or waiting for another goroutine), components must
release the syn lock. Once they unblock, they then must re-acquire the lock
before proceeding. This is the same pattern followed by sync.Cond. In fact,
sync.Cond can therefore be used to wait for other goroutines and to wake up
other goroutines. For example:

  // Create sync condition, passing the pipeline's syn lock that should be
  // already be acquired by this goroutine.
  cond := sync.NewCond(&syn)
  go func() {
    // Acquire the syn lock to ensure single-threaded access to state.
    syn.Lock()
    defer syn.Unlock()

    // Do a long-running op, and signal to foreground routine when done.
    ...
    cond.Signal()
  }()

  ...
  q.cond.Wait()

Note that the both goroutines acquire the syn lock. The Wait call will first
release the lock before blocking on the event. That will allow the background
routine to run. When it is done, it will signal the condition and release the
syn lock, at which point the foreground routine will wake up and reacquire the
lock.

This overall pattern of synchronization provides a simple concurrency model for
connections (single-threaded access to state), and yet minimizes time spent by
the component pipeline in acquiring and releasing locks (once per request, and
once per blocking operation).
*/
package connect
