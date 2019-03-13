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

package connect

import "context"

// Message objects are sent and received over a connection. Messages can
// be a piece of data, a command, or anything else; to the connect package, they
// are all just messages to be sent from place to place. Messages can be any
// type of Go object, but the typical case is for them to be generated by
// protobuf. Some components may require the message to implement additional
// interfaces.
type Message interface{}

// Chunkable is implemented by messages that are one chunk in a larger sequence
// of messages. Callers can determine when the last message of a sequence has
// been sent or received by detecting when IsFinal becomes true. If a message
// sequence is canceled or encounters an error while chunking is in progress,
// then an ErrorMessage will interrupt as the final "chunk".
type Chunkable interface {
	// IsFinal is true when this message is the final chunk in a larger sequence
	// of messages.
	IsFinal() bool
}

// Conn represents a communication channel between components. Messages are sent
// and received using a familiar request/reply protocol. Each request must have
// a corresponding reply, except in the case where a connection terminates early
// due to calling Close, or due to an error that terminates the connection. Once
// a connection has returned an error, it will continue to return an error for
// any future requests.
//
// Unlike a protocol like traditional HTTP, requests and replies can be
// pipelined, meaning the next request can be sent before receiving the previous
// reply. This allows many requests to be outstanding at once. In addition,
// large requests and replies can be broken into multiple message chunks and
// fully streamed using the Chunkable interface.
//
// The syn lock must be acquired before calling any of the methods on Conn. In
// addition, the Write and Read methods are not re-entrant (i.e. a second call
// to Write cannot happen until the first call is complete). However, the Close
// method can be called at any time, by any goroutine (assuming the syn lock has
// been acquired, of course). This results in the premature shutdown of the
// connection.
type Conn interface {

	// Send sends a new request message across the connection. The given
	// context can contain tracing information or other context that's tied to
	// the request. The context covers the entire request/reply lifecycle of the
	// message. Once the connection has been closed, Write is a no-op.
	Send(ctx context.Context, request Message)

	// Recv receives a reply to a previous request sent via the Write method.
	// Note that no context parameter is required, since it was provided to the
	// Write method instead. Once the connection has been closed, Read returns
	// nil.
	Recv() Message

	// Close shuts down the connection. This can be called even if other
	// goroutines are writing or reading. In that case, Close results in the
	// premature shutdown of the connection. If drain is true, then Close will
	// wait for a graceful shutdown of the pipeline. Otherwise, it will tear
	// down the connection as quickly as possible. Note that in the latter case,
	// the Close call may return before the server side of the connection has
	// been fully torn down.
	Close(ctx context.Context, drain bool)
}
