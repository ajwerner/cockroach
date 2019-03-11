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

type Conn interface {
	// Write sends a new request message across the connection. The given
	// context can contain tracing information or other context that's tied to
	// the request. The context covers the entire request/reply lifecycle of the
	// message. Once the connection has been closed, Write is a no-op.
	Send(ctx context.Context, request Message)

	// Read receives a reply to a previous request sent via the Write method.
	// Note that no context parameter is required, since it was provided to the
	// Write method instead. Once the connection has been closed, Read returns
	// nil.
	Read() Message

	// Close shuts down the connection. This can be called even if other
	// goroutines are writing or reading. In that case, Close results in the
	// premature shutdown of the connection. If drain is true, then Close will
	// wait for a graceful shutdown of the pipeline. Otherwise, it will tear
	// down the connection as quickly as possible. Note that in the latter case,
	// the Close call may return before the server side of the connection has
	// been fully torn down.
	Close(ctx context.Context, drain bool)
}

type Message interface{}
