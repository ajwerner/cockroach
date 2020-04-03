// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import "github.com/kr/pretty"

// Config configures a Server.
type Config struct {
	ListenAddr string

	// NodeCertsDir is the path to a directory containing cockroach node
	// certificates. The server will act as a cockroach node as far as the
	// client is concerned.
	NodeCertsDir string

	// ClientCertsDir is the path to a directory containing client certs which
	// the proxy will use to connect to cockroach gateway servers.
	ClientCertsDir string

	// DownstreamJoinAddrs are the addresses of cockroach gateway nodes to
	// bootstrap downstream discovery.
	DownstreamJoinAddrs []string

	// TODO(ajwerner): Some control over which cockroach gateway nodes this
	// server should connect to.
}

func (c *Config) String() string {
	return pretty.Sprint(*c)
}

// Server is a proxy server. It listens on a port and then connects to
// downstream servers.
type Server struct {
}
