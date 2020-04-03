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

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/logtags"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// Config configures a Server.
type Config struct {

	// ListenAddr is the address on which the proxy server listens.
	ListenAddr string

	// Insecure disables TLS both between the client and the proxy and the
	// proxy and the gateway.
	Insecure bool

	// CertsDir is the path to a directory containing cockroach node
	// certificates. The server will act as a cockroach node as far as the
	// client is concerned.
	CertsDir string

	// JoinAddrs are the addresses of cockroach gateway nodes to
	// bootstrap downstream discovery.
	JoinAddrs []string

	// TODO(ajwerner): Some control over which cockroach gateway nodes this
	// server should connect to.

}

// String pretty prints the Config.
func (c *Config) String() string {
	return pretty.Sprint(*c)
}

// Server is a proxy server. It listens on a port and then connects to
// downstream servers.
type Server struct {
	cfg                Config
	tlsConfig          *tls.Config
	certificateManager *security.CertificateManager
}

// New constructs a new Server.
func New(cfg Config) (*Server, error) {
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}
	s := Server{
		cfg: cfg,
	}
	if !cfg.Insecure {
		var err error
		if s.certificateManager, err = security.NewCertificateManager(cfg.CertsDir); err != nil {
			return nil, err
		}
		if s.tlsConfig, err = s.certificateManager.GetServerTLSConfig(); err != nil {
			return nil, err
		}
	}
	return &s, nil
}

func (s *Server) Run(ctx context.Context) error {
	l, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return err
	}
	// TODO(ajwerner): Think harder about lifecycles. If our listening socket dies
	// we'll cancel our parent context here. That's probably not the best thing
	// to do.
	log.Infof(ctx, "listening on %v", l.Addr())
	g, ctx := errgroup.WithContext(ctx)
	connCh := make(chan net.Conn)
	g.Go(func() error {
		for {
			conn, err := l.Accept()
			if err != nil {
				return err
			}
			select {
			case connCh <- conn:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
	g.Go(func() error {
		<-ctx.Done()
		_ = l.Close()
		return nil
	})
	g.Go(func() error {
		for {
			select {
			case conn := <-connCh:
				// TODO(ajwerner): keep track of the open connections and close them
				// on shutdown or do something better than the current nothing.
				g.Go(func() error {
					return s.handleConn(ctx, conn)
				})
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
	return g.Wait()
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) error {
	defer func() { _ = conn.Close() }()
	ctx = logtags.AddTag(ctx, "conn", fmt.Sprintf("%s->%s", conn.RemoteAddr(), conn.LocalAddr()))
	buf := make([]byte, 32<<10)
	for {
		n, err := conn.Read(buf)
		if err == io.EOF {
			log.Infof(ctx, "closing connection")
			return nil
		}
		if err != nil {
			log.Infof(ctx, "closing connection with error", err)
			return nil
		}

		// TODO(ajwerner): a whole handshake dance where we negotiate versions and
		// maybe upgrade to a secure connection.
		log.Infof(ctx, "message: %q", string(buf[:n]))
	}
}

func validateConfig(c *Config) error {
	if !c.Insecure {
		if err := validateDirectory(c.CertsDir); err != nil {
			return errors.Wrapf(err, "invalid node-certs-dir %q", c.CertsDir)
		}
	}
	if len(c.JoinAddrs) == 0 {
		return errors.Errorf("invalid empty join-addr")
	}
	return nil
}

var errNotDir = errors.New("not a directory")

// valdiateDirectory validates that dir is a directory
func validateDirectory(dir string) error {
	fi, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return errNotDir
	}
	return nil
}
