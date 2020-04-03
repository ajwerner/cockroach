// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/proxy/server"
	"github.com/spf13/cobra"
)

// Main is the entry point for the cli, with a single line calling it intended
// to be the body of an action package main `main` func elsewhere. It is
// abstracted for reuse by duplicated `main` funcs in different distributions.
func Main() {
	if err := app.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var app = cobra.Command{
	Use:   "proxy",
	Short: "Proxy is a smart proxy to maintain cockroachdb connections across gateway restarts",
	RunE:  run,
	Args:  cobra.NoArgs,
}

var serverCfg server.Config

func init() {
	app.Flags().StringVar(&serverCfg.ListenAddr, "listen-addr",
		"0.0.0.0:27327",
		"address on which to listen for inbound cockroach connections")
	app.Flags().StringSliceVar(&serverCfg.DownstreamJoinAddrs, "join-addr",
		nil,
		"addresses to connect to for discovery of cockroach gateway servers")
	app.Flags().StringVar(&serverCfg.ClientCertsDir, "client-certs-dir",
		"",
		"path to a directory with client certificates for connecting to cockroach nodes")
	app.Flags().StringVar(&serverCfg.NodeCertsDir, "node-certs-dir",
		"",
		"path to a directory with node certificates for terminating client connections")
}

func run(cmd *cobra.Command, args []string) error {
	_, err := fmt.Fprintf(cmd.OutOrStdout(), "%v\n", serverCfg.String())
	return err
}
