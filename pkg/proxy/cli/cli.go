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

func init() {
	// Set up flags.
}

func run(cmd *cobra.Command, args []string) error {
	_, err := fmt.Fprint(cmd.OutOrStdout(), "Hello, world\n")
	return err
}
