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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

// Command genterraform generate the terraform file used to configure AWS for
// multiregion support.
package main

import (
	"fmt"
	"io"
	"os"
	"text/template"

	"github.com/spf13/cobra"
)

var (
	base = template.New("base")

	header = template.Must(base.New("header").Parse(
		`# ---------------------------------------------------------------------------------------------------------------------
# TERRAFORM SETTINGS
# ---------------------------------------------------------------------------------------------------------------------
terraform {
  required_version = ">= 0.11.8"
  backend "s3" {
    # The key should include your cluster name.
    key            = "terraform/{{ .ResourcePrefix }}"

    # Fixed fields, DO NOT MODIFY.
    bucket         = "{{ .ResourcePrefix }}-cloud-state"
    region         = "us-east-2"
  }
}

# ---------------------------------------------------------------------------------------------------------------------
# Variable names that should not change beyond initial config.
# ---------------------------------------------------------------------------------------------------------------------
locals {
  # Cockroach-Labs Shared
  account_number = {{ printf "%q" .AccountNumber }}
  label          = {{ printf "%q" .ResourcePrefix }}
}

# ---------------------------------------------------------------------------------------------------------------------
# AWS
# ---------------------------------------------------------------------------------------------------------------------`,
	))

	regions = template.Must(base.New("regions").Parse(
		`{{ range .Regions }}
provider "aws" {
  alias   = "{{ $.Resource . }}"
  region  = "{{ . }}"

  # Fixed fields, DO NOT MODIFY.
  version = "~> 1.41"
}

module "aws_{{ $.Resource . }}" {
  providers {
    aws  = "aws.{{ $.Resource . }}"
  }
  region = {{ . | printf "%q" }}
  source = "aws-region"
  label  = {{ $.ResourcePrefix | printf "%q" }}
}
{{ end }}`,
	))

	peerings = template.Must(base.New("peerings").Parse(
		`
{{ range  .Peerings }}
module "vpc_peer_{{index . 0 }}-{{ index . 1 }}" {
  providers {
    aws.owner    = "aws.{{ index . 0 }}"
    aws.peer     = "aws.{{ index . 1 }}"
  }
  owner_vpc_info = "${module.aws_{{index . 0}}.vpc_info}"
  peer_vpc_info  = "${module.aws_{{index . 1}}.vpc_info}"

  label          = {{ $.ResourcePrefix | printf "%q" }}
  source         = "aws-vpc-peer"
}
{{ end }}
`,
	))

	output = template.Must(base.New("output").Parse(
		`output "regions" {
  value = "${list({{- range $index, $el := .Regions }}{{ if $index }},{{end}}
    "${module.aws_{{ $.Resource . }}.extended_vpc_info}"
    {{- end }}
  )}"
}
`))
	tf = template.Must(base.Parse(
		`{{ template "header" . }}
{{ template "regions" . }}
{{ template "peerings" . }}
{{ template "output" . }}
`))
)

type data struct {
	AccountNumber  string
	ResourcePrefix string
	Regions        []string
}

func (d *data) Resource(s string) string {
	return d.ResourcePrefix + "-" + s
}

func (d *data) Peerings() (peerings [][2]string) {
	for i := 0; i < len(d.Regions); i++ {
		for j := i + 1; j < len(d.Regions); j++ {
			peerings = append(peerings, [2]string{
				d.Resource(d.Regions[i]),
				d.Resource(d.Regions[j]),
			})
		}
	}
	return peerings
}

var defaultData = data{
	Regions: []string{
		"ap-northeast-1",
		"ap-northeast-2",
		"ap-south-1",
		"ap-southeast-1",
		"ap-southeast-2",
		"ca-central-1",
		"eu-central-1",
		"eu-west-1",
		"eu-west-2",
		"eu-west-3",
		"sa-east-1",
		"us-east-1",
		"us-east-2",
		"us-west-1",
		"us-west-2",
	},
	AccountNumber:  "541263489771",
	ResourcePrefix: "roachprod",
}

func main() {
	data := defaultData
	output := "-"
	rootCmd := &cobra.Command{
		Use:   "genterraform",
		Short: "genterraform generates a terraform file for use with roachprod on aws.",
		Long: `
genterraform sets up VPCs and security groups for each of specified regions for
the provided account number. The generated file then has an output which is
consumed by roachprod.
`,
		Run: func(_ *cobra.Command, _ []string) {
			out := io.Writer(os.Stderr)
			if output != "-" {
				f, err := os.Create(output)
				exitIfError(err)
				defer f.Close()
				out = f
			}
			exitIfError(tf.Execute(out, &data))
		},
	}
	rootCmd.Flags().StringSliceVar(&data.Regions, "regions", data.Regions,
		"list of regions to operate in")
	rootCmd.Flags().StringVar(&data.AccountNumber, "account-number", data.AccountNumber,
		"list of regions to operate in")
	rootCmd.Flags().StringVarP(&output, "output", "o", output,
		"path to output the generated file, \"-\" for stderr")

	exitIfError(rootCmd.Execute())
}

func exitIfError(err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "%v\n", err)
	os.Exit(1)
}
