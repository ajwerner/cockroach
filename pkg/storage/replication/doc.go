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

// Package replication abstracts state machine replication.
//
// The basic unit of replication is a group. A replication group is referenced
// by a GroupID. A replication group consists of a number of Peers working in
// concert to perform state machine replication. Communication between Peers and
// the network and storage as well as peer state updates are managed by an
// Orchestrator.
package replication
