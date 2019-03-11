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

package replication

import (
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type orchestrator struct {
	stopper   *stop.Stopper
	transport Transport
	storage   engine.Engine

	mu struct {
		syncutil.RWMutex
		peers map[GroupID]*peer
	}
}

var _ Orchestrator = (*orchestrator)(nil)

// NewOrchestrator constructs a new Orchestrator.
// TODO(ajwerner): where does the raft.EntryCache fit in?
func NewOrchestrator(
	stopper *stop.Stopper, transport Transport, storage engine.Engine, livenessMap LivenessMap,
) Orchestrator {
	o := &orchestrator{
		stopper:   stopper,
		transport: transport,
		storage:   storage,
	}
	o.mu.peers = make(map[GroupID]*peer)
	return o
}

type LivenessMap interface {
	IsAlive(groupID GroupID, replicaID PeerID) bool
}

func (o *orchestrator) NewPeer(id GroupID) (Peer, error) {
	panic("not implemented")
}

func (o *orchestrator) LoadPeer(id GroupID) (Peer, error) {
	panic("not implemented")
}
