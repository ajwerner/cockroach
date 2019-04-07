package kvtoypb

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/replication"
	"go.etcd.io/etcd/raft/raftpb"
)

func (r *RaftMessageRequest) GetGroupID() replication.GroupID {
	return replication.GroupID(r.RangeID)
}

func (r *RaftMessageRequest) SetGroupID(id replication.GroupID) {
	r.RangeID = roachpb.RangeID(id)
}

func (r *RaftMessageRequest) GetRaftMessage() raftpb.Message {
	return r.Message
}

func (r *RaftMessageRequest) Addr() interface{} {
	return r.ToReplica.StoreID
}

func (r *RaftMessageRequest) SetRaftMessage(msg raftpb.Message) {
	r.Message = msg
}
