package raftstorage

import (
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// DecodeRaftCommand splits a raftpb.Entry.Data into its commandID and
// command portions. The caller is responsible for checking that the data
// is not empty (which indicates a dummy entry generated by raft rather
// than a real command). Usage is mostly internal to the storage package
// but is exported for use by debugging tools.
func DecodeRaftCommand(data []byte) (storagebase.CmdIDKey, []byte) {
	v := raftCommandEncodingVersion(data[0] & raftCommandNoSplitMask)
	if v != raftVersionStandard && v != raftVersionSideloaded {
		panic(fmt.Sprintf("unknown command encoding version %v", data[0]))
	}
	return storagebase.CmdIDKey(data[1 : 1+raftCommandIDLen]), data[1+raftCommandIDLen:]
}

func MakeIDKey() storagebase.CmdIDKey {
	idKeyBuf := make([]byte, 0, raftCommandIDLen)
	idKeyBuf = encoding.EncodeUint64Ascending(idKeyBuf, uint64(rand.Int63()))
	return storagebase.CmdIDKey(idKeyBuf)
}

func EncodeRaftCommandV1(commandID storagebase.CmdIDKey, command []byte) []byte {
	return encodeRaftCommand(raftVersionStandard, commandID, command)
}

func EncodeRaftCommandV2(commandID storagebase.CmdIDKey, command []byte) []byte {
	return encodeRaftCommand(raftVersionSideloaded, commandID, command)
}

type raftCommandEncodingVersion byte

// Raft commands are encoded with a 1-byte version (currently 0 or 1), an 8-byte
// ID, followed by the payload. This inflexible encoding is used so we can
// efficiently parse the command id while processing the logs.
//
// TODO(bdarnell): is this commandID still appropriate for our needs?
const (
	// The initial Raft command version, used for all regular Raft traffic.
	raftVersionStandard raftCommandEncodingVersion = 0
	// A proposal containing an SSTable which preferably should be sideloaded
	// (i.e. not stored in the Raft log wholesale). Can be treated as a regular
	// proposal when arriving on the wire, but when retrieved from the local
	// Raft log it necessary to inline the payload first as it has usually
	// been sideloaded.
	raftVersionSideloaded raftCommandEncodingVersion = 1
	// The prescribed length for each command ID.
	raftCommandIDLen = 8
	// The prescribed length of each encoded command's prefix.
	raftCommandPrefixLen = 1 + raftCommandIDLen
	// The no-split bit is now unused, but we still apply the mask to the first
	// byte of the command for backward compatibility.
	//
	// TODO(tschottdorf): predates v1.0 by a significant margin. Remove.
	raftCommandNoSplitBit  = 1 << 7
	raftCommandNoSplitMask = raftCommandNoSplitBit - 1
)

func encodeRaftCommand(
	version raftCommandEncodingVersion, commandID storagebase.CmdIDKey, command []byte,
) []byte {
	b := make([]byte, raftCommandPrefixLen+len(command))
	encodeRaftCommandPrefix(b[:raftCommandPrefixLen], version, commandID)
	copy(b[raftCommandPrefixLen:], command)
	return b
}

func encodeRaftCommandPrefix(
	b []byte, version raftCommandEncodingVersion, commandID storagebase.CmdIDKey,
) {
	if len(commandID) != raftCommandIDLen {
		panic(fmt.Sprintf("invalid command ID length; %d != %d", len(commandID), raftCommandIDLen))
	}
	if len(b) != raftCommandPrefixLen {
		panic(fmt.Sprintf("invalid command prefix length; %d != %d", len(b), raftCommandPrefixLen))
	}
	b[0] = byte(version)
	copy(b[1:], []byte(commandID))
}
