package kvtoypb

import "github.com/cockroachdb/cockroach/pkg/util/protoutil"

// Method implements the Request interface.
func (*GetRequest) Method() Method { return Get }

func (*GetRequest) flags() flagSet { return isRead }

// Method implements the Request interface.
func (*PutRequest) Method() Method { return Put }

func (*PutRequest) flags() flagSet { return isWrite }

// Method implements the Request interface.
func (*ConditionalPutRequest) Method() Method { return ConditionalPut }

func (*ConditionalPutRequest) flags() flagSet { return isRead | isWrite }

// Method implements the Request interface.
func (*DeleteRequest) Method() Method { return Delete }

func (*DeleteRequest) flags() flagSet { return isWrite }

// Request is an interface for RPC requests.
type Request interface {
	protoutil.Message

	// Method returns the request method.
	Method() Method

	flags() flagSet
}

// Response is an interface for RPC responses.
type Response interface {
	protoutil.Message
}

type flagSet uint64

const (
	isRead flagSet = 1 << iota
	isWrite
)

func (f flagSet) isReadOnly() bool {
	return (f&isRead) != 0 && (f&isWrite) == 0
}

func IsReadOnly(args Request) bool { return args.flags().isReadOnly() }
