package kvtoypb

import "github.com/cockroachdb/cockroach/pkg/util/protoutil"

// Method implements the Request interface.
func (*GetRequest) Method() Method { return Get }

// Method implements the Request interface.
func (*PutRequest) Method() Method { return Put }

// Method implements the Request interface.
func (*ConditionalPutRequest) Method() Method { return ConditionalPut }

// Method implements the Request interface.
func (*DeleteRequest) Method() Method { return Delete }

// Request is an interface for RPC requests.
type Request interface {
	protoutil.Message

	// Method returns the request method.
	Method() Method
}

// Response is an interface for RPC responses.
type Response interface {
	protoutil.Message
}
