// Code generated by gen_batch.go; DO NOT EDIT.
// GENERATED FILE DO NOT EDIT

package kvtoypb

import (
	"fmt"
	"strconv"
	"strings"
)

// GetInner returns the Request contained in the union.
func (ru RequestUnion) GetInner() Request {
	switch t := ru.GetValue().(type) {
	case *RequestUnion_Get:
		return t.Get
	case *RequestUnion_Put:
		return t.Put
	case *RequestUnion_ConditionalPut:
		return t.ConditionalPut
	case *RequestUnion_Delete:
		return t.Delete
	default:
		return nil
	}
}

// GetInner returns the Response contained in the union.
func (ru ResponseUnion) GetInner() Response {
	switch t := ru.GetValue().(type) {
	case *ResponseUnion_Get:
		return t.Get
	case *ResponseUnion_Put:
		return t.Put
	case *ResponseUnion_ConditionalPut:
		return t.ConditionalPut
	case *ResponseUnion_Delete:
		return t.Delete
	default:
		return nil
	}
}

// SetInner sets the Request in the union.
func (ru *RequestUnion) SetInner(r Request) bool {
	var union isRequestUnion_Value
	switch t := r.(type) {
	case *GetRequest:
		union = &RequestUnion_Get{t}
	case *PutRequest:
		union = &RequestUnion_Put{t}
	case *ConditionalPutRequest:
		union = &RequestUnion_ConditionalPut{t}
	case *DeleteRequest:
		union = &RequestUnion_Delete{t}
	default:
		return false
	}
	ru.Value = union
	return true
}

// SetInner sets the Response in the union.
func (ru *ResponseUnion) SetInner(r Response) bool {
	var union isResponseUnion_Value
	switch t := r.(type) {
	case *GetResponse:
		union = &ResponseUnion_Get{t}
	case *PutResponse:
		union = &ResponseUnion_Put{t}
	case *ConditionalPutResponse:
		union = &ResponseUnion_ConditionalPut{t}
	case *DeleteResponse:
		union = &ResponseUnion_Delete{t}
	default:
		return false
	}
	ru.Value = union
	return true
}

type reqCounts [4]int32

// getReqCounts returns the number of times each
// request type appears in the batch.
func (ba *BatchRequest) getReqCounts() reqCounts {
	var counts reqCounts
	for _, ru := range ba.Requests {
		switch ru.GetValue().(type) {
		case *RequestUnion_Get:
			counts[0]++
		case *RequestUnion_Put:
			counts[1]++
		case *RequestUnion_ConditionalPut:
			counts[2]++
		case *RequestUnion_Delete:
			counts[3]++
		default:
			panic(fmt.Sprintf("unsupported request: %+v", ru))
		}
	}
	return counts
}

var requestNames = []string{
	"Get",
	"Put",
	"CPut",
	"Del",
}

// Summary prints a short summary of the requests in a batch.
func (ba *BatchRequest) Summary() string {
	var b strings.Builder
	ba.WriteSummary(&b)
	return b.String()
}

// WriteSummary writes a short summary of the requests in a batch
// to the provided builder.
func (ba *BatchRequest) WriteSummary(b *strings.Builder) {
	if len(ba.Requests) == 0 {
		b.WriteString("empty batch")
		return
	}
	counts := ba.getReqCounts()
	var tmp [10]byte
	var comma bool
	for i, v := range counts {
		if v != 0 {
			if comma {
				b.WriteString(", ")
			}
			comma = true

			b.Write(strconv.AppendInt(tmp[:0], int64(v), 10))
			b.WriteString(" ")
			b.WriteString(requestNames[i])
		}
	}
}

// The following types are used to group the allocations of Responses
// and their corresponding isResponseUnion_Value union wrappers together.
type getResponseAlloc struct {
	union ResponseUnion_Get
	resp  GetResponse
}
type putResponseAlloc struct {
	union ResponseUnion_Put
	resp  PutResponse
}
type conditionalPutResponseAlloc struct {
	union ResponseUnion_ConditionalPut
	resp  ConditionalPutResponse
}
type deleteResponseAlloc struct {
	union ResponseUnion_Delete
	resp  DeleteResponse
}

// CreateReply creates replies for each of the contained requests, wrapped in a
// BatchResponse. The response objects are batch allocated to minimize
// allocation overhead.
func (ba *BatchRequest) CreateReply() *BatchResponse {
	br := &BatchResponse{}
	br.Responses = make([]ResponseUnion, len(ba.Requests))

	counts := ba.getReqCounts()

	var buf0 []getResponseAlloc
	var buf1 []putResponseAlloc
	var buf2 []conditionalPutResponseAlloc
	var buf3 []deleteResponseAlloc

	for i, r := range ba.Requests {
		switch r.GetValue().(type) {
		case *RequestUnion_Get:
			if buf0 == nil {
				buf0 = make([]getResponseAlloc, counts[0])
			}
			buf0[0].union.Get = &buf0[0].resp
			br.Responses[i].Value = &buf0[0].union
			buf0 = buf0[1:]
		case *RequestUnion_Put:
			if buf1 == nil {
				buf1 = make([]putResponseAlloc, counts[1])
			}
			buf1[0].union.Put = &buf1[0].resp
			br.Responses[i].Value = &buf1[0].union
			buf1 = buf1[1:]
		case *RequestUnion_ConditionalPut:
			if buf2 == nil {
				buf2 = make([]conditionalPutResponseAlloc, counts[2])
			}
			buf2[0].union.ConditionalPut = &buf2[0].resp
			br.Responses[i].Value = &buf2[0].union
			buf2 = buf2[1:]
		case *RequestUnion_Delete:
			if buf3 == nil {
				buf3 = make([]deleteResponseAlloc, counts[3])
			}
			buf3[0].union.Delete = &buf3[0].resp
			br.Responses[i].Value = &buf3[0].union
			buf3 = buf3[1:]
		default:
			panic(fmt.Sprintf("unsupported request: %+v", r))
		}
	}
	return br
}
