package rttanalysisccl

import (
	"context"
	"fmt"
	golog "log"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

type eventKind int

const (
	_ eventKind = iota
	streamSend
	streamRecv
	invokeStart
	invokeEnd
)

type event struct {
	kind       eventKind
	method     string
	streamDesc *grpc.StreamDesc
	m          interface{}
	reply      interface{} // only populated with invoke
}

type loggingClassifier struct {
	buf strings.Builder
	l   *golog.Logger
}

func newLoggingClassifier() *loggingClassifier {
	lc := &loggingClassifier{}
	lc.l = golog.New(&lc.buf, "> ", golog.Ltime)
	return lc
}

func (l loggingClassifier) classify(ctx context.Context, ev event) bool {
	switch ev.kind {
	case streamSend:
		l.l.Printf("send %v: %T %v", ev.streamDesc.StreamName, ev.m, ev.m)
	case streamRecv:
		l.l.Printf("recv %v: %T %v", ev.streamDesc.StreamName, ev.m, ev.m)
	case invokeStart:
		l.l.Printf("%v: %T %v", ev.method, ev.m, ev.m)
	}
	return false
}

var _ classifier = loggingClassifier{}

type composingClassifier []classifier

func any(cc ...classifier) composingClassifier {
	return cc
}

func (cc composingClassifier) classify(ctx context.Context, ev event) bool {
	for _, c := range cc {
		if c.classify(ctx, ev) {
			return true
		}
	}
	return false
}

var _ classifier = (*composingClassifier)(nil)

type recordingStream struct {
	ctx  context.Context
	desc *grpc.StreamDesc
	c    classifier
	grpc.ClientStream
}

type classifier interface {
	classify(context.Context, event) bool
}

type completableClassifier interface {
	classifier
	done() bool
}

type sequentialClassifier []completableClassifier

func sequence(c ...completableClassifier) *sequentialClassifier {
	return (*sequentialClassifier)(&c)
}

func (e *sequentialClassifier) done() bool { return len(*e) == 0 }

func (e *sequentialClassifier) classify(ctx context.Context, ev event) bool {
	if len(*e) > 0 && (*e)[0].classify(ctx, ev) {
		(*e) = (*e)[1:]
		return true
	}
	return false
}

type parallelClassifier []completableClassifier

func parallel(c ...completableClassifier) *parallelClassifier {
	return (*parallelClassifier)(&c)
}

func (e *parallelClassifier) done() bool {
	return len(*e) == 0
}

func (e *parallelClassifier) classify(ctx context.Context, ev event) bool {
	for i, c := range *e {
		if c.classify(ctx, ev) {
			if c.done() {
				(*e) = append((*e)[:i], (*e)[i+1:]...)
			}
			return true
		}
	}
	return false
}

type classifierFunc func(context.Context, event) bool

type invokeClassifier classifierFunc

func (c invokeClassifier) classify(ctx context.Context, e event) bool {
	if e.kind != invokeStart {
		return false
	}
	return c(ctx, e)
}

func (c classifierFunc) classify(ctx context.Context, e event) bool {
	return c(ctx, e)
}

type epochsClassifier struct {
	c []*parallelClassifier
}

type rangeFeedClassifier struct {
	t    *testing.T
	span roachpb.Span
}

func (r rangeFeedClassifier) classify(ctx context.Context, e event) bool {
	var span roachpb.Span
	switch m := e.m.(type) {
	case *roachpb.RangeFeedEvent:
		if m.Val != nil {
			span.Key = decodeKeyPrefix(r.t, m.Val.Key)
		} else if m.Checkpoint != nil {
			span = decodeSpanPrefix(r.t, m.Checkpoint.Span)
		}
	case *roachpb.RangeFeedRequest:
		span = decodeSpanPrefix(r.t, m.Span)
	default:
		return false
	}
	log.Infof(ctx, "%v %v", span, r.span)
	return r.span.Contains(span)
}

func decodeSpanPrefix(t *testing.T, sp roachpb.Span) roachpb.Span {
	return roachpb.Span{
		Key:    decodeKeyPrefix(t, sp.Key),
		EndKey: decodeKeyPrefix(t, sp.EndKey),
	}
}

func decodeKeyPrefix(t *testing.T, key roachpb.Key) roachpb.Key {
	if len(key) == 0 {
		return key
	}
	rem, _, err := keys.DecodeTenantPrefix(key)
	assert.NoError(t, err)
	return rem
}

func (r recordingStream) SendMsg(m interface{}) error {
	r.c.classify(r.ctx, event{
		kind:       streamSend,
		streamDesc: r.desc,
		m:          m,
	})
	return r.ClientStream.SendMsg(m)
}

func (r recordingStream) RecvMsg(m interface{}) error {
	if err := r.ClientStream.RecvMsg(m); err != nil {
		return err
	}
	r.c.classify(r.ctx, event{
		kind:       streamRecv,
		streamDesc: r.desc,
		m:          m,
	})
	return nil
}

type atomicClassifier struct {
	mu struct {
		syncutil.Mutex
		classifier
	}
}

func (c *atomicClassifier) classify(ctx context.Context, e event) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.classify(ctx, e)
}

func (c *atomicClassifier) done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cc, ok := c.mu.classifier.(completableClassifier); ok {
		return cc.done()
	}
	return false
}

type settingsClassifier struct {
	settingsSent, responsesReceived bool
}

func newAtomicClassifier(c classifier) *atomicClassifier {
	var ac atomicClassifier
	ac.mu.classifier = c
	return &ac
}

type gossipClassifier struct {
	haveClusterID bool
}

func (g *gossipClassifier) classify(ctx context.Context, e event) bool {
	switch m := e.m.(type) {
	case *roachpb.GossipSubscriptionRequest:
		return true
	case *roachpb.GossipSubscriptionEvent:
		if m.Key == gossip.KeyClusterID {
			g.haveClusterID = true
		}
		return true
	default:
		return false
	}
}

func (g *gossipClassifier) done() bool {
	return g.haveClusterID
}

func newGossipClassifier() *gossipClassifier {
	return &gossipClassifier{}
}

func newSettingsClassifier() *settingsClassifier {
	return &settingsClassifier{}
}

func (s *settingsClassifier) classify(ctx context.Context, e event) bool {
	switch e.m.(type) {
	case *roachpb.TenantSettingsRequest:
		s.settingsSent = true
	case *roachpb.TenantSettingsEvent:
		s.responsesReceived = true
	default:
		return false
	}
	return true
}

func (s *settingsClassifier) done() bool {
	return s.responsesReceived && s.settingsSent
}

type pingClassifier struct{}

func (p pingClassifier) classify(ctx context.Context, e event) bool {
	switch e.m.(type) {
	case *rpc.PingRequest, *rpc.PingResponse:
		return true
	default:
		return false
	}
}

func newRequiredClassifier(t *testing.T, c classifier) classifier {
	return classifierFunc(func(ctx context.Context, e event) bool {
		return assert.Truef(t, c.classify(ctx, e), "%v %T %v", e.kind, e.m, e.m)
	})
}

type rangeLookupRequestClassifier struct {
}

func (r rangeLookupRequestClassifier) classify(ctx context.Context, e event) bool {
	_, isRangeLookup := e.m.(*roachpb.RangeLookupRequest)
	return isRangeLookup
}

type sqlLivenessSessionClassifier struct {
	t    *testing.T
	seen bool
}

var sqllivenessSpan = tableSpan(keys.SqllivenessID)

func (s *sqlLivenessSessionClassifier) classify(ctx context.Context, e event) bool {

	ba, ok := e.m.(*roachpb.BatchRequest)
	if !ok {
		return false
	}
	for i := range ba.Requests {
		if !sqllivenessSpan.Contains(
			decodeSpanPrefix(s.t, ba.Requests[i].GetInner().Header().Span()),
		) {
			return false
		}
	}
	if e.kind == invokeEnd {
		if _, ok := ba.GetArg(roachpb.InitPut); ok {
			s.seen = true
		}
	}
	return true
}

func (s sqlLivenessSessionClassifier) done() bool {
	panic("implement me")
}

func TestServerStartup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DisableDefaultTestTenant: true, // we're going to manually add a tenant
		},
	})
	defer tc.Stopper().Stop(ctx)
	tenantID := serverutils.TestTenantID()
	// Create the tenant, then stop the server.
	{
		ti, sqlDB := serverutils.StartTenant(
			t, tc.Server(0), base.TestTenantArgs{
				TenantID: tenantID,
			},
		)
		sqlutils.MakeSQLRunner(sqlDB).
			CheckQueryResults(t, "SELECT 1", [][]string{{"1"}})
		ti.Stopper().Stop(ctx)
	}
	lc := newLoggingClassifier()
	sc := &settingsClassifier{}
	gc := &gossipClassifier{}
	c := newAtomicClassifier(newRequiredClassifier(t, any(
		lc,
		sequence(
			parallel(sc, gc),
			&sqlLivenessSessionClassifier{t: t},
		),
		any(
			pingClassifier{},
			rangeLookupRequestClassifier{},
			sc,
			gc,
			&sqlLivenessSessionClassifier{t: t},
			&rangeFeedClassifier{
				span: tableSpan(keys.TableStatisticsTableID),
			},
		),
	),
	),
	)
	si, ui := interceptors(c)
	{
		ti, sqlDB := serverutils.StartTenant(
			t, tc.Server(0), base.TestTenantArgs{
				TenantID: tenantID,
				TestingKnobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						ContextTestingKnobs: rpc.ContextTestingKnobs{
							StreamClientInterceptor: si,
							UnaryClientInterceptor:  ui,
						},
					},
				},
			},
		)
		sqlutils.MakeSQLRunner(sqlDB).
			CheckQueryResults(t, "SELECT 1", [][]string{{"1"}})
		ti.Stopper().Stop(ctx)
	}
	fmt.Print(lc.buf.String())
}

func tableSpan(id uint32) roachpb.Span {
	k := keys.SystemSQLCodec.TablePrefix(id)
	return roachpb.Span{Key: k, EndKey: k.PrefixEnd()}
}

func interceptors(
	c classifier,
) (
	si func(target string, class rpc.ConnectionClass) grpc.StreamClientInterceptor,
	ui func(target string, class rpc.ConnectionClass) grpc.UnaryClientInterceptor,
) {
	return func(
			target string, class rpc.ConnectionClass,
		) grpc.StreamClientInterceptor {
			return func(
				ctx context.Context, desc *grpc.StreamDesc,
				cc *grpc.ClientConn, method string,
				streamer grpc.Streamer, opts ...grpc.CallOption,
			) (grpc.ClientStream, error) {
				cs, err := streamer(ctx, desc, cc, method)
				if err != nil {
					return nil, err
				}
				return recordingStream{
					ctx:          ctx,
					desc:         desc,
					c:            c,
					ClientStream: cs,
				}, nil
			}
		}, func(
			target string, class rpc.ConnectionClass,
		) grpc.UnaryClientInterceptor {
			return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				classifyInvocation(ctx, c, method, req)
				if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
					return err
				}
				classifyInvocationResponse(ctx, c, method, req, reply)
				return nil
			}
		}
}

func classifyInvocation(ctx context.Context, c classifier, method string, req interface{}) bool {
	return c.classify(ctx, event{
		kind:   invokeStart,
		m:      req,
		method: method,
	})
}

func classifyInvocationResponse(
	ctx context.Context, c classifier, method string, req, reply interface{},
) bool {
	return c.classify(ctx, event{
		kind:   invokeEnd,
		m:      req,
		method: method,
		reply:  reply,
	})
}
