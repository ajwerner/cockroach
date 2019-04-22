
# Motivation

In order to build trust in our customers CockroachDB needs to be reliable.
Sometimes databases experience workloads which demand more resources than are
available. This state is called overload. 

It is imperative that CRDB not crash during overload. Furthmore it is preferable
to continue servicing some requests at an accepable latency than to service no
requests or requests only at an extreme latency. 

In order to cope with overload the system must regulate the amount of work that
is accepted in to the system such that overload is mitigated.

Admission control and overload detection is a relatively deep field that very
quickly gets in to areas of theory often outside the realm of computer science.

The reason for this is the primary unit of work are driven to and from the leaf
services yet the admission control decisions must be made at the gateways to be
effective.

This is trivially true because once work begins on some accepted query it is
wasteful to not complete that query. Thus aborting a query once it has reached a
leaf service is generally not a good idea.

That being said, the leaf services generally have the best ability to account
for overload.

# Guide-level

# Quota Pools

# Admission Control using a two layer approach

CockroachDB can be throught of as having several layers of execution.

Commands enter on a client connection, are parsed, then they are
planned and prepared for execution. At some point it may be valuable
to prune load at the inbound client connection level. 
(Footnote) We can certainly imagine adversarial scenarios where floods of client
connections harm the system but let's not primarily concern ourselves
with that situation. A simple knob on # of conns is probably
sufficieint. Maybe a rate limiter on connection creation.
What we want to do is delay and reject queries at the gateway after parsing
and planning before execution.

A challenge is that CRDB is a complex distributed system with a dynamic
topology. It is critical that the mechanisms proposed here protect local
nodes for system-critical tasks without requiring global coordination.
(Foornote) This doesn't mean that it can't augment its behavior by integrating
global information on longer timescales. The system should successfully detect
overload and shed load even without up-to-date global information.

In order to make progress on this topic we split CRDB into two layers:

```
SQL statements 
--------------
KV BatchRequests
```

Ultimately it's likely to make more sense to split CRDB in to three layers
```
SQL statements
--------------
DistSQL Flows
--------------
KV BatchRequests
```

But that's a wrinkle that we'll add later.


We look at these as two domains for quota and rejection. At each layer we'll
independently manage quota but information will propagate asynchronously from
the KV layer to the SQL layer. These coupled systems will then dynamically tune
their allowed quotas to keep the system operating successfully while shedding
load in an understandable and policy defined way. We will likely want the
metric for a positive policy that it disrupts the lowest number of client
connections.

Successful operation is the notion that operations which are accepted by the
system are completed by the system without too much waiting. It also implies
that all required internal operations complete successfully.

When requests are dropped its best that that disruption happen
deterministically. It's better to have one user get no service than all users
getting degraded service. Furthermore it's better for lower priority traffic
to get less sevice than higher priority traffic.

To deal with this we allow clients to set priority in two ways, firstly we
have to define our priority space. For this we're going to rip right off of
DAGOR with its two level priority space

Open question: how big should this space be?

([SYSTEM][USER 1-127][BACKGROUND], [SESS)

In order to have this discussion we need to define successful operation and

Another challenge over traditional admission control systems is that single
queries can represent over an incredible range of costs. It might be the case
that could handle 10000s QPS of a certain type of query can not handle even
2 (total concurrent) of another query due to its processing burden.

This scenario requires different mechanisms to slow queries which are
themselves system crippling. In general we want the system to process accepted
queries as quickly as possible but this scenario implies that we need a
solution which can never starve system critical functions.

In order to do this we are going to have a simplistic model at the KV layer
whereby we look at time spend queued. In general we never want requests queued
for too long but we also don't want requests to fail. 

Queue size is an open question. 

Randomly rejecting requests is a bad idea. Instead we want to reject based on a
current admission control status which uses the DAGOR model.

The other big question is how the system decides to allow a request to pulled
from the queue. For this we need to capture some more information. We need to
allow the domain expert to provide some oracle to decide when a request can be
processed. This might be related to a static number of current outstanding
requests or some other dynamically updated property of the requests.

In the read quota case we can set this to be the guess on the read size
(I suppose). 

Another question is whether it's reasonable to have administrative requests
bypass the mechanism entirely. This simplification means that the system is
not accounting for all traffic. An early simplification that we may later
address is properly handling and priorizing administrative request traffic.
Maybe it's actually critical that we do that early on.


The basic architecture for a rate-keeping system is:

```

type Priority uint16

type BusinessPriority uint8
type UserPriority uint8

package admission

type Controler interface {
     Accept(ctx context.Context, p Priority, req Request) (Ticket, error)
     Release(ctx context.Context, ticket Ticket)
}
```
```
type RateKeeper interface {
     Acquire(ctx context.Context, req Request) Response
     Release(ctx context.Context, resp Response)
     Report(ctx context.Context, report Report)
}
```

TYpe rate keeper state:
     RateKeeper

     # All requests are treated the same
     # Decides whether you can go right now
     # Decision changes based on Tick, Report, and Release, not on Request
     # Controls the rate at which work flows through the system
     # Does not think about admission control
     #      Relates to admission control
     #      Needs to decide how fast work can come out of the queues
     #      This should be controlled by some feedback
     #           Maybe a good signal is the change in the rate of cost flowing through the system
     #                 Imagine that each Request has a Cost and that we know when the request started
     #                         and thus how long the request took then we could compute the rate of cost for the request
     #                             (in average weighted cost/s) and then we can combine that with cost that is in the system
     #                             then we can work on modulating the ammount of cost allowed in the system based on the
     #                             change and value of the rate cost flows through the system. When requests slow down more
     #                             then we should pull fewer things off the queue but we shouldn't touch the queue itself.
     #                             So we just look at what? The byte/s of a request? Maybe the difference between the trailing
     #                             bytes/s at different timescales? Look also at some derivatives? 

     #   Read Quota
     #                 Basic algorithm:
     #                       Trailing QPS
     #                       Trailing weighted average bytes/s (maybe at different intervals)
     #                       Increase quota when the trailing averages for the throughput of individual requests are converged
     #                                It is important to note that this isn't the aggregate throughput
     #                                Have some max.
     #                       We have two free variables, projected quota and total quota size.
     #                       Total quota size we'll do based on a combination of changes in the request-observed throughput
     #                             and the absolute request-observed throughput
     #                       Per-request guess we'll do based on a combination of tracking historical and we could accept estimates to improve the system.

     
        Tick(time.Time)
        Report(Report)
        Release(Response)
        Request(Request) (Response, bool)

     QueueSystem
        Interacts with priority
        QueueSize - should this change dynamically?
        QueuePolicy
        
Inside we have logic to add other information:
              






# Reads backpressure

Reads drive computing cost into the system in a way that writes do not.
This isn't universally true. Imagine using a select as the source for a write.
We need to think carefully about those sorts of operations. The thing is that
those operations will carry at least the cost (think optimizer cost) of their
reads. 

At the very bottom the kv can become a major source of data and needs to be
protected. In most of the OOM situations we observe large volumes of data
flowing out of the key-value store. The best way then to mitigate OOMs is to
slow data coming out of the KV.

That's not to say that all overload conditions require pulling lots of data out
of KV. We can observe performance degrading from pretty simple TPC-C situations.

But maybe those situations are okay? Do we see node liveness fail because of CPU?
I guess we do, but let's put that out of scope for now. We can deal with that
later but we can imagine it being addressed pretty trivially. When node liveness
starts getting slow we can just throttle things (esp distsql).

Dealing with the catastrophic cases we see today is the primary motivation

Goals:
        Prevent OOMs
        Prevent starvations due to large quantities of reads
        Push backpressure to the edge

# Design

Be cautious at the leaf.
Report failure back to the client.
Have the client use that feedback to control flows

We want to approximate the total amount of cost flowing through the system at a time by tracking the 
amount of cost finishing per second and then tracking a ema of that which 

You want to rate limit queries which represent a big shift away from what you've seen.
Okay let's track the exponential moving average of what's finishing and what's starting
