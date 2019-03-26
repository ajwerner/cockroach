
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
