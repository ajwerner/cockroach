- Feature Name: Zone Config Caching
- Status: draft
- Start Date: 2019-10-22
- Authors: Andrew Werner
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

**Remember, you can submit a PR with your RFC before the text is
complete. Refer to the [README](README.md#rfc-process) for details.**

# Summary

CockroachDB allows users to control replication, placement, and GC policies
through an abstraction called a zone config. These configs are stored in a
system table called zones. These zone configs are a huge part of properly
using CockroachDB. They are notoriously difficult to use and interact with.

Over time we are certain to want to provide increased visibility into these
zone configs and they are going to become an ever more integral part of the
database.

Each and every node needs a copy of the zone configs to drive the rebalancing,
garbage collection, split and merge subsystems. In order to prevent every node
from constantly reading this relatively large table we gossip the zone config.

This is unfortunate for the very least because the zone config can grow
proportionally with the number of tables and indices in the database.

In common deployments it remains in the kilobytes but it's imaginable that
it could be much larger.

Some operations needs to interact with zone configs transactionally.
In these cases we cannot use the gossipped values as they have no
correspondence serializability.

Furthermore, we tend to keep the state of zone configs as a serialized
blob in memory rather than a more useful data structure.

This work is a critical precursor to implementing protected timestamps.
When going to protect a timestamp we to make sure that the transaction
which protects the timestamp commits at some timestamp prior to which
the range range must have read the protected timestamp table to prove
that it can safely GC that timestamp. In order to determine that deadline
we need to know the minimum gcttl for the span in question (in theory we
should split ranges which are covered by more than one zone config but
we'll just assume that isn't true for now).

Currently interacting with zone configs transactionally is a huge bummer.
If you know exactly which zone config you want because you have all of the
structured IDs then it's ... okay, you can call `GetZoneConfigInTxn` which
will do the transaction read and decode, but it's definitely not great.
Interacting with the total set zone configs when dealing in keys is a
bummer too.

I propose that we add a zone config meta table which has a version which is
incremented on every change to zone configs. Sure it will serialize all zone 
config changes but that seems ... totally fine.

```
CREATE TABLE system.zones_meta (
     _exists BOOL NOT NULL CHECK (_exists) DEFAULT (true) PRIMARY KEY, -- hidden in the table descriptor?
     version INT
)
```

Every time we modify zone configs we'll update the version:
```
UPDATE system.zone_meta SET version = version + 1;
```
(Fine, it'll be an `IncRequest` in practice)

This way we can cache the entire state of the zones table with good 'ol version numbers.
Furthermore, we can ratchet the timestamp we know the current version to be active with
a tiny amount of gossipped metadata.

```
type ZoneConfigsSnapshot interface{
    Version int()
    GetZoneForKey(...)
    GetZoneForTableIndex(...)
}
```

Then we can one node poll and gossip just this version number as well as a
timestamp at which this version number was read.

Then a transaction can interact with a snapshot of this in-memory
data-structure by just observing the version in the meta table. So
long as the transaction observes the version in the meta table it
knows that if it commits then the zone configs have not changed
:raised_hands: serializability.


I then propose we create a new package which will keep a cache of zone
configs in a more useful in-memory data structure.

What we get out of this is a sane and cheap way to transactionally
interact with zone configs. Instead of gossiping all zone configs we
could get away with gossiping just the version. I imagine there's some
edge case about bootstrapping I might be missing but I'm not sure I
see it. If you can only update the zone configs with transactions then
the cluster has to be live for the zone config subsystem to be useful
at all.

