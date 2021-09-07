// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package rel provides mechanisms to model, index, and query go structs
// using a relational paradigm.
//
// The package provides a means to map a struct fields to a set of Attributes
// to form a schema. This schema can be used to construct a database which may
// index these entities (structs) by those attributes. These entities can be
// queried using an embedded query language modeled on a non-recursive datalog
// and inspired heavily by datomic.
//
// Terminology
//
// TODO(ajwerner)
// Clause, Entity, Attribute, Value, Fact.
//
// Design
//
// TODO(ajwerner):
//  * Be reasonably general, should be able to model everything in protos.
//  * Use underlying fields of structs to avoid allocating like crazy.
//  * Use the go type system, erase it internally.
//
// Query Language
//
// The query language provides a mechanism to reason relationally about data
// stored in regular structs which may themselves have hierarchy between them.
// The structure of the query language is motivated by datomic which is itself
// motivated by datalog. However, the implementation requirements are simpler
// than datomic. We don't need durability and we know that we're embedded in a
// running program. The language is not a true datalog: it does not really
// have the notion of a rule and it certainly doesn't have a means to express
// recursion during the execution of queries. This means that the queries can
// only represent fixed depth joins between relations. Of course, users of
// libraries can generate queries of an arbitrary depth. Furthermore, users
// can implement their own forms of recursion.
//
// Future work
//
// * Arrays, Maps, Slices
//
// TODO(ajwerner): Note that arrays of bytes can probably be used as slice but
// that would probably be unfortunate. We'd probably prefer to shove them into
// a string using some unsafe magic
//
// Motivation
//
// The primary motivation for this package was the relatively straightforward
// problem of determining the set of dependency edges which need to exist in
// the graph for scplan. However, in addition to runtime performance, which
// matters, a big motivation is in the understandability of the thing.
//
// Imagine we have k queries we're going to write, one for each rule we want
// and that we have N elements and that we each query has a depth of d. A
// naive implementation will be O(k*N*d) as we filter the cross product.
// However, queries generally let you filter the to something much smaller than
// N as you go down in depth, usually the query is easy to perfectly constrain
// if you do it based on just a couple of attributes like descriptor ID or
// types.
//
// For example, imagine we have a rule to make all types in some status depend on
// tables which have columns or expressions which use that type. We might want
// to write a query that says: for all types, for all type references that use
// that type, for all tables which are a part of that type reference, add an
// edge. This is a depth 3 query. Now imagine we do a drop cascade such that we
// drop 1000 tables. If we had zero indexing structures, we'd need to scan all
// of the elements just to find the types and then again to find the references
// and again to find the tables. Now, of course, we could make per type hash
// maps or something like that. That would be even more efficient, but less
// expressive and much more verbose.
//
// At the end of the day, for each statement we're going to evaluate some
// constant number of clauses, each of which is going to apply to some subset of
// the elements and is going to need to explore some other constrained subset of
// the elements. If we assume that our queries are generally depth 2 (i.variable. just
// direct references), then maybe this isn't so bad, it'd mean that we'd do at
// most N^2 work for each statement. However, it gets worse when you think about
// transactions which contain many statements (think big migrations). In that
// case, we'd have to do N^2 work N times (N^3). N^3 is starting to get bad even
// if each step only takes a microsecond. Again, all of this could be defeated with
// maps on the right attributes. However, I had a hard time coming up with a nice way
// to reason about writing clauses declaratively which allows us to utilizing map-based
// indexing structures, at least, without generating code and writing a DSL. This seems
// less complex than code generation.
//
// To demonstrate the importance of this, I've added a benchmark that created
// lists+depth = N entities which are tuples (id, next) and the data just points
// to some other element element. Then I defined a constant number of queries
// for each depth to find the list starting at some id. We benchmark how long it
// takes to execute each individual query. We see that the runtime of the
// indexed version runs in O(depth*log(N)) whereas the unindexed version runs in
// O(depth*N).
//
// What this ultimately means is that we can execute queries defined with proper
// indexes in O(N*log(N)) per statement meaning at worst N^2 log(N) which is
// acceptable for an N of ~1000 as opposed to N^3 which isn't really.
//
package rel
