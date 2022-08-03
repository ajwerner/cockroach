// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// FunctionDefinition implements a reference to the (possibly several)
// overloads for a built-in function.
// TODO(Chengxiong): Remove this struct entirely. Instead, use overloads from
// function resolution or use "GetBuiltinProperties" if the need is to only look
// at builtin functions(there are such existing use cases). Also change "Name"
// of ResolvedFunctionDefinition to Name type.
type FunctionDefinition struct {
	// Name is the short name of the function.
	Name string

	// Definition is the set of overloads for this function name.
	Definition []*Overload

	// FunctionProperties are the properties common to all overloads.
	FunctionProperties
}

// ResolvedFunctionDefinition is similar to FunctionDefinition but with all the
// overloads qualified with schema name.
type ResolvedFunctionDefinition struct {
	// Name is the name of the function and not the name of the schema. And, it's
	// not qualified.
	Name string

	overloads []*Overload
	schemas   []string
}

// NewResolvedFunctionDefinition constructs a new ResolvedFunctionDefinition.
func NewResolvedFunctionDefinition(
	name string, schema string, overloads []*Overload,
) *ResolvedFunctionDefinition {
	repeatString := func(s string, n int) []string {
		ret := make([]string, n)
		for i := 0; i < n; i++ {
			ret[i] = s
		}
		return ret
	}
	return &ResolvedFunctionDefinition{
		Name:      name,
		overloads: overloads,
		schemas:   repeatString(schema, len(overloads)),
	}
}

// QualifiedOverload is a wrapper of Overload prefixed with a schema name.
// It indicates that the overload is defined with the specified schema.
type QualifiedOverload struct {
	Schema string
	*Overload
}

// FunctionProperties defines the properties of the built-in
// functions that are common across all overloads.
type FunctionProperties struct {
	// UnsupportedWithIssue, if non-zero indicates the built-in is not
	// really supported; the name is a placeholder. Value -1 just says
	// "not supported" without an issue to link; values > 0 provide an
	// issue number to link.
	UnsupportedWithIssue int

	// Undocumented, when set to true, indicates that the built-in function is
	// hidden from documentation. This is currently used to hide experimental
	// functionality as it is being developed.
	Undocumented bool

	// Private, when set to true, indicates the built-in function is not
	// available for use by user queries. This is currently used by some
	// aggregates due to issue #10495. Private functions are implicitly
	// considered undocumented.
	Private bool

	// DistsqlBlocklist is set to true when a function depends on
	// members of the EvalContext that are not marshaled by DistSQL
	// (e.g. planner). Currently used for DistSQL to determine if
	// expressions can be evaluated on a different node without sending
	// over the EvalContext.
	//
	// TODO(andrei): Get rid of the planner from the EvalContext and then we can
	// get rid of this blocklist.
	DistsqlBlocklist bool

	// Class is the kind of built-in function (normal/aggregate/window/etc.)
	Class FunctionClass

	// Category is used to generate documentation strings.
	Category string

	// AvailableOnPublicSchema indicates whether the function can be resolved
	// if it is found on the public schema.
	AvailableOnPublicSchema bool

	// ReturnLabels can be used to override the return column name of a
	// function in a FROM clause.
	// This satisfies a Postgres quirk where some json functions have
	// different return labels when used in SELECT or FROM clause.
	ReturnLabels []string

	// AmbiguousReturnType is true if the builtin's return type can't be
	// determined without extra context. This is used for formatting builtins
	// with the FmtParsable directive.
	AmbiguousReturnType bool

	// HasSequenceArguments is true if the builtin function takes in a sequence
	// name (string) and can be used in a scalar expression.
	// TODO(richardjcai): When implicit casting is supported, these builtins
	// should take RegClass as the arg type for the sequence name instead of
	// string, we will add a dependency on all RegClass types used in a view.
	HasSequenceArguments bool

	// CompositeInsensitive indicates that this function returns equal results
	// when evaluated on equal inputs. This is a non-trivial property for
	// composite types which can be equal but not identical
	// (e.g. decimals 1.0 and 1.00). For example, converting a decimal to string
	// is not CompositeInsensitive.
	//
	// See memo.CanBeCompositeSensitive.
	CompositeInsensitive bool
}

// ShouldDocument returns whether the built-in function should be included in
// external-facing documentation.
func (fp *FunctionProperties) ShouldDocument() bool {
	return !(fp.Undocumented || fp.Private)
}

// FunctionClass specifies the class of the builtin function.
type FunctionClass int

const (
	// NormalClass is a standard builtin function.
	NormalClass FunctionClass = iota
	// AggregateClass is a builtin aggregate function.
	AggregateClass
	// WindowClass is a builtin window function.
	WindowClass
	// GeneratorClass is a builtin generator function.
	GeneratorClass
	// SQLClass is a builtin function that executes a SQL statement as a side
	// effect of the function call.
	//
	// For example, AddGeometryColumn is a SQLClass function that executes an
	// ALTER TABLE ... ADD COLUMN statement to add a geometry column to an
	// existing table. It returns metadata about the column added.
	//
	// All builtin functions of this class should include a definition for
	// Overload.SQLFn, which returns the SQL statement to be executed. They
	// should also include a definition for Overload.Fn, which is executed
	// like a NormalClass function and returns a Datum.
	SQLClass
)

// Avoid vet warning about unused enum value.
var _ = NormalClass

// NewFunctionDefinition allocates a function definition corresponding
// to the given built-in definition.
func NewFunctionDefinition(
	name string, props *FunctionProperties, def []Overload,
) *FunctionDefinition {
	overloads := make([]*Overload, len(def))

	for i := range def {
		if def[i].PreferredOverload {
			// Builtins with a preferred overload are always ambiguous.
			props.AmbiguousReturnType = true
		}

		def[i].FunctionProperties = *props
		overloads[i] = &def[i]
	}
	return &FunctionDefinition{
		Name:               name,
		Definition:         overloads,
		FunctionProperties: *props,
	}
}

// FunDefs holds pre-allocated FunctionDefinition instances
// for every builtin function. Initialized by builtins.init().
//
// Note that this is extremely similar to the set stored in builtinsregistry.
// The hope is to remove this map at some point in the future as we delegate
// function definition resolution to interfaces defined in the SemaContext.
var FunDefs map[string]*FunctionDefinition

// ResolvedBuiltinFuncDefs holds pre-allocated ResolvedFunctionDefinition
// instances. Keys of the map is schema qualified function names.
var ResolvedBuiltinFuncDefs map[string]*ResolvedFunctionDefinition

// OidToBuiltinName contains a map from the hashed OID of all builtin functions
// to their name. We populate this from the pg_catalog.go file in the sql
// package because of dependency issues: we can't use oidHasher from this file.
var OidToBuiltinName map[oid.Oid]string

// Format implements the NodeFormatter interface.
func (fd *FunctionDefinition) Format(ctx *FmtCtx) {
	ctx.WriteString(fd.Name)
}

// String implements the Stringer interface.
func (fd *FunctionDefinition) String() string { return AsString(fd) }

// Format implements the NodeFormatter interface.
func (fd *ResolvedFunctionDefinition) Format(ctx *FmtCtx) {
	ctx.WriteString(fd.Name)
}

// String implements the Stringer interface.
func (fd *ResolvedFunctionDefinition) String() string { return AsString(fd) }

// MergeWith is used to merge two UDF definitions with same name.
func (fd *ResolvedFunctionDefinition) MergeWith(
	another *ResolvedFunctionDefinition,
) (*ResolvedFunctionDefinition, error) {
	if fd == nil {
		return another, nil
	}
	if another == nil {
		return fd, nil
	}

	if fd.Name != another.Name {
		return nil, errors.Newf("cannot merge function definition of %q with %q", fd.Name, another.Name)
	}
	combineStrings := func(a, b []string) []string {
		return append(append(make([]string, 0, len(a)+len(b)), a...), b...)
	}
	combineOverloads := func(a, b []*Overload) []*Overload {
		return append(append(make([]*Overload, 0, len(a)+len(b)), a...), b...)
	}
	return &ResolvedFunctionDefinition{
		Name:      fd.Name,
		overloads: combineOverloads(fd.overloads, another.overloads),
		schemas:   combineStrings(fd.schemas, another.schemas),
	}, nil
}

// GetClass returns function class by checking each overload's Class and returns
// the homogeneous Class value if all overloads are the same Class. Ambiguous
// error is returned if there is any overload with different Class.
//
// TODO(chengxiong,mgartner): make sure that, at places of the use cases of this
// method, function is resolved to one overload, so that we can get rid of this
// function and similar methods below.
func (fd *ResolvedFunctionDefinition) GetClass() (FunctionClass, error) {
	ret := fd.overloads[0].Class
	for _, overload := range fd.overloads {
		if overload.Class != ret {
			return 0, pgerror.Newf(pgcode.AmbiguousFunction, "ambiguous function class on %s", fd.Name)
		}
	}
	return ret, nil
}

// GetReturnLabel returns function ReturnLabel by checking each overload and
// returns a ReturnLabel if all overloads have a ReturnLabel of the same length.
// Ambiguous error is returned if there is any overload has ReturnLabel of a
// different length. This is good enough since we don't create UDF with
// ReturnLabel.
func (fd *ResolvedFunctionDefinition) GetReturnLabel() ([]string, error) {
	ret := fd.overloads[0].ReturnLabels
	for _, overload := range fd.overloads {
		if len(ret) != len(overload.ReturnLabels) {
			return nil, pgerror.Newf(pgcode.AmbiguousFunction, "ambiguous function return label on %s", fd.Name)
		}
	}
	return ret, nil
}

// GetHasSequenceArguments returns function's HasSequenceArguments flag by
// checking each overload's HasSequenceArguments flag. Ambiguous error is
// returned if there is any overload has a different flag.
func (fd *ResolvedFunctionDefinition) GetHasSequenceArguments() (bool, error) {
	ret := fd.overloads[0].HasSequenceArguments
	for _, overload := range fd.overloads {
		if ret != overload.HasSequenceArguments {
			return false, pgerror.Newf(pgcode.AmbiguousFunction, "ambiguous function sequence argument on %s", fd.Name)
		}
	}
	return ret, nil
}

// OverloadIterator is used to iterate the overloads of a
// ResolvedFunctionDefinition. It follows the pattern of iterutil in
// terms of error handling.
type OverloadIterator = func(schema string, overload *Overload) error

// ForEachOverload iterates the set of overloads.
func (fd *ResolvedFunctionDefinition) ForEachOverload(it OverloadIterator) error {
	for i, overload := range fd.overloads {
		if err := iterutil.Map(it(fd.schemas[i], overload)); err != nil {
			return err
		}
	}
	return nil
}

// NumOverloads returns the number of overloads.
func (fd *ResolvedFunctionDefinition) NumOverloads() int {
	return len(fd.overloads)
}

// GetOverload returns the ith overload. Note that i must be less than
// NumOverloads.
func (fd *ResolvedFunctionDefinition) GetOverload(i int) (schemaName string, overload *Overload) {
	return fd.schemas[i], fd.overloads[i]
}

func (fd *ResolvedFunctionDefinition) overloadImpls() []overloadImpl {
	ret := make([]overloadImpl, len(fd.overloads))
	for i, ol := range fd.overloads {
		ret[i] = ol
	}
	return ret
}

// GetSchemasForOverloads finds the set of schemas for the passed overloads.
// Note that if any of the overloads provided as arguments are not from this
// ResolvedFunctionDefinition, an error will be returned. The result maps
// schemas to indices into the input.
func (fd *ResolvedFunctionDefinition) GetSchemasForOverloads(
	overloads []*Overload,
) (map[string]util.FastIntSet, error) {
	overloadsToIndexes, err := fd.findOverloads(overloads)
	if err != nil {
		return nil, err
	}
	ret := make(map[string]util.FastIntSet)
	for !overloadsToIndexes.Empty() {
		var curSchema string
		var curMatches util.FastIntSet
		overloadsToIndexes.ForEach(func(inputOrd, fdOrd int) {
			switch schema := fd.schemas[fdOrd]; curSchema {
			case "":
				curSchema = schema
				fallthrough
			case schema:
				curMatches.Add(inputOrd)
			}
		})
		ret[curSchema] = curMatches
		curMatches.ForEach(overloadsToIndexes.Unset)
	}
	return ret, nil
}

// findOverloads maps the passed overloads to their indexes in the
// ResolvedFunctionDefinition.
func (fd *ResolvedFunctionDefinition) findOverloads(
	overloads []*Overload,
) (overloadsToIndexes util.FastIntMap, _ error) {
	// TODO(ajwerner): Consider allocating a map to do the search if the number
	// of candidates is very large. We only expect this if the number of schemas
	// with a function of the correct name and matching type signatures is very
	// large. For now, just do the quadratic search.
outer:
	for i, ol := range overloads {
		for j, other := range fd.overloads {
			if ol == other {
				overloadsToIndexes.Set(i, j)
				continue outer
			}
		}
		return util.FastIntMap{}, errors.AssertionFailedf(
			"failed to find overload in ResolvedFunctionDefinition %s", fd.Name,
		)
	}
	return overloadsToIndexes, nil
}

// QualifyBuiltinFunctionDefinition qualified all overloads in a function
// definition with a schema name. Note that this function can only be used for
// builtin functions.
func QualifyBuiltinFunctionDefinition(
	def *FunctionDefinition, schema string,
) *ResolvedFunctionDefinition {
	return NewResolvedFunctionDefinition(def.Name, schema, def.Definition)
}

// GetBuiltinFuncDefinitionOrFail is similar to GetBuiltinFuncDefinition but
// returns an error if function is not found.
func GetBuiltinFuncDefinitionOrFail(
	fName *FunctionName, searchPath SearchPath,
) (*ResolvedFunctionDefinition, error) {
	def, err := GetBuiltinFuncDefinition(fName, searchPath)
	if err != nil {
		return nil, err
	}
	if def == nil {
		return nil, pgerror.Newf(pgcode.UndefinedFunction, "unknown function: %s()", ErrString(fName))
	}
	return def, nil
}

// GetBuiltinFuncDefinition search for a builtin function given a function name
// and a search path. If function name is prefixed, only the builtin functions
// in the specific schema are searched. Otherwise, all schemas on the given
// searchPath are searched. A nil is returned if no function is found. It's
// caller's choice to error out if function not found.
//
// In theory, this function returns an error only when the search path iterator
// errors which won't happen since the iterating function never errors out. But
// error is still checked and return from the function signature just in case
// we change the iterating function in the future.
func GetBuiltinFuncDefinition(
	fName *FunctionName, searchPath SearchPath,
) (*ResolvedFunctionDefinition, error) {
	if fName.ExplicitSchema {
		return ResolvedBuiltinFuncDefs[fName.Schema()+"."+fName.Object()], nil
	}

	// First try that if we can get function directly with the function name.
	// There is a case where the part[0] of the name is a qualified string.
	// TODO(Chengxiong): figure out why that could be an input.
	if def, ok := ResolvedBuiltinFuncDefs[fName.Object()]; ok {
		return def, nil
	}

	// Then try if it's in pg_catalog.
	if def, ok := ResolvedBuiltinFuncDefs[catconstants.PgCatalogName+"."+fName.Object()]; ok {
		return def, nil
	}

	// If not in pg_catalog, go through search path.
	var resolvedDef *ResolvedFunctionDefinition
	if err := searchPath.IterateSearchPath(func(schema string) error {
		fullName := schema + "." + fName.Object()
		if def, ok := ResolvedBuiltinFuncDefs[fullName]; ok {
			resolvedDef = def
			return iterutil.StopIteration()
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return resolvedDef, nil
}
