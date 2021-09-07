// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package screl

import (
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/pkg/errors"
)

// ToString renders an element's attributes to a string.
func ToString(e *scpb.Node) string {
	var buf strings.Builder

	return buf.String()
}

func FormatElement(e scpb.Element, w io.Writer) (err error) {
	if e == nil {
		return errors.Errorf("nil element")
	}
	defer catchPanic(&err)

	panicIfN(io.WriteString(w, reflect.TypeOf(e).Elem().Name()))
	panicIfN(io.WriteString(w, ": {"))

	// TODO(ajwerner): This is totally janky. Instead we ought to just iterate
	// the variables we do have.
	var written int
	if err := Schema.IterateAttributes(e, func(attr rel.Attribute, value interface{}) error {
		if written > 0 {
			panicIfN(io.WriteString(w, ", "))
		}
		written++
		panicIfN(fmt.Fprintf(w, "%s: %v", attr, value))
		return nil
	}); err != nil {
		return err
	}
	panicIfN(io.WriteString(w, "}"))
	return nil
}

// Format serializes attribute into a writer.
func Format(e *scpb.Node, w io.Writer) (err error) {
	defer catchPanic(&err)
	ws := func(s string) {
		if _, err := io.WriteString(w, s); err != nil {
			panic(err)
		}
	}
	ws("[")
	if err := FormatElement(e.GetElement(), w); err != nil {
		panic(err)
	}
	ws(", ")
	ws(e.Status.String())
	ws(", ")
	ws(e.Target.Direction.String())
	ws("]")
	return nil
}

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}

func panicIfN(_ int, err error) { panicIf(err) }

func catchPanic(err *error) {
	if r := recover(); r != nil {
		if rErr, ok := r.(error); ok && *err == nil {
			*err = errors.WithStack(rErr)
			return
		}
		panic(r)
	}
}
