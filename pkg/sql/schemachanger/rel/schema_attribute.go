// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rel

import "github.com/cockroachdb/errors"

// GetAttribute returns the requested attribute from the passed entity. If
// the entity is of a type not defined for this schema, an error will be
// returned. If the entity is nil, an error will be returned. If the entity
// does not have this attribute defined, an error will be returned. If the
// entity does not have a populated value for this attribute, a nil value
// will be returned without an error.
func (sc *Schema) GetAttribute(attribute Attribute, v interface{}) (interface{}, error) {
	ord := sc.getOrd(attribute)
	ti, value, err := getEntityValueInfo(sc, v)
	if err != nil {
		return nil, err
	}

	fi, ok := ti.attrFields[ord]
	if !ok {
		return nil, errors.Errorf("no field defined on %v for %v", ti.typ, attribute)
	}
	for i := range fi {
		got := fi[i].value(value.Pointer())
		if got != nil {
			return got, nil
		}
	}
	return nil, nil
}
