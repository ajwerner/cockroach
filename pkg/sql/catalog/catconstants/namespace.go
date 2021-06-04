// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catconstants

const (
	// NamespaceTableFamilyID is the column family of the namespace table which is
	// actually written to.
	NamespaceTableFamilyID = 4

	// NamespaceTablePrimaryIndexID is the id of the primary index of the
	// namespace table.
	NamespaceTablePrimaryIndexID = 1

	// DeprecatedNamespaceTableFamilyID is the column family of the namespace table which is
	// actually written to.
	DeprecatedNamespaceTableFamilyID = 3

	// DeprecatedNamespaceTablePrimaryIndexID is the id of the primary index of the
	// namespace table.
	DeprecatedNamespaceTablePrimaryIndexID = 1
)
