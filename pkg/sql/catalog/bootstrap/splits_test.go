// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bootstrap

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestStaticSplits(t *testing.T) {
	defer leaktest.AfterTest(t)()

	splits := StaticSplits()
	for i := 1; i < len(splits); i++ {
		if !splits[i-1].Less(splits[i]) {
			t.Errorf("previous split %q should be less than next split %q", splits[i-1], splits[i])
		}
	}
}
