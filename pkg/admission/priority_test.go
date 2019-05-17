package admission

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPriorityBuckets(t *testing.T) {
	for _, tc := range []struct {
		level  uint8
		bucket int
	}{
		{MaxLevel, numLevels - 1},
		{DefaultLevel, numLevels / 2},
		{MinLevel, 0},
	} {
		t.Run(fmt.Sprintf("%d->%d", tc.level, tc.bucket), func(t *testing.T) {
			assert.Equal(t, tc.bucket, bucketFromLevel(tc.level))
			assert.Equal(t, tc.level, levelFromBucket(tc.bucket))
		})
	}
}
