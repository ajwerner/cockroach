package admission

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPriorityBuckets(t *testing.T) {
	for _, tc := range []struct {
		p       Priority
		encoded uint32
		str     string
	}{
		{
			Priority{MaxLevel, 127},
			0x0000FFFF,
			"max:127",
		},
		{
			Priority{MaxLevel, 126},
			0x0000FFFD,
			"max:126",
		},
		{
			Priority{MaxLevel, 1},
			0x0000FF03,
			"max:1",
		},
		{
			Priority{MaxLevel, 0},
			0x0000FF01,
			"max:0",
		},
		{
			Priority{DefaultLevel, 127},
			0x000080FF,
			"def:127",
		},
		{
			Priority{MinLevel, 125},
			0x000001FB,
			"min:125",
		},
	} {
		t.Run(fmt.Sprintf("%s->%x", tc.p, tc.encoded), func(t *testing.T) {
			assert.Equal(t, tc.p, Decode(tc.encoded))
			assert.Equal(t, tc.encoded, tc.p.Encode())
			assert.Equal(t, tc.str, tc.p.String())
		})
	}
}
