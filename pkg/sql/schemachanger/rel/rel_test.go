package rel_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel/internal/entitynodetest"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel/internal/reltest"
)

func TestRel(t *testing.T) {
	for _, s := range []reltest.Suite{
		entitynodetest.Suite,
	} {
		t.Run(s.Name, func(t *testing.T) {
			s.Run(t)
		})
	}
}
