package ptprovider

import (
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptstorage"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/pttracker"
)

// Config configures the Provider.
type Config struct {
	Settings         *cluster.Settings
	DB               *client.DB
	InternalExecutor sqlutil.InternalExecutor
}

type provider struct {
	*ptstorage.Storage
	*pttracker.Tracker
}

// New creates a new protectedts.Provider.
func New(c Config) protectedts.Provider {
	s := ptstorage.New(c.Settings, c.InternalExecutor)
	t := pttracker.New(c.Settings, c.DB, s)
	return &provider{
		Storage: s,
		Tracker: t,
	}
}
