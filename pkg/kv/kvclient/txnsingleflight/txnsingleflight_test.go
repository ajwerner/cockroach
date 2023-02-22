package txnsingleflight_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/txnsingleflight"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGroup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	g := txnsingleflight.NewGroup(txnsingleflight.Config{
		OpName:  "test",
		TagName: "sf",
		DB:      kvDB,
		Time:    timeutil.DefaultTimeSource{},
		Stopper: s.Stopper(),
	})

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY)")
	var tableID descpb.ID
	tdb.QueryRow(t, "SELECT 'foo'::regclass::int").Scan(&tableID)
	prefix := s.Codec().IndexPrefix(uint32(tableID), 1)
	prefix = prefix[:len(prefix):len(prefix)]
	// We're going to have a transaction create a lock, and then
	// have another transaction as part of the singleflight block
	// on the lock, and we'll see that one of them gets aborted.
	key1 := roachpb.Key(encoding.EncodeVarintAscending(prefix, 1))
	require.NoError(t, kvDB.Put(ctx, key1, 1))
	errCh := make(chan error)
	var read bool
	go func() {
		errCh <- kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			if err := txn.Put(ctx, key1, 2); err != nil {
				return err
			}
			if !read {
				f, leader := g.DoChan(ctx, key1, txn, func(ctx context.Context, d *kv.Txn) (any, error) {
					got, err := d.Get(ctx, key1)
					if err != nil {
						log.Infof(ctx, "here: %v", err)
						return nil, err
					}
					assert.Equal(t, int64(1), got.ValueInt())
					return nil, nil
				})
				assert.True(t, leader)
				defer f.Reset()
				if res := f.WaitForResult(ctx); res.Err != nil {
					log.Infof(ctx, "got an error: %v", res.Err)
					return res.Err
				}
				read = true
			}
			return nil
		})
	}()
	require.NoError(t, <-errCh)
	require.True(t, read)
}
