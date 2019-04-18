package test

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/storage/replication/part2/cli"
)

func TestServer(t *testing.T) {

	go cli.Run([]string{"start", "--addresses", "1@:10103", "--init"})
	time.Sleep(1000 * time.Millisecond)
	cli.Run([]string{"get", "asdf"})
	cli.Run([]string{"put", "asdf", "1"})
	cli.Run([]string{"get", "asdf"})
}
