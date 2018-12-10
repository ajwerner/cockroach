package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm"
	"golang.org/x/sync/errgroup"
)

func main() {
	cluster := flag.String("cluster", "", "cluster name of interest")
	remoteLogsPath := flag.String("remote-logs", "logs", "path on remote hosts of logs")
	localLogsPath := flag.String("local-logs", "", "defaults to <cluster>.logs")
	remoteUser := flag.String("remote-user", "ubuntu", "user relative to whom the logs directory exists")
	flag.Parse()

	// we want to grab a timestamp, rsync from the nodes, invoke merge logs to the timestamp and continue
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := getCluster(*cluster)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get cluster: %v\n", err)
		os.Exit(1)
	}
	if *localLogsPath == "" {
		*localLogsPath = "./" + c.Name + ".logs"
	}
	if err := os.MkdirAll(*localLogsPath, 0755); err != nil {
		panic(err)
	}
	const timeFormat = "060102 15:04:05.999999"
	prev := time.Now().UTC().Add(-2 * time.Second)
	for {
		t := time.Now().UTC().Add(-1 * time.Second).Truncate(time.Microsecond)
		if err := rsyncLogs(ctx, *remoteLogsPath, *localLogsPath, *remoteUser, c); err != nil {
			panic(err)
		}
		cmd := exec.CommandContext(ctx, "cockroach", "debug", "merge-logs",
			*localLogsPath+"/*/*",
			"--from", prev.Format(timeFormat),
			"--to", t.Format(timeFormat))
		cmd.Stdout = os.Stdout
		if err := cmd.Run(); err != nil {
			panic(err)
		}
		prev = t
		select {
		case <-time.After(200 * time.Millisecond):
		case <-ctx.Done():
			return
		}
	}
}

func rsyncLogs(ctx context.Context, remote, local, remoteUser string, c *cloud.Cluster) error {
	g, ctx := errgroup.WithContext(ctx)
	for i := range c.VMs {
		m := c.VMs[i]
		g.Go(func() error { return rsyncVMLogs(ctx, remote, local, remoteUser, m) })
	}
	return g.Wait()
}

func rsyncVMLogs(ctx context.Context, remote, local, remoteUser string, m vm.VM) error {
	outpath := filepath.Join(local, m.Name)
	rsyncPath := "rsync"
	if remoteUser != m.RemoteUser {
		rsyncPath = "sudo -u " + remoteUser + " rsync"
	}
	cmd := exec.CommandContext(ctx, "rsync", "-az",
		"-e",
		"ssh -o StrictHostKeyChecking=no "+
			"-o ControlMaster=auto "+
			"-o ControlPath=~/.ssh/%r@%h:%p "+
			"-o ControlPersist=2m",
		"--rsync-path", rsyncPath,
		"--size-only",
		m.RemoteUser+"@"+m.PublicIP+":"+remote+"/", outpath+"/")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func getCluster(clusterName string) (*cloud.Cluster, error) {
	nodeNames := "all"
	{
		parts := strings.Split(clusterName, ":")
		switch len(parts) {
		case 2:
			nodeNames = parts[1]
			fallthrough
		case 1:
			clusterName = parts[0]
		case 0:
			return nil, fmt.Errorf("no cluster specified")
		default:
			return nil, fmt.Errorf("invalid cluster name: %s", clusterName)
		}
	}
	var outBuf bytes.Buffer
	cmd := exec.Command("roachprod", "list", clusterName, "--json")
	cmd.Stdout = &outBuf
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	var clusters struct {
		Clusters map[string]*cloud.Cluster `json:"clusters"`
	}
	if err := json.NewDecoder(&outBuf).Decode(&clusters); err != nil {
		return nil, err
	}
	c, ok := clusters.Clusters[clusterName]
	if !ok {
		return nil, fmt.Errorf("failed to find cluster of name %v", clusterName)
	}
	nodeIndices, err := install.ListNodes(nodeNames, len(c.VMs))
	if err != nil {
		return nil, err
	}
	nodes := make(vm.List, 0, len(nodeIndices))
	for _, idx := range nodeIndices {
		nodes = append(nodes, c.VMs[idx-1])
	}
	c.VMs = nodes
	return c, nil
}
