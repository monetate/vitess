/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysqlctl

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	primaryTablet   cluster.Vttablet
	replicaTablet   cluster.Vttablet
	hostname        = "localhost"
	keyspaceName    = "test_keyspace"
	shardName       = "0"
	cell            = "zone1"
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		if err := clusterInstance.VtctldClientProcess.CreateKeyspace(keyspaceName, sidecar.DefaultName, ""); err != nil {
			return 1
		}

		initCluster([]string{"0"}, 2)

		// Collect tablet paths and ports
		tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
		for _, tablet := range tablets {
			if tablet.Type == "primary" {
				primaryTablet = *tablet
			} else if tablet.Type != "rdonly" {
				replicaTablet = *tablet
			}
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func initCluster(shardNames []string, totalTabletsRequired int) {
	keyspace := cluster.Keyspace{
		Name: keyspaceName,
	}
	for _, shardName := range shardNames {
		shard := &cluster.Shard{
			Name: shardName,
		}
		var mysqlCtlProcessList []*exec.Cmd
		for i := 0; i < totalTabletsRequired; i++ {
			// instantiate vttablet object with reserved ports
			tabletUID := clusterInstance.GetAndReserveTabletUID()
			tablet := &cluster.Vttablet{
				TabletUID: tabletUID,
				HTTPPort:  clusterInstance.GetAndReservePort(),
				GrpcPort:  clusterInstance.GetAndReservePort(),
				MySQLPort: clusterInstance.GetAndReservePort(),
				Alias:     fmt.Sprintf("%s-%010d", clusterInstance.Cell, tabletUID),
			}
			if i == 0 { // Make the first one as primary
				tablet.Type = "primary"
			}
			// Start Mysqlctl process
			mysqlctlProcess, err := cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
			if err != nil {
				return
			}
			tablet.MysqlctlProcess = *mysqlctlProcess
			proc, err := tablet.MysqlctlProcess.StartProcess()
			if err != nil {
				return
			}
			mysqlCtlProcessList = append(mysqlCtlProcessList, proc)

			// start vttablet process
			tablet.VttabletProcess = cluster.VttabletProcessInstance(
				tablet.HTTPPort,
				tablet.GrpcPort,
				tablet.TabletUID,
				clusterInstance.Cell,
				shardName,
				keyspaceName,
				clusterInstance.VtctldProcess.Port,
				tablet.Type,
				clusterInstance.TopoProcess.Port,
				clusterInstance.Hostname,
				clusterInstance.TmpDirectory,
				clusterInstance.VtTabletExtraArgs,
				clusterInstance.DefaultCharset)
			tablet.Alias = tablet.VttabletProcess.TabletPath

			shard.Vttablets = append(shard.Vttablets, tablet)
		}
		for _, proc := range mysqlCtlProcessList {
			if err := proc.Wait(); err != nil {
				return
			}
		}

		keyspace.Shards = append(keyspace.Shards, *shard)
	}
	clusterInstance.Keyspaces = append(clusterInstance.Keyspaces, keyspace)
}

func TestRestart(t *testing.T) {
	err := primaryTablet.MysqlctlProcess.Stop()
	require.NoError(t, err)
	primaryTablet.MysqlctlProcess.CleanupFiles(primaryTablet.TabletUID)
	err = primaryTablet.MysqlctlProcess.Start()
	require.NoError(t, err)
}

func TestAutoDetect(t *testing.T) {

	err := clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].VttabletProcess.Setup()
	require.NoError(t, err)
	err = clusterInstance.Keyspaces[0].Shards[0].Vttablets[1].VttabletProcess.Setup()
	require.NoError(t, err)

	// Reparent tablets, which requires flavor detection
	err = clusterInstance.VtctldClientProcess.InitializeShard(keyspaceName, shardName, cell, primaryTablet.TabletUID)
	require.NoError(t, err)
}
