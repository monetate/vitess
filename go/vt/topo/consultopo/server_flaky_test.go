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

package consultopo

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"

	"vitess.io/vitess/go/testfiles"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/test"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// startConsul starts a consul subprocess, and waits for it to be ready.
// Returns the exec.Cmd forked, the config file to remove after the test,
// and the server address to RPC-connect to.
func startConsul(t *testing.T, authToken string) (*exec.Cmd, string, string) {
	// Create a temporary config file, as ports cannot all be set
	// via command line. The file name has to end with '.json' so
	// we're not using TempFile.
	configDir := t.TempDir()

	configFilename := path.Join(configDir, "consul.json")
	configFile, err := os.OpenFile(configFilename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("cannot create tempfile: %v", err)
	}

	// Create the JSON config, save it.
	port := testfiles.GoVtTopoConsultopoPort
	config := map[string]any{
		"ports": map[string]int{
			"dns":      port,
			"http":     port + 1,
			"serf_lan": port + 2,
			"serf_wan": port + 3,
		},
	}

	// TODO(deepthi): this is the legacy ACL format. We run v1.4.0 by default in which this has been deprecated.
	// We should start using the new format
	// https://learn.hashicorp.com/tutorials/consul/access-control-replication-multiple-datacenters?in=consul/security-operations
	if authToken != "" {
		config["datacenter"] = "vitess"
		config["acl_datacenter"] = "vitess"
		config["acl_master_token"] = authToken
		config["acl_default_policy"] = "deny"
		config["acl_down_policy"] = "extend-cache"
	}

	data, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("cannot json-encode config: %v", err)
	}
	if _, err := configFile.Write(data); err != nil {
		t.Fatalf("cannot write config: %v", err)
	}
	if err := configFile.Close(); err != nil {
		t.Fatalf("cannot close config: %v", err)
	}

	cmd := exec.Command("consul",
		"agent",
		"-dev",
		"-config-file", configFilename)
	err = cmd.Start()
	if err != nil {
		t.Fatalf("failed to start consul: %v", err)
	}

	// Create a client to connect to the created consul.
	serverAddr := fmt.Sprintf("localhost:%v", port+1)
	cfg := api.DefaultConfig()
	cfg.Address = serverAddr
	if authToken != "" {
		cfg.Token = authToken
	}
	c, err := api.NewClient(cfg)
	if err != nil {
		t.Fatalf("api.NewClient(%v) failed: %v", serverAddr, err)
	}

	// Wait until we can list "/", or timeout.
	start := time.Now()
	kv := c.KV()
	for {
		_, _, err := kv.List("/", nil)
		if err == nil {
			break
		}
		if time.Since(start) > 10*time.Second {
			t.Fatalf("Failed to start consul daemon in time. Consul is returning error: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	return cmd, configFilename, serverAddr
}

func TestConsulTopo(t *testing.T) {
	// One test is going to wait that full period, so make it shorter.
	watchPollDuration = 100 * time.Millisecond

	// Start a single consul in the background.
	cmd, configFilename, serverAddr := startConsul(t, "")
	defer func() {
		// Alerts command did not run successful
		if err := cmd.Process.Kill(); err != nil {
			log.Errorf("cmd process kill has an error: %v", err)
		}
		// Alerts command did not run successful
		if err := cmd.Wait(); err != nil {
			log.Errorf("cmd wait has an error: %v", err)
		}

		os.Remove(configFilename)
	}()

	// Run the TopoServerTestSuite tests.
	testIndex := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	test.TopoServerTestSuite(t, ctx, func() *topo.Server {
		// Each test will use its own sub-directories.
		testRoot := fmt.Sprintf("test-%v", testIndex)
		testIndex++

		// Create the server on the new root.
		ts, err := topo.OpenServer("consul", serverAddr, path.Join(testRoot, topo.GlobalCell))
		if err != nil {
			t.Fatalf("OpenServer() failed: %v", err)
		}

		// Create the CellInfo.
		if err := ts.CreateCellInfo(context.Background(), test.LocalCellName, &topodatapb.CellInfo{
			ServerAddress: serverAddr,
			Root:          path.Join(testRoot, test.LocalCellName),
		}); err != nil {
			t.Fatalf("CreateCellInfo() failed: %v", err)
		}

		return ts
	}, []string{})
}

func TestConsulTopoWithChecks(t *testing.T) {
	// One test is going to wait that full period, so make it shorter.
	watchPollDuration = 100 * time.Millisecond
	consulLockSessionChecks = "serfHealth"
	consulLockSessionTTL = "15s"

	// Start a single consul in the background.
	cmd, configFilename, serverAddr := startConsul(t, "")
	defer func() {
		// Alerts command did not run successful
		if err := cmd.Process.Kill(); err != nil {
			log.Errorf("cmd process kill has an error: %v", err)
		}
		// Alerts command did not run successful
		if err := cmd.Wait(); err != nil {
			log.Errorf("cmd wait has an error: %v", err)
		}

		os.Remove(configFilename)
	}()

	// Run the TopoServerTestSuite tests.
	testIndex := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	test.TopoServerTestSuite(t, ctx, func() *topo.Server {
		// Each test will use its own sub-directories.
		testRoot := fmt.Sprintf("test-%v", testIndex)
		testIndex++

		// Create the server on the new root.
		ts, err := topo.OpenServer("consul", serverAddr, path.Join(testRoot, topo.GlobalCell))
		if err != nil {
			t.Fatalf("OpenServer() failed: %v", err)
		}

		// Create the CellInfo.
		if err := ts.CreateCellInfo(context.Background(), test.LocalCellName, &topodatapb.CellInfo{
			ServerAddress: serverAddr,
			Root:          path.Join(testRoot, test.LocalCellName),
		}); err != nil {
			t.Fatalf("CreateCellInfo() failed: %v", err)
		}

		return ts
	}, []string{})
}

func TestConsulTopoWithAuth(t *testing.T) {
	// One test is going to wait that full period, so make it shorter.
	watchPollDuration = 100 * time.Millisecond

	// Start a single consul in the background.
	cmd, configFilename, serverAddr := startConsul(t, "123456")
	defer func() {
		// Alerts command did not run successful
		if err := cmd.Process.Kill(); err != nil {
			log.Errorf("cmd process kill has an error: %v", err)
		}
		// Alerts command did not run successful
		if err := cmd.Wait(); err != nil {
			log.Errorf("cmd process wait has an error: %v", err)
		}
		os.Remove(configFilename)
	}()

	// Run the TopoServerTestSuite tests.
	testIndex := 0
	tmpFile, err := os.CreateTemp("", "consul_auth_client_static_file.json")

	if err != nil {
		t.Fatalf("couldn't create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	consulAuthClientStaticFile = tmpFile.Name()

	jsonConfig := "{\"global\":{\"acl_token\":\"123456\"}, \"test\":{\"acl_token\":\"123456\"}}"
	if err := os.WriteFile(tmpFile.Name(), []byte(jsonConfig), 0600); err != nil {
		t.Fatalf("couldn't write temp file: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	test.TopoServerTestSuite(t, ctx, func() *topo.Server {
		// Each test will use its own sub-directories.
		testRoot := fmt.Sprintf("test-%v", testIndex)
		testIndex++

		// Create the server on the new root.
		ts, err := topo.OpenServer("consul", serverAddr, path.Join(testRoot, topo.GlobalCell))
		if err != nil {
			t.Fatalf("OpenServer() failed: %v", err)
		}

		// Create the CellInfo.
		if err := ts.CreateCellInfo(context.Background(), test.LocalCellName, &topodatapb.CellInfo{
			ServerAddress: serverAddr,
			Root:          path.Join(testRoot, test.LocalCellName),
		}); err != nil {
			t.Fatalf("CreateCellInfo() failed: %v", err)
		}

		return ts
	}, []string{})
}

func TestConsulTopoWithAuthFailure(t *testing.T) {
	// One test is going to wait that full period, so make it shorter.
	watchPollDuration = 100 * time.Millisecond

	// Start a single consul in the background.
	cmd, configFilename, serverAddr := startConsul(t, "123456")
	defer func() {
		cmd.Process.Kill()
		cmd.Wait()
		os.Remove(configFilename)
	}()

	tmpFile, err := os.CreateTemp("", "consul_auth_client_static_file.json")

	if err != nil {
		t.Fatalf("couldn't create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	consulAuthClientStaticFile = tmpFile.Name()

	// check valid, empty json causes error
	{
		jsonConfig := "{}"
		if err := os.WriteFile(tmpFile.Name(), []byte(jsonConfig), 0600); err != nil {
			t.Fatalf("couldn't write temp file: %v", err)
		}

		// Create the server on the new root.
		_, err := topo.OpenServer("consul", serverAddr, path.Join("globalRoot", topo.GlobalCell))
		if err == nil {
			t.Fatal("Expected OpenServer() to return an error due to bad config, got nil")
		}
	}

	// check bad token causes error
	{
		jsonConfig := "{\"global\":{\"acl_token\":\"badtoken\"}}"
		if err := os.WriteFile(tmpFile.Name(), []byte(jsonConfig), 0600); err != nil {
			t.Fatalf("couldn't write temp file: %v", err)
		}

		// Create the server on the new root.
		ts, err := topo.OpenServer("consul", serverAddr, path.Join("globalRoot", topo.GlobalCell))
		if err != nil {
			t.Fatalf("OpenServer() failed: %v", err)
		}

		// Attempt to Create the CellInfo.
		err = ts.CreateCellInfo(context.Background(), test.LocalCellName, &topodatapb.CellInfo{
			ServerAddress: serverAddr,
			Root:          path.Join("globalRoot", test.LocalCellName),
		})

		want := "Failed request: ACL not found"
		if err == nil || err.Error() != want {
			t.Errorf("Expected CreateCellInfo to fail: got  %v, want %s", err, want)
		}
	}
}
