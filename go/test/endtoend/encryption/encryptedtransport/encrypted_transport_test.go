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

/* This test makes sure encrypted transport over gRPC works.

The security chains are setup the following way:

* root CA
* vttablet server CA
* vttablet server instance cert/key
* vttablet client CA
* vttablet client 1 cert/key
* vtgate server CA
* vtgate server instance cert/key (common name is 'localhost')
* vtgate client CA
* vtgate client 1 cert/key
* vtgate client 2 cert/key

The following table shows all the checks we perform:
process:            will check its peer is signed by:  for link:

vttablet             vttablet client CA                vtgate -> vttablet
vtgate               vttablet server CA                vtgate -> vttablet

vtgate               vtgate client CA                  client -> vtgate
client               vtgate server CA                  client -> vtgate

Additionally, we have the following constraints:
- the client certificate common name is used as immediate
caller ID by vtgate, and forwarded to vttablet. This allows us to use
table ACLs on the vttablet side.
- the vtgate server certificate common name is set to 'localhost' so it matches
the hostname dialed by the vtgate clients. This is not a requirement for the
go client, that can set its expected server name. However, the python gRPC
client doesn't have the ability to set the server name, so they must match.
- the python client needs to have the full chain for the server validation
(that is 'vtgate server CA' + 'root CA'). A go client doesn't. So we read both
below when using the python client, but we only pass the intermediate cert
to the go clients (for vtgate -> vttablet link). */

package encryptedtransport

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"testing"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/test/endtoend/encryption"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vterrors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtgateservicepb "vitess.io/vitess/go/vt/proto/vtgateservice"
)

var (
	clusterInstance    *cluster.LocalProcessCluster
	createVtInsertTest = `create table vt_insert_test (
								id bigint auto_increment,
								msg varchar(64),
								keyspace_id bigint(20) unsigned NOT NULL,
								primary key (id)
								) Engine = InnoDB`
	keyspace      = "test_keyspace"
	hostname      = "localhost"
	shardName     = "0"
	cell          = "zone1"
	certDirectory string
	grpcCert      = ""
	grpcKey       = ""
	grpcCa        = ""
	grpcName      = ""
)

func TestSecureTransport(t *testing.T) {
	flag.Parse()

	// initialize cluster
	_, err := clusterSetUp(t)
	require.Nil(t, err, "setup failed")

	primaryTablet := *clusterInstance.Keyspaces[0].Shards[0].Vttablets[0]
	replicaTablet := *clusterInstance.Keyspaces[0].Shards[0].Vttablets[1]

	// creating table_acl_config.json file
	tableACLConfigJSON := path.Join(certDirectory, "table_acl_config.json")
	f, err := os.Create(tableACLConfigJSON)
	require.NoError(t, err)

	_, err = f.WriteString(`{
	"table_groups": [
	{
		"table_names_or_prefixes": ["vt_insert_test"],
		"readers": ["vtgate client 1"],
		"writers": ["vtgate client 1"],
		"admins": ["vtgate client 1"]
	}
  ]
}`)
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)

	// start the tablets
	for _, tablet := range []cluster.Vttablet{primaryTablet, replicaTablet} {
		tablet.VttabletProcess.ExtraArgs = append(tablet.VttabletProcess.ExtraArgs, "--table-acl-config", tableACLConfigJSON, "--queryserver-config-strict-table-acl")
		tablet.VttabletProcess.ExtraArgs = append(tablet.VttabletProcess.ExtraArgs, serverExtraArguments("vttablet-server-instance", "vttablet-client")...)
		err = tablet.VttabletProcess.Setup()
		require.NoError(t, err)
	}

	// Shared flags.
	vtctldClientArgs := []string{"--server", "internal"}
	vtctldClientArgs = append(vtctldClientArgs, tmclientExtraArgs("vttablet-client-1")...)

	// Reparenting.
	vtctlInitArgs := append(vtctldClientArgs, "InitShardPrimary", "--force", "test_keyspace/0", primaryTablet.Alias)
	err = clusterInstance.VtctldClientProcess.ExecuteCommand(vtctlInitArgs...)
	require.NoError(t, err)

	err = clusterInstance.StartVTOrc("test_keyspace")
	require.NoError(t, err)

	// Apply schema.
	var vtctlApplySchemaArgs = append(vtctldClientArgs, "ApplySchema", "--sql", createVtInsertTest, "test_keyspace")
	err = clusterInstance.VtctldClientProcess.ExecuteCommand(vtctlApplySchemaArgs...)
	require.NoError(t, err)

	for _, tablet := range []cluster.Vttablet{primaryTablet, replicaTablet} {
		vtctlTabletArgs := append(vtctldClientArgs, "RunHealthCheck", tablet.Alias)
		_, err = clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput(vtctlTabletArgs...)
		require.NoError(t, err)
	}

	// Start vtgate.
	clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, tabletConnExtraArgs("vttablet-client-1")...)
	clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, serverExtraArguments("vtgate-server-instance", "vtgate-client")...)
	err = clusterInstance.StartVtgate()
	require.NoError(t, err)

	grpcAddress := fmt.Sprintf("%s:%d", "localhost", clusterInstance.VtgateProcess.GrpcPort)

	// 'vtgate client 1' is authorized to access vt_insert_test
	setCreds(t, "vtgate-client-1", "vtgate-server")
	ctx := context.Background()
	request := getRequest("select * from vt_insert_test")
	vc, err := getVitessClient(ctx, grpcAddress)
	require.NoError(t, err)

	qr, err := vc.Execute(ctx, request)
	require.NoError(t, err)
	err = vterrors.FromVTRPC(qr.Error)
	require.NoError(t, err)

	// 'vtgate client 2' is not authorized to access vt_insert_test
	setCreds(t, "vtgate-client-2", "vtgate-server")
	request = getRequest("select * from vt_insert_test")
	vc, err = getVitessClient(ctx, grpcAddress)
	require.NoError(t, err)
	qr, err = vc.Execute(ctx, request)
	require.NoError(t, err)
	err = vterrors.FromVTRPC(qr.Error)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Select command denied to user")
	assert.Contains(t, err.Error(), "for table 'vt_insert_test' (ACL check error)")

	useEffectiveCallerID(ctx, t)
	useEffectiveGroups(ctx, t)

	clusterInstance.Teardown()
}

func useEffectiveCallerID(ctx context.Context, t *testing.T) {
	// now restart vtgate in the mode where we don't use SSL
	// for client connections, but we copy effective caller id
	// into immediate caller id.
	clusterInstance.VtGateExtraArgs = []string{utils.GetFlagVariantForTests("--grpc-use-effective-callerid")}
	clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, tabletConnExtraArgs("vttablet-client-1")...)
	err := clusterInstance.RestartVtgate()
	require.NoError(t, err)

	grpcAddress := fmt.Sprintf("%s:%d", "localhost", clusterInstance.VtgateProcess.GrpcPort)

	setSSLInfoEmpty()

	// get vitess client
	vc, err := getVitessClient(ctx, grpcAddress)
	require.NoError(t, err)

	// test with empty effective caller Id
	request := getRequest("select * from vt_insert_test")
	qr, err := vc.Execute(ctx, request)
	require.NoError(t, err)
	err = vterrors.FromVTRPC(qr.Error)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Select command denied to user")
	assert.Contains(t, err.Error(), "for table 'vt_insert_test' (ACL check error)")

	// 'vtgate client 1' is authorized to access vt_insert_test
	callerID := &vtrpc.CallerID{
		Principal: "vtgate client 1",
	}
	request = getRequestWithCallerID(callerID, "select * from vt_insert_test")
	qr, err = vc.Execute(ctx, request)
	require.NoError(t, err)
	err = vterrors.FromVTRPC(qr.Error)
	require.NoError(t, err)

	// 'vtgate client 2' is not authorized to access vt_insert_test
	callerID = &vtrpc.CallerID{
		Principal: "vtgate client 2",
	}
	request = getRequestWithCallerID(callerID, "select * from vt_insert_test")
	qr, err = vc.Execute(ctx, request)
	require.NoError(t, err)
	err = vterrors.FromVTRPC(qr.Error)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Select command denied to user")
	assert.Contains(t, err.Error(), "for table 'vt_insert_test' (ACL check error)")
}

func useEffectiveGroups(ctx context.Context, t *testing.T) {
	// now restart vtgate in the mode where we don't use SSL
	// for client connections, but we copy effective caller's groups
	// into immediate caller id.
	clusterInstance.VtGateExtraArgs = []string{utils.GetFlagVariantForTests("--grpc-use-effective-callerid"), utils.GetFlagVariantForTests("--grpc-use-effective-groups")}
	clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, tabletConnExtraArgs("vttablet-client-1")...)
	err := clusterInstance.RestartVtgate()
	require.NoError(t, err)

	grpcAddress := fmt.Sprintf("%s:%d", "localhost", clusterInstance.VtgateProcess.GrpcPort)

	setSSLInfoEmpty()

	// get vitess client
	vc, err := getVitessClient(ctx, grpcAddress)
	require.NoError(t, err)

	// test with empty effective caller Id
	request := getRequest("select * from vt_insert_test")
	qr, err := vc.Execute(ctx, request)
	require.NoError(t, err)
	err = vterrors.FromVTRPC(qr.Error)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Select command denied to user")
	assert.Contains(t, err.Error(), "for table 'vt_insert_test' (ACL check error)")

	// 'vtgate client 1' is authorized to access vt_insert_test
	callerID := &vtrpc.CallerID{
		Principal: "my-caller",
		Groups:    []string{"vtgate client 1"},
	}
	request = getRequestWithCallerID(callerID, "select * from vt_insert_test")
	qr, err = vc.Execute(ctx, request)
	require.NoError(t, err)
	err = vterrors.FromVTRPC(qr.Error)
	require.NoError(t, err)

	// 'vtgate client 2' is not authorized to access vt_insert_test
	callerID = &vtrpc.CallerID{
		Principal: "my-caller",
		Groups:    []string{"vtgate client 2"},
	}
	request = getRequestWithCallerID(callerID, "select * from vt_insert_test")
	qr, err = vc.Execute(ctx, request)
	require.NoError(t, err)
	err = vterrors.FromVTRPC(qr.Error)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Select command denied to user")
	assert.Contains(t, err.Error(), "for table 'vt_insert_test' (ACL check error)")
}

func clusterSetUp(t *testing.T) (int, error) {
	var mysqlProcesses []*exec.Cmd
	clusterInstance = cluster.NewCluster(cell, hostname)

	// Start topo server
	if err := clusterInstance.StartTopo(); err != nil {
		return 1, fmt.Errorf("unable to start topo %w", err)
	}

	// create all certs
	log.Info("Creating certificates")
	certDirectory = path.Join(clusterInstance.TmpDirectory, "certs")
	_ = encryption.CreateDirectory(certDirectory, 0700)

	err := encryption.ExecuteVttlstestCommand("--root", certDirectory, "CreateCA")
	require.NoError(t, err)

	err = createIntermediateCA("ca", "01", "vttablet-server", "vttablet server CA")
	require.NoError(t, err)

	err = createIntermediateCA("ca", "02", "vttablet-client", "vttablet client CA")
	require.NoError(t, err)

	err = createIntermediateCA("ca", "03", "vtgate-server", "vtgate server CA")
	require.NoError(t, err)

	err = createIntermediateCA("ca", "04", "vtgate-client", "vtgate client CA")
	require.NoError(t, err)

	err = createSignedCert("vttablet-server", "01", "vttablet-server-instance", "vttablet server instance")
	require.NoError(t, err)

	err = createSignedCert("vttablet-client", "01", "vttablet-client-1", "vttablet client 1")
	require.NoError(t, err)

	err = createSignedCert("vtgate-server", "01", "vtgate-server-instance", "localhost")
	require.NoError(t, err)

	err = createSignedCert("vtgate-client", "01", "vtgate-client-1", "vtgate client 1")
	require.NoError(t, err)

	err = createSignedCert("vtgate-client", "02", "vtgate-client-2", "vtgate client 2")
	require.NoError(t, err)

	for _, keyspaceStr := range []string{keyspace} {
		KeyspacePtr := &cluster.Keyspace{Name: keyspaceStr}
		keyspace := *KeyspacePtr
		if err := clusterInstance.VtctldClientProcess.CreateKeyspace(keyspace.Name, sidecar.DefaultName, ""); err != nil {
			return 1, err
		}
		shard := &cluster.Shard{
			Name: shardName,
		}
		for i := 0; i < 2; i++ {
			// instantiate vttablet object with reserved ports
			tablet := clusterInstance.NewVttabletInstance("replica", 0, cell)

			// Start Mysqlctl process
			mysqlctlProcess, err := cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
			if err != nil {
				return 1, err
			}
			tablet.MysqlctlProcess = *mysqlctlProcess
			proc, err := tablet.MysqlctlProcess.StartProcess()
			if err != nil {
				return 1, err
			}
			mysqlProcesses = append(mysqlProcesses, proc)
			// start vttablet process
			tablet.VttabletProcess = cluster.VttabletProcessInstance(
				tablet.HTTPPort,
				tablet.GrpcPort,
				tablet.TabletUID,
				clusterInstance.Cell,
				shardName,
				keyspace.Name,
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
		keyspace.Shards = append(keyspace.Shards, *shard)
		clusterInstance.Keyspaces = append(clusterInstance.Keyspaces, keyspace)
	}
	for _, proc := range mysqlProcesses {
		err := proc.Wait()
		if err != nil {
			return 1, fmt.Errorf("unable to wait on mysql process %w", err)
		}
	}
	return 0, nil
}

func createIntermediateCA(ca string, serial string, name string, commonName string) error {
	log.Infof("Creating intermediate signed cert and key %s", commonName)
	tmpProcess := exec.Command(
		"vttlstest",
		"CreateIntermediateCA",
		"--root", certDirectory,
		"--parent", ca,
		"--serial", serial,
		"--common-name", commonName,
		name)
	return tmpProcess.Run()
}

func createSignedCert(ca string, serial string, name string, commonName string) error {
	log.Infof("Creating signed cert and key %s", commonName)
	tmpProcess := exec.Command(
		"vttlstest",
		"CreateSignedCert",
		"--root", certDirectory,
		"--parent", ca,
		"--serial", serial,
		"--common-name", commonName,
		name)
	return tmpProcess.Run()
}

func serverExtraArguments(name string, ca string) []string {
	args := []string{
		utils.GetFlagVariantForTests("--grpc-cert"), certDirectory + "/" + name + "-cert.pem",
		utils.GetFlagVariantForTests("--grpc-key"), certDirectory + "/" + name + "-key.pem",
		utils.GetFlagVariantForTests("--grpc-ca"), certDirectory + "/" + ca + "-cert.pem",
	}
	return args
}

func tmclientExtraArgs(name string) []string {
	ca := "vttablet-server"
	var args = []string{"--tablet-manager-grpc-cert", certDirectory + "/" + name + "-cert.pem",
		"--tablet-manager-grpc-key", certDirectory + "/" + name + "-key.pem",
		"--tablet-manager-grpc-ca", certDirectory + "/" + ca + "-cert.pem",
		"--tablet-manager-grpc-server-name", "vttablet server instance"}
	return args
}

func tabletConnExtraArgs(name string) []string {
	ca := "vttablet-server"
	args := []string{utils.GetFlagVariantForTests("--tablet-grpc-cert"), certDirectory + "/" + name + "-cert.pem",
		utils.GetFlagVariantForTests("--tablet-grpc-key"), certDirectory + "/" + name + "-key.pem",
		utils.GetFlagVariantForTests("--tablet-grpc-ca"), certDirectory + "/" + ca + "-cert.pem",
		utils.GetFlagVariantForTests("--tablet-grpc-server-name"), "vttablet server instance"}
	return args
}

func getVitessClient(ctx context.Context, addr string) (vtgateservicepb.VitessClient, error) {
	opt, err := grpcclient.SecureDialOption(grpcCert, grpcKey, grpcCa, "", grpcName)
	if err != nil {
		return nil, err
	}
	cc, err := grpcclient.DialContext(ctx, addr, grpcclient.FailFast(false), opt)
	if err != nil {
		return nil, err
	}
	c := vtgateservicepb.NewVitessClient(cc)
	return c, nil
}

func setCreds(t *testing.T, name string, ca string) {
	f1, err := os.Open(path.Join(certDirectory, "ca-cert.pem"))
	require.NoError(t, err)
	b1, err := io.ReadAll(f1)
	require.NoError(t, err)

	f2, err := os.Open(path.Join(certDirectory, ca+"-cert.pem"))
	require.NoError(t, err)
	b2, err := io.ReadAll(f2)
	require.NoError(t, err)

	caContent := append(b1, b2...)
	fileName := "ca-" + name + ".pem"
	caVtgateClient := path.Join(certDirectory, fileName)
	f, err := os.Create(caVtgateClient)
	require.NoError(t, err)
	_, err = f.Write(caContent)
	require.NoError(t, err)

	grpcCa = caVtgateClient
	grpcKey = path.Join(certDirectory, name+"-key.pem")
	grpcCert = path.Join(certDirectory, name+"-cert.pem")

	err = f.Close()
	require.NoError(t, err)
	err = f2.Close()
	require.NoError(t, err)
	err = f1.Close()
	require.NoError(t, err)
}

func setSSLInfoEmpty() {
	grpcCa = ""
	grpcCert = ""
	grpcKey = ""
	grpcName = ""
}

func getSession() *vtgatepb.Session {
	return &vtgatepb.Session{
		TargetString: "test_keyspace:0@primary",
	}
}

func getRequestWithCallerID(callerID *vtrpc.CallerID, sql string) *vtgatepb.ExecuteRequest {
	session := getSession()
	return &vtgatepb.ExecuteRequest{
		CallerId: callerID,
		Session:  session,
		Query: &querypb.BoundQuery{
			Sql: sql,
		},
	}
}

func getRequest(sql string) *vtgatepb.ExecuteRequest {
	session := getSession()
	return &vtgatepb.ExecuteRequest{
		Session: session,
		Query: &querypb.BoundQuery{
			Sql: sql,
		},
	}
}
