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

package binlogplayer

import (
	"context"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/servenv"
	vtutils "vitess.io/vitess/go/vt/utils"
)

/*
This file contains the API and registration mechanism for binlog player client.
*/

var binlogPlayerProtocol = "grpc"

func init() {
	servenv.OnParseFor("vtcombo", registerFlags)
	servenv.OnParseFor("vttablet", registerFlags)
}

func registerFlags(fs *pflag.FlagSet) {
	vtutils.SetFlagStringVar(fs, &binlogPlayerProtocol, "binlog-player-protocol", binlogPlayerProtocol, "the protocol to download binlogs from a vttablet")
}

// BinlogTransactionStream is the interface of the object returned by
// StreamTables and StreamKeyRange
type BinlogTransactionStream interface {
	// Recv returns the next BinlogTransaction, or an error if the RPC was
	// interrupted.
	Recv() (*binlogdatapb.BinlogTransaction, error)
}

// Client is the interface all clients must satisfy
type Client interface {
	// Dial a server
	Dial(ctx context.Context, tablet *topodatapb.Tablet) error

	// Close the connection
	Close()

	// Ask the server to stream updates related to the provided tables.
	// Should return context.Canceled if the context is canceled.
	StreamTables(ctx context.Context, position string, tables []string, charset *binlogdatapb.Charset) (BinlogTransactionStream, error)

	// Ask the server to stream updates related to the provided keyrange.
	// Should return context.Canceled if the context is canceled.
	StreamKeyRange(ctx context.Context, position string, keyRange *topodatapb.KeyRange, charset *binlogdatapb.Charset) (BinlogTransactionStream, error)
}

// ClientFactory is the factory method to create a Client
type ClientFactory func() Client

var clientFactories = make(map[string]ClientFactory)

// RegisterClientFactory adds a new factory. Call during init().
func RegisterClientFactory(name string, factory ClientFactory) {
	if _, ok := clientFactories[name]; ok {
		log.Fatalf("ClientFactory %s already exists", name)
	}
	clientFactories[name] = factory
}
