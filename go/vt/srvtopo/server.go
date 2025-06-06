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

/*
Package srvtopo contains a set of helper methods and classes to
use the topology service in a serving environment.
*/
package srvtopo

import (
	"context"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/topo"
)

// Server is a subset of the topo.Server API that only contains
// the serving graph read-only calls used by clients to resolve
// serving addresses, and to get VSchema.
type Server interface {
	// GetTopoServer returns the full topo.Server instance.
	GetTopoServer() (*topo.Server, error)

	// GetSrvKeyspaceNames returns the list of keyspaces served in
	// the provided cell.
	GetSrvKeyspaceNames(ctx context.Context, cell string, staleOK bool) ([]string, error)

	// GetSrvKeyspace returns the SrvKeyspace for a cell/keyspace.
	GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error)

	// WatchSrvKeyspace starts watching the SrvKeyspace object for changes. The callback function is called upon change.
	// The callback function should return `true` if it wishes to continue watching for further changes, or `false` if it
	// is done and wants to stop watching.
	WatchSrvKeyspace(ctx context.Context, cell, keyspace string, callback func(*topodatapb.SrvKeyspace, error) bool)

	// WatchSrvVSchema starts watching the SrvVSchema object for
	// the provided cell.  It will call the callback when
	// a new value or an error occurs.
	WatchSrvVSchema(ctx context.Context, cell string, callback func(*vschemapb.SrvVSchema, error) bool)
}
