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
Package topo is the module responsible for interacting with the topology
service. It uses one Conn connection to the global topo service (with
possibly another one to a read-only version of the global topo service),
and one to each cell topo service.

It contains the plug-in interfaces Conn, Factory and Version that topo
implementations will use. We support Zookeeper, etcd, consul as real
topo servers, and in-memory, tee as test and utility topo servers.
Implementations are in sub-directories here.

In tests, we do not mock this package. Instead, we just use a memorytopo.

We also support copying data across topo servers (using helpers/copy.go
and the topo2topo cmd binary), and writing to two topo servers at the same
time (using helpers/tee.go). This is to facilitate migrations between
topo servers.

There are two test sub-packages associated with this code:
  - test/ contains a test suite that is run against all of our implementations.
    It just performs a bunch of common topo server activities (create, list,
    delete various objects, ...). If a topo implementation passes all these
    tests, it most likely will work as expected in a real deployment.
  - topotests/ contains tests that use a memorytopo to test the code in this
    package.
*/
package topo

import (
	"context"
	"fmt"
	"path"
	"sync"

	"github.com/spf13/pflag"
	"golang.org/x/sync/semaphore"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	// GlobalCell is the name of the global cell.  It is special
	// as it contains the global topology, and references the other cells.
	GlobalCell = "global"

	// GlobalReadOnlyCell is the name of the global read-only cell
	// connection cell name.
	GlobalReadOnlyCell = "global-read-only"
)

// Filenames for all object types.
const (
	CellInfoFile           = "CellInfo"
	CellsAliasFile         = "CellsAlias"
	KeyspaceFile           = "Keyspace"
	ShardFile              = "Shard"
	VSchemaFile            = "VSchema"
	ShardReplicationFile   = "ShardReplication"
	TabletFile             = "Tablet"
	SrvVSchemaFile         = "SrvVSchema"
	SrvKeyspaceFile        = "SrvKeyspace"
	RoutingRulesFile       = "RoutingRules"
	ExternalClustersFile   = "ExternalClusters"
	ShardRoutingRulesFile  = "ShardRoutingRules"
	CommonRoutingRulesFile = "Rules"
	MirrorRulesFile        = "MirrorRules"
)

// Path for all object types.
const (
	CellsPath                = "cells"
	CellsAliasesPath         = "cells_aliases"
	KeyspacesPath            = "keyspaces"
	ShardsPath               = "shards"
	TabletsPath              = "tablets"
	MetadataPath             = "metadata"
	ExternalClusterVitess    = "vitess"
	RoutingRulesPath         = "routing_rules"
	KeyspaceRoutingRulesPath = "keyspace"
	NamedLocksPath           = "internal/named_locks"
)

// Factory is a factory interface to create Conn objects.
// Topo implementations will provide an implementation for this.
type Factory interface {
	// HasGlobalReadOnlyCell returns true if the global cell
	// has read-only replicas of the topology data. The global topology
	// is usually more expensive to read from / write to, as it is
	// replicated over many cells. Some topology services provide
	// more efficient way to read the data, like Observer servers
	// for Zookeeper. If this returns true, we will maintain
	// two connections for the global topology: the 'global' cell
	// for consistent reads and writes, and the 'global-read-only'
	// cell for reads only.
	HasGlobalReadOnlyCell(serverAddr, root string) bool

	// Create creates a topo.Conn object.
	Create(cell, serverAddr, root string) (Conn, error)
}

// Server is the main topo.Server object. We support two ways of creating one:
//  1. From an implementation, server address, and root path.
//     This uses a plugin mechanism, and we have implementations for
//     etcd, zookeeper and consul.
//  2. Specific implementations may have higher level creation methods
//     (in which case they may provide a more complex Factory).
//     We support memorytopo (for tests and processes that only need an
//     in-memory server), and tee (a helper implementation to transition
//     between one server implementation and another).
type Server struct {
	// globalCell is the main connection to the global topo service.
	// It is created once at construction time.
	globalCell Conn

	// globalReadOnlyCell is the read-only connection to the global
	// topo service. It will be equal to globalCell if we don't distinguish
	// the two.
	globalReadOnlyCell Conn

	// factory allows the creation of connections to various backends.
	// It is set at construction time.
	factory Factory

	// mu protects the following fields.
	mu sync.Mutex
	// cellConns contains clients configured to talk to a list of
	// topo instances representing local topo clusters. These
	// should be accessed with the ConnForCell() method, which
	// will read the list of addresses for that cell from the
	// global cluster and create clients as needed.
	cellConns map[string]cellConn
}

type cellConn struct {
	cellInfo *topodata.CellInfo
	conn     Conn
}

type cellsToAliasesMap struct {
	mu sync.Mutex
	// cellsToAliases contains all cell->alias mappings
	cellsToAliases map[string]string
}

var (
	// topoImplementation is the flag for which implementation to use.
	topoImplementation string

	// topoGlobalServerAddress is the address of the global topology
	// server.
	topoGlobalServerAddress string

	// topoGlobalRoot is the root path to use for the global topology
	// server.
	topoGlobalRoot string

	// factories has the factories for the Conn objects.
	factories = make(map[string]Factory)

	cellsAliases = cellsToAliasesMap{
		cellsToAliases: make(map[string]string),
	}

	FlagBinaries = []string{"vttablet", "vtctl", "vtctld", "vtcombo", "vtgate",
		"vtorc", "vtbackup"}

	// Default read concurrency to use in order to avoid overhwelming the topo server.
	DefaultReadConcurrency int64 = 32
)

func init() {
	for _, cmd := range FlagBinaries {
		servenv.OnParseFor(cmd, registerTopoFlags)
	}
}

func registerTopoFlags(fs *pflag.FlagSet) {
	utils.SetFlagStringVar(fs, &topoImplementation, "topo-implementation", topoImplementation, "the topology implementation to use")
	utils.SetFlagStringVar(fs, &topoGlobalServerAddress, "topo-global-server-address", topoGlobalServerAddress, "the address of the global topology server")
	utils.SetFlagStringVar(fs, &topoGlobalRoot, "topo-global-root", topoGlobalRoot, "the path of the global topology data in the global topology server")
	utils.SetFlagInt64Var(fs, &DefaultReadConcurrency, "topo-read-concurrency", DefaultReadConcurrency, "Maximum concurrency of topo reads per global or local cell.")
}

// RegisterFactory registers a Factory for an implementation for a Server.
// If an implementation with that name already exists, it log.Fatals out.
// Call this in the 'init' function in your topology implementation module.
func RegisterFactory(name string, factory Factory) {
	if factories[name] != nil {
		log.Fatalf("Duplicate topo.Factory registration for %v", name)
	}
	factories[name] = factory
}

// NewWithFactory creates a new Server based on the given Factory.
// It also opens the global cell connection.
func NewWithFactory(factory Factory, serverAddress, root string) (*Server, error) {
	globalReadSem := semaphore.NewWeighted(DefaultReadConcurrency)
	conn, err := factory.Create(GlobalCell, serverAddress, root)
	if err != nil {
		return nil, err
	}
	conn = NewStatsConn(GlobalCell, conn, globalReadSem)

	var connReadOnly Conn
	if factory.HasGlobalReadOnlyCell(serverAddress, root) {
		connReadOnly, err = factory.Create(GlobalReadOnlyCell, serverAddress, root)
		if err != nil {
			return nil, err
		}
		connReadOnly = NewStatsConn(GlobalReadOnlyCell, connReadOnly, globalReadSem)
	} else {
		connReadOnly = conn
	}

	return &Server{
		globalCell:         conn,
		globalReadOnlyCell: connReadOnly,
		factory:            factory,
		cellConns:          make(map[string]cellConn),
	}, nil
}

// OpenServer returns a Server using the provided implementation,
// address and root for the global server.
func OpenServer(implementation, serverAddress, root string) (*Server, error) {
	factory, ok := factories[implementation]
	if !ok {
		return nil, NewError(NoImplementation, implementation)
	}
	return NewWithFactory(factory, serverAddress, root)
}

// Open returns a Server using the command line parameter flags
// for implementation, address and root. It log.Exits out if an error occurs.
func Open() *Server {
	if topoGlobalServerAddress == "" {
		log.Exitf("topo-global-server-address must be configured")
	}
	if topoGlobalRoot == "" {
		log.Exit("topo-global-root must be non-empty")
	}
	ts, err := OpenServer(topoImplementation, topoGlobalServerAddress, topoGlobalRoot)
	if err != nil {
		log.Exitf("Failed to open topo server (%v,%v,%v): %v", topoImplementation, topoGlobalServerAddress, topoGlobalRoot, err)
	}
	return ts
}

// ConnForCell returns a Conn object for the given cell.
// It caches Conn objects from previously requested cells.
func (ts *Server) ConnForCell(ctx context.Context, cell string) (Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Global cell is the easy case.
	if cell == GlobalCell {
		return ts.globalCell, nil
	}

	// Fetch cell cluster addresses from the global cluster.
	// We can use the GlobalReadOnlyCell for this call.
	ci, err := ts.GetCellInfo(ctx, cell, false /*strongRead*/)
	if err != nil {
		return nil, err
	}

	// Return a cached client if present.
	ts.mu.Lock()
	defer ts.mu.Unlock()
	cc, ok := ts.cellConns[cell]
	if ok {
		// Client exists in cache.
		// Let's verify that it is the same cell as we are looking for.
		// The cell name can be re-used with a different ServerAddress and/or Root
		// in which case we should get a new connection and update the cache
		if ci.ServerAddress == cc.cellInfo.ServerAddress && ci.Root == cc.cellInfo.Root {
			return cc.conn, nil
		}
		// Close the cached connection, we don't need it anymore
		if cc.conn != nil {
			cc.conn.Close()
		}
	}

	// Connect to the cell topo server, while holding the lock.
	// This ensures only one connection is established at any given time.
	// Create the connection and cache it
	conn, err := ts.factory.Create(cell, ci.ServerAddress, ci.Root)
	switch {
	case err == nil:
		cellReadSem := semaphore.NewWeighted(DefaultReadConcurrency)
		conn = NewStatsConn(cell, conn, cellReadSem)
		ts.cellConns[cell] = cellConn{ci, conn}
		return conn, nil
	case IsErrType(err, NoNode):
		err = vterrors.Wrap(err, fmt.Sprintf("failed to create topo connection to %v, %v", ci.ServerAddress, ci.Root))
		return nil, NewError(NoNode, err.Error())
	default:
		return nil, vterrors.Wrap(err, fmt.Sprintf("failed to create topo connection to %v, %v", ci.ServerAddress, ci.Root))
	}
}

// GetAliasByCell returns the alias group this `cell` belongs to, if there's none, it returns the `cell` as alias.
func GetAliasByCell(ctx context.Context, ts *Server, cell string) string {
	cellsAliases.mu.Lock()
	defer cellsAliases.mu.Unlock()
	if region, ok := cellsAliases.cellsToAliases[cell]; ok {
		return region
	}
	if ts != nil {
		// lazily get the region from cell info if `aliases` are available
		cellAliases, err := ts.GetCellsAliases(ctx, false)
		if err != nil {
			// for backward compatibility
			return cell
		}

		for alias, cellsAlias := range cellAliases {
			for _, cellAlias := range cellsAlias.Cells {
				if cellAlias == cell {
					cellsAliases.cellsToAliases[cell] = alias
					return alias
				}
			}
		}
	}
	// for backward compatibility
	return cell
}

// Close will close all connections to underlying topo Server.
// It will nil all member variables, so any further access will panic.
func (ts *Server) Close() {
	if ts.globalCell != nil {
		ts.globalCell.Close()
	}
	if ts.globalReadOnlyCell != nil && ts.globalReadOnlyCell != ts.globalCell {
		ts.globalReadOnlyCell.Close()
	}
	ts.globalCell = nil
	ts.globalReadOnlyCell = nil
	ts.mu.Lock()
	defer ts.mu.Unlock()
	for _, cc := range ts.cellConns {
		cc.conn.Close()
	}
	ts.cellConns = make(map[string]cellConn)
}

func (ts *Server) clearCellAliasesCache() {
	cellsAliases.mu.Lock()
	defer cellsAliases.mu.Unlock()
	cellsAliases.cellsToAliases = make(map[string]string)
}

// OpenExternalVitessClusterServer returns the topo server of the external cluster
func (ts *Server) OpenExternalVitessClusterServer(ctx context.Context, clusterName string) (*Server, error) {
	vc, err := ts.GetExternalVitessCluster(ctx, clusterName)
	if err != nil {
		return nil, err
	}
	if vc == nil {
		return nil, fmt.Errorf("no vitess cluster found with name %s", clusterName)
	}
	var externalTopo *Server
	externalTopo, err = OpenServer(vc.TopoConfig.TopoType, vc.TopoConfig.Server, vc.TopoConfig.Root)
	if err != nil {
		return nil, err
	}
	if externalTopo == nil {
		return nil, fmt.Errorf("unable to open external topo for config %s", clusterName)
	}
	return externalTopo, nil
}

// SetReadOnly is initially ONLY implemented by StatsConn and used in ReadOnlyServer
func (ts *Server) SetReadOnly(readOnly bool) error {
	globalCellConn, ok := ts.globalCell.(*StatsConn)
	if !ok {
		return fmt.Errorf("invalid global cell connection type, expected StatsConn but found: %T", ts.globalCell)
	}
	globalCellConn.SetReadOnly(readOnly)

	for _, cc := range ts.cellConns {
		localCellConn, ok := cc.conn.(*StatsConn)
		if !ok {
			return fmt.Errorf("invalid local cell connection type, expected StatsConn but found: %T", cc.conn)
		}
		localCellConn.SetReadOnly(true)
	}

	return nil
}

// IsReadOnly is initially ONLY implemented by StatsConn and used in ReadOnlyServer
func (ts *Server) IsReadOnly() (bool, error) {
	globalCellConn, ok := ts.globalCell.(*StatsConn)
	if !ok {
		return false, fmt.Errorf("invalid global cell connection type, expected StatsConn but found: %T", ts.globalCell)
	}
	if !globalCellConn.IsReadOnly() {
		return false, nil
	}

	for _, cc := range ts.cellConns {
		localCellConn, ok := cc.conn.(*StatsConn)
		if !ok {
			return false, fmt.Errorf("invalid local cell connection type, expected StatsConn but found: %T", cc.conn)
		}
		if !localCellConn.IsReadOnly() {
			return false, nil
		}
	}

	return true, nil
}

// GetKeyspaceRoutingRulesPath returns the path to the keyspace routing rules file in the topo.
func (ts *Server) GetKeyspaceRoutingRulesPath() string {
	return path.Join(RoutingRulesPath, KeyspaceRoutingRulesPath, CommonRoutingRulesFile)
}
