package plancontext

import (
	"context"
	"strings"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// PlannerVersion is an alias here to make the code more readable
type PlannerVersion = querypb.ExecuteOptions_PlannerVersion

// VSchema defines the interface for this package to fetch
// info about tables.
type VSchema interface {
	FindTable(tablename sqlparser.TableName) (*vindexes.BaseTable, string, topodatapb.TabletType, key.ShardDestination, error)
	FindView(name sqlparser.TableName) sqlparser.TableStatement
	// FindViewTarget finds the target keyspace for the view table provided.
	FindViewTarget(name sqlparser.TableName) (*vindexes.Keyspace, error)
	FindTableOrVindex(tablename sqlparser.TableName) (*vindexes.BaseTable, vindexes.Vindex, string, topodatapb.TabletType, key.ShardDestination, error)

	// SelectedKeyspace returns the current keyspace if set, otherwise returns an error
	SelectedKeyspace() (*vindexes.Keyspace, error)
	TargetString() string
	ShardDestination() key.ShardDestination
	TabletType() topodatapb.TabletType
	TargetDestination(qualifier string) (key.ShardDestination, *vindexes.Keyspace, topodatapb.TabletType, error)
	AnyKeyspace() (*vindexes.Keyspace, error)
	FirstSortedKeyspace() (*vindexes.Keyspace, error)
	SysVarSetEnabled() bool
	KeyspaceExists(keyspace string) bool
	AllKeyspace() ([]*vindexes.Keyspace, error)
	FindKeyspace(keyspace string) (*vindexes.Keyspace, error)
	GetSemTable() *semantics.SemTable
	Planner() PlannerVersion
	SetPlannerVersion(pv PlannerVersion)
	ConnCollation() collations.ID
	Environment() *vtenv.Environment

	// ErrorIfShardedF will return an error if the keyspace is sharded,
	// and produce a warning if the vtgate if configured to do so
	ErrorIfShardedF(keyspace *vindexes.Keyspace, warn, errFmt string, params ...any) error

	// WarnUnshardedOnly is used when a feature is only supported in unsharded mode.
	// This will let the user know that they are using something
	// that could become a problem if they move to a sharded keyspace
	WarnUnshardedOnly(format string, params ...any)

	// PlannerWarning records warning created during planning.
	PlannerWarning(message string)

	// ForeignKeyMode returns the foreign_key flag value
	ForeignKeyMode(keyspace string) (vschemapb.Keyspace_ForeignKeyMode, error)

	// KeyspaceError returns any error in the keyspace vschema.
	KeyspaceError(keyspace string) error

	GetForeignKeyChecksState() *bool

	// GetVSchema returns the latest cached vindexes.VSchema
	GetVSchema() *vindexes.VSchema

	// GetSrvVschema returns the latest cached vschema.SrvVSchema
	GetSrvVschema() *vschemapb.SrvVSchema

	// FindRoutedShard looks up shard routing rules for a shard
	FindRoutedShard(keyspace, shard string) (string, error)

	// IsShardRoutingEnabled returns true if partial shard routing is enabled
	IsShardRoutingEnabled() bool

	// IsViewsEnabled returns true if Vitess manages the views.
	IsViewsEnabled() bool

	// GetUDV returns user defined value from the variable passed.
	GetUDV(name string) *querypb.BindVariable

	// PlanPrepareStatement plans the prepared statement.
	PlanPrepareStatement(ctx context.Context, query string) (*engine.Plan, error)

	// ClearPrepareData clears the prepared data from the session.
	ClearPrepareData(stmtName string)

	// GetPrepareData returns the prepared data for the statement from the session.
	GetPrepareData(stmtName string) *vtgatepb.PrepareData

	// StorePrepareData stores the prepared data in the session.
	StorePrepareData(name string, v *vtgatepb.PrepareData)

	// GetAggregateUDFs returns the list of aggregate UDFs.
	GetAggregateUDFs() []string

	// FindMirrorRule finds the mirror rule for the requested keyspace, table
	// name, and the tablet type in the VSchema.
	FindMirrorRule(tablename sqlparser.TableName) (*vindexes.MirrorRule, error)

	// GetBindVars returns the bindvars. If we are executing a prepared statement for the first time,
	// we re-plan with the bindvar values to see if we find any better plans now that we can see parameter values.
	// If we find a better plan, we store it, and use it when the bindvars line up
	GetBindVars() map[string]*querypb.BindVariable
}

// PlannerNameToVersion returns the numerical representation of the planner
func PlannerNameToVersion(s string) (PlannerVersion, bool) {
	switch strings.ToLower(s) {
	case "gen4":
		return querypb.ExecuteOptions_Gen4, true
	case "gen4greedy", "greedy":
		return querypb.ExecuteOptions_Gen4Greedy, true
	case "left2right":
		return querypb.ExecuteOptions_Gen4Left2Right, true
	}
	return 0, false
}
