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

package vtgate

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/callinfo"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttls"
)

var (
	mysqlServerPort                   = -1
	mysqlServerBindAddress            string
	mysqlServerSocketPath             string
	mysqlTCPVersion                   = "tcp"
	mysqlAuthServerImpl               = "static"
	mysqlAllowClearTextWithoutTLS     bool
	mysqlProxyProtocol                bool
	mysqlServerRequireSecureTransport bool
	mysqlSslCert                      string
	mysqlSslKey                       string
	mysqlSslCa                        string
	mysqlSslCrl                       string
	mysqlSslServerCA                  string
	mysqlTLSMinVersion                string

	mysqlKeepAlivePeriod          time.Duration
	mysqlConnReadTimeout          time.Duration
	mysqlConnWriteTimeout         time.Duration
	mysqlQueryTimeout             time.Duration
	mysqlSlowConnectWarnThreshold time.Duration
	mysqlConnBufferPooling        bool

	mysqlDefaultWorkloadName = "OLTP"
	mysqlDefaultWorkload     int32
	mysqlDrainOnTerm         bool

	mysqlServerFlushDelay = 100 * time.Millisecond
	mysqlServerMultiQuery = false
)

func registerPluginFlags(fs *pflag.FlagSet) {
	utils.SetFlagIntVar(fs, &mysqlServerPort, "mysql-server-port", mysqlServerPort, "If set, also listen for MySQL binary protocol connections on this port.")
	utils.SetFlagStringVar(fs, &mysqlServerBindAddress, "mysql-server-bind-address", mysqlServerBindAddress, "Binds on this address when listening to MySQL binary protocol. Useful to restrict listening to 'localhost' only for instance.")
	utils.SetFlagStringVar(fs, &mysqlServerSocketPath, "mysql-server-socket-path", mysqlServerSocketPath, "This option specifies the Unix socket file to use when listening for local connections. By default it will be empty and it won't listen to a unix socket")
	utils.SetFlagStringVar(fs, &mysqlTCPVersion, "mysql-tcp-version", mysqlTCPVersion, "Select tcp, tcp4, or tcp6 to control the socket type.")
	utils.SetFlagStringVar(fs, &mysqlAuthServerImpl, "mysql-auth-server-impl", mysqlAuthServerImpl, "Which auth server implementation to use. Options: none, ldap, clientcert, static, vault.")
	utils.SetFlagBoolVar(fs, &mysqlAllowClearTextWithoutTLS, "mysql-allow-clear-text-without-tls", mysqlAllowClearTextWithoutTLS, "If set, the server will allow the use of a clear text password over non-SSL connections.")
	utils.SetFlagBoolVar(fs, &mysqlProxyProtocol, "proxy-protocol", mysqlProxyProtocol, "Enable HAProxy PROXY protocol on MySQL listener socket")
	utils.SetFlagBoolVar(fs, &mysqlServerRequireSecureTransport, "mysql-server-require-secure-transport", mysqlServerRequireSecureTransport, "Reject insecure connections but only if mysql-server-ssl-cert and mysql-server-ssl-key are provided")
	utils.SetFlagStringVar(fs, &mysqlSslCert, "mysql-server-ssl-cert", mysqlSslCert, "Path to the ssl cert for mysql server plugin SSL")
	utils.SetFlagStringVar(fs, &mysqlSslKey, "mysql-server-ssl-key", mysqlSslKey, "Path to ssl key for mysql server plugin SSL")
	utils.SetFlagStringVar(fs, &mysqlSslCa, "mysql-server-ssl-ca", mysqlSslCa, "Path to ssl CA for mysql server plugin SSL. If specified, server will require and validate client certs.")
	utils.SetFlagStringVar(fs, &mysqlSslCrl, "mysql-server-ssl-crl", mysqlSslCrl, "Path to ssl CRL for mysql server plugin SSL")
	utils.SetFlagStringVar(fs, &mysqlTLSMinVersion, "mysql-server-tls-min-version", mysqlTLSMinVersion, "Configures the minimal TLS version negotiated when SSL is enabled. Defaults to TLSv1.2. Options: TLSv1.0, TLSv1.1, TLSv1.2, TLSv1.3.")
	utils.SetFlagStringVar(fs, &mysqlSslServerCA, "mysql-server-ssl-server-ca", mysqlSslServerCA, "path to server CA in PEM format, which will be combine with server cert, return full certificate chain to clients")
	utils.SetFlagDurationVar(fs, &mysqlSlowConnectWarnThreshold, "mysql-slow-connect-warn-threshold", mysqlSlowConnectWarnThreshold, "Warn if it takes more than the given threshold for a mysql connection to establish")
	utils.SetFlagDurationVar(fs, &mysqlConnReadTimeout, "mysql-server-read-timeout", mysqlConnReadTimeout, "connection read timeout")
	utils.SetFlagDurationVar(fs, &mysqlConnWriteTimeout, "mysql-server-write-timeout", mysqlConnWriteTimeout, "connection write timeout")
	utils.SetFlagDurationVar(fs, &mysqlQueryTimeout, "mysql-server-query-timeout", mysqlQueryTimeout, "mysql query timeout")
	fs.BoolVar(&mysqlConnBufferPooling, "mysql-server-pool-conn-read-buffers", mysqlConnBufferPooling, "If set, the server will pool incoming connection read buffers")
	fs.DurationVar(&mysqlKeepAlivePeriod, "mysql-server-keepalive-period", mysqlKeepAlivePeriod, "TCP period between keep-alives")
	utils.SetFlagDurationVar(fs, &mysqlServerFlushDelay, "mysql-server-flush-delay", mysqlServerFlushDelay, "Delay after which buffered response will be flushed to the client.")
	utils.SetFlagStringVar(fs, &mysqlDefaultWorkloadName, "mysql-default-workload", mysqlDefaultWorkloadName, "Default session workload (OLTP, OLAP, DBA)")
	fs.BoolVar(&mysqlDrainOnTerm, "mysql-server-drain-onterm", mysqlDrainOnTerm, "If set, the server waits for --onterm-timeout for already connected clients to complete their in flight work")
	utils.SetFlagBoolVar(fs, &mysqlServerMultiQuery, "mysql-server-multi-query-protocol", mysqlServerMultiQuery, "If set, the server will use the new implementation of handling queries where-in multiple queries are sent together.")
}

// vtgateHandler implements the Listener interface.
// It stores the Session in the ClientData of a Connection.
type vtgateHandler struct {
	mysql.UnimplementedHandler
	mu sync.Mutex

	vtg         *VTGate
	connections map[uint32]*mysql.Conn

	busyConnections atomic.Int32
}

func newVtgateHandler(vtg *VTGate) *vtgateHandler {
	return &vtgateHandler{
		vtg:         vtg,
		connections: make(map[uint32]*mysql.Conn),
	}
}

func (vh *vtgateHandler) NewConnection(c *mysql.Conn) {
	vh.mu.Lock()
	defer vh.mu.Unlock()
	vh.connections[c.ConnectionID] = c
}

func (vh *vtgateHandler) numConnections() int {
	vh.mu.Lock()
	defer vh.mu.Unlock()
	return len(vh.connections)
}

func (vh *vtgateHandler) ComResetConnection(c *mysql.Conn) {
	ctx := context.Background()
	session := vh.session(c)
	if session.InTransaction {
		defer vh.busyConnections.Add(-1)
	}
	err := vh.vtg.CloseSession(ctx, session)
	if err != nil {
		log.Errorf("Error happened in transaction rollback: %v", err)
	}
}

func (vh *vtgateHandler) ConnectionClosed(c *mysql.Conn) {
	// Rollback if there is an ongoing transaction. Ignore error.
	defer func() {
		vh.mu.Lock()
		delete(vh.connections, c.ConnectionID)
		vh.mu.Unlock()
	}()

	var ctx context.Context
	var cancel context.CancelFunc
	if mysqlQueryTimeout != 0 {
		ctx, cancel = context.WithTimeout(context.Background(), mysqlQueryTimeout)
		defer cancel()
	} else {
		ctx = context.Background()
	}
	session := vh.session(c)
	if session.InTransaction {
		defer vh.busyConnections.Add(-1)
	}
	_ = vh.vtg.CloseSession(ctx, session)
}

// Regexp to extract parent span id over the sql query
var r = regexp.MustCompile(`/\*VT_SPAN_CONTEXT=(.*)\*/`)

// this function is here to make this logic easy to test by decoupling the logic from the `trace.NewSpan` and `trace.NewFromString` functions
func startSpanTestable(ctx context.Context, query, label string,
	newSpan func(context.Context, string) (trace.Span, context.Context),
	newSpanFromString func(context.Context, string, string) (trace.Span, context.Context, error)) (trace.Span, context.Context, error) {
	_, comments := sqlparser.SplitMarginComments(query)
	match := r.FindStringSubmatch(comments.Leading)
	span, ctx := getSpan(ctx, match, newSpan, label, newSpanFromString)

	trace.AnnotateSQL(span, sqlparser.Preview(query))

	return span, ctx, nil
}

func getSpan(ctx context.Context, match []string, newSpan func(context.Context, string) (trace.Span, context.Context), label string, newSpanFromString func(context.Context, string, string) (trace.Span, context.Context, error)) (trace.Span, context.Context) {
	var span trace.Span
	if len(match) != 0 {
		var err error
		span, ctx, err = newSpanFromString(ctx, match[1], label)
		if err == nil {
			return span, ctx
		}
		log.Warningf("Unable to parse VT_SPAN_CONTEXT: %s", err.Error())
	}
	span, ctx = newSpan(ctx, label)
	return span, ctx
}

func startSpan(ctx context.Context, query, label string) (trace.Span, context.Context, error) {
	return startSpanTestable(ctx, query, label, trace.NewSpan, trace.NewFromString)
}

func (vh *vtgateHandler) ComQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	session := vh.session(c)
	if c.IsShuttingDown() && !session.InTransaction {
		c.MarkForClose()
		return sqlerror.NewSQLError(sqlerror.ERServerShutdown, sqlerror.SSNetError, "Server shutdown in progress")
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.UpdateCancelCtx(cancel)

	if mysqlQueryTimeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, mysqlQueryTimeout)
		defer cancel()
	}

	span, ctx, err := startSpan(ctx, query, "vtgateHandler.ComQuery")
	if err != nil {
		return vterrors.Wrap(err, "failed to extract span")
	}
	defer span.Finish()

	ctx = callinfo.MysqlCallInfo(ctx, c)

	// Fill in the ImmediateCallerID with the UserData returned by
	// the AuthServer plugin for that user. If nothing was
	// returned, use the User. This lets the plugin map a MySQL
	// user used for authentication to a Vitess User used for
	// Table ACLs and Vitess authentication in general.
	im := c.UserData.Get()
	ef := callerid.NewEffectiveCallerID(
		c.User,                  /* principal: who */
		c.RemoteAddr().String(), /* component: running client process */
		"VTGate MySQL Connector" /* subcomponent: part of the client */)
	ctx = callerid.NewContext(ctx, ef, im)

	if !session.InTransaction {
		vh.busyConnections.Add(1)
	}
	defer func() {
		if !session.InTransaction {
			vh.busyConnections.Add(-1)
		}
	}()

	if session.Options.Workload == querypb.ExecuteOptions_OLAP {
		session, err := vh.vtg.StreamExecute(ctx, vh, session, query, make(map[string]*querypb.BindVariable), callback)
		if err != nil {
			return sqlerror.NewSQLErrorFromError(err)
		}
		fillInTxStatusFlags(c, session)
		return nil
	}
	session, result, err := vh.vtg.Execute(ctx, vh, session, query, make(map[string]*querypb.BindVariable), false)

	if err := sqlerror.NewSQLErrorFromError(err); err != nil {
		return err
	}
	fillInTxStatusFlags(c, session)
	return callback(result)
}

// ComQueryMulti is a newer version of ComQuery that supports running multiple queries in a single call.
func (vh *vtgateHandler) ComQueryMulti(c *mysql.Conn, sql string, callback func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error) error {
	session := vh.session(c)
	if c.IsShuttingDown() && !session.InTransaction {
		c.MarkForClose()
		return sqlerror.NewSQLError(sqlerror.ERServerShutdown, sqlerror.SSNetError, "Server shutdown in progress")
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.UpdateCancelCtx(cancel)

	span, ctx, err := startSpan(ctx, sql, "vtgateHandler.ComQueryMulti")
	if err != nil {
		return vterrors.Wrap(err, "failed to extract span")
	}
	defer span.Finish()

	ctx = callinfo.MysqlCallInfo(ctx, c)

	// Fill in the ImmediateCallerID with the UserData returned by
	// the AuthServer plugin for that user. If nothing was
	// returned, use the User. This lets the plugin map a MySQL
	// user used for authentication to a Vitess User used for
	// Table ACLs and Vitess authentication in general.
	im := c.UserData.Get()
	ef := callerid.NewEffectiveCallerID(
		c.User,                  /* principal: who */
		c.RemoteAddr().String(), /* component: running client process */
		"VTGate MySQL Connector" /* subcomponent: part of the client */)
	ctx = callerid.NewContext(ctx, ef, im)

	if !session.InTransaction {
		vh.busyConnections.Add(1)
	}
	defer func() {
		if !session.InTransaction {
			vh.busyConnections.Add(-1)
		}
	}()

	if session.Options.Workload == querypb.ExecuteOptions_OLAP {
		if c.Capabilities&mysql.CapabilityClientMultiStatements != 0 {
			session, err = vh.vtg.StreamExecuteMulti(ctx, vh, session, sql, callback)
		} else {
			firstPacket := true
			session, err = vh.vtg.StreamExecute(ctx, vh, session, sql, make(map[string]*querypb.BindVariable), func(result *sqltypes.Result) error {
				defer func() {
					firstPacket = false
				}()
				return callback(sqltypes.QueryResponse{QueryResult: result}, false, firstPacket)
			})
		}
		if err != nil {
			return sqlerror.NewSQLErrorFromError(err)
		}
		fillInTxStatusFlags(c, session)
		return nil
	}
	var results []*sqltypes.Result
	var result *sqltypes.Result
	var queryResults []sqltypes.QueryResponse
	if c.Capabilities&mysql.CapabilityClientMultiStatements != 0 {
		session, results, err = vh.vtg.ExecuteMulti(ctx, vh, session, sql)
		for _, res := range results {
			queryResults = append(queryResults, sqltypes.QueryResponse{QueryResult: res})
		}
		if err != nil {
			queryResults = append(queryResults, sqltypes.QueryResponse{QueryError: sqlerror.NewSQLErrorFromError(err)})
		}
	} else {
		session, result, err = vh.vtg.Execute(ctx, vh, session, sql, make(map[string]*querypb.BindVariable), false)
		queryResults = append(queryResults, sqltypes.QueryResponse{QueryResult: result, QueryError: sqlerror.NewSQLErrorFromError(err)})
	}

	fillInTxStatusFlags(c, session)
	for idx, res := range queryResults {
		if callbackErr := callback(res, idx < len(queryResults)-1, true); callbackErr != nil {
			return callbackErr
		}
	}
	return nil
}

func fillInTxStatusFlags(c *mysql.Conn, session *vtgatepb.Session) {
	if session.InTransaction {
		c.StatusFlags |= mysql.ServerStatusInTrans
	} else {
		c.StatusFlags &= mysql.NoServerStatusInTrans
	}
	if session.Autocommit {
		c.StatusFlags |= mysql.ServerStatusAutocommit
	} else {
		c.StatusFlags &= mysql.NoServerStatusAutocommit
	}
}

// ComPrepare is the handler for command prepare.
func (vh *vtgateHandler) ComPrepare(c *mysql.Conn, query string) ([]*querypb.Field, uint16, error) {
	var ctx context.Context
	var cancel context.CancelFunc
	if mysqlQueryTimeout != 0 {
		ctx, cancel = context.WithTimeout(context.Background(), mysqlQueryTimeout)
		defer cancel()
	} else {
		ctx = context.Background()
	}

	ctx = callinfo.MysqlCallInfo(ctx, c)

	// Fill in the ImmediateCallerID with the UserData returned by
	// the AuthServer plugin for that user. If nothing was
	// returned, use the User. This lets the plugin map a MySQL
	// user used for authentication to a Vitess User used for
	// Table ACLs and Vitess authentication in general.
	im := c.UserData.Get()
	ef := callerid.NewEffectiveCallerID(
		c.User,                  /* principal: who */
		c.RemoteAddr().String(), /* component: running client process */
		"VTGate MySQL Connector" /* subcomponent: part of the client */)
	ctx = callerid.NewContext(ctx, ef, im)

	session := vh.session(c)
	if !session.InTransaction {
		vh.busyConnections.Add(1)
	}
	defer func() {
		if !session.InTransaction {
			vh.busyConnections.Add(-1)
		}
	}()

	session, fld, paramsCount, err := vh.vtg.Prepare(ctx, session, query)
	err = sqlerror.NewSQLErrorFromError(err)
	if err != nil {
		return nil, 0, err
	}
	return fld, paramsCount, nil
}

func (vh *vtgateHandler) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	ctx, cancel := context.WithCancel(context.Background())
	c.UpdateCancelCtx(cancel)

	if mysqlQueryTimeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, mysqlQueryTimeout)
		defer cancel()
	}

	ctx = callinfo.MysqlCallInfo(ctx, c)

	// Fill in the ImmediateCallerID with the UserData returned by
	// the AuthServer plugin for that user. If nothing was
	// returned, use the User. This lets the plugin map a MySQL
	// user used for authentication to a Vitess User used for
	// Table ACLs and Vitess authentication in general.
	im := c.UserData.Get()
	ef := callerid.NewEffectiveCallerID(
		c.User,                  /* principal: who */
		c.RemoteAddr().String(), /* component: running client process */
		"VTGate MySQL Connector" /* subcomponent: part of the client */)
	ctx = callerid.NewContext(ctx, ef, im)

	session := vh.session(c)
	if !session.InTransaction {
		vh.busyConnections.Add(1)
	}
	defer func() {
		if !session.InTransaction {
			vh.busyConnections.Add(-1)
		}
	}()

	if session.Options.Workload == querypb.ExecuteOptions_OLAP {
		_, err := vh.vtg.StreamExecute(ctx, vh, session, prepare.PrepareStmt, prepare.BindVars, callback)
		if err != nil {
			return sqlerror.NewSQLErrorFromError(err)
		}
		fillInTxStatusFlags(c, session)
		return nil
	}
	_, qr, err := vh.vtg.Execute(ctx, vh, session, prepare.PrepareStmt, prepare.BindVars, true)
	if err != nil {
		return sqlerror.NewSQLErrorFromError(err)
	}
	fillInTxStatusFlags(c, session)

	return callback(qr)
}

func (vh *vtgateHandler) WarningCount(c *mysql.Conn) uint16 {
	return uint16(len(vh.session(c).GetWarnings()))
}

// ComRegisterReplica is part of the mysql.Handler interface.
func (vh *vtgateHandler) ComRegisterReplica(c *mysql.Conn, replicaHost string, replicaPort uint16, replicaUser string, replicaPassword string) error {
	return vterrors.VT12001("ComRegisterReplica for the VTGate handler")
}

// ComBinlogDump is part of the mysql.Handler interface.
func (vh *vtgateHandler) ComBinlogDump(c *mysql.Conn, logFile string, binlogPos uint32) error {
	return vterrors.VT12001("ComBinlogDump for the VTGate handler")
}

// ComBinlogDumpGTID is part of the mysql.Handler interface.
func (vh *vtgateHandler) ComBinlogDumpGTID(c *mysql.Conn, logFile string, logPos uint64, gtidSet replication.GTIDSet) error {
	return vterrors.VT12001("ComBinlogDumpGTID for the VTGate handler")
}

// KillConnection closes an open connection by connection ID.
func (vh *vtgateHandler) KillConnection(ctx context.Context, connectionID uint32) error {
	vh.mu.Lock()
	defer vh.mu.Unlock()

	c, exists := vh.connections[connectionID]
	if !exists {
		return sqlerror.NewSQLErrorf(sqlerror.ERNoSuchThread, sqlerror.SSUnknownSQLState, "Unknown thread id: %d", connectionID)
	}

	// First, we mark the connection for close, so that even when the context is cancelled, while returning the response back to client,
	// the connection can get closed,
	// Closing the connection will trigger ConnectionClosed method which rollback any open transaction.
	c.MarkForClose()
	c.CancelCtx()

	return nil
}

// KillQuery cancels any execution query on the provided connection ID.
func (vh *vtgateHandler) KillQuery(connectionID uint32) error {
	vh.mu.Lock()
	defer vh.mu.Unlock()
	c, exists := vh.connections[connectionID]
	if !exists {
		return sqlerror.NewSQLErrorf(sqlerror.ERNoSuchThread, sqlerror.SSUnknownSQLState, "Unknown thread id: %d", connectionID)
	}
	c.CancelCtx()
	return nil
}

func (vh *vtgateHandler) Env() *vtenv.Environment {
	return vh.vtg.executor.env
}

func (vh *vtgateHandler) session(c *mysql.Conn) *vtgatepb.Session {
	session, _ := c.ClientData.(*vtgatepb.Session)
	if session == nil {
		u, _ := uuid.NewUUID()
		session = &vtgatepb.Session{
			Options: &querypb.ExecuteOptions{
				IncludedFields: querypb.ExecuteOptions_ALL,
				Workload:       querypb.ExecuteOptions_Workload(mysqlDefaultWorkload),

				// The collation field of ExecuteOption is set right before an execution.
			},
			Autocommit:           true,
			DDLStrategy:          defaultDDLStrategy,
			MigrationContext:     "",
			SessionUUID:          u.String(),
			EnableSystemSettings: sysVarSetEnabled,
		}
		if c.Capabilities&mysql.CapabilityClientFoundRows != 0 {
			session.Options.ClientFoundRows = true
		}
		c.ClientData = session
	}
	return session
}

type mysqlServer struct {
	tcpListener  *mysql.Listener
	unixListener *mysql.Listener
	sigChan      chan os.Signal
	vtgateHandle *vtgateHandler
}

// initTLSConfig inits tls config for the given mysql listener
func initTLSConfig(ctx context.Context, srv *mysqlServer, mysqlSslCert, mysqlSslKey, mysqlSslCa, mysqlSslCrl, mysqlSslServerCA string, mysqlServerRequireSecureTransport bool, mysqlMinTLSVersion uint16) error {
	serverConfig, err := vttls.ServerConfig(mysqlSslCert, mysqlSslKey, mysqlSslCa, mysqlSslCrl, mysqlSslServerCA, mysqlMinTLSVersion)
	if err != nil {
		log.Exitf("grpcutils.TLSServerConfig failed: %v", err)
		return err
	}
	srv.tcpListener.TLSConfig.Store(serverConfig)
	srv.tcpListener.RequireSecureTransport = mysqlServerRequireSecureTransport
	srv.sigChan = make(chan os.Signal, 1)
	signal.Notify(srv.sigChan, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-srv.sigChan:
				serverConfig, err := vttls.ServerConfig(mysqlSslCert, mysqlSslKey, mysqlSslCa, mysqlSslCrl, mysqlSslServerCA, mysqlMinTLSVersion)
				if err != nil {
					log.Errorf("grpcutils.TLSServerConfig failed: %v", err)
				} else {
					log.Info("grpcutils.TLSServerConfig updated")
					srv.tcpListener.TLSConfig.Store(serverConfig)
				}
			}
		}
	}()
	return nil
}

// initMySQLProtocol starts the mysql protocol.
// It should be called only once in a process.
func initMySQLProtocol(vtgate *VTGate) *mysqlServer {
	// Flag is not set, just return.
	if mysqlServerPort < 0 && mysqlServerSocketPath == "" {
		return nil
	}

	// If no VTGate was created, just return.
	if vtgate == nil {
		return nil
	}

	// Initialize registered AuthServer implementations (or other plugins)
	for _, initFn := range pluginInitializers {
		initFn()
	}
	authServer := mysql.GetAuthServer(mysqlAuthServerImpl)

	// Check mysql-default-workload
	var ok bool
	if mysqlDefaultWorkload, ok = querypb.ExecuteOptions_Workload_value[strings.ToUpper(mysqlDefaultWorkloadName)]; !ok {
		log.Exitf("-mysql-default-workload must be one of [OLTP, OLAP, DBA, UNSPECIFIED]")
	}

	switch mysqlTCPVersion {
	case "tcp", "tcp4", "tcp6":
		// Valid flag value.
	default:
		log.Exitf("-mysql-tcp-version must be one of [tcp, tcp4, tcp6]")
	}

	// Create a Listener.
	var err error
	srv := &mysqlServer{}
	srv.vtgateHandle = newVtgateHandler(vtgate)
	if mysqlServerPort >= 0 {
		srv.tcpListener, err = mysql.NewListener(
			mysqlTCPVersion,
			net.JoinHostPort(mysqlServerBindAddress, fmt.Sprintf("%v", mysqlServerPort)),
			authServer,
			srv.vtgateHandle,
			mysqlConnReadTimeout,
			mysqlConnWriteTimeout,
			mysqlProxyProtocol,
			mysqlConnBufferPooling,
			mysqlKeepAlivePeriod,
			mysqlServerFlushDelay,
			mysqlServerMultiQuery,
		)
		if err != nil {
			log.Exitf("mysql.NewListener failed: %v", err)
		}
		if mysqlSslCert != "" && mysqlSslKey != "" {
			tlsVersion, err := vttls.TLSVersionToNumber(mysqlTLSMinVersion)
			if err != nil {
				log.Exitf("mysql.NewListener failed: %v", err)
			}

			_ = initTLSConfig(context.Background(), srv, mysqlSslCert, mysqlSslKey, mysqlSslCa, mysqlSslCrl, mysqlSslServerCA, mysqlServerRequireSecureTransport, tlsVersion)
		}
		srv.tcpListener.AllowClearTextWithoutTLS.Store(mysqlAllowClearTextWithoutTLS)
		// Check for the connection threshold
		if mysqlSlowConnectWarnThreshold != 0 {
			log.Infof("setting mysql slow connection threshold to %v", mysqlSlowConnectWarnThreshold)
			srv.tcpListener.SlowConnectWarnThreshold.Store(mysqlSlowConnectWarnThreshold.Nanoseconds())
		}
		// Start listening for tcp
		go srv.tcpListener.Accept()
	}

	if mysqlServerSocketPath != "" {
		err = setupUnixSocket(srv, authServer, mysqlServerSocketPath)
		if err != nil {
			log.Exitf("mysql.NewListener failed: %v", err)
		}
	}
	return srv
}

// newMysqlUnixSocket creates a new unix socket mysql listener. If a socket file already exists, attempts
// to clean it up.
func newMysqlUnixSocket(address string, authServer mysql.AuthServer, handler mysql.Handler) (*mysql.Listener, error) {
	listener, err := mysql.NewListener(
		"unix",
		address,
		authServer,
		handler,
		mysqlConnReadTimeout,
		mysqlConnWriteTimeout,
		false,
		mysqlConnBufferPooling,
		mysqlKeepAlivePeriod,
		mysqlServerFlushDelay,
		mysqlServerMultiQuery,
	)

	switch err := err.(type) {
	case nil:
		return listener, nil
	case *net.OpError:
		log.Warningf("Found existent socket when trying to create new unix mysql listener: %s, attempting to clean up", address)
		// err.Op should never be different from listen, just being extra careful
		// in case in the future other errors are returned here
		if err.Op != "listen" {
			return nil, err
		}
		_, dialErr := net.Dial("unix", address)
		if dialErr == nil {
			log.Errorf("Existent socket '%s' is still accepting connections, aborting", address)
			return nil, err
		}
		removeFileErr := os.Remove(address)
		if removeFileErr != nil {
			log.Errorf("Couldn't remove existent socket file: %s", address)
			return nil, err
		}
		listener, listenerErr := mysql.NewListener(
			"unix",
			address,
			authServer,
			handler,
			mysqlConnReadTimeout,
			mysqlConnWriteTimeout,
			false,
			mysqlConnBufferPooling,
			mysqlKeepAlivePeriod,
			mysqlServerFlushDelay,
			mysqlServerMultiQuery,
		)
		return listener, listenerErr
	default:
		return nil, err
	}
}

func (srv *mysqlServer) shutdownMysqlProtocolAndDrain() {
	if srv.sigChan != nil {
		signal.Stop(srv.sigChan)
	}
	setListenerToNil := func() {
		srv.tcpListener = nil
		srv.unixListener = nil
	}

	if mysqlDrainOnTerm {
		stopListener(srv.unixListener, false)
		stopListener(srv.tcpListener, false)
		setListenerToNil()
		// We wait for connected clients to drain by themselves or to run into the onterm timeout
		log.Infof("Starting drain loop, waiting for all clients to disconnect")
		reported := time.Now()
		for srv.vtgateHandle.numConnections() > 0 {
			if time.Since(reported) > 2*time.Second {
				log.Infof("Still waiting for client connections to drain (%d connected)...", srv.vtgateHandle.numConnections())
				reported = time.Now()
			}
			time.Sleep(1000 * time.Millisecond)
		}
		return
	}

	stopListener(srv.unixListener, true)
	stopListener(srv.tcpListener, true)
	setListenerToNil()
	if busy := srv.vtgateHandle.busyConnections.Load(); busy > 0 {
		log.Infof("Waiting for all client connections to be idle (%d active)...", busy)
		start := time.Now()
		reported := start
		for busy > 0 {
			if time.Since(reported) > 2*time.Second {
				log.Infof("Still waiting for client connections to be idle (%d active)...", busy)
				reported = time.Now()
			}

			time.Sleep(1 * time.Millisecond)
			busy = srv.vtgateHandle.busyConnections.Load()
		}
	}
}

// stopListener Close or Shutdown a mysql listener depending on the shutdown argument.
func stopListener(listener *mysql.Listener, shutdown bool) {
	if listener == nil {
		return
	}
	if shutdown {
		listener.Shutdown()
	} else {
		listener.Close()
	}
}

func (srv *mysqlServer) rollbackAtShutdown() {
	defer log.Flush()
	if srv.vtgateHandle == nil {
		// we still haven't been able to initialise the vtgateHandler, so we don't need to rollback anything
		return
	}

	// Close all open connections. If they're waiting for reads, this will cause
	// them to error out, which will automatically rollback open transactions.
	func() {
		if srv.vtgateHandle != nil {
			srv.vtgateHandle.mu.Lock()
			defer srv.vtgateHandle.mu.Unlock()
			for id, c := range srv.vtgateHandle.connections {
				if c != nil {
					log.Infof("Rolling back transactions associated with connection ID: %v", id)
					c.Close()
				}
			}
		}
	}()

	// If vtgate is instead busy executing a query, the number of open conns
	// will be non-zero. Give another second for those queries to finish.
	for i := 0; i < 100; i++ {
		if srv.vtgateHandle.numConnections() == 0 {
			log.Infof("All connections have been rolled back.")
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	log.Errorf("All connections did not go idle. Shutting down anyway.")
}

func mysqlSocketPath() string {
	if mysqlServerSocketPath == "" {
		return ""
	}
	return mysqlServerSocketPath
}

// GetMysqlServerSSLCA returns the current value of the mysql-server-ssl-ca flag
func GetMysqlServerSSLCA() string {
	return mysqlSslCa
}

func init() {
	servenv.OnParseFor("vtgate", registerPluginFlags)
	servenv.OnParseFor("vtcombo", registerPluginFlags)
}

var pluginInitializers []func()

// RegisterPluginInitializer lets plugins register themselves to be init'ed at servenv.OnRun-time
func RegisterPluginInitializer(initializer func()) {
	pluginInitializers = append(pluginInitializers, initializer)
}
