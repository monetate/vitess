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

package mysql

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/bucketpool"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	DefaultFlushDelay = 100 * time.Millisecond

	// connBufferSize is how much we buffer for reading and
	// writing. It is also how much we allocate for ephemeral buffers.
	connBufferSize = 16 * 1024

	// packetHeaderSize is the 4 bytes of header per MySQL packet
	// sent over
	packetHeaderSize = 4
)

// Constants for how ephemeral buffers were used for reading / writing.
const (
	// ephemeralUnused means the ephemeral buffer is not in use at this
	// moment. This is the default value, and is checked so we don't
	// read or write a packet while one is already used.
	ephemeralUnused = iota

	// ephemeralWrite means we currently in process of writing from  currentEphemeralBuffer
	ephemeralWrite

	// ephemeralRead means we currently in process of reading into currentEphemeralBuffer
	ephemeralRead
)

// A Getter has a Get()
type Getter interface {
	Get() *querypb.VTGateCallerID
}

// Conn is a connection between a client and a server, using the MySQL
// binary protocol. It is built on top of an existing net.Conn, that
// has already been established.
//
// Use Connect on the client side to create a connection.
// Use NewListener to create a server side and listen for connections.
type Conn struct {
	// fields contains the fields definitions for an on-going
	// streaming query. It is set by ExecuteStreamFetch, and
	// cleared by the last FetchNext().  It is nil if no streaming
	// query is in progress.  If the streaming query returned no
	// fields, this is set to an empty array (but not nil).
	fields []*querypb.Field

	// salt is sent by the server during initial handshake to be used for authentication
	salt []byte

	// authPluginName is the name of server's authentication plugin.
	// It is set during the initial handshake.
	authPluginName AuthMethodDescription

	// schemaName is the default database name to use. It is set
	// during handshake, and by ComInitDb packets. Both client and
	// servers maintain it. This member is private because it's
	// non-authoritative: the client can change the schema name
	// through the 'USE' statement, which will bypass this variable.
	schemaName string

	// ClientData is a place where an application can store any
	// connection-related data. Mostly used on the server side, to
	// avoid maps indexed by ConnectionID for instance.
	ClientData any

	// conn is the underlying network connection.
	// Calling Close() on the Conn will close this connection.
	// If there are any ongoing reads or writes, they may get interrupted.
	conn net.Conn

	// flavor contains the auto-detected flavor for this client
	// connection. It is unused for server-side connections.
	flavor flavor

	// ServerVersion is set during Connect with the server
	// version.  It is not changed afterwards. It is unused for
	// server-side connections.
	ServerVersion string

	// User is the name used by the client to connect.
	// It is set during the initial handshake.
	User string // For server-side connections, listener points to the server object.

	// UserData is custom data returned by the AuthServer module.
	// It is set during the initial handshake.
	UserData Getter

	bufferedReader *bufio.Reader
	flushTimer     *time.Timer
	flushDelay     time.Duration
	header         [packetHeaderSize]byte

	// Keep track of how and of the buffer we allocated for an
	// ephemeral packet on the read and write sides.
	// These fields are used by:
	// - startEphemeralPacketWithHeader / writeEphemeralPacket methods for writes.
	// - readEphemeralPacket / recycleReadPacket methods for reads.
	currentEphemeralPolicy int
	// currentEphemeralBuffer for tracking allocated temporary buffer for writes and reads respectively.
	// It can be allocated from bufPool or heap and should be recycled in the same manner.
	currentEphemeralBuffer *[]byte

	listener *Listener

	// Buffered writing has a timer which flushes on inactivity.
	bufferedWriter *bufio.Writer

	// PrepareData is the map to use a prepared statement.
	PrepareData map[uint32]*PrepareData

	// protects the bufferedWriter and bufferedReader
	bufMu sync.Mutex

	// Capabilities is the current set of features this connection
	// is using.  It is the features that are both supported by
	// the client and the server, and currently in use.
	// It is set during the initial handshake.
	//
	// It is only used for CapabilityClientDeprecateEOF
	// and CapabilityClientFoundRows.
	Capabilities uint32

	// closed is set to true when Close() is called on the connection.
	closed atomic.Bool

	// ConnectionID is set:
	// - at Connect() time for clients, with the value returned by
	// the server.
	// - at accept time for the server.
	ConnectionID uint32

	// StatementID is the prepared statement ID.
	StatementID uint32

	// StatusFlags are the status flags we will base our returned flags on.
	// This is a bit field, with values documented in constants.go.
	// An interesting value here would be ServerStatusAutocommit.
	// It is only used by the server. These flags can be changed
	// by Handler methods.
	StatusFlags uint16

	// CharacterSet is the charset for this connection, as negotiated
	// in our handshake with the server. Note that although the MySQL protocol lists this
	// as a "character set", the returned byte value is actually a Collation ID,
	// and hence it's casted as such here.
	// If the user has specified a custom Collation in the ConnParams for this
	// connection, once the CharacterSet has been negotiated, we will override
	// it via SQL and update this field accordingly.
	CharacterSet collations.ID

	// Packet encoding variables.
	sequence uint8

	// ExpectSemiSyncIndicator is applicable when the connection is used for replication (ComBinlogDump).
	// When 'true', events are assumed to be padded with 2-byte semi-sync information
	// See https://dev.mysql.com/doc/internals/en/semi-sync-binlog-event.html
	ExpectSemiSyncIndicator bool

	// enableQueryInfo controls whether we parse the INFO field in QUERY_OK packets
	// See: ConnParams.EnableQueryInfo
	enableQueryInfo bool

	// keepAliveOn marks when keep alive is active on the connection.
	// This is currently used for testing.
	keepAliveOn bool

	multiQuery bool

	// mu protects the fields below
	mu sync.Mutex
	// cancel keep the cancel function for the current executing query.
	// this is used by `kill [query|connection] ID` command from other connection.
	cancel context.CancelFunc
	// this is used to mark the connection to be closed so that the command phase for the connection can be stopped and
	// the connection gets closed.
	closing bool

	truncateErrLen int
}

// PrepareData is a buffer used for store prepare statement meta data
type PrepareData struct {
	ParamsType  []int32
	ColumnNames []string
	PrepareStmt string
	BindVars    map[string]*querypb.BindVariable
	StatementID uint32
	ParamsCount uint16
}

// execResult is an enum signifying the result of executing a query
type execResult byte

const (
	execSuccess execResult = iota
	execErr
	connErr
)

// bufPool is used to allocate and free buffers in an efficient way.
var bufPool = bucketpool.New(connBufferSize, MaxPacketSize)

// writersPool is used for pooling bufio.Writer objects.
var writersPool = sync.Pool{New: func() any { return bufio.NewWriterSize(nil, connBufferSize) }}

var readersPool = sync.Pool{New: func() any { return bufio.NewReaderSize(nil, connBufferSize) }}

// newConn is an internal method to create a Conn. Used by client and server
// side for common creation code.
func newConn(conn net.Conn, flushDelay time.Duration, truncateErrLen int) *Conn {
	if flushDelay == 0 {
		flushDelay = DefaultFlushDelay
	}
	return &Conn{
		conn:           conn,
		bufferedReader: bufio.NewReaderSize(conn, connBufferSize),
		flushDelay:     flushDelay,
		truncateErrLen: truncateErrLen,
	}
}

// newServerConn should be used to create server connections.
//
// It stashes a reference to the listener to be able to determine if
// the server is shutting down, and has the ability to control buffer
// size for reads.
func newServerConn(conn net.Conn, listener *Listener) *Conn {
	// Enable KeepAlive on TCP connections and change keep-alive period if provided.
	enabledKeepAlive := false
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := setTcpConnProperties(tcpConn, listener.connKeepAlivePeriod); err != nil {
			log.Errorf("error in setting tcp properties: %v", err)
		} else {
			enabledKeepAlive = true
		}
	}

	c := &Conn{
		conn:           conn,
		listener:       listener,
		PrepareData:    make(map[uint32]*PrepareData),
		keepAliveOn:    enabledKeepAlive,
		flushDelay:     listener.flushDelay,
		truncateErrLen: listener.truncateErrLen,
		multiQuery:     listener.multiQuery,
	}

	if listener.connReadBufferSize > 0 {
		var buf *bufio.Reader
		if listener.connBufferPooling {
			buf = readersPool.Get().(*bufio.Reader)
			buf.Reset(conn)
		} else {
			buf = bufio.NewReaderSize(conn, listener.connReadBufferSize)
		}

		c.bufferedReader = buf
	}

	return c
}

func setTcpConnProperties(conn *net.TCPConn, keepAlivePeriod time.Duration) error {
	if err := conn.SetKeepAlive(true); err != nil {
		return vterrors.Wrapf(err, "unable to enable keepalive on tcp connection")
	}

	if keepAlivePeriod <= 0 {
		return nil
	}

	if err := conn.SetKeepAlivePeriod(keepAlivePeriod); err != nil {
		return vterrors.Wrapf(err, "unable to set keepalive period on tcp connection")
	}

	return nil
}

// startWriterBuffering starts using buffered writes. This should
// be terminated by a call to endWriteBuffering.
func (c *Conn) startWriterBuffering() {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	c.bufferedWriter = writersPool.Get().(*bufio.Writer)
	c.bufferedWriter.Reset(c.conn)
}

// endWriterBuffering must be called to terminate startWriteBuffering.
func (c *Conn) endWriterBuffering() error {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	if c.bufferedWriter == nil {
		return nil
	}

	defer func() {
		c.bufferedWriter.Reset(nil)
		writersPool.Put(c.bufferedWriter)
		c.bufferedWriter = nil
	}()

	c.flushTimer.Stop()
	return c.bufferedWriter.Flush()
}

func (c *Conn) returnReader() {
	if c.bufferedReader == nil {
		return
	}
	c.bufferedReader.Reset(nil)
	readersPool.Put(c.bufferedReader)
}

// startFlushTimer must be called while holding lock on bufMu.
func (c *Conn) startFlushTimer() {
	if c.flushTimer == nil {
		c.flushTimer = time.AfterFunc(c.flushDelay, func() {
			c.bufMu.Lock()
			defer c.bufMu.Unlock()

			if c.bufferedWriter == nil {
				return
			}
			c.bufferedWriter.Flush()
		})
	} else {
		c.flushTimer.Reset(c.flushDelay)
	}
}

// getReader returns reader for connection. It can be *bufio.Reader or net.Conn
// depending on which buffer size was passed to newServerConn.
func (c *Conn) getReader() io.Reader {
	if c.bufferedReader != nil {
		return c.bufferedReader
	}
	return c.conn
}

func (c *Conn) readHeaderFrom(r io.Reader) (int, error) {
	// Note io.ReadFull will return two different types of errors:
	// 1. if the socket is already closed, and the go runtime knows it,
	//   then ReadFull will return an error (different than EOF),
	//   something like 'read: connection reset by peer'.
	// 2. if the socket is not closed while we start the read,
	//   but gets closed after the read is started, we'll get io.EOF.
	if _, err := io.ReadFull(r, c.header[:]); err != nil {
		// The special casing of propagating io.EOF up
		// is used by the server side only, to suppress an error
		// message if a client just disconnects.
		if err == io.EOF {
			return 0, err
		}
		if strings.HasSuffix(err.Error(), "read: connection reset by peer") {
			return 0, io.EOF
		}
		return 0, vterrors.Wrapf(err, "io.ReadFull(header size) failed")
	}

	sequence := c.header[3]
	if sequence != c.sequence {
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid sequence, expected %v got %v", c.sequence, sequence)
	}

	c.sequence++

	return int(uint32(c.header[0]) | uint32(c.header[1])<<8 | uint32(c.header[2])<<16), nil
}

// readEphemeralPacket attempts to read a packet into buffer from sync.Pool.  Do
// not use this method if the contents of the packet needs to be kept
// after the next readEphemeralPacket.
//
// Note if the connection is closed already, an error will be
// returned, and it may not be io.EOF. If the connection closes while
// we are stuck waiting for data, an error will also be returned, and
// it most likely will be io.EOF.
func (c *Conn) readEphemeralPacket() ([]byte, error) {
	if c.currentEphemeralPolicy != ephemeralUnused {
		panic(vterrors.Errorf(vtrpcpb.Code_INTERNAL, "readEphemeralPacket: unexpected currentEphemeralPolicy: %v", c.currentEphemeralPolicy))
	}

	r := c.getReader()

	length, err := c.readHeaderFrom(r)
	if err != nil {
		return nil, err
	}

	c.currentEphemeralPolicy = ephemeralRead
	if length == 0 {
		// This can be caused by the packet after a packet of
		// exactly size MaxPacketSize.
		return nil, nil
	}

	// Use the bufPool.
	if length < MaxPacketSize {
		c.currentEphemeralBuffer = bufPool.Get(length)
		if _, err := io.ReadFull(r, *c.currentEphemeralBuffer); err != nil {
			return nil, vterrors.Wrapf(err, "io.ReadFull(packet body of length %v) failed", length)
		}
		return *c.currentEphemeralBuffer, nil
	}

	// Much slower path, revert to allocating everything from scratch.
	// We're going to concatenate a lot of data anyway, can't really
	// optimize this code path easily.
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, vterrors.Wrapf(err, "io.ReadFull(packet body of length %v) failed", length)
	}
	for {
		next, err := c.readOnePacket()
		if err != nil {
			return nil, err
		}

		if len(next) == 0 {
			// Again, the packet after a packet of exactly size MaxPacketSize.
			break
		}

		data = append(data, next...)
		if len(next) < MaxPacketSize {
			break
		}
	}

	return data, nil
}

// readEphemeralPacketDirect attempts to read a packet from the socket directly.
// It needs to be used for the first handshake packet the server receives,
// so we do't buffer the SSL negotiation packet. As a shortcut, only
// packets smaller than MaxPacketSize can be read here.
// This function usually shouldn't be used - use readEphemeralPacket.
func (c *Conn) readEphemeralPacketDirect() ([]byte, error) {
	if c.currentEphemeralPolicy != ephemeralUnused {
		panic(vterrors.Errorf(vtrpcpb.Code_INTERNAL, "readEphemeralPacketDirect: unexpected currentEphemeralPolicy: %v", c.currentEphemeralPolicy))
	}

	var r io.Reader = c.conn

	length, err := c.readHeaderFrom(r)
	if err != nil {
		return nil, err
	}

	c.currentEphemeralPolicy = ephemeralRead
	if length == 0 {
		// This can be caused by the packet after a packet of
		// exactly size MaxPacketSize.
		return nil, nil
	}

	if length < MaxPacketSize {
		c.currentEphemeralBuffer = bufPool.Get(length)
		if _, err := io.ReadFull(r, *c.currentEphemeralBuffer); err != nil {
			return nil, vterrors.Wrapf(err, "io.ReadFull(packet body of length %v) failed", length)
		}
		return *c.currentEphemeralBuffer, nil
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "readEphemeralPacketDirect doesn't support more than one packet")
}

// recycleReadPacket recycles the read packet. It needs to be called
// after readEphemeralPacket was called.
func (c *Conn) recycleReadPacket() {
	if c.currentEphemeralPolicy != ephemeralRead {
		// Programming error.
		panic(vterrors.Errorf(vtrpcpb.Code_INTERNAL, "trying to call recycleReadPacket while currentEphemeralPolicy is %d", c.currentEphemeralPolicy))
	}
	if c.currentEphemeralBuffer != nil {
		// We are using the pool, put the buffer back in.
		bufPool.Put(c.currentEphemeralBuffer)
		c.currentEphemeralBuffer = nil
	}
	c.currentEphemeralPolicy = ephemeralUnused
}

// readOnePacket reads a single packet into a newly allocated buffer.
func (c *Conn) readOnePacket() ([]byte, error) {
	r := c.getReader()
	length, err := c.readHeaderFrom(r)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		// This can be caused by the packet after a packet of
		// exactly size MaxPacketSize.
		return nil, nil
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, vterrors.Wrapf(err, "io.ReadFull(packet body of length %v) failed", length)
	}
	return data, nil
}

// readPacket reads a packet from the underlying connection.
// It re-assembles packets that span more than one message.
// This method returns a generic error, not a SQLError.
func (c *Conn) readPacket() ([]byte, error) {
	// Optimize for a single packet case.
	data, err := c.readOnePacket()
	if err != nil {
		return nil, err
	}

	// This is a single packet.
	if len(data) < MaxPacketSize {
		return data, nil
	}

	// There is more than one packet, read them all.
	for {
		next, err := c.readOnePacket()
		if err != nil {
			return nil, err
		}

		if len(next) == 0 {
			// Again, the packet after a packet of exactly size MaxPacketSize.
			break
		}

		data = append(data, next...)
		if len(next) < MaxPacketSize {
			break
		}
	}

	return data, nil
}

// ReadPacket reads a packet from the underlying connection.
// it is the public API version, that returns a SQLError.
// The memory for the packet is always allocated, and it is owned by the caller
// after this function returns.
func (c *Conn) ReadPacket() ([]byte, error) {
	result, err := c.readPacket()
	if err != nil {
		return nil, sqlerror.NewSQLErrorf(sqlerror.CRServerLost, sqlerror.SSUnknownSQLState, "%v", err)
	}
	return result, err
}

// writePacket writes a packet, possibly cutting it into multiple
// chunks.  Note this is not very efficient, as the client probably
// has to build the []byte and that makes a memory copy.
// Try to use startEphemeralPacketWithHeader/writeEphemeralPacket instead.
//
// This method returns a generic error, not a SQLError.
func (c *Conn) writePacket(data []byte) error {
	index := 0
	dataLength := len(data) - packetHeaderSize

	var w io.Writer

	c.bufMu.Lock()
	if c.bufferedWriter != nil {
		w = c.bufferedWriter
		defer func() {
			c.startFlushTimer()
			c.bufMu.Unlock()
		}()
	} else {
		c.bufMu.Unlock()
		w = c.conn
	}

	var header [packetHeaderSize]byte
	for {
		// toBeSent is capped to MaxPacketSize.
		toBeSent := dataLength
		if toBeSent > MaxPacketSize {
			toBeSent = MaxPacketSize
		}

		// save the first 4 bytes of the payload, we will overwrite them with the
		// header below
		copy(header[0:packetHeaderSize], data[index:index+packetHeaderSize])

		// Compute and write the header.
		data[index] = byte(toBeSent)
		data[index+1] = byte(toBeSent >> 8)
		data[index+2] = byte(toBeSent >> 16)
		data[index+3] = c.sequence

		// Write the body.
		if n, err := w.Write(data[index : index+toBeSent+packetHeaderSize]); err != nil {
			return vterrors.Wrapf(err, "Write(packet) failed")
		} else if n != (toBeSent + packetHeaderSize) {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Write(packet) returned a short write: %v < %v", n, (toBeSent + packetHeaderSize))
		}

		// restore the first 4 bytes once the network send is done
		copy(data[index:index+packetHeaderSize], header[0:packetHeaderSize])

		// Update our state.
		c.sequence++
		dataLength -= toBeSent
		if dataLength == 0 {
			if toBeSent == MaxPacketSize {
				// The packet we just sent had exactly
				// MaxPacketSize size, we need to
				// sent a zero-size packet too.
				header[0] = 0
				header[1] = 0
				header[2] = 0
				header[3] = c.sequence
				if n, err := w.Write(header[:]); err != nil {
					return vterrors.Wrapf(err, "Write(empty header) failed")
				} else if n != packetHeaderSize {
					return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Write(empty header) returned a short write: %v < 4", n)
				}
				c.sequence++
			}
			return nil
		}
		index += toBeSent
	}
}

func (c *Conn) startEphemeralPacketWithHeader(length int) ([]byte, int) {
	if c.currentEphemeralPolicy != ephemeralUnused {
		panic("startEphemeralPacketWithHeader cannot be used while a packet is already started.")
	}

	c.currentEphemeralPolicy = ephemeralWrite
	// get buffer from pool or it'll be allocated if length is too big
	c.currentEphemeralBuffer = bufPool.Get(length + packetHeaderSize)
	return *c.currentEphemeralBuffer, packetHeaderSize
}

// writeEphemeralPacket writes the packet that was allocated by
// startEphemeralPacketWithHeader.
func (c *Conn) writeEphemeralPacket() error {
	defer c.recycleWritePacket()

	switch c.currentEphemeralPolicy {
	case ephemeralWrite:
		if err := c.writePacket(*c.currentEphemeralBuffer); err != nil {
			return vterrors.Wrapf(err, "conn %v", c.ID())
		}
	case ephemeralUnused, ephemeralRead:
		// Programming error.
		panic(vterrors.Errorf(vtrpcpb.Code_INTERNAL, "conn %v: trying to call writeEphemeralPacket while currentEphemeralPolicy is %v", c.ID(), c.currentEphemeralPolicy))
	}

	return nil
}

// recycleWritePacket recycles the write packet. It needs to be called
// after writeEphemeralPacket was called.
func (c *Conn) recycleWritePacket() {
	if c.currentEphemeralPolicy != ephemeralWrite {
		// Programming error.
		panic(vterrors.Errorf(vtrpcpb.Code_INTERNAL, "trying to call recycleWritePacket while currentEphemeralPolicy is %d", c.currentEphemeralPolicy))
	}
	// Release our reference so the buffer can be gced
	bufPool.Put(c.currentEphemeralBuffer)
	c.currentEphemeralBuffer = nil
	c.currentEphemeralPolicy = ephemeralUnused
}

// writeComQuit writes a Quit message for the server, to indicate we
// want to close the connection.
// Client -> Server.
// Returns SQLError(CRServerGone) if it can't.
func (c *Conn) writeComQuit() error {
	// This is a new command, need to reset the sequence.
	c.sequence = 0

	data, pos := c.startEphemeralPacketWithHeader(1)
	data[pos] = ComQuit
	if err := c.writeEphemeralPacket(); err != nil {
		return sqlerror.NewSQLError(sqlerror.CRServerGone, sqlerror.SSUnknownSQLState, err.Error())
	}
	return nil
}

// RemoteAddr returns the underlying socket RemoteAddr().
func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// ID returns the MySQL connection ID for this connection.
func (c *Conn) ID() int64 {
	return int64(c.ConnectionID)
}

// Ident returns a useful identification string for error logging
func (c *Conn) String() string {
	return fmt.Sprintf("client %v (%s)", c.ConnectionID, c.RemoteAddr().String())
}

// Close closes the connection. It can be called from a different go
// routine to interrupt the current connection.
func (c *Conn) Close() {
	if c.closed.CompareAndSwap(false, true) {
		c.conn.Close()
	}
}

// IsClosed returns true if this connection was ever closed by the
// Close() method.  Note if the other side closes the connection, but
// Close() wasn't called, this will return false.
func (c *Conn) IsClosed() bool {
	return c.closed.Load()
}

//
// Packet writing methods, for generic packets.
//

// writeOKPacket writes an OK packet.
// Server -> Client.
// This method returns a generic error, not a SQLError.
func (c *Conn) writeOKPacket(packetOk *PacketOK) error {
	return c.writeOKPacketWithHeader(packetOk, OKPacket)
}

// writeOKPacketWithEOFHeader writes an OK packet with an EOF header.
// This is used at the end of a result set if
// CapabilityClientDeprecateEOF is set.
// Server -> Client.
// This method returns a generic error, not a SQLError.
func (c *Conn) writeOKPacketWithEOFHeader(packetOk *PacketOK) error {
	return c.writeOKPacketWithHeader(packetOk, EOFPacket)
}

// writeOKPacketWithHeader writes an OK packet with an EOF header.
// This is used at the end of a result set if
// CapabilityClientDeprecateEOF is set.
// Server -> Client.
// This method returns a generic error, not a SQLError.
func (c *Conn) writeOKPacketWithHeader(packetOk *PacketOK, headerType byte) error {
	length := 1 + // OKPacket
		lenEncIntSize(packetOk.affectedRows) +
		lenEncIntSize(packetOk.lastInsertID)
	// assuming CapabilityClientProtocol41
	length += 4 // status_flags + warnings

	hasSessionTrack := c.Capabilities&CapabilityClientSessionTrack == CapabilityClientSessionTrack
	hasGtidData := hasSessionTrack && packetOk.statusFlags&ServerSessionStateChanged == ServerSessionStateChanged

	var gtidData []byte

	if hasSessionTrack {
		length += lenEncStringSize(packetOk.info) // info
		if hasGtidData {
			gtidData = encGtidData(packetOk.sessionStateData)
			length += len(gtidData)
		}
	} else {
		length += len(packetOk.info) // info
	}

	bytes, pos := c.startEphemeralPacketWithHeader(length)
	data := &coder{data: bytes, pos: pos}
	data.writeByte(headerType) // header - OK or EOF
	data.writeLenEncInt(packetOk.affectedRows)
	data.writeLenEncInt(packetOk.lastInsertID)
	data.writeUint16(packetOk.statusFlags)
	data.writeUint16(packetOk.warnings)
	if hasSessionTrack {
		data.writeLenEncString(packetOk.info)
		if hasGtidData {
			data.writeEOFBytes(gtidData)
		}
	} else {
		data.writeEOFString(packetOk.info)
	}
	return c.writeEphemeralPacket()
}

func (c *Conn) WriteErrorAndLog(format string, args ...interface{}) bool {
	return c.writeErrorAndLog(sqlerror.ERUnknownComError, sqlerror.SSNetError, format, args...)
}

func (c *Conn) writeErrorAndLog(errorCode sqlerror.ErrorCode, sqlState string, format string, args ...any) bool {
	if err := c.writeErrorPacket(errorCode, sqlState, format, args...); err != nil {
		log.Errorf("Error writing error to %s: %v", c, err)
		return false
	}
	return true
}

func (c *Conn) writeErrorPacketFromErrorAndLog(err error) bool {
	werr := c.writeErrorPacketFromError(err)
	if werr != nil {
		log.Errorf("Error writing error to %s: %v", c, werr)
		return false
	}
	return true
}

// writeErrorPacket writes an error packet.
// Server -> Client.
// This method returns a generic error, not a SQLError.
func (c *Conn) writeErrorPacket(errorCode sqlerror.ErrorCode, sqlState string, format string, args ...any) error {
	errorMessage := fmt.Sprintf(format, args...)
	length := 1 + 2 + 1 + 5 + len(errorMessage)
	data, pos := c.startEphemeralPacketWithHeader(length)
	pos = writeByte(data, pos, ErrPacket)
	pos = writeUint16(data, pos, uint16(errorCode))
	pos = writeByte(data, pos, '#')
	if sqlState == "" {
		sqlState = sqlerror.SSUnknownSQLState
	}
	if len(sqlState) != 5 {
		panic("sqlState has to be 5 characters long")
	}
	pos = writeEOFString(data, pos, sqlState)
	_ = writeEOFString(data, pos, errorMessage)

	return c.writeEphemeralPacket()
}

// writeErrorPacketFromError writes an error packet, from a regular error.
// See writeErrorPacket for other info.
func (c *Conn) writeErrorPacketFromError(err error) error {
	if se, ok := err.(*sqlerror.SQLError); ok {
		return c.writeErrorPacket(se.Num, se.State, "%v", se.Message)
	}

	return c.writeErrorPacket(sqlerror.ERUnknownError, sqlerror.SSUnknownSQLState, "unknown error: %v", err)
}

// writeEOFPacket writes an EOF packet, through the buffer, and
// doesn't flush (as it is used as part of a query result).
func (c *Conn) writeEOFPacket(flags uint16, warnings uint16) error {
	length := 5
	data, pos := c.startEphemeralPacketWithHeader(length)
	pos = writeByte(data, pos, EOFPacket)
	pos = writeUint16(data, pos, warnings)
	_ = writeUint16(data, pos, flags)

	return c.writeEphemeralPacket()
}

// handleNextCommand is called in the server loop to process
// incoming packets.
func (c *Conn) handleNextCommand(handler Handler) bool {
	c.sequence = 0
	data, err := c.readEphemeralPacket()
	if err != nil {
		// Don't log EOF errors. They cause too much spam.
		if err != io.EOF && !strings.Contains(err.Error(), "use of closed network connection") {
			log.Errorf("Error reading packet from %s: %v", c, err)
		}
		return false
	}
	if len(data) == 0 {
		return false
	}
	// before continue to process the packet, check if the connection should be closed or not.
	if c.IsMarkedForClose() {
		return false
	}

	switch data[0] {
	case ComQuit:
		c.recycleReadPacket()
		return false
	case ComInitDB:
		db := c.parseComInitDB(data)
		c.recycleReadPacket()
		res := c.execQuery("use "+sqlescape.EscapeID(db), handler, false)
		return res != connErr
	case ComQuery:
		if c.multiQuery {
			return c.handleComQueryMulti(handler, data)
		}
		return c.handleComQuery(handler, data)
	case ComPing:
		return c.handleComPing()
	case ComSetOption:
		return c.handleComSetOption(data)
	case ComPrepare:
		return c.handleComPrepare(handler, data)
	case ComStmtExecute:
		return c.handleComStmtExecute(handler, data)
	case ComStmtSendLongData:
		return c.handleComStmtSendLongData(data)
	case ComStmtClose:
		stmtID, ok := c.parseComStmtClose(data)
		c.recycleReadPacket()
		if ok {
			delete(c.PrepareData, stmtID)
		}
	case ComStmtReset:
		return c.handleComStmtReset(data)
	case ComResetConnection:
		c.handleComResetConnection(handler)
		return true
	case ComFieldList:
		c.recycleReadPacket()
		if !c.writeErrorAndLog(sqlerror.ERUnknownComError, sqlerror.SSNetError, "command handling not implemented yet: %v", data[0]) {
			return false
		}
	case ComBinlogDump:
		return c.handleComBinlogDump(handler, data)
	case ComBinlogDumpGTID:
		return c.handleComBinlogDumpGTID(handler, data)
	case ComRegisterReplica:
		return c.handleComRegisterReplica(handler, data)
	default:
		log.Errorf("Got unhandled packet (default) from %s, returning error: %v", c, data)
		c.recycleReadPacket()
		if !c.writeErrorAndLog(sqlerror.ERUnknownComError, sqlerror.SSNetError, "command handling not implemented yet: %v", data[0]) {
			return false
		}
	}

	return true
}

func (c *Conn) handleComRegisterReplica(handler Handler, data []byte) (kontinue bool) {
	c.recycleReadPacket()

	replicaHost, replicaPort, replicaUser, replicaPassword, err := c.parseComRegisterReplica(data)
	if err != nil {
		log.Errorf("conn %v: parseComRegisterReplica failed: %v", c.ID(), err)
		return false
	}
	if err := handler.ComRegisterReplica(c, replicaHost, replicaPort, replicaUser, replicaPassword); err != nil {
		c.writeErrorPacketFromError(err)
		return false
	}
	if err := c.writeOKPacket(&PacketOK{}); err != nil {
		c.writeErrorPacketFromError(err)
	}
	return true
}

func (c *Conn) handleComBinlogDump(handler Handler, data []byte) (kontinue bool) {
	c.recycleReadPacket()
	kontinue = true

	c.startWriterBuffering()
	defer func() {
		if err := c.endWriterBuffering(); err != nil {
			log.Errorf("conn %v: flush() failed: %v", c.ID(), err)
			kontinue = false
		}
	}()

	logfile, binlogPos, err := c.parseComBinlogDump(data)
	if err != nil {
		log.Errorf("conn %v: parseComBinlogDumpGTID failed: %v", c.ID(), err)
		return false
	}
	if err := handler.ComBinlogDump(c, logfile, binlogPos); err != nil {
		log.Error(err.Error())
		return false
	}
	return kontinue
}

func (c *Conn) handleComBinlogDumpGTID(handler Handler, data []byte) (kontinue bool) {
	c.recycleReadPacket()
	kontinue = true

	c.startWriterBuffering()
	defer func() {
		if err := c.endWriterBuffering(); err != nil {
			log.Errorf("conn %v: flush() failed: %v", c.ID(), err)
			kontinue = false
		}
	}()

	logFile, logPos, position, err := c.parseComBinlogDumpGTID(data)
	if err != nil {
		log.Errorf("conn %v: parseComBinlogDumpGTID failed: %v", c.ID(), err)
		return false
	}
	if err := handler.ComBinlogDumpGTID(c, logFile, logPos, position.GTIDSet); err != nil {
		log.Error(err.Error())
		return false
	}
	return kontinue
}

func (c *Conn) handleComResetConnection(handler Handler) {
	// Clean up and reset the connection
	c.recycleReadPacket()
	handler.ComResetConnection(c)
	// Reset prepared statements
	c.PrepareData = make(map[uint32]*PrepareData)
	err := c.writeOKPacket(&PacketOK{})
	if err != nil {
		c.writeErrorPacketFromError(err)
	}
}

func (c *Conn) handleComStmtReset(data []byte) bool {
	stmtID, ok := c.parseComStmtReset(data)
	c.recycleReadPacket()
	if !ok {
		log.Error("Got unhandled packet from client %v, returning error: %v", c.ConnectionID, data)
		if !c.writeErrorAndLog(sqlerror.ERUnknownComError, sqlerror.SSNetError, "error handling packet: %v", data) {
			return false
		}
	}

	prepare, ok := c.PrepareData[stmtID]
	if !ok {
		log.Error("Commands were executed in an improper order from client %v, packet: %v", c.ConnectionID, data)
		if !c.writeErrorAndLog(sqlerror.CRCommandsOutOfSync, sqlerror.SSNetError, "commands were executed in an improper order: %v", data) {
			return false
		}
	}

	if prepare.BindVars != nil {
		for k := range prepare.BindVars {
			prepare.BindVars[k] = nil
		}
	}

	if err := c.writeOKPacket(&PacketOK{statusFlags: c.StatusFlags}); err != nil {
		log.Error("Error writing ComStmtReset OK packet to client %v: %v", c.ConnectionID, err)
		return false
	}
	return true
}

func (c *Conn) handleComStmtSendLongData(data []byte) bool {
	stmtID, paramID, chunk, ok := c.parseComStmtSendLongData(data)
	c.recycleReadPacket()
	if !ok {
		err := fmt.Errorf("error parsing statement send long data from client %v, returning error: %v", c.ConnectionID, data)
		return c.writeErrorPacketFromErrorAndLog(err)
	}

	prepare, ok := c.PrepareData[stmtID]
	if !ok {
		err := fmt.Errorf("got wrong statement id from client %v, statement ID(%v) is not found from record", c.ConnectionID, stmtID)
		return c.writeErrorPacketFromErrorAndLog(err)
	}

	if prepare.BindVars == nil ||
		prepare.ParamsCount == uint16(0) ||
		paramID >= prepare.ParamsCount {
		err := fmt.Errorf("invalid parameter Number from client %v, statement: %v", c.ConnectionID, prepare.PrepareStmt)
		return c.writeErrorPacketFromErrorAndLog(err)
	}

	key := fmt.Sprintf("v%d", paramID+1)
	if val, ok := prepare.BindVars[key]; ok {
		val.Value = append(val.Value, chunk...)
	} else {
		prepare.BindVars[key] = sqltypes.BytesBindVariable(chunk)
	}
	return true
}

func (c *Conn) handleComStmtExecute(handler Handler, data []byte) (kontinue bool) {
	c.startWriterBuffering()
	defer func() {
		if err := c.endWriterBuffering(); err != nil {
			log.Errorf("conn %v: flush() failed: %v", c.ID(), err)
			kontinue = false
		}
	}()
	queryStart := time.Now()
	stmtID, _, err := c.parseComStmtExecute(c.PrepareData, data)
	c.recycleReadPacket()

	if stmtID != uint32(0) {
		defer func() {
			// Allocate a new bindvar map every time since VTGate.Execute() mutates it.
			prepare := c.PrepareData[stmtID]
			prepare.BindVars = make(map[string]*querypb.BindVariable, prepare.ParamsCount)
		}()
	}

	if err != nil {
		return c.writeErrorPacketFromErrorAndLog(err)
	}

	receivedResult := false
	// sendFinished is set if the response should just be an OK packet.
	sendFinished := false
	prepare := c.PrepareData[stmtID]
	err = handler.ComStmtExecute(c, prepare, func(qr *sqltypes.Result) error {
		if sendFinished {
			// Failsafe: Unreachable if server is well-behaved.
			return io.EOF
		}

		if !receivedResult {
			receivedResult = true

			if len(qr.Fields) == 0 {
				sendFinished = true
				// We should not send any more packets after this.
				ok := PacketOK{
					affectedRows:     qr.RowsAffected,
					lastInsertID:     qr.InsertID,
					statusFlags:      c.StatusFlags,
					warnings:         0,
					info:             "",
					sessionStateData: qr.SessionStateChanges,
				}
				return c.writeOKPacket(&ok)
			}
			if err := c.writeFields(qr); err != nil {
				return err
			}
		}

		return c.writeBinaryRows(qr)
	})

	// If no field was sent, we expect an error.
	if !receivedResult {
		// This is just a failsafe. Should never happen.
		if err == nil || err == io.EOF {
			err = sqlerror.NewSQLErrorFromError(errors.New("unexpected: query ended without no results and no error"))
		}
		if !c.writeErrorPacketFromErrorAndLog(err) {
			return false
		}
	} else {
		if err != nil {
			// We can't send an error in the middle of a stream.
			// All we can do is abort the send, which will cause a 2013.
			log.Errorf("Error in the middle of a stream to %s: %v", c, err)
			return false
		}

		// Send the end packet only sendFinished is false (results were streamed).
		// In this case the affectedRows and lastInsertID are always 0 since it
		// was a read operation.
		if !sendFinished {
			if err := c.writeEndResult(false, 0, 0, handler.WarningCount(c)); err != nil {
				log.Errorf("Error writing result to %s: %v", c, err)
				return false
			}
		}
	}

	timings.Record(queryTimingKey, queryStart)
	return true
}

func (c *Conn) handleComPrepare(handler Handler, data []byte) (kontinue bool) {
	c.startWriterBuffering()
	defer func() {
		if err := c.endWriterBuffering(); err != nil {
			log.Errorf("conn %v: flush() failed: %v", c.ID(), err)
			kontinue = false
		}
	}()

	query := c.parseComPrepare(data)
	c.recycleReadPacket()

	if c.Capabilities&CapabilityClientMultiStatements != 0 {
		queries, err := handler.Env().Parser().SplitStatementToPieces(query)
		if err != nil {
			log.Errorf("Conn %v: Error splitting query: %v", c, err)
			return c.writeErrorPacketFromErrorAndLog(err)
		}
		if len(queries) != 1 {
			log.Errorf("Conn %v: can not prepare multiple statements", c)
			return c.writeErrorPacketFromErrorAndLog(err)
		}
		query = queries[0]
	}

	fld, paramsCount, err := handler.ComPrepare(c, query)
	if err != nil {
		return c.writeErrorPacketFromErrorAndLog(err)
	}

	// Populate PrepareData
	c.StatementID++
	prepare := &PrepareData{
		StatementID: c.StatementID,
		PrepareStmt: query,
		ParamsCount: paramsCount,
		ParamsType:  make([]int32, paramsCount),
		BindVars:    make(map[string]*querypb.BindVariable, paramsCount),
	}
	c.PrepareData[c.StatementID] = prepare

	if err := c.writePrepare(fld, prepare); err != nil {
		log.Error("Error writing prepare data to client %v: %v", c.ConnectionID, err)
		return false
	}
	return true
}

func (c *Conn) handleComSetOption(data []byte) bool {
	operation, ok := c.parseComSetOption(data)
	c.recycleReadPacket()
	if ok {
		switch operation {
		case 0:
			c.Capabilities |= CapabilityClientMultiStatements
		case 1:
			c.Capabilities &^= CapabilityClientMultiStatements
		default:
			log.Errorf("Got unhandled packet (ComSetOption default) from client %v, returning error: %v", c.ConnectionID, data)
			if !c.writeErrorAndLog(sqlerror.ERUnknownComError, sqlerror.SSNetError, "error handling packet: %v", data) {
				return false
			}
		}
		if err := c.writeEndResult(false, 0, 0, 0); err != nil {
			log.Errorf("Error writeEndResult error %v ", err)
			return false
		}
	} else {
		log.Errorf("Got unhandled packet (ComSetOption else) from client %v, returning error: %v", c.ConnectionID, data)
		if !c.writeErrorAndLog(sqlerror.ERUnknownComError, sqlerror.SSNetError, "error handling packet: %v", data) {
			return false
		}
	}
	return true
}

func (c *Conn) handleComPing() bool {
	c.recycleReadPacket()
	// Return error if listener was shut down and OK otherwise
	if c.listener.shutdown.Load() {
		if !c.writeErrorAndLog(sqlerror.ERServerShutdown, sqlerror.SSNetError, "Server shutdown in progress") {
			return false
		}
	} else {
		if err := c.writeOKPacket(&PacketOK{statusFlags: c.StatusFlags}); err != nil {
			log.Errorf("Error writing ComPing result to %s: %v", c, err)
			return false
		}
	}
	return true
}

// handleComQueryMulti is a newer version of handleComQuery that uses
// the StreamExecuteMulti and ExecuteMulti RPC calls to push the splitting of statements
// down to Vtgate.
func (c *Conn) handleComQueryMulti(handler Handler, data []byte) (kontinue bool) {
	c.startWriterBuffering()
	defer func() {
		if err := c.endWriterBuffering(); err != nil {
			log.Errorf("conn %v: flush() failed: %v", c.ID(), err)
			kontinue = false
		}
	}()

	queryStart := time.Now()
	query := c.parseComQuery(data)
	c.recycleReadPacket()

	res := c.execQueryMulti(query, handler)
	if res != execSuccess {
		return res != connErr
	}

	timings.Record(queryTimingKey, queryStart)
	return true
}

// execQueryMulti is a newer version of execQuery that uses
// the StreamExecuteMulti and ExecuteMulti RPC calls to push the splitting of statements
// down to Vtgate.
func (c *Conn) execQueryMulti(query string, handler Handler) execResult {
	// needsEndPacket signifies whether we have need to send the last packet to the client
	// for a given query. This is used to determine whether we should send an
	// end packet after the query is done or not. Initially we don't need to send an end packet
	// so we initialize this value to false.
	needsEndPacket := false
	callbackCalled := false
	var res = execSuccess

	err := handler.ComQueryMulti(c, query, func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error {
		callbackCalled = true
		flag := c.StatusFlags
		if more {
			flag |= ServerMoreResultsExists
		}

		// firstPacket tells us that this is the start of a new query result.
		// If we haven't sent a last packet yet, we should send the end result packet.
		if firstPacket && needsEndPacket {
			if err := c.writeEndResult(true, 0, 0, handler.WarningCount(c)); err != nil {
				log.Errorf("Error writing result to %s: %v", c, err)
				return err
			}
		}

		// We receive execution errors in a query as part of the QueryResponse.
		// We check for those errors and send a error packet. If we are unable
		// to send the error packet, then there is a connection error too.
		if qr.QueryError != nil {
			res = execErr
			if !c.writeErrorPacketFromErrorAndLog(qr.QueryError) {
				res = connErr
			}
			return nil
		}

		if firstPacket {
			// The first packet signifies the start of a new query result.
			// So we reset the needsEndPacket variable to signify we haven't sent the last
			// packet for this query.
			needsEndPacket = true
			if len(qr.QueryResult.Fields) == 0 {

				// A successful callback with no fields means that this was a
				// DML or other write-only operation.
				//
				// We should not send any more packets after this, but make sure
				// to extract the affected rows and last insert id from the result
				// struct here since clients expect it.
				ok := PacketOK{
					affectedRows:     qr.QueryResult.RowsAffected,
					lastInsertID:     qr.QueryResult.InsertID,
					statusFlags:      flag,
					warnings:         handler.WarningCount(c),
					info:             "",
					sessionStateData: qr.QueryResult.SessionStateChanges,
				}
				needsEndPacket = false
				return c.writeOKPacket(&ok)
			}

			if err := c.writeFields(qr.QueryResult); err != nil {
				return err
			}
		}

		return c.writeRows(qr.QueryResult)
	})

	// If callback was not called, we expect an error.
	// It is possible that we don't get a callback if some condition checks
	// fail before the query starts execution. In this case, we need to write some
	// error back.
	if !callbackCalled {
		// This is just a failsafe. Should never happen.
		if err == nil || err == io.EOF {
			err = sqlerror.NewSQLErrorFromError(errors.New("unexpected: query ended without no results and no error"))
		}
		if !c.writeErrorPacketFromErrorAndLog(err) {
			return connErr
		}
		return execErr
	}

	if res != execSuccess {
		// We failed during the stream itself.
		return res
	}

	if err != nil {
		// We can't send an error in the middle of a stream.
		// All we can do is abort the send, which will cause a 2013.
		log.Errorf("Error in the middle of a stream to %s: %v", c, err)
		return connErr
	}

	// If we haven't sent the final packet for the last query, we should send that too.
	if needsEndPacket {
		if err := c.writeEndResult(false, 0, 0, handler.WarningCount(c)); err != nil {
			log.Errorf("Error writing result to %s: %v", c, err)
			return connErr
		}
	}

	return execSuccess
}

var errEmptyStatement = sqlerror.NewSQLError(sqlerror.EREmptyQuery, sqlerror.SSClientError, "Query was empty")

func (c *Conn) handleComQuery(handler Handler, data []byte) (kontinue bool) {
	c.startWriterBuffering()
	defer func() {
		if err := c.endWriterBuffering(); err != nil {
			log.Errorf("conn %v: flush() failed: %v", c.ID(), err)
			kontinue = false
		}
	}()

	queryStart := time.Now()
	query := c.parseComQuery(data)
	c.recycleReadPacket()

	var queries []string
	var err error
	if c.Capabilities&CapabilityClientMultiStatements != 0 {
		queries, err = handler.Env().Parser().SplitStatementToPieces(query)
		if err != nil {
			log.Errorf("Conn %v: Error splitting query: %v", c, err)
			return c.writeErrorPacketFromErrorAndLog(err)
		}
	} else {
		queries = []string{query}
	}

	if len(queries) == 0 {
		return c.writeErrorPacketFromErrorAndLog(errEmptyStatement)
	}

	for index, sql := range queries {
		more := false
		if index != len(queries)-1 {
			more = true
		}
		res := c.execQuery(sql, handler, more)
		if res != execSuccess {
			return res != connErr
		}
	}

	timings.Record(queryTimingKey, queryStart)
	return true
}

func (c *Conn) execQuery(query string, handler Handler, more bool) execResult {
	callbackCalled := false
	// sendFinished is set if the response should just be an OK packet.
	sendFinished := false

	err := handler.ComQuery(c, query, func(qr *sqltypes.Result) error {
		flag := c.StatusFlags
		if more {
			flag |= ServerMoreResultsExists
		}
		if sendFinished {
			// Failsafe: Unreachable if server is well-behaved.
			return io.EOF
		}

		if !callbackCalled {
			callbackCalled = true

			if len(qr.Fields) == 0 {
				sendFinished = true

				// A successful callback with no fields means that this was a
				// DML or other write-only operation.
				//
				// We should not send any more packets after this, but make sure
				// to extract the affected rows and last insert id from the result
				// struct here since clients expect it.
				ok := PacketOK{
					affectedRows:     qr.RowsAffected,
					lastInsertID:     qr.InsertID,
					statusFlags:      flag,
					warnings:         handler.WarningCount(c),
					info:             "",
					sessionStateData: qr.SessionStateChanges,
				}
				return c.writeOKPacket(&ok)
			}
			if err := c.writeFields(qr); err != nil {
				return err
			}
		}

		return c.writeRows(qr)
	})

	// If callback was not called, we expect an error.
	if !callbackCalled {
		// This is just a failsafe. Should never happen.
		if err == nil || err == io.EOF {
			err = sqlerror.NewSQLErrorFromError(errors.New("unexpected: query ended without no results and no error"))
		}
		if !c.writeErrorPacketFromErrorAndLog(err) {
			return connErr
		}
		return execErr
	}
	if err != nil {
		// We can't send an error in the middle of a stream.
		// All we can do is abort the send, which will cause a 2013.
		log.Errorf("Error in the middle of a stream to %s: %v", c, err)
		return connErr
	}

	// Send the end packet only sendFinished is false (results were streamed).
	// In this case the affectedRows and lastInsertID are always 0 since it
	// was a read operation.
	if !sendFinished {
		if err := c.writeEndResult(more, 0, 0, handler.WarningCount(c)); err != nil {
			log.Errorf("Error writing result to %s: %v", c, err)
			return connErr
		}
	}

	return execSuccess
}

//
// Packet parsing methods, for generic packets.
//

// isEOFPacket determines whether a data packet is an EOF. In case the client capabilities
// do not have DEPRECATE_EOF set, DO NOT blindly compare the first byte of a packet to EOFPacket
// as you might do for other packet types, as 0xfe is overloaded as a first byte.

// In case that DEPRECATE_EOF is set, we have really an OK packet which is always maximum a single
// packet and not multiple, but otherwise 0xfe definitely indicates it is an EOF.
//
// Per https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html, a packet starting with 0xfe
// but having length >= 9 (on top of 4 byte header)  without DEPRECATE_EOF set is not a true EOF but
// a LengthEncodedInteger (typically preceding a LengthEncodedString). Thus, all EOF checks without
// DEPRECATE_EOF must validate the payload size before exiting.
//
// More docs here:
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_response_packets.html
func (c *Conn) isEOFPacket(data []byte) bool {
	if data[0] != EOFPacket {
		return false
	}
	if c.Capabilities&CapabilityClientDeprecateEOF == 0 {
		return len(data) < 9
	}
	return len(data) < MaxPacketSize
}

// parseEOFPacket returns the warning count and a boolean to indicate if there
// are more results to receive.
//
// Note: This is only valid on actual EOF packets and not on OK packets with the EOF
// type code set, i.e. should not be used if ClientDeprecateEOF is set.
func parseEOFPacket(data []byte) (warnings uint16, statusFlags uint16, err error) {
	// The warning count is in position 2 & 3
	warnings, _, _ = readUint16(data, 1)

	// The status flag is in position 4 & 5
	statusFlags, _, ok := readUint16(data, 3)
	if !ok {
		return 0, 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid EOF packet statusFlags: %v", data)
	}
	return warnings, statusFlags, nil
}

// PacketOK contains the ok packet details
type PacketOK struct {
	affectedRows uint64
	lastInsertID uint64
	statusFlags  uint16
	warnings     uint16
	info         string

	// at the moment, we only store GTID information in this field
	sessionStateData string
}

func (c *Conn) parseOKPacket(packetOK *PacketOK, in []byte) error {
	data := &coder{
		data: in,
		pos:  1, // We already read the type.
	}

	// Affected rows.
	affectedRows, ok := data.readLenEncInt()
	if !ok {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid OK packet affectedRows: %v", data.data)
	}
	packetOK.affectedRows = affectedRows

	// Last Insert ID.
	lastInsertID, ok := data.readLenEncInt()
	if !ok {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid OK packet lastInsertID: %v", data.data)
	}
	packetOK.lastInsertID = lastInsertID

	// Status flags.
	statusFlags, ok := data.readUint16()
	if !ok {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid OK packet statusFlags: %v", data.data)
	}
	packetOK.statusFlags = statusFlags

	// assuming CapabilityClientProtocol41
	// Warnings.
	warnings, ok := data.readUint16()
	if !ok {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid OK packet warnings: %v", data.data)
	}
	packetOK.warnings = warnings

	// info
	info, _ := data.readLenEncInfo()
	if c.enableQueryInfo {
		packetOK.info = info
	}

	if c.Capabilities&uint32(CapabilityClientSessionTrack) == CapabilityClientSessionTrack {
		// session tracking
		if statusFlags&ServerSessionStateChanged == ServerSessionStateChanged {
			length, ok := data.readLenEncInt()
			if !ok || length == 0 {
				// In case we have no more data or a zero length string, there's no additional information so
				// we can return the packet.
				return nil
			}

			// Alright, now we need to read each sub packet from the session state change.
			for {
				sscType, ok := data.readByte()
				if !ok {
					// We're done, there's no more session state parts in the packet.
					break
				}
				sessionLen, ok := data.readLenEncInt()
				if !ok {
					return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid OK packet session state change length for type %v", sscType)
				}

				if sscType != SessionTrackGtids {
					// Still need to increase the pointer here to indicate we're consuming
					// but otherwise ignoring the rest of this packet
					data.pos = data.pos + int(sessionLen)
					continue
				}

				// read (and ignore for now) the GTIDS encoding specification code: 1 byte
				_, ok = data.readByte()
				if !ok {
					return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid OK packet gtids type: %v", data.data)
				}

				gtids, ok := data.readLenEncString()
				if !ok {
					return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid OK packet gtids: %v", data.data)
				}
				packetOK.sessionStateData = gtids
			}
		}
	}

	return nil
}

// isErrorPacket determines whether or not the packet is an error packet. Mostly here for
// consistency with isEOFPacket
func isErrorPacket(data []byte) bool {
	return data[0] == ErrPacket
}

// ParseErrorPacket parses the error packet and returns a SQLError.
func ParseErrorPacket(data []byte) error {
	// We already read the type.
	pos := 1

	// Error code is 2 bytes.
	code, pos, ok := readUint16(data, pos)
	if !ok {
		return sqlerror.NewSQLErrorf(sqlerror.CRUnknownError, sqlerror.SSUnknownSQLState, "invalid error packet code: %v", data)
	}

	// '#' marker of the SQL state is 1 byte. Ignored.
	pos++

	// SQL state is 5 bytes
	sqlState, pos, ok := readBytes(data, pos, 5)
	if !ok {
		return sqlerror.NewSQLErrorf(sqlerror.CRUnknownError, sqlerror.SSUnknownSQLState, "invalid error packet sqlState: %v", data)
	}

	// Human readable error message is the rest.
	msg := string(data[pos:])

	return sqlerror.NewSQLErrorf(sqlerror.ErrorCode(code), string(sqlState), "%v", msg)
}

// GetTLSClientCerts gets TLS certificates.
func (c *Conn) GetTLSClientCerts() []*x509.Certificate {
	if tlsConn, ok := c.conn.(*tls.Conn); ok {
		return tlsConn.ConnectionState().PeerCertificates
	}
	return nil
}

// TLSEnabled returns true if this connection is using TLS.
func (c *Conn) TLSEnabled() bool {
	return c.Capabilities&CapabilityClientSSL > 0
}

// IsUnixSocket returns true if the server connection is over a Unix socket.
func (c *Conn) IsUnixSocket() bool {
	_, ok := c.listener.listener.(*net.UnixListener)
	return ok
}

// IsClientUnixSocket returns true if the client connection is over a Unix socket with the server.
func (c *Conn) IsClientUnixSocket() bool {
	_, ok := c.conn.(*net.UnixConn)
	return ok
}

// GetRawConn returns the raw net.Conn for nefarious purposes.
func (c *Conn) GetRawConn() net.Conn {
	return c.conn
}

// CancelCtx aborts an existing running query
func (c *Conn) CancelCtx() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cancel != nil {
		c.cancel()
	}
}

// UpdateCancelCtx updates the cancel function on the connection.
func (c *Conn) UpdateCancelCtx(cancel context.CancelFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancel = cancel
}

// MarkForClose marks the connection for close.
func (c *Conn) MarkForClose() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closing = true
}

// IsMarkedForClose return true if the connection should be closed.
func (c *Conn) IsMarkedForClose() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closing
}

func (c *Conn) IsShuttingDown() bool {
	return c.listener.shutdown.Load()
}
