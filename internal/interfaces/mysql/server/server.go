// Package server provides the MySQL protocol server implementation.
package server

import (
	"context"
	"crypto/sha1"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
	"go.uber.org/zap"

	"github.com/turtacn/staravail/internal/application/interfaces"
	"github.com/turtacn/staravail/internal/infrastructure/config"
	logging "github.com/turtacn/staravail/internal/common/logger"
	"github.com/turtacn/staravail/internal/common/metrics"
	- "github.com/turtacn/staravail/internal/common/types/model"
	"github.com/turtacn/staravail/internal/domain/query"
	"github.com/turtacn/staravail/internal/domain/write"
	"github.com/turtacn/staravail/internal/interfaces/mysql/protocol"
)


// MySQLServer defines the interface for a MySQL protocol server.
type MySQLServer interface {
	// Start starts the MySQL server.
	Start() error

	// Stop gracefully stops the MySQL server.
	Stop() error

	// GetStatus returns the current status of the MySQL server.
	GetStatus() ServerStatus

	// GetMetrics returns server metrics.
	GetMetrics() ServerMetrics
}

// ServerStatus represents the current status of the MySQL server.
type ServerStatus struct {
	// IsRunning indicates whether the server is currently running.
	IsRunning bool

	// StartTime is the time when the server was started.
	StartTime time.Time

	// Address is the address the server is listening on.
	Address string

	// ActiveConnections is the number of currently active connections.
	ActiveConnections int

	// TotalConnections is the total number of connections since startup.
	TotalConnections int64

	// LastError contains the last error encountered, if any.
	LastError string
}

// ServerMetrics contains server performance metrics.
type ServerMetrics struct {
	// ConnectionsTotal is the total number of connections since startup.
	ConnectionsTotal int64

	// ActiveConnections is the current number of active connections.
	ActiveConnections int

	// QueriesTotal is the total number of queries processed.
	QueriesTotal int64

	// QueriesPerSecond is the average number of queries per second.
	QueriesPerSecond float64

	// ErrorsTotal is the total number of errors encountered.
	ErrorsTotal int64

	// AvgQueryTime is the average query execution time in milliseconds.
	AvgQueryTime float64

	// AvgConnLifetime is the average connection lifetime in seconds.
	AvgConnLifetime float64
}

// mysqlServer implements the MySQLServer interface.
type mysqlServer struct {
	// Server configuration
	config *config.MySQLServerConfig

	// Dependencies
	queryService   interfaces.QueryService
	writeService   interfaces.WriteService
	authService    interfaces.AuthService
	healthService  interfaces.HealthService
	metricsCollector *metrics.MetricsCollector

	// Server state
	listener         net.Listener
	tlsConfig        *tls.Config
	running          atomic.Bool
	startTime        time.Time
	stopChan         chan struct{}
	wg               sync.WaitGroup
	activeConnections int64
	totalConnections  int64

	// Connection management
	connections      map[uint32]*mysqlConnection
	connectionsMutex sync.RWMutex
	nextConnID       uint32

	// Query parsing
	sqlParser *parser.Parser

	// Logger
	logger *zap.Logger
}

// NewMySQLServer creates a new MySQL protocol server.
func NewMySQLServer(
	config *config.MySQLServerConfig,
	serviceDeps interfaces.ServiceDependencies,
	metricsCollector *metrics.MetricsCollector,
) (MySQLServer, error) {
	if config == nil {
		return nil, fmt.Errorf("MySQL server config is required")
	}

	if serviceDeps.QueryService == nil {
		return nil, fmt.Errorf("query service is required")
	}

	if serviceDeps.WriteService == nil {
		return nil, fmt.Errorf("write service is required")
	}

	if serviceDeps.AuthService == nil {
		return nil, fmt.Errorf("auth service is required")
	}

	// Initialize TLS config if needed
	var tlsConfig *tls.Config
	if config.TLSEnabled {
		var err error
		tlsConfig, err = createTLSConfig(config)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config: %w", err)
		}
	}

	// Create SQL parser
	sqlParser := parser.New()

	return &mysqlServer{
		config:            config,
		queryService:      serviceDeps.QueryService,
		writeService:      serviceDeps.WriteService,
		authService:       serviceDeps.AuthService,
		healthService:     serviceDeps.HealthService,
		metricsCollector:  metricsCollector,
		tlsConfig:         tlsConfig,
		stopChan:          make(chan struct{}),
		connections:       make(map[uint32]*mysqlConnection),
		sqlParser:         sqlParser,
		logger:            logging.GetLogger().Named("mysql.server"),
	}, nil
}

// Start starts the MySQL server.
func (s *mysqlServer) Start() error {
	if s.running.Load() {
		return fmt.Errorf("MySQL server is already running")
	}

	// Create listener
	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to create listener on %s: %w", addr, err)
	}

	s.listener = listener
	s.startTime = time.Now()
	s.running.Store(true)

	s.logger.Info("MySQL server started",
		zap.String("address", addr),
		zap.Bool("tls_enabled", s.config.TLSEnabled),
		zap.Int("max_connections", s.config.MaxConnections),
	)

	// Start acceptor loop
	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Stop gracefully stops the MySQL server.
func (s *mysqlServer) Stop() error {
	if !s.running.Load() {
		return nil
	}

	// Signal stop to all goroutines
	close(s.stopChan)

	// Close listener to stop accepting new connections
	if s.listener != nil {
		s.listener.Close()
	}

	// Close all active connections
	s.closeAllConnections()

	// Wait for all goroutines to finish
	s.wg.Wait()

	s.running.Store(false)
	s.logger.Info("MySQL server stopped")

	return nil
}

// GetStatus returns the current status of the MySQL server.
func (s *mysqlServer) GetStatus() ServerStatus {
	status := ServerStatus{
		IsRunning:         s.running.Load(),
		StartTime:         s.startTime,
		ActiveConnections: int(atomic.LoadInt64(&s.activeConnections)),
		TotalConnections:  atomic.LoadInt64(&s.totalConnections),
	}

	if s.listener != nil {
		status.Address = s.listener.Addr().String()
	}

	return status
}

// GetMetrics returns server metrics.
func (s *mysqlServer) GetMetrics() ServerMetrics {
	// Calculate metrics
	uptime := time.Since(s.startTime).Seconds()
	queriesTotal := s.metricsCollector.MySQLQueriesTotal.Value()
	errorsTotal := s.metricsCollector.MySQLErrorsTotal.Value()

	// Calculate queries per second
	var qps float64
	if uptime > 0 {
		qps = float64(queriesTotal) / uptime
	}

	// Get average metrics
	avgQueryTime := s.metricsCollector.MySQLQueryTime.Mean() * 1000 // Convert to milliseconds
	avgConnLifetime := s.metricsCollector.MySQLConnectionLifetime.Mean()

	return ServerMetrics{
		ConnectionsTotal:  atomic.LoadInt64(&s.totalConnections),
		ActiveConnections: int(atomic.LoadInt64(&s.activeConnections)),
		QueriesTotal:      queriesTotal,
		QueriesPerSecond:  qps,
		ErrorsTotal:       errorsTotal,
		AvgQueryTime:      avgQueryTime,
		AvgConnLifetime:   avgConnLifetime,
	}
}

// acceptLoop accepts incoming client connections.
func (s *mysqlServer) acceptLoop() {
	defer s.wg.Done()

	for {
		// Check if server is stopping
		select {
		case <-s.stopChan:
			return
		default:
			// Continue accepting connections
		}

		// Check if connection limit is reached
		if s.config.MaxConnections > 0 && atomic.LoadInt64(&s.activeConnections) >= int64(s.config.MaxConnections) {
			s.logger.Warn("Connection limit reached, waiting for connections to close",
				zap.Int("max_connections", s.config.MaxConnections),
			)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Accept connection with timeout
		s.listener.(*net.TCPListener).SetDeadline(time.Now().Add(1 * time.Second))
		clientConn, err := s.listener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// This is just a timeout, continue
				continue
			}

			// Check if server is stopping (listener closed)
			select {
			case <-s.stopChan:
				return
			default:
				s.logger.Error("Error accepting connection", zap.Error(err))
				continue
			}
		}

		// Increment connection counters
		connID := atomic.AddUint32(&s.nextConnID, 1)
		atomic.AddInt64(&s.activeConnections, 1)
		atomic.AddInt64(&s.totalConnections, 1)

		// Update metrics
		s.metricsCollector.MySQLConnectionsTotal.Inc()
		s.metricsCollector.MySQLActiveConnections.Set(float64(atomic.LoadInt64(&s.activeConnections)))

		// Create connection handler
		conn := newMySQLConnection(connID, clientConn, s)

		// Store connection
		s.connectionsMutex.Lock()
		s.connections[connID] = conn
		s.connectionsMutex.Unlock()

		// Handle connection in a new goroutine
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.removeConnection(connID)

			// Start connection handler
			conn.handle()
		}()
	}
}

// removeConnection removes a connection from the active connections list.
func (s *mysqlServer) removeConnection(connID uint32) {
	// Remove from connection map
	s.connectionsMutex.Lock()
	delete(s.connections, connID)
	s.connectionsMutex.Unlock()

	// Decrement active connections counter
	atomic.AddInt64(&s.activeConnections, -1)

	// Update metrics
	s.metricsCollector.MySQLActiveConnections.Set(float64(atomic.LoadInt64(&s.activeConnections)))
}

// closeAllConnections closes all active connections.
func (s *mysqlServer) closeAllConnections() {
	s.connectionsMutex.Lock()
	defer s.connectionsMutex.Unlock()

	for _, conn := range s.connections {
		conn.close()
	}
}

// createTLSConfig creates a TLS configuration for the server.
func createTLSConfig(config *config.MySQLServerConfig) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(config.TLSCertFile, config.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// mysqlConnection represents a client connection.
type mysqlConnection struct {
	id              uint32
	conn            net.Conn
	server          *mysqlServer
	protocol        *protocol.MySQLProtocol
	user            *auth.UserInfo
	database        string
	authenticated   bool
	capabilities    uint32
	status          uint16
	lastQueryTime   time.Time
	createTime      time.Time
	closed          bool
	mutex           sync.Mutex
	serverVersion   string
	charset         uint8
	collation       uint8
	txnStarted      bool
	stmtPrepareData map[uint32]*preparedStatement
	logger          *zap.Logger
}

// preparedStatement represents a prepared statement.
type preparedStatement struct {
	id         uint32
	sql        string
	params     int
	columns    int
	paramTypes []byte
	ast        ast.StmtNode
}

// newMySQLConnection creates a new MySQL connection handler.
func newMySQLConnection(id uint32, conn net.Conn, server *mysqlServer) *mysqlConnection {
	return &mysqlConnection{
		id:              id,
		conn:            conn,
		server:          server,
		protocol:        protocol.NewMySQLProtocol(conn),
		createTime:      time.Now(),
		charset:         33, // utf8 COLLATE utf8_general_ci
		collation:       33,
		serverVersion:   "5.7.30-StarRocksProxy",
		status:          protocol.ServerStatusAutocommit,
		stmtPrepareData: make(map[uint32]*preparedStatement),
		logger:          server.logger.With(zap.Uint32("conn_id", id)),
	}
}

// handle processes the client connection.
func (c *mysqlConnection) handle() {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("Panic in connection handler", zap.Any("error", r))
		}
	}()

	// Set connection timeout
	if c.server.config.ConnectionTimeout > 0 {
		c.conn.SetDeadline(time.Now().Add(time.Duration(c.server.config.ConnectionTimeout) * time.Second))
	}

	// Start connection lifecycle timer
	startTime := time.Now()

	// Handle connection
	c.logger.Debug("New connection established",
		zap.String("remote_addr", c.conn.RemoteAddr().String()),
	)

	// Send handshake packet
	err := c.sendHandshake()
	if err != nil {
		c.logger.Error("Failed to send handshake", zap.Error(err))
		c.close()
		return
	}

	// Read client authentication response
	err = c.handleAuth()
	if err != nil {
		c.logger.Error("Authentication failed", zap.Error(err))
		c.close()
		return
	}

	// Authentication successful
	c.authenticated = true

	// Update metrics
	c.server.metricsCollector.MySQLAuthSuccessTotal.Inc()

	// Command loop
	for {
		// Check if connection is closed
		if c.closed {
			break
		}

		// Reset connection timeout for each command
		if c.server.config.CommandTimeout > 0 {
			c.conn.SetDeadline(time.Now().Add(time.Duration(c.server.config.CommandTimeout) * time.Second))
		}

		// Read command packet
		cmd, data, err := c.protocol.ReadCommandPacket()
		if err != nil {
			if err == protocol.ErrConnectionClosed {
				c.logger.Debug("Client closed connection")
			} else {
				c.logger.Error("Failed to read command", zap.Error(err))
			}
			break
		}

		// Update last query time
		c.lastQueryTime = time.Now()

		// Handle command
		if err := c.handleCommand(cmd, data); err != nil {
			if err == protocol.ErrConnectionClosed {
				break
			}
			c.logger.Error("Error handling command",
				zap.Uint8("cmd", uint8(cmd)),
				zap.Error(err),
			)
			// Send error to client
			c.protocol.WriteError(1105, err.Error())
		}
	}

	// Record connection lifetime
	connectionLifetime := time.Since(startTime).Seconds()
	c.server.metricsCollector.MySQLConnectionLifetime.Observe(connectionLifetime)

	c.close()
}

// sendHandshake sends the initial handshake packet to the client.
func (c *mysqlConnection) sendHandshake() error {
	// Generate auth plugin data (random salt)
	authPluginData := protocol.GenerateRandomBytes(20)

	// Create handshake packet
	handshake := &protocol.HandshakePacket{
		ProtocolVersion:    10,
		ServerVersion:      c.serverVersion,
		ConnectionID:       c.id,
		AuthPluginData:     authPluginData,
		CapabilityFlags:    protocol.DefaultServerCapabilities,
		CharacterSet:       c.charset,
		StatusFlags:        c.status,
		AuthPluginName:     "mysql_native_password",
	}

	// Send handshake packet
	return c.protocol.WriteHandshake(handshake)
}

// handleAuth processes client authentication.
func (c *mysqlConnection) handleAuth() error {
	// Read client handshake response
	authResp, err := c.protocol.ReadHandshakeResponse()
	if err != nil {
		return fmt.Errorf("read handshake response: %w", err)
	}

	// Save client capabilities
	c.capabilities = authResp.CapabilityFlags

	// Set client-requested database if provided
	if len(authResp.Database) > 0 {
		c.database = authResp.Database
	}

	// Set client-requested charset and collation
	if authResp.CharacterSet > 0 {
		c.charset = authResp.CharacterSet
		// Set corresponding collation (simplified)
		c.collation = authResp.CharacterSet
	}

	// Authenticate user
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create authentication request
	authRequest := &auth.AuthRequest{
		Username:   authResp.Username,
		Password:   string(authResp.AuthResponse),
		Database:   c.database,
		ClientHost: c.conn.RemoteAddr().String(),
		ClientApp:  "mysql-client",
	}

	// Authenticate with auth service
	userInfo, err := c.server.authService.Authenticate(ctx, authRequest)
	if err != nil {
		// Authentication failed
		c.server.metricsCollector.MySQLAuthFailureTotal.Inc()
		c.protocol.WriteError(protocol.ErrAccessDenied, fmt.Sprintf("Access denied for user '%s'", authResp.Username))
		return fmt.Errorf("authentication failed: %w", err)
	}

	// Store authenticated user
	c.user = userInfo

	// Send OK packet
	err = c.protocol.WriteOK(0, 0, c.status, 0)
	if err != nil {
		return fmt.Errorf("write auth OK: %w", err)
	}

	c.logger.Info("Client authenticated",
		zap.String("user", c.user.Username),
		zap.String("database", c.database),
		zap.String("remote_addr", c.conn.RemoteAddr().String()),
	)

	return nil
}

// handleCommand processes a client command.
func (c *mysqlConnection) handleCommand(cmd protocol.CommandType, data []byte) error {
	// Update command count metrics
	c.server.metricsCollector.MySQLCommandsTotal.WithLabelValues(cmd.String()).Inc()

	switch cmd {
	case protocol.ComQuit:
		return protocol.ErrConnectionClosed

	case protocol.ComInitDB:
		return c.handleInitDB(string(data))

	case protocol.ComQuery:
		return c.handleQuery(string(data))

	case protocol.ComPing:
		return c.protocol.WriteOK(0, 0, c.status, 0)

	case protocol.ComStmtPrepare:
		return c.handleStmtPrepare(string(data))

	case protocol.ComStmtExecute:
		return c.handleStmtExecute(data)

	case protocol.ComStmtClose:
		return c.handleStmtClose(data)

	case protocol.ComStmtReset:
		return c.handleStmtReset(data)

	case protocol.ComFieldList:
		return c.handleFieldList(string(data))

	case protocol.ComSetOption:
		return c.handleSetOption(data)

	default:
		c.logger.Warn("Unsupported command",
			zap.Uint8("cmd", uint8(cmd)),
			zap.Binary("data", data),
		)
		return c.protocol.WriteError(protocol.ErrUnknownCommand, fmt.Sprintf("Unsupported command %d", cmd))
	}
}

// handleInitDB handles COM_INIT_DB command.
func (c *mysqlConnection) handleInitDB(dbName string) error {
	if dbName == "" {
		return c.protocol.WriteError(protocol.ErrNoDb, "No database selected")
	}

	// Check if user has access to this database
	if !c.user.HasDatabaseAccess(dbName) {
		return c.protocol.WriteError(protocol.ErrDbAccessDenied, fmt.Sprintf("Access denied for user '%s' to database '%s'", c.user.Username, dbName))
	}

	// Set current database
	c.database = dbName

	c.logger.Debug("Database changed", zap.String("database", dbName))

	// Send OK packet
	return c.protocol.WriteOK(0, 0, c.status, 0)
}

// handleQuery handles COM_QUERY command.
func (c *mysqlConnection) handleQuery(query string) error {
	if query == "" {
		return c.protocol.WriteOK(0, 0, c.status, 0)
	}

	// Check if database is selected
	if c.database == "" && !isUseDBQuery(query) && !isShowDatabasesQuery(query) {
		return c.protocol.WriteError(protocol.ErrNoDb, "No database selected")
	}

	// Log query
	c.logger.Debug("Executing query",
		zap.String("query", query),
		zap.String("database", c.database),
		zap.String("user", c.user.Username),
	)

	// Start query execution timer
	startTime := time.Now()

	// Parse query to determine if it's a read or write operation
	isRead, err := c.isReadQuery(query)
	if err != nil {
		c.logger.Error("Error parsing query", zap.Error(err))
		return c.protocol.WriteError(protocol.ErrParse, fmt.Sprintf("Error parsing query: %v", err))
	}

	// Handle special queries
	if handleSpecialQuery(c, query) {
		return nil
	}

	if isRead {
		// Execute read query
		err = c.executeReadQuery(query)
	} else {
		// Execute write query
		err = c.executeWriteQuery(query)
	}

	// Calculate query execution time
	executionTime := time.Since(startTime)

	// Record metrics
	c.server.metricsCollector.MySQLQueryTime.Observe(executionTime.Seconds())
	if err != nil {
		c.server.metricsCollector.MySQLErrorsTotal.Inc()
	}

	// Log query execution
	if err != nil {
		c.logger.Error("Query execution failed",
			zap.String("query", query),
			zap.Duration("execution_time", executionTime),
			zap.Error(err),
		)
	} else {
		c.logger.Debug("Query executed successfully",
			zap.String("query", query),
			zap.Duration("execution_time", executionTime),
		)
	}

	return err
}

// executeReadQuery executes a read-only query.
func (c *mysqlConnection) executeReadQuery(sql string) error {
	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.server.config.QueryTimeout)*time.Second)
	defer cancel()

	// Create query request
	req := &query.QueryRequest{
		SQL:       sql,
		Database:  c.database,
		User:      c.user.Username,
		RequestID: fmt.Sprintf("mysql-%d-%d", c.id, time.Now().UnixNano()),
		Options:   &query.QueryOptions{
			MaxRows: c.server.config.MaxRowsPerResult,
		},
	}

	// Execute query
	result, err := c.server.queryService.ExecuteQuery(ctx, req)
	if err != nil {
		// Convert error to MySQL error
		mysqlErr := convertToMySQLError(err)
		return c.protocol.WriteError(mysqlErr.Code, mysqlErr.Message)
	}

	// Write result to client
	return c.writeQueryResult(result)
}

// executeWriteQuery executes a write query.
func (c *mysqlConnection) executeWriteQuery(sql string) error {
	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.server.config.WriteTimeout)*time.Second)
	defer cancel()

	// Create write request
	req := &write.WriteRequest{
		SQL:       sql,
		Database:  c.database,
		User:      c.user.Username,
		RequestID: fmt.Sprintf("mysql-%d-%d", c.id, time.Now().UnixNano()),
		Options:   &write.WriteOptions{
			AutoCommit: (c.status & protocol.ServerStatusAutocommit) != 0,
		},
	}

	// Execute write
	result, err := c.server.writeService.ExecuteWrite(ctx, req)
	if err != nil {
		// Convert error to MySQL error
		mysqlErr := convertToMySQLError(err)
		return c.protocol.WriteError(mysqlErr.Code, mysqlErr.Message)
	}

	// Send OK packet with affected rows
	return c.protocol.WriteOK(uint64(result.AffectedRows), uint64(result.LastInsertID), c.status, 0)
}

// writeQueryResult writes a query result to the client.
func (c *mysqlConnection) writeQueryResult(result *query.QueryResult) error {
	if result == nil {
		return c.protocol.WriteOK(0, 0, c.status, 0)
	}

	// Check if result is empty (no columns)
	if len(result.Columns) == 0 {
		// For queries like SHOW VARIABLES that return empty result
		return c.protocol.WriteOK(0, 0, c.status, 0)
	}

	// Convert columns to MySQL format
	fields := make([]*protocol.Field, len(result.Columns))
	for i, col := range result.Columns {
		fields[i] = convertColumnToField(col)
	}

	// Start result set
	if err := c.protocol.WriteResultSetHeader(len(fields)); err != nil {
		return err
	}

	// Write column definitions
	for _, field := range fields {
		if err := c.protocol.WriteColumnDefinition(field); err != nil {
			return err
		}
	}

	// Write EOF packet after columns
	if err := c.protocol.WriteEOF(c.status); err != nil {
		return err
	}

	// Write rows
	for _, row := range result.Rows {
		if err := c.protocol.WriteTextRow(row); err != nil {
			return err
		}
	}

	// Write EOF packet after rows
	return c.protocol.WriteEOF(c.status)
}

// handleStmtPrepare handles COM_STMT_PREPARE command.
func (c *mysqlConnection) handleStmtPrepare(query string) error {
	// Start execution timer
	startTime := time.Now()

	// Parse query to check syntax
	stmt, err := c.server.sqlParser.ParseOneStmt(query, "", "")
	if err != nil {
		c.logger.Error("Error parsing prepared statement",
			zap.String("query", query),
			zap.Error(err),
		)
		return c.protocol.WriteError(protocol.ErrParse, fmt.Sprintf("Error parsing prepared statement: %v", err))
	}

	// Generate statement ID
	stmtID := uint32(len(c.stmtPrepareData) + 1)

	// Count parameters (? placeholders) - simplified
	paramCount := countParams(query)

	// Count result columns - simplified approximation
	columnCount := 0
	if selectStmt, ok := stmt.(*ast.SelectStmt); ok && selectStmt.Fields != nil {
		columnCount = len(selectStmt.Fields.Fields)
	}

	// Create prepared statement object
	ps := &preparedStatement{
		id:      stmtID,
		sql:     query,
		params:  paramCount,
		columns: columnCount,
		ast:     stmt,
	}

	// Store prepared statement
	c.stmtPrepareData[stmtID] = ps

	// Write prepare response
	if err := c.protocol.WritePrepareOK(stmtID, uint16(paramCount), uint16(columnCount)); err != nil {
		return err
	}

	// Send parameter definitions if needed
	if paramCount > 0 {
		for i := 0; i < paramCount; i++ {
			// Use default parameter type (VARCHAR)
			param := &protocol.Field{
				Database: "",
				Table:    "",
				Name:     fmt.Sprintf("param%d", i),
				Type:     protocol.FieldTypeVarString,
				Flags:    protocol.FieldFlagNone,
			}
			if err := c.protocol.WriteColumnDefinition(param); err != nil {
				return err
			}
		}

		// Write EOF after parameters
		if err := c.protocol.WriteEOF(c.status); err != nil {
			return err
		}
	}

	// Send column definitions if needed
	if columnCount > 0 {
		// In a real implementation, we would determine column definitions
		// This is simplified to just send placeholder columns
		for i := 0; i < columnCount; i++ {
			column := &protocol.Field{
				Database: "",
				Table:    "",
				Name:     fmt.Sprintf("col%d", i),
				Type:     protocol.FieldTypeVarString,
				Flags:    protocol.FieldFlagNone,
			}
			if err := c.protocol.WriteColumnDefinition(column); err != nil {
				return err
			}
		}

		// Write EOF after columns
		if err := c.protocol.WriteEOF(c.status); err != nil {
			return err
		}
	}

	// Record metrics
	executionTime := time.Since(startTime)
	c.server.metricsCollector.MySQLPrepareTime.Observe(executionTime.Seconds())
	c.server.metricsCollector.MySQLPreparesTotal.Inc()

	c.logger.Debug("Prepared statement",
		zap.Uint32("stmt_id", stmtID),
		zap.String("query", query),
		zap.Int("params", paramCount),
		zap.Int("columns", columnCount),
	)

	return nil
}

// handleStmtExecute handles COM_STMT_EXECUTE command.
func (c *mysqlConnection) handleStmtExecute(data []byte) error {
	// Start execution timer
	startTime := time.Now()

	// Read statement ID (first 4 bytes)
	if len(data) < 4 {
		return c.protocol.WriteError(protocol.ErrMalformPacket, "Invalid statement execute packet")
	}
	stmtID := protocol.ReadUint32(data, 0)

	// Find prepared statement
	ps, ok := c.stmtPrepareData[stmtID]
	if !ok {
		return c.protocol.WriteError(protocol.ErrUnknownStmtHandler, fmt.Sprintf("Unknown prepared statement ID %d", stmtID))
	}

	// Parse execute packet to extract parameters
	// This is a simplified implementation
	params, err := c.protocol.ReadExecuteParameters(data, ps.params)
	if err != nil {
		return c.protocol.WriteError(protocol.ErrMalformPacket, fmt.Sprintf("Error reading parameters: %v", err))
	}

	// Determine if read or write query
	isRead, err := c.isReadQuery(ps.sql)
	if err != nil {
		return c.protocol.WriteError(protocol.ErrParse, fmt.Sprintf("Error parsing query: %v", err))
	}

	// Replace placeholders with parameter values
	// In a real implementation, you would use prepared statements properly
	// This is simplified to just replace ? with values
	execSQL := ps.sql
	for _, param := range params {
		// Simple string replacement (not secure for production!)
		execSQL = strings.Replace(execSQL, "?", fmt.Sprintf("'%s'", param), 1)
	}

	// Execute query
	var execErr error
	if isRead {
		execErr = c.executeReadQuery(execSQL)
	} else {
		execErr = c.executeWriteQuery(execSQL)
	}

	// Record metrics
	executionTime := time.Since(startTime)
	c.server.metricsCollector.MySQLExecuteTime.Observe(executionTime.Seconds())
	c.server.metricsCollector.MySQLExecutesTotal.Inc()

	if execErr != nil {
		c.logger.Error("Execute prepared statement failed",
			zap.Uint32("stmt_id", stmtID),
			zap.String("query", execSQL),
			zap.Error(execErr),
		)
	} else {
		c.logger.Debug("Executed prepared statement",
			zap.Uint32("stmt_id", stmtID),
			zap.String("query", execSQL),
			zap.Duration("execution_time", executionTime),
		)
	}

	return execErr
}

// handleStmtClose handles COM_STMT_CLOSE command.
func (c *mysqlConnection) handleStmtClose(data []byte) error {
	if len(data) < 4 {
		return nil // Silently ignore invalid packets
	}

	stmtID := protocol.ReadUint32(data, 0)

	// Remove prepared statement from cache
	delete(c.stmtPrepareData, stmtID)

	c.logger.Debug("Closed prepared statement", zap.Uint32("stmt_id", stmtID))

	// No response needed for COM_STMT_CLOSE
	return nil
}

// handleStmtReset handles COM_STMT_RESET command.
func (c *mysqlConnection) handleStmtReset(data []byte) error {
	if len(data) < 4 {
		return c.protocol.WriteError(protocol.ErrMalformPacket, "Invalid statement reset packet")
	}

	stmtID := protocol.ReadUint32(data, 0)

	// Find prepared statement
	_, ok := c.stmtPrepareData[stmtID]
	if !ok {
		return c.protocol.WriteError(protocol.ErrUnknownStmtHandler, fmt.Sprintf("Unknown prepared statement ID %d", stmtID))
	}

	// Reset statement (in a real implementation, this would clear parameter data)

	c.logger.Debug("Reset prepared statement", zap.Uint32("stmt_id", stmtID))

	// Send OK packet
	return c.protocol.WriteOK(0, 0, c.status, 0)
}

// handleFieldList handles COM_FIELD_LIST command.
func (c *mysqlConnection) handleFieldList(data string) error {
	// Parse table name (data before null terminator)
	parts := strings.Split(data, "\x00")
	if len(parts) < 1 {
		return c.protocol.WriteError(protocol.ErrMalformPacket, "Invalid field list packet")
	}

	tableName := parts[0]

	// Check if database is selected
	if c.database == "" {
		return c.protocol.WriteError(protocol.ErrNoDb, "No database selected")
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.server.config.QueryTimeout)*time.Second)
	defer cancel()

	// Create metadata request
	req := &query.MetadataRequest{
		Database:  c.database,
		Table:     tableName,
		User:      c.user.Username,
		RequestID: fmt.Sprintf("mysql-%d-%d", c.id, time.Now().UnixNano()),
	}

	// Get table columns
	columns, err := c.server.queryService.GetTableColumns(ctx, req)
	if err != nil {
		mysqlErr := convertToMySQLError(err)
		return c.protocol.WriteError(mysqlErr.Code, mysqlErr.Message)
	}

	// Write column definitions
	for _, col := range columns {
		field := convertColumnToField(col)
		if err := c.protocol.WriteColumnDefinition(field); err != nil {
			return err
		}
	}

	// Write EOF packet
	return c.protocol.WriteEOF(c.status)
}

// handleSetOption handles COM_SET_OPTION command.
func (c *mysqlConnection) handleSetOption(data []byte) error {
	if len(data) < 2 {
		return c.protocol.WriteError(protocol.ErrMalformPacket, "Invalid set option packet")
	}

	option := protocol.ReadUint16(data, 0)

	switch option {
	case protocol.OptionMultiStatements:
		// Enable or disable multi-statements
		// In this implementation, we'll just acknowledge
		c.logger.Debug("Set option", zap.Uint16("option", option))

	default:
		return c.protocol.WriteError(protocol.ErrUnknown, fmt.Sprintf("Unknown option: %d", option))
	}

	// Send OK packet
	return c.protocol.WriteOK(0, 0, c.status, 0)
}

// close closes the connection.
func (c *mysqlConnection) close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.closed {
		return
	}

	c.closed = true
	c.conn.Close()

	c.logger.Debug("Connection closed",
		zap.String("remote_addr", c.conn.RemoteAddr().String()),
		zap.Duration("lifetime", time.Since(c.createTime)),
	)
}

// Helper functions

// isReadQuery determines if a query is read-only.
func (c *mysqlConnection) isReadQuery(sql string) (bool, error) {
	// Parse SQL statement
	stmt, err := c.server.sqlParser.ParseOneStmt(sql, "", "")
	if err != nil {
		return false, err
	}

	// Check statement type
	switch stmt.(type) {
	case *ast.SelectStmt, *ast.ShowStmt, *ast.ExplainStmt:
		return true, nil
	default:
		return false, nil
	}
}

// convertColumnToField converts a query column to a MySQL protocol field.
func convertColumnToField(col *query.Column) *protocol.Field {
	field := &protocol.Field{
		Database: col.Database,
		Table:    col.Table,
		OrgTable: col.OrgTable,
		Name:     col.Name,
		OrgName:  col.OrgName,
		Charset:  33, // utf8 default
		Length:   uint32(col.Length),
		Type:     mapDataType(col.Type),
		Flags:    mapColumnFlags(col),
		Decimals: uint8(col.Decimals),
	}

	return field
}

// mapDataType maps StarRocks data types to MySQL protocol field types.
func mapDataType(typeName string) uint8 {
	// Map StarRocks data types to MySQL data types
	switch strings.ToUpper(typeName) {
	case "BOOLEAN":
		return protocol.FieldTypeTiny
	case "TINYINT":
		return protocol.FieldTypeTiny
	case "SMALLINT":
		return protocol.FieldTypeShort
	case "INT", "INTEGER":
		return protocol.FieldTypeLong
	case "BIGINT":
		return protocol.FieldTypeLongLong
	case "LARGEINT":
		return protocol.FieldTypeLongLong
	case "FLOAT":
		return protocol.FieldTypeFloat
	case "DOUBLE":
		return protocol.FieldTypeDouble
	case "DECIMAL":
		return protocol.FieldTypeNewDecimal
	case "DATE":
		return protocol.FieldTypeDate
	case "DATETIME":
		return protocol.FieldTypeDateTime
	case "CHAR", "VARCHAR":
		return protocol.FieldTypeVarString
	case "STRING", "TEXT":
		return protocol.FieldTypeBlob
	default:
		return protocol.FieldTypeVarString
	}
}

// mapColumnFlags maps column attributes to MySQL protocol field flags.
func mapColumnFlags(col *query.Column) uint16 {
	var flags uint16

	if col.NotNull {
		flags |= protocol.FieldFlagNotNull
	}

	if col.PrimaryKey {
		flags |= protocol.FieldFlagPrimaryKey
	}

	if col.UniqueKey {
		flags |= protocol.FieldFlagUniqueKey
	}

	if col.AutoIncrement {
		flags |= protocol.FieldFlagAutoIncrement
	}

	if col.Binary {
		flags |= protocol.FieldFlagBinary
	}

	return flags
}

// convertToMySQLError converts domain errors to MySQL protocol errors.
func convertToMySQLError(err error) *protocol.MySQLError {
	// Default error
	mysqlErr := &protocol.MySQLError{
		Code:    protocol.ErrUnknown,
		Message: err.Error(),
	}

	// Map domain errors to MySQL errors
	switch {
	case errors.Is(err, query.ErrSyntaxError):
		mysqlErr.Code = protocol.ErrParse
	case errors.Is(err, query.ErrDatabaseNotFound):
		mysqlErr.Code = protocol.ErrBadDb
	case errors.Is(err, query.ErrTableNotFound):
		mysqlErr.Code = protocol.ErrNoSuchTable
	case errors.Is(err, query.ErrColumnNotFound):
		mysqlErr.Code = protocol.ErrBadField
	case errors.Is(err, query.ErrPermissionDenied):
		mysqlErr.Code = protocol.ErrAccessDenied
	case errors.Is(err, query.ErrTimeout):
		mysqlErr.Code = protocol.ErrQueryTimeout
	case errors.Is(err, write.ErrDuplicateKey):
		mysqlErr.Code = protocol.ErrDupKey
	case errors.Is(err, write.ErrConstraintViolation):
		mysqlErr.Code = protocol.ErrRowIsReferenced
	case errors.Is(err, write.ErrSyntaxError):
		mysqlErr.Code = protocol.ErrParse
	}

	return mysqlErr
}

// countParams counts the number of parameter placeholders (?) in a query.
func countParams(query string) int {
	return strings.Count(query, "?")
}

// isUseDBQuery checks if the query is a USE DATABASE statement.
func isUseDBQuery(query string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "USE ")
}

// isShowDatabasesQuery checks if the query is a SHOW DATABASES statement.
func isShowDatabasesQuery(query string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "SHOW DATABASES")
}

// handleSpecialQuery handles special queries like SHOW VARIABLES.
// Returns true if the query was handled, false otherwise.
func handleSpecialQuery(c *mysqlConnection, query string) bool {
	query = strings.TrimSpace(query)
	upperQuery := strings.ToUpper(query)

	switch {
	case strings.HasPrefix(upperQuery, "SHOW VARIABLES"):
		return handleShowVariables(c, query)

	case strings.HasPrefix(upperQuery, "SELECT @@VERSION"):
		return handleSelectVersion(c)

	case strings.HasPrefix(upperQuery, "SELECT DATABASE()"):
		return handleSelectDatabase(c)

	default:
		return false
	}
}

// handleShowVariables handles SHOW VARIABLES queries.
func handleShowVariables(c *mysqlConnection, query string) bool {
	// Create columns for result
	cols := []*query.Column{
		{Name: "Variable_name", Type: "VARCHAR"},
		{Name: "Value", Type: "VARCHAR"},
	}

	// Create result
	result := &query.QueryResult{
		Columns: cols,
		Rows: [][]interface{}{
			{"version", c.serverVersion},
			{"version_comment", "StarRocks Proxy Server"},
			{"character_set_client", "utf8"},
			{"character_set_connection", "utf8"},
			{"character_set_results", "utf8"},
			{"character_set_server", "utf8"},
			{"autocommit", (c.status & protocol.ServerStatusAutocommit) != 0 ? "ON" : "OFF"},
			{"max_allowed_packet", "16777216"},
			{"interactive_timeout", "28800"},
			{"wait_timeout", "28800"},
		},
	}

	// Write result to client
	err := c.writeQueryResult(result)
	return err == nil
}

// handleSelectVersion handles SELECT @@VERSION queries.
func handleSelectVersion(c *mysqlConnection) bool {
	// Create columns for result
	cols := []*query.Column{
		{Name: "@@version", Type: "VARCHAR"},
	}

	// Create result
	result := &query.QueryResult{
		Columns: cols,
		Rows: [][]interface{}{
			{c.serverVersion},
		},
	}

	// Write result to client
	err := c.writeQueryResult(result)
	return err == nil
}

// handleSelectDatabase handles SELECT DATABASE() queries.
func handleSelectDatabase(c *mysqlConnection) bool {
	// Create columns for result
	cols := []*query.Column{
		{Name: "DATABASE()", Type: "VARCHAR"},
	}

	// Create result
	result := &query.QueryResult{
		Columns: cols,
		Rows: [][]interface{}{
			{c.database},
		},
	}

	// Write result to client
	err := c.writeQueryResult(result)
	return err == nil
}
//Personal.AI order the ending
