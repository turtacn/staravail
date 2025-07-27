// Package starrocks provides a client for interacting with StarRocks cluster.
package starrocks

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"github.com/turtacn/staravail/internal/common/config"
	"github.com/turtacn/staravail/internal/common/logging"
	"github.com/turtacn/staravail/internal/common/metrics"
)

// QueryResult represents the result of a query execution
type QueryResult struct {
	// Columns contains the column names
	Columns []string
	// Types contains the column types
	Types []string
	// Rows contains the result rows
	Rows [][]interface{}
	// RowsAffected contains the number of rows affected
	RowsAffected int64
	// LastInsertID contains the last insert ID
	LastInsertID int64
	// ExecutionTime contains the execution time
	ExecutionTime time.Duration
}

// BackendStatus represents the status of a BE node
type BackendStatus struct {
	// ID is the ID of the BE node
	ID int64 `json:"id"`
	// Host is the hostname of the BE node
	Host string `json:"host"`
	// Port is the port of the BE node
	Port int `json:"be_port"`
	// HTTPPort is the HTTP port of the BE node
	HTTPPort int `json:"http_port"`
	// BrpcPort is the BRPC port of the BE node
	BrpcPort int `json:"brpc_port"`
	// AliveStatus indicates if the BE node is alive
	AliveStatus bool `json:"alive"`
	// SystemDecommissioned indicates if the BE node is decommissioned
	SystemDecommissioned bool `json:"SystemDecommissioned"`
	// ClusterDecommissioned indicates if the BE node is decommissioned in the cluster
	ClusterDecommissioned bool `json:"ClusterDecommissioned"`
	// TabletNum is the number of tablets on the BE node
	TabletNum int64 `json:"tabletNum"`
	// DataUsedCapacity is the used capacity of the BE node
	DataUsedCapacity int64 `json:"dataUsedCapacity"`
	// AvailCapacity is the available capacity of the BE node
	AvailCapacity int64 `json:"availCapacity"`
	// TotalCapacity is the total capacity of the BE node
	TotalCapacity int64 `json:"totalCapacity"`
	// UsedPct is the used percentage of the BE node
	UsedPct float64 `json:"usedPct"`
	// MaxDiskUsedPct is the maximum disk used percentage of the BE node
	MaxDiskUsedPct float64 `json:"maxDiskUsedPct"`
	// Tag is the tag of the BE node
	Tag string `json:"tag"`
	// HeartbeatFailureCounter is the heartbeat failure counter of the BE node
	HeartbeatFailureCounter int `json:"heartbeatFailureCounter"`
	// LastUpdateTime is the last update time of the BE node
	LastUpdateTime int64 `json:"lastUpdateTime"`
	// StartTime is the start time of the BE node
	StartTime int64 `json:"startTime"`
	// LastHeartbeat is the last heartbeat of the BE node
	LastHeartbeat int64 `json:"lastHeartbeat"`
	// Status is the status of the BE node
	Status string `json:"status"`
	// ErrMsg is the error message of the BE node
	ErrMsg string `json:"errMsg"`
	// Version is the version of the BE node
	Version string `json:"version"`
}

// TabletInfo represents information about a tablet
type TabletInfo struct {
	// TabletID is the ID of the tablet
	TabletID int64 `json:"tablet_id"`
	// ReplicaID is the ID of the replica
	ReplicaID int64 `json:"replica_id"`
	// BackendID is the ID of the BE node
	BackendID int64 `json:"backend_id"`
	// BackendHost is the hostname of the BE node
	BackendHost string `json:"backend_host"`
	// BackendHTTPPort is the HTTP port of the BE node
	BackendHTTPPort int `json:"backend_http_port"`
	// SchemaHash is the hash of the schema
	SchemaHash int64 `json:"schema_hash"`
	// DatabaseName is the name of the database
	DatabaseName string `json:"database_name"`
	// TableName is the name of the table
	TableName string `json:"table_name"`
	// PartitionName is the name of the partition
	PartitionName string `json:"partition_name"`
	// PartitionID is the ID of the partition
	PartitionID int64 `json:"partition_id"`
	// BucketID is the ID of the bucket
	BucketID int `json:"bucket_id"`
	// Path is the path of the tablet
	Path string `json:"path"`
	// DataSize is the data size of the tablet
	DataSize int64 `json:"data_size"`
	// RowCount is the row count of the tablet
	RowCount int64 `json:"row_count"`
	// State is the state of the tablet
	State string `json:"state"`
	// LastFailure is the last failure of the tablet
	LastFailure string `json:"last_failure"`
	// LastFailureTimestamp is the last failure timestamp of the tablet
	LastFailureTimestamp int64 `json:"last_failure_timestamp"`
	// LastSuccess is the last success of the tablet
	LastSuccess string `json:"last_success"`
	// LastSuccessTimestamp is the last success timestamp of the tablet
	LastSuccessTimestamp int64 `json:"last_success_timestamp"`
	// Version is the version of the tablet
	Version int `json:"version"`
	// VersionHash is the hash of the version
	VersionHash int64 `json:"version_hash"`
	// LastChecksum is the last checksum of the tablet
	LastChecksum int64 `json:"last_checksum"`
	// LastChecksumTimestamp is the last checksum timestamp of the tablet
	LastChecksumTimestamp int64 `json:"last_checksum_timestamp"`
	// IsConsistent is whether the tablet is consistent
	IsConsistent bool `json:"is_consistent"`
}

// StreamLoadResponse represents the response of a stream load
type StreamLoadResponse struct {
	// Status indicates if the stream load succeeded
	Status string `json:"Status"`
	// Message contains the message from the stream load
	Message string `json:"Message"`
	// Label is the label of the stream load
	Label string `json:"Label"`
	// TxnID is the transaction ID of the stream load
	TxnID int64 `json:"TxnId"`
	// LoadedRows is the number of rows loaded
	LoadedRows int64 `json:"LoadedRows"`
	// FilteredRows is the number of rows filtered
	FilteredRows int64 `json:"FilteredRows"`
	// UnselectedRows is the number of rows unselected
	UnselectedRows int64 `json:"UnselectedRows"`
	// LoadBytes is the number of bytes loaded
	LoadBytes int64 `json:"LoadBytes"`
	// LoadTimeMS is the load time in milliseconds
	LoadTimeMS int64 `json:"LoadTimeMs"`
	// BeginTxnTimeMs is the begin transaction time in milliseconds
	BeginTxnTimeMs int64 `json:"BeginTxnTimeMs"`
	// StreamLoadPutTimeMs is the stream load put time in milliseconds
	StreamLoadPutTimeMs int64 `json:"StreamLoadPutTimeMs"`
	// ReadDataTimeMs is the read data time in milliseconds
	ReadDataTimeMs int64 `json:"ReadDataTimeMs"`
	// WriteDataTimeMs is the write data time in milliseconds
	WriteDataTimeMs int64 `json:"WriteDataTimeMs"`
	// CommitAndPublishTimeMs is the commit and publish time in milliseconds
	CommitAndPublishTimeMs int64 `json:"CommitAndPublishTimeMs"`
	// ErrorURL contains the URL to the error log
	ErrorURL string `json:"ErrorURL"`
	// Tracking is the tracking info for the load
	Tracking string `json:"Tracking"`
}

// StarRocksClient defines the interface for interacting with StarRocks
type StarRocksClient interface {
	// ExecuteQuery executes a SQL query and returns the result
	ExecuteQuery(ctx context.Context, sql string) (*QueryResult, error)

	// ExecuteStreamLoad performs a stream load to import data
	ExecuteStreamLoad(ctx context.Context, database, table string, data []byte, options map[string]string) (*StreamLoadResponse, error)

	// GetBackendStatus gets the status of the BE nodes
	GetBackendStatus(ctx context.Context) ([]BackendStatus, error)

	// GetTabletDistribution gets the distribution of tablets
	GetTabletDistribution(ctx context.Context, database, table string) ([]TabletInfo, error)

	// GetTableSchema gets the schema of a table
	GetTableSchema(ctx context.Context, database, table string) (map[string]string, error)

	// GetPartitions gets the partitions of a table
	GetPartitions(ctx context.Context, database, table string) ([]string, error)

	// Ping checks if the client can connect to StarRocks
	Ping(ctx context.Context) error

	// Close closes the client
	Close() error
}

// StarRocksClientConfig represents configuration for the StarRocks client
type StarRocksClientConfig struct {
	// FEAddresses is a list of StarRocks FE addresses
	FEAddresses []string
	// User is the username for connecting to StarRocks
	User string
	// Password is the password for connecting to StarRocks
	Password string
	// MySQLPort is the MySQL port of the FE nodes
	MySQLPort int
	// HTTPPort is the HTTP port of the FE nodes
	HTTPPort int
	// MaxOpenConns is the maximum number of open connections
	MaxOpenConns int
	// MaxIdleConns is the maximum number of idle connections
	MaxIdleConns int
	// ConnMaxLifetime is the maximum lifetime of a connection
	ConnMaxLifetime time.Duration
	// QueryTimeout is the timeout for queries
	QueryTimeout time.Duration
	// RetryCount is the number of retries for requests
	RetryCount int
	// RetryWaitMin is the minimum wait time between retries
	RetryWaitMin time.Duration
	// RetryWaitMax is the maximum wait time between retries
	RetryWaitMax time.Duration
}

// StarRocksClientImpl implements the StarRocksClient interface
type StarRocksClientImpl struct {
	// config is the client configuration
	config StarRocksClientConfig
	// db is the database connection
	db *sql.DB
	// feAddresses is a list of FE addresses
	feAddresses []string
	// currentFEIndex is the index of the current FE address
	currentFEIndex int
	// httpClient is the HTTP client for API requests
	httpClient *http.Client
	// logger is used for logging
	logger logging.Logger
	// metrics is used for recording metrics
	metrics metrics.MetricsRecorder
	// mutex protects shared resources
	mutex sync.RWMutex
}

// NewStarRocksClient creates a new StarRocksClient instance
func NewStarRocksClient(
	cfg config.StarRocksClientConfig,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) (StarRocksClient, error) {
	// Validate configuration
	if len(cfg.FEAddresses) == 0 {
		return nil, errors.New("no FE addresses provided")
	}
	if cfg.User == "" {
		return nil, errors.New("no username provided")
	}

	// Create client
	client := &StarRocksClientImpl{
		config: StarRocksClientConfig{
			FEAddresses:     cfg.FEAddresses,
			User:            cfg.User,
			Password:        cfg.Password,
			MySQLPort:       cfg.MySQLPort,
			HTTPPort:        cfg.HTTPPort,
			MaxOpenConns:    cfg.MaxOpenConns,
			MaxIdleConns:    cfg.MaxIdleConns,
			ConnMaxLifetime: cfg.ConnMaxLifetime,
			QueryTimeout:    cfg.QueryTimeout,
			RetryCount:      cfg.RetryCount,
			RetryWaitMin:    cfg.RetryWaitMin,
			RetryWaitMax:    cfg.RetryWaitMax,
		},
		feAddresses:    cfg.FEAddresses,
		currentFEIndex: rand.Intn(len(cfg.FEAddresses)),
		logger:         logger,
		metrics:        metricsRecorder,
	}

	// Initialize HTTP client
	client.httpClient = &http.Client{
		Timeout: cfg.QueryTimeout,
		Transport: &http.Transport{
			MaxIdleConns:          cfg.MaxIdleConns,
			MaxConnsPerHost:       cfg.MaxOpenConns,
			IdleConnTimeout:       cfg.ConnMaxLifetime,
			ResponseHeaderTimeout: cfg.QueryTimeout,
		},
	}

	// Initialize database connection
	if err := client.initDBConnection(); err != nil {
		return nil, err
	}

	return client, nil
}

// initDBConnection initializes the database connection
func (c *StarRocksClientImpl) initDBConnection() error {
	// Get the current FE address
	c.mutex.RLock()
	feAddr := c.feAddresses[c.currentFEIndex]
	c.mutex.RUnlock()

	// Create the DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8&timeout=%s",
		c.config.User,
		c.config.Password,
		feAddr,
		c.config.MySQLPort,
		c.config.QueryTimeout.String(),
	)

	// Open the database connection
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return errors.Wrap(err, "failed to open database connection")
	}

	// Configure the connection pool
	db.SetMaxOpenConns(c.config.MaxOpenConns)
	db.SetMaxIdleConns(c.config.MaxIdleConns)
	db.SetConnMaxLifetime(c.config.ConnMaxLifetime)

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return errors.Wrap(err, "failed to ping database")
	}

	// Close the old connection if it exists
	if c.db != nil {
		c.db.Close()
	}

	// Set the new connection
	c.db = db

	c.logger.Info("Initialized database connection", "fe_address", feAddr)

	return nil
}

// getCurrentFEAddress gets the current FE address
func (c *StarRocksClientImpl) getCurrentFEAddress() string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.feAddresses[c.currentFEIndex]
}

// rotateToNextFE rotates to the next FE address
func (c *StarRocksClientImpl) rotateToNextFE() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.currentFEIndex = (c.currentFEIndex + 1) % len(c.feAddresses)
	c.metrics.CounterInc("starrocks_client_fe_rotations", nil)
	c.logger.Info("Rotated to next FE", "fe_address", c.feAddresses[c.currentFEIndex])
}

// ExecuteQuery executes a SQL query and returns the result
func (c *StarRocksClientImpl) ExecuteQuery(ctx context.Context, sql string) (*QueryResult, error) {
	// Create context with timeout if not already set
	var cancel context.CancelFunc
	if _, ok := ctx.Deadline(); !ok && c.config.QueryTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.config.QueryTimeout)
		defer cancel()
	}

	// Start time measurement
	startTime := time.Now()
	defer func() {
		// Record query time
		duration := time.Since(startTime)
		c.metrics.HistogramObserve("starrocks_client_query_time_ms", float64(duration.Milliseconds()), nil)
	}()

	// Initialize result
	result := &QueryResult{
		ExecutionTime: 0,
	}

	// Create retry backoff
	backoffConfig := backoff.NewExponentialBackOff()
	backoffConfig.InitialInterval = c.config.RetryWaitMin
	backoffConfig.MaxInterval = c.config.RetryWaitMax
	backoffConfig.MaxElapsedTime = c.config.QueryTimeout

	retryBackoff := backoff.WithContext(backoffConfig, ctx)

	// Execute with retry
	operation := func() error {
		// Check if we need to reconnect
		if c.db == nil {
			if err := c.initDBConnection(); err != nil {
				c.rotateToNextFE()
				return errors.Wrap(err, "failed to initialize database connection")
			}
		}

		// Prepare query
		rows, err := c.db.QueryContext(ctx, sql)
		if err != nil {
			// Handle connection errors by rotating to next FE
			if isConnectionError(err) {
				c.rotateToNextFE()
				if err := c.initDBConnection(); err != nil {
					return errors.Wrap(err, "failed to initialize database connection after rotation")
				}
			}
			return errors.Wrap(err, "query failed")
		}
		defer rows.Close()

		// Get column names and types
		columns, err := rows.Columns()
		if err != nil {
			return errors.Wrap(err, "failed to get columns")
		}
		result.Columns = columns

		// Get column types
		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			return errors.Wrap(err, "failed to get column types")
		}

		// Extract type names
		types := make([]string, len(columnTypes))
		for i, ct := range columnTypes {
			types[i] = ct.DatabaseTypeName()
		}
		result.Types = types

		// Collect rows
		var rows2d [][]interface{}
		for rows.Next() {
			// Create a slice of interface{} to hold the row values
			rowValues := make([]interface{}, len(columns))
			rowValuePtrs := make([]interface{}, len(columns))

			// Create pointers to each element in the row
			for i := range rowValues {
				rowValuePtrs[i] = &rowValues[i]
			}

			// Scan the row into the slice of interface{}
			if err := rows.Scan(rowValuePtrs...); err != nil {
				return errors.Wrap(err, "failed to scan row")
			}

			// Convert []byte to string for string columns
			for i, val := range rowValues {
				if b, ok := val.([]byte); ok && (types[i] == "VARCHAR" || types[i] == "CHAR" || types[i] == "TEXT") {
					rowValues[i] = string(b)
				}
			}

			rows2d = append(rows2d, rowValues)
		}

		if err := rows.Err(); err != nil {
			return errors.Wrap(err, "error iterating rows")
		}

		result.Rows = rows2d
		result.ExecutionTime = time.Since(startTime)

		// If it's a write query, get affected rows
		if isWriteQuery(sql) {
			res, err := c.db.ExecContext(ctx, sql)
			if err != nil {
				return errors.Wrap(err, "exec failed")
			}

			rowsAffected, err := res.RowsAffected()
			if err == nil {
				result.RowsAffected = rowsAffected
			}

			lastInsertID, err := res.LastInsertId()
			if err == nil {
				result.LastInsertID = lastInsertID
			}
		}

		return nil
	}

	// Execute with retry
	err := backoff.Retry(operation, retryBackoff)
	if err != nil {
		c.metrics.CounterInc("starrocks_client_query_errors", map[string]string{
			"error_type": getErrorType(err),
		})
		return nil, errors.Wrap(err, "query failed after retries")
	}

	c.metrics.CounterInc("starrocks_client_queries", map[string]string{
		"query_type": getQueryType(sql),
	})

	return result, nil
}

// ExecuteStreamLoad performs a stream load to import data
func (c *StarRocksClientImpl) ExecuteStreamLoad(ctx context.Context, database, table string, data []byte, options map[string]string) (*StreamLoadResponse, error) {
	// Create context with timeout if not already set
	var cancel context.CancelFunc
	if _, ok := ctx.Deadline(); !ok && c.config.QueryTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.config.QueryTimeout)
		defer cancel()
	}

	// Start time measurement
	startTime := time.Now()
	defer func() {
		// Record stream load time
		duration := time.Since(startTime)
		c.metrics.HistogramObserve("starrocks_client_stream_load_time_ms", float64(duration.Milliseconds()), nil)
	}()

	// Create retry backoff
	backoffConfig := backoff.NewExponentialBackOff()
	backoffConfig.InitialInterval = c.config.RetryWaitMin
	backoffConfig.MaxInterval = c.config.RetryWaitMax
	backoffConfig.MaxElapsedTime = c.config.QueryTimeout

	retryBackoff := backoff.WithContext(backoffConfig, ctx)

	// Generate a unique label for the load
	label := fmt.Sprintf("proxy_load_%s_%d", table, time.Now().UnixNano())

	// Initialize response
	var response *StreamLoadResponse

	// Execute with retry
	operation := func() error {
		// Get current FE address
		feAddr := c.getCurrentFEAddress()
		httpPort := c.config.HTTPPort

		// Build URL
		urlStr := fmt.Sprintf("http://%s:%d/api/%s/%s/_stream_load",
			feAddr, httpPort, database, table)

		// Create request
		req, err := http.NewRequestWithContext(ctx, "PUT", urlStr, bytes.NewReader(data))
		if err != nil {
			return errors.Wrap(err, "failed to create request")
		}

		// Set basic auth
		req.SetBasicAuth(c.config.User, c.config.Password)

		// Set standard headers
		req.Header.Set("Expect", "100-continue")
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("Content-Length", strconv.Itoa(len(data)))
		req.Header.Set("label", label)

		// Set optional headers from options
		for k, v := range options {
			req.Header.Set(k, v)
		}

		// Execute request
		resp, err := c.httpClient.Do(req)
		if err != nil {
			// Handle connection errors by rotating to next FE
			if isConnectionError(err) {
				c.rotateToNextFE()
			}
			return errors.Wrap(err, "failed to execute request")
		}
		defer resp.Body.Close()

		// Read response body
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.Wrap(err, "failed to read response body")
		}

		// Check response status
		if resp.StatusCode != http.StatusOK {
			return errors.Errorf("stream load failed with status %d: %s", resp.StatusCode, string(body))
		}

		// Parse response
		var streamLoadResp StreamLoadResponse
		if err := json.Unmarshal(body, &streamLoadResp); err != nil {
			return errors.Wrap(err, "failed to parse response")
		}

		// Check if load was successful
		if streamLoadResp.Status != "Success" && streamLoadResp.Status != "OK" {
			return errors.Errorf("stream load failed: %s", streamLoadResp.Message)
		}

		response = &streamLoadResp
		return nil
	}

	// Execute with retry
	err := backoff.Retry(operation, retryBackoff)
	if err != nil {
		c.metrics.CounterInc("starrocks_client_stream_load_errors", map[string]string{
			"database":   database,
			"table":      table,
			"error_type": getErrorType(err),
		})
		return nil, errors.Wrap(err, "stream load failed after retries")
	}

	// Record metrics
	c.metrics.CounterInc("starrocks_client_stream_loads", map[string]string{
		"database": database,
		"table":    table,
	})
	if response != nil {
		c.metrics.CounterAdd("starrocks_client_stream_load_rows", float64(response.LoadedRows), map[string]string{
			"database": database,
			"table":    table,
		})
		c.metrics.CounterAdd("starrocks_client_stream_load_bytes", float64(response.LoadBytes), map[string]string{
			"database": database,
			"table":    table,
		})
	}

	return response, nil
}

// GetBackendStatus gets the status of the BE nodes
func (c *StarRocksClientImpl) GetBackendStatus(ctx context.Context) ([]BackendStatus, error) {
	// Create context with timeout if not already set
	var cancel context.CancelFunc
	if _, ok := ctx.Deadline(); !ok && c.config.QueryTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.config.QueryTimeout)
		defer cancel()
	}

	// Start time measurement
	startTime := time.Now()
	defer func() {
		// Record time
		duration := time.Since(startTime)
		c.metrics.HistogramObserve("starrocks_client_backend_status_time_ms", float64(duration.Milliseconds()), nil)
	}()

	// Create retry backoff
	backoffConfig := backoff.NewExponentialBackOff()
	backoffConfig.InitialInterval = c.config.RetryWaitMin
	backoffConfig.MaxInterval = c.config.RetryWaitMax
	backoffConfig.MaxElapsedTime = c.config.QueryTimeout

	retryBackoff := backoff.WithContext(backoffConfig, ctx)

	// Initialize result
	var backends []BackendStatus

	// Execute with retry
	operation := func() error {
		// Get current FE address
		feAddr := c.getCurrentFEAddress()
		httpPort := c.config.HTTPPort

		// Build URL
		urlStr := fmt.Sprintf("http://%s:%d/api/backends", feAddr, httpPort)

		// Create request
		req, err := http.NewRequestWithContext(ctx, "GET", urlStr, nil)
		if err != nil {
			return errors.Wrap(err, "failed to create request")
		}

		// Set basic auth
		req.SetBasicAuth(c.config.User, c.config.Password)

		// Execute request
		resp, err := c.httpClient.Do(req)
		if err != nil {
			// Handle connection errors by rotating to next FE
			if isConnectionError(err) {
				c.rotateToNextFE()
			}
			return errors.Wrap(err, "failed to execute request")
		}
		defer resp.Body.Close()

		// Read response body
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.Wrap(err, "failed to read response body")
		}

		// Check response status
		if resp.StatusCode != http.StatusOK {
			return errors.Errorf("get backends failed with status %d: %s", resp.StatusCode, string(body))
		}

		// Parse response
		var response struct {
			Backends []BackendStatus `json:"backends"`
		}
		if err := json.Unmarshal(body, &response); err != nil {
			return errors.Wrap(err, "failed to parse response")
		}

		backends = response.Backends
		return nil
	}

	// Execute with retry
	err := backoff.Retry(operation, retryBackoff)
	if err != nil {
		c.metrics.CounterInc("starrocks_client_backend_status_errors", map[string]string{
			"error_type": getErrorType(err),
		})
		return nil, errors.Wrap(err, "get backends failed after retries")
	}

	c.metrics.CounterInc("starrocks_client_backend_status_requests", nil)

	return backends, nil
}

// GetTabletDistribution gets the distribution of tablets
func (c *StarRocksClientImpl) GetTabletDistribution(ctx context.Context, database, table string) ([]TabletInfo, error) {
	// Construct the query
	query := fmt.Sprintf("SHOW TABLETS FROM %s.%s", database, table)

	// Execute the query
	result, err := c.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get tablet distribution")
	}

	// Parse the result
	tablets := make([]TabletInfo, 0, len(result.Rows))
	for _, row := range result.Rows {
		// Map row data to tablet info structure
		tablet := TabletInfo{}

		// Default to database.table.partition.bucket format for names
		// This will need to be adjusted based on actual query results from StarRocks
		for i, col := range result.Columns {
			// Skip null values
			if row[i] == nil {
				continue
			}

			// Map columns to struct fields
			switch strings.ToLower(col) {
			case "tablet_id":
				if id, ok := row[i].(int64); ok {
					tablet.TabletID = id
				} else if idStr, ok := row[i].(string); ok {
					if id, err := strconv.ParseInt(idStr, 10, 64); err == nil {
						tablet.TabletID = id
					}
				}
			case "replica_id":
				if id, ok := row[i].(int64); ok {
					tablet.ReplicaID = id
				} else if idStr, ok := row[i].(string); ok {
					if id, err := strconv.ParseInt(idStr, 10, 64); err == nil {
						tablet.ReplicaID = id
					}
				}
			case "backend_id":
				if id, ok := row[i].(int64); ok {
					tablet.BackendID = id
				} else if idStr, ok := row[i].(string); ok {
					if id, err := strconv.ParseInt(idStr, 10, 64); err == nil {
						tablet.BackendID = id
					}
				}
			case "backend_host", "host":
				if host, ok := row[i].(string); ok {
					tablet.BackendHost = host
				}
			case "backend_http_port", "http_port":
				if port, ok := row[i].(int); ok {
					tablet.BackendHTTPPort = port
				} else if portStr, ok := row[i].(string); ok {
					if port, err := strconv.Atoi(portStr); err == nil {
						tablet.BackendHTTPPort = port
					}
				}
			case "schema_hash":
				if hash, ok := row[i].(int64); ok {
					tablet.SchemaHash = hash
				} else if hashStr, ok := row[i].(string); ok {
					if hash, err := strconv.ParseInt(hashStr, 10, 64); err == nil {
						tablet.SchemaHash = hash
					}
				}
			case "partition_id":
				if id, ok := row[i].(int64); ok {
					tablet.PartitionID = id
				} else if idStr, ok := row[i].(string); ok {
					if id, err := strconv.ParseInt(idStr, 10, 64); err == nil {
						tablet.PartitionID = id
					}
				}
			case "partition_name":
				if name, ok := row[i].(string); ok {
					tablet.PartitionName = name
				}
			case "bucket_id", "bucket":
				if id, ok := row[i].(int); ok {
					tablet.BucketID = id
				} else if idStr, ok := row[i].(string); ok {
					if id, err := strconv.Atoi(idStr); err == nil {
						tablet.BucketID = id
					}
				}
			case "path":
				if path, ok := row[i].(string); ok {
					tablet.Path = path
				}
			case "data_size":
				if size, ok := row[i].(int64); ok {
					tablet.DataSize = size
				} else if sizeStr, ok := row[i].(string); ok {
					if size, err := strconv.ParseInt(sizeStr, 10, 64); err == nil {
						tablet.DataSize = size
					}
				}
			case "row_count":
				if count, ok := row[i].(int64); ok {
					tablet.RowCount = count
				} else if countStr, ok := row[i].(string); ok {
					if count, err := strconv.ParseInt(countStr, 10, 64); err == nil {
						tablet.RowCount = count
					}
				}
			case "state":
				if state, ok := row[i].(string); ok {
					tablet.State = state
				}
			case "last_failure":
				if failure, ok := row[i].(string); ok {
					tablet.LastFailure = failure
				}
			case "last_success":
				if success, ok := row[i].(string); ok {
					tablet.LastSuccess = success
				}
			case "version":
				if version, ok := row[i].(int); ok {
					tablet.Version = version
				} else if versionStr, ok := row[i].(string); ok {
					if version, err := strconv.Atoi(versionStr); err == nil {
						tablet.Version = version
					}
				}
			}
		}

		// Fill in database and table info
		tablet.DatabaseName = database
		tablet.TableName = table

		tablets = append(tablets, tablet)
	}

	c.metrics.CounterInc("starrocks_client_tablet_distribution_requests", map[string]string{
		"database": database,
		"table":    table,
	})

	return tablets, nil
}

// GetTableSchema gets the schema of a table
func (c *StarRocksClientImpl) GetTableSchema(ctx context.Context, database, table string) (map[string]string, error) {
	// Construct the query
	query := fmt.Sprintf("DESCRIBE %s.%s", database, table)

	// Execute the query
	result, err := c.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get table schema")
	}

	// Parse the result
	schema := make(map[string]string)
	for _, row := range result.Rows {
		// Ensure we have at least 2 columns (field and type)
		if len(row) < 2 {
			continue
		}

		// Extract field name and type
		fieldName, ok1 := row[0].(string)
		fieldType, ok2 := row[1].(string)

		if ok1 && ok2 {
			schema[fieldName] = fieldType
		}
	}

	c.metrics.CounterInc("starrocks_client_table_schema_requests", map[string]string{
		"database": database,
		"table":    table,
	})

	return schema, nil
}

// GetPartitions gets the partitions of a table
func (c *StarRocksClientImpl) GetPartitions(ctx context.Context, database, table string) ([]string, error) {
	// Construct the query
	query := fmt.Sprintf("SHOW PARTITIONS FROM %s.%s", database, table)

	// Execute the query
	result, err := c.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get partitions")
	}

	// Find the column index for PartitionName
	partitionNameIdx := -1
	for i, col := range result.Columns {
		if strings.EqualFold(col, "PartitionName") || strings.EqualFold(col, "Partition") {
			partitionNameIdx = i
			break
		}
	}

	// If we couldn't find the partition name column, return an error
	if partitionNameIdx == -1 {
		return nil, errors.New("partition name column not found in result")
	}

	// Extract partition names
	partitions := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		if partitionName, ok := row[partitionNameIdx].(string); ok {
			partitions = append(partitions, partitionName)
		}
	}

	c.metrics.CounterInc("starrocks_client_partition_requests", map[string]string{
		"database": database,
		"table":    table,
	})

	return partitions, nil
}

// Ping checks if the client can connect to StarRocks
func (c *StarRocksClientImpl) Ping(ctx context.Context) error {
	// Create context with timeout if not already set
	var cancel context.CancelFunc
	if _, ok := ctx.Deadline(); !ok && c.config.QueryTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.config.QueryTimeout)
		defer cancel()
	}

	// Try to ping the database
	if c.db != nil {
		if err := c.db.PingContext(ctx); err != nil {
			// If ping fails, try to reconnect
			c.logger.Warn("Ping failed, trying to reconnect", "error", err)
			if err := c.initDBConnection(); err != nil {
				// If reconnect fails, rotate to next FE
				c.rotateToNextFE()
				if err := c.initDBConnection(); err != nil {
					return errors.Wrap(err, "failed to reconnect after ping failure")
				}
			}
		}
	} else {
		// If db is nil, try to initialize connection
		if err := c.initDBConnection(); err != nil {
			return errors.Wrap(err, "failed to initialize database connection")
		}
	}

	c.metrics.CounterInc("starrocks_client_ping", nil)

	return nil
}

// Close closes the client
func (c *StarRocksClientImpl) Close() error {
	// Close the database connection
	if c.db != nil {
		err := c.db.Close()
		c.db = nil
		return err
	}
	return nil
}

// isConnectionError checks if an error is a connection error
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	return strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "no route to host") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection closed") ||
		strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "i/o timeout") ||
		strings.Contains(errStr, "network is unreachable")
}

// isWriteQuery checks if a query is a write query
func isWriteQuery(sql string) bool {
	sqlUpper := strings.ToUpper(strings.TrimSpace(sql))
	return strings.HasPrefix(sqlUpper, "INSERT") ||
		strings.HasPrefix(sqlUpper, "UPDATE") ||
		strings.HasPrefix(sqlUpper, "DELETE") ||
		strings.HasPrefix(sqlUpper, "TRUNCATE") ||
		strings.HasPrefix(sqlUpper, "CREATE") ||
		strings.HasPrefix(sqlUpper, "DROP") ||
		strings.HasPrefix(sqlUpper, "ALTER")
}

// getQueryType gets the type of a query
func getQueryType(sql string) string {
	sqlUpper := strings.ToUpper(strings.TrimSpace(sql))

	if strings.HasPrefix(sqlUpper, "SELECT") {
		return "SELECT"
	} else if strings.HasPrefix(sqlUpper, "INSERT") {
		return "INSERT"
	} else if strings.HasPrefix(sqlUpper, "UPDATE") {
		return "UPDATE"
	} else if strings.HasPrefix(sqlUpper, "DELETE") {
		return "DELETE"
	} else if strings.HasPrefix(sqlUpper, "CREATE") {
		return "CREATE"
	} else if strings.HasPrefix(sqlUpper, "DROP") {
		return "DROP"
	} else if strings.HasPrefix(sqlUpper, "ALTER") {
		return "ALTER"
	} else if strings.HasPrefix(sqlUpper, "SHOW") {
		return "SHOW"
	} else if strings.HasPrefix(sqlUpper, "DESCRIBE") || strings.HasPrefix(sqlUpper, "DESC") {
		return "DESCRIBE"
	}

	return "OTHER"
}

// getErrorType gets a standardized error type from an error
func getErrorType(err error) string {
	if err == nil {
		return "NONE"
	}

	errStr := err.Error()

	if isConnectionError(err) {
		return "CONNECTION"
	} else if strings.Contains(errStr, "timeout") || strings.Contains(errStr, "deadline exceeded") {
		return "TIMEOUT"
	} else if strings.Contains(errStr, "permission denied") || strings.Contains(errStr, "access denied") {
		return "PERMISSION"
	} else if strings.Contains(errStr, "not found") || strings.Contains(errStr, "doesn't exist") {
		return "NOT_FOUND"
	} else if strings.Contains(errStr, "duplicate") || strings.Contains(errStr, "already exists") {
		return "DUPLICATE"
	} else if strings.Contains(errStr, "syntax error") {
		return "SYNTAX"
	}

	return "OTHER"
}

//Personal.AI order the ending
