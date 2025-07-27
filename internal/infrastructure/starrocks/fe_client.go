// Package starrocks provides a client for interacting with StarRocks cluster.
package starrocks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"github.com/turtacn/staravail/internal/common/config"
	"github.com/turtacn/staravail/internal/common/logging"
	"github.com/turtacn/staravail/internal/common/metrics"
)

// BackendInfo represents the status information of a BE node
type BackendInfo struct {
	ID                    int64   `json:"id"`
	Host                  string  `json:"host"`
	BePort                int     `json:"be_port"`
	HTTPPort              int     `json:"http_port"`
	BrpcPort              int     `json:"brpc_port"`
	AliveStatus           bool    `json:"alive"`
	SystemDecommissioned  bool    `json:"SystemDecommissioned"`
	ClusterDecommissioned bool    `json:"ClusterDecommissioned"`
	TabletNum             int64   `json:"tabletNum"`
	DataUsedCapacity      int64   `json:"dataUsedCapacity"`
	AvailCapacity         int64   `json:"availCapacity"`
	TotalCapacity         int64   `json:"totalCapacity"`
	UsedPct               float64 `json:"usedPct"`
	MaxDiskUsedPct        float64 `json:"maxDiskUsedPct"`
	Tag                   string  `json:"tag"`
	ErrMsg                string  `json:"errMsg"`
	Status                string  `json:"status"`
	HeartbeatFailureCount int     `json:"heartbeatFailureCounter"`
	LastHeartbeat         int64   `json:"lastHeartbeat"`
	LastUpdateTime        int64   `json:"lastUpdateTime"`
	StartTime             int64   `json:"startTime"`
	Version               string  `json:"version"`
}

// TabletInfo represents information about a tablet
type TabletInfo struct {
	TabletID             int64  `json:"tablet_id"`
	ReplicaID            int64  `json:"replica_id"`
	BackendID            int64  `json:"backend_id"`
	BackendHost          string `json:"backend_host"`
	DatabaseName         string `json:"database_name"`
	TableName            string `json:"table_name"`
	PartitionID          int64  `json:"partition_id"`
	PartitionName        string `json:"partition_name"`
	BucketID             int    `json:"bucket_id"`
	SchemaHash           int64  `json:"schema_hash"`
	Version              int    `json:"version"`
	VersionHash          int64  `json:"version_hash"`
	DataSize             int64  `json:"data_size"`
	RowCount             int64  `json:"row_count"`
	State                string `json:"state"`
	LastFailure          string `json:"last_failure"`
	LastSuccessVersion   int    `json:"last_success_version"`
	LastSuccessVersionTs int64  `json:"last_success_version_ts"`
	Path                 string `json:"path"`
	IsConsistent         bool   `json:"is_consistent"`
}

// StreamLoadResponse represents the response of a stream load
type StreamLoadResponse struct {
	Status                 string `json:"Status"`
	Message                string `json:"Message"`
	Label                  string `json:"Label"`
	TxnID                  int64  `json:"TxnId"`
	LoadedRows             int64  `json:"LoadedRows"`
	FilteredRows           int64  `json:"FilteredRows"`
	UnselectedRows         int64  `json:"UnselectedRows"`
	LoadBytes              int64  `json:"LoadBytes"`
	LoadTimeMS             int64  `json:"LoadTimeMs"`
	BeginTxnTimeMs         int64  `json:"BeginTxnTimeMs"`
	StreamLoadPutTimeMs    int64  `json:"StreamLoadPutTimeMs"`
	ReadDataTimeMs         int64  `json:"ReadDataTimeMs"`
	WriteDataTimeMs        int64  `json:"WriteDataTimeMs"`
	CommitAndPublishTimeMs int64  `json:"CommitAndPublishTimeMs"`
	ErrorURL               string `json:"ErrorURL"`
	Tracking               string `json:"Tracking"`
}

// TableSchema represents the schema information of a table
type TableSchema struct {
	DatabaseName string                 `json:"database_name"`
	TableName    string                 `json:"table_name"`
	CreateTime   int64                  `json:"create_time"`
	TableType    string                 `json:"table_type"`
	KeyType      string                 `json:"key_type"`
	Columns      []ColumnSchema         `json:"columns"`
	Partitions   []PartitionSchema      `json:"partitions"`
	Properties   map[string]interface{} `json:"properties"`
	IndexInfo    []IndexSchema          `json:"index_info"`
}

// ColumnSchema represents the schema information of a column
type ColumnSchema struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	IsKey         bool   `json:"is_key"`
	IsNullable    bool   `json:"is_nullable"`
	DefaultValue  string `json:"default_value"`
	Comment       string `json:"comment"`
	IsAutoIncr    bool   `json:"is_auto_incr"`
	PrecisionInfo string `json:"precision_info"`
}

// PartitionSchema represents the schema information of a partition
type PartitionSchema struct {
	PartitionID       int64                  `json:"partition_id"`
	PartitionName     string                 `json:"partition_name"`
	PartitionType     string                 `json:"partition_type"`
	PartitionKeyDesc  string                 `json:"partition_key_desc"`
	PartitionValue    string                 `json:"partition_value"`
	PartitionDataSize int64                  `json:"partition_data_size"`
	PartitionRowCount int64                  `json:"partition_row_count"`
	PartitionBuckets  []BucketSchema         `json:"partition_buckets"`
	Properties        map[string]interface{} `json:"properties"`
}

// BucketSchema represents the schema information of a bucket
type BucketSchema struct {
	BucketID   int   `json:"bucket_id"`
	ReplicaNum int   `json:"replica_num"`
	Version    int   `json:"version"`
	BackendIDs []int `json:"backend_ids"`
}

// IndexSchema represents the schema information of an index
type IndexSchema struct {
	IndexName    string   `json:"index_name"`
	IndexType    string   `json:"index_type"`
	IndexColumns []string `json:"index_columns"`
	IsUnique     bool     `json:"is_unique"`
	Comment      string   `json:"comment"`
}

// DatabaseInfo represents information about a database
type DatabaseInfo struct {
	ID           int64     `json:"id"`
	Name         string    `json:"name"`
	CreateTime   time.Time `json:"create_time"`
	TablesCount  int       `json:"tables_count"`
	TableNames   []string  `json:"table_names"`
	ClusterNames []string  `json:"cluster_names"`
}

// FEClient defines the interface for interacting with StarRocks FE HTTP API
type FEClient interface {
	// GetBackends gets the status of all BE nodes
	GetBackends(ctx context.Context) ([]BackendInfo, error)

	// GetTablets gets information about tablets for a specific table
	GetTablets(ctx context.Context, dbName, tableName string) ([]TabletInfo, error)

	// ExecuteStreamLoad performs a stream load to import data
	ExecuteStreamLoad(ctx context.Context, dbName, tableName string, data []byte, options map[string]string) (*StreamLoadResponse, error)

	// GetTableSchema gets the schema of a table
	GetTableSchema(ctx context.Context, dbName, tableName string) (*TableSchema, error)

	// GetDatabases gets information about all databases
	GetDatabases(ctx context.Context) ([]DatabaseInfo, error)

	// GetDatabaseTables gets the names of all tables in a database
	GetDatabaseTables(ctx context.Context, dbName string) ([]string, error)

	// IsBackendAlive checks if a specific BE node is alive
	IsBackendAlive(ctx context.Context, beID int64) (bool, error)

	// IsTableHealthy checks if a specific table is healthy
	IsTableHealthy(ctx context.Context, dbName, tableName string) (bool, error)

	// IsPartitionHealthy checks if a specific partition is healthy
	IsPartitionHealthy(ctx context.Context, dbName, tableName, partitionName string) (bool, error)

	// Ping checks if the FE is reachable
	Ping(ctx context.Context) error

	// Close closes the client
	Close() error
}

// FEClientConfig represents configuration for the FE client
type FEClientConfig struct {
	// FEAddresses is a list of StarRocks FE addresses
	FEAddresses []string
	// HTTPPort is the HTTP port of the FE nodes
	HTTPPort int
	// User is the username for connecting to StarRocks
	User string
	// Password is the password for connecting to StarRocks
	Password string
	// RequestTimeout is the timeout for requests
	RequestTimeout time.Duration
	// MaxRetries is the maximum number of retries for requests
	MaxRetries int
	// RetryBaseDelay is the base delay for retries
	RetryBaseDelay time.Duration
	// RetryMaxDelay is the maximum delay for retries
	RetryMaxDelay time.Duration
	// IdleConnTimeout is the timeout for idle connections
	IdleConnTimeout time.Duration
	// MaxIdleConns is the maximum number of idle connections
	MaxIdleConns int
	// MaxConnsPerHost is the maximum number of connections per host
	MaxConnsPerHost int
}

// FEClientImpl implements the FEClient interface
type FEClientImpl struct {
	// config is the client configuration
	config FEClientConfig
	// httpClient is the HTTP client for API requests
	httpClient *http.Client
	// feAddresses is a list of FE addresses
	feAddresses []string
	// currentFEIndex is the index of the current FE address
	currentFEIndex int
	// logger is used for logging
	logger logging.Logger
	// metrics is used for recording metrics
	metrics metrics.MetricsRecorder
	// mutex protects currentFEIndex
	mutex sync.RWMutex
}

// NewFEClient creates a new FEClient instance
func NewFEClient(
	cfg config.FEClientConfig,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) (FEClient, error) {
	// Validate configuration
	if len(cfg.FEAddresses) == 0 {
		return nil, errors.New("no FE addresses provided")
	}
	if cfg.User == "" {
		return nil, errors.New("no username provided")
	}

	// Create the HTTP transport
	transport := &http.Transport{
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxConnsPerHost:       cfg.MaxConnsPerHost,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		ResponseHeaderTimeout: cfg.RequestTimeout,
	}

	// Create the HTTP client
	httpClient := &http.Client{
		Timeout:   cfg.RequestTimeout,
		Transport: transport,
	}

	// Create the client
	client := &FEClientImpl{
		config: FEClientConfig{
			FEAddresses:     cfg.FEAddresses,
			HTTPPort:        cfg.HTTPPort,
			User:            cfg.User,
			Password:        cfg.Password,
			RequestTimeout:  cfg.RequestTimeout,
			MaxRetries:      cfg.MaxRetries,
			RetryBaseDelay:  cfg.RetryBaseDelay,
			RetryMaxDelay:   cfg.RetryMaxDelay,
			IdleConnTimeout: cfg.IdleConnTimeout,
			MaxIdleConns:    cfg.MaxIdleConns,
			MaxConnsPerHost: cfg.MaxConnsPerHost,
		},
		httpClient:     httpClient,
		feAddresses:    cfg.FEAddresses,
		currentFEIndex: rand.Intn(len(cfg.FEAddresses)),
		logger:         logger,
		metrics:        metricsRecorder,
		mutex:          sync.RWMutex{},
	}

	return client, nil
}

// getCurrentFE gets the current FE address
func (c *FEClientImpl) getCurrentFE() string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.feAddresses[c.currentFEIndex]
}

// rotateToNextFE rotates to the next FE address
func (c *FEClientImpl) rotateToNextFE() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.currentFEIndex = (c.currentFEIndex + 1) % len(c.feAddresses)
	c.logger.Info("Rotated to next FE",
		"fe_address", c.feAddresses[c.currentFEIndex],
		"index", c.currentFEIndex)
	c.metrics.CounterInc("fe_client_rotations", nil)
}

// executeRequest executes an HTTP request with retries and failover
func (c *FEClientImpl) executeRequest(ctx context.Context, method, path string, body io.Reader, headers map[string]string) ([]byte, error) {
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		c.metrics.HistogramObserve("fe_client_request_duration_ms", float64(duration.Milliseconds()), map[string]string{
			"method": method,
			"path":   path,
		})
	}()

	// Create retry backoff
	backoffConfig := backoff.NewExponentialBackOff()
	backoffConfig.InitialInterval = c.config.RetryBaseDelay
	backoffConfig.MaxInterval = c.config.RetryMaxDelay
	backoffConfig.MaxElapsedTime = c.config.RequestTimeout

	retryBackoff := backoff.WithMaxRetries(
		backoff.WithContext(backoffConfig, ctx),
		uint64(c.config.MaxRetries),
	)

	var responseBody []byte
	operation := func() error {
		// Get current FE address
		feAddr := c.getCurrentFE()

		// Build URL
		urlStr := fmt.Sprintf("http://%s:%d%s", feAddr, c.config.HTTPPort, path)

		// Create request
		req, err := http.NewRequestWithContext(ctx, method, urlStr, body)
		if err != nil {
			return backoff.Permanent(errors.Wrap(err, "failed to create request"))
		}

		// Set basic auth
		req.SetBasicAuth(c.config.User, c.config.Password)

		// Set default headers
		req.Header.Set("Content-Type", "application/json")

		// Set custom headers
		for key, value := range headers {
			req.Header.Set(key, value)
		}

		// Execute request
		resp, err := c.httpClient.Do(req)
		if err != nil {
			// Handle connection errors by rotating to next FE
			if isConnectionError(err) {
				c.rotateToNextFE()
				c.logger.Warn("Connection error, rotated to next FE",
					"error", err.Error(),
					"new_fe", c.getCurrentFE())
				return err // Retry with new FE
			}
			return backoff.Permanent(errors.Wrap(err, "failed to execute request"))
		}
		defer resp.Body.Close()

		// Read response body
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.Wrap(err, "failed to read response body")
		}

		// Check response status
		if resp.StatusCode >= 400 {
			// Handle status errors
			switch resp.StatusCode {
			case http.StatusUnauthorized, http.StatusForbidden:
				// Auth errors are permanent
				return backoff.Permanent(errors.Errorf("authentication error: %s", string(respBody)))
			case http.StatusNotFound:
				// Not found errors are permanent
				return backoff.Permanent(errors.Errorf("resource not found: %s", string(respBody)))
			case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable:
				// Server errors might be temporary, try another FE
				c.rotateToNextFE()
				c.logger.Warn("Server error, rotated to next FE",
					"status", resp.StatusCode,
					"error", string(respBody),
					"new_fe", c.getCurrentFE())
				return errors.Errorf("server error: %s", string(respBody))
			default:
				// Other errors are permanent
				return backoff.Permanent(errors.Errorf("request failed with status %d: %s", resp.StatusCode, string(respBody)))
			}
		}

		responseBody = respBody
		return nil
	}

	err := backoff.Retry(operation, retryBackoff)
	if err != nil {
		c.metrics.CounterInc("fe_client_request_errors", map[string]string{
			"method":     method,
			"path":       path,
			"error_type": getErrorType(err),
		})
		return nil, err
	}

	c.metrics.CounterInc("fe_client_requests", map[string]string{
		"method": method,
		"path":   path,
	})

	return responseBody, nil
}

// GetBackends gets the status of all BE nodes
func (c *FEClientImpl) GetBackends(ctx context.Context) ([]BackendInfo, error) {
	// Make request to /api/backends endpoint
	respBody, err := c.executeRequest(ctx, "GET", "/api/backends", nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get backends")
	}

	// Parse response
	var response struct {
		Backends []BackendInfo `json:"backends"`
	}
	if err := json.Unmarshal(respBody, &response); err != nil {
		return nil, errors.Wrap(err, "failed to parse backends response")
	}

	c.logger.Debug("Got backends", "count", len(response.Backends))

	return response.Backends, nil
}

// GetTablets gets information about tablets for a specific table
func (c *FEClientImpl) GetTablets(ctx context.Context, dbName, tableName string) ([]TabletInfo, error) {
	// Build path
	path := fmt.Sprintf("/api/tablets?db=%s&table=%s", url.QueryEscape(dbName), url.QueryEscape(tableName))

	// Make request
	respBody, err := c.executeRequest(ctx, "GET", path, nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get tablets")
	}

	// Parse response
	var response struct {
		Tablets []TabletInfo `json:"tablets"`
	}
	if err := json.Unmarshal(respBody, &response); err != nil {
		return nil, errors.Wrap(err, "failed to parse tablets response")
	}

	c.logger.Debug("Got tablets",
		"db", dbName,
		"table", tableName,
		"count", len(response.Tablets))

	return response.Tablets, nil
}

// ExecuteStreamLoad performs a stream load to import data
func (c *FEClientImpl) ExecuteStreamLoad(ctx context.Context, dbName, tableName string, data []byte, options map[string]string) (*StreamLoadResponse, error) {
	// Build path
	path := fmt.Sprintf("/api/%s/%s/_stream_load", url.QueryEscape(dbName), url.QueryEscape(tableName))

	// Generate a unique label for the load
	label := fmt.Sprintf("proxy_load_%s_%d", tableName, time.Now().UnixNano())

	// Set headers
	headers := map[string]string{
		"Expect":         "100-continue",
		"Content-Type":   "text/plain", // Default content type, can be overridden
		"Content-Length": strconv.Itoa(len(data)),
		"label":          label,
	}

	// Add optional headers from options
	for k, v := range options {
		headers[k] = v
	}

	// Make request
	startTime := time.Now()
	respBody, err := c.executeRequest(ctx, "PUT", path, bytes.NewReader(data), headers)
	duration := time.Since(startTime)

	if err != nil {
		return nil, errors.Wrap(err, "failed to execute stream load")
	}

	// Parse response
	var response StreamLoadResponse
	if err := json.Unmarshal(respBody, &response); err != nil {
		return nil, errors.Wrap(err, "failed to parse stream load response")
	}

	// Log and record metrics
	c.logger.Info("Stream load completed",
		"db", dbName,
		"table", tableName,
		"status", response.Status,
		"rows", response.LoadedRows,
		"bytes", response.LoadBytes,
		"duration_ms", duration.Milliseconds())

	c.metrics.CounterAdd("fe_client_stream_load_rows", float64(response.LoadedRows), map[string]string{
		"db":    dbName,
		"table": tableName,
	})
	c.metrics.CounterAdd("fe_client_stream_load_bytes", float64(response.LoadBytes), map[string]string{
		"db":    dbName,
		"table": tableName,
	})

	return &response, nil
}

// GetTableSchema gets the schema of a table
func (c *FEClientImpl) GetTableSchema(ctx context.Context, dbName, tableName string) (*TableSchema, error) {
	// Build path
	path := fmt.Sprintf("/api/metadata/table?db=%s&table=%s", url.QueryEscape(dbName), url.QueryEscape(tableName))

	// Make request
	respBody, err := c.executeRequest(ctx, "GET", path, nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get table schema")
	}

	// Parse response
	var response struct {
		Data *TableSchema `json:"data"`
	}
	if err := json.Unmarshal(respBody, &response); err != nil {
		return nil, errors.Wrap(err, "failed to parse table schema response")
	}

	if response.Data == nil {
		return nil, errors.New("table schema not found in response")
	}

	c.logger.Debug("Got table schema",
		"db", dbName,
		"table", tableName,
		"columns", len(response.Data.Columns),
		"partitions", len(response.Data.Partitions))

	return response.Data, nil
}

// GetDatabases gets information about all databases
func (c *FEClientImpl) GetDatabases(ctx context.Context) ([]DatabaseInfo, error) {
	// Make request
	respBody, err := c.executeRequest(ctx, "GET", "/api/metadata/databases", nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get databases")
	}

	// Parse response
	var response struct {
		Data []DatabaseInfo `json:"data"`
	}
	if err := json.Unmarshal(respBody, &response); err != nil {
		return nil, errors.Wrap(err, "failed to parse databases response")
	}

	c.logger.Debug("Got databases", "count", len(response.Data))

	return response.Data, nil
}

// GetDatabaseTables gets the names of all tables in a database
func (c *FEClientImpl) GetDatabaseTables(ctx context.Context, dbName string) ([]string, error) {
	// Build path
	path := fmt.Sprintf("/api/metadata/database_tables?db=%s", url.QueryEscape(dbName))

	// Make request
	respBody, err := c.executeRequest(ctx, "GET", path, nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get database tables")
	}

	// Parse response
	var response struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(respBody, &response); err != nil {
		return nil, errors.Wrap(err, "failed to parse database tables response")
	}

	c.logger.Debug("Got database tables", "db", dbName, "count", len(response.Data))

	return response.Data, nil
}

// IsBackendAlive checks if a specific BE node is alive
func (c *FEClientImpl) IsBackendAlive(ctx context.Context, beID int64) (bool, error) {
	// Get all backends
	backends, err := c.GetBackends(ctx)
	if err != nil {
		return false, errors.Wrap(err, "failed to get backends")
	}

	// Find the backend with the specified ID
	for _, be := range backends {
		if be.ID == beID {
			return be.AliveStatus, nil
		}
	}

	return false, errors.Errorf("backend with ID %d not found", beID)
}

// IsTableHealthy checks if a specific table is healthy
func (c *FEClientImpl) IsTableHealthy(ctx context.Context, dbName, tableName string) (bool, error) {
	// Get tablets for the table
	tablets, err := c.GetTablets(ctx, dbName, tableName)
	if err != nil {
		return false, errors.Wrap(err, "failed to get tablets")
	}

	// If there are no tablets, the table is considered unhealthy
	if len(tablets) == 0 {
		return false, nil
	}

	// Check if all tablets are in a healthy state
	for _, tablet := range tablets {
		if tablet.State != "NORMAL" && tablet.State != "CLONE" {
			return false, nil
		}
	}

	return true, nil
}

// IsPartitionHealthy checks if a specific partition is healthy
func (c *FEClientImpl) IsPartitionHealthy(ctx context.Context, dbName, tableName, partitionName string) (bool, error) {
	// Get tablets for the table
	tablets, err := c.GetTablets(ctx, dbName, tableName)
	if err != nil {
		return false, errors.Wrap(err, "failed to get tablets")
	}

	// Filter tablets for the specified partition
	var partitionTablets []TabletInfo
	for _, tablet := range tablets {
		if tablet.PartitionName == partitionName {
			partitionTablets = append(partitionTablets, tablet)
		}
	}

	// If there are no tablets for the partition, the partition is considered unhealthy
	if len(partitionTablets) == 0 {
		return false, nil
	}

	// Check if all tablets for the partition are in a healthy state
	for _, tablet := range partitionTablets {
		if tablet.State != "NORMAL" && tablet.State != "CLONE" {
			return false, nil
		}
	}

	return true, nil
}

// Ping checks if the FE is reachable
func (c *FEClientImpl) Ping(ctx context.Context) error {
	// Try to get a simple endpoint
	_, err := c.executeRequest(ctx, "GET", "/api/health", nil, nil)
	if err != nil {
		// If the health endpoint is not available, try the backends endpoint
		_, err = c.executeRequest(ctx, "GET", "/api/backends", nil, nil)
	}
	return err
}

// Close closes the client
func (c *FEClientImpl) Close() error {
	// Close any resources if needed
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
