// Package engine provides the core execution engine for the system.
package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/turtacn/staravail/internal/client"
	"github.com/turtacn/staravail/internal/common/logging"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/common/utils"
	"go.uber.org/zap"
)

// ExecutorStatus represents the current status of an executor.
type ExecutorStatus string

const (
	// ExecutorStatusPending indicates the executor is waiting to start.
	ExecutorStatusPending ExecutorStatus = "pending"
	// ExecutorStatusRunning indicates the executor is currently running.
	ExecutorStatusRunning ExecutorStatus = "running"
	// ExecutorStatusCompleted indicates the executor has completed successfully.
	ExecutorStatusCompleted ExecutorStatus = "completed"
	// ExecutorStatusFailed indicates the executor has failed.
	ExecutorStatusFailed ExecutorStatus = "failed"
	// ExecutorStatusCancelled indicates the executor was cancelled.
	ExecutorStatusCancelled ExecutorStatus = "cancelled"
)

// ExecutionMode represents the mode of execution.
type ExecutionMode string

const (
	// ExecutionModeSync executes the request synchronously.
	ExecutionModeSync ExecutionMode = "sync"
	// ExecutionModeAsync executes the request asynchronously.
	ExecutionModeAsync ExecutionMode = "async"
)

// ExecutorType represents the type of executor.
type ExecutorType string

const (
	// ExecutorTypeQuery is a query executor.
	ExecutorTypeQuery ExecutorType = "query"
	// ExecutorTypeWrite is a write executor.
	ExecutorTypeWrite ExecutorType = "write"
)

// RetryPolicy defines how retries should be handled.
type RetryPolicy struct {
	// MaxRetries is the maximum number of retries.
	MaxRetries int
	// RetryInterval is the interval between retries.
	RetryInterval time.Duration
	// RetryableErrors are the error types that can be retried.
	RetryableErrors []string
	// ExponentialBackoff indicates whether to use exponential backoff.
	ExponentialBackoff bool
	// MaxRetryInterval is the maximum interval for exponential backoff.
	MaxRetryInterval time.Duration
}

// DefaultRetryPolicy returns the default retry policy.
func DefaultRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		MaxRetries:         3,
		RetryInterval:      time.Second,
		RetryableErrors:    []string{"connection", "timeout", "resource"},
		ExponentialBackoff: true,
		MaxRetryInterval:   time.Second * 30,
	}
}

// ExecutorStats represents statistics about an execution.
type ExecutorStats struct {
	// StartTime is when the execution started.
	StartTime time.Time
	// EndTime is when the execution ended.
	EndTime time.Time
	// Duration is how long the execution took.
	Duration time.Duration
	// Status is the current status of the execution.
	Status ExecutorStatus
	// RetryCount is the number of retries performed.
	RetryCount int
	// RetryErrors contains the errors that caused retries.
	RetryErrors []string
	// RowCount is the number of rows affected or returned.
	RowCount int64
	// BytesProcessed is the number of bytes processed.
	BytesProcessed int64
	// NodeID is the ID of the node that executed the request.
	NodeID string
	// ErrorMessage contains any error message if the execution failed.
	ErrorMessage string
	// QueryType is the type of query (for query executors).
	QueryType string
	// WriteType is the type of write (for write executors).
	WriteType string
	// CacheHit indicates whether the result was retrieved from cache.
	CacheHit bool
	// QueueTime is how long the request waited in the queue.
	QueueTime time.Duration
}

// ExecutorResult represents the result of an execution.
type ExecutorResult struct {
	// Status is the final status of the execution.
	Status ExecutorStatus
	// Data contains the result data for queries.
	Data interface{}
	// Columns contains the column information for queries.
	Columns []string
	// ColumnTypes contains the column types for queries.
	ColumnTypes []*sql.ColumnType
	// RowsAffected is the number of rows affected for write operations.
	RowsAffected int64
	// ExecutionTime is how long the execution took.
	ExecutionTime time.Duration
	// Stats contains detailed statistics about the execution.
	Stats *ExecutorStats
	// Warnings contains any warnings that occurred during execution.
	Warnings []string
	// Error contains any error that occurred during execution.
	Error error
	// NodeID is the ID of the node that executed the request.
	NodeID string
	// CacheHit indicates whether the result was retrieved from cache.
	CacheHit bool
}

// Executor defines the interface for executing requests.
type Executor interface {
	// Execute executes the request and returns the result.
	Execute(ctx context.Context) (*ExecutorResult, error)

	// Cancel cancels the execution.
	Cancel() error

	// GetStatus returns the current status of the execution.
	GetStatus() ExecutorStatus

	// GetStats returns statistics about the execution.
	GetStats() *ExecutorStats

	// GetType returns the type of the executor.
	GetType() ExecutorType

	// GetID returns the unique ID of the executor.
	GetID() string
}

// baseExecutor contains common functionality for all executors.
type baseExecutor struct {
	// id is the unique identifier for this executor.
	id string

	// client is the StarRocks client.
	client client.StarRocksClient

	// logger is the logger for this executor.
	logger *zap.Logger

	// metrics is the metrics registry.
	metrics *metrics.Registry

	// executorType is the type of this executor.
	executorType ExecutorType

	// status is the current status of the execution.
	status ExecutorStatus

	// statusMu protects the status field.
	statusMu sync.RWMutex

	// ctx is the context for the execution.
	ctx context.Context

	// cancelFunc is the function to cancel the execution context.
	cancelFunc context.CancelFunc

	// startTime is when the execution started.
	startTime time.Time

	// endTime is when the execution ended.
	endTime time.Time

	// retryPolicy is the retry policy for this executor.
	retryPolicy *RetryPolicy

	// retryCount is the number of retries performed.
	retryCount int

	// retryErrors contains the errors that caused retries.
	retryErrors []string

	// nodeID is the ID of the node that executed the request.
	nodeID string

	// errorMessage contains any error message if the execution failed.
	errorMessage string

	// queueTime is how long the request waited in the queue.
	queueTime time.Duration

	// rowCount is the number of rows affected or returned.
	rowCount int64

	// bytesProcessed is the number of bytes processed.
	bytesProcessed int64

	// cacheHit indicates whether the result was retrieved from cache.
	cacheHit bool
}

// newBaseExecutor creates a new baseExecutor.
func newBaseExecutor(
	executorType ExecutorType,
	starrocksClient client.StarRocksClient,
	metricsRegistry *metrics.Registry,
	retryPolicy *RetryPolicy,
) *baseExecutor {
	id := utils.GenerateUUID()
	ctx, cancelFunc := context.WithCancel(context.Background())

	logger := logging.GetLogger().Named("executor").With(
		zap.String("executor_id", id),
		zap.String("executor_type", string(executorType)),
	)

	return &baseExecutor{
		id:           id,
		client:       starrocksClient,
		logger:       logger,
		metrics:      metricsRegistry,
		executorType: executorType,
		status:       ExecutorStatusPending,
		ctx:          ctx,
		cancelFunc:   cancelFunc,
		retryPolicy:  retryPolicy,
		retryErrors:  make([]string, 0),
	}
}

// GetID returns the unique ID of the executor.
func (e *baseExecutor) GetID() string {
	return e.id
}

// GetType returns the type of the executor.
func (e *baseExecutor) GetType() ExecutorType {
	return e.executorType
}

// GetStatus returns the current status of the execution.
func (e *baseExecutor) GetStatus() ExecutorStatus {
	e.statusMu.RLock()
	defer e.statusMu.RUnlock()
	return e.status
}

// setStatus sets the current status of the execution.
func (e *baseExecutor) setStatus(status ExecutorStatus) {
	e.statusMu.Lock()
	defer e.statusMu.Unlock()
	e.status = status

	// Log status change
	e.logger.Debug("Executor status changed", zap.String("status", string(status)))
}

// Cancel cancels the execution.
func (e *baseExecutor) Cancel() error {
	e.statusMu.Lock()
	currentStatus := e.status
	e.statusMu.Unlock()

	if currentStatus == ExecutorStatusCompleted || currentStatus == ExecutorStatusFailed || currentStatus == ExecutorStatusCancelled {
		return fmt.Errorf("cannot cancel execution with status %s", currentStatus)
	}

	e.logger.Info("Cancelling execution")
	e.cancelFunc()
	e.setStatus(ExecutorStatusCancelled)

	return nil
}

// GetStats returns statistics about the execution.
func (e *baseExecutor) GetStats() *ExecutorStats {
	e.statusMu.RLock()
	defer e.statusMu.RUnlock()

	var duration time.Duration
	if !e.startTime.IsZero() {
		if e.endTime.IsZero() {
			duration = time.Since(e.startTime)
		} else {
			duration = e.endTime.Sub(e.startTime)
		}
	}

	return &ExecutorStats{
		StartTime:      e.startTime,
		EndTime:        e.endTime,
		Duration:       duration,
		Status:         e.status,
		RetryCount:     e.retryCount,
		RetryErrors:    e.retryErrors,
		RowCount:       e.rowCount,
		BytesProcessed: e.bytesProcessed,
		NodeID:         e.nodeID,
		ErrorMessage:   e.errorMessage,
		CacheHit:       e.cacheHit,
		QueueTime:      e.queueTime,
	}
}

// shouldRetry determines if a retry should be attempted based on the error.
func (e *baseExecutor) shouldRetry(err error) bool {
	if e.retryCount >= e.retryPolicy.MaxRetries {
		return false
	}

	errStr := err.Error()
	for _, retryableErr := range e.retryPolicy.RetryableErrors {
		if retryableErr != "" && errStr != "" && utils.ContainsIgnoreCase(errStr, retryableErr) {
			return true
		}
	}

	return false
}

// sleep sleeps for the appropriate retry interval.
func (e *baseExecutor) sleep() {
	var interval time.Duration

	if e.retryPolicy.ExponentialBackoff {
		// Calculate exponential backoff: interval * 2^retryCount
		interval = e.retryPolicy.RetryInterval * (1 << uint(e.retryCount))
		if interval > e.retryPolicy.MaxRetryInterval {
			interval = e.retryPolicy.MaxRetryInterval
		}
	} else {
		interval = e.retryPolicy.RetryInterval
	}

	e.logger.Debug("Sleeping before retry",
		zap.Duration("interval", interval),
		zap.Int("retry_count", e.retryCount))

	select {
	case <-time.After(interval):
		// Continue after sleep
	case <-e.ctx.Done():
		// Context cancelled, no need to sleep
	}
}

// Execute implements the Executor interface, but should be overridden by derived types.
func (e *baseExecutor) Execute(ctx context.Context) (*ExecutorResult, error) {
	return nil, errors.New("Execute not implemented in baseExecutor")
}

// QueryExecutor is responsible for executing query requests.
type QueryExecutor struct {
	*baseExecutor

	// query is the SQL query to execute.
	query string

	// database is the database to execute the query against.
	database string

	// user is the user executing the query.
	user string

	// parameters are the parameters for the query.
	parameters []interface{}

	// timeout is the maximum time to allow for query execution.
	timeout time.Duration

	// priority is the priority of the query.
	priority int

	// requestID is the ID of the request.
	requestID string

	// targetNodes are the nodes to execute the query on.
	targetNodes []string

	// useCache indicates whether to use caching.
	useCache bool

	// cacheTTL is the TTL for cached results.
	cacheTTL time.Duration

	// queryType is the type of query.
	queryType string

	// consistent indicates whether to use consistent node selection.
	consistent bool

	// executionMode indicates whether to execute synchronously or asynchronously.
	executionMode ExecutionMode

	// resultsMu protects the results field.
	resultsMu sync.RWMutex

	// results stores the query results.
	results interface{}

	// columns stores the column information.
	columns []string

	// columnTypes stores the column type information.
	columnTypes []*sql.ColumnType
}

// NewQueryExecutor creates a new QueryExecutor.
func NewQueryExecutor(
	starrocksClient client.StarRocksClient,
	metricsRegistry *metrics.Registry,
	query string,
	database string,
	user string,
	parameters []interface{},
	timeout time.Duration,
	priority int,
	requestID string,
	targetNodes []string,
	useCache bool,
	cacheTTL time.Duration,
	consistent bool,
	executionMode ExecutionMode,
	retryPolicy *RetryPolicy,
) *QueryExecutor {
	if retryPolicy == nil {
		retryPolicy = DefaultRetryPolicy()
	}

	base := newBaseExecutor(
		ExecutorTypeQuery,
		starrocksClient,
		metricsRegistry,
		retryPolicy,
	)

	if requestID != "" {
		base.logger = base.logger.With(zap.String("request_id", requestID))
	}

	return &QueryExecutor{
		baseExecutor:  base,
		query:         query,
		database:      database,
		user:          user,
		parameters:    parameters,
		timeout:       timeout,
		priority:      priority,
		requestID:     requestID,
		targetNodes:   targetNodes,
		useCache:      useCache,
		cacheTTL:      cacheTTL,
		consistent:    consistent,
		executionMode: executionMode,
		results:       nil,
		columns:       make([]string, 0),
		columnTypes:   make([]*sql.ColumnType, 0),
	}
}

// Execute executes the query and returns the result.
func (e *QueryExecutor) Execute(ctx context.Context) (*ExecutorResult, error) {
	// Store the context
	if ctx != nil {
		// Create a new context with timeout
		newCtx, cancel := context.WithTimeout(ctx, e.timeout)
		e.ctx = newCtx

		// Replace the original cancel function
		oldCancel := e.cancelFunc
		e.cancelFunc = func() {
			oldCancel()
			cancel()
		}

		defer cancel()
	}

	// Update status and record start time
	e.setStatus(ExecutorStatusRunning)
	e.startTime = time.Now()

	// Initialize retry count
	e.retryCount = 0

	// Validate the query
	if err := e.validateQuery(); err != nil {
		e.handleError(err)
		return e.prepareResult(), err
	}

	// Check cache if enabled
	if e.useCache {
		cacheHit, cacheResult, err := e.checkCache()
		if err != nil {
			e.logger.Warn("Error checking cache", zap.Error(err))
		} else if cacheHit {
			e.cacheHit = true
			e.handleCacheHit(cacheResult)
			return e.prepareResult(), nil
		}
	}

	// Execute the query with retries
	var lastError error
	for {
		// Check if context is done
		if e.ctx.Err() != nil {
			if errors.Is(e.ctx.Err(), context.Canceled) {
				e.setStatus(ExecutorStatusCancelled)
				return e.prepareResult(), e.ctx.Err()
			}
			e.handleError(e.ctx.Err())
			return e.prepareResult(), e.ctx.Err()
		}

		// Select a node to execute on
		nodeID, err := e.selectNode()
		if err != nil {
			e.handleError(err)
			return e.prepareResult(), err
		}
		e.nodeID = nodeID

		// Execute the query
		result, err := e.executeQuery(nodeID)
		if err != nil {
			lastError = err
			if e.shouldRetry(err) {
				e.retryCount++
				e.retryErrors = append(e.retryErrors, err.Error())
				e.logger.Warn("Retrying query after error",
					zap.Error(err),
					zap.Int("retry", e.retryCount),
					zap.Int("max_retries", e.retryPolicy.MaxRetries))
				e.sleep()
				continue
			}
			e.handleError(err)
			return e.prepareResult(), err
		}

		// Process the result
		err = e.processResult(result)
		if err != nil {
			e.handleError(err)
			return e.prepareResult(), err
		}

		// Cache the result if enabled
		if e.useCache && !e.cacheHit {
			e.cacheResult()
		}

		// Update metrics
		e.updateMetrics(true)

		// Mark as completed
		e.endTime = time.Now()
		e.setStatus(ExecutorStatusCompleted)

		return e.prepareResult(), nil
	}
}

// validateQuery validates the query.
func (e *QueryExecutor) validateQuery() error {
	if e.query == "" {
		return errors.New("empty query")
	}

	// Determine query type
	e.queryType = e.classifyQuery(e.query)

	// Validate parameters if they exist
	if len(e.parameters) > 0 {
		// Count the number of parameter placeholders in the query
		placeholderCount := utils.CountSQLPlaceholders(e.query)
		if placeholderCount != len(e.parameters) {
			return fmt.Errorf("parameter count mismatch: expected %d parameters, got %d",
				placeholderCount, len(e.parameters))
		}
	}

	return nil
}

// selectNode selects a node to execute the query on.
func (e *QueryExecutor) selectNode() (string, error) {
	if len(e.targetNodes) == 0 {
		return "", errors.New("no target nodes specified")
	}

	// If consistent node selection is enabled, select based on query hash
	if e.consistent {
		// Use the query and parameters to compute a hash
		hash := utils.HashString(e.query + fmt.Sprintf("%v", e.parameters))
		idx := hash % uint32(len(e.targetNodes))
		return e.targetNodes[idx], nil
	}

	// Otherwise, select the node based on client's node selection strategy
	return e.client.SelectNode(e.targetNodes, client.QueryOperation)
}

// executeQuery executes the query on the specified node.
func (e *QueryExecutor) executeQuery(nodeID string) (*client.QueryResult, error) {
	e.logger.Debug("Executing query",
		zap.String("node_id", nodeID),
		zap.String("query", e.query),
		zap.String("database", e.database),
		zap.String("user", e.user),
		zap.Duration("timeout", e.timeout),
		zap.Int("priority", e.priority),
		zap.Int("parameter_count", len(e.parameters)))

	// Prepare the query request
	queryRequest := &client.QueryRequest{
		Query:      e.query,
		Database:   e.database,
		User:       e.user,
		Parameters: e.parameters,
		Timeout:    e.timeout,
		Priority:   e.priority,
		RequestID:  e.requestID,
		Context:    e.ctx,
	}

	// Execute the query
	return e.client.ExecuteQuery(nodeID, queryRequest)
}

// processResult processes the query result.
func (e *QueryExecutor) processResult(result *client.QueryResult) error {
	if result == nil {
		return errors.New("nil query result")
	}

	e.resultsMu.Lock()
	defer e.resultsMu.Unlock()

	e.results = result.Data
	e.columns = result.Columns
	e.columnTypes = result.ColumnTypes
	e.rowCount = int64(result.RowCount)
	e.bytesProcessed = result.BytesProcessed

	return nil
}

// checkCache checks if the result is in the cache.
func (e *QueryExecutor) checkCache() (bool, *client.QueryResult, error) {
	// Create a cache key from the query and parameters
	cacheKey := utils.GenerateCacheKey(e.query, e.parameters, e.database, e.user)

	e.logger.Debug("Checking cache", zap.String("cache_key", cacheKey))

	// Try to get from cache
	result, err := e.client.GetFromCache(cacheKey)
	if err != nil {
		if errors.Is(err, client.ErrCacheMiss) {
			return false, nil, nil
		}
		return false, nil, err
	}

	return true, result, nil
}

// cacheResult caches the query result.
func (e *QueryExecutor) cacheResult() {
	e.resultsMu.RLock()
	defer e.resultsMu.RUnlock()

	if e.results == nil {
		e.logger.Debug("Not caching nil result")
		return
	}

	// Create a cache key from the query and parameters
	cacheKey := utils.GenerateCacheKey(e.query, e.parameters, e.database, e.user)

	// Create a result to cache
	result := &client.QueryResult{
		Data:           e.results,
		Columns:        e.columns,
		ColumnTypes:    e.columnTypes,
		RowCount:       int(e.rowCount),
		BytesProcessed: e.bytesProcessed,
	}

	e.logger.Debug("Caching result",
		zap.String("cache_key", cacheKey),
		zap.Duration("ttl", e.cacheTTL),
		zap.Int64("row_count", e.rowCount))

	// Cache the result
	err := e.client.StoreInCache(cacheKey, result, e.cacheTTL)
	if err != nil {
		e.logger.Warn("Failed to cache result", zap.Error(err))
	}
}

// handleCacheHit handles a cache hit.
func (e *QueryExecutor) handleCacheHit(result *client.QueryResult) {
	e.logger.Debug("Cache hit")

	e.cacheHit = true
	e.processResult(result)
	e.updateMetrics(true)
	e.endTime = time.Now()
	e.setStatus(ExecutorStatusCompleted)
}

// handleError handles an error.
func (e *QueryExecutor) handleError(err error) {
	e.logger.Error("Query execution error", zap.Error(err))

	e.errorMessage = err.Error()
	e.endTime = time.Now()
	e.setStatus(ExecutorStatusFailed)
	e.updateMetrics(false)
}

// updateMetrics updates the metrics for this execution.
func (e *QueryExecutor) updateMetrics(success bool) {
	if e.metrics == nil {
		return
	}

	duration := time.Since(e.startTime)

	// Update query execution time
	e.metrics.ObserveHistogram("query_execution_time", duration.Seconds())

	// Update query count
	e.metrics.IncrementCounter("queries_total")

	if success {
		e.metrics.IncrementCounter("queries_successful")
	} else {
		e.metrics.IncrementCounter("queries_failed")
	}

	// Update query type counts
	if e.queryType != "" {
		e.metrics.IncrementCounterWithLabels("query_type", map[string]string{
			"type": e.queryType,
		})
	}

	// Update retry counts
	if e.retryCount > 0 {
		e.metrics.IncrementCounterWithLabels("query_retries", map[string]string{
			"count": fmt.Sprintf("%d", e.retryCount),
		})
	}

	// Update cache metrics
	if e.useCache {
		if e.cacheHit {
			e.metrics.IncrementCounter("cache_hits")
		} else {
			e.metrics.IncrementCounter("cache_misses")
		}
	}

	// Update row count
	e.metrics.AddToGauge("rows_processed", float64(e.rowCount))

	// Update bytes processed
	e.metrics.AddToGauge("bytes_processed", float64(e.bytesProcessed))
}

// prepareResult prepares the execution result.
func (e *QueryExecutor) prepareResult() *ExecutorResult {
	e.resultsMu.RLock()
	defer e.resultsMu.RUnlock()

	status := e.GetStatus()
	stats := e.GetStats()

	var resultError error
	if status == ExecutorStatusFailed {
		resultError = errors.New(e.errorMessage)
	}

	return &ExecutorResult{
		Status:        status,
		Data:          e.results,
		Columns:       e.columns,
		ColumnTypes:   e.columnTypes,
		RowsAffected:  0, // Not applicable for queries
		ExecutionTime: stats.Duration,
		Stats:         stats,
		Warnings:      []string{}, // TODO: populate warnings
		Error:         resultError,
		NodeID:        e.nodeID,
		CacheHit:      e.cacheHit,
	}
}

// classifyQuery analyzes a query to determine its type.
func (e *QueryExecutor) classifyQuery(query string) string {
	// This is a simple classification based on the first word
	// A production implementation would use a proper SQL parser
	trimmedQuery := utils.TrimAndLowerCase(query)

	if utils.HasPrefix(trimmedQuery, "select") {
		return "select"
	} else if utils.HasPrefix(trimmedQuery, "show") {
		return "show"
	} else if utils.HasPrefix(trimmedQuery, "describe") || utils.HasPrefix(trimmedQuery, "desc") {
		return "describe"
	} else if utils.HasPrefix(trimmedQuery, "explain") {
		return "explain"
	} else if utils.HasPrefix(trimmedQuery, "analyze") {
		return "analyze"
	} else if utils.HasPrefix(trimmedQuery, "set") {
		return "set"
	} else if utils.HasPrefix(trimmedQuery, "use") {
		return "use"
	}

	return "other"
}

// WriteExecutor is responsible for executing write requests.
type WriteExecutor struct {
	*baseExecutor

	// statement is the SQL statement to execute.
	statement string

	// database is the database to execute the statement against.
	database string

	// user is the user executing the statement.
	user string

	// parameters are the parameters for the statement.
	parameters []interface{}

	// timeout is the maximum time to allow for statement execution.
	timeout time.Duration

	// priority is the priority of the statement.
	priority int

	// requestID is the ID of the request.
	requestID string

	// targetNodes are the nodes to execute the statement on.
	targetNodes []string

	// writeType is the type of write operation.
	writeType string

	// executionMode indicates whether to execute synchronously or asynchronously.
	executionMode ExecutionMode

	// resultsMu protects the results field.
	resultsMu sync.RWMutex

	// rowsAffected is the number of rows affected by the write operation.
	rowsAffected int64

	// useTransactions indicates whether to use transactions.
	useTransactions bool

	// isolationLevel is the transaction isolation level.
	isolationLevel sql.IsolationLevel
}

// NewWriteExecutor creates a new WriteExecutor.
func NewWriteExecutor(
	starrocksClient client.StarRocksClient,
	metricsRegistry *metrics.Registry,
	statement string,
	database string,
	user string,
	parameters []interface{},
	timeout time.Duration,
	priority int,
	requestID string,
	targetNodes []string,
	executionMode ExecutionMode,
	useTransactions bool,
	isolationLevel sql.IsolationLevel,
	retryPolicy *RetryPolicy,
) *WriteExecutor {
	if retryPolicy == nil {
		retryPolicy = DefaultRetryPolicy()
	}

	base := newBaseExecutor(
		ExecutorTypeWrite,
		starrocksClient,
		metricsRegistry,
		retryPolicy,
	)

	if requestID != "" {
		base.logger = base.logger.With(zap.String("request_id", requestID))
	}

	return &WriteExecutor{
		baseExecutor:    base,
		statement:       statement,
		database:        database,
		user:            user,
		parameters:      parameters,
		timeout:         timeout,
		priority:        priority,
		requestID:       requestID,
		targetNodes:     targetNodes,
		executionMode:   executionMode,
		rowsAffected:    0,
		useTransactions: useTransactions,
		isolationLevel:  isolationLevel,
	}
}

// Execute executes the write statement and returns the result.
func (e *WriteExecutor) Execute(ctx context.Context) (*ExecutorResult, error) {
	// Store the context
	if ctx != nil {
		// Create a new context with timeout
		newCtx, cancel := context.WithTimeout(ctx, e.timeout)
		e.ctx = newCtx

		// Replace the original cancel function
		oldCancel := e.cancelFunc
		e.cancelFunc = func() {
			oldCancel()
			cancel()
		}

		defer cancel()
	}

	// Update status and record start time
	e.setStatus(ExecutorStatusRunning)
	e.startTime = time.Now()

	// Initialize retry count
	e.retryCount = 0

	// Validate the statement
	if err := e.validateStatement(); err != nil {
		e.handleError(err)
		return e.prepareResult(), err
	}

	// Execute the statement with retries
	var lastError error
	for {
		// Check if context is done
		if e.ctx.Err() != nil {
			if errors.Is(e.ctx.Err(), context.Canceled) {
				e.setStatus(ExecutorStatusCancelled)
				return e.prepareResult(), e.ctx.Err()
			}
			e.handleError(e.ctx.Err())
			return e.prepareResult(), e.ctx.Err()
		}

		// Select a node to execute on
		nodeID, err := e.selectNode()
		if err != nil {
			e.handleError(err)
			return e.prepareResult(), err
		}
		e.nodeID = nodeID

		// Execute the statement
		var result *client.WriteResult
		if e.useTransactions {
			result, err = e.executeWithTransaction(nodeID)
		} else {
			result, err = e.executeStatement(nodeID)
		}

		if err != nil {
			lastError = err
			if e.shouldRetry(err) {
				e.retryCount++
				e.retryErrors = append(e.retryErrors, err.Error())
				e.logger.Warn("Retrying write after error",
					zap.Error(err),
					zap.Int("retry", e.retryCount),
					zap.Int("max_retries", e.retryPolicy.MaxRetries))
				e.sleep()
				continue
			}
			e.handleError(err)
			return e.prepareResult(), err
		}

		// Process the result
		err = e.processResult(result)
		if err != nil {
			e.handleError(err)
			return e.prepareResult(), err
		}

		// Update metrics
		e.updateMetrics(true)

		// Mark as completed
		e.endTime = time.Now()
		e.setStatus(ExecutorStatusCompleted)

		return e.prepareResult(), nil
	}
}

// validateStatement validates the write statement.
func (e *WriteExecutor) validateStatement() error {
	if e.statement == "" {
		return errors.New("empty statement")
	}

	// Determine write type
	e.writeType = e.classifyWrite(e.statement)

	// Validate parameters if they exist
	if len(e.parameters) > 0 {
		// Count the number of parameter placeholders in the statement
		placeholderCount := utils.CountSQLPlaceholders(e.statement)
		if placeholderCount != len(e.parameters) {
			return fmt.Errorf("parameter count mismatch: expected %d parameters, got %d",
				placeholderCount, len(e.parameters))
		}
	}

	return nil
}

// selectNode selects a node to execute the statement on.
func (e *WriteExecutor) selectNode() (string, error) {
	if len(e.targetNodes) == 0 {
		return "", errors.New("no target nodes specified")
	}

	// For writes, we prefer primary nodes when available
	return e.client.SelectNode(e.targetNodes, client.WriteOperation)
}

// executeStatement executes the statement on the specified node.
func (e *WriteExecutor) executeStatement(nodeID string) (*client.WriteResult, error) {
	e.logger.Debug("Executing write statement",
		zap.String("node_id", nodeID),
		zap.String("statement", e.statement),
		zap.String("database", e.database),
		zap.String("user", e.user),
		zap.Duration("timeout", e.timeout),
		zap.Int("priority", e.priority),
		zap.Int("parameter_count", len(e.parameters)))

	// Prepare the write request
	writeRequest := &client.WriteRequest{
		Statement:  e.statement,
		Database:   e.database,
		User:       e.user,
		Parameters: e.parameters,
		Timeout:    e.timeout,
		Priority:   e.priority,
		RequestID:  e.requestID,
		Context:    e.ctx,
	}

	// Execute the statement
	return e.client.ExecuteWrite(nodeID, writeRequest)
}

// executeWithTransaction executes the statement within a transaction.
func (e *WriteExecutor) executeWithTransaction(nodeID string) (*client.WriteResult, error) {
	e.logger.Debug("Executing write statement in transaction",
		zap.String("node_id", nodeID),
		zap.String("statement", e.statement),
		zap.String("database", e.database),
		zap.String("user", e.user),
		zap.Duration("timeout", e.timeout),
		zap.Int("priority", e.priority),
		zap.Int("parameter_count", len(e.parameters)),
		zap.Int("isolation_level", int(e.isolationLevel)))

	// Prepare the transaction request
	txRequest := &client.TransactionRequest{
		Database:       e.database,
		User:           e.user,
		Timeout:        e.timeout,
		IsolationLevel: e.isolationLevel,
		RequestID:      e.requestID,
		Context:        e.ctx,
	}

	// Start transaction
	tx, err := e.client.BeginTransaction(nodeID, txRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Prepare to rollback on failure
	defer func() {
		if tx != nil {
			// If we get here with tx still set, it means we didn't commit
			e.logger.Debug("Rolling back transaction")
			tx.Rollback()
		}
	}()

	// Prepare the write request
	writeRequest := &client.WriteRequest{
		Statement:   e.statement,
		Database:    e.database,
		User:        e.user,
		Parameters:  e.parameters,
		Timeout:     e.timeout,
		Priority:    e.priority,
		RequestID:   e.requestID,
		Context:     e.ctx,
		Transaction: tx,
	}

	// Execute the statement
	result, err := e.client.ExecuteWriteInTransaction(nodeID, writeRequest)
	if err != nil {
		return nil, err
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Clear tx to avoid rollback
	tx = nil

	return result, nil
}

// processResult processes the write result.
func (e *WriteExecutor) processResult(result *client.WriteResult) error {
	if result == nil {
		return errors.New("nil write result")
	}

	e.resultsMu.Lock()
	defer e.resultsMu.Unlock()

	e.rowsAffected = result.RowsAffected
	e.bytesProcessed = result.BytesProcessed

	return nil
}

// handleError handles an error.
func (e *WriteExecutor) handleError(err error) {
	e.logger.Error("Write execution error", zap.Error(err))

	e.errorMessage = err.Error()
	e.endTime = time.Now()
	e.setStatus(ExecutorStatusFailed)
	e.updateMetrics(false)
}

// updateMetrics updates the metrics for this execution.
func (e *WriteExecutor) updateMetrics(success bool) {
	if e.metrics == nil {
		return
	}

	duration := time.Since(e.startTime)

	// Update write execution time
	e.metrics.ObserveHistogram("write_execution_time", duration.Seconds())

	// Update write count
	e.metrics.IncrementCounter("writes_total")

	if success {
		e.metrics.IncrementCounter("writes_successful")
	} else {
		e.metrics.IncrementCounter("writes_failed")
	}

	// Update write type counts
	if e.writeType != "" {
		e.metrics.IncrementCounterWithLabels("write_type", map[string]string{
			"type": e.writeType,
		})
	}

	// Update retry counts
	if e.retryCount > 0 {
		e.metrics.IncrementCounterWithLabels("write_retries", map[string]string{
			"count": fmt.Sprintf("%d", e.retryCount),
		})
	}

	// Update rows affected
	e.metrics.AddToGauge("rows_affected", float64(e.rowsAffected))

	// Update bytes processed
	e.metrics.AddToGauge("bytes_processed", float64(e.bytesProcessed))
}

// prepareResult prepares the execution result.
func (e *WriteExecutor) prepareResult() *ExecutorResult {
	e.resultsMu.RLock()
	defer e.resultsMu.RUnlock()

	status := e.GetStatus()
	stats := e.GetStats()

	var resultError error
	if status == ExecutorStatusFailed {
		resultError = errors.New(e.errorMessage)
	}

	return &ExecutorResult{
		Status:        status,
		Data:          nil, // Not applicable for writes
		Columns:       nil, // Not applicable for writes
		ColumnTypes:   nil, // Not applicable for writes
		RowsAffected:  e.rowsAffected,
		ExecutionTime: stats.Duration,
		Stats:         stats,
		Warnings:      []string{}, // TODO: populate warnings
		Error:         resultError,
		NodeID:        e.nodeID,
		CacheHit:      false, // Not applicable for writes
	}
}

// classifyWrite analyzes a write statement to determine its type.
func (e *WriteExecutor) classifyWrite(statement string) string {
	// This is a simple classification based on the first word
	// A production implementation would use a proper SQL parser
	trimmedStatement := utils.TrimAndLowerCase(statement)

	if utils.HasPrefix(trimmedStatement, "insert") {
		return "insert"
	} else if utils.HasPrefix(trimmedStatement, "update") {
		return "update"
	} else if utils.HasPrefix(trimmedStatement, "delete") {
		return "delete"
	} else if utils.HasPrefix(trimmedStatement, "create") {
		return "create"
	} else if utils.HasPrefix(trimmedStatement, "alter") {
		return "alter"
	} else if utils.HasPrefix(trimmedStatement, "drop") {
		return "drop"
	} else if utils.HasPrefix(trimmedStatement, "truncate") {
		return "truncate"
	}

	return "other"
}

// ExecutorFactory creates executors based on the request type.
type ExecutorFactory struct {
	client  client.StarRocksClient
	metrics *metrics.Registry
	logger  *zap.Logger
}

// NewExecutorFactory creates a new ExecutorFactory.
func NewExecutorFactory(
	client client.StarRocksClient,
	metrics *metrics.Registry,
) *ExecutorFactory {
	return &ExecutorFactory{
		client:  client,
		metrics: metrics,
		logger:  logging.GetLogger().Named("executor_factory"),
	}
}

// CreateQueryExecutor creates a new QueryExecutor.
func (f *ExecutorFactory) CreateQueryExecutor(
	query string,
	database string,
	user string,
	parameters []interface{},
	timeout time.Duration,
	priority int,
	requestID string,
	targetNodes []string,
	useCache bool,
	cacheTTL time.Duration,
	consistent bool,
	executionMode ExecutionMode,
	retryPolicy *RetryPolicy,
) Executor {
	return NewQueryExecutor(
		f.client,
		f.metrics,
		query,
		database,
		user,
		parameters,
		timeout,
		priority,
		requestID,
		targetNodes,
		useCache,
		cacheTTL,
		consistent,
		executionMode,
		retryPolicy,
	)
}

// CreateWriteExecutor creates a new WriteExecutor.
func (f *ExecutorFactory) CreateWriteExecutor(
	statement string,
	database string,
	user string,
	parameters []interface{},
	timeout time.Duration,
	priority int,
	requestID string,
	targetNodes []string,
	executionMode ExecutionMode,
	useTransactions bool,
	isolationLevel sql.IsolationLevel,
	retryPolicy *RetryPolicy,
) Executor {
	return NewWriteExecutor(
		f.client,
		f.metrics,
		statement,
		database,
		user,
		parameters,
		timeout,
		priority,
		requestID,
		targetNodes,
		executionMode,
		useTransactions,
		isolationLevel,
		retryPolicy,
	)
}

// ExecutorManager manages execution of requests.
type ExecutorManager struct {
	// factory is the factory for creating executors.
	factory *ExecutorFactory

	// logger is the logger for this manager.
	logger *zap.Logger

	// activeExecutors tracks active executors.
	activeExecutors sync.Map

	// activeCount is the count of active executors.
	activeCount int64

	// metrics is the metrics registry.
	metrics *metrics.Registry
}

// NewExecutorManager creates a new ExecutorManager.
func NewExecutorManager(
	factory *ExecutorFactory,
	metrics *metrics.Registry,
) *ExecutorManager {
	return &ExecutorManager{
		factory:         factory,
		logger:          logging.GetLogger().Named("executor_manager"),
		activeExecutors: sync.Map{},
		metrics:         metrics,
	}
}

// Submit submits an executor for execution.
func (m *ExecutorManager) Submit(ctx context.Context, executor Executor) (*ExecutorResult, error) {
	// Register the executor
	m.registerExecutor(executor)

	// Execute the request
	result, err := executor.Execute(ctx)

	// Unregister the executor
	m.unregisterExecutor(executor.GetID())

	return result, err
}

// SubmitAsync submits an executor for asynchronous execution.
func (m *ExecutorManager) SubmitAsync(ctx context.Context, executor Executor) (string, error) {
	// Register the executor
	m.registerExecutor(executor)

	// Start execution in a goroutine
	go func() {
		_, err := executor.Execute(ctx)
		if err != nil {
			m.logger.Error("Async execution error",
				zap.String("executor_id", executor.GetID()),
				zap.Error(err))
		}

		// Unregister the executor
		m.unregisterExecutor(executor.GetID())
	}()

	return executor.GetID(), nil
}

// GetExecutor gets an executor by ID.
func (m *ExecutorManager) GetExecutor(id string) (Executor, bool) {
	value, ok := m.activeExecutors.Load(id)
	if !ok {
		return nil, false
	}

	executor, ok := value.(Executor)
	return executor, ok
}

// CancelExecutor cancels an executor by ID.
func (m *ExecutorManager) CancelExecutor(id string) error {
	executor, ok := m.GetExecutor(id)
	if !ok {
		return fmt.Errorf("executor not found: %s", id)
	}

	return executor.Cancel()
}

// GetActiveCount returns the count of active executors.
func (m *ExecutorManager) GetActiveCount() int {
	return int(atomic.LoadInt64(&m.activeCount))
}

// registerExecutor registers an executor with the manager.
func (m *ExecutorManager) registerExecutor(executor Executor) {
	m.activeExecutors.Store(executor.GetID(), executor)
	atomic.AddInt64(&m.activeCount, 1)

	if m.metrics != nil {
		m.metrics.SetGauge("active_executors", float64(atomic.LoadInt64(&m.activeCount)))

		if executor.GetType() == ExecutorTypeQuery {
			m.metrics.IncrementCounter("active_queries")
		} else if executor.GetType() == ExecutorTypeWrite {
			m.metrics.IncrementCounter("active_writes")
		}
	}
}

// unregisterExecutor unregisters an executor from the manager.
func (m *ExecutorManager) unregisterExecutor(id string) {
	value, loaded := m.activeExecutors.LoadAndDelete(id)
	if loaded {
		atomic.AddInt64(&m.activeCount, -1)

		if m.metrics != nil {
			m.metrics.SetGauge("active_executors", float64(atomic.LoadInt64(&m.activeCount)))

			if executor, ok := value.(Executor); ok {
				if executor.GetType() == ExecutorTypeQuery {
					m.metrics.DecrementCounter("active_queries")
				} else if executor.GetType() == ExecutorTypeWrite {
					m.metrics.DecrementCounter("active_writes")
				}
			}
		}
	}
}

// BatchExecutor is responsible for executing multiple statements in a batch.
type BatchExecutor struct {
	*baseExecutor

	// statements are the SQL statements to execute.
	statements []string

	// database is the database to execute the statements against.
	database string

	// user is the user executing the statements.
	user string

	// parameterSets are the sets of parameters for the statements.
	parameterSets [][]interface{}

	// timeout is the maximum time to allow for batch execution.
	timeout time.Duration

	// priority is the priority of the batch.
	priority int

	// requestID is the ID of the request.
	requestID string

	// targetNodes are the nodes to execute the statements on.
	targetNodes []string

	// executionMode indicates whether to execute synchronously or asynchronously.
	executionMode ExecutionMode

	// resultsMu protects the results fields.
	resultsMu sync.RWMutex

	// results contains the results for each statement.
	results []*ExecutorResult

	// useTransactions indicates whether to use a transaction for the batch.
	useTransactions bool

	// isolationLevel is the transaction isolation level.
	isolationLevel sql.IsolationLevel

	// executorFactory is the factory for creating individual executors.
	executorFactory *ExecutorFactory

	// failFast indicates whether to stop execution on first error.
	failFast bool
}

// NewBatchExecutor creates a new BatchExecutor.
func NewBatchExecutor(
	starrocksClient client.StarRocksClient,
	metricsRegistry *metrics.Registry,
	statements []string,
	database string,
	user string,
	parameterSets [][]interface{},
	timeout time.Duration,
	priority int,
	requestID string,
	targetNodes []string,
	executionMode ExecutionMode,
	useTransactions bool,
	isolationLevel sql.IsolationLevel,
	failFast bool,
	retryPolicy *RetryPolicy,
) *BatchExecutor {
	if retryPolicy == nil {
		retryPolicy = DefaultRetryPolicy()
	}

	base := newBaseExecutor(
		ExecutorTypeWrite, // Default to write since batches often involve writes
		starrocksClient,
		metricsRegistry,
		retryPolicy,
	)

	if requestID != "" {
		base.logger = base.logger.With(zap.String("request_id", requestID))
	}

	executorFactory := &ExecutorFactory{
		client:  starrocksClient,
		metrics: metricsRegistry,
		logger:  base.logger.Named("executor_factory"),
	}

	return &BatchExecutor{
		baseExecutor:    base,
		statements:      statements,
		database:        database,
		user:            user,
		parameterSets:   parameterSets,
		timeout:         timeout,
		priority:        priority,
		requestID:       requestID,
		targetNodes:     targetNodes,
		executionMode:   executionMode,
		results:         make([]*ExecutorResult, len(statements)),
		useTransactions: useTransactions,
		isolationLevel:  isolationLevel,
		executorFactory: executorFactory,
		failFast:        failFast,
	}
}

// Execute executes the batch and returns the result.
func (e *BatchExecutor) Execute(ctx context.Context) (*ExecutorResult, error) {
	// Store the context
	if ctx != nil {
		// Create a new context with timeout
		newCtx, cancel := context.WithTimeout(ctx, e.timeout)
		e.ctx = newCtx

		// Replace the original cancel function
		oldCancel := e.cancelFunc
		e.cancelFunc = func() {
			oldCancel()
			cancel()
		}

		defer cancel()
	}

	// Update status and record start time
	e.setStatus(ExecutorStatusRunning)
	e.startTime = time.Now()

	// Validate the batch
	if err := e.validateBatch(); err != nil {
		e.handleError(err)
		return e.prepareResult(), err
	}

	// Execute the batch
	var err error
	if e.useTransactions {
		err = e.executeInTransaction()
	} else {
		err = e.executeIndividually()
	}

	if err != nil {
		e.handleError(err)
		return e.prepareResult(), err
	}

	// Mark as completed
	e.endTime = time.Now()
	e.setStatus(ExecutorStatusCompleted)

	// Update metrics
	e.updateMetrics(true)

	return e.prepareResult(), nil
}

// validateBatch validates the batch.
func (e *BatchExecutor) validateBatch() error {
	if len(e.statements) == 0 {
		return errors.New("empty batch")
	}

	// Validate statements and parameter sets
	if len(e.parameterSets) > 0 && len(e.parameterSets) != len(e.statements) {
		return fmt.Errorf("parameter sets count (%d) doesn't match statements count (%d)",
			len(e.parameterSets), len(e.statements))
	}

	// Validate individual statements
	for i, stmt := range e.statements {
		if stmt == "" {
			return fmt.Errorf("empty statement at index %d", i)
		}

		// Validate parameters if they exist for this statement
		if len(e.parameterSets) > i && len(e.parameterSets[i]) > 0 {
			// Count the number of parameter placeholders in the statement
			placeholderCount := utils.CountSQLPlaceholders(stmt)
			if placeholderCount != len(e.parameterSets[i]) {
				return fmt.Errorf("parameter count mismatch at index %d: expected %d parameters, got %d",
					i, placeholderCount, len(e.parameterSets[i]))
			}
		}
	}

	return nil
}

// executeInTransaction executes all statements in a single transaction.
func (e *BatchExecutor) executeInTransaction() error {
	e.logger.Debug("Executing batch in transaction",
		zap.Int("statement_count", len(e.statements)),
		zap.String("database", e.database),
		zap.String("user", e.user),
		zap.Duration("timeout", e.timeout),
		zap.Int("priority", e.priority),
		zap.Int("isolation_level", int(e.isolationLevel)))

	// Select a node to execute on
	nodeID, err := e.selectNode()
	if err != nil {
		return err
	}
	e.nodeID = nodeID

	// Prepare the transaction request
	txRequest := &client.TransactionRequest{
		Database:       e.database,
		User:           e.user,
		Timeout:        e.timeout,
		IsolationLevel: e.isolationLevel,
		RequestID:      e.requestID,
		Context:        e.ctx,
	}

	// Start transaction
	tx, err := e.client.BeginTransaction(nodeID, txRequest)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Prepare to rollback on failure
	var success bool
	defer func() {
		if tx != nil && !success {
			// If we get here with tx still set and not successful, it means we didn't commit
			e.logger.Debug("Rolling back transaction")
			tx.Rollback()
		}
	}()

	// Execute each statement in the transaction
	for i, stmt := range e.statements {
		// Check if context is done
		if e.ctx.Err() != nil {
			return e.ctx.Err()
		}

		// Get parameters for this statement
		var params []interface{}
		if len(e.parameterSets) > i {
			params = e.parameterSets[i]
		}

		// Prepare the write request
		writeRequest := &client.WriteRequest{
			Statement:   stmt,
			Database:    e.database,
			User:        e.user,
			Parameters:  params,
			Timeout:     e.timeout,
			Priority:    e.priority,
			RequestID:   fmt.Sprintf("%s-%d", e.requestID, i),
			Context:     e.ctx,
			Transaction: tx,
		}

		// Execute the statement
		result, err := e.client.ExecuteWriteInTransaction(nodeID, writeRequest)
		if err != nil {
			e.logger.Error("Error executing statement in transaction",
				zap.Int("index", i),
				zap.String("statement", stmt),
				zap.Error(err))

			if e.failFast {
				return err
			}

			// Store the error result
			e.storeResult(i, &ExecutorResult{
				Status:        ExecutorStatusFailed,
				Error:         err,
				ExecutionTime: time.Since(e.startTime),
				NodeID:        nodeID,
			})

			continue
		}

		// Store the successful result
		e.storeResult(i, &ExecutorResult{
			Status:        ExecutorStatusCompleted,
			RowsAffected:  result.RowsAffected,
			ExecutionTime: time.Since(e.startTime),
			NodeID:        nodeID,
		})

		// Update row count
		atomic.AddInt64(&e.rowCount, result.RowsAffected)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Mark as successful
	success = true

	return nil
}

// executeIndividually executes each statement individually.
func (e *BatchExecutor) executeIndividually() error {
	e.logger.Debug("Executing batch individually",
		zap.Int("statement_count", len(e.statements)),
		zap.String("database", e.database),
		zap.String("user", e.user),
		zap.Duration("timeout", e.timeout),
		zap.Int("priority", e.priority))

	// Execute each statement individually
	for i, stmt := range e.statements {
		// Check if context is done
		if e.ctx.Err() != nil {
			return e.ctx.Err()
		}

		// Get parameters for this statement
		var params []interface{}
		if len(e.parameterSets) > i {
			params = e.parameterSets[i]
		}

		// Determine the executor type (query or write)
		var executor Executor
		if isQuery(stmt) {
			executor = e.executorFactory.CreateQueryExecutor(
				stmt,
				e.database,
				e.user,
				params,
				e.timeout,
				e.priority,
				fmt.Sprintf("%s-%d", e.requestID, i),
				e.targetNodes,
				false, // No cache for batch queries
				0,     // No cache TTL
				false, // No consistent routing
				ExecutionModeSync,
				e.retryPolicy,
			)
		} else {
			executor = e.executorFactory.CreateWriteExecutor(
				stmt,
				e.database,
				e.user,
				params,
				e.timeout,
				e.priority,
				fmt.Sprintf("%s-%d", e.requestID, i),
				e.targetNodes,
				ExecutionModeSync,
				false, // No transactions for individual statements
				sql.LevelDefault,
				e.retryPolicy,
			)
		}

		// Execute the statement
		result, err := executor.Execute(e.ctx)
		if err != nil {
			e.logger.Error("Error executing statement individually",
				zap.Int("index", i),
				zap.String("statement", stmt),
				zap.Error(err))

			if e.failFast {
				return err
			}

			// Store the error result
			e.storeResult(i, result)

			continue
		}

		// Store the successful result
		e.storeResult(i, result)

		// Update row count
		if result.RowsAffected > 0 {
			atomic.AddInt64(&e.rowCount, result.RowsAffected)
		} else if result.Data != nil {
			// For queries, count rows in the result set
			rows, ok := countRows(result.Data)
			if ok {
				atomic.AddInt64(&e.rowCount, int64(rows))
			}
		}
	}

	return nil
}

// selectNode selects a node to execute the batch on.
func (e *BatchExecutor) selectNode() (string, error) {
	if len(e.targetNodes) == 0 {
		return "", errors.New("no target nodes specified")
	}

	// For batches with transactions, prefer primary nodes
	return e.client.SelectNode(e.targetNodes, client.WriteOperation)
}

// storeResult stores a result for a statement.
func (e *BatchExecutor) storeResult(index int, result *ExecutorResult) {
	e.resultsMu.Lock()
	defer e.resultsMu.Unlock()

	if index >= 0 && index < len(e.results) {
		e.results[index] = result
	}
}

// handleError handles an error.
func (e *BatchExecutor) handleError(err error) {
	e.logger.Error("Batch execution error", zap.Error(err))

	e.errorMessage = err.Error()
	e.endTime = time.Now()
	e.setStatus(ExecutorStatusFailed)
	e.updateMetrics(false)
}

// updateMetrics updates the metrics for this execution.
func (e *BatchExecutor) updateMetrics(success bool) {
	if e.metrics == nil {
		return
	}

	duration := time.Since(e.startTime)

	// Update batch execution time
	e.metrics.ObserveHistogram("batch_execution_time", duration.Seconds())

	// Update batch count
	e.metrics.IncrementCounter("batches_total")

	if success {
		e.metrics.IncrementCounter("batches_successful")
	} else {
		e.metrics.IncrementCounter("batches_failed")
	}

	// Update statement count
	e.metrics.AddToGauge("statements_executed", float64(len(e.statements)))

	// Update rows affected
	e.metrics.AddToGauge("rows_affected", float64(e.rowCount))
}

// prepareResult prepares the execution result.
func (e *BatchExecutor) prepareResult() *ExecutorResult {
	e.resultsMu.RLock()
	defer e.resultsMu.RUnlock()

	status := e.GetStatus()
	stats := e.GetStats()

	var resultError error
	if status == ExecutorStatusFailed {
		resultError = errors.New(e.errorMessage)
	}

	// Count successful and failed statements
	successCount := 0
	failCount := 0
	for _, res := range e.results {
		if res != nil {
			if res.Status == ExecutorStatusCompleted {
				successCount++
			} else if res.Status == ExecutorStatusFailed {
				failCount++
			}
		}
	}

	return &ExecutorResult{
		Status:        status,
		ExecutionTime: stats.Duration,
		Stats:         stats,
		Error:         resultError,
		NodeID:        e.nodeID,
		RowsAffected:  e.rowCount,
		Data:          e.results, // Return all individual results
	}
}

// isQuery determines if a statement is a query.
func isQuery(statement string) bool {
	trimmedStatement := utils.TrimAndLowerCase(statement)

	return utils.HasPrefix(trimmedStatement, "select") ||
		utils.HasPrefix(trimmedStatement, "show") ||
		utils.HasPrefix(trimmedStatement, "describe") ||
		utils.HasPrefix(trimmedStatement, "desc") ||
		utils.HasPrefix(trimmedStatement, "explain")
}

// countRows counts the number of rows in a result set.
func countRows(data interface{}) (int, bool) {
	// Handle different types of result data
	switch v := data.(type) {
	case []map[string]interface{}:
		return len(v), true
	case [][]interface{}:
		return len(v), true
	case []interface{}:
		return len(v), true
	}

	return 0, false
}

//Personal.AI order the ending
