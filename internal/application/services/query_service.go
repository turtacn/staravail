// Package services provides application-level services for the StarRocks proxy.
package services

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"math/rand" // For fallback random number generation
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/turtacn/staravail/internal/common/logging"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/config"
	"github.com/turtacn/staravail/internal/domain"
	"github.com/turtacn/staravail/internal/infrastructure/clients"
	"github.com/turtacn/staravail/internal/infrastructure/health"
	"github.com/turtacn/staravail/internal/query"
)

// QueryResultFormat represents the format of query results
type QueryResultFormat string

const (
	// QueryResultFormatJSON represents JSON format for query results
	QueryResultFormatJSON QueryResultFormat = "json"

	// QueryResultFormatCSV represents CSV format for query results
	QueryResultFormatCSV QueryResultFormat = "csv"

	// QueryResultFormatRaw represents raw format for query results
	QueryResultFormatRaw QueryResultFormat = "raw"

	// QueryResultFormatSQL represents SQL format for query results
	QueryResultFormatSQL QueryResultFormat = "sql"
)

// QueryOptions contains options for executing queries
type QueryOptions struct {
	// Timeout specifies the maximum duration for query execution
	Timeout time.Duration

	// MaxRows specifies the maximum number of rows to return
	MaxRows int

	// ResultFormat specifies the format of the query results
	ResultFormat QueryResultFormat

	// IgnoreWarnings indicates whether to ignore warnings
	IgnoreWarnings bool

	// EnableCache indicates whether to use the query cache
	EnableCache bool

	// CacheTTL specifies the TTL for cached query results
	CacheTTL time.Duration

	// User is the user executing the query
	User string

	// Database is the database to use for the query
	Database string

	// Labels are arbitrary key-value pairs associated with the query
	Labels map[string]string

	// TraceID is the trace ID for the query
	TraceID string

	// AllowPartialResults indicates whether to allow partial results
	AllowPartialResults bool

	// QueryID is an optional ID for the query
	QueryID string

	// Priority specifies the priority of the query
	Priority int

	// ExplainOptions are options for EXPLAIN queries
	ExplainOptions map[string]string
}

// DefaultQueryOptions returns the default query options
func DefaultQueryOptions() QueryOptions {
	return QueryOptions{
		Timeout:             30 * time.Second,
		MaxRows:             10000,
		ResultFormat:        QueryResultFormatJSON,
		IgnoreWarnings:      false,
		EnableCache:         true,
		CacheTTL:            5 * time.Minute,
		Labels:              make(map[string]string),
		AllowPartialResults: false,
		Priority:            1,
		ExplainOptions:      make(map[string]string),
	}
}

// QueryResult represents the result of a query
type QueryResult struct {
	// Columns contains the column definitions
	Columns []query.Column

	// Rows contains the result rows
	Rows [][]interface{}

	// RowCount is the number of rows returned
	RowCount int

	// ExecutionTime is the time it took to execute the query
	ExecutionTime time.Duration

	// Warnings contains any warnings that occurred during query execution
	Warnings []string

	// IsPartial indicates whether the results are partial
	IsPartial bool

	// CachedResult indicates whether the results came from cache
	CachedResult bool

	// QueryID is the ID of the query
	QueryID string

	// Metadata contains additional metadata about the query
	Metadata map[string]interface{}

	// Error is any error that occurred during query execution
	Error error
}

// QueryValidationResult represents the result of query validation
type QueryValidationResult struct {
	// Valid indicates whether the query is valid
	Valid bool

	// Errors contains any validation errors
	Errors []string

	// Warnings contains any validation warnings
	Warnings []string

	// RewrittenQuery is the rewritten query if any
	RewrittenQuery string

	// EstimatedCost is the estimated cost of the query
	EstimatedCost query.QueryCost
}

// QueryService defines the interface for query services
type QueryService interface {
	// ExecuteQuery executes a query and returns the results
	ExecuteQuery(ctx context.Context, queryStr string, options QueryOptions) (*QueryResult, error)

	// ExplainQuery explains a query's execution plan
	ExplainQuery(ctx context.Context, queryStr string, options QueryOptions) (*QueryResult, error)

	// ValidateQuery validates a query
	ValidateQuery(ctx context.Context, queryStr string, options QueryOptions) (*QueryValidationResult, error)

	// CancelQuery cancels a query with the given ID
	CancelQuery(ctx context.Context, queryID string) error

	// GetQueryStatus gets the status of a query
	GetQueryStatus(ctx context.Context, queryID string) (domain.QueryStatus, error)

	// GetRunningQueries gets all currently running queries
	GetRunningQueries(ctx context.Context) ([]domain.QueryInfo, error)
}

// StarRocksQueryService implements QueryService for StarRocks
type StarRocksQueryService struct {
	// Config is the configuration for the service
	Config *config.Config

	// Logger is the logger for the service
	Logger logging.Logger

	// Metrics is the metrics recorder for the service
	Metrics metrics.MetricsRecorder

	// StarRocksClient is the client for interacting with StarRocks
	StarRocksClient clients.StarRocksClient

	// QueryPlanner is used for planning query execution
	QueryPlanner query.QueryPlanner

	// HealthChecker is used for checking the health of components
	HealthChecker health.HealthChecker

	// QueryCache is used for caching query results
	QueryCache QueryCache

	// runningQueries tracks currently running queries
	runningQueries sync.Map

	// queryHistogram records query execution times
	queryHistogram *prometheus.HistogramVec

	// queryCounter counts queries by type and status
	queryCounter *prometheus.CounterVec

	// rowsProcessedCounter counts rows processed by queries
	rowsProcessedCounter *prometheus.CounterVec
}

// QueryCache defines the interface for a query cache
type QueryCache interface {
	// Get gets a cached query result
	Get(key string) (*QueryResult, bool)

	// Set sets a cached query result
	Set(key string, result *QueryResult, ttl time.Duration)

	// Delete deletes a cached query result
	Delete(key string)

	// Clear clears all cached query results
	Clear()
}

// NewStarRocksQueryService creates a new StarRocksQueryService
func NewStarRocksQueryService(
	config *config.Config,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
	starRocksClient clients.StarRocksClient,
	queryPlanner query.QueryPlanner,
	healthChecker health.HealthChecker,
	queryCache QueryCache,
) *StarRocksQueryService {
	// Create metrics
	queryHistogram := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "query_execution_time_seconds",
			Help:    "Histogram of query execution times",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 120},
		},
		[]string{"query_type", "status", "database"},
	)

	queryCounter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "queries_total",
			Help: "Total number of queries",
		},
		[]string{"query_type", "status", "database"},
	)

	rowsProcessedCounter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "query_rows_processed_total",
			Help: "Total number of rows processed by queries",
		},
		[]string{"query_type", "database"},
	)

	// Register metrics if a registry is provided
	if metricsRecorder != nil {
		if registry, ok := metricsRecorder.GetRegistry().(*prometheus.Registry); ok {
			registry.MustRegister(queryHistogram, queryCounter, rowsProcessedCounter)
		}
	}

	return &StarRocksQueryService{
		Config:               config,
		Logger:               logger,
		Metrics:              metricsRecorder,
		StarRocksClient:      starRocksClient,
		QueryPlanner:         queryPlanner,
		HealthChecker:        healthChecker,
		QueryCache:           queryCache,
		runningQueries:       sync.Map{},
		queryHistogram:       queryHistogram,
		queryCounter:         queryCounter,
		rowsProcessedCounter: rowsProcessedCounter,
	}
}

// ExecuteQuery executes a query and returns the results
func (s *StarRocksQueryService) ExecuteQuery(
	ctx context.Context,
	queryStr string,
	options QueryOptions,
) (*QueryResult, error) {
	startTime := time.Now()
	queryType := s.getQueryType(queryStr)
	database := options.Database
	queryID := options.QueryID

	// Generate query ID if not provided
	if queryID == "" {
		queryID = generateQueryID()
		options.QueryID = queryID
	}

	// Set up logger with query ID
	queryLogger := s.Logger.With(
		"query_id", queryID,
		"query_type", queryType,
		"database", database,
	)

	queryLogger.Info("Executing query",
		"query", truncateQuery(queryStr),
		"timeout", options.Timeout,
		"max_rows", options.MaxRows,
		"user", options.User,
	)

	// Check if query is cached
	if options.EnableCache && queryType == "SELECT" {
		cacheKey := s.generateCacheKey(queryStr, options)
		if cachedResult, found := s.QueryCache.Get(cacheKey); found {
			queryLogger.Debug("Query result found in cache")

			// Update cached result
			cachedResult.CachedResult = true
			cachedResult.ExecutionTime = time.Since(startTime)

			// Update metrics
			s.recordQueryMetrics(queryType, "cache_hit", database, cachedResult.RowCount, cachedResult.ExecutionTime)

			return cachedResult, nil
		}
	}

	// Create a timeout context if timeout is specified
	var cancelFunc context.CancelFunc = func() {}
	if options.Timeout > 0 {
		ctx, cancelFunc = context.WithTimeout(ctx, options.Timeout)
	}
	defer cancelFunc()

	// Track the running query
	runningQuery := &domain.QueryInfo{
		ID:        queryID,
		Query:     queryStr,
		StartTime: startTime,
		User:      options.User,
		Database:  database,
		Status:    domain.QueryStatusRunning,
	}
	s.runningQueries.Store(queryID, runningQuery)
	defer s.runningQueries.Delete(queryID)

	// Validate the query
	validationResult, err := s.ValidateQuery(ctx, queryStr, options)
	if err != nil {
		queryLogger.Error("Query validation failed", "error", err)
		return s.createErrorResult(err, startTime, queryID), err
	}

	if !validationResult.Valid {
		err := fmt.Errorf("invalid query: %s", strings.Join(validationResult.Errors, "; "))
		queryLogger.Error("Query is invalid", "errors", validationResult.Errors)
		return s.createErrorResult(err, startTime, queryID), err
	}

	// Check health of required components
	healthResult := s.HealthChecker.Check(ctx)
	if !healthResult.Healthy && !options.AllowPartialResults {
		err := fmt.Errorf("system is unhealthy: %s", healthResult.Message)
		queryLogger.Error("System is unhealthy", "health_result", healthResult)
		return s.createErrorResult(err, startTime, queryID), err
	}

	// Plan the query
	plan, err := s.QueryPlanner.PlanQuery(ctx, queryStr, &query.QueryPlannerOptions{
		Database:       database,
		User:           options.User,
		TraceID:        options.TraceID,
		AllowRewrite:   true,
		MaxCost:        s.Config.Query.MaxQueryCost,
		SystemHealth:   healthResult,
		ValidateResult: validationResult,
	})

	if err != nil {
		queryLogger.Error("Query planning failed", "error", err)
		return s.createErrorResult(err, startTime, queryID), err
	}

	queryLogger.Debug("Query plan created",
		"plan_type", plan.Type,
		"estimated_cost", plan.EstimatedCost,
		"estimated_rows", plan.EstimatedRows,
	)

	// Process the query plan
	var result *QueryResult
	switch plan.Type {
	case query.QueryPlanTypeOriginal:
		result, err = s.executeOriginalQuery(ctx, queryStr, options, queryLogger)
	case query.QueryPlanTypeRewrite:
		queryLogger.Info("Query will be rewritten",
			"original", truncateQuery(queryStr),
			"rewritten", truncateQuery(plan.RewrittenQuery),
		)
		result, err = s.executeOriginalQuery(ctx, plan.RewrittenQuery, options, queryLogger)
	case query.QueryPlanTypeRejected:
		err = fmt.Errorf("query rejected: %s", plan.RejectReason)
		queryLogger.Warn("Query rejected", "reason", plan.RejectReason)
		return s.createErrorResult(err, startTime, queryID), err
	default:
		err = fmt.Errorf("unknown query plan type: %s", plan.Type)
		queryLogger.Error("Unknown query plan type", "plan_type", plan.Type)
		return s.createErrorResult(err, startTime, queryID), err
	}

	// Handle errors
	if err != nil {
		queryLogger.Error("Query execution failed", "error", err)

		// Record metrics
		s.recordQueryMetrics(queryType, "error", database, 0, time.Since(startTime))

		return s.createErrorResult(err, startTime, queryID), err
	}

	// Update result metadata
	result.QueryID = queryID
	result.ExecutionTime = time.Since(startTime)
	result.IsPartial = !healthResult.Healthy && options.AllowPartialResults

	// Add warning for partial results
	if result.IsPartial {
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("Query returned partial results due to system health: %s", healthResult.Message))
	}

	// Cache the result if caching is enabled
	if options.EnableCache && queryType == "SELECT" && err == nil {
		cacheKey := s.generateCacheKey(queryStr, options)
		s.QueryCache.Set(cacheKey, result, options.CacheTTL)
		queryLogger.Debug("Query result cached", "ttl", options.CacheTTL)
	}

	// Record metrics
	s.recordQueryMetrics(queryType, "success", database, result.RowCount, result.ExecutionTime)

	queryLogger.Info("Query executed successfully",
		"rows", result.RowCount,
		"time", result.ExecutionTime,
		"partial", result.IsPartial,
	)

	return result, nil
}

// ExplainQuery explains a query's execution plan
func (s *StarRocksQueryService) ExplainQuery(
	ctx context.Context,
	queryStr string,
	options QueryOptions,
) (*QueryResult, error) {
	startTime := time.Now()
	queryType := "EXPLAIN"
	database := options.Database
	queryID := options.QueryID

	// Generate query ID if not provided
	if queryID == "" {
		queryID = generateQueryID()
		options.QueryID = queryID
	}

	// Set up logger with query ID
	queryLogger := s.Logger.With(
		"query_id", queryID,
		"query_type", queryType,
		"database", database,
	)

	queryLogger.Info("Explaining query",
		"query", truncateQuery(queryStr),
		"user", options.User,
	)

	// Validate the query
	validationResult, err := s.ValidateQuery(ctx, queryStr, options)
	if err != nil {
		queryLogger.Error("Query validation failed", "error", err)
		return s.createErrorResult(err, startTime, queryID), err
	}

	if !validationResult.Valid {
		err := fmt.Errorf("invalid query: %s", strings.Join(validationResult.Errors, "; "))
		queryLogger.Error("Query is invalid", "errors", validationResult.Errors)
		return s.createErrorResult(err, startTime, queryID), err
	}

	// Construct the EXPLAIN query
	explainQuery := "EXPLAIN "

	// Add EXPLAIN options if provided
	if len(options.ExplainOptions) > 0 {
		explainQuery += "("
		first := true
		for k, v := range options.ExplainOptions {
			if !first {
				explainQuery += ", "
			}
			explainQuery += k
			if v != "" {
				explainQuery += "=" + v
			}
			first = false
		}
		explainQuery += ") "
	}

	explainQuery += queryStr

	// Execute the EXPLAIN query
	explainOptions := options
	explainOptions.ResultFormat = QueryResultFormatRaw

	result, err := s.executeOriginalQuery(ctx, explainQuery, explainOptions, queryLogger)
	if err != nil {
		queryLogger.Error("EXPLAIN query execution failed", "error", err)

		// Record metrics
		s.recordQueryMetrics(queryType, "error", database, 0, time.Since(startTime))

		return s.createErrorResult(err, startTime, queryID), err
	}

	// Update result metadata
	result.QueryID = queryID
	result.ExecutionTime = time.Since(startTime)

	// Record metrics
	s.recordQueryMetrics(queryType, "success", database, result.RowCount, result.ExecutionTime)

	queryLogger.Info("Query explained successfully",
		"rows", result.RowCount,
		"time", result.ExecutionTime,
	)

	return result, nil
}

// ValidateQuery validates a query
func (s *StarRocksQueryService) ValidateQuery(
	ctx context.Context,
	queryStr string,
	options QueryOptions,
) (*QueryValidationResult, error) {
	startTime := time.Now()

	// Set up logger
	queryLogger := s.Logger.With(
		"query_type", "VALIDATE",
		"database", options.Database,
	)

	queryLogger.Debug("Validating query", "query", truncateQuery(queryStr))

	// Check if the query is empty
	if strings.TrimSpace(queryStr) == "" {
		return &QueryValidationResult{
			Valid:  false,
			Errors: []string{"query is empty"},
		}, nil
	}

	// Basic syntax validation
	parser := query.NewQueryParser()
	parseResult, err := parser.Parse(queryStr)
	if err != nil {
		queryLogger.Debug("Query parsing failed", "error", err)
		return &QueryValidationResult{
			Valid:  false,
			Errors: []string{fmt.Sprintf("syntax error: %v", err)},
		}, nil
	}

	// Check if the query type is supported
	if !s.isQueryTypeSupported(parseResult.Type) {
		return &QueryValidationResult{
			Valid:  false,
			Errors: []string{fmt.Sprintf("query type not supported: %s", parseResult.Type)},
		}, nil
	}

	// Check for blocked SQL patterns
	for _, pattern := range s.Config.Query.BlockedSQLPatterns {
		if pattern != "" && strings.Contains(strings.ToUpper(queryStr), strings.ToUpper(pattern)) {
			return &QueryValidationResult{
				Valid:  false,
				Errors: []string{fmt.Sprintf("query contains blocked pattern: %s", pattern)},
			}, nil
		}
	}

	// Try to analyze the query in StarRocks to get additional validation
	if parseResult.Type != "SET" && parseResult.Type != "USE" {
		analyzeOptions := options
		analyzeOptions.ResultFormat = QueryResultFormatRaw

		analyzeQuery := fmt.Sprintf("EXPLAIN %s", queryStr)
		_, err := s.StarRocksClient.ExecuteQuery(ctx, analyzeQuery, clients.QueryOptions{
			Database: options.Database,
			User:     options.User,
			Password: "", // Password should be handled by the client
			Timeout:  10 * time.Second,
		})

		if err != nil {
			queryLogger.Debug("Query analysis failed", "error", err)

			// Check for specific error messages that indicate validation issues
			errorMsg := err.Error()
			if strings.Contains(errorMsg, "syntax error") ||
				strings.Contains(errorMsg, "not found") ||
				strings.Contains(errorMsg, "unknown") {
				return &QueryValidationResult{
					Valid:  false,
					Errors: []string{fmt.Sprintf("validation error: %v", err)},
				}, nil
			}

			// If it's a different error, it might be a connection issue
			// We'll consider the query valid but add a warning
			return &QueryValidationResult{
				Valid:    true,
				Warnings: []string{fmt.Sprintf("could not fully validate query: %v", err)},
			}, nil
		}
	}

	// Try to get cost estimates for SELECT queries
	var estimatedCost query.QueryCost
	if parseResult.Type == "SELECT" {
		// Get query cost estimate
		costEstimator := query.NewQueryCostEstimator(s.StarRocksClient)
		cost, err := costEstimator.EstimateQueryCost(ctx, queryStr, options.Database)

		if err == nil {
			estimatedCost = cost

			// Check if the estimated cost exceeds limits
			if cost.TotalCost > s.Config.Query.MaxQueryCost {
				return &QueryValidationResult{
					Valid:         false,
					Errors:        []string{fmt.Sprintf("query cost exceeds maximum allowed: %.2f > %.2f", cost.TotalCost, s.Config.Query.MaxQueryCost)},
					EstimatedCost: cost,
				}, nil
			}

			// Add warnings for high-cost queries
			if cost.TotalCost > s.Config.Query.HighQueryCostThreshold {
				return &QueryValidationResult{
					Valid:         true,
					Warnings:      []string{fmt.Sprintf("query has high cost: %.2f", cost.TotalCost)},
					EstimatedCost: cost,
				}, nil
			}
		}
	}

	// Check for query rewrite opportunities
	var rewrittenQuery string
	if parseResult.Type == "SELECT" {
		rewriter := query.NewQueryRewriter(s.Config)
		newQuery, rewritten := rewriter.TryRewrite(queryStr, parseResult)
		if rewritten {
			rewrittenQuery = newQuery
			queryLogger.Debug("Query rewritten",
				"original", truncateQuery(queryStr),
				"rewritten", truncateQuery(newQuery),
			)
		}
	}

	// Record metrics
	if s.Metrics != nil {
		s.Metrics.CounterInc("query_validations_total", map[string]string{
			"query_type": parseResult.Type,
			"valid":      "true",
		})
		s.Metrics.HistogramObserve("query_validation_time_ms",
			float64(time.Since(startTime).Milliseconds()),
			map[string]string{
				"query_type": parseResult.Type,
			})
	}

	queryLogger.Debug("Query validated successfully", "time", time.Since(startTime))

	return &QueryValidationResult{
		Valid:          true,
		RewrittenQuery: rewrittenQuery,
		EstimatedCost:  estimatedCost,
	}, nil
}

// CancelQuery cancels a query with the given ID
func (s *StarRocksQueryService) CancelQuery(
	ctx context.Context,
	queryID string,
) error {
	// Check if query is running
	queryInfo, running := s.runningQueries.Load(queryID)
	if !running {
		return fmt.Errorf("query not found: %s", queryID)
	}

	info := queryInfo.(*domain.QueryInfo)

	// Log cancellation attempt
	s.Logger.Info("Canceling query",
		"query_id", queryID,
		"query", truncateQuery(info.Query),
		"user", info.User,
		"running_time", time.Since(info.StartTime),
	)

	// Try to cancel the query in StarRocks
	// This requires the actual StarRocks query ID, which we need to map from our ID
	// This implementation assumes the StarRocks client has a method to cancel queries
	err := s.StarRocksClient.CancelQuery(ctx, queryID)
	if err != nil {
		s.Logger.Error("Failed to cancel query",
			"query_id", queryID,
			"error", err,
		)
		return err
	}

	// Update query status
	info.Status = domain.QueryStatusCancelled
	info.EndTime = time.Now()
	s.runningQueries.Store(queryID, info)

	// Record metrics
	if s.Metrics != nil {
		s.Metrics.CounterInc("queries_cancelled", map[string]string{
			"database": info.Database,
		})
	}

	s.Logger.Info("Query cancelled successfully", "query_id", queryID)

	return nil
}

// GetQueryStatus gets the status of a query
func (s *StarRocksQueryService) GetQueryStatus(
	ctx context.Context,
	queryID string,
) (domain.QueryStatus, error) {
	// Check if query is in our running queries map
	queryInfo, running := s.runningQueries.Load(queryID)
	if running {
		info := queryInfo.(*domain.QueryInfo)
		return info.Status, nil
	}

	// Query not found in our running queries, check with StarRocks
	// This implementation assumes the StarRocks client has a method to get query status
	status, err := s.StarRocksClient.GetQueryStatus(ctx, queryID)
	if err != nil {
		// If there's an error, it might mean the query doesn't exist
		// or has completed and been removed from the system
		return domain.QueryStatusUnknown, err
	}

	// Map StarRocks status to our status enum
	// This would need to be implemented based on the actual status values from StarRocks
	return mapStarRocksStatusToQueryStatus(status), nil
}

// GetRunningQueries gets all currently running queries
func (s *StarRocksQueryService) GetRunningQueries(
	ctx context.Context,
) ([]domain.QueryInfo, error) {
	var queries []domain.QueryInfo

	// Collect all running queries from our map
	s.runningQueries.Range(func(key, value interface{}) bool {
		info := value.(*domain.QueryInfo)
		if info.Status == domain.QueryStatusRunning {
			queries = append(queries, *info)
		}
		return true
	})

	// Additionally, we could fetch running queries from StarRocks
	// and merge them with our list
	// This implementation assumes the StarRocks client has a method to list running queries
	starRocksQueries, err := s.StarRocksClient.GetRunningQueries(ctx)
	if err != nil {
		s.Logger.Warn("Failed to get running queries from StarRocks", "error", err)
		// Continue with the queries we already have
	} else {
		// Map StarRocks queries to our format and add them to the list
		// This would need to be implemented based on the actual query info from StarRocks
		for _, q := range starRocksQueries {
			// Check if we already have this query in our list
			alreadyExists := false
			for _, existing := range queries {
				if existing.ID == q.QueryID {
					alreadyExists = true
					break
				}
			}

			if !alreadyExists {
				queries = append(queries, domain.QueryInfo{
					ID:        q.QueryID,
					Query:     q.SQL,
					StartTime: q.StartTime,
					User:      q.User,
					Database:  q.Database,
					Status:    domain.QueryStatusRunning,
				})
			}
		}
	}

	return queries, nil
}

// executeOriginalQuery executes a query directly against StarRocks
func (s *StarRocksQueryService) executeOriginalQuery(
	ctx context.Context,
	queryStr string,
	options QueryOptions,
	logger logging.Logger,
) (*QueryResult, error) {
	// Convert our options to StarRocks client options
	clientOptions := clients.QueryOptions{
		Database: options.Database,
		User:     options.User,
		Password: "", // Password should be handled by the client
		Timeout:  options.Timeout,
		MaxRows:  options.MaxRows,
	}

	// Execute the query
	clientResult, err := s.StarRocksClient.ExecuteQuery(ctx, queryStr, clientOptions)
	if err != nil {
		return nil, err
	}

	// Convert client result to our QueryResult format
	result := &QueryResult{
		RowCount:      len(clientResult.Rows),
		ExecutionTime: clientResult.ExecutionTime,
		Warnings:      clientResult.Warnings,
		IsPartial:     false, // Will be set by caller if needed
		CachedResult:  false,
		QueryID:       options.QueryID,
		Metadata:      make(map[string]interface{}),
	}

	// Copy columns
	result.Columns = make([]query.Column, len(clientResult.Columns))
	for i, col := range clientResult.Columns {
		result.Columns[i] = query.Column{
			Name: col.Name,
			Type: col.Type,
		}
	}

	// Copy rows
	result.Rows = make([][]interface{}, len(clientResult.Rows))
	for i, row := range clientResult.Rows {
		result.Rows[i] = make([]interface{}, len(row))
		for j, val := range row {
			result.Rows[i][j] = val
		}
	}

	// Add execution metadata
	if clientResult.Metadata != nil {
		for k, v := range clientResult.Metadata {
			result.Metadata[k] = v
		}
	}

	// Add query stats to metadata
	result.Metadata["bytes_scanned"] = clientResult.BytesScanned
	result.Metadata["rows_scanned"] = clientResult.RowsScanned
	result.Metadata["cpu_time_ms"] = clientResult.CPUTimeMs

	return result, nil
}

// createErrorResult creates a QueryResult for an error
func (s *StarRocksQueryService) createErrorResult(
	err error,
	startTime time.Time,
	queryID string,
) *QueryResult {
	return &QueryResult{
		Columns:       []query.Column{},
		Rows:          [][]interface{}{},
		RowCount:      0,
		ExecutionTime: time.Since(startTime),
		Warnings:      []string{},
		IsPartial:     false,
		CachedResult:  false,
		QueryID:       queryID,
		Metadata:      make(map[string]interface{}),
		Error:         err,
	}
}

// generateCacheKey generates a cache key for a query
func (s *StarRocksQueryService) generateCacheKey(queryStr string, options QueryOptions) string {
	// Create a key that includes the query and relevant options
	return fmt.Sprintf(
		"%s:%s:%d:%s",
		options.Database,
		options.User,
		options.MaxRows,
		queryStr,
	)
}

// getQueryType gets the type of a query (SELECT, INSERT, etc.)
func (s *StarRocksQueryService) getQueryType(queryStr string) string {
	parser := query.NewQueryParser()
	result, err := parser.Parse(queryStr)
	if err != nil {
		return "UNKNOWN"
	}
	return result.Type
}

// isQueryTypeSupported checks if a query type is supported
func (s *StarRocksQueryService) isQueryTypeSupported(queryType string) bool {
	supportedTypes := map[string]bool{
		"SELECT":  true,
		"INSERT":  true,
		"UPDATE":  true,
		"DELETE":  true,
		"CREATE":  true,
		"DROP":    true,
		"ALTER":   true,
		"SHOW":    true,
		"DESC":    true,
		"USE":     true,
		"SET":     true,
		"EXPLAIN": true,
	}

	return supportedTypes[queryType]
}

// recordQueryMetrics records metrics for a query
func (s *StarRocksQueryService) recordQueryMetrics(
	queryType string,
	status string,
	database string,
	rowCount int,
	duration time.Duration,
) {
	if s.queryHistogram != nil {
		s.queryHistogram.WithLabelValues(queryType, status, database).Observe(duration.Seconds())
	}

	if s.queryCounter != nil {
		s.queryCounter.WithLabelValues(queryType, status, database).Inc()
	}

	if s.rowsProcessedCounter != nil && rowCount > 0 {
		s.rowsProcessedCounter.WithLabelValues(queryType, database).Add(float64(rowCount))
	}

	// Also record through the metrics recorder interface if available
	if s.Metrics != nil {
		s.Metrics.HistogramObserve("query_execution_time_ms",
			float64(duration.Milliseconds()),
			map[string]string{
				"query_type": queryType,
				"status":     status,
				"database":   database,
			})

		s.Metrics.CounterInc("queries_total", map[string]string{
			"query_type": queryType,
			"status":     status,
			"database":   database,
		})

		if rowCount > 0 {
			s.Metrics.CounterAdd("query_rows_processed_total",
				float64(rowCount),
				map[string]string{
					"query_type": queryType,
					"database":   database,
				})
		}
	}
}

// generateQueryID generates a unique query ID
func generateQueryID() string {
	return fmt.Sprintf("q-%d-%x", time.Now().UnixNano(), randomBytes(4))
}

// randomBytes generates random bytes
func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, err := cryptoRand.Read(b)
	if err != nil {
		// Fallback to less secure random if crypto random fails
		for i := range b {
			b[i] = byte(rand.Intn(256))
		}
	}
	return b
}

// truncateQuery truncates a query string for logging
func truncateQuery(queryStr string) string {
	maxLength := 100
	if len(queryStr) <= maxLength {
		return queryStr
	}
	return queryStr[:maxLength] + "..."
}

// mapStarRocksStatusToQueryStatus maps StarRocks status to our QueryStatus
func mapStarRocksStatusToQueryStatus(status string) domain.QueryStatus {
	switch strings.ToUpper(status) {
	case "RUNNING":
		return domain.QueryStatusRunning
	case "FINISHED", "COMPLETED":
		return domain.QueryStatusCompleted
	case "CANCELLED", "CANCELED":
		return domain.QueryStatusCancelled
	case "FAILED", "ERROR":
		return domain.QueryStatusFailed
	case "PENDING":
		return domain.QueryStatusPending
	default:
		return domain.QueryStatusUnknown
	}
}

//Personal.AI order the ending
