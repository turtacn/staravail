// Package handlers provides HTTP handlers for the StarRocks proxy API.
// This file contains the QueryHandler implementation for handling SQL query requests.
package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/turtacn/staravail/internal/application/interfaces"
	"github.com/turtacn/staravail/internal/common/constants"
	logging "github.com/turtacn/staravail/internal/common/logger"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/domain/query"
)

// QueryRequest represents a SQL query request from the client.
type QueryRequest struct {
	// SQL statement to execute
	SQL string `json:"sql" binding:"required"`

	// Database to query against
	Database string `json:"database"`

	// Maximum rows to return (0 means no limit)
	MaxRows int `json:"max_rows"`

	// Query timeout in seconds
	TimeoutSeconds int `json:"timeout_seconds"`

	// Query options
	Options map[string]interface{} `json:"options"`

	// Whether to stream the results
	Stream bool `json:"stream"`

	// Parameters for prepared statements
	Parameters []interface{} `json:"parameters"`

	// Query format for results (json, csv, arrow, etc.)
	Format string `json:"format"`
}

// ExplainRequest represents a request to explain a query execution plan.
type ExplainRequest struct {
	// SQL statement to explain
	SQL string `json:"sql" binding:"required"`

	// Database to query against
	Database string `json:"database"`

	// Explain format (text, json, dot, etc.)
	Format string `json:"format"`

	// Explain options
	Options map[string]string `json:"options"`
}

// CancelQueryRequest represents a request to cancel a running query.
type CancelQueryRequest struct {
	// ID of the query to cancel
	QueryID string `json:"query_id" binding:"required"`

	// Reason for cancellation
	Reason string `json:"reason"`
}

// AnalyzeQueryRequest represents a request to analyze a query.
type AnalyzeQueryRequest struct {
	// SQL statement to analyze
	SQL string `json:"sql" binding:"required"`

	// Database to query against
	Database string `json:"database"`

	// Analysis options
	Options map[string]interface{} `json:"options"`
}

// QueryHandler handles HTTP requests for SQL queries.
type QueryHandler struct {
	queryService     interfaces.QueryService
	metadataService  interfaces.MetadataService
	metricsCollector *metrics.MetricsCollector
	logger           *zap.Logger
}

// NewQueryHandler creates a new QueryHandler instance.
func NewQueryHandler(
	serviceDeps interfaces.ServiceDependencies,
	metricsCollector *metrics.MetricsCollector,
) (*QueryHandler, error) {
	if serviceDeps.QueryService == nil {
		return nil, errors.New("query service is required")
	}

	return &QueryHandler{
		queryService:     serviceDeps.QueryService,
		metadataService:  serviceDeps.MetadataService,
		metricsCollector: metricsCollector,
		logger:           logging.GetLogger().Named("http.handler.query"),
	}, nil
}

// ExecuteQuery handles SQL query execution requests.
func (h *QueryHandler) ExecuteQuery(c *gin.Context) {
	// Extract request ID for logging and tracking
	requestID := c.GetString("request_id")
	if requestID == "" {
		requestID = uuid.New().String()
	}

	// Extract user information from context
	userInfo, exists := c.Get("user")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "User authentication required",
		})
		return
	}
	user, ok := userInfo.(*auth.UserInfo)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Invalid user information",
		})
		return
	}

	// Parse request body
	var req QueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Warn("Failed to parse query request",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("Invalid request format: %v", err),
		})
		return
	}

	// Validate request
	if strings.TrimSpace(req.SQL) == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "SQL query cannot be empty",
		})
		return
	}

	// Set default format if not specified
	if req.Format == "" {
		req.Format = "json"
	}

	// Set default timeout if not specified or too large
	if req.TimeoutSeconds <= 0 {
		req.TimeoutSeconds = constants.DefaultQueryTimeoutSeconds
	} else if req.TimeoutSeconds > constants.MaxQueryTimeoutSeconds {
		req.TimeoutSeconds = constants.MaxQueryTimeoutSeconds
	}

	// Create query context with timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(req.TimeoutSeconds)*time.Second,
	)
	defer cancel()

	// Log query request
	h.logger.Info("Executing query",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("database", req.Database),
		zap.String("query", req.SQL),
		zap.Int("timeout", req.TimeoutSeconds),
		zap.String("format", req.Format),
	)

	// Create query options
	options := &query.QueryOptions{
		MaxRows:      req.MaxRows,
		Timeout:      time.Duration(req.TimeoutSeconds) * time.Second,
		Format:       req.Format,
		Stream:       req.Stream,
		Parameters:   req.Parameters,
		ExtraOptions: req.Options,
	}

	// Create query execution request
	queryReq := &query.QueryRequest{
		SQL:       req.SQL,
		Database:  req.Database,
		User:      user.Username,
		RequestID: requestID,
		Options:   options,
	}

	// Start execution timer
	startTime := time.Now()

	// Check if this is a streaming request
	if req.Stream {
		h.handleStreamingQuery(c, queryReq, startTime)
		return
	}

	// Execute the query
	result, err := h.queryService.ExecuteQuery(ctx, queryReq)
	if err != nil {
		h.handleQueryError(c, err, requestID, req.SQL, startTime)
		return
	}

	// Record query metrics
	executionTime := time.Since(startTime)
	h.recordQueryMetrics(user.Username, req.Database, executionTime, result)

	// Log query completion
	h.logger.Info("Query completed",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("database", req.Database),
		zap.Duration("execution_time", executionTime),
		zap.Int("row_count", result.RowCount),
		zap.Int("affected_rows", result.AffectedRows),
	)

	// Format and send response based on requested format
	switch strings.ToLower(req.Format) {
	case "json":
		h.sendJSONResponse(c, result)
	case "csv":
		h.sendCSVResponse(c, result)
	case "arrow":
		h.sendArrowResponse(c, result)
	default:
		h.sendJSONResponse(c, result)
	}
}

// ExecuteQueryArrow handles SQL query execution with Arrow format results.
func (h *QueryHandler) ExecuteQueryArrow(c *gin.Context) {
	// Extract request ID for logging and tracking
	requestID := c.GetString("request_id")
	if requestID == "" {
		requestID = uuid.New().String()
	}

	// Extract user information from context
	userInfo, exists := c.Get("user")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "User authentication required",
		})
		return
	}
	user, ok := userInfo.(*auth.UserInfo)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Invalid user information",
		})
		return
	}

	// Parse request body
	var req QueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Warn("Failed to parse query request",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("Invalid request format: %v", err),
		})
		return
	}

	// Force Arrow format
	req.Format = "arrow"

	// Set default timeout if not specified or too large
	if req.TimeoutSeconds <= 0 {
		req.TimeoutSeconds = constants.DefaultQueryTimeoutSeconds
	} else if req.TimeoutSeconds > constants.MaxQueryTimeoutSeconds {
		req.TimeoutSeconds = constants.MaxQueryTimeoutSeconds
	}

	// Create query context with timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(req.TimeoutSeconds)*time.Second,
	)
	defer cancel()

	// Log query request
	h.logger.Info("Executing arrow query",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("database", req.Database),
		zap.String("query", req.SQL),
		zap.Int("timeout", req.TimeoutSeconds),
	)

	// Create query options
	options := &query.QueryOptions{
		MaxRows:      req.MaxRows,
		Timeout:      time.Duration(req.TimeoutSeconds) * time.Second,
		Format:       "arrow",
		Parameters:   req.Parameters,
		ExtraOptions: req.Options,
	}

	// Create query execution request
	queryReq := &query.QueryRequest{
		SQL:       req.SQL,
		Database:  req.Database,
		User:      user.Username,
		RequestID: requestID,
		Options:   options,
	}

	// Start execution timer
	startTime := time.Now()

	// Execute the query
	result, err := h.queryService.ExecuteQueryArrow(ctx, queryReq)
	if err != nil {
		h.handleQueryError(c, err, requestID, req.SQL, startTime)
		return
	}

	// Record query metrics
	executionTime := time.Since(startTime)
	h.metricsCollector.QueryExecutionTime.Observe(executionTime.Seconds())
	h.metricsCollector.ArrowQueriesTotal.Inc()

	// Log query completion
	h.logger.Info("Arrow query completed",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("database", req.Database),
		zap.Duration("execution_time", executionTime),
		zap.Int("record_batch_count", len(result.RecordBatches)),
	)

	// Send Arrow response
	c.Header("Content-Type", "application/vnd.apache.arrow.stream")
	c.Header("X-Query-ID", result.QueryID)
	c.Header("X-Execution-Time-Ms", strconv.FormatInt(executionTime.Milliseconds(), 10))

	// Write Arrow record batches
	for _, batch := range result.RecordBatches {
		if _, err := c.Writer.Write(batch); err != nil {
			h.logger.Error("Failed to write Arrow record batch",
				zap.String("request_id", requestID),
				zap.Error(err),
			)
			return
		}
	}
}

// ExplainQuery handles query execution plan explanation requests.
func (h *QueryHandler) ExplainQuery(c *gin.Context) {
	// Extract request ID for logging and tracking
	requestID := c.GetString("request_id")
	if requestID == "" {
		requestID = uuid.New().String()
	}

	// Extract user information from context
	userInfo, exists := c.Get("user")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "User authentication required",
		})
		return
	}
	user, ok := userInfo.(*auth.UserInfo)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Invalid user information",
		})
		return
	}

	// Parse request body
	var req ExplainRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Warn("Failed to parse explain request",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("Invalid request format: %v", err),
		})
		return
	}

	// Validate request
	if strings.TrimSpace(req.SQL) == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "SQL query cannot be empty",
		})
		return
	}

	// Set default format if not specified
	if req.Format == "" {
		req.Format = "text"
	}

	// Create query context with timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(constants.DefaultExplainTimeoutSeconds)*time.Second,
	)
	defer cancel()

	// Log explain request
	h.logger.Info("Explaining query",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("database", req.Database),
		zap.String("query", req.SQL),
		zap.String("format", req.Format),
	)

	// Create explain options
	options := &query.ExplainOptions{
		Format:  req.Format,
		Options: req.Options,
	}

	// Create explain request
	explainReq := &query.ExplainRequest{
		SQL:       req.SQL,
		Database:  req.Database,
		User:      user.Username,
		RequestID: requestID,
		Options:   options,
	}

	// Start execution timer
	startTime := time.Now()

	// Execute the explain
	result, err := h.queryService.ExplainQuery(ctx, explainReq)
	if err != nil {
		h.handleQueryError(c, err, requestID, req.SQL, startTime)
		return
	}

	// Record metrics
	executionTime := time.Since(startTime)
	h.metricsCollector.ExplainExecutionTime.Observe(executionTime.Seconds())
	h.metricsCollector.ExplainQueriesTotal.Inc()

	// Log completion
	h.logger.Info("Explain completed",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("database", req.Database),
		zap.Duration("execution_time", executionTime),
	)

	// Send response based on format
	switch strings.ToLower(req.Format) {
	case "json":
		c.JSON(http.StatusOK, result)
	case "dot":
		c.Header("Content-Type", "text/vnd.graphviz")
		c.String(http.StatusOK, result.Plan)
	default:
		c.Header("Content-Type", "text/plain")
		c.String(http.StatusOK, result.Plan)
	}
}

// GetActiveQueries handles requests to list active queries.
func (h *QueryHandler) GetActiveQueries(c *gin.Context) {
	// Extract request ID for logging and tracking
	requestID := c.GetString("request_id")
	if requestID == "" {
		requestID = uuid.New().String()
	}

	// Extract user information from context
	userInfo, exists := c.Get("user")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "User authentication required",
		})
		return
	}
	user, ok := userInfo.(*auth.UserInfo)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Invalid user information",
		})
		return
	}

	// Parse query parameters
	database := c.Query("database")
	limitStr := c.DefaultQuery("limit", "100")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 100
	}

	username := c.Query("user")
	if username == "" && !user.HasRole("admin") {
		// Non-admin users can only see their own queries
		username = user.Username
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(constants.DefaultAdminTimeoutSeconds)*time.Second,
	)
	defer cancel()

	// Log request
	h.logger.Info("Getting active queries",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("filter_database", database),
		zap.String("filter_user", username),
		zap.Int("limit", limit),
	)

	// Get active queries
	queries, err := h.queryService.GetActiveQueries(ctx, &query.ActiveQueryFilter{
		Database: database,
		User:     username,
		Limit:    limit,
	})

	if err != nil {
		h.logger.Error("Failed to get active queries",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to get active queries: %v", err),
		})
		return
	}

	// Return the active queries
	c.JSON(http.StatusOK, gin.H{
		"queries": queries,
		"count":   len(queries),
	})
}

// GetQueryHistory handles requests to get query history.
func (h *QueryHandler) GetQueryHistory(c *gin.Context) {
	// Extract request ID for logging and tracking
	requestID := c.GetString("request_id")
	if requestID == "" {
		requestID = uuid.New().String()
	}

	// Extract user information from context
	userInfo, exists := c.Get("user")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "User authentication required",
		})
		return
	}
	user, ok := userInfo.(*auth.UserInfo)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Invalid user information",
		})
		return
	}

	// Parse query parameters
	database := c.Query("database")
	limitStr := c.DefaultQuery("limit", "100")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 100
	}

	username := c.Query("user")
	if username == "" && !user.HasRole("admin") {
		// Non-admin users can only see their own queries
		username = user.Username
	}

	startTimeStr := c.Query("start_time")
	var startTime *time.Time
	if startTimeStr != "" {
		t, err := time.Parse(time.RFC3339, startTimeStr)
		if err == nil {
			startTime = &t
		}
	}

	endTimeStr := c.Query("end_time")
	var endTime *time.Time
	if endTimeStr != "" {
		t, err := time.Parse(time.RFC3339, endTimeStr)
		if err == nil {
			endTime = &t
		}
	}

	status := c.Query("status")

	// Create context with timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(constants.DefaultAdminTimeoutSeconds)*time.Second,
	)
	defer cancel()

	// Log request
	h.logger.Info("Getting query history",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("filter_database", database),
		zap.String("filter_user", username),
		zap.String("filter_status", status),
		zap.Int("limit", limit),
	)

	// Get query history
	history, err := h.queryService.GetQueryHistory(ctx, &query.QueryHistoryFilter{
		Database:  database,
		User:      username,
		StartTime: startTime,
		EndTime:   endTime,
		Status:    status,
		Limit:     limit,
	})

	if err != nil {
		h.logger.Error("Failed to get query history",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to get query history: %v", err),
		})
		return
	}

	// Return the query history
	c.JSON(http.StatusOK, gin.H{
		"queries": history,
		"count":   len(history),
	})
}

// GetQueryMetrics handles requests to get metrics for a specific query.
func (h *QueryHandler) GetQueryMetrics(c *gin.Context) {
	// Extract request ID for logging and tracking
	requestID := c.GetString("request_id")
	if requestID == "" {
		requestID = uuid.New().String()
	}

	// Extract user information from context
	userInfo, exists := c.Get("user")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "User authentication required",
		})
		return
	}
	user, ok := userInfo.(*auth.UserInfo)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Invalid user information",
		})
		return
	}

	// Get query ID from path parameter
	queryID := c.Param("queryID")
	if queryID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Query ID is required",
		})
		return
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(constants.DefaultAdminTimeoutSeconds)*time.Second,
	)
	defer cancel()

	// Log request
	h.logger.Info("Getting query metrics",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("query_id", queryID),
	)

	// Get query metrics
	metrics, err := h.queryService.GetQueryMetrics(ctx, queryID)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, query.ErrQueryNotFound) {
			status = http.StatusNotFound
		}

		h.logger.Error("Failed to get query metrics",
			zap.String("request_id", requestID),
			zap.String("query_id", queryID),
			zap.Error(err),
		)

		c.JSON(status, gin.H{
			"error": fmt.Sprintf("Failed to get query metrics: %v", err),
		})
		return
	}

	// Return the query metrics
	c.JSON(http.StatusOK, metrics)
}

// CancelQuery handles requests to cancel a running query.
func (h *QueryHandler) CancelQuery(c *gin.Context) {
	// Extract request ID for logging and tracking
	requestID := c.GetString("request_id")
	if requestID == "" {
		requestID = uuid.New().String()
	}

	// Extract user information from context
	userInfo, exists := c.Get("user")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "User authentication required",
		})
		return
	}
	user, ok := userInfo.(*auth.UserInfo)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Invalid user information",
		})
		return
	}

	// Get query ID from path parameter
	queryID := c.Param("queryID")
	if queryID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Query ID is required",
		})
		return
	}

	// Parse request body
	var req CancelQueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		// Use query ID from path if body parsing fails
		req.QueryID = queryID
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(constants.DefaultAdminTimeoutSeconds)*time.Second,
	)
	defer cancel()

	// Log request
	h.logger.Info("Cancelling query",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("query_id", queryID),
		zap.String("reason", req.Reason),
	)

	// Check if user has permission to cancel this query
	if !user.HasRole("admin") {
		// Non-admin users can only cancel their own queries
		queryInfo, err := h.queryService.GetQueryInfo(ctx, queryID)
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, query.ErrQueryNotFound) {
				status = http.StatusNotFound
			}

			c.JSON(status, gin.H{
				"error": fmt.Sprintf("Failed to get query information: %v", err),
			})
			return
		}

		if queryInfo.User != user.Username {
			c.JSON(http.StatusForbidden, gin.H{
				"error": "You can only cancel your own queries",
			})
			return
		}
	}

	// Cancel the query
	err := h.queryService.CancelQuery(ctx, queryID, req.Reason)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, query.ErrQueryNotFound) {
			status = http.StatusNotFound
		}

		h.logger.Error("Failed to cancel query",
			zap.String("request_id", requestID),
			zap.String("query_id", queryID),
			zap.Error(err),
		)

		c.JSON(status, gin.H{
			"error": fmt.Sprintf("Failed to cancel query: %v", err),
		})
		return
	}

	// Return success response
	c.JSON(http.StatusOK, gin.H{
		"message":  fmt.Sprintf("Query %s cancelled successfully", queryID),
		"query_id": queryID,
	})
}

// AnalyzeQuery handles requests to analyze a query.
func (h *QueryHandler) AnalyzeQuery(c *gin.Context) {
	// Extract request ID for logging and tracking
	requestID := c.GetString("request_id")
	if requestID == "" {
		requestID = uuid.New().String()
	}

	// Extract user information from context
	userInfo, exists := c.Get("user")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "User authentication required",
		})
		return
	}
	user, ok := userInfo.(*auth.UserInfo)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Invalid user information",
		})
		return
	}

	// Parse request body
	var req AnalyzeQueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Warn("Failed to parse analyze request",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("Invalid request format: %v", err),
		})
		return
	}

	// Validate request
	if strings.TrimSpace(req.SQL) == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "SQL query cannot be empty",
		})
		return
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(constants.DefaultAnalyzeTimeoutSeconds)*time.Second,
	)
	defer cancel()

	// Log request
	h.logger.Info("Analyzing query",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("database", req.Database),
		zap.String("query", req.SQL),
	)

	// Create analyze options
	options := &query.AnalyzeOptions{
		ExtraOptions: req.Options,
	}

	// Create analyze request
	analyzeReq := &query.AnalyzeRequest{
		SQL:       req.SQL,
		Database:  req.Database,
		User:      user.Username,
		RequestID: requestID,
		Options:   options,
	}

	// Start execution timer
	startTime := time.Now()

	// Execute the analysis
	result, err := h.queryService.AnalyzeQuery(ctx, analyzeReq)
	if err != nil {
		h.logger.Error("Failed to analyze query",
			zap.String("request_id", requestID),
			zap.String("query", req.SQL),
			zap.Error(err),
		)

		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to analyze query: %v", err),
		})
		return
	}

	// Record metrics
	executionTime := time.Since(startTime)
	h.metricsCollector.AnalyzeExecutionTime.Observe(executionTime.Seconds())
	h.metricsCollector.AnalyzeQueriesTotal.Inc()

	// Log completion
	h.logger.Info("Query analysis completed",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("database", req.Database),
		zap.Duration("execution_time", executionTime),
	)

	// Return the analysis result
	c.JSON(http.StatusOK, result)
}

// handleStreamingQuery processes a streaming query request.
func (h *QueryHandler) handleStreamingQuery(c *gin.Context, queryReq *query.QueryRequest, startTime time.Time) {
	// Set appropriate headers for streaming
	c.Header("Content-Type", "application/json")
	c.Header("Transfer-Encoding", "chunked")

	// Create streaming context
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		queryReq.Options.Timeout,
	)
	defer cancel()

	// Get result channel from query service
	resultChan, errChan, err := h.queryService.ExecuteStreamingQuery(ctx, queryReq)
	if err != nil {
		h.handleQueryError(c, err, queryReq.RequestID, queryReq.SQL, startTime)
		return
	}

	// Set the headers
	c.Header("X-Request-ID", queryReq.RequestID)

	// Send opening JSON array
	c.Writer.Write([]byte("{\n  \"results\": [\n"))

	// Process streaming results
	isFirst := true
	rowCount := 0

	// Set up a ticker for heartbeat
	heartbeatTicker := time.NewTicker(30 * time.Second)
	defer heartbeatTicker.Stop()

	// Stream results to client
	for {
		select {
		case batch, ok := <-resultChan:
			if !ok {
				// Channel closed, we're done
				goto StreamEnd
			}

			// Convert batch to JSON
			batchJSON, err := json.Marshal(batch)
			if err != nil {
				h.logger.Error("Failed to marshal result batch",
					zap.String("request_id", queryReq.RequestID),
					zap.Error(err),
				)
				continue
			}

			// Add comma if not first batch
			if !isFirst {
				c.Writer.Write([]byte(",\n"))
			} else {
				isFirst = false
			}

			// Write batch to response
			c.Writer.Write(batchJSON)
			c.Writer.Flush()

			// Update row count
			rowCount += len(batch.Rows)

		case err, ok := <-errChan:
			if !ok || err == nil {
				// Error channel closed or nil error
				continue
			}

			// Log the error
			h.logger.Error("Streaming query error",
				zap.String("request_id", queryReq.RequestID),
				zap.Error(err),
			)

			// Send error as part of stream
			errorJSON, _ := json.Marshal(gin.H{
				"error": err.Error(),
			})
			c.Writer.Write([]byte(",\n"))
			c.Writer.Write([]byte("  \"error\": "))
			c.Writer.Write(errorJSON)
			goto StreamEnd

		case <-heartbeatTicker.C:
			// Send heartbeat comment to keep connection alive
			c.Writer.Write([]byte("\n  /* heartbeat */\n"))
			c.Writer.Flush()

		case <-ctx.Done():
			// Context cancelled or timed out
			errorJSON, _ := json.Marshal(gin.H{
				"error": "Query timeout or cancelled",
			})
			c.Writer.Write([]byte(",\n"))
			c.Writer.Write([]byte("  \"error\": "))
			c.Writer.Write(errorJSON)
			goto StreamEnd
		}
	}

StreamEnd:
	// Close JSON array and add metadata
	executionTime := time.Since(startTime)
	metadataJSON, _ := json.Marshal(gin.H{
		"execution_time_ms": executionTime.Milliseconds(),
		"row_count":         rowCount,
	})

	c.Writer.Write([]byte("\n  ],\n"))
	c.Writer.Write([]byte("  \"metadata\": "))
	c.Writer.Write(metadataJSON)
	c.Writer.Write([]byte("\n}"))
	c.Writer.Flush()

	// Record metrics
	h.metricsCollector.QueryExecutionTime.Observe(executionTime.Seconds())
	h.metricsCollector.StreamingQueriesTotal.Inc()
	h.metricsCollector.QueryRowsReturned.Observe(float64(rowCount))

	// Log query completion
	h.logger.Info("Streaming query completed",
		zap.String("request_id", queryReq.RequestID),
		zap.String("user", queryReq.User),
		zap.String("database", queryReq.Database),
		zap.Duration("execution_time", executionTime),
		zap.Int("row_count", rowCount),
	)
}

// handleQueryError processes query execution errors and returns appropriate responses.
func (h *QueryHandler) handleQueryError(c *gin.Context, err error, requestID, sql string, startTime time.Time) {
	// Calculate execution time
	executionTime := time.Since(startTime)

	// Determine HTTP status based on error type
	status := http.StatusInternalServerError
	errorMessage := err.Error()

	// Handle specific error types
	switch {
	case errors.Is(err, query.ErrSyntaxError):
		status = http.StatusBadRequest
		h.metricsCollector.QuerySyntaxErrorsTotal.Inc()
	case errors.Is(err, query.ErrTimeout):
		status = http.StatusGatewayTimeout
		h.metricsCollector.QueryTimeoutsTotal.Inc()
	case errors.Is(err, query.ErrCancelled):
		status = http.StatusRequestTimeout
		h.metricsCollector.QueryCancellationsTotal.Inc()
	case errors.Is(err, query.ErrPermissionDenied):
		status = http.StatusForbidden
		h.metricsCollector.QueryPermissionErrorsTotal.Inc()
	case errors.Is(err, query.ErrDatabaseNotFound):
		status = http.StatusNotFound
		h.metricsCollector.QueryResourceErrorsTotal.Inc()
	case errors.Is(err, query.ErrTableNotFound):
		status = http.StatusNotFound
		h.metricsCollector.QueryResourceErrorsTotal.Inc()
	case errors.Is(err, context.DeadlineExceeded):
		status = http.StatusGatewayTimeout
		errorMessage = "Query execution timed out"
		h.metricsCollector.QueryTimeoutsTotal.Inc()
	default:
		h.metricsCollector.QueryErrorsTotal.Inc()
	}

	// Log the error
	h.logger.Error("Query execution failed",
		zap.String("request_id", requestID),
		zap.String("query", sql),
		zap.Error(err),
		zap.Duration("execution_time", executionTime),
		zap.Int("http_status", status),
	)

	// Return error response
	c.JSON(status, gin.H{
		"error":             errorMessage,
		"execution_time_ms": executionTime.Milliseconds(),
		"request_id":        requestID,
	})
}

// sendJSONResponse sends query results as JSON.
func (h *QueryHandler) sendJSONResponse(c *gin.Context, result *query.QueryResult) {
	// Set response headers
	c.Header("Content-Type", "application/json")
	c.Header("X-Query-ID", result.QueryID)
	c.Header("X-Execution-Time-Ms", strconv.FormatInt(result.ExecutionTime.Milliseconds(), 10))
	c.Header("X-Row-Count", strconv.Itoa(result.RowCount))
	c.Header("X-Affected-Rows", strconv.Itoa(result.AffectedRows))

	// Create response structure
	response := gin.H{
		"metadata": gin.H{
			"query_id":          result.QueryID,
			"execution_time_ms": result.ExecutionTime.Milliseconds(),
			"row_count":         result.RowCount,
			"affected_rows":     result.AffectedRows,
			"has_more":          result.HasMore,
			"column_count":      len(result.Columns),
		},
		"columns": result.Columns,
		"rows":    result.Rows,
	}

	// Add warnings if present
	if len(result.Warnings) > 0 {
		response["warnings"] = result.Warnings
	}

	// Add schema if present
	if result.Schema != nil {
		response["schema"] = result.Schema
	}

	// Return the response
	c.JSON(http.StatusOK, response)
}

// sendCSVResponse sends query results as CSV.
func (h *QueryHandler) sendCSVResponse(c *gin.Context, result *query.QueryResult) {
	// Set response headers
	c.Header("Content-Type", "text/csv")
	c.Header("Content-Disposition", "attachment; filename=query-result.csv")
	c.Header("X-Query-ID", result.QueryID)
	c.Header("X-Execution-Time-Ms", strconv.FormatInt(result.ExecutionTime.Milliseconds(), 10))
	c.Header("X-Row-Count", strconv.Itoa(result.RowCount))

	// Write CSV header row
	columnNames := make([]string, len(result.Columns))
	for i, col := range result.Columns {
		columnNames[i] = col.Name
	}
	c.Writer.Write([]byte(strings.Join(columnNames, ",") + "\n"))

	// Write data rows
	for _, row := range result.Rows {
		rowValues := make([]string, len(row))
		for i, val := range row {
			// Format the value based on type
			switch v := val.(type) {
			case nil:
				rowValues[i] = ""
			case string:
				// Escape quotes and wrap in quotes
				escaped := strings.ReplaceAll(v, "\"", "\"\"")
				rowValues[i] = "\"" + escaped + "\""
			default:
				rowValues[i] = fmt.Sprintf("%v", v)
			}
		}
		c.Writer.Write([]byte(strings.Join(rowValues, ",") + "\n"))
	}
}

// sendArrowResponse sends query results in Arrow format.
func (h *QueryHandler) sendArrowResponse(c *gin.Context, result *query.QueryResult) {
	// Set response headers
	c.Header("Content-Type", "application/vnd.apache.arrow.file")
	c.Header("Content-Disposition", "attachment; filename=query-result.arrow")
	c.Header("X-Query-ID", result.QueryID)
	c.Header("X-Execution-Time-Ms", strconv.FormatInt(result.ExecutionTime.Milliseconds(), 10))
	c.Header("X-Row-Count", strconv.Itoa(result.RowCount))

	// Convert to Arrow format (placeholder - actual implementation would use Arrow libraries)
	arrowData := []byte("This is a placeholder for actual Arrow binary data")
	c.Data(http.StatusOK, "application/vnd.apache.arrow.file", arrowData)
}

// recordQueryMetrics records metrics for query execution.
func (h *QueryHandler) recordQueryMetrics(username, database string, executionTime time.Duration, result *query.QueryResult) {
	// Record query execution time
	h.metricsCollector.QueryExecutionTime.Observe(executionTime.Seconds())

	// Record rows returned
	h.metricsCollector.QueryRowsReturned.Observe(float64(result.RowCount))

	// Record data size if available
	if result.DataSize > 0 {
		h.metricsCollector.QueryDataSize.Observe(float64(result.DataSize))
	}

	// Record queries by user
	h.metricsCollector.QueriesByUser.WithLabelValues(username).Inc()

	// Record queries by database
	if database != "" {
		h.metricsCollector.QueriesByDatabase.WithLabelValues(database).Inc()
	}

	// Record successful queries total
	h.metricsCollector.QueriesTotal.Inc()

	// Record affected rows for write operations
	if result.AffectedRows > 0 {
		h.metricsCollector.QueryAffectedRows.Observe(float64(result.AffectedRows))
	}
}

//Personal.AI order the ending
