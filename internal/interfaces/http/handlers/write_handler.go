// Package handlers provides HTTP handlers for the StarRocks proxy API.
// This file contains the WriteHandler implementation for handling write operations.
package handlers

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/turtacn/staravail/internal/application/interfaces"
	"github.com/turtacn/staravail/internal/common/auth"
	"github.com/turtacn/staravail/internal/common/constants"
	"github.com/turtacn/staravail/internal/common/logging"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/domain/models"
	"github.com/turtacn/staravail/internal/domain/write"
)

// WriteRequest represents a SQL write operation request.
type WriteRequest struct {
	// SQL statement to execute
	SQL string `json:"sql" binding:"required"`

	// Database to write to
	Database string `json:"database"`

	// Parameters for prepared statements
	Parameters []interface{} `json:"parameters"`

	// Transaction ID for multi-statement transactions
	TransactionID string `json:"transaction_id"`

	// Whether to auto-commit the transaction
	AutoCommit bool `json:"auto_commit"`

	// Write options
	Options map[string]interface{} `json:"options"`
}

// BatchWriteRequest represents a batch of write operations.
type BatchWriteRequest struct {
	// Database to write to
	Database string `json:"database"`

	// Array of SQL statements to execute
	Statements []string `json:"statements" binding:"required"`

	// Transaction ID for multi-statement transactions
	TransactionID string `json:"transaction_id"`

	// Whether to execute as a single transaction
	AsSingleTransaction bool `json:"as_single_transaction"`

	// Whether to continue on error
	ContinueOnError bool `json:"continue_on_error"`

	// Write options
	Options map[string]interface{} `json:"options"`
}

// LoadRequest represents a data load request.
type LoadRequest struct {
	// Database to load data into
	Database string `json:"database" binding:"required"`

	// Table to load data into
	Table string `json:"table" binding:"required"`

	// Format of the data (CSV, JSON, etc.)
	Format string `json:"format" binding:"required"`

	// Column names (if not specified in the data)
	Columns []string `json:"columns"`

	// Load options
	Options map[string]interface{} `json:"options"`

	// Data to load (for direct API calls)
	Data string `json:"data"`

	// URL to load data from
	URL string `json:"url"`
}

// StreamLoadRequest represents a streaming data load request.
type StreamLoadRequest struct {
	// Database to load data into
	Database string `json:"database" binding:"required"`

	// Table to load data into
	Table string `json:"table" binding:"required"`

	// Format of the data (CSV, JSON, etc.)
	Format string `json:"format" binding:"required"`

	// Column names (if not specified in the data)
	Columns []string `json:"columns"`

	// Load options
	Options map[string]interface{} `json:"options"`

	// Expected size of the data in bytes (optional)
	ExpectedSize int64 `json:"expected_size"`

	// Timeout in seconds
	TimeoutSeconds int `json:"timeout_seconds"`
}

// LoadStatusRequest represents a request to get the status of a load job.
type LoadStatusRequest struct {
	// ID of the load job
	LoadID string `json:"load_id" binding:"required"`
}

// CancelLoadRequest represents a request to cancel a load job.
type CancelLoadRequest struct {
	// ID of the load job to cancel
	LoadID string `json:"load_id" binding:"required"`

	// Reason for cancellation
	Reason string `json:"reason"`
}

// WriteHandler handles HTTP requests for data write operations.
type WriteHandler struct {
	writeService     interfaces.WriteService
	metricsCollector *metrics.MetricsCollector
	logger           *zap.Logger
}

// NewWriteHandler creates a new WriteHandler instance.
func NewWriteHandler(
	serviceDeps interfaces.ServiceDependencies,
	metricsCollector *metrics.MetricsCollector,
) (*WriteHandler, error) {
	if serviceDeps.WriteService == nil {
		return nil, errors.New("write service is required")
	}

	return &WriteHandler{
		writeService:     serviceDeps.WriteService,
		metricsCollector: metricsCollector,
		logger:           logging.GetLogger().Named("http.handler.write"),
	}, nil
}

// ExecuteUpdate handles SQL write operation requests.
func (h *WriteHandler) ExecuteUpdate(c *gin.Context) {
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
	var req WriteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Warn("Failed to parse write request",
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
			"error": "SQL statement cannot be empty",
		})
		return
	}

	// Create context with default timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(constants.DefaultWriteTimeoutSeconds)*time.Second,
	)
	defer cancel()

	// Log write request
	h.logger.Info("Executing write operation",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("database", req.Database),
		zap.String("sql", req.SQL),
		zap.String("transaction_id", req.TransactionID),
		zap.Bool("auto_commit", req.AutoCommit),
	)

	// Create write options
	options := &write.WriteOptions{
		Parameters:    req.Parameters,
		TransactionID: req.TransactionID,
		AutoCommit:    req.AutoCommit,
		ExtraOptions:  req.Options,
	}

	// Create write request
	writeReq := &write.WriteRequest{
		SQL:       req.SQL,
		Database:  req.Database,
		User:      user.Username,
		RequestID: requestID,
		Options:   options,
	}

	// Start execution timer
	startTime := time.Now()

	// Execute the write operation
	result, err := h.writeService.ExecuteWrite(ctx, writeReq)
	if err != nil {
		h.handleWriteError(c, err, requestID, req.SQL, startTime)
		return
	}

	// Record write metrics
	executionTime := time.Since(startTime)
	h.recordWriteMetrics(user.Username, req.Database, executionTime, result)

	// Log write completion
	h.logger.Info("Write operation completed",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("database", req.Database),
		zap.Duration("execution_time", executionTime),
		zap.Int("affected_rows", result.AffectedRows),
		zap.String("transaction_id", result.TransactionID),
	)

	// Return response
	c.JSON(http.StatusOK, gin.H{
		"metadata": gin.H{
			"request_id":         requestID,
			"execution_time_ms":  executionTime.Milliseconds(),
			"affected_rows":      result.AffectedRows,
			"transaction_id":     result.TransactionID,
			"transaction_status": result.TransactionStatus,
		},
		"last_insert_id": result.LastInsertID,
		"warnings":       result.Warnings,
	})
}

// ExecuteBatch handles batch write operation requests.
func (h *WriteHandler) ExecuteBatch(c *gin.Context) {
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
	var req BatchWriteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Warn("Failed to parse batch write request",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("Invalid request format: %v", err),
		})
		return
	}

	// Validate request
	if len(req.Statements) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Batch must contain at least one statement",
		})
		return
	}

	// Create context with extended timeout for batch operations
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(constants.DefaultBatchWriteTimeoutSeconds)*time.Second,
	)
	defer cancel()

	// Log batch write request
	h.logger.Info("Executing batch write operation",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("database", req.Database),
		zap.Int("statement_count", len(req.Statements)),
		zap.String("transaction_id", req.TransactionID),
		zap.Bool("as_single_transaction", req.AsSingleTransaction),
		zap.Bool("continue_on_error", req.ContinueOnError),
	)

	// Create batch options
	options := &write.BatchWriteOptions{
		TransactionID:       req.TransactionID,
		AsSingleTransaction: req.AsSingleTransaction,
		ContinueOnError:     req.ContinueOnError,
		ExtraOptions:        req.Options,
	}

	// Create batch write request
	batchReq := &write.BatchWriteRequest{
		Statements: req.Statements,
		Database:   req.Database,
		User:       user.Username,
		RequestID:  requestID,
		Options:    options,
	}

	// Start execution timer
	startTime := time.Now()

	// Execute the batch write operation
	result, err := h.writeService.ExecuteBatch(ctx, batchReq)
	if err != nil {
		h.handleWriteError(c, err, requestID, "BATCH", startTime)
		return
	}

	// Record batch write metrics
	executionTime := time.Since(startTime)
	h.metricsCollector.BatchWriteExecutionTime.Observe(executionTime.Seconds())
	h.metricsCollector.BatchWritesTotal.Inc()
	h.metricsCollector.BatchStatementsTotal.Add(float64(len(req.Statements)))

	// Calculate totals
	totalAffectedRows := 0
	for _, res := range result.Results {
		totalAffectedRows += res.AffectedRows
	}

	// Log batch write completion
	h.logger.Info("Batch write operation completed",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("database", req.Database),
		zap.Duration("execution_time", executionTime),
		zap.Int("total_affected_rows", totalAffectedRows),
		zap.Int("successful_statements", len(result.Results)),
		zap.Int("failed_statements", len(result.Errors)),
		zap.String("transaction_id", result.TransactionID),
	)

	// Return response
	c.JSON(http.StatusOK, gin.H{
		"metadata": gin.H{
			"request_id":            requestID,
			"execution_time_ms":     executionTime.Milliseconds(),
			"total_affected_rows":   totalAffectedRows,
			"successful_statements": len(result.Results),
			"failed_statements":     len(result.Errors),
			"transaction_id":        result.TransactionID,
			"transaction_status":    result.TransactionStatus,
		},
		"results": result.Results,
		"errors":  result.Errors,
	})
}

// LoadData handles data load requests.
func (h *WriteHandler) LoadData(c *gin.Context) {
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

	// Check content type to determine how to handle the request
	contentType := c.GetHeader("Content-Type")
	var req LoadRequest
	var dataReader io.Reader
	var dataSize int64

	if strings.HasPrefix(contentType, "multipart/form-data") {
		// Handle multipart form data (file upload)
		if err := c.Request.ParseMultipartForm(constants.MaxMultipartSize); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("Failed to parse multipart form: %v", err),
			})
			return
		}

		// Extract form fields
		database := c.Request.FormValue("database")
		table := c.Request.FormValue("table")
		format := c.Request.FormValue("format")
		columnsStr := c.Request.FormValue("columns")
		optionsStr := c.Request.FormValue("options")

		if database == "" || table == "" || format == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Database, table, and format are required",
			})
			return
		}

		// Parse columns
		var columns []string
		if columnsStr != "" {
			if err := json.Unmarshal([]byte(columnsStr), &columns); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": fmt.Sprintf("Invalid columns format: %v", err),
				})
				return
			}
		}

		// Parse options
		var options map[string]interface{}
		if optionsStr != "" {
			if err := json.Unmarshal([]byte(optionsStr), &options); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": fmt.Sprintf("Invalid options format: %v", err),
				})
				return
			}
		}

		// Get uploaded file
		file, header, err := c.Request.FormFile("file")
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("No file uploaded: %v", err),
			})
			return
		}
		defer file.Close()

		// Prepare request
		req = LoadRequest{
			Database: database,
			Table:    table,
			Format:   format,
			Columns:  columns,
			Options:  options,
		}

		dataReader = file
		dataSize = header.Size

	} else {
		// Handle JSON request
		if err := c.ShouldBindJSON(&req); err != nil {
			h.logger.Warn("Failed to parse load request",
				zap.String("request_id", requestID),
				zap.Error(err),
			)
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("Invalid request format: %v", err),
			})
			return
		}

		// Validate request
		if req.Database == "" || req.Table == "" || req.Format == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Database, table, and format are required",
			})
			return
		}

		// Determine data source
		if req.Data != "" {
			dataReader = strings.NewReader(req.Data)
			dataSize = int64(len(req.Data))
		} else if req.URL != "" {
			// URL loading will be handled by the service
			dataReader = nil
			dataSize = 0
		} else {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Either data or url must be provided",
			})
			return
		}
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(constants.DefaultLoadTimeoutSeconds)*time.Second,
	)
	defer cancel()

	// Log load request
	h.logger.Info("Loading data",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("database", req.Database),
		zap.String("table", req.Table),
		zap.String("format", req.Format),
		zap.Int64("data_size", dataSize),
		zap.Bool("from_url", req.URL != ""),
	)

	// Create load options
	options := &write.LoadOptions{
		Format:       req.Format,
		Columns:      req.Columns,
		URL:          req.URL,
		ExtraOptions: req.Options,
	}

	// Create load request
	loadReq := &write.LoadRequest{
		Database:  req.Database,
		Table:     req.Table,
		User:      user.Username,
		RequestID: requestID,
		Options:   options,
		DataSize:  dataSize,
	}

	// Start execution timer
	startTime := time.Now()

	// Execute the load operation
	var result *write.LoadResult
	var err error

	if req.URL != "" {
		// Load from URL
		result, err = h.writeService.LoadFromURL(ctx, loadReq)
	} else {
		// Load from provided data
		result, err = h.writeService.LoadData(ctx, loadReq, dataReader)
	}

	if err != nil {
		h.handleLoadError(c, err, requestID, req.Database, req.Table, startTime)
		return
	}

	// Record load metrics
	executionTime := time.Since(startTime)
	h.recordLoadMetrics(user.Username, req.Database, req.Table, req.Format, executionTime, dataSize, result)

	// Log load completion
	h.logger.Info("Load operation completed",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("database", req.Database),
		zap.String("table", req.Table),
		zap.Duration("execution_time", executionTime),
		zap.Int64("loaded_rows", result.LoadedRows),
		zap.Int64("filtered_rows", result.FilteredRows),
		zap.String("load_id", result.LoadID),
		zap.String("status", result.Status),
	)

	// Return response
	c.JSON(http.StatusOK, gin.H{
		"metadata": gin.H{
			"request_id":        requestID,
			"execution_time_ms": executionTime.Milliseconds(),
			"database":          req.Database,
			"table":             req.Table,
		},
		"load_id":       result.LoadID,
		"status":        result.Status,
		"loaded_rows":   result.LoadedRows,
		"filtered_rows": result.FilteredRows,
		"errors":        result.Errors,
		"warnings":      result.Warnings,
		"message":       result.Message,
	})
}

// StreamLoad handles streaming data load requests.
func (h *WriteHandler) StreamLoad(c *gin.Context) {
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

	// First phase: create the stream load session
	var req StreamLoadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Warn("Failed to parse stream load request",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("Invalid request format: %v", err),
		})
		return
	}

	// Validate request
	if req.Database == "" || req.Table == "" || req.Format == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Database, table, and format are required",
		})
		return
	}

	// Set default timeout if not specified
	if req.TimeoutSeconds <= 0 {
		req.TimeoutSeconds = constants.DefaultStreamLoadTimeoutSeconds
	} else if req.TimeoutSeconds > constants.MaxStreamLoadTimeoutSeconds {
		req.TimeoutSeconds = constants.MaxStreamLoadTimeoutSeconds
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(req.TimeoutSeconds)*time.Second,
	)
	defer cancel()

	// Log stream load request
	h.logger.Info("Creating stream load session",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("database", req.Database),
		zap.String("table", req.Table),
		zap.String("format", req.Format),
		zap.Int64("expected_size", req.ExpectedSize),
	)

	// Create stream load options
	options := &write.StreamLoadOptions{
		Format:       req.Format,
		Columns:      req.Columns,
		ExpectedSize: req.ExpectedSize,
		Timeout:      time.Duration(req.TimeoutSeconds) * time.Second,
		ExtraOptions: req.Options,
	}

	// Create stream load request
	streamLoadReq := &write.StreamLoadRequest{
		Database:  req.Database,
		Table:     req.Table,
		User:      user.Username,
		RequestID: requestID,
		Options:   options,
	}

	// Start execution timer
	startTime := time.Now()

	// Create the stream load session
	session, err := h.writeService.CreateStreamLoad(ctx, streamLoadReq)
	if err != nil {
		h.handleLoadError(c, err, requestID, req.Database, req.Table, startTime)
		return
	}

	// Return the session information
	c.JSON(http.StatusOK, gin.H{
		"session_id": session.SessionID,
		"endpoint":   session.Endpoint,
		"expires_at": session.ExpiresAt.Format(time.RFC3339),
		"metadata": gin.H{
			"request_id": requestID,
			"database":   req.Database,
			"table":      req.Table,
			"format":     req.Format,
		},
	})
}

// GetLoadStatus handles requests to get the status of a load job.
func (h *WriteHandler) GetLoadStatus(c *gin.Context) {
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

	// Get load ID from path parameter
	loadID := c.Param("loadID")
	if loadID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Load ID is required",
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
	h.logger.Info("Getting load status",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("load_id", loadID),
	)

	// Get load status
	status, err := h.writeService.GetLoadStatus(ctx, loadID, user.Username)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if errors.Is(err, write.ErrLoadNotFound) {
			statusCode = http.StatusNotFound
		}

		h.logger.Error("Failed to get load status",
			zap.String("request_id", requestID),
			zap.String("load_id", loadID),
			zap.Error(err),
		)

		c.JSON(statusCode, gin.H{
			"error": fmt.Sprintf("Failed to get load status: %v", err),
		})
		return
	}

	// Return the load status
	c.JSON(http.StatusOK, status)
}

// CancelLoad handles requests to cancel a load job.
func (h *WriteHandler) CancelLoad(c *gin.Context) {
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

	// Get load ID from path parameter
	loadID := c.Param("loadID")
	if loadID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Load ID is required",
		})
		return
	}

	// Parse request body
	var req CancelLoadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		// Use load ID from path if body parsing fails
		req.LoadID = loadID
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(constants.DefaultAdminTimeoutSeconds)*time.Second,
	)
	defer cancel()

	// Log request
	h.logger.Info("Cancelling load job",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("load_id", loadID),
		zap.String("reason", req.Reason),
	)

	// Cancel the load job
	err := h.writeService.CancelLoad(ctx, loadID, user.Username, req.Reason)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if errors.Is(err, write.ErrLoadNotFound) {
			statusCode = http.StatusNotFound
		} else if errors.Is(err, write.ErrPermissionDenied) {
			statusCode = http.StatusForbidden
		}

		h.logger.Error("Failed to cancel load job",
			zap.String("request_id", requestID),
			zap.String("load_id", loadID),
			zap.Error(err),
		)

		c.JSON(statusCode, gin.H{
			"error": fmt.Sprintf("Failed to cancel load job: %v", err),
		})
		return
	}

	// Return success response
	c.JSON(http.StatusOK, gin.H{
		"message": fmt.Sprintf("Load job %s cancelled successfully", loadID),
		"load_id": loadID,
	})
}

// handleWriteError processes write operation errors and returns appropriate responses.
func (h *WriteHandler) handleWriteError(c *gin.Context, err error, requestID, sql string, startTime time.Time) {
	// Calculate execution time
	executionTime := time.Since(startTime)

	// Determine HTTP status based on error type
	status := http.StatusInternalServerError
	errorMessage := err.Error()

	// Handle specific error types
	switch {
	case errors.Is(err, write.ErrSyntaxError):
		status = http.StatusBadRequest
		h.metricsCollector.WriteSyntaxErrorsTotal.Inc()
	case errors.Is(err, write.ErrTimeout):
		status = http.StatusGatewayTimeout
		h.metricsCollector.WriteTimeoutsTotal.Inc()
	case errors.Is(err, write.ErrCancelled):
		status = http.StatusRequestTimeout
		h.metricsCollector.WriteCancellationsTotal.Inc()
	case errors.Is(err, write.ErrPermissionDenied):
		status = http.StatusForbidden
		h.metricsCollector.WritePermissionErrorsTotal.Inc()
	case errors.Is(err, write.ErrDatabaseNotFound):
		status = http.StatusNotFound
		h.metricsCollector.WriteResourceErrorsTotal.Inc()
	case errors.Is(err, write.ErrTableNotFound):
		status = http.StatusNotFound
		h.metricsCollector.WriteResourceErrorsTotal.Inc()
	case errors.Is(err, write.ErrConstraintViolation):
		status = http.StatusConflict
		h.metricsCollector.WriteConstraintErrorsTotal.Inc()
	case errors.Is(err, write.ErrDuplicateKey):
		status = http.StatusConflict
		h.metricsCollector.WriteConstraintErrorsTotal.Inc()
	case errors.Is(err, context.DeadlineExceeded):
		status = http.StatusGatewayTimeout
		errorMessage = "Write operation timed out"
		h.metricsCollector.WriteTimeoutsTotal.Inc()
	default:
		h.metricsCollector.WriteErrorsTotal.Inc()
	}

	// Log the error
	h.logger.Error("Write operation failed",
		zap.String("request_id", requestID),
		zap.String("sql", sql),
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

// handleLoadError processes load operation errors and returns appropriate responses.
func (h *WriteHandler) handleLoadError(c *gin.Context, err error, requestID, database, table string, startTime time.Time) {
	// Calculate execution time
	executionTime := time.Since(startTime)

	// Determine HTTP status based on error type
	status := http.StatusInternalServerError
	errorMessage := err.Error()

	// Handle specific error types
	switch {
	case errors.Is(err, write.ErrInvalidFormat):
		status = http.StatusBadRequest
		h.metricsCollector.LoadFormatErrorsTotal.Inc()
	case errors.Is(err, write.ErrTimeout):
		status = http.StatusGatewayTimeout
		h.metricsCollector.LoadTimeoutsTotal.Inc()
	case errors.Is(err, write.ErrCancelled):
		status = http.StatusRequestTimeout
		h.metricsCollector.LoadCancellationsTotal.Inc()
	case errors.Is(err, write.ErrPermissionDenied):
		status = http.StatusForbidden
		h.metricsCollector.LoadPermissionErrorsTotal.Inc()
	case errors.Is(err, write.ErrDatabaseNotFound):
		status = http.StatusNotFound
		h.metricsCollector.LoadResourceErrorsTotal.Inc()
	case errors.Is(err, write.ErrTableNotFound):
		status = http.StatusNotFound
		h.metricsCollector.LoadResourceErrorsTotal.Inc()
	case errors.Is(err, write.ErrInvalidData):
		status = http.StatusBadRequest
		h.metricsCollector.LoadDataErrorsTotal.Inc()
	case errors.Is(err, context.DeadlineExceeded):
		status = http.StatusGatewayTimeout
		errorMessage = "Load operation timed out"
		h.metricsCollector.LoadTimeoutsTotal.Inc()
	default:
		h.metricsCollector.LoadErrorsTotal.Inc()
	}

	// Log the error
	h.logger.Error("Load operation failed",
		zap.String("request_id", requestID),
		zap.String("database", database),
		zap.String("table", table),
		zap.Error(err),
		zap.Duration("execution_time", executionTime),
		zap.Int("http_status", status),
	)

	// Return error response
	c.JSON(status, gin.H{
		"error":             errorMessage,
		"execution_time_ms": executionTime.Milliseconds(),
		"request_id":        requestID,
		"database":          database,
		"table":             table,
	})
}

// recordWriteMetrics records metrics for write operations.
func (h *WriteHandler) recordWriteMetrics(username, database string, executionTime time.Duration, result *write.WriteResult) {
	// Record write execution time
	h.metricsCollector.WriteExecutionTime.Observe(executionTime.Seconds())

	// Record affected rows
	h.metricsCollector.WriteAffectedRows.Observe(float64(result.AffectedRows))

	// Record writes by user
	h.metricsCollector.WritesByUser.WithLabelValues(username).Inc()

	// Record writes by database
	if database != "" {
		h.metricsCollector.WritesByDatabase.WithLabelValues(database).Inc()
	}

	// Record successful writes total
	h.metricsCollector.WritesTotal.Inc()
}

// recordLoadMetrics records metrics for load operations.
func (h *WriteHandler) recordLoadMetrics(username, database, table, format string, executionTime time.Duration, dataSize int64, result *write.LoadResult) {
	// Record load execution time
	h.metricsCollector.LoadExecutionTime.Observe(executionTime.Seconds())

	// Record data size
	if dataSize > 0 {
		h.metricsCollector.LoadDataSize.Observe(float64(dataSize))
	}

	// Record loaded rows
	h.metricsCollector.LoadRowsLoaded.Observe(float64(result.LoadedRows))

	// Record filtered rows
	h.metricsCollector.LoadRowsFiltered.Observe(float64(result.FilteredRows))

	// Record load throughput (bytes per second)
	if dataSize > 0 && executionTime > 0 {
		throughput := float64(dataSize) / executionTime.Seconds()
		h.metricsCollector.LoadThroughput.Observe(throughput)
	}

	// Record loads by user
	h.metricsCollector.LoadsByUser.WithLabelValues(username).Inc()

	// Record loads by database and table
	if database != "" && table != "" {
		h.metricsCollector.LoadsByTable.WithLabelValues(database, table).Inc()
	}

	// Record loads by format
	h.metricsCollector.LoadsByFormat.WithLabelValues(format).Inc()

	// Record successful loads total
	h.metricsCollector.LoadsTotal.Inc()
}

//Personal.AI order the ending
