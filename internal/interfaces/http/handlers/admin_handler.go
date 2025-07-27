// Package handlers provides HTTP handlers for the StarRocks proxy API.
// This file contains the AdminHandler implementation for handling administrative operations.
package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
)


// ConfigUpdateRequest represents a request to update configuration.
type ConfigUpdateRequest struct {
	// Configuration parameters to update
	Params map[string]interface{} `json:"params" binding:"required"`

	// Scope of the update (global, cluster, fe, be)
	Scope string `json:"scope"`

	// Target ID (cluster ID, FE/BE name, etc.) if applicable
	TargetID string `json:"target_id"`

	// Whether to persist the changes
	Persist bool `json:"persist"`

	// Comment for the change
	Comment string `json:"comment"`
}

// TabletRequest represents a request for tablet information.
type TabletRequest struct {
	// Database name
	Database string `json:"database"`

	// Table name
	Table string `json:"table"`

	// Tablet ID
	TabletID string `json:"tablet_id"`

	// Partition name
	Partition string `json:"partition"`

	// Maximum number of tablets to return
	Limit int `json:"limit"`

	// Tablet status filter
	Status string `json:"status"`
}

// BackendRequest represents a request for backend server information.
type BackendRequest struct {
	// Backend ID
	BackendID string `json:"backend_id"`

	// Backend host
	Host string `json:"host"`

	// Backend status filter
	Status string `json:"status"`

	// Whether to include detailed metrics
	DetailedMetrics bool `json:"detailed_metrics"`

	// Whether to include tablet distribution
	IncludeTablets bool `json:"include_tablets"`

	// Whether to include task information
	IncludeTasks bool `json:"include_tasks"`
}

// JobStatusRequest represents a request for job status information.
type JobStatusRequest struct {
	// Job type
	JobType string `json:"job_type"`

	// Job ID
	JobID string `json:"job_id"`

	// Job status filter
	Status string `json:"status"`

	// Maximum number of jobs to return
	Limit int `json:"limit"`

	// Start time for filtering
	StartTime string `json:"start_time"`

	// End time for filtering
	EndTime string `json:"end_time"`
}

// ClusterInfoRequest represents a request for cluster information.
type ClusterInfoRequest struct {
	// Whether to include detailed FE information
	IncludeFEs bool `json:"include_fes"`

	// Whether to include detailed BE information
	IncludeBEs bool `json:"include_bes"`

	// Whether to include system information
	IncludeSystemInfo bool `json:"include_system_info"`

	// Whether to include resource usage
	IncludeResourceUsage bool `json:"include_resource_usage"`
}

// AdminHandler handles HTTP requests for administrative operations.
type AdminHandler struct {
	adminService     interfaces.AdminService
	healthService    interfaces.HealthService
	configService    interfaces.ConfigService
	metricsCollector *metrics.MetricsCollector
	logger           *zap.Logger
}

// NewAdminHandler creates a new AdminHandler instance.
func NewAdminHandler(
	serviceDeps interfaces.ServiceDependencies,
	metricsCollector *metrics.MetricsCollector,
) (*AdminHandler, error) {
	if serviceDeps.AdminService == nil {
		return nil, errors.New("admin service is required")
	}
	if serviceDeps.HealthService == nil {
		return nil, errors.New("health service is required")
	}
	if serviceDeps.ConfigService == nil {
		return nil, errors.New("config service is required")
	}

	return &AdminHandler{
		adminService:     serviceDeps.AdminService,
		healthService:    serviceDeps.HealthService,
		configService:    serviceDeps.ConfigService,
		metricsCollector: metricsCollector,
		logger:           logging.GetLogger().Named("http.handler.admin"),
	}, nil
}

// HandleHealth handles health check requests.
func (h *AdminHandler) HandleHealth(c *gin.Context) {
	// Extract request ID for logging and tracking
	requestID := c.GetString("request_id")
	if requestID == "" {
		requestID = uuid.New().String()
	}

	// Parse query parameters
	detailed := c.DefaultQuery("detailed", "false") == "true"
	components := c.DefaultQuery("components", "all")
	timeout := c.DefaultQuery("timeout", "5")

	// Parse timeout
	timeoutSec, err := strconv.Atoi(timeout)
	if err != nil || timeoutSec <= 0 || timeoutSec > 60 {
		timeoutSec = 5
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(timeoutSec)*time.Second,
	)
	defer cancel()

	// Log health check request
	h.logger.Debug("Health check requested",
		zap.String("request_id", requestID),
		zap.Bool("detailed", detailed),
		zap.String("components", components),
		zap.Int("timeout", timeoutSec),
	)

	// Prepare health check options
	options := &admin.HealthCheckOptions{
		Detailed:   detailed,
		Components: strings.Split(components, ","),
		Timeout:    time.Duration(timeoutSec) * time.Second,
	}

	// Start execution timer
	startTime := time.Now()

	// Perform health check
	healthStatus, err := h.healthService.CheckHealth(ctx, options)
	if err != nil {
		h.logger.Error("Health check failed",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status": "error",
			"error":  err.Error(),
		})
		return
	}

	// Calculate execution time
	executionTime := time.Since(startTime)

	// Record metrics
	h.metricsCollector.HealthCheckLatency.Observe(executionTime.Seconds())
	h.metricsCollector.HealthChecksTotal.Inc()

	// Determine HTTP status code based on health status
	httpStatus := http.StatusOK
	if !healthStatus.IsHealthy {
		httpStatus = http.StatusServiceUnavailable
	}

	// Add execution time to response
	healthStatus.CheckTime = time.Now()
	healthStatus.ExecutionTimeMs = executionTime.Milliseconds()

	// Return health status
	c.JSON(httpStatus, healthStatus)
}

// HandleLiveness handles liveness probe requests.
func (h *AdminHandler) HandleLiveness(c *gin.Context) {
	// Extract request ID for logging and tracking
	requestID := c.GetString("request_id")
	if requestID == "" {
		requestID = uuid.New().String()
	}

	// Create context with short timeout for liveness check
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(2)*time.Second,
	)
	defer cancel()

	// Start execution timer
	startTime := time.Now()

	// Perform liveness check
	isLive, err := h.healthService.CheckLiveness(ctx)
	if err != nil {
		h.logger.Error("Liveness check failed",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status": "error",
			"error":  err.Error(),
		})
		return
	}

	// Calculate execution time
	executionTime := time.Since(startTime)

	// Record metrics
	h.metricsCollector.LivenessCheckLatency.Observe(executionTime.Seconds())
	h.metricsCollector.LivenessChecksTotal.Inc()

	// Determine HTTP status code based on liveness status
	httpStatus := http.StatusOK
	if !isLive {
		httpStatus = http.StatusServiceUnavailable
	}

	// Return liveness status
	c.JSON(httpStatus, gin.H{
		"status":             isLive ? "live" : "not_live",
		"execution_time_ms":  executionTime.Milliseconds(),
		"timestamp":          time.Now().Format(time.RFC3339),
	})
}

// HandleReadiness handles readiness probe requests.
func (h *AdminHandler) HandleReadiness(c *gin.Context) {
	// Extract request ID for logging and tracking
	requestID := c.GetString("request_id")
	if requestID == "" {
		requestID = uuid.New().String()
	}

	// Create context with short timeout for readiness check
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(3)*time.Second,
	)
	defer cancel()

	// Start execution timer
	startTime := time.Now()

	// Perform readiness check
	isReady, components, err := h.healthService.CheckReadiness(ctx)
	if err != nil {
		h.logger.Error("Readiness check failed",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status": "error",
			"error":  err.Error(),
		})
		return
	}

	// Calculate execution time
	executionTime := time.Since(startTime)

	// Record metrics
	h.metricsCollector.ReadinessCheckLatency.Observe(executionTime.Seconds())
	h.metricsCollector.ReadinessChecksTotal.Inc()

	// Determine HTTP status code based on readiness status
	httpStatus := http.StatusOK
	if !isReady {
		httpStatus = http.StatusServiceUnavailable
	}

	// Return readiness status
	c.JSON(httpStatus, gin.H{
		"status":             isReady ? "ready" : "not_ready",
		"components":         components,
		"execution_time_ms":  executionTime.Milliseconds(),
		"timestamp":          time.Now().Format(time.RFC3339),
	})
}

// HandleConfig handles configuration retrieval and update requests.
func (h *AdminHandler) HandleConfig(c *gin.Context) {
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

	// Check if user has admin privileges
	if !user.HasRole("admin") {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "Admin privileges required",
		})
		return
	}

	// Handle based on HTTP method
	switch c.Request.Method {
	case http.MethodGet:
		h.getConfig(c, requestID, user)
	case http.MethodPut, http.MethodPost:
		h.updateConfig(c, requestID, user)
	default:
		c.JSON(http.StatusMethodNotAllowed, gin.H{
			"error": "Method not allowed",
		})
	}
}

// getConfig handles configuration retrieval requests.
func (h *AdminHandler) getConfig(c *gin.Context, requestID string, user *auth.UserInfo) {
	// Parse query parameters
	scope := c.DefaultQuery("scope", "global")
	targetID := c.Query("target_id")
	includeDefaults := c.DefaultQuery("include_defaults", "false") == "true"
	includeDescriptions := c.DefaultQuery("include_descriptions", "true") == "true"
	filter := c.Query("filter")

	// Create context with timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(constants.DefaultAdminTimeoutSeconds)*time.Second,
	)
	defer cancel()

	// Log config request
	h.logger.Info("Configuration retrieval requested",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("scope", scope),
		zap.String("target_id", targetID),
		zap.Bool("include_defaults", includeDefaults),
		zap.Bool("include_descriptions", includeDescriptions),
		zap.String("filter", filter),
	)

	// Prepare config options
	options := &admin.ConfigGetOptions{
		Scope:               scope,
		TargetID:            targetID,
		IncludeDefaults:     includeDefaults,
		IncludeDescriptions: includeDescriptions,
		Filter:              filter,
	}

	// Start execution timer
	startTime := time.Now()

	// Get configuration
	config, err := h.configService.GetConfig(ctx, options)
	if err != nil {
		h.logger.Error("Configuration retrieval failed",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to retrieve configuration: %v", err),
		})
		return
	}

	// Calculate execution time
	executionTime := time.Since(startTime)

	// Record metrics
	h.metricsCollector.ConfigRetrievalLatency.Observe(executionTime.Seconds())
	h.metricsCollector.ConfigRetrievalsTotal.Inc()

	// Return configuration
	c.JSON(http.StatusOK, gin.H{
		"config":             config,
		"scope":              scope,
		"target_id":          targetID,
		"execution_time_ms":  executionTime.Milliseconds(),
		"timestamp":          time.Now().Format(time.RFC3339),
	})
}

// updateConfig handles configuration update requests.
func (h *AdminHandler) updateConfig(c *gin.Context, requestID string, user *auth.UserInfo) {
	// Parse request body
	var req ConfigUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Warn("Failed to parse config update request",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("Invalid request format: %v", err),
		})
		return
	}

	// Validate request
	if len(req.Params) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "No configuration parameters provided",
		})
		return
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(constants.DefaultAdminTimeoutSeconds)*time.Second,
	)
	defer cancel()

	// Log config update request
	h.logger.Info("Configuration update requested",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("scope", req.Scope),
		zap.String("target_id", req.TargetID),
		zap.Bool("persist", req.Persist),
		zap.Any("params", req.Params),
	)

	// Prepare config update options
	options := &admin.ConfigUpdateOptions{
		Scope:    req.Scope,
		TargetID: req.TargetID,
		Persist:  req.Persist,
		Comment:  req.Comment,
		Params:   req.Params,
	}

	// Start execution timer
	startTime := time.Now()

	// Update configuration
	result, err := h.configService.UpdateConfig(ctx, options)
	if err != nil {
		h.logger.Error("Configuration update failed",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to update configuration: %v", err),
		})
		return
	}

	// Calculate execution time
	executionTime := time.Since(startTime)

	// Record metrics
	h.metricsCollector.ConfigUpdateLatency.Observe(executionTime.Seconds())
	h.metricsCollector.ConfigUpdatesTotal.Inc()

	// Return update result
	c.JSON(http.StatusOK, gin.H{
		"result":             result,
		"execution_time_ms":  executionTime.Milliseconds(),
		"timestamp":          time.Now().Format(time.RFC3339),
	})
}

// HandleMetrics handles metrics retrieval requests.
func (h *AdminHandler) HandleMetrics(c *gin.Context) {
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

	// Check if user has admin privileges
	if !user.HasRole("admin") && !user.HasRole("monitor") {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "Admin or monitor privileges required",
		})
		return
	}

	// Parse query parameters
	scope := c.DefaultQuery("scope", "proxy")
	targetID := c.Query("target_id")
	metricType := c.DefaultQuery("type", "all")
	format := c.DefaultQuery("format", "json")
	filter := c.Query("filter")

	// Create context with timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(constants.DefaultAdminTimeoutSeconds)*time.Second,
	)
	defer cancel()

	// Log metrics request
	h.logger.Info("Metrics retrieval requested",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("scope", scope),
		zap.String("target_id", targetID),
		zap.String("type", metricType),
		zap.String("format", format),
		zap.String("filter", filter),
	)

	// Prepare metrics options
	options := &admin.MetricsOptions{
		Scope:    scope,
		TargetID: targetID,
		Type:     metricType,
		Format:   format,
		Filter:   filter,
	}

	// Start execution timer
	startTime := time.Now()

	// Get metrics
	metrics, err := h.adminService.GetMetrics(ctx, options)
	if err != nil {
		h.logger.Error("Metrics retrieval failed",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to retrieve metrics: %v", err),
		})
		return
	}

	// Calculate execution time
	executionTime := time.Since(startTime)

	// Record metrics about metrics retrieval
	h.metricsCollector.MetricsRetrievalLatency.Observe(executionTime.Seconds())
	h.metricsCollector.MetricsRetrievalsTotal.Inc()

	// Return metrics based on requested format
	switch strings.ToLower(format) {
	case "prometheus":
		c.Header("Content-Type", "text/plain")
		c.String(http.StatusOK, metrics.PrometheusFormat)
	case "json":
		c.JSON(http.StatusOK, gin.H{
			"metrics":            metrics.Metrics,
			"scope":              scope,
			"target_id":          targetID,
			"execution_time_ms":  executionTime.Milliseconds(),
			"timestamp":          time.Now().Format(time.RFC3339),
		})
	default:
		c.JSON(http.StatusOK, gin.H{
			"metrics":            metrics.Metrics,
			"scope":              scope,
			"target_id":          targetID,
			"execution_time_ms":  executionTime.Milliseconds(),
			"timestamp":          time.Now().Format(time.RFC3339),
		})
	}
}

// HandleTablets handles tablet information requests.
func (h *AdminHandler) HandleTablets(c *gin.Context) {
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

	// Check if user has admin privileges
	if !user.HasRole("admin") && !user.HasRole("monitor") {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "Admin or monitor privileges required",
		})
		return
	}

	// Parse query parameters
	database := c.Query("database")
	table := c.Query("table")
	tabletID := c.Query("tablet_id")
	partition := c.Query("partition")
	status := c.Query("status")
	limitStr := c.DefaultQuery("limit", "100")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 100
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(constants.DefaultAdminTimeoutSeconds)*time.Second,
	)
	defer cancel()

	// Log tablet info request
	h.logger.Info("Tablet information requested",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("database", database),
		zap.String("table", table),
		zap.String("tablet_id", tabletID),
		zap.String("partition", partition),
		zap.String("status", status),
		zap.Int("limit", limit),
	)

	// Prepare tablet options
	options := &admin.TabletQueryOptions{
		Database:  database,
		Table:     table,
		TabletID:  tabletID,
		Partition: partition,
		Status:    status,
		Limit:     limit,
	}

	// Start execution timer
	startTime := time.Now()

	// Get tablet information
	tablets, err := h.adminService.GetTablets(ctx, options)
	if err != nil {
		h.logger.Error("Tablet information retrieval failed",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to retrieve tablet information: %v", err),
		})
		return
	}

	// Calculate execution time
	executionTime := time.Since(startTime)

	// Record metrics
	h.metricsCollector.TabletInfoRetrievalLatency.Observe(executionTime.Seconds())
	h.metricsCollector.TabletInfoRetrievalsTotal.Inc()

	// Return tablet information
	c.JSON(http.StatusOK, gin.H{
		"tablets":            tablets,
		"count":              len(tablets),
		"execution_time_ms":  executionTime.Milliseconds(),
		"timestamp":          time.Now().Format(time.RFC3339),
	})
}

// HandleBackends handles backend server information requests.
func (h *AdminHandler) HandleBackends(c *gin.Context) {
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

	// Check if user has admin privileges
	if !user.HasRole("admin") && !user.HasRole("monitor") {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "Admin or monitor privileges required",
		})
		return
	}

	// Parse query parameters
	backendID := c.Query("backend_id")
	host := c.Query("host")
	status := c.Query("status")
	detailedMetrics := c.DefaultQuery("detailed_metrics", "false") == "true"
	includeTablets := c.DefaultQuery("include_tablets", "false") == "true"
	includeTasks := c.DefaultQuery("include_tasks", "false") == "true"

	// Create context with timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(constants.DefaultAdminTimeoutSeconds)*time.Second,
	)
	defer cancel()

	// Log backend info request
	h.logger.Info("Backend information requested",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("backend_id", backendID),
		zap.String("host", host),
		zap.String("status", status),
		zap.Bool("detailed_metrics", detailedMetrics),
		zap.Bool("include_tablets", includeTablets),
		zap.Bool("include_tasks", includeTasks),
	)

	// Prepare backend options
	options := &admin.BackendQueryOptions{
		BackendID:       backendID,
		Host:            host,
		Status:          status,
		DetailedMetrics: detailedMetrics,
		IncludeTablets:  includeTablets,
		IncludeTasks:    includeTasks,
	}

	// Start execution timer
	startTime := time.Now()

	// Get backend information
	backends, err := h.adminService.GetBackends(ctx, options)
	if err != nil {
		h.logger.Error("Backend information retrieval failed",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to retrieve backend information: %v", err),
		})
		return
	}

	// Calculate execution time
	executionTime := time.Since(startTime)

	// Record metrics
	h.metricsCollector.BackendInfoRetrievalLatency.Observe(executionTime.Seconds())
	h.metricsCollector.BackendInfoRetrievalsTotal.Inc()

	// Return backend information
	c.JSON(http.StatusOK, gin.H{
		"backends":           backends,
		"count":              len(backends),
		"execution_time_ms":  executionTime.Milliseconds(),
		"timestamp":          time.Now().Format(time.RFC3339),
	})
}

// HandleJobs handles job status information requests.
func (h *AdminHandler) HandleJobs(c *gin.Context) {
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

	// Check if user has admin privileges
	if !user.HasRole("admin") && !user.HasRole("monitor") {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "Admin or monitor privileges required",
		})
		return
	}

	// Parse query parameters
	jobType := c.DefaultQuery("job_type", "")
	jobID := c.Query("job_id")
	status := c.Query("status")
	limitStr := c.DefaultQuery("limit", "100")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 100
	}
	startTimeStr := c.Query("start_time")
	endTimeStr := c.Query("end_time")

	// Parse time ranges if provided
	var startTime, endTime *time.Time
	if startTimeStr != "" {
		t, err := time.Parse(time.RFC3339, startTimeStr)
		if err == nil {
			startTime = &t
		}
	}
	if endTimeStr != "" {
		t, err := time.Parse(time.RFC3339, endTimeStr)
		if err == nil {
			endTime = &t
		}
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(constants.DefaultAdminTimeoutSeconds)*time.Second,
	)
	defer cancel()

	// Log job info request
	h.logger.Info("Job information requested",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.String("job_type", jobType),
		zap.String("job_id", jobID),
		zap.String("status", status),
		zap.Int("limit", limit),
	)

	// Prepare job options
	options := &admin.JobQueryOptions{
		JobType:   jobType,
		JobID:     jobID,
		Status:    status,
		Limit:     limit,
		StartTime: startTime,
		EndTime:   endTime,
	}

	// Start execution timer
	startTime2 := time.Now()

	// Get job information
	jobs, err := h.adminService.GetJobs(ctx, options)
	if err != nil {
		h.logger.Error("Job information retrieval failed",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to retrieve job information: %v", err),
		})
		return
	}

	// Calculate execution time
	executionTime := time.Since(startTime2)

	// Record metrics
	h.metricsCollector.JobInfoRetrievalLatency.Observe(executionTime.Seconds())
	h.metricsCollector.JobInfoRetrievalsTotal.Inc()

	// Return job information
	c.JSON(http.StatusOK, gin.H{
		"jobs":               jobs,
		"count":              len(jobs),
		"execution_time_ms":  executionTime.Milliseconds(),
		"timestamp":          time.Now().Format(time.RFC3339),
	})
}

// HandleClusterInfo handles cluster information requests.
func (h *AdminHandler) HandleClusterInfo(c *gin.Context) {
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

	// Check if user has admin privileges
	if !user.HasRole("admin") && !user.HasRole("monitor") {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "Admin or monitor privileges required",
		})
		return
	}

	// Parse query parameters
	includeFEs := c.DefaultQuery("include_fes", "true") == "true"
	includeBEs := c.DefaultQuery("include_bes", "true") == "true"
	includeSystemInfo := c.DefaultQuery("include_system_info", "true") == "true"
	includeResourceUsage := c.DefaultQuery("include_resource_usage", "true") == "true"

	// Create context with timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(constants.DefaultAdminTimeoutSeconds)*time.Second,
	)
	defer cancel()

	// Log cluster info request
	h.logger.Info("Cluster information requested",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.Bool("include_fes", includeFEs),
		zap.Bool("include_bes", includeBEs),
		zap.Bool("include_system_info", includeSystemInfo),
		zap.Bool("include_resource_usage", includeResourceUsage),
	)

	// Prepare cluster info options
	options := &admin.ClusterInfoOptions{
		IncludeFEs:          includeFEs,
		IncludeBEs:          includeBEs,
		IncludeSystemInfo:   includeSystemInfo,
		IncludeResourceUsage: includeResourceUsage,
	}

	// Start execution timer
	startTime := time.Now()

	// Get cluster information
	clusterInfo, err := h.adminService.GetClusterInfo(ctx, options)
	if err != nil {
		h.logger.Error("Cluster information retrieval failed",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to retrieve cluster information: %v", err),
		})
		return
	}

	// Calculate execution time
	executionTime := time.Since(startTime)

	// Record metrics
	h.metricsCollector.ClusterInfoRetrievalLatency.Observe(executionTime.Seconds())
	h.metricsCollector.ClusterInfoRetrievalsTotal.Inc()

	// Return cluster information
	c.JSON(http.StatusOK, gin.H{
		"cluster":            clusterInfo,
		"execution_time_ms":  executionTime.Milliseconds(),
		"timestamp":          time.Now().Format(time.RFC3339),
	})
}

// HandleLogLevel handles log level changes.
func (h *AdminHandler) HandleLogLevel(c *gin.Context) {
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

	// Check if user has admin privileges
	if !user.HasRole("admin") {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "Admin privileges required",
		})
		return
	}

	// Handle based on HTTP method
	switch c.Request.Method {
	case http.MethodGet:
		// Return current log level
		level := logging.GetLogLevel()
		c.JSON(http.StatusOK, gin.H{
			"level": level,
		})

	case http.MethodPut, http.MethodPost:
		// Parse request body
		var req struct {
			Level string `json:"level" binding:"required"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			h.logger.Warn("Failed to parse log level update request",
				zap.String("request_id", requestID),
				zap.Error(err),
			)
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("Invalid request format: %v", err),
			})
			return
		}

		// Validate and update log level
		err := logging.SetLogLevel(req.Level)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("Invalid log level: %v", err),
			})
			return
		}

		// Log the change
		h.logger.Info("Log level changed",
			zap.String("request_id", requestID),
			zap.String("user", user.Username),
			zap.String("level", req.Level),
		)

		c.JSON(http.StatusOK, gin.H{
			"message": fmt.Sprintf("Log level changed to %s", req.Level),
			"level":   req.Level,
		})

	default:
		c.JSON(http.StatusMethodNotAllowed, gin.H{
			"error": "Method not allowed",
		})
	}
}

// HandleSystemDiagnostics handles system diagnostics information requests.
func (h *AdminHandler) HandleSystemDiagnostics(c *gin.Context) {
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

	// Check if user has admin privileges
	if !user.HasRole("admin") {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "Admin privileges required",
		})
		return
	}

	// Parse query parameters
	includeProxy := c.DefaultQuery("include_proxy", "true") == "true"
	includeFEs := c.DefaultQuery("include_fes", "true") == "true"
	includeBEs := c.DefaultQuery("include_bes", "true") == "true"
	includeSystem := c.DefaultQuery("include_system", "true") == "true"
	includeProcesses := c.DefaultQuery("include_processes", "true") == "true"

	// Create context with timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(constants.DefaultAdminLongTimeoutSeconds)*time.Second,
	)
	defer cancel()

	// Log diagnostics request
	h.logger.Info("System diagnostics requested",
		zap.String("request_id", requestID),
		zap.String("user", user.Username),
		zap.Bool("include_proxy", includeProxy),
		zap.Bool("include_fes", includeFEs),
		zap.Bool("include_bes", includeBEs),
		zap.Bool("include_system", includeSystem),
		zap.Bool("include_processes", includeProcesses),
	)

	// Prepare diagnostics options
	options := &admin.DiagnosticsOptions{
		IncludeProxy:     includeProxy,
		IncludeFEs:       includeFEs,
		IncludeBEs:       includeBEs,
		IncludeSystem:    includeSystem,
		IncludeProcesses: includeProcesses,
	}

	// Start execution timer
	startTime := time.Now()

	// Get system diagnostics
	diagnostics, err := h.adminService.GetSystemDiagnostics(ctx, options)
	if err != nil {
		h.logger.Error("System diagnostics retrieval failed",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to retrieve system diagnostics: %v", err),
		})
		return
	}

	// Calculate execution time
	executionTime := time.Since(startTime)

	// Record metrics
	h.metricsCollector.DiagnosticsRetrievalLatency.Observe(executionTime.Seconds())
	h.metricsCollector.DiagnosticsRetrievalsTotal.Inc()

	// Return system diagnostics
	c.JSON(http.StatusOK, gin.H{
		"diagnostics":        diagnostics,
		"execution_time_ms":  executionTime.Milliseconds(),
		"timestamp":          time.Now().Format(time.RFC3339),
	})
}

// HandleProxyInfo handles proxy information requests.
func (h *AdminHandler) HandleProxyInfo(c *gin.Context) {
	// Extract request ID for logging and tracking
	requestID := c.GetString("request_id")
	if requestID == "" {
		requestID = uuid.New().String()
	}

	// Create context with short timeout
	ctx, cancel := context.WithTimeout(
		c.Request.Context(),
		time.Duration(2)*time.Second,
	)
	defer cancel()

	// Log request
	h.logger.Debug("Proxy information requested",
		zap.String("request_id", requestID),
	)

	// Start execution timer
	startTime := time.Now()

	// Get proxy information
	proxyInfo, err := h.adminService.GetProxyInfo(ctx)
	if err != nil {
		h.logger.Error("Proxy information retrieval failed",
			zap.String("request_id", requestID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to retrieve proxy information: %v", err),
		})
		return
	}

	// Calculate execution time
	executionTime := time.Since(startTime)

	// Record metrics
	h.metricsCollector.ProxyInfoRetrievalLatency.Observe(executionTime.Seconds())
	h.metricsCollector.ProxyInfoRetrievalsTotal.Inc()

	// Return proxy information
	c.JSON(http.StatusOK, gin.H{
		"proxy":              proxyInfo,
		"execution_time_ms":  executionTime.Milliseconds(),
		"timestamp":          time.Now().Format(time.RFC3339),
	})
}
//Personal.AI order the ending
