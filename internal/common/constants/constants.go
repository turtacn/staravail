// Package constants defines global constants used across the project.
package constants

import "time"

// Default configuration constants
const (
	// DefaultAPIPort is the default port for the API server
	DefaultAPIPort = 8080
	// DefaultMetricsPort is the default port for metrics endpoint
	DefaultMetricsPort = 9090
	// DefaultAdminPort is the default port for admin interface
	DefaultAdminPort = 8081

	// DefaultRequestTimeout is the default timeout for HTTP requests
	DefaultRequestTimeout = 30 * time.Second
	// DefaultDialTimeout is the default timeout for establishing connections
	DefaultDialTimeout = 5 * time.Second
	// DefaultShutdownTimeout is the default timeout for graceful shutdown
	DefaultShutdownTimeout = 10 * time.Second

	// DefaultRetryCount is the default number of retries for operations
	DefaultRetryCount = 3
	// DefaultRetryInterval is the default interval between retries
	DefaultRetryInterval = 500 * time.Millisecond
	// DefaultBackoffMultiplier is the default multiplier for exponential backoff
	DefaultBackoffMultiplier = 2.0
	// DefaultMaxRetryInterval is the maximum interval between retries
	DefaultMaxRetryInterval = 30 * time.Second

	// DefaultWorkerPoolSize is the default size of worker pools
	DefaultWorkerPoolSize = 100
	// DefaultQueueSize is the default size of work queues
	DefaultQueueSize = 1000
)

// API paths
const (
	// HealthCheckPath is the API path for health checks
	HealthCheckPath = "/api/health"
	// ReadyCheckPath is the API path for readiness checks
	ReadyCheckPath = "/api/ready"
	// MetricsPath is the API path for metrics endpoint
	MetricsPath = "/metrics"

	// AdminAPIPrefix is the prefix for all admin APIs
	AdminAPIPrefix = "/admin/api"
	// ConfigEndpoint is the API path for configuration management
	ConfigEndpoint = AdminAPIPrefix + "/config"
	// StatusEndpoint is the API path for service status
	StatusEndpoint = AdminAPIPrefix + "/status"
	// ControlEndpoint is the API path for control operations
	ControlEndpoint = AdminAPIPrefix + "/control"

	// V1APIPrefix is the prefix for v1 APIs
	V1APIPrefix = "/api/v1"
	// QueryEndpoint is the API path for query operations
	QueryEndpoint = V1APIPrefix + "/query"
	// WriteEndpoint is the API path for write operations
	WriteEndpoint = V1APIPrefix + "/write"
)

// StarRocks FE API paths
const (
	// StarRocksAPIPrefix is the prefix for StarRocks FE API
	StarRocksAPIPrefix = "/api"

	// BackendsEndpoint is the API path for backend information
	BackendsEndpoint = StarRocksAPIPrefix + "/show_backends"
	// TabletsEndpoint is the API path for tablet information
	TabletsEndpoint = StarRocksAPIPrefix + "/show_tablets"
	// DatabasesEndpoint is the API path for database information
	DatabasesEndpoint = StarRocksAPIPrefix + "/show_databases"
	// TablesEndpoint is the API path for table information
	TablesEndpoint = StarRocksAPIPrefix + "/show_tables"
	// VariablesEndpoint is the API path for variable information
	VariablesEndpoint = StarRocksAPIPrefix + "/show_variables"
	// ProcEndpoint is the API path for process information
	ProcEndpoint = StarRocksAPIPrefix + "/show_proc"
	// SubmitTaskEndpoint is the API path for submitting tasks
	SubmitTaskEndpoint = StarRocksAPIPrefix + "/submit_task"
)

// HTTP status codes and custom status codes
const (
	// Success status code
	StatusSuccess = 0
	// Generic error status code
	StatusError = 1

	// StatusInvalidRequest indicates an invalid request
	StatusInvalidRequest = 100
	// StatusAuthenticationFailed indicates an authentication failure
	StatusAuthenticationFailed = 101
	// StatusAuthorizationFailed indicates an authorization failure
	StatusAuthorizationFailed = 102
	// StatusResourceNotFound indicates a resource was not found
	StatusResourceNotFound = 103
	// StatusResourceExists indicates a resource already exists
	StatusResourceExists = 104
	// StatusResourceUnavailable indicates a resource is unavailable
	StatusResourceUnavailable = 105

	// StatusBackendError indicates a backend error
	StatusBackendError = 200
	// StatusBackendTimeout indicates a backend timeout
	StatusBackendTimeout = 201
	// StatusBackendOverload indicates a backend is overloaded
	StatusBackendOverload = 202

	// StatusInternalError indicates an internal server error
	StatusInternalError = 500
	// StatusNotImplemented indicates a feature is not implemented
	StatusNotImplemented = 501
)

// Query optimization constants
const (
	// MaxQueryLength is the maximum length of a query in bytes
	MaxQueryLength = 1 * 1024 * 1024 // 1MB
	// MaxPruneDepth is the maximum depth for query pruning
	MaxPruneDepth = 5
	// DefaultPruneThreshold is the default threshold for pruning decisions
	DefaultPruneThreshold = 0.7
	// MaxQueryComplexity is the maximum complexity score for a query
	MaxQueryComplexity = 100
	// DefaultQueryTimeout is the default timeout for query execution
	DefaultQueryTimeout = 60 * time.Second
	// MaxConcurrentQueries is the maximum number of concurrent queries
	MaxConcurrentQueries = 200
	// DefaultQueryCacheTTL is the default TTL for query cache entries
	DefaultQueryCacheTTL = 5 * time.Minute
)

// Write buffer constants
const (
	// DefaultBufferSize is the default size of write buffers
	DefaultBufferSize = 10 * 1024 * 1024 // 10MB
	// DefaultFlushInterval is the default interval for buffer flushing
	DefaultFlushInterval = 5 * time.Second
	// MaxBufferSize is the maximum size of write buffers
	MaxBufferSize = 100 * 1024 * 1024 // 100MB
	// MinFlushInterval is the minimum interval for buffer flushing
	MinFlushInterval = 100 * time.Millisecond
	// BufferHighWatermark is the high watermark percentage for buffer flushing
	BufferHighWatermark = 0.8
	// BufferLowWatermark is the low watermark percentage for buffer flushing
	BufferLowWatermark = 0.2
)

// Batch processing constants
const (
	// DefaultBatchSize is the default size of a batch
	DefaultBatchSize = 1000
	// DefaultBatchInterval is the default interval for batch processing
	DefaultBatchInterval = 1 * time.Second
	// MaxBatchSize is the maximum size of a batch
	MaxBatchSize = 10000
	// MinBatchSize is the minimum size of a batch
	MinBatchSize = 10
	// MaxBatchDelay is the maximum delay for batch processing
	MaxBatchDelay = 10 * time.Second
	// BatchSizeIncreaseRate is the rate at which batch size increases under low load
	BatchSizeIncreaseRate = 1.2
	// BatchSizeDecreaseRate is the rate at which batch size decreases under high load
	BatchSizeDecreaseRate = 0.8
)

// Monitoring constants
const (
	// MetricsPrefix is the prefix for all metrics names
	MetricsPrefix = "starrocks_proxy_"
	// DefaultMonitoringInterval is the default interval for metrics collection
	DefaultMonitoringInterval = 15 * time.Second

	// QueryCountMetric is the metric name for query count
	QueryCountMetric = MetricsPrefix + "query_count"
	// QueryLatencyMetric is the metric name for query latency
	QueryLatencyMetric = MetricsPrefix + "query_latency"
	// QueryErrorMetric is the metric name for query errors
	QueryErrorMetric = MetricsPrefix + "query_error"

	// WriteCountMetric is the metric name for write count
	WriteCountMetric = MetricsPrefix + "write_count"
	// WriteLatencyMetric is the metric name for write latency
	WriteLatencyMetric = MetricsPrefix + "write_latency"
	// WriteErrorMetric is the metric name for write errors
	WriteErrorMetric = MetricsPrefix + "write_error"

	// BackendHealthMetric is the metric name for backend health
	BackendHealthMetric = MetricsPrefix + "backend_health"
	// BackendLatencyMetric is the metric name for backend latency
	BackendLatencyMetric = MetricsPrefix + "backend_latency"

	// BufferSizeMetric is the metric name for buffer size
	BufferSizeMetric = MetricsPrefix + "buffer_size"
	// BufferFlushCountMetric is the metric name for buffer flush count
	BufferFlushCountMetric = MetricsPrefix + "buffer_flush_count"

	// ResourceUsageMetric is the metric name for resource usage
	ResourceUsageMetric = MetricsPrefix + "resource_usage"
)

//Personal.AI order the ending
