// Package dto defines data transfer objects used across the project.
package dto

import (
	"time"

	"github.com/turtacn/staravail/internal/common/types/enum"
	"github.com/turtacn/staravail/internal/common/types/model"
)

// Common response structure
// -------------------------------

// BaseResponse is the base structure for all API responses.
type BaseResponse struct {
	// Code is the status code of the response
	Code int `json:"code"`
	// Message is a human-readable message
	Message string `json:"msg"`
	// RequestID is the unique identifier for the request
	RequestID string `json:"request_id"`
	// Timestamp is the time when the response was generated
	Timestamp int64 `json:"timestamp"`
}

// StarRocks FE API Response DTOs
// -------------------------------

// BackendResponse represents the response from the StarRocks FE backend API.
type BackendResponse struct {
	BaseResponse
	// Data contains the backend information
	Data struct {
		// Backends is the list of backends
		Backends []BackendDTO `json:"backends"`
		// TotalRows is the total number of backends
		TotalRows int `json:"total_rows"`
	} `json:"data"`
}

// BackendDTO represents a backend in the StarRocks FE API response.
type BackendDTO struct {
	// ID is the unique identifier of the backend
	ID int64 `json:"id"`
	// Host is the hostname or IP address of the backend
	Host string `json:"host"`
	// HeartbeatPort is the port used for heartbeat communication
	HeartbeatPort int `json:"heartbeat_port"`
	// BePort is the port used for backend service
	BePort int `json:"be_port"`
	// HttpPort is the port used for HTTP service
	HttpPort int `json:"http_port"`
	// BrpcPort is the port used for BRPC service
	BrpcPort int `json:"brpc_port"`
	// Status is the status of the backend
	Status string `json:"status"`
	// Alive indicates whether the backend is alive
	Alive bool `json:"alive"`
	// LastStartTime is the time when this backend was last started
	LastStartTime int64 `json:"last_start_time"`
	// LastHeartbeat is the time of the last heartbeat received from this backend
	LastHeartbeat int64 `json:"last_heartbeat"`
	// DiskCapacity is the total disk capacity of this backend in bytes
	DiskCapacity int64 `json:"disk_capacity"`
	// DiskAvailable is the available disk space of this backend in bytes
	DiskAvailable int64 `json:"disk_available"`
	// DiskUsed is the used disk space of this backend in bytes
	DiskUsed int64 `json:"disk_used"`
}

// TabletResponse represents the response from the StarRocks FE tablet API.
type TabletResponse struct {
	BaseResponse
	// Data contains the tablet information
	Data struct {
		// Tablets is the list of tablets
		Tablets []TabletDTO `json:"tablets"`
		// TotalRows is the total number of tablets
		TotalRows int `json:"total_rows"`
	} `json:"data"`
}

// TabletDTO represents a tablet in the StarRocks FE API response.
type TabletDTO struct {
	// ID is the unique identifier of the tablet
	ID int64 `json:"tablet_id"`
	// TableID is the ID of the table this tablet belongs to
	TableID int64 `json:"table_id"`
	// PartitionID is the ID of the partition this tablet belongs to
	PartitionID int64 `json:"partition_id"`
	// BackendIDs is the list of backend IDs hosting this tablet
	BackendIDs []int64 `json:"backend_ids"`
	// Path is the storage path of this tablet
	Path string `json:"path"`
	// SchemaHash is the hash of the schema for this tablet
	SchemaHash int `json:"schema_hash"`
	// Status is the status of the tablet
	Status string `json:"status"`
	// Version is the version number of this tablet
	Version int64 `json:"version"`
	// DataSize is the size of data in this tablet in bytes
	DataSize int64 `json:"data_size"`
	// RowCount is the approximate number of rows in this tablet
	RowCount int64 `json:"row_count"`
	// LastCheckTime is the time when this tablet was last checked
	LastCheckTime int64 `json:"last_check_time"`
}

// DatabaseResponse represents the response from the StarRocks FE database API.
type DatabaseResponse struct {
	BaseResponse
	// Data contains the database information
	Data struct {
		// Databases is the list of databases
		Databases []DatabaseDTO `json:"databases"`
		// TotalRows is the total number of databases
		TotalRows int `json:"total_rows"`
	} `json:"data"`
}

// DatabaseDTO represents a database in the StarRocks FE API response.
type DatabaseDTO struct {
	// ID is the unique identifier of the database
	ID int64 `json:"id"`
	// Name is the name of the database
	Name string `json:"name"`
	// ClusterID is the ID of the cluster this database belongs to
	ClusterID int64 `json:"cluster_id"`
	// Tables is the number of tables in this database
	Tables int `json:"tables"`
	// CreatedTime is the time when this database was created
	CreatedTime int64 `json:"create_time"`
}

// TableResponse represents the response from the StarRocks FE table API.
type TableResponse struct {
	BaseResponse
	// Data contains the table information
	Data struct {
		// Tables is the list of tables
		Tables []TableDTO `json:"tables"`
		// TotalRows is the total number of tables
		TotalRows int `json:"total_rows"`
	} `json:"data"`
}

// TableDTO represents a table in the StarRocks FE API response.
type TableDTO struct {
	// ID is the unique identifier of the table
	ID int64 `json:"id"`
	// Name is the name of the table
	Name string `json:"name"`
	// DatabaseID is the ID of the database this table belongs to
	DatabaseID int64 `json:"database_id"`
	// Type is the type of the table
	Type string `json:"type"`
	// CreateTime is the time when this table was created
	CreateTime int64 `json:"create_time"`
	// LastUpdateTime is the time when this table was last updated
	LastUpdateTime int64 `json:"last_update_time"`
	// IndexID is the ID of the index
	IndexID int64 `json:"index_id"`
	// State is the state of the table
	State string `json:"state"`
	// RowCount is the approximate number of rows in this table
	RowCount int64 `json:"row_count"`
	// DataSize is the size of data in this table in bytes
	DataSize int64 `json:"data_size"`
	// ReplicaCount is the number of replicas for this table
	ReplicaCount int `json:"replica_count"`
}

// PartitionResponse represents the response from the StarRocks FE partition API.
type PartitionResponse struct {
	BaseResponse
	// Data contains the partition information
	Data struct {
		// Partitions is the list of partitions
		Partitions []PartitionDTO `json:"partitions"`
		// TotalRows is the total number of partitions
		TotalRows int `json:"total_rows"`
	} `json:"data"`
}

// PartitionDTO represents a partition in the StarRocks FE API response.
type PartitionDTO struct {
	// ID is the unique identifier of the partition
	ID int64 `json:"id"`
	// Name is the name of the partition
	Name string `json:"name"`
	// TableID is the ID of the table this partition belongs to
	TableID int64 `json:"table_id"`
	// State is the state of the partition
	State string `json:"state"`
	// VisibleVersion is the visible version of this partition
	VisibleVersion int64 `json:"visible_version"`
	// CreateTime is the time when this partition was created
	CreateTime int64 `json:"create_time"`
	// DataSize is the size of data in this partition in bytes
	DataSize int64 `json:"data_size"`
	// RowCount is the approximate number of rows in this partition
	RowCount int64 `json:"row_count"`
	// PartitionKey is the key used for partitioning
	PartitionKey string `json:"partition_key"`
	// PartitionValues are the values defining this partition
	PartitionValues []string `json:"partition_values"`
}

// HTTP API Request/Response DTOs
// -------------------------------

// QueryRequest represents a query request to the proxy API.
type QueryRequest struct {
	// SQL is the SQL query to be executed
	SQL string `json:"sql"`
	// Database is the database to execute the query against
	Database string `json:"database"`
	// Timeout is the maximum time to wait for the query to complete (in milliseconds)
	Timeout int `json:"timeout,omitempty"`
	// MaxRows is the maximum number of rows to return
	MaxRows int `json:"max_rows,omitempty"`
	// Parameters are the named parameters for the query
	Parameters map[string]interface{} `json:"parameters,omitempty"`
	// Options are additional query options
	Options map[string]interface{} `json:"options,omitempty"`
	// TraceID is the trace ID for distributed tracing
	TraceID string `json:"trace_id,omitempty"`
}

// QueryResponse represents a query response from the proxy API.
type QueryResponse struct {
	BaseResponse
	// Data contains the query results
	Data struct {
		// Columns is the list of columns in the result
		Columns []ColumnMetadata `json:"columns"`
		// Rows is the list of rows in the result
		Rows [][]interface{} `json:"rows"`
		// RowCount is the number of rows in the result
		RowCount int `json:"row_count"`
		// ExecutionTime is the time taken to execute the query in milliseconds
		ExecutionTime int64 `json:"execution_time"`
		// ScanRows is the number of rows scanned
		ScanRows int64 `json:"scan_rows,omitempty"`
		// ScanBytes is the number of bytes scanned
		ScanBytes int64 `json:"scan_bytes,omitempty"`
	} `json:"data"`
}

// ColumnMetadata represents metadata about a column in a query result.
type ColumnMetadata struct {
	// Name is the name of the column
	Name string `json:"name"`
	// Type is the data type of the column
	Type string `json:"type"`
	// Nullable indicates whether the column can contain NULL values
	Nullable bool `json:"nullable"`
}

// WriteRequest represents a write request to the proxy API.
type WriteRequest struct {
	// Database is the database to write to
	Database string `json:"database"`
	// Table is the table to write to
	Table string `json:"table"`
	// Format is the format of the data
	Format string `json:"format"`
	// Data is the data to be written
	Data interface{} `json:"data"`
	// Columns are the columns in the data
	Columns []string `json:"columns,omitempty"`
	// PartitionKey is the partition key to use
	PartitionKey string `json:"partition_key,omitempty"`
	// PartitionValue is the partition value to use
	PartitionValue string `json:"partition_value,omitempty"`
	// Options are additional write options
	Options map[string]interface{} `json:"options,omitempty"`
	// BatchID is the ID of the batch this write is part of
	BatchID string `json:"batch_id,omitempty"`
	// Timeout is the maximum time to wait for the write to complete (in milliseconds)
	Timeout int `json:"timeout,omitempty"`
	// TraceID is the trace ID for distributed tracing
	TraceID string `json:"trace_id,omitempty"`
}

// WriteResponse represents a write response from the proxy API.
type WriteResponse struct {
	BaseResponse
	// Data contains the write results
	Data struct {
		// RowsWritten is the number of rows written
		RowsWritten int64 `json:"rows_written"`
		// BytesWritten is the number of bytes written
		BytesWritten int64 `json:"bytes_written"`
		// ExecutionTime is the time taken to execute the write in milliseconds
		ExecutionTime int64 `json:"execution_time"`
		// FailedRows is the number of rows that failed to write
		FailedRows int64 `json:"failed_rows,omitempty"`
	} `json:"data"`
}

// HealthCheckRequest represents a health check request to the proxy API.
type HealthCheckRequest struct {
	// Detailed indicates whether to return detailed health information
	Detailed bool `json:"detailed,omitempty"`
	// Components is the list of components to check
	Components []string `json:"components,omitempty"`
}

// HealthCheckResponse represents a health check response from the proxy API.
type HealthCheckResponse struct {
	BaseResponse
	// Data contains the health check results
	Data struct {
		// Status is the overall status of the system
		Status string `json:"status"`
		// Uptime is the uptime of the system in seconds
		Uptime int64 `json:"uptime"`
		// Components is a map of component names to their health status
		Components map[string]ComponentHealth `json:"components,omitempty"`
		// Details contains detailed health information
		Details HealthDetails `json:"details,omitempty"`
	} `json:"data"`
}

// ComponentHealth represents the health of a system component.
type ComponentHealth struct {
	// Status is the status of the component
	Status string `json:"status"`
	// Message is a message about the component health
	Message string `json:"message,omitempty"`
	// LastCheck is the time when the component was last checked
	LastCheck int64 `json:"last_check"`
}

// HealthDetails represents detailed health information.
type HealthDetails struct {
	// BackendHealth is the health of the backends
	BackendHealth BackendHealth `json:"backend_health,omitempty"`
	// TabletHealth is the health of the tablets
	TabletHealth TabletHealth `json:"tablet_health,omitempty"`
	// SystemHealth is the health of the system
	SystemHealth SystemHealth `json:"system_health,omitempty"`
}

// BackendHealth represents the health of the backends.
type BackendHealth struct {
	// TotalBackends is the total number of backends
	TotalBackends int `json:"total_backends"`
	// HealthyBackends is the number of healthy backends
	HealthyBackends int `json:"healthy_backends"`
	// UnhealthyBackends is the list of unhealthy backends
	UnhealthyBackends []string `json:"unhealthy_backends,omitempty"`
}

// TabletHealth represents the health of the tablets.
type TabletHealth struct {
	// TotalTablets is the total number of tablets
	TotalTablets int `json:"total_tablets"`
	// HealthyTablets is the number of healthy tablets
	HealthyTablets int `json:"healthy_tablets"`
	// UnhealthyTablets is the number of unhealthy tablets
	UnhealthyTablets int `json:"unhealthy_tablets"`
}

// SystemHealth represents the health of the system.
type SystemHealth struct {
	// CPUUsage is the CPU usage as a percentage
	CPUUsage float64 `json:"cpu_usage"`
	// MemoryUsage is the memory usage as a percentage
	MemoryUsage float64 `json:"memory_usage"`
	// DiskUsage is the disk usage as a percentage
	DiskUsage float64 `json:"disk_usage"`
	// QPS is the queries per second
	QPS float64 `json:"qps"`
	// ErrorRate is the error rate as a percentage
	ErrorRate float64 `json:"error_rate"`
}

// Internal Service Communication DTOs
// -------------------------------

// TabletStateNotification represents a notification about a tablet state change.
type TabletStateNotification struct {
	// TabletID is the ID of the tablet
	TabletID int64 `json:"tablet_id"`
	// TableID is the ID of the table
	TableID int64 `json:"table_id"`
	// PartitionID is the ID of the partition
	PartitionID int64 `json:"partition_id"`
	// OldStatus is the old status of the tablet
	OldStatus enum.TabletStatus `json:"old_status"`
	// NewStatus is the new status of the tablet
	NewStatus enum.TabletStatus `json:"new_status"`
	// BackendID is the ID of the backend where the status changed
	BackendID int64 `json:"backend_id"`
	// Timestamp is the time when the status changed
	Timestamp time.Time `json:"timestamp"`
	// Reason is the reason for the status change
	Reason string `json:"reason,omitempty"`
}

// BEStateChange represents a notification about a backend state change.
type BEStateChange struct {
	// BackendID is the ID of the backend
	BackendID int64 `json:"backend_id"`
	// Host is the hostname or IP address of the backend
	Host string `json:"host"`
	// OldStatus is the old status of the backend
	OldStatus enum.BackendStatus `json:"old_status"`
	// NewStatus is the new status of the backend
	NewStatus enum.BackendStatus `json:"new_status"`
	// Timestamp is the time when the status changed
	Timestamp time.Time `json:"timestamp"`
	// Reason is the reason for the status change
	Reason string `json:"reason,omitempty"`
	// AffectedTablets is the list of affected tablets
	AffectedTablets []int64 `json:"affected_tablets,omitempty"`
}

// QueryRoutingInfo represents information about how to route a query.
type QueryRoutingInfo struct {
	// QueryID is the unique identifier for the query
	QueryID string `json:"query_id"`
	// SQL is the SQL text of the query
	SQL string `json:"sql"`
	// Database is the database against which the query will be executed
	Database string `json:"database"`
	// Tables is the list of tables involved in the query
	Tables []string `json:"tables"`
	// PartitionIDs is the list of partition IDs involved in the query
	PartitionIDs []int64 `json:"partition_ids"`
	// TabletIDs is the list of tablet IDs involved in the query
	TabletIDs []int64 `json:"tablet_ids"`
	// PreferredBackendIDs is the list of preferred backend IDs for the query
	PreferredBackendIDs []int64 `json:"preferred_backend_ids"`
	// Strategy is the query handling strategy
	Strategy enum.QueryHandlingStrategy `json:"strategy"`
	// Deadline is the deadline for the query
	Deadline time.Time `json:"deadline"`
}

// WriteRoutingInfo represents information about how to route a write operation.
type WriteRoutingInfo struct {
	// WriteID is the unique identifier for the write operation
	WriteID string `json:"write_id"`
	// Database is the database to write to
	Database string `json:"database"`
	// Table is the table to write to
	Table string `json:"table"`
	// PartitionID is the ID of the partition to write to
	PartitionID int64 `json:"partition_id"`
	// TabletIDs is the list of tablet IDs to write to
	TabletIDs []int64 `json:"tablet_ids"`
	// PreferredBackendIDs is the list of preferred backend IDs for the write
	PreferredBackendIDs []int64 `json:"preferred_backend_ids"`
	// Strategy is the write handling strategy
	Strategy enum.WriteHandlingStrategy `json:"strategy"`
	// Deadline is the deadline for the write operation
	Deadline time.Time `json:"deadline"`
}

// Batch Processing DTOs
// -------------------------------

// BatchJob represents a batch processing job.
type BatchJob struct {
	// ID is the unique identifier for the batch job
	ID string `json:"id"`
	// Type is the type of batch job
	Type string `json:"type"`
	// Status is the status of the batch job
	Status string `json:"status"`
	// CreatedAt is the time when the batch job was created
	CreatedAt time.Time `json:"created_at"`
	// StartedAt is the time when the batch job was started
	StartedAt time.Time `json:"started_at,omitempty"`
	// CompletedAt is the time when the batch job was completed
	CompletedAt time.Time `json:"completed_at,omitempty"`
	// Progress is the progress of the batch job as a percentage
	Progress float64 `json:"progress"`
	// Database is the database for the batch job
	Database string `json:"database"`
	// Table is the table for the batch job
	Table string `json:"table"`
	// DataSource is the data source for the batch job
	DataSource string `json:"data_source"`
	// DataFormat is the data format for the batch job
	DataFormat enum.DataFormat `json:"data_format"`
	// TotalItems is the total number of items in the batch job
	TotalItems int64 `json:"total_items"`
	// ProcessedItems is the number of items processed
	ProcessedItems int64 `json:"processed_items"`
	// FailedItems is the number of items that failed to process
	FailedItems int64 `json:"failed_items"`
	// BatchSize is the size of each batch
	BatchSize int `json:"batch_size"`
	// Strategy is the batch strategy
	Strategy enum.BatchStrategy `json:"strategy"`
	// Options are additional options for the batch job
	Options map[string]interface{} `json:"options,omitempty"`
	// Error is the error message if the batch job failed
	Error string `json:"error,omitempty"`
}

// BatchResult represents the result of a batch processing job.
type BatchResult struct {
	// JobID is the ID of the batch job
	JobID string `json:"job_id"`
	// Status is the status of the batch job
	Status string `json:"status"`
	// CompletedAt is the time when the batch job was completed
	CompletedAt time.Time `json:"completed_at"`
	// TotalItems is the total number of items in the batch job
	TotalItems int64 `json:"total_items"`
	// ProcessedItems is the number of items processed
	ProcessedItems int64 `json:"processed_items"`
	// FailedItems is the number of items that failed to process
	FailedItems int64 `json:"failed_items"`
	// Duration is the duration of the batch job in milliseconds
	Duration int64 `json:"duration"`
	// BytesProcessed is the number of bytes processed
	BytesProcessed int64 `json:"bytes_processed"`
	// Error is the error message if the batch job failed
	Error string `json:"error,omitempty"`
	// Warnings are warnings from the batch job
	Warnings []string `json:"warnings,omitempty"`
	// Details contains detailed results
	Details map[string]interface{} `json:"details,omitempty"`
}

// BatchStatusRequest represents a request for batch job status.
type BatchStatusRequest struct {
	// JobID is the ID of the batch job
	JobID string `json:"job_id"`
}

// BatchStatusResponse represents a response with batch job status.
type BatchStatusResponse struct {
	BaseResponse
	// Data contains the batch job status
	Data BatchJob `json:"data"`
}

// Monitoring Metrics DTOs
// -------------------------------

// MetricData represents a single metric data point.
type MetricData struct {
	// Name is the name of the metric
	Name string `json:"name"`
	// Value is the value of the metric
	Value float64 `json:"value"`
	// Timestamp is the time when the metric was collected
	Timestamp time.Time `json:"timestamp"`
	// Labels are the labels for the metric
	Labels map[string]string `json:"labels,omitempty"`
}

// MetricsRequest represents a request for metrics.
type MetricsRequest struct {
	// Metrics is the list of metrics to retrieve
	Metrics []string `json:"metrics,omitempty"`
	// From is the start time for the metrics
	From time.Time `json:"from,omitempty"`
	// To is the end time for the metrics
	To time.Time `json:"to,omitempty"`
	// Resolution is the time resolution for the metrics in seconds
	Resolution int `json:"resolution,omitempty"`
	// Labels are the labels to filter the metrics by
	Labels map[string]string `json:"labels,omitempty"`
}

// MetricsResponse represents a response with metrics.
type MetricsResponse struct {
	BaseResponse
	// Data contains the metrics
	Data struct {
		// Metrics is the list of metrics
		Metrics []MetricSeries `json:"metrics"`
	} `json:"data"`
}

// MetricSeries represents a series of metric data points.
type MetricSeries struct {
	// Name is the name of the metric
	Name string `json:"name"`
	// Labels are the labels for the metric series
	Labels map[string]string `json:"labels,omitempty"`
	// Datapoints is the list of data points in the series
	Datapoints []MetricDatapoint `json:"datapoints"`
}

// MetricDatapoint represents a single data point in a metric series.
type MetricDatapoint struct {
	// Timestamp is the time of the data point
	Timestamp time.Time `json:"timestamp"`
	// Value is the value of the data point
	Value float64 `json:"value"`
}

// HealthReport represents a comprehensive health report.
type HealthReport struct {
	// Timestamp is the time of the report
	Timestamp time.Time `json:"timestamp"`
	// Status is the overall status of the system
	Status enum.ServiceStatus `json:"status"`
	// ServiceUptime is the uptime of the service in seconds
	ServiceUptime int64 `json:"service_uptime"`
	// ClusterName is the name of the cluster
	ClusterName string `json:"cluster_name"`
	// StarRocksVersion is the version of StarRocks
	StarRocksVersion string `json:"starrocks_version"`
	// BackendHealth is the health of the backends
	BackendHealth BackendHealthReport `json:"backend_health"`
	// TabletHealth is the health of the tablets
	TabletHealth TabletHealthReport `json:"tablet_health"`
	// QueryStats contains query statistics
	QueryStats QueryStatsReport `json:"query_stats"`
	// ResourceUsage contains resource usage information
	ResourceUsage ResourceUsageReport `json:"resource_usage"`
	// Alerts are alerts triggered by the health check
	Alerts []Alert `json:"alerts,omitempty"`
}

// BackendHealthReport represents the health of the backends.
type BackendHealthReport struct {
	// TotalBackends is the total number of backends
	TotalBackends int `json:"total_backends"`
	// HealthyBackends is the number of healthy backends
	HealthyBackends int `json:"healthy_backends"`
	// UnhealthyBackends is the list of unhealthy backends
	UnhealthyBackends []BackendHealthItem `json:"unhealthy_backends,omitempty"`
	// HealthScore is a score representing the overall health (0-100)
	HealthScore float64 `json:"health_score"`
}

// BackendHealthItem represents the health of a single backend.
type BackendHealthItem struct {
	// BackendID is the ID of the backend
	BackendID int64 `json:"backend_id"`
	// Host is the hostname or IP address of the backend
	Host string `json:"host"`
	// Status is the status of the backend
	Status enum.BackendStatus `json:"status"`
	// LastHeartbeat is the time of the last heartbeat
	LastHeartbeat time.Time `json:"last_heartbeat"`
	// Reason is the reason for the unhealthy status
	Reason string `json:"reason,omitempty"`
}

// TabletHealthReport represents the health of the tablets.
type TabletHealthReport struct {
	// TotalTablets is the total number of tablets
	TotalTablets int `json:"total_tablets"`
	// HealthyTablets is the number of healthy tablets
	HealthyTablets int `json:"healthy_tablets"`
	// UnhealthyTablets is the list of unhealthy tablets
	UnhealthyTablets []TabletHealthItem `json:"unhealthy_tablets,omitempty"`
	// HealthScore is a score representing the overall health (0-100)
	HealthScore float64 `json:"health_score"`
}

// TabletHealthItem represents the health of a single tablet.
type TabletHealthItem struct {
	// TabletID is the ID of the tablet
	TabletID int64 `json:"tablet_id"`
	// TableID is the ID of the table
	TableID int64 `json:"table_id"`
	// PartitionID is the ID of the partition
	PartitionID int64 `json:"partition_id"`
	// Status is the status of the tablet
	Status enum.TabletStatus `json:"status"`
	// Reason is the reason for the unhealthy status
	Reason string `json:"reason,omitempty"`
}

// QueryStatsReport represents query statistics.
type QueryStatsReport struct {
	// QPS is the queries per second
	QPS float64 `json:"qps"`
	// AverageLatency is the average query latency in milliseconds
	AverageLatency float64 `json:"average_latency"`
	// P95Latency is the 95th percentile query latency in milliseconds
	P95Latency float64 `json:"p95_latency"`
	// P99Latency is the 99th percentile query latency in milliseconds
	P99Latency float64 `json:"p99_latency"`
	// ErrorRate is the error rate as a percentage
	ErrorRate float64 `json:"error_rate"`
	// TotalQueries is the total number of queries
	TotalQueries int64 `json:"total_queries"`
	// SlowQueries is the number of slow queries
	SlowQueries int64 `json:"slow_queries"`
	// FailedQueries is the number of failed queries
	FailedQueries int64 `json:"failed_queries"`
}

// ResourceUsageReport represents resource usage information.
type ResourceUsageReport struct {
	// CPUUsage is the CPU usage as a percentage
	CPUUsage float64 `json:"cpu_usage"`
	// MemoryUsage is the memory usage as a percentage
	MemoryUsage float64 `json:"memory_usage"`
	// DiskUsage is the disk usage as a percentage
	DiskUsage float64 `json:"disk_usage"`
	// NetworkInbound is the inbound network traffic in bytes per second
	NetworkInbound float64 `json:"network_inbound"`
	// NetworkOutbound is the outbound network traffic in bytes per second
	NetworkOutbound float64 `json:"network_outbound"`
}

// Alert represents an alert triggered by the health check.
type Alert struct {
	// ID is the unique identifier for the alert
	ID string `json:"id"`
	// Severity is the severity of the alert
	Severity string `json:"severity"`
	// Component is the component that triggered the alert
	Component string `json:"component"`
	// Message is the alert message
	Message string `json:"message"`
	// Timestamp is the time when the alert was triggered
	Timestamp time.Time `json:"timestamp"`
	// Details contains additional details about the alert
	Details map[string]interface{} `json:"details,omitempty"`
}

// Configuration DTOs
// -------------------------------

// ProxyConfig represents the configuration for the proxy.
type ProxyConfig struct {
	// Server contains server configuration
	Server ServerConfig `json:"server"`
	// StarRocks contains StarRocks configuration
	StarRocks StarRocksConfig `json:"starrocks"`
	// Monitoring contains monitoring configuration
	Monitoring MonitorConfig `json:"monitoring"`
	// Logging contains logging configuration
	Logging LoggingConfig `json:"logging"`
	// Batch contains batch processing configuration
	Batch BatchConfig `json:"batch"`
}

// ServerConfig represents server configuration.
type ServerConfig struct {
	// Host is the host to bind to
	Host string `json:"host"`
	// Port is the port to bind to
	Port int `json:"port"`
	// AdminPort is the port for the admin interface
	AdminPort int `json:"admin_port"`
	// MetricsPort is the port for the metrics endpoint
	MetricsPort int `json:"metrics_port"`
	// MaxConcurrentRequests is the maximum number of concurrent requests
	MaxConcurrentRequests int `json:"max_concurrent_requests"`
	// RequestTimeout is the default timeout for requests in milliseconds
	RequestTimeout int `json:"request_timeout"`
	// ShutdownTimeout is the timeout for graceful shutdown in milliseconds
	ShutdownTimeout int `json:"shutdown_timeout"`
	// EnableTLS indicates whether to enable TLS
	EnableTLS bool `json:"enable_tls"`
	// TLSCertFile is the path to the TLS certificate file
	TLSCertFile string `json:"tls_cert_file,omitempty"`
	// TLSKeyFile is the path to the TLS key file
	TLSKeyFile string `json:"tls_key_file,omitempty"`
}

// StarRocksConfig represents StarRocks configuration.
type StarRocksConfig struct {
	// FrontendAddresses is the list of frontend addresses
	FrontendAddresses []string `json:"frontend_addresses"`
	// FrontendUser is the username for the frontend
	FrontendUser string `json:"frontend_user"`
	// FrontendPassword is the password for the frontend
	FrontendPassword string `json:"frontend_password,omitempty"`
	// QueryTimeoutMs is the timeout for queries in milliseconds
	QueryTimeoutMs int `json:"query_timeout_ms"`
	// ConnectionPoolSize is the size of the connection pool
	ConnectionPoolSize int `json:"connection_pool_size"`
	// HeartbeatIntervalMs is the interval for heartbeats in milliseconds
	HeartbeatIntervalMs int `json:"heartbeat_interval_ms"`
	// RetryCount is the number of retries for failed operations
	RetryCount int `json:"retry_count"`
	// RetryIntervalMs is the interval between retries in milliseconds
	RetryIntervalMs int `json:"retry_interval_ms"`
}

// MonitorConfig represents monitoring configuration.
type MonitorConfig struct {
	// EnableMetrics indicates whether to enable metrics collection
	EnableMetrics bool `json:"enable_metrics"`
	// CollectionIntervalMs is the interval for metrics collection in milliseconds
	CollectionIntervalMs int `json:"collection_interval_ms"`
	// HealthCheckIntervalMs is the interval for health checks in milliseconds
	HealthCheckIntervalMs int `json:"health_check_interval_ms"`
	// EnableTracing indicates whether to enable distributed tracing
	EnableTracing bool `json:"enable_tracing"`
	// TracingSampleRate is the sampling rate for tracing
	TracingSampleRate float64 `json:"tracing_sample_rate"`
	// MetricsExportEndpoints is the list of endpoints to export metrics to
	MetricsExportEndpoints []string `json:"metrics_export_endpoints,omitempty"`
	// AlertThresholds contains thresholds for alerts
	AlertThresholds AlertThresholds `json:"alert_thresholds"`
}

// AlertThresholds represents thresholds for alerts.
type AlertThresholds struct {
	// CPUUsagePercent is the threshold for CPU usage alerts
	CPUUsagePercent float64 `json:"cpu_usage_percent"`
	// MemoryUsagePercent is the threshold for memory usage alerts
	MemoryUsagePercent float64 `json:"memory_usage_percent"`
	// DiskUsagePercent is the threshold for disk usage alerts
	DiskUsagePercent float64 `json:"disk_usage_percent"`
	// ErrorRatePercent is the threshold for error rate alerts
	ErrorRatePercent float64 `json:"error_rate_percent"`
	// SlowQueryLatencyMs is the threshold for slow query alerts
	SlowQueryLatencyMs int `json:"slow_query_latency_ms"`
	// UnhealthyBackendPercent is the threshold for unhealthy backend alerts
	UnhealthyBackendPercent float64 `json:"unhealthy_backend_percent"`
}

// LoggingConfig represents logging configuration.
type LoggingConfig struct {
	// Level is the logging level
	Level string `json:"level"`
	// Format is the logging format
	Format string `json:"format"`
	// Output is the logging output destination
	Output string `json:"output"`
	// EnableConsole indicates whether to log to console
	EnableConsole bool `json:"enable_console"`
	// FileLocation is the location of the log file
	FileLocation string `json:"file_location,omitempty"`
	// MaxSize is the maximum size of the log file before rotation
	MaxSize int `json:"max_size"`
	// MaxBackups is the maximum number of old log files to keep
	MaxBackups int `json:"max_backups"`
	// MaxAge is the maximum number of days to keep old log files
	MaxAge int `json:"max_age"`
}

// BatchConfig represents batch processing configuration.
type BatchConfig struct {
	// DefaultBatchSize is the default size of a batch
	DefaultBatchSize int `json:"default_batch_size"`
	// DefaultBatchIntervalMs is the default interval for batch processing in milliseconds
	DefaultBatchIntervalMs int `json:"default_batch_interval_ms"`
	// MaxBatchSize is the maximum size of a batch
	MaxBatchSize int `json:"max_batch_size"`
	// MinBatchSize is the minimum size of a batch
	MinBatchSize int `json:"min_batch_size"`
	// MaxBatchDelayMs is the maximum delay for batch processing in milliseconds
	MaxBatchDelayMs int `json:"max_batch_delay_ms"`
	// EnableAdaptiveBatching indicates whether to enable adaptive batching
	EnableAdaptiveBatching bool `json:"enable_adaptive_batching"`
	// WorkerPoolSize is the size of the worker pool for batch processing
	WorkerPoolSize int `json:"worker_pool_size"`
	// MaxConcurrentBatches is the maximum number of concurrent batches
	MaxConcurrentBatches int `json:"max_concurrent_batches"`
}

// Conversion Functions
// -------------------------------

// ConvertBackendDTOToModel converts a BackendDTO to a Backend model.
func ConvertBackendDTOToModel(dto BackendDTO) *model.Backend {
	status := enum.BackendStatusUnknown
	if dto.Alive {
		status = enum.BackendStatusHealthy
	} else {
		status = enum.BackendStatusUnhealthy
	}

	return &model.Backend{
		ID:            dto.ID,
		Host:          dto.Host,
		HeartbeatPort: dto.HeartbeatPort,
		BePort:        dto.BePort,
		HttpPort:      dto.HttpPort,
		BrpcPort:      dto.BrpcPort,
		Status:        status,
		LastHeartbeat: time.Unix(dto.LastHeartbeat/1000, 0),
		StartTime:     time.Unix(dto.LastStartTime/1000, 0),
		Alive:         dto.Alive,
		DiskCapacity:  dto.DiskCapacity,
		DiskAvailable: dto.DiskAvailable,
		Tags:          make(map[string]string),
	}
}

// ConvertTabletDTOToModel converts a TabletDTO to a Tablet model.
func ConvertTabletDTOToModel(dto TabletDTO) *model.Tablet {
	status := enum.TabletStatusUnknown
	if dto.Status == "NORMAL" {
		status = enum.TabletStatusAvailable
	} else {
		status = enum.TabletStatusUnavailable
	}

	var leaderBackendID int64
	if len(dto.BackendIDs) > 0 {
		leaderBackendID = dto.BackendIDs[0]
	}

	return &model.Tablet{
		ID:              dto.ID,
		TableID:         dto.TableID,
		PartitionID:     dto.PartitionID,
		BackendIDs:      dto.BackendIDs,
		LeaderBackendID: leaderBackendID,
		Status:          status,
		DataSize:        dto.DataSize,
		RowCount:        dto.RowCount,
		LastUpdateTime:  time.Unix(dto.LastCheckTime/1000, 0),
		Version:         dto.Version,
		Path:            dto.Path,
		SchemaHash:      dto.SchemaHash,
	}
}

// ConvertDatabaseDTOToModel converts a DatabaseDTO to a Database model.
func ConvertDatabaseDTOToModel(dto DatabaseDTO) *model.Database {
	return &model.Database{
		ID:             dto.ID,
		Name:           dto.Name,
		Tables:         make([]*model.Table, 0),
		CreateTime:     time.Unix(dto.CreatedTime/1000, 0),
		LastModifyTime: time.Unix(dto.CreatedTime/1000, 0), // Use creation time as last modify time if not available
	}
}

// ConvertTableDTOToModel converts a TableDTO to a Table model.
func ConvertTableDTOToModel(dto TableDTO) *model.Table {
	return &model.Table{
		ID:             dto.ID,
		Name:           dto.Name,
		DatabaseID:     dto.DatabaseID,
		DatabaseName:   "", // This will need to be filled in separately
		Partitions:     make([]*model.Partition, 0),
		Columns:        make([]*model.Column, 0),
		CreateTime:     time.Unix(dto.CreateTime/1000, 0),
		LastModifyTime: time.Unix(dto.LastUpdateTime/1000, 0),
		Engine:         dto.Type,
		RowCount:       dto.RowCount,
		DataSize:       dto.DataSize,
		ReplicationNum: dto.ReplicaCount,
		State:          dto.State,
	}
}

// ConvertPartitionDTOToModel converts a PartitionDTO to a Partition model.
func ConvertPartitionDTOToModel(dto PartitionDTO) *model.Partition {
	return &model.Partition{
		ID:              dto.ID,
		TableID:         dto.TableID,
		Name:            dto.Name,
		PartitionKey:    dto.PartitionKey,
		PartitionValues: dto.PartitionValues,
		Tablets:         make([]*model.Tablet, 0),
		DataSize:        dto.DataSize,
		RowCount:        dto.RowCount,
		CreateTime:      time.Unix(dto.CreateTime/1000, 0),
		LastModifyTime:  time.Unix(dto.CreateTime/1000, 0), // Use creation time as last modify time if not available
		VisibleVersion:  dto.VisibleVersion,
		State:           dto.State,
	}
}

// CreateHealthReport creates a HealthReport from a HealthState model.
func CreateHealthReport(state *model.HealthState) *HealthReport {
	report := &HealthReport{
		Timestamp: time.Now(),
		Status:    state.Status,
		BackendHealth: BackendHealthReport{
			TotalBackends:     state.TotalBackends,
			HealthyBackends:   state.HealthyBackends,
			UnhealthyBackends: make([]BackendHealthItem, 0, len(state.UnhealthyBackends)),
		},
		TabletHealth: TabletHealthReport{
			TotalTablets:     state.TotalTablets,
			HealthyTablets:   state.AvailableTablets,
			UnhealthyTablets: make([]TabletHealthItem, 0, len(state.UnavailableTablets)),
		},
		Alerts: make([]Alert, 0),
	}

	// Convert unhealthy backends
	for _, be := range state.UnhealthyBackends {
		report.BackendHealth.UnhealthyBackends = append(report.BackendHealth.UnhealthyBackends, BackendHealthItem{
			BackendID:     be.ID,
			Host:          be.Host,
			Status:        be.Status,
			LastHeartbeat: be.LastHeartbeat,
		})
	}

	// Convert unavailable tablets
	for _, tablet := range state.UnavailableTablets {
		report.TabletHealth.UnhealthyTablets = append(report.TabletHealth.UnhealthyTablets, TabletHealthItem{
			TabletID:    tablet.ID,
			TableID:     tablet.TableID,
			PartitionID: tablet.PartitionID,
			Status:      tablet.Status,
		})
	}

	// Add health scores
	if state.TotalBackends > 0 {
		report.BackendHealth.HealthScore = float64(state.HealthyBackends) / float64(state.TotalBackends) * 100
	}
	if state.TotalTablets > 0 {
		report.TabletHealth.HealthScore = float64(state.AvailableTablets) / float64(state.TotalTablets) * 100
	}

	return report
}

// ConvertModelToQueryResponse converts query results to a QueryResponse.
func ConvertModelToQueryResponse(columns []model.Column, rows [][]interface{}, executionTime time.Duration) *QueryResponse {
	response := &QueryResponse{}
	response.Code = 0
	response.Message = "Success"
	response.RequestID = "" // Would be set by middleware
	response.Timestamp = time.Now().UnixNano() / 1e6

	// Convert columns to ColumnMetadata
	colMetas := make([]ColumnMetadata, 0, len(columns))
	for _, col := range columns {
		colMetas = append(colMetas, ColumnMetadata{
			Name:     col.Name,
			Type:     col.Type,
			Nullable: col.Nullable,
		})
	}

	response.Data.Columns = colMetas
	response.Data.Rows = rows
	response.Data.RowCount = len(rows)
	response.Data.ExecutionTime = int64(executionTime / time.Millisecond)

	return response
}

//Personal.AI order the ending
