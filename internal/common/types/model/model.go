// Package model defines core data models used across the project.
package model

import (
	"time"

	"github.com/turtacn/staravail/internal/common/types/enum"
)

// Tablet represents a StarRocks tablet.
type Tablet struct {
	// ID is the unique identifier of the tablet
	ID int64 `json:"id"`
	// TableID is the ID of the table this tablet belongs to
	TableID int64 `json:"table_id"`
	// PartitionID is the ID of the partition this tablet belongs to
	PartitionID int64 `json:"partition_id"`
	// BucketID is the ID of the bucket this tablet belongs to
	BucketID int `json:"bucket_id"`
	// BackendIDs is the list of backend IDs hosting this tablet
	BackendIDs []int64 `json:"backend_ids"`
	// LeaderBackendID is the ID of the backend that is the leader for this tablet
	LeaderBackendID int64 `json:"leader_backend_id"`
	// Status represents the current status of the tablet
	Status enum.TabletStatus `json:"status"`
	// DataSize is the size of data in this tablet in bytes
	DataSize int64 `json:"data_size"`
	// RowCount is the approximate number of rows in this tablet
	RowCount int64 `json:"row_count"`
	// LastUpdateTime is the time when this tablet's information was last updated
	LastUpdateTime time.Time `json:"last_update_time"`
	// Version is the version number of this tablet
	Version int64 `json:"version"`
	// Path is the storage path of this tablet
	Path string `json:"path"`
	// SchemaHash is the hash of the schema for this tablet
	SchemaHash int `json:"schema_hash"`
}

// Backend represents a StarRocks BE node.
type Backend struct {
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
	// Status represents the current status of the backend
	Status enum.BackendStatus `json:"status"`
	// LastHeartbeat is the time of the last heartbeat received from this backend
	LastHeartbeat time.Time `json:"last_heartbeat"`
	// StartTime is the time when this backend started
	StartTime time.Time `json:"start_time"`
	// Alive indicates whether the backend is alive
	Alive bool `json:"alive"`
	// DiskCapacity is the total disk capacity of this backend in bytes
	DiskCapacity int64 `json:"disk_capacity"`
	// DiskAvailable is the available disk space of this backend in bytes
	DiskAvailable int64 `json:"disk_available"`
	// Tags are the labels assigned to this backend
	Tags map[string]string `json:"tags"`
	// TabletCount is the number of tablets hosted on this backend
	TabletCount int `json:"tablet_count"`
	// LoadScore is the current load score of this backend
	LoadScore float64 `json:"load_score"`
	// SystemLoad is the system load average of this backend
	SystemLoad float64 `json:"system_load"`
}

// Partition represents a partition in a StarRocks table.
type Partition struct {
	// ID is the unique identifier of the partition
	ID int64 `json:"id"`
	// TableID is the ID of the table this partition belongs to
	TableID int64 `json:"table_id"`
	// Name is the name of the partition
	Name string `json:"name"`
	// PartitionKey is the key used for partitioning
	PartitionKey string `json:"partition_key"`
	// PartitionValues are the values defining this partition
	PartitionValues []string `json:"partition_values"`
	// BucketCount is the number of buckets in this partition
	BucketCount int `json:"bucket_count"`
	// Tablets is the list of tablets in this partition
	Tablets []*Tablet `json:"tablets"`
	// DataSize is the size of data in this partition in bytes
	DataSize int64 `json:"data_size"`
	// RowCount is the approximate number of rows in this partition
	RowCount int64 `json:"row_count"`
	// CreateTime is the time when this partition was created
	CreateTime time.Time `json:"create_time"`
	// LastModifyTime is the time when this partition was last modified
	LastModifyTime time.Time `json:"last_modify_time"`
	// VisibleVersion is the visible version of this partition
	VisibleVersion int64 `json:"visible_version"`
	// State indicates whether this partition is visible or under schema change
	State string `json:"state"`
}

// Table represents a StarRocks table.
type Table struct {
	// ID is the unique identifier of the table
	ID int64 `json:"id"`
	// Name is the name of the table
	Name string `json:"name"`
	// DatabaseID is the ID of the database this table belongs to
	DatabaseID int64 `json:"database_id"`
	// DatabaseName is the name of the database this table belongs to
	DatabaseName string `json:"database_name"`
	// PartitionKeyColumns are the columns used for partitioning
	PartitionKeyColumns []string `json:"partition_key_columns"`
	// DistributionKeyColumns are the columns used for distribution (bucketing)
	DistributionKeyColumns []string `json:"distribution_key_columns"`
	// Partitions is the list of partitions in this table
	Partitions []*Partition `json:"partitions"`
	// Columns is the list of columns in this table
	Columns []*Column `json:"columns"`
	// CreateTime is the time when this table was created
	CreateTime time.Time `json:"create_time"`
	// LastModifyTime is the time when this table was last modified
	LastModifyTime time.Time `json:"last_modify_time"`
	// Engine is the storage engine of this table
	Engine string `json:"engine"`
	// RowCount is the approximate number of rows in this table
	RowCount int64 `json:"row_count"`
	// DataSize is the size of data in this table in bytes
	DataSize int64 `json:"data_size"`
	// ReplicationNum is the replication factor for this table
	ReplicationNum int `json:"replication_num"`
	// State indicates whether this table is visible, hidden, or in another state
	State string `json:"state"`
	// Comment is the comment for this table
	Comment string `json:"comment"`
}

// Column represents a column in a StarRocks table.
type Column struct {
	// Name is the name of the column
	Name string `json:"name"`
	// Type is the data type of the column
	Type string `json:"type"`
	// Nullable indicates whether the column can contain NULL values
	Nullable bool `json:"nullable"`
	// DefaultValue is the default value for the column
	DefaultValue interface{} `json:"default_value"`
	// Position is the position of the column in the table
	Position int `json:"position"`
	// Comment is the comment for this column
	Comment string `json:"comment"`
	// IsKey indicates whether this column is part of the primary key
	IsKey bool `json:"is_key"`
	// AggregateFunctionName is the name of the aggregate function for this column (if applicable)
	AggregateFunctionName string `json:"aggregate_function_name,omitempty"`
	// IsVisible indicates whether this column is visible
	IsVisible bool `json:"is_visible"`
}

// Database represents a StarRocks database.
type Database struct {
	// ID is the unique identifier of the database
	ID int64 `json:"id"`
	// Name is the name of the database
	Name string `json:"name"`
	// Tables is the list of tables in this database
	Tables []*Table `json:"tables"`
	// CreateTime is the time when this database was created
	CreateTime time.Time `json:"create_time"`
	// LastModifyTime is the time when this database was last modified
	LastModifyTime time.Time `json:"last_modify_time"`
}

// StarrocksCluster represents a StarRocks cluster.
type StarrocksCluster struct {
	// Name is the name of the cluster
	Name string `json:"name"`
	// FrontendNodes is the list of FE nodes in this cluster
	FrontendNodes []*FrontendNode `json:"frontend_nodes"`
	// BackendNodes is the list of BE nodes in this cluster
	BackendNodes []*Backend `json:"backend_nodes"`
	// Databases is the list of databases in this cluster
	Databases []*Database `json:"databases"`
	// Status represents the current status of the cluster
	Status enum.ServiceStatus `json:"status"`
	// Version is the StarRocks version of this cluster
	Version string `json:"version"`
	// LastUpdateTime is the time when this cluster's information was last updated
	LastUpdateTime time.Time `json:"last_update_time"`
	// Config is the configuration of this cluster
	Config map[string]string `json:"config"`
}

// FrontendNode represents a StarRocks FE node.
type FrontendNode struct {
	// ID is the unique identifier of the frontend node
	ID int64 `json:"id"`
	// Host is the hostname or IP address of the frontend node
	Host string `json:"host"`
	// EditLogPort is the port used for edit log communication
	EditLogPort int `json:"edit_log_port"`
	// HttpPort is the port used for HTTP service
	HttpPort int `json:"http_port"`
	// RpcPort is the port used for RPC service
	RpcPort int `json:"rpc_port"`
	// Role is the role of this frontend node (Leader, Follower, Observer)
	Role string `json:"role"`
	// Status represents the current status of the frontend node
	Status enum.ServiceStatus `json:"status"`
	// LastHeartbeat is the time of the last heartbeat received from this frontend node
	LastHeartbeat time.Time `json:"last_heartbeat"`
	// StartTime is the time when this frontend node started
	StartTime time.Time `json:"start_time"`
	// Alive indicates whether the frontend node is alive
	Alive bool `json:"alive"`
	// JoinTime is the time when this frontend node joined the cluster
	JoinTime time.Time `json:"join_time"`
}

// QueryContext represents the context for a query.
type QueryContext struct {
	// OriginalSQL is the original SQL query
	OriginalSQL string `json:"original_sql"`
	// DatabaseName is the name of the database being queried
	DatabaseName string `json:"database_name"`
	// TableNames are the names of the tables being queried
	TableNames []string `json:"table_names"`
	// Tables are the table objects being queried
	Tables []*Table `json:"tables"`
	// Partitions are the partitions involved in this query
	Partitions []*Partition `json:"partitions"`
	// TargetBackends are the backends targeted for this query
	TargetBackends []*Backend `json:"target_backends"`
	// HandlingStrategy is the strategy for handling this query
	HandlingStrategy enum.QueryHandlingStrategy `json:"handling_strategy"`
	// StartTime is the time when this query started
	StartTime time.Time `json:"start_time"`
	// ExecutionTimeout is the timeout for execution of this query
	ExecutionTimeout time.Duration `json:"execution_timeout"`
	// Parameters are the parameters for this query
	Parameters map[string]interface{} `json:"parameters"`
	// RequiredColumns are the columns required from this query
	RequiredColumns []string `json:"required_columns"`
	// Filters are the filters for this query
	Filters map[string]interface{} `json:"filters"`
	// PlanningMetadata is metadata about the query planning
	PlanningMetadata map[string]interface{} `json:"planning_metadata"`
	// ExecutionMetadata is metadata about the query execution
	ExecutionMetadata map[string]interface{} `json:"execution_metadata"`
}

// WriteContext represents the context for a write operation.
type WriteContext struct {
	// Data is the data to be written
	Data interface{} `json:"data"`
	// DataFormat is the format of the data
	DataFormat enum.DataFormat `json:"data_format"`
	// DatabaseName is the name of the database to write to
	DatabaseName string `json:"database_name"`
	// TableName is the name of the table to write to
	TableName string `json:"table_name"`
	// Table is the table object to write to
	Table *Table `json:"table"`
	// TargetPartition is the partition to write to (if specified)
	TargetPartition *Partition `json:"target_partition"`
	// PartitionValues are the values used for partitioning
	PartitionValues map[string]interface{} `json:"partition_values"`
	// HandlingStrategy is the strategy for handling this write
	HandlingStrategy enum.WriteHandlingStrategy `json:"handling_strategy"`
	// StartTime is the time when this write operation started
	StartTime time.Time `json:"start_time"`
	// ExecutionTimeout is the timeout for execution of this write operation
	ExecutionTimeout time.Duration `json:"execution_timeout"`
	// BatchID is the ID of the batch this write is part of
	BatchID string `json:"batch_id"`
	// Schema is the schema of the data being written
	Schema map[string]string `json:"schema"`
	// ColumnMapping is the mapping between source and target columns
	ColumnMapping map[string]string `json:"column_mapping"`
	// StreamName is the name of the stream this write is part of (if applicable)
	StreamName string `json:"stream_name"`
	// Labels are the labels for this write operation
	Labels map[string]string `json:"labels"`
}

// HealthState represents the health state of the system.
type HealthState struct {
	// Status represents the overall status of the system
	Status enum.ServiceStatus `json:"status"`
	// UnhealthyBackends is the list of unhealthy backend nodes
	UnhealthyBackends []*Backend `json:"unhealthy_backends"`
	// UnavailableTablets is the list of unavailable tablets
	UnavailableTablets []*Tablet `json:"unavailable_tablets"`
	// LastCheckTime is the time of the last health check
	LastCheckTime time.Time `json:"last_check_time"`
	// TotalBackends is the total number of backend nodes
	TotalBackends int `json:"total_backends"`
	// HealthyBackends is the number of healthy backend nodes
	HealthyBackends int `json:"healthy_backends"`
	// TotalTablets is the total number of tablets
	TotalTablets int `json:"total_tablets"`
	// AvailableTablets is the number of available tablets
	AvailableTablets int `json:"available_tablets"`
	// HealthScore is a score representing the overall health (0-100)
	HealthScore float64 `json:"health_score"`
	// Warnings are warnings about the system health
	Warnings []string `json:"warnings"`
	// Issues are detailed issues affecting system health
	Issues map[string]string `json:"issues"`
	// FEStatus is the status of the FE nodes
	FEStatus enum.ServiceStatus `json:"fe_status"`
	// UnhealthyFrontends is the list of unhealthy frontend nodes
	UnhealthyFrontends []*FrontendNode `json:"unhealthy_frontends"`
}

// Metrics represents a collection of system metrics.
type Metrics struct {
	// Timestamp is the time when these metrics were collected
	Timestamp time.Time `json:"timestamp"`
	// QPS is the queries per second
	QPS float64 `json:"qps"`
	// WriteQPS is the write queries per second
	WriteQPS float64 `json:"write_qps"`
	// ReadQPS is the read queries per second
	ReadQPS float64 `json:"read_qps"`
	// AverageQueryLatency is the average query latency in milliseconds
	AverageQueryLatency float64 `json:"average_query_latency"`
	// P95QueryLatency is the 95th percentile query latency in milliseconds
	P95QueryLatency float64 `json:"p95_query_latency"`
	// P99QueryLatency is the 99th percentile query latency in milliseconds
	P99QueryLatency float64 `json:"p99_query_latency"`
	// ErrorRate is the error rate as a percentage
	ErrorRate float64 `json:"error_rate"`
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
	// BackendMetrics is a map of backend ID to backend-specific metrics
	BackendMetrics map[int64]map[string]float64 `json:"backend_metrics"`
	// QueryCount is the total number of queries since startup
	QueryCount int64 `json:"query_count"`
	// WriteCount is the total number of writes since startup
	WriteCount int64 `json:"write_count"`
	// ErrorCount is the total number of errors since startup
	ErrorCount int64 `json:"error_count"`
}

// QueryStats represents statistics for a query.
type QueryStats struct {
	// QueryID is the unique identifier for this query
	QueryID string `json:"query_id"`
	// SQL is the SQL text of the query
	SQL string `json:"sql"`
	// User is the user who executed the query
	User string `json:"user"`
	// Database is the database against which the query was executed
	Database string `json:"database"`
	// StartTime is the time when the query started
	StartTime time.Time `json:"start_time"`
	// EndTime is the time when the query ended
	EndTime time.Time `json:"end_time"`
	// Duration is the duration of the query in milliseconds
	Duration int64 `json:"duration"`
	// Status is the status of the query (success, failed, etc.)
	Status string `json:"status"`
	// ErrorMessage is the error message if the query failed
	ErrorMessage string `json:"error_message"`
	// ScanRows is the number of rows scanned
	ScanRows int64 `json:"scan_rows"`
	// ScanBytes is the number of bytes scanned
	ScanBytes int64 `json:"scan_bytes"`
	// ReturnRows is the number of rows returned
	ReturnRows int64 `json:"return_rows"`
	// CPUTimeMs is the CPU time used in milliseconds
	CPUTimeMs int64 `json:"cpu_time_ms"`
	// MemoryBytes is the memory used in bytes
	MemoryBytes int64 `json:"memory_bytes"`
	// IOWaitTimeMs is the I/O wait time in milliseconds
	IOWaitTimeMs int64 `json:"io_wait_time_ms"`
	// NetworkTimeMs is the network time in milliseconds
	NetworkTimeMs int64 `json:"network_time_ms"`
	// PlanningTimeMs is the planning time in milliseconds
	PlanningTimeMs int64 `json:"planning_time_ms"`
	// ExecutionTimeMs is the execution time in milliseconds
	ExecutionTimeMs int64 `json:"execution_time_ms"`
	// TabletCount is the number of tablets accessed
	TabletCount int `json:"tablet_count"`
	// BackendCount is the number of backends accessed
	BackendCount int `json:"backend_count"`
	// ScanType is the type of scan (full scan, index scan, etc.)
	ScanType string `json:"scan_type"`
}

//Personal.AI order the ending
