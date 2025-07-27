// Package interfaces defines the application layer interfaces for the StarRocks proxy.
// These interfaces serve as contracts between service implementations and their callers,
// establishing a clear boundary for application services.
package interfaces

import (
	"context"
	"io"
	"time"
)

// ===============================
// Service Lifecycle Interfaces
// ===============================

// Lifecycle defines the interface for service lifecycle operations.
// Services implementing this interface can be started, stopped, and have their status checked.
type Lifecycle interface {
	// Start initializes and starts the service.
	// Returns an error if the service fails to start.
	Start(ctx context.Context) error

	// Stop gracefully stops the service.
	// Returns an error if the service fails to stop properly.
	Stop(ctx context.Context) error

	// Status returns the current status of the service.
	Status() ServiceStatus
}

// ServiceStatus represents the current status of a service.
type ServiceStatus string

const (
	// ServiceStatusUninitialized indicates the service has not been initialized.
	ServiceStatusUninitialized ServiceStatus = "UNINITIALIZED"

	// ServiceStatusStarting indicates the service is in the process of starting.
	ServiceStatusStarting ServiceStatus = "STARTING"

	// ServiceStatusRunning indicates the service is running normally.
	ServiceStatusRunning ServiceStatus = "RUNNING"

	// ServiceStatusDegraded indicates the service is running but with degraded functionality.
	ServiceStatusDegraded ServiceStatus = "DEGRADED"

	// ServiceStatusStopping indicates the service is in the process of stopping.
	ServiceStatusStopping ServiceStatus = "STOPPING"

	// ServiceStatusStopped indicates the service has been stopped.
	ServiceStatusStopped ServiceStatus = "STOPPED"

	// ServiceStatusFailed indicates the service has failed and cannot function properly.
	ServiceStatusFailed ServiceStatus = "FAILED"
)

// ===============================
// Service Factory Interfaces
// ===============================

// ServiceFactory defines the interface for creating service instances.
// This factory approach allows for dependency injection and easier testing.
type ServiceFactory interface {
	// CreateQueryService creates a new instance of QueryService.
	CreateQueryService() (QueryService, error)

	// CreateWriteService creates a new instance of WriteService.
	CreateWriteService() (WriteService, error)

	// CreateHealthService creates a new instance of HealthService.
	CreateHealthService() (HealthService, error)

	// CreateMetadataService creates a new instance of MetadataService.
	CreateMetadataService() (MetadataService, error)

	// CreateUserService creates a new instance of UserService.
	CreateUserService() (UserService, error)

	// CreateStreamService creates a new instance of StreamService.
	CreateStreamService() (StreamService, error)

	// CreateCacheService creates a new instance of CacheService.
	CreateCacheService() (CacheService, error)
}

// ===============================
// Service Dependency Interfaces
// ===============================

// ServiceDependencies defines the interface for retrieving service dependencies.
// This interface helps manage and clarify service dependencies.
type ServiceDependencies interface {
	// GetQueryService returns the QueryService dependency.
	GetQueryService() (QueryService, error)

	// GetWriteService returns the WriteService dependency.
	GetWriteService() (WriteService, error)

	// GetHealthService returns the HealthService dependency.
	GetHealthService() (HealthService, error)

	// GetMetadataService returns the MetadataService dependency.
	GetMetadataService() (MetadataService, error)

	// GetUserService returns the UserService dependency.
	GetUserService() (UserService, error)

	// GetStreamService returns the StreamService dependency.
	GetStreamService() (StreamService, error)

	// GetCacheService returns the CacheService dependency.
	GetCacheService() (CacheService, error)
}

// ===============================
// Common Response Interfaces
// ===============================

// Result is a generic interface for service operation results.
// It provides a unified way to handle success and error cases.
type Result interface {
	// IsSuccess returns true if the operation was successful.
	IsSuccess() bool

	// GetError returns the error if the operation failed, or nil if successful.
	GetError() error

	// GetMessage returns a human-readable message about the result.
	GetMessage() string

	// GetTimestamp returns when the result was created.
	GetTimestamp() time.Time
}

// PagedResult extends Result with pagination information.
// It's used for operations that return multiple items with pagination.
type PagedResult interface {
	Result

	// GetItems returns the items in the current page.
	GetItems() interface{}

	// GetTotalCount returns the total number of items across all pages.
	GetTotalCount() int64

	// GetPageSize returns the maximum number of items per page.
	GetPageSize() int

	// GetPageNumber returns the current page number (1-based).
	GetPageNumber() int

	// GetTotalPages returns the total number of pages.
	GetTotalPages() int

	// HasNextPage returns true if there are more pages after the current one.
	HasNextPage() bool

	// HasPreviousPage returns true if there are pages before the current one.
	HasPreviousPage() bool
}

// ===============================
// Query Service Interfaces
// ===============================

// QueryService defines the interface for executing queries against StarRocks.
// This is one of the core services, responsible for processing SELECT statements.
type QueryService interface {
	Lifecycle

	// ExecuteQuery executes a SQL query and returns the result.
	// This method is for queries that return a result set.
	ExecuteQuery(ctx context.Context, request QueryRequest) (QueryResult, error)

	// ExecuteExplain executes an EXPLAIN statement and returns the execution plan.
	ExecuteExplain(ctx context.Context, request ExplainRequest) (ExplainResult, error)

	// GetQueryMetrics returns metrics for a specific query or all recent queries.
	GetQueryMetrics(ctx context.Context, queryID string) (QueryMetrics, error)

	// CancelQuery cancels a running query by its ID.
	CancelQuery(ctx context.Context, queryID string) error

	// GetQueryHistory returns the history of executed queries.
	GetQueryHistory(ctx context.Context, request QueryHistoryRequest) (QueryHistoryResult, error)

	// GetActiveQueries returns information about currently running queries.
	GetActiveQueries(ctx context.Context) ([]QueryInfo, error)

	// AnalyzeQuery analyzes a query without executing it, returning statistics and suggestions.
	AnalyzeQuery(ctx context.Context, query string) (QueryAnalysisResult, error)
}

// QueryRequest represents a request to execute a SQL query.
type QueryRequest struct {
	// SQL is the query to execute.
	SQL string `json:"sql"`

	// Database is the default database for the query.
	Database string `json:"database,omitempty"`

	// Parameters contains parameter values for prepared statements.
	Parameters []interface{} `json:"parameters,omitempty"`

	// Options contains additional execution options.
	Options QueryOptions `json:"options,omitempty"`

	// Timeout is the maximum time to wait for the query to complete.
	Timeout time.Duration `json:"timeout,omitempty"`

	// ClientInfo contains information about the client making the request.
	ClientInfo ClientInfo `json:"client_info,omitempty"`

	// TraceID is an identifier for tracing the query through the system.
	TraceID string `json:"trace_id,omitempty"`
}

// QueryOptions contains options for query execution.
type QueryOptions struct {
	// MaxRows is the maximum number of rows to return (0 means no limit).
	MaxRows int `json:"max_rows,omitempty"`

	// BatchSize is the number of rows to fetch in each batch (0 means use default).
	BatchSize int `json:"batch_size,omitempty"`

	// TimeZone is the timezone to use for the query.
	TimeZone string `json:"time_zone,omitempty"`

	// UseCache indicates whether to use query cache if available.
	UseCache bool `json:"use_cache,omitempty"`

	// Priority sets the execution priority (1-10, where 10 is highest).
	Priority int `json:"priority,omitempty"`

	// ResourceGroup specifies a resource group for the query.
	ResourceGroup string `json:"resource_group,omitempty"`

	// SessionVariables contains session variables to set for this query.
	SessionVariables map[string]string `json:"session_variables,omitempty"`

	// FetchArrowFormat indicates whether to fetch results in Arrow format.
	FetchArrowFormat bool `json:"fetch_arrow_format,omitempty"`
}

// ClientInfo contains information about the client making a request.
type ClientInfo struct {
	// ClientID is a unique identifier for the client.
	ClientID string `json:"client_id,omitempty"`

	// User is the username the client is authenticated as.
	User string `json:"user,omitempty"`

	// Host is the client's hostname or IP address.
	Host string `json:"host,omitempty"`

	// Application is the name of the client application.
	Application string `json:"application,omitempty"`

	// ConnectionID is the ID of the client's connection.
	ConnectionID string `json:"connection_id,omitempty"`
}

// QueryResult represents the result of a query execution.
type QueryResult interface {
	Result

	// GetColumnCount returns the number of columns in the result.
	GetColumnCount() int

	// GetColumns returns metadata about the columns in the result.
	GetColumns() []ColumnInfo

	// GetRowCount returns the number of rows in the result.
	GetRowCount() int64

	// GetRows returns the rows in the result.
	// Note: For large result sets, this may only contain the first batch of rows.
	GetRows() [][]interface{}

	// HasMoreRows returns true if there are more rows to fetch.
	HasMoreRows() bool

	// FetchNextRows fetches the next batch of rows.
	FetchNextRows(ctx context.Context) ([][]interface{}, error)

	// GetQueryID returns the ID of the executed query.
	GetQueryID() string

	// GetExecutionTime returns how long the query took to execute.
	GetExecutionTime() time.Duration

	// GetScanRows returns how many rows were scanned during execution.
	GetScanRows() int64

	// GetScanBytes returns how many bytes were scanned during execution.
	GetScanBytes() int64

	// GetArrowStream returns the result as an Arrow record batch stream, if available.
	GetArrowStream() (io.ReadCloser, error)

	// Close releases any resources held by the result.
	Close() error
}

// ColumnInfo contains metadata about a column in a query result.
type ColumnInfo struct {
	// Name is the name of the column.
	Name string `json:"name"`

	// Type is the data type of the column.
	Type string `json:"type"`

	// Nullable indicates whether the column can contain NULL values.
	Nullable bool `json:"nullable"`

	// Table is the name of the table the column belongs to, if applicable.
	Table string `json:"table,omitempty"`

	// Database is the name of the database the column belongs to, if applicable.
	Database string `json:"database,omitempty"`

	// Precision is the precision for numeric types.
	Precision int `json:"precision,omitempty"`

	// Scale is the scale for decimal types.
	Scale int `json:"scale,omitempty"`

	// Length is the length for string types.
	Length int `json:"length,omitempty"`
}

// ExplainRequest represents a request to explain a SQL query.
type ExplainRequest struct {
	// SQL is the query to explain.
	SQL string `json:"sql"`

	// Database is the default database for the query.
	Database string `json:"database,omitempty"`

	// Format specifies the format of the explain output (e.g., "text", "json", "graph").
	Format string `json:"format,omitempty"`

	// Verbose indicates whether to include additional details.
	Verbose bool `json:"verbose,omitempty"`

	// AnalyzeStatistics indicates whether to include runtime statistics (if available).
	AnalyzeStatistics bool `json:"analyze_statistics,omitempty"`
}

// ExplainResult represents the result of an EXPLAIN statement.
type ExplainResult interface {
	Result

	// GetPlan returns the execution plan as a string or structured data.
	GetPlan() interface{}

	// GetFormat returns the format of the plan (e.g., "text", "json", "graph").
	GetFormat() string

	// GetQueryID returns the ID of the explained query.
	GetQueryID() string

	// GetEstimatedCost returns the estimated cost of the query.
	GetEstimatedCost() float64

	// GetEstimatedRows returns the estimated number of rows the query will process.
	GetEstimatedRows() int64

	// GetEstimatedBytes returns the estimated number of bytes the query will process.
	GetEstimatedBytes() int64

	// GetWarnings returns any warnings generated during plan creation.
	GetWarnings() []string
}

// QueryHistoryRequest represents a request for query history.
type QueryHistoryRequest struct {
	// StartTime is the earliest time to include in the history.
	StartTime time.Time `json:"start_time,omitempty"`

	// EndTime is the latest time to include in the history.
	EndTime time.Time `json:"end_time,omitempty"`

	// User filters the history to a specific user.
	User string `json:"user,omitempty"`

	// Database filters the history to a specific database.
	Database string `json:"database,omitempty"`

	// Status filters the history to queries with a specific status.
	Status string `json:"status,omitempty"`

	// MinDuration filters the history to queries that took at least this long.
	MinDuration time.Duration `json:"min_duration,omitempty"`

	// SQLPattern filters the history to queries matching this pattern.
	SQLPattern string `json:"sql_pattern,omitempty"`

	// PageSize is the maximum number of entries to return.
	PageSize int `json:"page_size,omitempty"`

	// PageNumber is the page number to return (1-based).
	PageNumber int `json:"page_number,omitempty"`

	// SortBy specifies the field to sort by (e.g., "start_time", "duration").
	SortBy string `json:"sort_by,omitempty"`

	// SortOrder specifies the sort order ("asc" or "desc").
	SortOrder string `json:"sort_order,omitempty"`
}

// QueryHistoryResult represents the result of a query history request.
type QueryHistoryResult interface {
	PagedResult

	// GetQueries returns the query history entries in the current page.
	GetQueries() []QueryHistoryEntry
}

// QueryHistoryEntry represents a single entry in the query history.
type QueryHistoryEntry struct {
	// QueryID is the unique identifier for the query.
	QueryID string `json:"query_id"`

	// SQL is the SQL text of the query.
	SQL string `json:"sql"`

	// User is the user who executed the query.
	User string `json:"user"`

	// Database is the database the query was executed against.
	Database string `json:"database"`

	// StartTime is when the query started.
	StartTime time.Time `json:"start_time"`

	// EndTime is when the query finished.
	EndTime time.Time `json:"end_time"`

	// Duration is how long the query took to execute.
	Duration time.Duration `json:"duration"`

	// Status is the final status of the query (e.g., "completed", "failed", "cancelled").
	Status string `json:"status"`

	// Error is the error message if the query failed.
	Error string `json:"error,omitempty"`

	// ScanRows is the number of rows scanned during execution.
	ScanRows int64 `json:"scan_rows"`

	// ScanBytes is the number of bytes scanned during execution.
	ScanBytes int64 `json:"scan_bytes"`

	// ResultRows is the number of rows in the result.
	ResultRows int64 `json:"result_rows"`

	// ClientInfo contains information about the client that executed the query.
	ClientInfo ClientInfo `json:"client_info"`
}

// QueryInfo represents information about a running query.
type QueryInfo struct {
	// QueryID is the unique identifier for the query.
	QueryID string `json:"query_id"`

	// SQL is the SQL text of the query.
	SQL string `json:"sql"`

	// User is the user who is executing the query.
	User string `json:"user"`

	// Database is the database the query is being executed against.
	Database string `json:"database"`

	// StartTime is when the query started.
	StartTime time.Time `json:"start_time"`

	// ElapsedTime is how long the query has been running.
	ElapsedTime time.Duration `json:"elapsed_time"`

	// State is the current state of the query (e.g., "running", "queued").
	State string `json:"state"`

	// Progress is the execution progress as a percentage (0-100).
	Progress float64 `json:"progress"`

	// CurrentStage is the name of the current execution stage.
	CurrentStage string `json:"current_stage,omitempty"`

	// ScanRows is the number of rows scanned so far.
	ScanRows int64 `json:"scan_rows"`

	// ScanBytes is the number of bytes scanned so far.
	ScanBytes int64 `json:"scan_bytes"`

	// ResourceGroup is the resource group the query is using.
	ResourceGroup string `json:"resource_group,omitempty"`

	// ClientInfo contains information about the client executing the query.
	ClientInfo ClientInfo `json:"client_info"`
}

// QueryMetrics contains metrics about a query's execution.
type QueryMetrics struct {
	// QueryID is the unique identifier for the query.
	QueryID string `json:"query_id"`

	// ExecutionTime is how long the query took to execute.
	ExecutionTime time.Duration `json:"execution_time"`

	// CPUTime is how much CPU time the query consumed.
	CPUTime time.Duration `json:"cpu_time"`

	// MemoryUsage is the peak memory usage during query execution.
	MemoryUsage int64 `json:"memory_usage"`

	// ScanRows is the number of rows scanned during execution.
	ScanRows int64 `json:"scan_rows"`

	// ScanBytes is the number of bytes scanned during execution.
	ScanBytes int64 `json:"scan_bytes"`

	// ResultRows is the number of rows in the result.
	ResultRows int64 `json:"result_rows"`

	// Spills is the number of times data was spilled to disk.
	Spills int64 `json:"spills"`

	// NetworkTransfer is the amount of data transferred over the network.
	NetworkTransfer int64 `json:"network_transfer"`

	// IOWaitTime is the time spent waiting for I/O operations.
	IOWaitTime time.Duration `json:"io_wait_time"`

	// StageMetrics contains metrics for each execution stage.
	StageMetrics map[string]StageMetrics `json:"stage_metrics"`
}

// StageMetrics contains metrics for a single execution stage.
type StageMetrics struct {
	// StageName is the name of the execution stage.
	StageName string `json:"stage_name"`

	// Instances is the number of instances for this stage.
	Instances int `json:"instances"`

	// ExecutionTime is how long the stage took to execute.
	ExecutionTime time.Duration `json:"execution_time"`

	// CPUTime is how much CPU time the stage consumed.
	CPUTime time.Duration `json:"cpu_time"`

	// MemoryUsage is the peak memory usage during stage execution.
	MemoryUsage int64 `json:"memory_usage"`

	// InputRows is the number of rows input to the stage.
	InputRows int64 `json:"input_rows"`

	// OutputRows is the number of rows output from the stage.
	OutputRows int64 `json:"output_rows"`

	// OperatorMetrics contains metrics for each operator in the stage.
	OperatorMetrics map[string]map[string]interface{} `json:"operator_metrics"`
}

// QueryAnalysisResult represents the result of analyzing a query.
type QueryAnalysisResult struct {
	// SQL is the analyzed SQL query.
	SQL string `json:"sql"`

	// ParsedSuccessfully indicates whether the query could be parsed without errors.
	ParsedSuccessfully bool `json:"parsed_successfully"`

	// ParseErrors contains any errors encountered during parsing.
	ParseErrors []string `json:"parse_errors,omitempty"`

	// QueryType is the type of query (e.g., "SELECT", "INSERT", "UPDATE").
	QueryType string `json:"query_type"`

	// TablesReferenced is a list of tables referenced in the query.
	TablesReferenced []string `json:"tables_referenced"`

	// ColumnsReferenced is a list of columns referenced in the query.
	ColumnsReferenced []string `json:"columns_referenced"`

	// EstimatedCost is an estimate of the query's execution cost.
	EstimatedCost float64 `json:"estimated_cost"`

	// EstimatedRows is an estimate of the number of rows the query will process.
	EstimatedRows int64 `json:"estimated_rows"`

	// Suggestions contains suggestions for improving the query.
	Suggestions []QuerySuggestion `json:"suggestions"`

	// Warnings contains warnings about potential issues with the query.
	Warnings []string `json:"warnings"`

	// ExplainPlan contains the execution plan for the query.
	ExplainPlan string `json:"explain_plan,omitempty"`
}

// QuerySuggestion represents a suggestion for improving a query.
type QuerySuggestion struct {
	// Type is the type of suggestion (e.g., "index", "rewrite", "partition").
	Type string `json:"type"`

	// Description is a human-readable description of the suggestion.
	Description string `json:"description"`

	// Impact is an estimate of the improvement if the suggestion is applied (0-1).
	Impact float64 `json:"impact"`

	// RewrittenSQL is the suggested SQL rewrite, if applicable.
	RewrittenSQL string `json:"rewritten_sql,omitempty"`
}

// ===============================
// Write Service Interfaces
// ===============================

// WriteService defines the interface for writing data to StarRocks.
// This service handles INSERT, UPDATE, DELETE, and other data modification operations.
type WriteService interface {
	Lifecycle

	// ExecuteUpdate executes a SQL statement that modifies data and returns the result.
	// This includes INSERT, UPDATE, DELETE, and other DML statements.
	ExecuteUpdate(ctx context.Context, request UpdateRequest) (UpdateResult, error)

	// ExecuteBatch executes a batch of SQL statements as a single transaction.
	ExecuteBatch(ctx context.Context, request BatchRequest) (BatchResult, error)

	// LoadData loads data from a file or stream into a table.
	LoadData(ctx context.Context, request LoadDataRequest) (LoadDataResult, error)

	// StreamLoad streams data into a table.
	StreamLoad(ctx context.Context, request StreamLoadRequest, data io.Reader) (StreamLoadResult, error)

	// GetLoadStatus retrieves the status of a load job.
	GetLoadStatus(ctx context.Context, loadID string) (LoadStatus, error)

	// CancelLoad cancels a running load job.
	CancelLoad(ctx context.Context, loadID string) error
}

// UpdateRequest represents a request to execute a data modification statement.
type UpdateRequest struct {
	// SQL is the statement to execute.
	SQL string `json:"sql"`

	// Database is the default database for the statement.
	Database string `json:"database,omitempty"`

	// Parameters contains parameter values for prepared statements.
	Parameters []interface{} `json:"parameters,omitempty"`

	// Options contains additional execution options.
	Options WriteOptions `json:"options,omitempty"`

	// Timeout is the maximum time to wait for the statement to complete.
	Timeout time.Duration `json:"timeout,omitempty"`

	// ClientInfo contains information about the client making the request.
	ClientInfo ClientInfo `json:"client_info,omitempty"`

	// TraceID is an identifier for tracing the statement through the system.
	TraceID string `json:"trace_id,omitempty"`
}

// WriteOptions contains options for write operations.
type WriteOptions struct {
	// SynchronousMode indicates whether to wait for the operation to be fully applied.
	SynchronousMode bool `json:"synchronous_mode,omitempty"`

	// Priority sets the execution priority (1-10, where 10 is highest).
	Priority int `json:"priority,omitempty"`

	// ResourceGroup specifies a resource group for the operation.
	ResourceGroup string `json:"resource_group,omitempty"`

	// SessionVariables contains session variables to set for this operation.
	SessionVariables map[string]string `json:"session_variables,omitempty"`
}

// UpdateResult represents the result of a data modification operation.
type UpdateResult struct {
	// AffectedRows is the number of rows affected by the operation.
	AffectedRows int64 `json:"affected_rows"`

	// LastInsertID is the ID of the last inserted row, if applicable.
	LastInsertID int64 `json:"last_insert_id,omitempty"`

	// ExecutionTime is how long the operation took to execute.
	ExecutionTime time.Duration `json:"execution_time"`

	// Warnings contains any warnings generated during execution.
	Warnings []string `json:"warnings,omitempty"`

	// TransactionID is the ID of the transaction, if applicable.
	TransactionID string `json:"transaction_id,omitempty"`

	// Message is a human-readable message about the result.
	Message string `json:"message"`

	// Success indicates whether the operation was successful.
	Success bool `json:"success"`

	// Error is the error message if the operation failed.
	Error string `json:"error,omitempty"`

	// Timestamp is when the result was created.
	Timestamp time.Time `json:"timestamp"`
}

// BatchRequest represents a request to execute a batch of statements.
type BatchRequest struct {
	// Statements is the list of statements to execute.
	Statements []StatementRequest `json:"statements"`

	// Database is the default database for the statements.
	Database string `json:"database,omitempty"`

	// Options contains additional execution options.
	Options WriteOptions `json:"options,omitempty"`

	// Timeout is the maximum time to wait for the batch to complete.
	Timeout time.Duration `json:"timeout,omitempty"`

	// ClientInfo contains information about the client making the request.
	ClientInfo ClientInfo `json:"client_info,omitempty"`

	// TraceID is an identifier for tracing the batch through the system.
	TraceID string `json:"trace_id,omitempty"`
}

// StatementRequest represents a single statement in a batch request.
type StatementRequest struct {
	// SQL is the statement to execute.
	SQL string `json:"sql"`

	// Parameters contains parameter values for prepared statements.
	Parameters []interface{} `json:"parameters,omitempty"`

	// StatementType is the type of statement (e.g., "INSERT", "UPDATE", "DELETE").
	StatementType string `json:"statement_type,omitempty"`
}

// BatchResult represents the result of a batch operation.
type BatchResult struct {
	// Results is the list of individual statement results.
	Results []StatementResult `json:"results"`

	// Success indicates whether the entire batch was successful.
	Success bool `json:"success"`

	// ExecutionTime is how long the batch took to execute.
	ExecutionTime time.Duration `json:"execution_time"`

	// TransactionID is the ID of the transaction.
	TransactionID string `json:"transaction_id,omitempty"`

	// Error is the error message if the batch failed.
	Error string `json:"error,omitempty"`

	// Message is a human-readable message about the result.
	Message string `json:"message"`

	// Timestamp is when the result was created.
	Timestamp time.Time `json:"timestamp"`
}

// StatementResult represents the result of a single statement in a batch.
type StatementResult struct {
	// StatementIndex is the index of the statement in the batch.
	StatementIndex int `json:"statement_index"`

	// AffectedRows is the number of rows affected by the statement.
	AffectedRows int64 `json:"affected_rows"`

	// LastInsertID is the ID of the last inserted row, if applicable.
	LastInsertID int64 `json:"last_insert_id,omitempty"`

	// Success indicates whether the statement was successful.
	Success bool `json:"success"`

	// Error is the error message if the statement failed.
	Error string `json:"error,omitempty"`

	// Warnings contains any warnings generated during execution.
	Warnings []string `json:"warnings,omitempty"`
}

// LoadDataRequest represents a request to load data from a file or stream.
type LoadDataRequest struct {
	// Database is the database containing the target table.
	Database string `json:"database"`

	// Table is the table to load data into.
	Table string `json:"table"`

	// FilePath is the path to the file containing the data.
	FilePath string `json:"file_path,omitempty"`

	// Format is the format of the data (e.g., "CSV", "JSON", "PARQUET").
	Format string `json:"format"`

	// Columns is a list of column names to load data into.
	Columns []string `json:"columns,omitempty"`

	// ColumnSeparator is the separator between columns (for CSV).
	ColumnSeparator string `json:"column_separator,omitempty"`

	// RowDelimiter is the delimiter between rows (for CSV and JSON).
	RowDelimiter string `json:"row_delimiter,omitempty"`

	// MaxErrors is the maximum number of errors to allow before aborting.
	MaxErrors int `json:"max_errors,omitempty"`

	// Timeout is the maximum time to wait for the load to complete.
	Timeout time.Duration `json:"timeout,omitempty"`

	// Properties contains additional load properties.
	Properties map[string]string `json:"properties,omitempty"`

	// ClientInfo contains information about the client making the request.
	ClientInfo ClientInfo `json:"client_info,omitempty"`

	// TraceID is an identifier for tracing the load through the system.
	TraceID string `json:"trace_id,omitempty"`
}

// StreamLoadRequest represents a request to stream data into a table.
type StreamLoadRequest struct {
	// Database is the database containing the target table.
	Database string `json:"database"`

	// Table is the table to load data into.
	Table string `json:"table"`

	// Format is the format of the data (e.g., "CSV", "JSON").
	Format string `json:"format"`

	// Columns is a list of column names to load data into.
	Columns []string `json:"columns,omitempty"`

	// ColumnSeparator is the separator between columns (for CSV).
	ColumnSeparator string `json:"column_separator,omitempty"`

	// RowDelimiter is the delimiter between rows (for CSV and JSON).
	RowDelimiter string `json:"row_delimiter,omitempty"`

	// MaxErrors is the maximum number of errors to allow before aborting.
	MaxErrors int `json:"max_errors,omitempty"`

	// Properties contains additional load properties.
	Properties map[string]string `json:"properties,omitempty"`

	// ContentLength is the length of the data in bytes, if known.
	ContentLength int64 `json:"content_length,omitempty"`

	// ClientInfo contains information about the client making the request.
	ClientInfo ClientInfo `json:"client_info,omitempty"`

	// TraceID is an identifier for tracing the load through the system.
	TraceID string `json:"trace_id,omitempty"`
}

// LoadDataResult represents the result of a load data operation.
type LoadDataResult struct {
	// LoadID is the unique identifier for the load job.
	LoadID string `json:"load_id"`

	// Success indicates whether the load was successful.
	Success bool `json:"success"`

	// Status is the status of the load (e.g., "COMPLETED", "FAILED", "RUNNING").
	Status string `json:"status"`

	// Message is a human-readable message about the result.
	Message string `json:"message"`

	// Error is the error message if the load failed.
	Error string `json:"error,omitempty"`

	// TotalRows is the total number of rows processed.
	TotalRows int64 `json:"total_rows"`

	// LoadedRows is the number of rows successfully loaded.
	LoadedRows int64 `json:"loaded_rows"`

	// FilteredRows is the number of rows filtered out.
	FilteredRows int64 `json:"filtered_rows"`

	// UnselectedRows is the number of rows that didn't match any target table.
	UnselectedRows int64 `json:"unselected_rows"`

	// LoadBytes is the number of bytes loaded.
	LoadBytes int64 `json:"load_bytes"`

	// LoadTimeMs is the time taken to load the data in milliseconds.
	LoadTimeMs int64 `json:"load_time_ms"`

	// ErrorURL is a URL to detailed error information, if applicable.
	ErrorURL string `json:"error_url,omitempty"`

	// Warnings contains any warnings generated during the load.
	Warnings []string `json:"warnings,omitempty"`

	// Timestamp is when the result was created.
	Timestamp time.Time `json:"timestamp"`
}

// StreamLoadResult represents the result of a stream load operation.
type StreamLoadResult struct {
	// LoadID is the unique identifier for the load job.
	LoadID string `json:"load_id"`

	// Success indicates whether the load was successful.
	Success bool `json:"success"`

	// Status is the status of the load (e.g., "COMPLETED", "FAILED").
	Status string `json:"status"`

	// Message is a human-readable message about the result.
	Message string `json:"message"`

	// Error is the error message if the load failed.
	Error string `json:"error,omitempty"`

	// TotalRows is the total number of rows processed.
	TotalRows int64 `json:"total_rows"`

	// LoadedRows is the number of rows successfully loaded.
	LoadedRows int64 `json:"loaded_rows"`

	// FilteredRows is the number of rows filtered out.
	FilteredRows int64 `json:"filtered_rows"`

	// LoadBytes is the number of bytes loaded.
	LoadBytes int64 `json:"load_bytes"`

	// LoadTimeMs is the time taken to load the data in milliseconds.
	LoadTimeMs int64 `json:"load_time_ms"`

	// ErrorURL is a URL to detailed error information, if applicable.
	ErrorURL string `json:"error_url,omitempty"`

	// Timestamp is when the result was created.
	Timestamp time.Time `json:"timestamp"`
}

// LoadStatus represents the status of a load job.
type LoadStatus struct {
	// LoadID is the unique identifier for the load job.
	LoadID string `json:"load_id"`

	// Database is the database containing the target table.
	Database string `json:"database"`

	// Table is the table the data is being loaded into.
	Table string `json:"table"`

	// Status is the status of the load (e.g., "COMPLETED", "FAILED", "RUNNING", "CANCELLED").
	Status string `json:"status"`

	// Progress is the loading progress as a percentage (0-100).
	Progress float64 `json:"progress"`

	// Message is a human-readable message about the status.
	Message string `json:"message"`

	// Error is the error message if the load failed.
	Error string `json:"error,omitempty"`

	// CreateTime is when the load job was created.
	CreateTime time.Time `json:"create_time"`

	// StartTime is when the load job started executing.
	StartTime time.Time `json:"start_time,omitempty"`

	// FinishTime is when the load job finished.
	FinishTime time.Time `json:"finish_time,omitempty"`

	// TotalRows is the total number of rows processed so far.
	TotalRows int64 `json:"total_rows"`

	// LoadedRows is the number of rows successfully loaded so far.
	LoadedRows int64 `json:"loaded_rows"`

	// FilteredRows is the number of rows filtered out so far.
	FilteredRows int64 `json:"filtered_rows"`

	// LoadBytes is the number of bytes loaded so far.
	LoadBytes int64 `json:"load_bytes"`

	// ErrorURL is a URL to detailed error information, if applicable.
	ErrorURL string `json:"error_url,omitempty"`

	// User is the user who initiated the load job.
	User string `json:"user"`

	// ClientInfo contains information about the client that initiated the load.
	ClientInfo ClientInfo `json:"client_info"`
}

// ===============================
// Health Service Interfaces
// ===============================

// HealthService defines the interface for health monitoring services.
// This service provides information about the health of the system and its components.
type HealthService interface {
	Lifecycle

	// GetClusterHealth returns the overall health of the cluster.
	GetClusterHealth(ctx context.Context) (*HealthStatus, error)

	// GetDatabaseHealth returns the health of a specific database.
	GetDatabaseHealth(ctx context.Context, database string) (*HealthStatus, error)

	// GetTableHealth returns the health of a specific table.
	GetTableHealth(ctx context.Context, database, table string) (*HealthStatus, error)

	// GetPartitionHealth returns the health of a specific partition.
	GetPartitionHealth(ctx context.Context, database, table, partition string) (*HealthStatus, error)

	// GetTabletHealth returns the health of a specific tablet.
	GetTabletHealth(ctx context.Context, tabletID string) (*HealthStatus, error)

	// GetBackendHealth returns the health of a specific backend.
	GetBackendHealth(ctx context.Context, backendID string) (*HealthStatus, error)

	// GetComponentHealth returns the health of a specific component.
	GetComponentHealth(ctx context.Context, component string) (*ComponentHealth, error)

	// GetHealthHistory returns the health history for a target.
	GetHealthHistory(ctx context.Context, scope HealthScope, target string, limit int, since time.Time) (*HealthHistory, error)

	// GetHealthDiagnosis returns a diagnosis of health issues for a target.
	GetHealthDiagnosis(ctx context.Context, scope HealthScope, target string) (*HealthDiagnosis, error)

	// SubscribeHealthUpdates subscribes to health updates.
	SubscribeHealthUpdates(ctx context.Context, scopes []HealthScope, targets []string, levels []HealthLevel, eventTypes []string) (*HealthSubscription, error)

	// UnsubscribeHealthUpdates unsubscribes from health updates.
	UnsubscribeHealthUpdates(ctx context.Context, subscriptionID string) error

	// ReportHealthIssue reports a health issue.
	ReportHealthIssue(ctx context.Context, scope HealthScope, target string, level HealthLevel, message string, details map[string]interface{}) error
}

// HealthLevel represents the severity level of a health status.
type HealthLevel string

const (
	// HealthLevelNormal indicates normal operation.
	HealthLevelNormal HealthLevel = "NORMAL"

	// HealthLevelWarning indicates minor issues that don't affect functionality.
	HealthLevelWarning HealthLevel = "WARNING"

	// HealthLevelDegraded indicates functionality is degraded but still operational.
	HealthLevelDegraded HealthLevel = "DEGRADED"

	// HealthLevelCritical indicates severe issues that affect functionality.
	HealthLevelCritical HealthLevel = "CRITICAL"

	// HealthLevelUnknown indicates the health status is unknown.
	HealthLevelUnknown HealthLevel = "UNKNOWN"
)

// HealthScope defines the scope of a health check.
type HealthScope string

const (
	// HealthScopeCluster indicates a cluster-wide health check.
	HealthScopeCluster HealthScope = "CLUSTER"

	// HealthScopeDatabase indicates a database-specific health check.
	HealthScopeDatabase HealthScope = "DATABASE"

	// HealthScopeTable indicates a table-specific health check.
	HealthScopeTable HealthScope = "TABLE"

	// HealthScopePartition indicates a partition-specific health check.
	HealthScopePartition HealthScope = "PARTITION"

	// HealthScopeTablet indicates a tablet-specific health check.
	HealthScopeTablet HealthScope = "TABLET"

	// HealthScopeBackend indicates a backend-specific health check.
	HealthScopeBackend HealthScope = "BACKEND"

	// HealthScopeComponent indicates a component-specific health check.
	HealthScopeComponent HealthScope = "COMPONENT"
)

// HealthStatus represents a detailed health status.
type HealthStatus struct {
	// Level is the overall health level.
	Level HealthLevel `json:"level"`

	// Scope is the scope of the health status.
	Scope HealthScope `json:"scope"`

	// Target is the specific target of the health status (e.g., table name).
	Target string `json:"target"`

	// Message is a human-readable message describing the health status.
	Message string `json:"message"`

	// Components is a map of component names to their health status.
	Components map[string]*ComponentHealth `json:"components,omitempty"`

	// Metrics contains relevant metrics for this health status.
	Metrics map[string]float64 `json:"metrics,omitempty"`

	// Details contains additional details about the health status.
	Details map[string]interface{} `json:"details,omitempty"`

	// Timestamp is when this health status was generated.
	Timestamp time.Time `json:"timestamp"`

	// Duration is how long the check took.
	Duration time.Duration `json:"duration"`

	// Issues is a list of specific issues found.
	Issues []HealthIssue `json:"issues,omitempty"`

	// Recommendations is a list of recommendations for resolving issues.
	Recommendations []string `json:"recommendations,omitempty"`
}

// ComponentHealth represents the health status of a specific component.
type ComponentHealth struct {
	// Name is the name of the component.
	Name string `json:"name"`

	// Level is the health level of the component.
	Level HealthLevel `json:"level"`

	// Message is a human-readable message describing the component's health.
	Message string `json:"message"`

	// Details contains additional details about the component's health.
	Details map[string]interface{} `json:"details,omitempty"`

	// Timestamp is when this component health was last updated.
	Timestamp time.Time `json:"timestamp"`
}

// HealthIssue represents a specific health issue.
type HealthIssue struct {
	// ID is a unique identifier for this issue.
	ID string `json:"id"`

	// Level is the severity level of the issue.
	Level HealthLevel `json:"level"`

	// Message is a human-readable message describing the issue.
	Message string `json:"message"`

	// Component is the component where the issue was found.
	Component string `json:"component"`

	// FirstDetected is when the issue was first detected.
	FirstDetected time.Time `json:"first_detected"`

	// LastDetected is when the issue was last detected.
	LastDetected time.Time `json:"last_detected"`

	// Count is how many times this issue has been detected.
	Count int `json:"count"`

	// Status indicates whether the issue is new, ongoing, or resolved.
	Status string `json:"status"`

	// Resolution is a message describing how the issue was resolved, if applicable.
	Resolution string `json:"resolution,omitempty"`
}

// HealthHistory represents the history of health status changes.
type HealthHistory struct {
	// Target is the specific target (e.g., table name).
	Target string `json:"target"`

	// Scope is the scope of the health history.
	Scope HealthScope `json:"scope"`

	// Entries is a list of historical health statuses.
	Entries []HealthHistoryEntry `json:"entries"`
}

// HealthHistoryEntry represents a single health status change.
type HealthHistoryEntry struct {
	// Timestamp is when the health status changed.
	Timestamp time.Time `json:"timestamp"`

	// Level is the health level at this point in time.
	Level HealthLevel `json:"level"`

	// Message is a human-readable message describing the health status.
	Message string `json:"message"`

	// Duration is how long this health status lasted.
	Duration time.Duration `json:"duration,omitempty"`
}

// HealthDiagnosis represents a diagnosis of health issues.
type HealthDiagnosis struct {
	// Target is the specific target (e.g., table name).
	Target string `json:"target"`

	// Scope is the scope of the diagnosis.
	Scope HealthScope `json:"scope"`

	// Level is the overall health level.
	Level HealthLevel `json:"level"`

	// Summary is a human-readable summary of the diagnosis.
	Summary string `json:"summary"`

	// Issues is a list of identified issues.
	Issues []HealthIssue `json:"issues"`

	// Recommendations is a list of recommendations for resolving issues.
	Recommendations []string `json:"recommendations"`

	// RootCauses is a list of potential root causes.
	RootCauses []string `json:"root_causes"`

	// RelatedComponents is a list of related components that may be affected.
	RelatedComponents []string `json:"related_components"`

	// Timestamp is when this diagnosis was generated.
	Timestamp time.Time `json:"timestamp"`
}

// HealthEvent represents a health status change event.
type HealthEvent struct {
	// ID is a unique identifier for this event.
	ID string `json:"id"`

	// Type is the type of event (e.g., "status_change", "issue_detected").
	Type string `json:"type"`

	// Level is the health level associated with this event.
	Level HealthLevel `json:"level"`

	// PreviousLevel is the previous health level, if applicable.
	PreviousLevel HealthLevel `json:"previous_level,omitempty"`

	// Scope is the scope of the event.
	Scope HealthScope `json:"scope"`

	// Target is the specific target of the event (e.g., table name).
	Target string `json:"target"`

	// Message is a human-readable message describing the event.
	Message string `json:"message"`

	// Timestamp is when this event occurred.
	Timestamp time.Time `json:"timestamp"`

	// Details contains additional details about the event.
	Details map[string]interface{} `json:"details,omitempty"`
}

// HealthSubscription represents a subscription to health events.
type HealthSubscription struct {
	// ID is a unique identifier for this subscription.
	ID string `json:"id"`

	// Scopes is a list of scopes to subscribe to.
	Scopes []HealthScope `json:"scopes"`

	// Targets is a list of specific targets to subscribe to.
	Targets []string `json:"targets"`

	// Levels is a list of health levels to subscribe to.
	Levels []HealthLevel `json:"levels"`

	// EventTypes is a list of event types to subscribe to.
	EventTypes []string `json:"event_types"`

	// Channel is a channel to send events to.
	Channel chan HealthEvent
}

// ===============================
// Metadata Service Interfaces
// ===============================

// MetadataService defines the interface for accessing StarRocks metadata.
// This service provides information about databases, tables, partitions, and other objects.
type MetadataService interface {
	Lifecycle

	// GetDatabases returns a list of all databases.
	GetDatabases(ctx context.Context) ([]DatabaseInfo, error)

	// GetDatabaseInfo returns detailed information about a specific database.
	GetDatabaseInfo(ctx context.Context, database string) (*DatabaseInfo, error)

	// GetTables returns a list of tables in a database.
	GetTables(ctx context.Context, database string) ([]TableInfo, error)

	// GetTableInfo returns detailed information about a specific table.
	GetTableInfo(ctx context.Context, database, table string) (*TableInfo, error)

	// GetColumns returns a list of columns in a table.
	GetColumns(ctx context.Context, database, table string) ([]ColumnInfo, error)

	// GetPartitions returns a list of partitions in a table.
	GetPartitions(ctx context.Context, database, table string) ([]PartitionInfo, error)

	// GetDistribution returns information about the distribution of a table.
	GetDistribution(ctx context.Context, database, table string) (*DistributionInfo, error)

	// GetStats returns statistics for a table or column.
	GetStats(ctx context.Context, database, table string, column string) (*StatsInfo, error)

	// RefreshStats refreshes statistics for a table.
	RefreshStats(ctx context.Context, database, table string, full bool) error

	// GetSchema returns the schema of a table as a CREATE TABLE statement.
	GetSchema(ctx context.Context, database, table string) (string, error)

	// GetBackends returns a list of all backend nodes.
	GetBackends(ctx context.Context) ([]BackendInfo, error)

	// GetFrontends returns a list of all frontend nodes.
	GetFrontends(ctx context.Context) ([]FrontendInfo, error)

	// GetJobs returns a list of running and recent jobs.
	GetJobs(ctx context.Context, jobType string, limit int) ([]JobInfo, error)

	// SearchObjects searches for database objects matching a pattern.
	SearchObjects(ctx context.Context, pattern string, objectTypes []string) ([]ObjectInfo, error)
}

// DatabaseInfo contains information about a database.
type DatabaseInfo struct {
	// Name is the name of the database.
	Name string `json:"name"`

	// Owner is the owner of the database.
	Owner string `json:"owner"`

	// CreateTime is when the database was created.
	CreateTime time.Time `json:"create_time"`

	// TablesCount is the number of tables in the database.
	TablesCount int `json:"tables_count"`

	// PartitionsCount is the total number of partitions across all tables in the database.
	PartitionsCount int `json:"partitions_count"`

	// TabletsCount is the total number of tablets across all tables in the database.
	TabletsCount int `json:"tablets_count"`

	// ReplicasCount is the total number of replicas across all tablets in the database.
	ReplicasCount int `json:"replicas_count"`

	// SizeBytes is the total size of the database in bytes.
	SizeBytes int64 `json:"size_bytes"`

	// Properties contains additional database properties.
	Properties map[string]string `json:"properties,omitempty"`
}

// TableInfo contains information about a table.
type TableInfo struct {
	// Name is the name of the table.
	Name string `json:"name"`

	// Database is the database the table belongs to.
	Database string `json:"database"`

	// Type is the type of table (e.g., "OLAP", "MYSQL", "ODBC").
	Type string `json:"type"`

	// CreateTime is when the table was created.
	CreateTime time.Time `json:"create_time"`

	// UpdateTime is when the table was last updated.
	UpdateTime time.Time `json:"update_time"`

	// Owner is the owner of the table.
	Owner string `json:"owner"`

	// Comment is the comment for the table.
	Comment string `json:"comment,omitempty"`

	// PartitionType is the partitioning type of the table.
	PartitionType string `json:"partition_type,omitempty"`

	// PartitionsCount is the number of partitions in the table.
	PartitionsCount int `json:"partitions_count"`

	// TabletsCount is the number of tablets in the table.
	TabletsCount int `json:"tablets_count"`

	// ReplicasCount is the number of replicas across all tablets in the table.
	ReplicasCount int `json:"replicas_count"`

	// SizeBytes is the size of the table in bytes.
	SizeBytes int64 `json:"size_bytes"`

	// RowCount is the estimated number of rows in the table.
	RowCount int64 `json:"row_count"`

	// DistributionType is the distribution type of the table.
	DistributionType string `json:"distribution_type,omitempty"`

	// DistributionColumns is a list of columns used for distribution.
	DistributionColumns []string `json:"distribution_columns,omitempty"`

	// BucketCount is the number of buckets used for distribution.
	BucketCount int `json:"bucket_count,omitempty"`

	// Properties contains additional table properties.
	Properties map[string]string `json:"properties,omitempty"`
}

// PartitionInfo contains information about a partition.
type PartitionInfo struct {
	// Name is the name of the partition.
	Name string `json:"name"`

	// Database is the database the partition belongs to.
	Database string `json:"database"`

	// Table is the table the partition belongs to.
	Table string `json:"table"`

	// CreateTime is when the partition was created.
	CreateTime time.Time `json:"create_time"`

	// LastModifyTime is when the partition was last modified.
	LastModifyTime time.Time `json:"last_modify_time"`

	// PartitionKey is the partition key expression.
	PartitionKey string `json:"partition_key,omitempty"`

	// PartitionValue is the partition value or range.
	PartitionValue string `json:"partition_value,omitempty"`

	// TabletsCount is the number of tablets in the partition.
	TabletsCount int `json:"tablets_count"`

	// ReplicasCount is the number of replicas across all tablets in the partition.
	ReplicasCount int `json:"replicas_count"`

	// SizeBytes is the size of the partition in bytes.
	SizeBytes int64 `json:"size_bytes"`

	// RowCount is the estimated number of rows in the partition.
	RowCount int64 `json:"row_count"`

	// DataProperty contains data properties for the partition.
	DataProperty map[string]string `json:"data_property,omitempty"`

	// InMemory indicates whether the partition is in-memory.
	InMemory bool `json:"in_memory"`

	// Status is the status of the partition.
	Status string `json:"status"`
}

// DistributionInfo contains information about a table's distribution.
type DistributionInfo struct {
	// Database is the database the table belongs to.
	Database string `json:"database"`

	// Table is the table name.
	Table string `json:"table"`

	// Type is the distribution type (e.g., "HASH", "RANDOM").
	Type string `json:"type"`

	// Columns is a list of columns used for distribution.
	Columns []string `json:"columns,omitempty"`

	// BucketCount is the number of buckets used for distribution.
	BucketCount int `json:"bucket_count"`

	// BucketByHost is a map of host names to the number of buckets on that host.
	BucketByHost map[string]int `json:"bucket_by_host,omitempty"`

	// SizeBytesPerBucket is the average size in bytes per bucket.
	SizeBytesPerBucket int64 `json:"size_bytes_per_bucket"`

	// RowCountPerBucket is the average number of rows per bucket.
	RowCountPerBucket int64 `json:"row_count_per_bucket"`

	// DistributionDetails contains detailed distribution information.
	DistributionDetails map[string]interface{} `json:"distribution_details,omitempty"`
}

// StatsInfo contains statistics for a table or column.
type StatsInfo struct {
	// Database is the database name.
	Database string `json:"database"`

	// Table is the table name.
	Table string `json:"table"`

	// Column is the column name, if applicable.
	Column string `json:"column,omitempty"`

	// RowCount is the number of rows in the table.
	RowCount int64 `json:"row_count"`

	// DistinctValues is the number of distinct values, if applicable.
	DistinctValues int64 `json:"distinct_values,omitempty"`

	// NullCount is the number of NULL values, if applicable.
	NullCount int64 `json:"null_count,omitempty"`

	// Min is the minimum value, if applicable.
	Min interface{} `json:"min,omitempty"`

	// Max is the maximum value, if applicable.
	Max interface{} `json:"max,omitempty"`

	// Avg is the average value, if applicable.
	Avg float64 `json:"avg,omitempty"`

	// DataSize is the size of the data in bytes.
	DataSize int64 `json:"data_size"`

	// IndexSize is the size of indexes in bytes.
	IndexSize int64 `json:"index_size"`

	// UpdateTime is when the statistics were last updated.
	UpdateTime time.Time `json:"update_time"`

	// Histogram is the histogram data, if available.
	Histogram []HistogramBucket `json:"histogram,omitempty"`
}

// HistogramBucket represents a bucket in a histogram.
type HistogramBucket struct {
	// LowerBound is the lower bound of the bucket.
	LowerBound interface{} `json:"lower_bound"`

	// UpperBound is the upper bound of the bucket.
	UpperBound interface{} `json:"upper_bound"`

	// Count is the number of values in the bucket.
	Count int64 `json:"count"`
}

// BackendInfo contains information about a backend node.
type BackendInfo struct {
	// ID is the unique identifier for the backend.
	ID string `json:"id"`

	// Host is the hostname or IP address of the backend.
	Host string `json:"host"`

	// Port is the port number the backend is listening on.
	Port int `json:"port"`

	// Alive indicates whether the backend is alive.
	Alive bool `json:"alive"`

	// LastHeartbeat is when the last heartbeat was received from the backend.
	LastHeartbeat time.Time `json:"last_heartbeat"`

	// TabletCount is the number of tablets on the backend.
	TabletCount int `json:"tablet_count"`

	// CapacityBytes is the total storage capacity of the backend in bytes.
	CapacityBytes int64 `json:"capacity_bytes"`

	// UsedBytes is the number of bytes used on the backend.
	UsedBytes int64 `json:"used_bytes"`

	// AvailableBytes is the number of bytes available on the backend.
	AvailableBytes int64 `json:"available_bytes"`

	// CPUUsagePercent is the CPU usage as a percentage.
	CPUUsagePercent float64 `json:"cpu_usage_percent"`

	// MemUsagePercent is the memory usage as a percentage.
	MemUsagePercent float64 `json:"mem_usage_percent"`

	// DiskUsagePercent is the disk usage as a percentage.
	DiskUsagePercent float64 `json:"disk_usage_percent"`

	// Tags contains tags associated with the backend.
	Tags []string `json:"tags,omitempty"`
}

// FrontendInfo contains information about a frontend node.
type FrontendInfo struct {
	// ID is the unique identifier for the frontend.
	ID string `json:"id"`

	// Host is the hostname or IP address of the frontend.
	Host string `json:"host"`

	// EditLogPort is the port for edit log communication.
	EditLogPort int `json:"edit_log_port"`

	// HTTPPort is the HTTP port for the frontend.
	HTTPPort int `json:"http_port"`

	// RPCPort is the RPC port for the frontend.
	RPCPort int `json:"rpc_port"`

	// Role is the role of the frontend (e.g., "LEADER", "FOLLOWER").
	Role string `json:"role"`

	// IsMaster indicates whether this frontend is the master.
	IsMaster bool `json:"is_master"`

	// JoinTime is when the frontend joined the cluster.
	JoinTime time.Time `json:"join_time"`

	// Alive indicates whether the frontend is alive.
	Alive bool `json:"alive"`

	// LastHeartbeat is when the last heartbeat was received from the frontend.
	LastHeartbeat time.Time `json:"last_heartbeat"`

	// Version is the software version of the frontend.
	Version string `json:"version"`
}

// JobInfo contains information about a job.
type JobInfo struct {
	// ID is the unique identifier for the job.
	ID string `json:"id"`

	// Type is the type of job (e.g., "LOAD", "SCHEMA_CHANGE", "ROLLUP").
	Type string `json:"type"`

	// Status is the status of the job (e.g., "PENDING", "RUNNING", "FINISHED", "CANCELLED", "FAILED").
	Status string `json:"status"`

	// Database is the database associated with the job.
	Database string `json:"database,omitempty"`

	// Table is the table associated with the job.
	Table string `json:"table,omitempty"`

	// CreateTime is when the job was created.
	CreateTime time.Time `json:"create_time"`

	// StartTime is when the job started executing.
	StartTime time.Time `json:"start_time,omitempty"`

	// FinishTime is when the job finished.
	FinishTime time.Time `json:"finish_time,omitempty"`

	// Progress is the job progress as a percentage (0-100).
	Progress float64 `json:"progress"`

	// User is the user who created the job.
	User string `json:"user"`

	// ErrorMessage is the error message if the job failed.
	ErrorMessage string `json:"error_message,omitempty"`

	// Details contains additional details about the job.
	Details map[string]interface{} `json:"details,omitempty"`
}

// ObjectInfo contains information about a database object.
type ObjectInfo struct {
	// Name is the name of the object.
	Name string `json:"name"`

	// Database is the database the object belongs to.
	Database string `json:"database,omitempty"`

	// Type is the type of object (e.g., "DATABASE", "TABLE", "VIEW", "FUNCTION").
	Type string `json:"type"`

	// Owner is the owner of the object.
	Owner string `json:"owner,omitempty"`

	// CreateTime is when the object was created.
	CreateTime time.Time `json:"create_time,omitempty"`

	// Description is a description of the object.
	Description string `json:"description,omitempty"`
}

// ===============================
// User Service Interfaces
// ===============================

// UserService defines the interface for user management.
// This service handles user authentication, authorization, and management.
type UserService interface {
	Lifecycle

	// AuthenticateUser authenticates a user with a username and password.
	AuthenticateUser(ctx context.Context, username, password string) (*UserAuthResult, error)

	// GetUser returns information about a user.
	GetUser(ctx context.Context, username string) (*UserInfo, error)

	// ListUsers returns a list of all users.
	ListUsers(ctx context.Context) ([]UserInfo, error)

	// CreateUser creates a new user.
	CreateUser(ctx context.Context, request CreateUserRequest) error

	// UpdateUser updates an existing user.
	UpdateUser(ctx context.Context, request UpdateUserRequest) error

	// DeleteUser deletes a user.
	DeleteUser(ctx context.Context, username string) error

	// GrantPrivilege grants a privilege to a user.
	GrantPrivilege(ctx context.Context, request GrantPrivilegeRequest) error

	// RevokePrivilege revokes a privilege from a user.
	RevokePrivilege(ctx context.Context, request RevokePrivilegeRequest) error

	// ListPrivileges returns a list of privileges for a user.
	ListPrivileges(ctx context.Context, username string) ([]PrivilegeInfo, error)

	// CreateRole creates a new role.
	CreateRole(ctx context.Context, roleName string, description string) error

	// DeleteRole deletes a role.
	DeleteRole(ctx context.Context, roleName string) error

	// GrantRoleToUser grants a role to a user.
	GrantRoleToUser(ctx context.Context, roleName, username string) error

	// RevokeRoleFromUser revokes a role from a user.
	RevokeRoleFromUser(ctx context.Context, roleName, username string) error

	// ListRoles returns a list of all roles.
	ListRoles(ctx context.Context) ([]RoleInfo, error)

	// GetRole returns information about a role.
	GetRole(ctx context.Context, roleName string) (*RoleInfo, error)
}

// UserAuthResult contains the result of user authentication.
type UserAuthResult struct {
	// Success indicates whether authentication was successful.
	Success bool `json:"success"`

	// User is the authenticated user information.
	User *UserInfo `json:"user,omitempty"`

	// Token is an authentication token for subsequent requests.
	Token string `json:"token,omitempty"`

	// Error is the error message if authentication failed.
	Error string `json:"error,omitempty"`

	// Timestamp is when authentication occurred.
	Timestamp time.Time `json:"timestamp"`
}

// UserInfo contains information about a user.
type UserInfo struct {
	// Username is the username of the user.
	Username string `json:"username"`

	// Host is the host the user is allowed to connect from.
	Host string `json:"host,omitempty"`

	// IsActive indicates whether the user is active.
	IsActive bool `json:"is_active"`

	// LastLogin is when the user last logged in.
	LastLogin time.Time `json:"last_login,omitempty"`

	// CreateTime is when the user was created.
	CreateTime time.Time `json:"create_time"`

	// Roles is a list of roles assigned to the user.
	Roles []string `json:"roles,omitempty"`

	// MaxUserConnections is the maximum number of connections the user can have.
	MaxUserConnections int `json:"max_user_connections,omitempty"`

	// DefaultRole is the default role for the user.
	DefaultRole string `json:"default_role,omitempty"`
}

// CreateUserRequest represents a request to create a new user.
type CreateUserRequest struct {
	// Username is the username for the new user.
	Username string `json:"username"`

	// Password is the password for the new user.
	Password string `json:"password"`

	// Host is the host the user is allowed to connect from.
	Host string `json:"host,omitempty"`

	// Roles is a list of roles to assign to the user.
	Roles []string `json:"roles,omitempty"`

	// MaxUserConnections is the maximum number of connections the user can have.
	MaxUserConnections int `json:"max_user_connections,omitempty"`

	// DefaultRole is the default role for the user.
	DefaultRole string `json:"default_role,omitempty"`
}

// UpdateUserRequest represents a request to update an existing user.
type UpdateUserRequest struct {
	// Username is the username of the user to update.
	Username string `json:"username"`

	// NewPassword is the new password for the user, if changing.
	NewPassword string `json:"new_password,omitempty"`

	// IsActive indicates whether the user should be active.
	IsActive *bool `json:"is_active,omitempty"`

	// MaxUserConnections is the maximum number of connections the user can have.
	MaxUserConnections *int `json:"max_user_connections,omitempty"`

	// DefaultRole is the default role for the user.
	DefaultRole string `json:"default_role,omitempty"`
}

// GrantPrivilegeRequest represents a request to grant a privilege to a user.
type GrantPrivilegeRequest struct {
	// Username is the username of the user to grant the privilege to.
	Username string `json:"username"`

	// PrivilegeType is the type of privilege to grant.
	PrivilegeType string `json:"privilege_type"`

	// ObjectType is the type of object the privilege applies to.
	ObjectType string `json:"object_type"`

	// Database is the database the privilege applies to, if applicable.
	Database string `json:"database,omitempty"`

	// Table is the table the privilege applies to, if applicable.
	Table string `json:"table,omitempty"`

	// Column is the column the privilege applies to, if applicable.
	Column string `json:"column,omitempty"`

	// WithGrantOption indicates whether the user can grant this privilege to others.
	WithGrantOption bool `json:"with_grant_option,omitempty"`
}

// RevokePrivilegeRequest represents a request to revoke a privilege from a user.
type RevokePrivilegeRequest struct {
	// Username is the username of the user to revoke the privilege from.
	Username string `json:"username"`

	// PrivilegeType is the type of privilege to revoke.
	PrivilegeType string `json:"privilege_type"`

	// ObjectType is the type of object the privilege applies to.
	ObjectType string `json:"object_type"`

	// Database is the database the privilege applies to, if applicable.
	Database string `json:"database,omitempty"`

	// Table is the table the privilege applies to, if applicable.
	Table string `json:"table,omitempty"`

	// Column is the column the privilege applies to, if applicable.
	Column string `json:"column,omitempty"`
}

// PrivilegeInfo contains information about a privilege.
type PrivilegeInfo struct {
	// Username is the username of the user the privilege is granted to.
	Username string `json:"username"`

	// PrivilegeType is the type of privilege.
	PrivilegeType string `json:"privilege_type"`

	// ObjectType is the type of object the privilege applies to.
	ObjectType string `json:"object_type"`

	// Database is the database the privilege applies to, if applicable.
	Database string `json:"database,omitempty"`

	// Table is the table the privilege applies to, if applicable.
	Table string `json:"table,omitempty"`

	// Column is the column the privilege applies to, if applicable.
	Column string `json:"column,omitempty"`

	// WithGrantOption indicates whether the user can grant this privilege to others.
	WithGrantOption bool `json:"with_grant_option"`

	// GrantedBy is the user who granted this privilege.
	GrantedBy string `json:"granted_by,omitempty"`

	// GrantTime is when the privilege was granted.
	GrantTime time.Time `json:"grant_time"`
}

// RoleInfo contains information about a role.
type RoleInfo struct {
	// Name is the name of the role.
	Name string `json:"name"`

	// Description is a description of the role.
	Description string `json:"description,omitempty"`

	// CreateTime is when the role was created.
	CreateTime time.Time `json:"create_time"`

	// CreatedBy is the user who created the role.
	CreatedBy string `json:"created_by,omitempty"`

	// Users is a list of users assigned to the role.
	Users []string `json:"users,omitempty"`

	// Privileges is a list of privileges granted to the role.
	Privileges []PrivilegeInfo `json:"privileges,omitempty"`
}

// ===============================
// Stream Service Interfaces
// ===============================

// StreamService defines the interface for streaming data operations.
// This service handles data streaming for real-time analytics.
type StreamService interface {
	Lifecycle

	// CreateStreamLoad creates a new stream load session.
	CreateStreamLoad(ctx context.Context, request StreamLoadRequest) (*StreamLoadSession, error)

	// AppendStreamData appends data to a stream load session.
	AppendStreamData(ctx context.Context, sessionID string, data []byte) error

	// CommitStreamLoad commits a stream load session.
	CommitStreamLoad(ctx context.Context, sessionID string) (*StreamLoadResult, error)

	// AbortStreamLoad aborts a stream load session.
	AbortStreamLoad(ctx context.Context, sessionID string) error

	// GetStreamLoadStatus gets the status of a stream load session.
	GetStreamLoadStatus(ctx context.Context, sessionID string) (*StreamLoadSession, error)

	// SubscribeStream subscribes to a real-time data stream.
	SubscribeStream(ctx context.Context, request StreamSubscriptionRequest) (*StreamSubscription, error)

	// UnsubscribeStream unsubscribes from a real-time data stream.
	UnsubscribeStream(ctx context.Context, subscriptionID string) error
}

// StreamLoadSession represents a stream load session.
type StreamLoadSession struct {
	// SessionID is the unique identifier for the session.
	SessionID string `json:"session_id"`

	// Database is the database to load data into.
	Database string `json:"database"`

	// Table is the table to load data into.
	Table string `json:"table"`

	// Status is the status of the session.
	Status string `json:"status"`

	// BytesReceived is the number of bytes received so far.
	BytesReceived int64 `json:"bytes_received"`

	// RowsReceived is the number of rows received so far.
	RowsReceived int64 `json:"rows_received"`

	// CreateTime is when the session was created.
	CreateTime time.Time `json:"create_time"`

	// LastUpdateTime is when the session was last updated.
	LastUpdateTime time.Time `json:"last_update_time"`

	// ExpirationTime is when the session will expire.
	ExpirationTime time.Time `json:"expiration_time"`

	// Properties contains additional session properties.
	Properties map[string]string `json:"properties,omitempty"`
}

// StreamSubscriptionRequest represents a request to subscribe to a data stream.
type StreamSubscriptionRequest struct {
	// Database is the database containing the stream.
	Database string `json:"database"`

	// Table is the table to stream data from.
	Table string `json:"table"`

	// Filter is a filter expression to apply to the stream.
	Filter string `json:"filter,omitempty"`

	// Columns is a list of columns to include in the stream.
	Columns []string `json:"columns,omitempty"`

	// Format is the format for streamed data (e.g., "JSON", "CSV").
	Format string `json:"format,omitempty"`

	// BufferSize is the size of the buffer for the stream.
	BufferSize int `json:"buffer_size,omitempty"`

	// Properties contains additional subscription properties.
	Properties map[string]string `json:"properties,omitempty"`
}

// StreamSubscription represents a subscription to a data stream.
type StreamSubscription struct {
	// SubscriptionID is the unique identifier for the subscription.
	SubscriptionID string `json:"subscription_id"`

	// Database is the database containing the stream.
	Database string `json:"database"`

	// Table is the table being streamed from.
	Table string `json:"table"`

	// Status is the status of the subscription.
	Status string `json:"status"`

	// CreateTime is when the subscription was created.
	CreateTime time.Time `json:"create_time"`

	// LastUpdateTime is when the subscription was last updated.
	LastUpdateTime time.Time `json:"last_update_time"`

	// Properties contains additional subscription properties.
	Properties map[string]string `json:"properties,omitempty"`

	// DataChannel is the channel for receiving stream data.
	DataChannel chan StreamData
}

// StreamData represents a single data element in a stream.
type StreamData struct {
	// Data is the data payload.
	Data []byte `json:"data"`

	// Format is the format of the data.
	Format string `json:"format"`

	// Timestamp is when the data was generated.
	Timestamp time.Time `json:"timestamp"`

	// Sequence is the sequence number of the data in the stream.
	Sequence int64 `json:"sequence"`

	// Metadata contains additional metadata about the data.
	Metadata map[string]string `json:"metadata,omitempty"`
}

// ===============================
// Cache Service Interfaces
// ===============================

// CacheService defines the interface for caching operations.
// This service handles caching of query results and metadata.
type CacheService interface {
	Lifecycle

	// GetQueryCache gets a cached query result.
	GetQueryCache(ctx context.Context, cacheKey string) (*CachedQueryResult, error)

	// SetQueryCache stores a query result in the cache.
	SetQueryCache(ctx context.Context, cacheKey string, result *CachedQueryResult, ttl time.Duration) error

	// InvalidateQueryCache invalidates a specific query cache entry.
	InvalidateQueryCache(ctx context.Context, cacheKey string) error

	// GetMetadataCache gets cached metadata.
	GetMetadataCache(ctx context.Context, cacheKey string) (*CachedMetadata, error)

	// SetMetadataCache stores metadata in the cache.
	SetMetadataCache(ctx context.Context, cacheKey string, metadata *CachedMetadata, ttl time.Duration) error

	// InvalidateMetadataCache invalidates a specific metadata cache entry.
	InvalidateMetadataCache(ctx context.Context, cacheKey string) error

	// InvalidateByPattern invalidates cache entries matching a pattern.
	InvalidateByPattern(ctx context.Context, pattern string) error

	// GetCacheStats returns statistics about the cache.
	GetCacheStats(ctx context.Context) (*CacheStats, error)

	// ClearCache clears the entire cache.
	ClearCache(ctx context.Context) error
}

// CachedQueryResult represents a cached query result.
type CachedQueryResult struct {
	// SQL is the SQL query that generated this result.
	SQL string `json:"sql"`

	// Database is the database the query was executed against.
	Database string `json:"database,omitempty"`

	// Columns is metadata about the columns in the result.
	Columns []ColumnInfo `json:"columns"`

	// Rows is the rows in the result.
	Rows [][]interface{} `json:"rows"`

	// RowCount is the number of rows in the result.
	RowCount int64 `json:"row_count"`

	// ExecutionTime is how long the query took to execute.
	ExecutionTime time.Duration `json:"execution_time"`

	// CacheTime is when the result was cached.
	CacheTime time.Time `json:"cache_time"`

	// ExpirationTime is when the cache entry will expire.
	ExpirationTime time.Time `json:"expiration_time"`

	// Parameters contains any parameters used in the query.
	Parameters []interface{} `json:"parameters,omitempty"`
}

// CachedMetadata represents cached metadata.
type CachedMetadata struct {
	// Type is the type of metadata (e.g., "table", "column", "partition").
	Type string `json:"type"`

	// Database is the database the metadata belongs to, if applicable.
	Database string `json:"database,omitempty"`

	// Table is the table the metadata belongs to, if applicable.
	Table string `json:"table,omitempty"`

	// Data is the metadata payload.
	Data interface{} `json:"data"`

	// CacheTime is when the metadata was cached.
	CacheTime time.Time `json:"cache_time"`

	// ExpirationTime is when the cache entry will expire.
	ExpirationTime time.Time `json:"expiration_time"`
}

// CacheStats contains statistics about the cache.
type CacheStats struct {
	// Size is the current size of the cache in bytes.
	Size int64 `json:"size"`

	// Count is the number of items in the cache.
	Count int `json:"count"`

	// Hits is the number of cache hits.
	Hits int64 `json:"hits"`

	// Misses is the number of cache misses.
	Misses int64 `json:"misses"`

	// HitRate is the cache hit rate as a percentage.
	HitRate float64 `json:"hit_rate"`

	// EvictionCount is the number of items evicted from the cache.
	EvictionCount int64 `json:"eviction_count"`

	// QueryCacheStats contains statistics for the query cache.
	QueryCacheStats QueryCacheStats `json:"query_cache_stats"`

	// MetadataCacheStats contains statistics for the metadata cache.
	MetadataCacheStats MetadataCacheStats `json:"metadata_cache_stats"`
}

// QueryCacheStats contains statistics about the query cache.
type QueryCacheStats struct {
	// Size is the current size of the query cache in bytes.
	Size int64 `json:"size"`

	// Count is the number of items in the query cache.
	Count int `json:"count"`

	// Hits is the number of query cache hits.
	Hits int64 `json:"hits"`

	// Misses is the number of query cache misses.
	Misses int64 `json:"misses"`

	// HitRate is the query cache hit rate as a percentage.
	HitRate float64 `json:"hit_rate"`

	// AvgQueryTime is the average execution time of cached queries.
	AvgQueryTime time.Duration `json:"avg_query_time"`
}

// MetadataCacheStats contains statistics about the metadata cache.
type MetadataCacheStats struct {
	// Size is the current size of the metadata cache in bytes.
	Size int64 `json:"size"`

	// Count is the number of items in the metadata cache.
	Count int `json:"count"`

	// Hits is the number of metadata cache hits.
	Hits int64 `json:"hits"`

	// Misses is the number of metadata cache misses.
	Misses int64 `json:"misses"`

	// HitRate is the metadata cache hit rate as a percentage.
	HitRate float64 `json:"hit_rate"`

	// ItemsByType is a breakdown of the number of items by metadata type.
	ItemsByType map[string]int `json:"items_by_type"`
}

//Personal.AI order the ending
