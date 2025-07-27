// Package services provides application-level services for the StarRocks proxy.
package services

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
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
	"github.com/turtacn/staravail/internal/tablet"
	"github.com/turtacn/staravail/internal/write"
)

// WriteOperation represents the type of write operation
type WriteOperation string

const (
	// WriteOperationInsert represents an INSERT operation
	WriteOperationInsert WriteOperation = "INSERT"

	// WriteOperationUpdate represents an UPDATE operation
	WriteOperationUpdate WriteOperation = "UPDATE"

	// WriteOperationDelete represents a DELETE operation
	WriteOperationDelete WriteOperation = "DELETE"

	// WriteOperationUpsert represents an UPSERT operation
	WriteOperationUpsert WriteOperation = "UPSERT"

	// WriteOperationReplace represents a REPLACE operation
	WriteOperationReplace WriteOperation = "REPLACE"

	// WriteOperationBulkLoad represents a BULK LOAD operation
	WriteOperationBulkLoad WriteOperation = "BULK_LOAD"

	// WriteOperationTransaction represents a transaction operation
	WriteOperationTransaction WriteOperation = "TRANSACTION"
)

// WriteMode represents the mode of write operation
type WriteMode string

const (
	// WriteModeSync represents synchronous write mode
	WriteModeSync WriteMode = "SYNC"

	// WriteModeAsync represents asynchronous write mode
	WriteModeAsync WriteMode = "ASYNC"

	// WriteModeBatch represents batch write mode
	WriteModeBatch WriteMode = "BATCH"

	// WriteModeStreaming represents streaming write mode
	WriteModeStreaming WriteMode = "STREAMING"
)

// WriteFormat represents the format of the write data
type WriteFormat string

const (
	// WriteFormatJson represents JSON format
	WriteFormatJson WriteFormat = "JSON"

	// WriteFormatCsv represents CSV format
	WriteFormatCsv WriteFormat = "CSV"

	// WriteFormatAvro represents Avro format
	WriteFormatAvro WriteFormat = "AVRO"

	// WriteFormatParquet represents Parquet format
	WriteFormatParquet WriteFormat = "PARQUET"

	// WriteFormatSql represents SQL format
	WriteFormatSql WriteFormat = "SQL"

	// WriteFormatRows represents row-based format
	WriteFormatRows WriteFormat = "ROWS"
)

// WriteOptions contains options for write operations
type WriteOptions struct {
	// Operation is the type of write operation
	Operation WriteOperation

	// Mode is the mode of write operation
	Mode WriteMode

	// Format is the format of the write data
	Format WriteFormat

	// Database is the target database
	Database string

	// Table is the target table
	Table string

	// Columns are the column names for the write operation
	Columns []string

	// User is the user performing the write
	User string

	// Timeout is the timeout for the write operation
	Timeout time.Duration

	// BatchSize is the batch size for batch writes
	BatchSize int

	// RetryCount is the number of retries for failed writes
	RetryCount int

	// RetryInterval is the interval between retries
	RetryInterval time.Duration

	// AllowPartialFailure indicates whether partial failures are allowed
	AllowPartialFailure bool

	// Labels are arbitrary key-value pairs associated with the write
	Labels map[string]string

	// TraceID is the trace ID for the write
	TraceID string

	// WriteID is an optional ID for the write
	WriteID string

	// Priority specifies the priority of the write
	Priority int

	// TransactionID is the ID of the transaction, if any
	TransactionID string

	// BufferTimeout is the maximum time a write can be buffered
	BufferTimeout time.Duration

	// FlushInterval is the interval for flushing buffered writes
	FlushInterval time.Duration

	// MaxBufferSize is the maximum size of the write buffer
	MaxBufferSize int

	// PartitionBy specifies the partitioning field for writes
	PartitionBy string

	// Preprocessors are functions to preprocess the data before writing
	Preprocessors []WritePreprocessor

	// Validators are functions to validate the data before writing
	Validators []WriteValidator

	// CommitTimeout is the timeout for committing a transaction
	CommitTimeout time.Duration
}

// DefaultWriteOptions returns the default write options
func DefaultWriteOptions() WriteOptions {
	return WriteOptions{
		Operation:         WriteOperationInsert,
		Mode:              WriteModeSync,
		Format:            WriteFormatJson,
		BatchSize:         1000,
		RetryCount:        3,
		RetryInterval:     time.Second,
		AllowPartialFailure: false,
		Labels:            make(map[string]string),
		Priority:          1,
		Timeout:           30 * time.Second,
		BufferTimeout:     5 * time.Minute,
		FlushInterval:     10 * time.Second,
		MaxBufferSize:     10000,
		Preprocessors:     []WritePreprocessor{},
		Validators:        []WriteValidator{},
		CommitTimeout:     10 * time.Second,
	}
}

// WriteRequest represents a request to write data
type WriteRequest struct {
	// Data is the data to write
	Data interface{}

	// Options are the write options
	Options WriteOptions

	// Context is the context for the write operation
	Context context.Context
}

// WriteResponse represents the response to a write operation
type WriteResponse struct {
	// Success indicates whether the write was successful
	Success bool

	// WriteID is the ID of the write
	WriteID string

	// RowsAffected is the number of rows affected by the write
	RowsAffected int64

	// ExecutionTime is the time it took to execute the write
	ExecutionTime time.Duration

	// Warnings are any warnings generated during the write
	Warnings []string

	// Errors are any errors that occurred during the write
	Errors []error

	// BufferedWrite indicates whether the write was buffered
	BufferedWrite bool

	// TransactionID is the ID of the transaction, if any
	TransactionID string

	// Metadata contains additional metadata about the write
	Metadata map[string]interface{}
}

// WriteBufferEntry represents an entry in the write buffer
type WriteBufferEntry struct {
	// Request is the original write request
	Request WriteRequest

	// CreatedAt is when the entry was created
	CreatedAt time.Time

	// Attempts is the number of attempts made to write this entry
	Attempts int

	// LastAttempt is when the last attempt was made
	LastAttempt time.Time

	// NextAttempt is when the next attempt should be made
	NextAttempt time.Time

	// Priority is the priority of the write
	Priority int
}

// WriteBuffer interfaces with a buffer for writes
type WriteBuffer interface {
	// Add adds a write request to the buffer
	Add(ctx context.Context, request WriteRequest) error

	// Flush flushes all buffered writes
	Flush(ctx context.Context) (int, error)

	// FlushByID flushes a specific buffered write
	FlushByID(ctx context.Context, writeID string) error

	// FlushByDatabase flushes all buffered writes for a database
	FlushByDatabase(ctx context.Context, database string) (int, error)

	// FlushByTable flushes all buffered writes for a table
	FlushByTable(ctx context.Context, database, table string) (int, error)

	// GetBufferedWrites gets all buffered writes
	GetBufferedWrites(ctx context.Context) ([]WriteBufferEntry, error)

	// GetBufferedWriteCount gets the count of buffered writes
	GetBufferedWriteCount(ctx context.Context) (int, error)

	// Clear clears all buffered writes
	Clear(ctx context.Context) error
}

// RetryManager manages retries for failed writes
type RetryManager interface {
	// ShouldRetry determines if a write should be retried
	ShouldRetry(ctx context.Context, request WriteRequest, attempt int, err error) bool

	// NextRetryDelay returns the delay before the next retry
	NextRetryDelay(ctx context.Context, request WriteRequest, attempt int) time.Duration

	// RecordSuccess records a successful write
	RecordSuccess(ctx context.Context, request WriteRequest)

	// RecordFailure records a failed write
	RecordFailure(ctx context.Context, request WriteRequest, err error)
}

// WritePreprocessor preprocesses data before writing
type WritePreprocessor interface {
	// Process preprocesses the data
	Process(ctx context.Context, data interface{}, options WriteOptions) (interface{}, error)

	// Name returns the name of the preprocessor
	Name() string
}

// WriteValidator validates data before writing
type WriteValidator interface {
	// Validate validates the data
	Validate(ctx context.Context, data interface{}, options WriteOptions) error

	// Name returns the name of the validator
	Name() string
}

// WriteService defines the interface for write services
type WriteService interface {
	// ExecuteWrite executes a write operation
	ExecuteWrite(ctx context.Context, data interface{}, options WriteOptions) (*WriteResponse, error)

	// BufferWrite buffers a write for later execution
	BufferWrite(ctx context.Context, data interface{}, options WriteOptions) (*WriteResponse, error)

	// FlushBufferedWrites flushes all buffered writes
	FlushBufferedWrites(ctx context.Context) (int, error)

	// GetBufferedWriteCount gets the count of buffered writes
	GetBufferedWriteCount(ctx context.Context) (int, error)

	// BeginTransaction begins a new transaction
	BeginTransaction(ctx context.Context, options WriteOptions) (string, error)

	// CommitTransaction commits a transaction
	CommitTransaction(ctx context.Context, transactionID string) error

	// RollbackTransaction rolls back a transaction
	RollbackTransaction(ctx context.Context, transactionID string) error

	// ExecuteBatchWrite executes a batch write
	ExecuteBatchWrite(ctx context.Context, data []interface{}, options WriteOptions) (*WriteResponse, error)
}

// StarRocksWriteService implements WriteService for StarRocks
type StarRocksWriteService struct {
	// Config is the configuration for the service
	Config *config.Config

	// Logger is the logger for the service
	Logger logging.Logger

	// Metrics is the metrics recorder for the service
	Metrics metrics.MetricsRecorder

	// StarRocksClient is the client for interacting with StarRocks
	StarRocksClient clients.StarRocksClient

	// HealthChecker is used for checking the health of components
	HealthChecker health.HealthChecker

	// TabletManager manages tablet information
	TabletManager tablet.TabletManager

	// WriteBuffer buffers writes that cannot be executed immediately
	WriteBuffer WriteBuffer

	// RetryManager manages retries for failed writes
	RetryManager RetryManager

	// active transactions
	transactions sync.Map

	// writeHistogram records write execution times
	writeHistogram *prometheus.HistogramVec

	// writeCounter counts writes by type and status
	writeCounter *prometheus.CounterVec

	// rowsWrittenCounter counts rows written
	rowsWrittenCounter *prometheus.CounterVec

	// writeBufferGauge tracks the size of the write buffer
	writeBufferGauge *prometheus.GaugeVec

	// scheduledFlushes tracks scheduled buffer flushes
	scheduledFlushes sync.Map

	// flushMutex coordinates buffer flushes
	flushMutex sync.Mutex
}

// NewStarRocksWriteService creates a new StarRocksWriteService
func NewStarRocksWriteService(
	config *config.Config,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
	starRocksClient clients.StarRocksClient,
	healthChecker health.HealthChecker,
	tabletManager tablet.TabletManager,
	writeBuffer WriteBuffer,
	retryManager RetryManager,
) *StarRocksWriteService {
	// Create metrics
	writeHistogram := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "write_execution_time_seconds",
			Help:    "Histogram of write execution times",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 120},
		},
		[]string{"operation", "status", "database", "table"},
	)

	writeCounter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "writes_total",
			Help: "Total number of writes",
		},
		[]string{"operation", "status", "database", "table"},
	)

	rowsWrittenCounter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rows_written_total",
			Help: "Total number of rows written",
		},
		[]string{"operation", "database", "table"},
	)

	writeBufferGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "write_buffer_size",
			Help: "Size of the write buffer",
		},
		[]string{"database", "table"},
	)

	// Register metrics if a registry is provided
	if metricsRecorder != nil {
		if registry, ok := metricsRecorder.GetRegistry().(*prometheus.Registry); ok {
			registry.MustRegister(writeHistogram, writeCounter, rowsWrittenCounter, writeBufferGauge)
		}
	}

	service := &StarRocksWriteService{
		Config:           config,
		Logger:           logger,
		Metrics:          metricsRecorder,
		StarRocksClient:  starRocksClient,
		HealthChecker:    healthChecker,
		TabletManager:    tabletManager,
		WriteBuffer:      writeBuffer,
		RetryManager:     retryManager,
		transactions:     sync.Map{},
		writeHistogram:   writeHistogram,
		writeCounter:     writeCounter,
		rowsWrittenCounter: rowsWrittenCounter,
		writeBufferGauge: writeBufferGauge,
		scheduledFlushes: sync.Map{},
		flushMutex:       sync.Mutex{},
	}

	// Start periodic buffer flushing
	service.startPeriodicBufferFlush()

	return service
}

// ExecuteWrite executes a write operation
func (s *StarRocksWriteService) ExecuteWrite(
	ctx context.Context,
	data interface{},
	options WriteOptions,
) (*WriteResponse, error) {
	startTime := time.Now()

	// Generate write ID if not provided
	if options.WriteID == "" {
		options.WriteID = generateWriteID()
	}

	// Set up logger with write information
	writeLogger := s.Logger.With(
		"write_id", options.WriteID,
		"operation", options.Operation,
		"database", options.Database,
		"table", options.Table,
		"user", options.User,
	)

	writeLogger.Info("Executing write operation",
		"mode", options.Mode,
		"format", options.Format,
	)

	// Create context with timeout if specified
	var cancelFunc context.CancelFunc = func() {}
	if options.Timeout > 0 {
		ctx, cancelFunc = context.WithTimeout(ctx, options.Timeout)
	}
	defer cancelFunc()

	// Check health of required components
	healthResult := s.HealthChecker.Check(ctx)
	if !healthResult.Healthy {
		writeLogger.Warn("System is unhealthy, checking write handling strategy",
			"health_result", healthResult)

		// Determine how to handle the write based on health status
		strategy := s.determineWriteStrategy(healthResult, options)

		switch strategy {
		case "buffer":
			writeLogger.Info("Buffering write due to system health",
				"health_message", healthResult.Message)
			return s.BufferWrite(ctx, data, options)

		case "reject":
			err := fmt.Errorf("write rejected due to system health: %s", healthResult.Message)
			writeLogger.Error("Write rejected due to system health", "error", err)

			// Record metrics
			s.recordWriteMetrics(string(options.Operation), "rejected", options.Database, options.Table, 0, time.Since(startTime))

			return &WriteResponse{
				Success:       false,
				WriteID:       options.WriteID,
				RowsAffected:  0,
				ExecutionTime: time.Since(startTime),
				Warnings:      []string{healthResult.Message},
				Errors:        []error{err},
				BufferedWrite: false,
			}, err

		case "proceed":
			// Continue with the write despite health warnings
			writeLogger.Warn("Proceeding with write despite system health warnings",
				"health_message", healthResult.Message)
		}
	}

	// If this is part of a transaction, validate the transaction
	if options.TransactionID != "" {
		if err := s.validateTransaction(ctx, options.TransactionID); err != nil {
			writeLogger.Error("Transaction validation failed", "error", err)

			// Record metrics
			s.recordWriteMetrics(string(options.Operation), "error", options.Database, options.Table, 0, time.Since(startTime))

			return &WriteResponse{
				Success:       false,
				WriteID:       options.WriteID,
				RowsAffected:  0,
				ExecutionTime: time.Since(startTime),
				Warnings:      []string{},
				Errors:        []error{err},
				BufferedWrite: false,
				TransactionID: options.TransactionID,
			}, err
		}
	}

	// Apply preprocessors
	processedData, err := s.applyPreprocessors(ctx, data, options)
	if err != nil {
		writeLogger.Error("Data preprocessing failed", "error", err)

		// Record metrics
		s.recordWriteMetrics(string(options.Operation), "error", options.Database, options.Table, 0, time.Since(startTime))

		return &WriteResponse{
			Success:       false,
			WriteID:       options.WriteID,
			RowsAffected:  0,
			ExecutionTime: time.Since(startTime),
			Warnings:      []string{},
			Errors:        []error{err},
			BufferedWrite: false,
			TransactionID: options.TransactionID,
		}, err
	}

	// Validate data
	if err := s.validateData(ctx, processedData, options); err != nil {
		writeLogger.Error("Data validation failed", "error", err)

		// Record metrics
		s.recordWriteMetrics(string(options.Operation), "error", options.Database, options.Table, 0, time.Since(startTime))

		return &WriteResponse{
			Success:       false,
			WriteID:       options.WriteID,
			RowsAffected:  0,
			ExecutionTime: time.Since(startTime),
			Warnings:      []string{},
			Errors:        []error{err},
			BufferedWrite: false,
			TransactionID: options.TransactionID,
		}, err
	}

	// Get tablet information for the write target
	tabletInfo, err := s.TabletManager.GetTabletInfo(ctx, options.Database, options.Table)
	if err != nil {
		writeLogger.Warn("Failed to get tablet information, proceeding with default endpoint",
			"error", err)
		// Continue with default endpoint
	}

	// Handle different write modes
	switch options.Mode {
	case WriteModeAsync:
		// For async mode, we buffer the write and return immediately
		return s.BufferWrite(ctx, processedData, options)

	case WriteModeBatch:
		// For batch mode, we expect data to be an array/slice
		dataSlice, ok := s.convertToSlice(processedData)
		if !ok {
			err := errors.New("data must be a slice/array for batch mode")
			writeLogger.Error("Invalid data for batch mode", "error", err)

			// Record metrics
			s.recordWriteMetrics(string(options.Operation), "error", options.Database, options.Table, 0, time.Since(startTime))

			return &WriteResponse{
				Success:       false,
				WriteID:       options.WriteID,
				RowsAffected:  0,
				ExecutionTime: time.Since(startTime),
				Warnings:      []string{},
				Errors:        []error{err},
				BufferedWrite: false,
				TransactionID: options.TransactionID,
			}, err
		}

		return s.executeBatchWriteInternal(ctx, dataSlice, options, writeLogger, startTime)

	case WriteModeStreaming:
		// For streaming mode, we set up a streaming connection to StarRocks
		// This is more complex and would involve a different approach
		err := errors.New("streaming mode not implemented yet")
		writeLogger.Error("Streaming mode not supported", "error", err)

		// Record metrics
		s.recordWriteMetrics(string(options.Operation), "error", options.Database, options.Table, 0, time.Since(startTime))

		return &WriteResponse{
			Success:       false,
			WriteID:       options.WriteID,
			RowsAffected:  0,
			ExecutionTime: time.Since(startTime),
			Warnings:      []string{},
			Errors:        []error{err},
			BufferedWrite: false,
			TransactionID: options.TransactionID,
		}, err

	default: // WriteModeSync
		// For sync mode, we execute the write directly
		return s.executeSyncWrite(ctx, processedData, options, tabletInfo, writeLogger, startTime)
	}
}

// BufferWrite buffers a write for later execution
func (s *StarRocksWriteService) BufferWrite(
	ctx context.Context,
	data interface{},
	options WriteOptions,
) (*WriteResponse, error) {
	startTime := time.Now()

	// Generate write ID if not provided
	if options.WriteID == "" {
		options.WriteID = generateWriteID()
	}

	// Set up logger with write information
	writeLogger := s.Logger.With(
		"write_id", options.WriteID,
		"operation", options.Operation,
		"database", options.Database,
		"table", options.Table,
		"user", options.User,
	)

	writeLogger.Info("Buffering write operation",
		"format", options.Format,
		"buffer_timeout", options.BufferTimeout,
	)

	// Apply preprocessors
	processedData, err := s.applyPreprocessors(ctx, data, options)
	if err != nil {
		writeLogger.Error("Data preprocessing failed", "error", err)

		// Record metrics
		s.recordWriteMetrics(string(options.Operation), "error", options.Database, options.Table, 0, time.Since(startTime))

		return &WriteResponse{
			Success:       false,
			WriteID:       options.WriteID,
			RowsAffected:  0,
			ExecutionTime: time.Since(startTime),
			Warnings:      []string{},
			Errors:        []error{err},
			BufferedWrite: false,
			TransactionID: options.TransactionID,
		}, err
	}

	// Validate data
	if err := s.validateData(ctx, processedData, options); err != nil {
		writeLogger.Error("Data validation failed", "error", err)

		// Record metrics
		s.recordWriteMetrics(string(options.Operation), "error", options.Database, options.Table, 0, time.Since(startTime))

		return &WriteResponse{
			Success:       false,
			WriteID:       options.WriteID,
			RowsAffected:  0,
			ExecutionTime: time.Since(startTime),
			Warnings:      []string{},
			Errors:        []error{err},
			BufferedWrite: false,
			TransactionID: options.TransactionID,
		}, err
	}

	// Create write request
	request := WriteRequest{
		Data:    processedData,
		Options: options,
		Context: ctx,
	}

	// Add to buffer
	if err := s.WriteBuffer.Add(ctx, request); err != nil {
		writeLogger.Error("Failed to buffer write", "error", err)

		// Record metrics
		s.recordWriteMetrics(string(options.Operation), "buffer_error", options.Database, options.Table, 0, time.Since(startTime))

		return &WriteResponse{
			Success:       false,
			WriteID:       options.WriteID,
			RowsAffected:  0,
			ExecutionTime: time.Since(startTime),
			Warnings:      []string{},
			Errors:        []error{err},
			BufferedWrite: false,
			TransactionID: options.TransactionID,
		}, err
	}

	// Update buffer size gauge
	bufferCount, _ := s.WriteBuffer.GetBufferedWriteCount(ctx)
	if s.writeBufferGauge != nil {
		s.writeBufferGauge.WithLabelValues(options.Database, options.Table).Set(float64(bufferCount))
	}

	// Schedule a flush for this specific write after buffer timeout
	if options.BufferTimeout > 0 {
		s.scheduleBufferFlush(options.WriteID, options.BufferTimeout)
	}

	// Record metrics
	s.recordWriteMetrics(string(options.Operation), "buffered", options.Database, options.Table, 0, time.Since(startTime))

	writeLogger.Info("Write buffered successfully",
		"buffer_count", bufferCount,
		"buffer_timeout", options.BufferTimeout,
	)

	return &WriteResponse{
		Success:       true,
		WriteID:       options.WriteID,
		RowsAffected:  0, // Unknown until flush
		ExecutionTime: time.Since(startTime),
		Warnings:      []string{},
		Errors:        []error{},
		BufferedWrite: true,
		TransactionID: options.TransactionID,
		Metadata: map[string]interface{}{
			"buffered_count": bufferCount,
		},
	}, nil
}

// FlushBufferedWrites flushes all buffered writes
func (s *StarRocksWriteService) FlushBufferedWrites(ctx context.Context) (int, error) {
	s.flushMutex.Lock()
	defer s.flushMutex.Unlock()

	startTime := time.Now()

	s.Logger.Info("Flushing all buffered writes")

	// Flush the buffer
	count, err := s.WriteBuffer.Flush(ctx)

	// Update metrics
	if s.Metrics != nil {
		s.Metrics.CounterInc("buffer_flushes_total", map[string]string{
			"status": err == nil ? "success" : "error",
		})
		s.Metrics.HistogramObserve("buffer_flush_time_ms",
			float64(time.Since(startTime).Milliseconds()),
			map[string]string{
				"status": err == nil ? "success" : "error",
			})
		s.Metrics.GaugeSet("buffered_writes", 0, nil)
	}

	if err != nil {
		s.Logger.Error("Failed to flush buffered writes", "error", err)
		return 0, err
	}

	// Update buffer size gauge
	if s.writeBufferGauge != nil {
		s.writeBufferGauge.Reset()
	}

	s.Logger.Info("Successfully flushed buffered writes",
		"count", count,
		"time", time.Since(startTime),
	)

	return count, nil
}

// GetBufferedWriteCount gets the count of buffered writes
func (s *StarRocksWriteService) GetBufferedWriteCount(ctx context.Context) (int, error) {
	return s.WriteBuffer.GetBufferedWriteCount(ctx)
}

// BeginTransaction begins a new transaction
func (s *StarRocksWriteService) BeginTransaction(
	ctx context.Context,
	options WriteOptions,
) (string, error) {
	// Generate transaction ID
	transactionID := generateTransactionID()

	// Set up logger with transaction information
	txLogger := s.Logger.With(
		"transaction_id", transactionID,
		"database", options.Database,
		"user", options.User,
	)

	txLogger.Info("Beginning transaction")

	// Begin transaction in StarRocks
	err := s.StarRocksClient.BeginTransaction(ctx, clients.TransactionOptions{
		Database: options.Database,
		User:     options.User,
		Timeout:  options.Timeout,
	})

	if err != nil {
		txLogger.Error("Failed to begin transaction", "error", err)

		// Record metrics
		if s.Metrics != nil {
			s.Metrics.CounterInc("transactions_total", map[string]string{
				"operation": "begin",
				"status":    "error",
				"database":  options.Database,
			})
		}

		return "", err
	}

	// Store transaction information
	txInfo := &domain.TransactionInfo{
		ID:        transactionID,
		Database:  options.Database,
		User:      options.User,
		StartTime: time.Now(),
		Status:    domain.TransactionStatusActive,
		Timeout:   options.Timeout,
	}

	s.transactions.Store(transactionID, txInfo)

	// Record metrics
	if s.Metrics != nil {
		s.Metrics.CounterInc("transactions_total", map[string]string{
			"operation": "begin",
			"status":    "success",
			"database":  options.Database,
		})
	}

	txLogger.Info("Transaction begun successfully")

	return transactionID, nil
}

// CommitTransaction commits a transaction
func (s *StarRocksWriteService) CommitTransaction(
	ctx context.Context,
	transactionID string,
) error {
	// Validate transaction
	txInfoAny, ok := s.transactions.Load(transactionID)
	if !ok {
		return fmt.Errorf("transaction not found: %s", transactionID)
	}

	txInfo := txInfoAny.(*domain.TransactionInfo)

	// Set up logger with transaction information
	txLogger := s.Logger.With(
		"transaction_id", transactionID,
		"database", txInfo.Database,
		"user", txInfo.User,
	)

	txLogger.Info("Committing transaction",
		"duration", time.Since(txInfo.StartTime),
	)

	// Create context with timeout if specified
	var cancelFunc context.CancelFunc = func() {}
	if txInfo.Timeout > 0 {
		ctx, cancelFunc = context.WithTimeout(ctx, txInfo.Timeout)
	}
	defer cancelFunc()

	// Commit transaction in StarRocks
	err := s.StarRocksClient.CommitTransaction(ctx, clients.TransactionOptions{
		Database: txInfo.Database,
		User:     txInfo.User,
		Timeout:  txInfo.Timeout,
	})

	if err != nil {
		txLogger.Error("Failed to commit transaction", "error", err)

		// Update transaction status
		txInfo.Status = domain.TransactionStatusFailed
		txInfo.EndTime = time.Now()
		s.transactions.Store(transactionID, txInfo)

		// Record metrics
		if s.Metrics != nil {
			s.Metrics.CounterInc("transactions_total", map[string]string{
				"operation": "commit",
				"status":    "error",
				"database":  txInfo.Database,
			})
		}

		return err
	}

	// Update transaction status
	txInfo.Status = domain.TransactionStatusCommitted
	txInfo.EndTime = time.Now()
	s.transactions.Store(transactionID, txInfo)

	// Record metrics
	if s.Metrics != nil {
		s.Metrics.CounterInc("transactions_total", map[string]string{
			"operation": "commit",
			"status":    "success",
			"database":  txInfo.Database,
		})
		s.Metrics.HistogramObserve("transaction_duration_ms",
			float64(time.Since(txInfo.StartTime).Milliseconds()),
			map[string]string{
				"database": txInfo.Database,
				"status":   "committed",
			})
	}

	txLogger.Info("Transaction committed successfully",
		"duration", time.Since(txInfo.StartTime),
	)

	// Schedule cleanup of transaction info after some time
	go func() {
		time.Sleep(1 * time.Hour)
		s.transactions.Delete(transactionID)
	}()

	return nil
}

// RollbackTransaction rolls back a transaction
func (s *StarRocksWriteService) RollbackTransaction(
	ctx context.Context,
	transactionID string,
) error {
	// Validate transaction
	txInfoAny, ok := s.transactions.Load(transactionID)
	if !ok {
		return fmt.Errorf("transaction not found: %s", transactionID)
	}

	txInfo := txInfoAny.(*domain.TransactionInfo)

	// Set up logger with transaction information
	txLogger := s.Logger.With(
		"transaction_id", transactionID,
		"database", txInfo.Database,
		"user", txInfo.User,
	)

	txLogger.Info("Rolling back transaction",
		"duration", time.Since(txInfo.StartTime),
	)

	// Create context with timeout if specified
	var cancelFunc context.CancelFunc = func() {}
	if txInfo.Timeout > 0 {
		ctx, cancelFunc = context.WithTimeout(ctx, txInfo.Timeout)
	}
	defer cancelFunc()

	// Rollback transaction in StarRocks
	err := s.StarRocksClient.RollbackTransaction(ctx, clients.TransactionOptions{
		Database: txInfo.Database,
		User:     txInfo.User,
		Timeout:  txInfo.Timeout,
	})

	if err != nil {
		txLogger.Error("Failed to rollback transaction", "error", err)

		// Update transaction status
		txInfo.Status = domain.TransactionStatusFailed
		txInfo.EndTime = time.Now()
		s.transactions.Store(transactionID, txInfo)

		// Record metrics
		if s.Metrics != nil {
			s.Metrics.CounterInc("transactions_total", map[string]string{
				"operation": "rollback",
				"status":    "error",
				"database":  txInfo.Database,
			})
		}

		return err
	}

	// Update transaction status
	txInfo.Status = domain.TransactionStatusRolledBack
	txInfo.EndTime = time.Now()
	s.transactions.Store(transactionID, txInfo)

	// Record metrics
	if s.Metrics != nil {
		s.Metrics.CounterInc("transactions_total", map[string]string{
			"operation": "rollback",
			"status":    "success",
			"database":  txInfo.Database,
		})
		s.Metrics.HistogramObserve("transaction_duration_ms",
			float64(time.Since(txInfo.StartTime).Milliseconds()),
			map[string]string{
				"database": txInfo.Database,
				"status":   "rolled_back",
			})
	}

	txLogger.Info("Transaction rolled back successfully",
		"duration", time.Since(txInfo.StartTime),
	)

	// Schedule cleanup of transaction info after some time
	go func() {
		time.Sleep(1 * time.Hour)
		s.transactions.Delete(transactionID)
	}()

	return nil
}

// ExecuteBatchWrite executes a batch write
func (s *StarRocksWriteService) ExecuteBatchWrite(
	ctx context.Context,
	data []interface{},
	options WriteOptions,
) (*WriteResponse, error) {
	startTime := time.Now()

	// Generate write ID if not provided
	if options.WriteID == "" {
		options.WriteID = generateWriteID()
	}

	// Set up logger with write information
	writeLogger := s.Logger.With(
		"write_id", options.WriteID,
		"operation", options.Operation,
		"database", options.Database,
		"table", options.Table,
		"user", options.User,
		"batch_size", len(data),
	)

	writeLogger.Info("Executing batch write operation",
		"format", options.Format,
	)

	// Set write mode to batch
	options.Mode = WriteModeBatch

	// Execute the batch write
	return s.executeBatchWriteInternal(ctx, data, options, writeLogger, startTime)
}

// executeSyncWrite executes a synchronous write
func (s *StarRocksWriteService) executeSyncWrite(
	ctx context.Context,
	data interface{},
	options WriteOptions,
	tabletInfo *tablet.TabletInfo,
	logger logging.Logger,
	startTime time.Time,
) (*WriteResponse, error) {
	// Convert data to the appropriate format for the client
	clientData, err := s.convertDataForClient(data, options)
	if err != nil {
		logger.Error("Failed to convert data for client", "error", err)

		// Record metrics
		s.recordWriteMetrics(string(options.Operation), "error", options.Database, options.Table, 0, time.Since(startTime))

		return &WriteResponse{
			Success:       false,
			WriteID:       options.WriteID,
			RowsAffected:  0,
			ExecutionTime: time.Since(startTime),
			Warnings:      []string{},
			Errors:        []error{err},
			BufferedWrite: false,
			TransactionID: options.TransactionID,
		}, err
	}

	// Prepare client options
	clientOptions := s.prepareClientOptions(options, tabletInfo)

	// Execute the write
	var clientResult *clients.WriteResult
	var writeErr error

	attempt := 0
	maxAttempts := options.RetryCount + 1

	for attempt < maxAttempts {
		// Execute the write in StarRocks
		clientResult, writeErr = s.StarRocksClient.ExecuteWrite(ctx, clientData, clientOptions)

		// If success or should not retry, break
		if writeErr == nil || !s.RetryManager.ShouldRetry(ctx, WriteRequest{Data: data, Options: options}, attempt, writeErr) {
			break
		}

		// Record retry
		if s.Metrics != nil {
			s.Metrics.CounterInc("write_retries", map[string]string{
				"operation": string(options.Operation),
				"database":  options.Database,
				"table":     options.Table,
			})
		}

		attempt++
		if attempt < maxAttempts {
			// Calculate retry delay
			retryDelay := s.RetryManager.NextRetryDelay(ctx, WriteRequest{Data: data, Options: options}, attempt)

			logger.Warn("Retrying write after error",
				"error", writeErr,
				"attempt", attempt,
				"max_attempts", maxAttempts,
				"retry_delay", retryDelay,
			)

			// Wait before retry
			select {
			case <-time.After(retryDelay):
				// Continue with retry
			case <-ctx.Done():
				// Context cancelled
				return &WriteResponse{
					Success:       false,
					WriteID:       options.WriteID,
					RowsAffected:  0,
					ExecutionTime: time.Since(startTime),
					Warnings:      []string{},
					Errors:        []error{ctx.Err()},
					BufferedWrite: false,
					TransactionID: options.TransactionID,
				}, ctx.Err()
			}
		}
	}

	// Handle write error
	if writeErr != nil {
		logger.Error("Write execution failed", "error", writeErr, "attempts", attempt+1)

		// Record metrics
		s.recordWriteMetrics(string(options.Operation), "error", options.Database, options.Table, 0, time.Since(startTime))

		// Record failure in retry manager
		s.RetryManager.RecordFailure(ctx, WriteRequest{Data: data, Options: options}, writeErr)

		return &WriteResponse{
			Success:       false,
			WriteID:       options.WriteID,
			RowsAffected:  0,
			ExecutionTime: time.Since(startTime),
			Warnings:      []string{},
			Errors:        []error{writeErr},
			BufferedWrite: false,
			TransactionID: options.TransactionID,
		}, writeErr
	}

	// Record success in retry manager
	s.RetryManager.RecordSuccess(ctx, WriteRequest{Data: data, Options: options})

	// Create successful response
	response := &WriteResponse{
		Success:       true,
		WriteID:       options.WriteID,
		RowsAffected:  clientResult.RowsAffected,
		ExecutionTime: time.Since(startTime),
		Warnings:      clientResult.Warnings,
		Errors:        []error{},
		BufferedWrite: false,
		TransactionID: options.TransactionID,
		Metadata:      clientResult.Metadata,
	}

	// Record metrics
	s.recordWriteMetrics(string(options.Operation), "success", options.Database, options.Table, clientResult.RowsAffected, time.Since(startTime))

	logger.Info("Write executed successfully",
		"rows_affected", clientResult.RowsAffected,
		"time", time.Since(startTime),
	)

	return response, nil
}

// executeBatchWriteInternal executes a batch write
func (s *StarRocksWriteService) executeBatchWriteInternal(
	ctx context.Context,
	data []interface{},
	options WriteOptions,
	logger logging.Logger,
	startTime time.Time,
) (*WriteResponse, error) {
	// Get tablet information for the write target
	tabletInfo, err := s.TabletManager.GetTabletInfo(ctx, options.Database, options.Table)
	if err != nil {
		logger.Warn("Failed to get tablet information, proceeding with default endpoint",
			"error", err)
		// Continue with default endpoint
	}

	// Prepare client options
	clientOptions := s.prepareClientOptions(options, tabletInfo)

	// Break data into batches if needed
	batchSize := options.BatchSize
	if batchSize <= 0 {
		batchSize = len(data) // Use all data in one batch
	}

	// Prepare batches
	var batches [][]interface{}
	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize
		if end > len(data) {
			end = len(data)
		}
		batches = append(batches, data[i:end])
	}

	logger.Info("Executing batch write",
		"total_records", len(data),
		"batch_size", batchSize,
		"batch_count", len(batches),
	)

	// Results
	var totalRowsAffected int64
	var allWarnings []string
	var allErrors []error
	successfulBatches := 0

	// Execute each batch
	for i, batch := range batches {
		batchLogger := logger.With("batch", i+1, "batch_size", len(batch))

		// Convert batch data for client
		clientData, err := s.convertDataForClient(batch, options)
		if err != nil {
			batchLogger.Error("Failed to convert batch data for client", "error", err)
			if !options.AllowPartialFailure {
				// Record metrics
				s.recordWriteMetrics(string(options.Operation), "error", options.Database, options.Table, totalRowsAffected, time.Since(startTime))

				return &WriteResponse{
					Success:       false,
					WriteID:       options.WriteID,
					RowsAffected:  totalRowsAffected,
					ExecutionTime: time.Since(startTime),
					Warnings:      allWarnings,
					Errors:        append(allErrors, err),
					BufferedWrite: false,
					TransactionID: options.TransactionID,
				}, err
			}

			allErrors = append(allErrors, err)
			allWarnings = append(allWarnings, fmt.Sprintf("Batch %d failed: %v", i+1, err))
			continue
		}

		// Execute the batch
		var batchResult *clients.WriteResult
		var batchErr error

		attempt := 0
		maxAttempts := options.RetryCount + 1

		for attempt < maxAttempts {
			// Execute the write in StarRocks
			batchResult, batchErr = s.StarRocksClient.ExecuteWrite(ctx, clientData, clientOptions)

			// If success or should not retry, break
			if batchErr == nil || !s.RetryManager.ShouldRetry(ctx, WriteRequest{Data: batch, Options: options}, attempt, batchErr) {
				break
			}

			// Record retry
			if s.Metrics != nil {
				s.Metrics.CounterInc("write_retries", map[string]string{
					"operation": string(options.Operation),
					"database":  options.Database,
					"table":     options.Table,
					"mode":      "batch",
				})
			}

			attempt++
			if attempt < maxAttempts {
				// Calculate retry delay
				retryDelay := s.RetryManager.NextRetryDelay(ctx, WriteRequest{Data: batch, Options: options}, attempt)

				batchLogger.Warn("Retrying batch after error",
					"error", batchErr,
					"attempt", attempt,
					"max_attempts", maxAttempts,
					"retry_delay", retryDelay,
				)

				// Wait before retry
				select {
				case <-time.After(retryDelay):
					// Continue with retry
				case <-ctx.Done():
					// Context cancelled
					return &WriteResponse{
						Success:       successfulBatches > 0,
						WriteID:       options.WriteID,
						RowsAffected:  totalRowsAffected,
						ExecutionTime: time.Since(startTime),
						Warnings:      allWarnings,
						Errors:        append(allErrors, ctx.Err()),
						BufferedWrite: false,
						TransactionID: options.TransactionID,
					}, ctx.Err()
				}
			}
		}

		// Handle batch error
		if batchErr != nil {
			batchLogger.Error("Batch execution failed", "error", batchErr, "attempts", attempt+1)

			if !options.AllowPartialFailure {
				// Record metrics
				s.recordWriteMetrics(string(options.Operation), "error", options.Database, options.Table, totalRowsAffected, time.Since(startTime))

				return &WriteResponse{
					Success:       false,
					WriteID:       options.WriteID,
					RowsAffected:  totalRowsAffected,
					ExecutionTime: time.Since(startTime),
					Warnings:      allWarnings,
					Errors:        append(allErrors, batchErr),
					BufferedWrite: false,
					TransactionID: options.TransactionID,
				}, batchErr
			}

			allErrors = append(allErrors, batchErr)
			allWarnings = append(allWarnings, fmt.Sprintf("Batch %d failed: %v", i+1, batchErr))
			continue
		}

		// Update totals
		totalRowsAffected += batchResult.RowsAffected
		successfulBatches++

		// Add batch warnings
		for _, warning := range batchResult.Warnings {
			allWarnings = append(allWarnings, fmt.Sprintf("Batch %d: %s", i+1, warning))
		}

		batchLogger.Info("Batch executed successfully",
			"rows_affected", batchResult.RowsAffected,
		)
	}

	// Determine overall success
	success := len(allErrors) == 0 || (options.AllowPartialFailure && successfulBatches > 0)

	// Record metrics
	status := "success"
	if !success {
		status = "error"
	} else if len(allErrors) > 0 {
		status = "partial_success"
	}

	s.recordWriteMetrics(string(options.Operation), status, options.Database, options.Table, totalRowsAffected, time.Since(startTime))

	// Create response
	response := &WriteResponse{
		Success:       success,
		WriteID:       options.WriteID,
		RowsAffected:  totalRowsAffected,
		ExecutionTime: time.Since(startTime),
		Warnings:      allWarnings,
		Errors:        allErrors,
		BufferedWrite: false,
		TransactionID: options.TransactionID,
		Metadata: map[string]interface{}{
			"successful_batches": successfulBatches,
			"total_batches":      len(batches),
		},
	}

	logger.Info("Batch write completed",
		"success", success,
		"total_rows_affected", totalRowsAffected,
		"successful_batches", successfulBatches,
		"total_batches", len(batches),
		"time", time.Since(startTime),
	)

	// Return appropriate error
	var resultErr error
	if len(allErrors) > 0 && !options.AllowPartialFailure {
		resultErr = allErrors[0]
	}

	return response, resultErr
}

// determineWriteStrategy determines how to handle a write based on health status
func (s *StarRocksWriteService) determineWriteStrategy(healthResult health.HealthResult, options WriteOptions) string {
	// Get service health components
	serviceHealth := healthResult.Components["starrocks_service"]
	dataHealth := healthResult.Components["starrocks_data"]

	// If data is healthy but service is degraded, we can still write
	if dataHealth != nil && dataHealth.Status == health.StatusHealthy &&
		serviceHealth != nil && serviceHealth.Status == health.StatusDegraded {
		return "proceed"
	}

	// If service is unhealthy, we can't write
	if serviceHealth != nil && serviceHealth.Status == health.StatusUnhealthy {
		return "reject"
	}

	// If data is degraded, we can buffer writes
	if dataHealth != nil && dataHealth.Status == health.StatusDegraded {
		return "buffer"
	}

	// If data is unhealthy, we can't write
	if dataHealth != nil && dataHealth.Status == health.StatusUnhealthy {
		return "reject"
	}

	// Default to buffering if we're unsure
	return "buffer"
}

// validateTransaction validates a transaction
func (s *StarRocksWriteService) validateTransaction(ctx context.Context, transactionID string) error {
	// Check if transaction exists
	txInfoAny, ok := s.transactions.Load(transactionID)
	if !ok {
		return fmt.Errorf("transaction not found: %s", transactionID)
	}

	txInfo := txInfoAny.(*domain.TransactionInfo)

	// Check transaction status
	if txInfo.Status != domain.TransactionStatusActive {
		return fmt.Errorf("transaction is not active: %s (status: %s)", transactionID, txInfo.Status)
	}

	// Check if transaction has timed out
	if txInfo.Timeout > 0 && time.Since(txInfo.StartTime) > txInfo.Timeout {
		txInfo.Status = domain.TransactionStatusTimedOut
		s.transactions.Store(transactionID, txInfo)
		return fmt.Errorf("transaction timed out: %s", transactionID)
	}

	return nil
}

// applyPreprocessors applies all preprocessors to the data
func (s *StarRocksWriteService) applyPreprocessors(
	ctx context.Context,
	data interface{},
	options WriteOptions,
) (interface{}, error) {
	result := data

	// Apply each preprocessor in sequence
	for _, preprocessor := range options.Preprocessors {
		var err error
		result, err = preprocessor.Process(ctx, result, options)
		if err != nil {
			return nil, fmt.Errorf("preprocessor %s failed: %w", preprocessor.Name(), err)
		}
	}

	return result, nil
}

// validateData validates the data using all validators
func (s *StarRocksWriteService) validateData(
	ctx context.Context,
	data interface{},
	options WriteOptions,
) error {
	// Apply each validator
	for _, validator := range options.Validators {
		if err := validator.Validate(ctx, data, options); err != nil {
			return fmt.Errorf("validator %s failed: %w", validator.Name(), err)
		}
	}

	return nil
}

// convertDataForClient converts data to the format expected by the client
func (s *StarRocksWriteService) convertDataForClient(data interface{}, options WriteOptions) (interface{}, error) {
	// Handle different formats
	switch options.Format {
	case WriteFormatJson:
		// The client expects a JSON string or byte array
		return write.ConvertToJSON(data)

	case WriteFormatCsv:
		// The client expects a CSV string or byte array
		return write.ConvertToCSV(data, options.Columns)

	case WriteFormatAvro:
		// The client expects Avro binary data
		return write.ConvertToAvro(data, options.Database, options.Table)

	case WriteFormatSql:
		// The client expects an SQL string
		return write.ConvertToSQL(data, options.Database, options.Table, options.Columns, string(options.Operation))

	case WriteFormatRows:
		// The client expects row data in a specific format
		return write.ConvertToRows(data, options.Columns)

	default:
		return nil, fmt.Errorf("unsupported format: %s", options.Format)
	}
}

// prepareClientOptions prepares options for the StarRocks client
func (s *StarRocksWriteService) prepareClientOptions(options WriteOptions, tabletInfo *tablet.TabletInfo) clients.WriteOptions {
	clientOptions := clients.WriteOptions{
		Database:    options.Database,
		Table:       options.Table,
		User:        options.User,
		Timeout:     options.Timeout,
		Format:      string(options.Format),
		Operation:   string(options.Operation),
		Columns:     options.Columns,
		Transaction: options.TransactionID != "",
	}

	// Add tablet information if available
	if tabletInfo != nil {
		clientOptions.TabletID = tabletInfo.TabletID
		clientOptions.TargetNode = tabletInfo.LeaderNode
	}

	return clientOptions
}

// convertToSlice attempts to convert data to a slice
func (s *StarRocksWriteService) convertToSlice(data interface{}) ([]interface{}, bool) {
	// Check if data is already a slice
	switch v := data.(type) {
	case []interface{}:
		return v, true
	case []map[string]interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = item
		}
		return result, true
	}

	// Try to use reflection for other slice types
	val := reflect.ValueOf(data)
	if val.Kind() == reflect.Slice || val.Kind() == reflect.Array {
		result := make([]interface{}, val.Len())
		for i := 0; i < val.Len(); i++ {
			result[i] = val.Index(i).Interface()
		}
		return result, true
	}

	return nil, false
}

// startPeriodicBufferFlush starts a goroutine for periodic buffer flushing
func (s *StarRocksWriteService) startPeriodicBufferFlush() {
	flushInterval := s.Config.Write.BufferFlushInterval
	if flushInterval <= 0 {
		flushInterval = 10 * time.Second
	}

	go func() {
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Create a background context for flushing
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

				s.Logger.Debug("Performing periodic buffer flush")
				_, err := s.FlushBufferedWrites(ctx)
				if err != nil {
					s.Logger.Error("Periodic buffer flush failed", "error", err)
				}

				cancel()
			}
		}
	}()

	s.Logger.Info("Started periodic buffer flush", "interval", flushInterval)
}

// scheduleBufferFlush schedules a flush for a specific write after a timeout
func (s *StarRocksWriteService) scheduleBufferFlush(writeID string, timeout time.Duration) {
	// Check if we already have a scheduled flush for this write
	if _, exists := s.scheduledFlushes.Load(writeID); exists {
		return
	}

	// Mark as scheduled
	s.scheduledFlushes.Store(writeID, true)

	// Schedule the flush
	go func() {
		// Wait for the timeout
		time.Sleep(timeout)

		// Remove from scheduled flushes
		s.scheduledFlushes.Delete(writeID)

		// Create a background context for flushing
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Flush this specific write
		s.Logger.Debug("Flushing buffered write after timeout", "write_id", writeID)
		err := s.WriteBuffer.FlushByID(ctx, writeID)
		if err != nil {
			s.Logger.Error("Failed to flush buffered write after timeout",
				"write_id", writeID,
				"error", err,
			)
		}
	}()
}

// recordWriteMetrics records metrics for a write operation
func (s *StarRocksWriteService) recordWriteMetrics(
	operation string,
	status string,
	database string,
	table string,
	rowsAffected int64,
	duration time.Duration,
) {
	if s.writeHistogram != nil {
		s.writeHistogram.WithLabelValues(operation, status, database, table).Observe(duration.Seconds())
	}

	if s.writeCounter != nil {
		s.writeCounter.WithLabelValues(operation, status, database, table).Inc()
	}

	if s.rowsWrittenCounter != nil && rowsAffected > 0 {
		s.rowsWrittenCounter.WithLabelValues(operation, database, table).Add(float64(rowsAffected))
	}

	// Also record through the metrics recorder interface if available
	if s.Metrics != nil {
		s.Metrics.HistogramObserve("write_execution_time_ms",
			float64(duration.Milliseconds()),
			map[string]string{
				"operation": operation,
				"status":    status,
				"database":  database,
				"table":     table,
			})

		s.Metrics.CounterInc("writes_total", map[string]string{
			"operation": operation,
			"status":    status,
			"database":  database,
			"table":     table,
		})

		if rowsAffected > 0 {
			s.Metrics.CounterAdd("rows_written_total",
				float64(rowsAffected),
				map[string]string{
					"operation": operation,
					"database":  database,
					"table":     table,
				})
		}
	}
}

// generateWriteID generates a unique write ID
func generateWriteID() string {
	return fmt.Sprintf("w-%d-%x", time.Now().UnixNano(), randomBytes(4))
}

// generateTransactionID generates a unique transaction ID
func generateTransactionID() string {
	return fmt.Sprintf("tx-%d-%x", time.Now().UnixNano(), randomBytes(4))
}

// DefaultRetryManager implements a simple retry manager
type DefaultRetryManager struct {
	// MaxRetries is the maximum number of retries
	MaxRetries int

	// BaseDelay is the base delay between retries
	BaseDelay time.Duration

	// MaxDelay is the maximum delay between retries
	MaxDelay time.Duration

	// RetryableErrors are error patterns that can be retried
	RetryableErrors []string

	// Logger is for logging
	Logger logging.Logger

	// RetryCounter counts retries by error type
	RetryCounter *prometheus.CounterVec
}

// NewDefaultRetryManager creates a new DefaultRetryManager
func NewDefaultRetryManager(
	maxRetries int,
	baseDelay time.Duration,
	maxDelay time.Duration,
	logger logging.Logger,
) *DefaultRetryManager {
	return &DefaultRetryManager{
		MaxRetries: maxRetries,
		BaseDelay:  baseDelay,
		MaxDelay:   maxDelay,
		RetryableErrors: []string{
			"connection reset",
			"timeout",
			"temporary",
			"retriable",
			"overload",
			"too many connections",
			"try again",
			"resource temporarily unavailable",
		},
		Logger: logger,
		RetryCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "write_retries_total",
				Help: "Total number of write retries",
			},
			[]string{"error_type"},
		),
	}
}

// ShouldRetry determines if a write should be retried
func (r *DefaultRetryManager) ShouldRetry(
	ctx context.Context,
	request WriteRequest,
	attempt int,
	err error,
) bool {
	// Check if we've reached the maximum retries
	if attempt >= r.MaxRetries {
		return false
	}

	// Check if the context is cancelled
	if ctx.Err() != nil {
		return false
	}

	// Check if the error is retryable
	if err == nil {
		return false
	}

	errStr := err.Error()
	for _, retryableErr := range r.RetryableErrors {
		if strings.Contains(strings.ToLower(errStr), strings.ToLower(retryableErr)) {
			// Record retry
			r.RetryCounter.WithLabelValues(retryableErr).Inc()
			return true
		}
	}

	return false
}

// NextRetryDelay returns the delay before the next retry
func (r *DefaultRetryManager) NextRetryDelay(
	ctx context.Context,
	request WriteRequest,
	attempt int,
) time.Duration {
	// Use exponential backoff with jitter
	delay := r.BaseDelay * time.Duration(1<<uint(attempt))
	if delay > r.MaxDelay {
		delay = r.MaxDelay
	}

	// Add jitter (±20%)
	jitter := time.Duration(float64(delay) * (0.8 + 0.4*rand.Float64()))

	return jitter
}

// RecordSuccess records a successful write
func (r *DefaultRetryManager) RecordSuccess(
	ctx context.Context,
	request WriteRequest,
) {
	// Nothing to do for simple implementation
}

// RecordFailure records a failed write
func (r *DefaultRetryManager) RecordFailure(
	ctx context.Context,
	request WriteRequest,
	err error,
) {
	// Nothing to do for simple implementation
}
