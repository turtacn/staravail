// Package batch provides functionality for managing and processing data in batches.
package batch

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/turtacn/staravail/internal/common/logger"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/common/types/model"
	"github.com/turtacn/staravail/internal/infrastructure/config"
)

// BatchProcessingStatus represents the current status of batch processing
type BatchProcessingStatus string

const (
	// BatchProcessingStatusIdle indicates that the processor is idle
	BatchProcessingStatusIdle BatchProcessingStatus = "idle"

	// BatchProcessingStatusStarting indicates that the processor is starting
	BatchProcessingStatusStarting BatchProcessingStatus = "starting"

	// BatchProcessingStatusRunning indicates that the processor is running
	BatchProcessingStatusRunning BatchProcessingStatus = "running"

	// BatchProcessingStatusStopping indicates that the processor is stopping
	BatchProcessingStatusStopping BatchProcessingStatus = "stopping"

	// BatchProcessingStatusStopped indicates that the processor is stopped
	BatchProcessingStatusStopped BatchProcessingStatus = "stopped"

	// BatchProcessingStatusError indicates that the processor encountered an error
	BatchProcessingStatusError BatchProcessingStatus = "error"
)

// BatchProcessingStatistics contains statistics about batch processing
type BatchProcessingStatistics struct {
	// TotalBatchesProcessed is the total number of batches processed
	TotalBatchesProcessed int64

	// TotalMessagesProcessed is the total number of messages processed
	TotalMessagesProcessed int64

	// TotalBytesProcessed is the total number of bytes processed
	TotalBytesProcessed int64

	// TotalProcessingTime is the total time spent processing batches
	TotalProcessingTime time.Duration

	// TotalSuccessfulBatches is the total number of successful batches
	TotalSuccessfulBatches int64

	// TotalFailedBatches is the total number of failed batches
	TotalFailedBatches int64

	// TotalRetries is the total number of retries
	TotalRetries int64

	// TotalDeadLetterMessages is the total number of messages sent to dead letter queue
	TotalDeadLetterMessages int64

	// AverageBatchSize is the average batch size in bytes
	AverageBatchSize float64

	// AverageBatchMessageCount is the average number of messages per batch
	AverageBatchMessageCount float64

	// AverageBatchProcessingTime is the average time to process a batch
	AverageBatchProcessingTime time.Duration

	// AverageMessageProcessingTime is the average time to process a message
	AverageMessageProcessingTime time.Duration

	// AverageMessageSize is the average message size in bytes
	AverageMessageSize float64

	// ProcessingRate is the number of messages processed per second
	ProcessingRate float64

	// SuccessRate is the percentage of batches that were successful
	SuccessRate float64

	// ErrorRate is the percentage of batches that failed
	ErrorRate float64

	// LastProcessedBatchTime is when the last batch was processed
	LastProcessedBatchTime time.Time

	// LastErrorTime is when the last error occurred
	LastErrorTime time.Time

	// LastError is the last error that occurred
	LastError error

	// CurrentBacklogCount is the current number of messages waiting to be processed
	CurrentBacklogCount int64

	// CurrentBacklogSize is the current size of messages waiting to be processed
	CurrentBacklogSize int64
}

// FlowControlStatus represents the current status of flow control
type FlowControlStatus struct {
	// IsThrottling indicates if flow control is currently throttling
	IsThrottling bool

	// CurrentThrottleRatio is the current throttle ratio (0-1)
	CurrentThrottleRatio float64

	// CurrentCompactionScore is the current compaction score
	CurrentCompactionScore float64

	// CurrentCPUUsage is the current CPU usage
	CurrentCPUUsage float64

	// CurrentMemoryUsage is the current memory usage
	CurrentMemoryUsage float64

	// CurrentIOUsage is the current IO usage
	CurrentIOUsage float64

	// CurrentWorkerUtilization is the worker pool utilization
	CurrentWorkerUtilization float64

	// CurrentBacklogRatio is the current backlog ratio
	CurrentBacklogRatio float64

	// LastThrottleAdjustmentTime is when flow control was last adjusted
	LastThrottleAdjustmentTime time.Time

	// ThrottleDuration is how long throttling has been active
	ThrottleDuration time.Duration
}

// BatchControllerConfig contains configuration for a batch controller
type BatchControllerConfig struct {
	// WorkerPoolSize is the number of worker goroutines
	WorkerPoolSize int

	// MaxConcurrentBatches is the maximum number of concurrent batches
	MaxConcurrentBatches int

	// MaxQueueSize is the maximum size of the batch queue
	MaxQueueSize int

	// BatchRetryCount is the number of times to retry a failed batch
	BatchRetryCount int

	// BatchRetryDelay is the delay between retries
	BatchRetryDelay time.Duration

	// DeadLetterEnabled controls whether dead letter queuing is enabled
	DeadLetterEnabled bool

	// FlowControlEnabled controls whether flow control is enabled
	FlowControlEnabled bool

	// CompactionScoreThreshold is the threshold for compaction score
	CompactionScoreThreshold float64

	// CPUUsageThreshold is the threshold for CPU usage
	CPUUsageThreshold float64

	// MemoryUsageThreshold is the threshold for memory usage
	MemoryUsageThreshold float64

	// IOUsageThreshold is the threshold for IO usage
	IOUsageThreshold float64

	// BacklogThreshold is the threshold for backlog
	BacklogThreshold int

	// FlowControlInterval is how often to check flow control
	FlowControlInterval time.Duration

	// StatsReportInterval is how often to report statistics
	StatsReportInterval time.Duration

	// MetricsEnabled controls whether metrics are enabled
	MetricsEnabled bool
}

// DefaultBatchControllerConfig returns the default batch controller configuration
func DefaultBatchControllerConfig() BatchControllerConfig {
	return BatchControllerConfig{
		WorkerPoolSize:           10,
		MaxConcurrentBatches:     50,
		MaxQueueSize:             1000,
		BatchRetryCount:          3,
		BatchRetryDelay:          time.Second * 5,
		DeadLetterEnabled:        true,
		FlowControlEnabled:       true,
		CompactionScoreThreshold: 80.0,
		CPUUsageThreshold:        0.8,
		MemoryUsageThreshold:     0.8,
		IOUsageThreshold:         0.8,
		BacklogThreshold:         500,
		FlowControlInterval:      time.Second * 10,
		StatsReportInterval:      time.Minute,
		MetricsEnabled:           true,
	}
}

// BatchController defines the interface for batch controllers
type BatchController interface {
	// StartProcessing starts batch processing
	StartProcessing(ctx context.Context) error

	// StopProcessing stops batch processing
	StopProcessing(ctx context.Context) error

	// GetProcessingStatus returns the current processing status
	GetProcessingStatus() (BatchProcessingStatus, error)

	// GetProcessingStatistics returns statistics about batch processing
	GetProcessingStatistics() (BatchProcessingStatistics, error)

	// GetFlowControlStatus returns the current flow control status
	GetFlowControlStatus() (FlowControlStatus, error)

	// SubmitBatch submits a batch for processing
	SubmitBatch(batch *Batch) error

	// SetConfig updates the controller configuration
	SetConfig(config BatchControllerConfig) error

	// SetFlowControlEnabled enables or disables flow control
	SetFlowControlEnabled(enabled bool) error

	// RegisterBatchEventListener registers a listener for batch events
	RegisterBatchEventListener(listener BatchEventListener) error

	// DeregisterBatchEventListener deregisters a listener for batch events
	DeregisterBatchEventListener(listener BatchEventListener) error
}

// BatchEventType represents the type of batch event
type BatchEventType string

const (
	// BatchEventSubmitted indicates that a batch was submitted
	BatchEventSubmitted BatchEventType = "submitted"

	// BatchEventProcessingStarted indicates that batch processing started
	BatchEventProcessingStarted BatchEventType = "processing_started"

	// BatchEventProcessingCompleted indicates that batch processing completed
	BatchEventProcessingCompleted BatchEventType = "processing_completed"

	// BatchEventProcessingFailed indicates that batch processing failed
	BatchEventProcessingFailed BatchEventType = "processing_failed"

	// BatchEventRetrying indicates that batch processing is being retried
	BatchEventRetrying BatchEventType = "retrying"

	// BatchEventSentToDeadLetter indicates that a batch was sent to the dead letter queue
	BatchEventSentToDeadLetter BatchEventType = "sent_to_dead_letter"
)

// BatchEvent represents an event related to batch processing
type BatchEvent struct {
	// Type is the event type
	Type BatchEventType

	// Batch is the batch that the event relates to
	Batch *Batch

	// Error is the error that occurred (if any)
	Error error

	// Timestamp is when the event occurred
	Timestamp time.Time

	// ProcessingTime is the time spent processing the batch
	ProcessingTime time.Duration

	// RetryCount is the retry count for the batch
	RetryCount int

	// AdditionalInfo contains additional information about the event
	AdditionalInfo map[string]interface{}
}

// BatchEventListener is the interface for batch event listeners
type BatchEventListener interface {
	// OnBatchEvent is called when a batch event occurs
	OnBatchEvent(event BatchEvent)
}

// BatchProcessor is the interface for batch processors
type BatchProcessor interface {
	// ProcessBatch processes a batch and returns the result
	ProcessBatch(ctx context.Context, batch *Batch) error
}

// StarRocksBatchProcessor implements BatchProcessor for StarRocks
type StarRocksBatchProcessor struct {
	// Client is the StarRocks client
	Client client.StarRocksClient

	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder

	// Config is the processor configuration
	Config StarRocksBatchProcessorConfig
}

// StarRocksBatchProcessorConfig contains configuration for StarRocksBatchProcessor
type StarRocksBatchProcessorConfig struct {
	// Format is the data format (e.g., "csv", "json")
	Format string

	// LoadProperties contains properties for the load
	LoadProperties map[string]string

	// Timeout is the timeout for the load
	Timeout time.Duration

	// MaxRowsPerLoad is the maximum number of rows per load
	MaxRowsPerLoad int

	// MaxBytesPerLoad is the maximum number of bytes per load
	MaxBytesPerLoad int64

	// StreamLoad controls whether to use stream load
	StreamLoad bool
}

// DefaultStarRocksBatchProcessorConfig returns the default StarRocksBatchProcessor configuration
func DefaultStarRocksBatchProcessorConfig() StarRocksBatchProcessorConfig {
	return StarRocksBatchProcessorConfig{
		Format: "json",
		LoadProperties: map[string]string{
			"format":            "json",
			"strip_outer_array": "true",
			"ignore_json_size":  "true",
			"fuzzy_parse":       "true",
			"max_filter_ratio":  "0.1",
			"skip_header":       "0",
			"enclose":           "\"",
			"escape":            "\\",
			"strict_mode":       "true",
			"json_root":         "",
			"json_paths":        "",
		},
		Timeout:         time.Minute * 10,
		MaxRowsPerLoad:  1000000,
		MaxBytesPerLoad: 100 * 1024 * 1024, // 100MB
		StreamLoad:      true,
	}
}

// NewStarRocksBatchProcessor creates a new StarRocksBatchProcessor
func NewStarRocksBatchProcessor(
	client client.StarRocksClient,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
	config StarRocksBatchProcessorConfig,
) *StarRocksBatchProcessor {
	return &StarRocksBatchProcessor{
		Client:  client,
		Logger:  logger,
		Metrics: metricsRecorder,
		Config:  config,
	}
}

// ProcessBatch processes a batch and returns the result
func (p *StarRocksBatchProcessor) ProcessBatch(ctx context.Context, batch *Batch) error {
	// Skip empty batches
	if batch.Size() == 0 || batch.Count() == 0 {
		p.Logger.Debug("Skipping empty batch", "batchId", batch.ID)
		return nil
	}

	// Get batch data
	data, err := batch.GetData()
	if err != nil {
		return errors.Wrap(err, "failed to get batch data")
	}

	// Get batch metadata
	metadata := batch.GetMetadata()
	database := metadata.Database
	table := metadata.Table

	// Prepare load properties
	loadProps := make(map[string]string)
	for k, v := range p.Config.LoadProperties {
		loadProps[k] = v
	}

	// Add batch-specific properties
	if metadata.Properties != nil {
		for k, v := range metadata.Properties {
			loadProps[k] = v
		}
	}

	// Record metrics before load
	if p.Metrics != nil {
		p.Metrics.CounterInc("starrocks_batch_load_attempts", map[string]string{
			"database": database,
			"table":    table,
		})
		p.Metrics.GaugeSet("starrocks_batch_load_size_bytes", float64(len(data)), map[string]string{
			"database": database,
			"table":    table,
		})
		p.Metrics.GaugeSet("starrocks_batch_load_rows", float64(batch.Count()), map[string]string{
			"database": database,
			"table":    table,
		})
	}

	// Use stream load if configured
	if p.Config.StreamLoad {
		// Create load context with timeout
		loadCtx, cancel := context.WithTimeout(ctx, p.Config.Timeout)
		defer cancel()

		// Execute stream load
		loadResp, err := p.Client.StreamLoad(loadCtx, &client.StreamLoadRequest{
			Database:   database,
			Table:      table,
			Data:       data,
			Properties: loadProps,
			Label:      fmt.Sprintf("batch-%s-%d", batch.ID, time.Now().UnixNano()),
		})

		if err != nil {
			// Record metrics for failure
			if p.Metrics != nil {
				p.Metrics.CounterInc("starrocks_batch_load_failures", map[string]string{
					"database": database,
					"table":    table,
					"reason":   "error",
				})
			}
			return errors.Wrap(err, "stream load failed")
		}

		// Check response status
		if loadResp.Status != "Success" {
			// Record metrics for failure
			if p.Metrics != nil {
				p.Metrics.CounterInc("starrocks_batch_load_failures", map[string]string{
					"database": database,
					"table":    table,
					"reason":   "status",
				})
			}
			return errors.Errorf("stream load failed: %s - %s", loadResp.Status, loadResp.Message)
		}

		// Record metrics for success
		if p.Metrics != nil {
			p.Metrics.CounterInc("starrocks_batch_load_successes", map[string]string{
				"database": database,
				"table":    table,
			})
			if loadResp.NumberLoadedRows > 0 {
				p.Metrics.GaugeSet("starrocks_batch_loaded_rows", float64(loadResp.NumberLoadedRows), map[string]string{
					"database": database,
					"table":    table,
				})
			}
			if loadResp.NumberFilteredRows > 0 {
				p.Metrics.GaugeSet("starrocks_batch_filtered_rows", float64(loadResp.NumberFilteredRows), map[string]string{
					"database": database,
					"table":    table,
				})
			}
			if loadResp.LoadBytes > 0 {
				p.Metrics.GaugeSet("starrocks_batch_loaded_bytes", float64(loadResp.LoadBytes), map[string]string{
					"database": database,
					"table":    table,
				})
			}
			if loadResp.LoadTimeMs > 0 {
				p.Metrics.GaugeSet("starrocks_batch_load_time_ms", float64(loadResp.LoadTimeMs), map[string]string{
					"database": database,
					"table":    table,
				})
			}
		}

		p.Logger.Info("Batch processed successfully",
			"batchId", batch.ID,
			"database", database,
			"table", table,
			"rows", batch.Count(),
			"bytes", len(data),
			"loadedRows", loadResp.NumberLoadedRows,
			"filteredRows", loadResp.NumberFilteredRows,
			"timeMs", loadResp.LoadTimeMs)

		return nil
	} else {
		// Use non-stream load (HTTP PUT)
		// Create load context with timeout
		loadCtx, cancel := context.WithTimeout(ctx, p.Config.Timeout)
		defer cancel()

		// Execute load
		loadResp, err := p.Client.Load(loadCtx, &client.LoadRequest{
			Database:   database,
			Table:      table,
			Data:       data,
			Format:     p.Config.Format,
			Properties: loadProps,
			Label:      fmt.Sprintf("batch-%s-%d", batch.ID, time.Now().UnixNano()),
		})

		if err != nil {
			// Record metrics for failure
			if p.Metrics != nil {
				p.Metrics.CounterInc("starrocks_batch_load_failures", map[string]string{
					"database": database,
					"table":    table,
					"reason":   "error",
				})
			}
			return errors.Wrap(err, "load failed")
		}

		// Check response status
		if loadResp.Status != "Success" {
			// Record metrics for failure
			if p.Metrics != nil {
				p.Metrics.CounterInc("starrocks_batch_load_failures", map[string]string{
					"database": database,
					"table":    table,
					"reason":   "status",
				})
			}
			return errors.Errorf("load failed: %s - %s", loadResp.Status, loadResp.Message)
		}

		// Record metrics for success
		if p.Metrics != nil {
			p.Metrics.CounterInc("starrocks_batch_load_successes", map[string]string{
				"database": database,
				"table":    table,
			})
		}

		p.Logger.Info("Batch processed successfully",
			"batchId", batch.ID,
			"database", database,
			"table", table,
			"rows", batch.Count(),
			"bytes", len(data))

		return nil
	}
}

// StarRocksCompactionMonitor monitors StarRocks compaction
type StarRocksCompactionMonitor struct {
	// Client is the StarRocks client
	Client client.StarRocksClient

	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder

	// UpdateInterval is how often to update metrics
	UpdateInterval time.Duration

	// CurrentCompactionScore is the current compaction score
	CurrentCompactionScore float64

	// CurrentTabletDistribution is the distribution of tablets by compaction score
	CurrentTabletDistribution map[string]int

	// Context for cancellation
	ctx context.Context

	// Cancel function
	cancel context.CancelFunc

	// Wait group for background tasks
	wg sync.WaitGroup

	// Mutex protects concurrent access
	mu sync.RWMutex

	// Running indicates if the monitor is running
	running bool
}

// NewStarRocksCompactionMonitor creates a new StarRocksCompactionMonitor
func NewStarRocksCompactionMonitor(
	client client.StarRocksClient,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
	updateInterval time.Duration,
) *StarRocksCompactionMonitor {
	ctx, cancel := context.WithCancel(context.Background())

	return &StarRocksCompactionMonitor{
		Client:                    client,
		Logger:                    logger,
		Metrics:                   metricsRecorder,
		UpdateInterval:            updateInterval,
		CurrentCompactionScore:    0,
		CurrentTabletDistribution: make(map[string]int),
		ctx:                       ctx,
		cancel:                    cancel,
		running:                   false,
	}
}

// Start starts the compaction monitor
func (m *StarRocksCompactionMonitor) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if already running
	if m.running {
		return nil
	}

	// Mark as running
	m.running = true

	// Start background task
	m.wg.Add(1)
	go m.monitorLoop()

	m.Logger.Info("Compaction monitor started")
	return nil
}

// Stop stops the compaction monitor
func (m *StarRocksCompactionMonitor) Stop() error {
	m.mu.Lock()
	// Check if already stopped
	if !m.running {
		m.mu.Unlock()
		return nil
	}

	// Mark as not running
	m.running = false

	// Cancel context to stop background tasks
	m.cancel()
	m.mu.Unlock()

	// Wait for background tasks to complete
	m.wg.Wait()

	m.Logger.Info("Compaction monitor stopped")
	return nil
}

// GetCompactionScore gets the current compaction score
func (m *StarRocksCompactionMonitor) GetCompactionScore() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.CurrentCompactionScore
}

// GetTabletDistribution gets the current tablet distribution
func (m *StarRocksCompactionMonitor) GetTabletDistribution() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Create a copy to avoid concurrent access issues
	distribution := make(map[string]int)
	for k, v := range m.CurrentTabletDistribution {
		distribution[k] = v
	}

	return distribution
}

// monitorLoop is the main monitoring loop
func (m *StarRocksCompactionMonitor) monitorLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.UpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.updateCompactionMetrics()
		case <-m.ctx.Done():
			return
		}
	}
}

// updateCompactionMetrics updates compaction metrics
func (m *StarRocksCompactionMonitor) updateCompactionMetrics() {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(m.ctx, time.Minute)
	defer cancel()

	// Get compaction status
	compactionStatus, err := m.Client.GetCompactionStatus(ctx)
	if err != nil {
		m.Logger.Warn("Failed to get compaction status", "error", err)
		return
	}

	// Update compaction score
	m.mu.Lock()
	m.CurrentCompactionScore = compactionStatus.Score
	m.CurrentTabletDistribution = compactionStatus.TabletDistribution
	m.mu.Unlock()

	// Record metrics
	if m.Metrics != nil {
		m.Metrics.GaugeSet("starrocks_compaction_score", compactionStatus.Score, nil)

		for scoreRange, count := range compactionStatus.TabletDistribution {
			m.Metrics.GaugeSet("starrocks_compaction_tablets", float64(count), map[string]string{
				"score_range": scoreRange,
			})
		}
	}

	m.Logger.Debug("Updated compaction metrics",
		"score", compactionStatus.Score,
		"distribution", compactionStatus.TabletDistribution)
}

// DefaultBatchController implements the BatchController interface
type DefaultBatchController struct {
	// Config is the controller configuration
	Config BatchControllerConfig

	// Processor is the batch processor
	Processor BatchProcessor

	// Client is the StarRocks client
	StarRocksClient client.StarRocksClient

	// CompactionMonitor monitors StarRocks compaction
	CompactionMonitor *StarRocksCompactionMonitor

	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder

	// BatchQueue is the queue of batches to process
	BatchQueue chan *BatchWithContext

	// CurrentStatus is the current processing status
	CurrentStatus BatchProcessingStatus

	// CurrentStatistics is the current processing statistics
	CurrentStatistics BatchProcessingStatistics

	// CurrentFlowControlStatus is the current flow control status
	CurrentFlowControlStatus FlowControlStatus

	// FlowControlEnabled controls whether flow control is enabled
	FlowControlEnabled bool

	// BatchEventListeners are the registered batch event listeners
	BatchEventListeners []BatchEventListener

	// DeadLetterQueue is the queue for batches that failed processing
	DeadLetterQueue chan *Batch

	// Context for cancellation
	ctx context.Context

	// Cancel function
	cancel context.CancelFunc

	// Wait group for background tasks
	wg sync.WaitGroup

	// Semaphore for limiting concurrent batches
	semaphore chan struct{}

	// Worker pool wait group
	workerWg sync.WaitGroup

	// Mutex protects concurrent access
	mu sync.RWMutex

	// Last error that occurred
	lastError error
}

// BatchWithContext is a batch with its context
type BatchWithContext struct {
	// Batch is the batch
	Batch *Batch

	// RetryCount is the number of retries so far
	RetryCount int

	// SubmitTime is when the batch was submitted
	SubmitTime time.Time

	// Context is the context for the batch
	Context context.Context

	// Cancel function for the context
	Cancel context.CancelFunc
}

// NewDefaultBatchController creates a new DefaultBatchController
func NewDefaultBatchController(
	processor BatchProcessor,
	starRocksClient client.StarRocksClient,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
	config BatchControllerConfig,
) *DefaultBatchController {
	ctx, cancel := context.WithCancel(context.Background())

	return &DefaultBatchController{
		Config:                   config,
		Processor:                processor,
		StarRocksClient:          starRocksClient,
		Logger:                   logger,
		Metrics:                  metricsRecorder,
		BatchQueue:               make(chan *BatchWithContext, config.MaxQueueSize),
		CurrentStatus:            BatchProcessingStatusIdle,
		CurrentStatistics:        BatchProcessingStatistics{},
		CurrentFlowControlStatus: FlowControlStatus{},
		FlowControlEnabled:       config.FlowControlEnabled,
		BatchEventListeners:      make([]BatchEventListener, 0),
		DeadLetterQueue:          make(chan *Batch, config.MaxQueueSize),
		ctx:                      ctx,
		cancel:                   cancel,
		semaphore:                make(chan struct{}, config.MaxConcurrentBatches),
		lastError:                nil,
	}
}

// StartProcessing starts batch processing
func (c *DefaultBatchController) StartProcessing(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already running
	if c.CurrentStatus == BatchProcessingStatusRunning ||
		c.CurrentStatus == BatchProcessingStatusStarting {
		return nil
	}

	// Update status
	c.CurrentStatus = BatchProcessingStatusStarting

	// Start compaction monitor if not already running
	if c.CompactionMonitor == nil {
		c.CompactionMonitor = NewStarRocksCompactionMonitor(
			c.StarRocksClient,
			c.Logger,
			c.Metrics,
			c.Config.FlowControlInterval,
		)
	}

	err := c.CompactionMonitor.Start()
	if err != nil {
		c.CurrentStatus = BatchProcessingStatusError
		c.lastError = errors.Wrap(err, "failed to start compaction monitor")
		return c.lastError
	}

	// Start worker pool
	c.startWorkerPool()

	// Start flow control if enabled
	if c.FlowControlEnabled {
		c.startFlowControl()
	}

	// Start stats reporter
	c.startStatsReporter()

	// Update status
	c.CurrentStatus = BatchProcessingStatusRunning

	c.Logger.Info("Batch processing started",
		"workerPoolSize", c.Config.WorkerPoolSize,
		"maxConcurrentBatches", c.Config.MaxConcurrentBatches,
		"maxQueueSize", c.Config.MaxQueueSize,
		"flowControlEnabled", c.FlowControlEnabled)

	return nil
}

// StopProcessing stops batch processing
func (c *DefaultBatchController) StopProcessing(ctx context.Context) error {
	c.mu.Lock()

	// Check if already stopped
	if c.CurrentStatus == BatchProcessingStatusStopped ||
		c.CurrentStatus == BatchProcessingStatusStopping {
		c.mu.Unlock()
		return nil
	}

	// Update status
	c.CurrentStatus = BatchProcessingStatusStopping
	c.mu.Unlock()

	// Cancel context to stop background tasks
	c.cancel()

	// Wait for all workers to finish
	c.workerWg.Wait()

	// Wait for all background tasks to finish
	c.wg.Wait()

	// Stop compaction monitor
	if c.CompactionMonitor != nil {
		err := c.CompactionMonitor.Stop()
		if err != nil {
			c.Logger.Warn("Failed to stop compaction monitor", "error", err)
		}
	}

	c.mu.Lock()
	// Update status
	c.CurrentStatus = BatchProcessingStatusStopped
	c.mu.Unlock()

	c.Logger.Info("Batch processing stopped")

	return nil
}

// GetProcessingStatus returns the current processing status
func (c *DefaultBatchController) GetProcessingStatus() (BatchProcessingStatus, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.CurrentStatus, nil
}

// GetProcessingStatistics returns statistics about batch processing
func (c *DefaultBatchController) GetProcessingStatistics() (BatchProcessingStatistics, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Create a copy to avoid concurrent access issues
	stats := c.CurrentStatistics

	// Calculate derived statistics
	if stats.TotalBatchesProcessed > 0 {
		stats.AverageBatchSize = float64(stats.TotalBytesProcessed) / float64(stats.TotalBatchesProcessed)
		stats.AverageBatchMessageCount = float64(stats.TotalMessagesProcessed) / float64(stats.TotalBatchesProcessed)
		stats.AverageBatchProcessingTime = time.Duration(int64(stats.TotalProcessingTime) / int64(stats.TotalBatchesProcessed))
		stats.SuccessRate = float64(stats.TotalSuccessfulBatches) / float64(stats.TotalBatchesProcessed) * 100.0
		stats.ErrorRate = float64(stats.TotalFailedBatches) / float64(stats.TotalBatchesProcessed) * 100.0
	}

	if stats.TotalMessagesProcessed > 0 {
		stats.AverageMessageSize = float64(stats.TotalBytesProcessed) / float64(stats.TotalMessagesProcessed)
		stats.AverageMessageProcessingTime = time.Duration(int64(stats.TotalProcessingTime) / int64(stats.TotalMessagesProcessed))
	}

	// Calculate processing rate (messages per second)
	if stats.TotalProcessingTime > 0 {
		stats.ProcessingRate = float64(stats.TotalMessagesProcessed) / stats.TotalProcessingTime.Seconds()
	}

	// Get current backlog
	stats.CurrentBacklogCount = int64(len(c.BatchQueue))

	return stats, nil
}

// GetFlowControlStatus returns the current flow control status
func (c *DefaultBatchController) GetFlowControlStatus() (FlowControlStatus, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.CurrentFlowControlStatus, nil
}

// SubmitBatch submits a batch for processing
func (c *DefaultBatchController) SubmitBatch(batch *Batch) error {
	// Check if controller is running
	status, _ := c.GetProcessingStatus()
	if status != BatchProcessingStatusRunning {
		return errors.Errorf("batch controller is not running, current status: %s", status)
	}

	// Create batch context
	batchCtx, cancel := context.WithTimeout(c.ctx, time.Hour) // Long timeout, will be cancelled if controller stops

	// Create batch with context
	batchWithCtx := &BatchWithContext{
		Batch:      batch,
		RetryCount: 0,
		SubmitTime: time.Now(),
		Context:    batchCtx,
		Cancel:     cancel,
	}

	// Try to enqueue the batch
	select {
	case c.BatchQueue <- batchWithCtx:
		// Successfully enqueued
		c.publishBatchEvent(BatchEvent{
			Type:           BatchEventSubmitted,
			Batch:          batch,
			Timestamp:      time.Now(),
			AdditionalInfo: map[string]interface{}{"queue_length": len(c.BatchQueue)},
		})

		// Update backlog statistics
		c.mu.Lock()
		c.CurrentStatistics.CurrentBacklogCount = int64(len(c.BatchQueue))
		c.CurrentStatistics.CurrentBacklogSize += batch.Size()
		c.mu.Unlock()

		// Record metrics
		if c.Metrics != nil {
			c.Metrics.GaugeSet("batch_queue_length", float64(len(c.BatchQueue)), nil)
			c.Metrics.GaugeSet("batch_queue_size_bytes", float64(c.CurrentStatistics.CurrentBacklogSize), nil)
			c.Metrics.CounterInc("batch_submitted", map[string]string{
				"database": batch.GetMetadata().Database,
				"table":    batch.GetMetadata().Table,
			})
		}

		return nil
	case <-time.After(time.Second * 5):
		// Queue is full
		cancel() // Cancel the context to prevent leaks
		return errors.Errorf("batch queue is full (%d/%d)", len(c.BatchQueue), cap(c.BatchQueue))
	case <-c.ctx.Done():
		// Controller is stopping
		cancel() // Cancel the context to prevent leaks
		return errors.New("batch controller is stopping")
	}
}

// SetConfig updates the controller configuration
func (c *DefaultBatchController) SetConfig(config BatchControllerConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Store old config for comparison
	oldConfig := c.Config

	// Update configuration
	c.Config = config

	// Check if we need to resize the worker pool
	if oldConfig.WorkerPoolSize != config.WorkerPoolSize &&
		c.CurrentStatus == BatchProcessingStatusRunning {
		// Stop the old worker pool
		c.cancel()
		c.workerWg.Wait()

		// Create a new context for the new worker pool
		c.ctx, c.cancel = context.WithCancel(context.Background())

		// Start the new worker pool
		c.startWorkerPool()
	}

	// Update flow control status
	c.FlowControlEnabled = config.FlowControlEnabled

	// Resize channels if needed
	if oldConfig.MaxQueueSize != config.MaxQueueSize {
		// This is tricky and potentially dangerous, so we'll log a warning
		c.Logger.Warn("Queue size changed, but existing queue will not be resized",
			"oldSize", oldConfig.MaxQueueSize,
			"newSize", config.MaxQueueSize)
	}

	if oldConfig.MaxConcurrentBatches != config.MaxConcurrentBatches {
		// This is tricky and potentially dangerous, so we'll log a warning
		c.Logger.Warn("Max concurrent batches changed, but semaphore will not be resized",
			"oldSize", oldConfig.MaxConcurrentBatches,
			"newSize", config.MaxConcurrentBatches)
	}

	c.Logger.Info("Batch controller configuration updated")

	return nil
}

// SetFlowControlEnabled enables or disables flow control
func (c *DefaultBatchController) SetFlowControlEnabled(enabled bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if status is changing
	if c.FlowControlEnabled != enabled {
		c.FlowControlEnabled = enabled

		// Start flow control if enabled and controller is running
		if enabled && c.CurrentStatus == BatchProcessingStatusRunning {
			c.startFlowControl()
		}

		c.Logger.Info("Flow control status changed", "enabled", enabled)
	}

	return nil
}

// RegisterBatchEventListener registers a listener for batch events
func (c *DefaultBatchController) RegisterBatchEventListener(listener BatchEventListener) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if listener is already registered
	for _, l := range c.BatchEventListeners {
		if l == listener {
			return nil
		}
	}

	// Add listener
	c.BatchEventListeners = append(c.BatchEventListeners, listener)

	return nil
}

// DeregisterBatchEventListener deregisters a listener for batch events
func (c *DefaultBatchController) DeregisterBatchEventListener(listener BatchEventListener) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Find and remove listener
	for i, l := range c.BatchEventListeners {
		if l == listener {
			// Remove listener by replacing it with the last element and shrinking the slice
			c.BatchEventListeners[i] = c.BatchEventListeners[len(c.BatchEventListeners)-1]
			c.BatchEventListeners = c.BatchEventListeners[:len(c.BatchEventListeners)-1]
			return nil
		}
	}

	return errors.New("listener not found")
}

// startWorkerPool starts the worker pool
func (c *DefaultBatchController) startWorkerPool() {
	// Start worker goroutines
	for i := 0; i < c.Config.WorkerPoolSize; i++ {
		c.workerWg.Add(1)
		go c.workerLoop(i)
	}
}

// workerLoop is the main loop for a worker
func (c *DefaultBatchController) workerLoop(workerID int) {
	defer c.workerWg.Done()

	c.Logger.Debug("Worker started", "workerID", workerID)

	for {
		select {
		case batchWithCtx, ok := <-c.BatchQueue:
			if !ok {
				// Channel closed
				return
			}

			// Check if batch context is done
			select {
			case <-batchWithCtx.Context.Done():
				// Batch context is done, skip processing
				c.Logger.Debug("Skipping batch with expired context",
					"batchId", batchWithCtx.Batch.ID,
					"error", batchWithCtx.Context.Err())
				continue
			default:
				// Context is still valid, proceed with processing
			}

			// Acquire semaphore to limit concurrent processing
			select {
			case c.semaphore <- struct{}{}:
				// Semaphore acquired, process batch
				c.processBatch(batchWithCtx)
				<-c.semaphore // Release semaphore
			case <-c.ctx.Done():
				// Controller is stopping
				return
			}

		case <-c.ctx.Done():
			// Controller is stopping
			return
		}
	}
}

// processBatch processes a batch
func (c *DefaultBatchController) processBatch(batchWithCtx *BatchWithContext) {
	batch := batchWithCtx.Batch
	ctx := batchWithCtx.Context
	retryCount := batchWithCtx.RetryCount

	// Update backlog statistics before processing
	c.mu.Lock()
	c.CurrentStatistics.CurrentBacklogCount--
	c.CurrentStatistics.CurrentBacklogSize -= batch.Size()
	c.mu.Unlock()

	// Publish processing started event
	c.publishBatchEvent(BatchEvent{
		Type:           BatchEventProcessingStarted,
		Batch:          batch,
		Timestamp:      time.Now(),
		RetryCount:     retryCount,
		AdditionalInfo: map[string]interface{}{"worker_id": -1}, // Worker ID not available here
	})

	// Record start time
	startTime := time.Now()

	// Process batch
	err := c.Processor.ProcessBatch(ctx, batch)

	// Record processing time
	processingTime := time.Since(startTime)

	// Update statistics
	c.mu.Lock()
	c.CurrentStatistics.TotalBatchesProcessed++
	c.CurrentStatistics.TotalMessagesProcessed += int64(batch.Count())
	c.CurrentStatistics.TotalBytesProcessed += batch.Size()
	c.CurrentStatistics.TotalProcessingTime += processingTime
	c.CurrentStatistics.LastProcessedBatchTime = time.Now()

	if err == nil {
		// Success
		c.CurrentStatistics.TotalSuccessfulBatches++
	} else {
		// Failure
		c.CurrentStatistics.TotalFailedBatches++
		c.CurrentStatistics.LastErrorTime = time.Now()
		c.CurrentStatistics.LastError = err
	}
	c.mu.Unlock()

	// Handle result
	if err == nil {
		// Success
		c.publishBatchEvent(BatchEvent{
			Type:           BatchEventProcessingCompleted,
			Batch:          batch,
			Timestamp:      time.Now(),
			ProcessingTime: processingTime,
			RetryCount:     retryCount,
			AdditionalInfo: map[string]interface{}{
				"messages": batch.Count(),
				"bytes":    batch.Size(),
			},
		})

		// Record metrics
		if c.Metrics != nil {
			c.Metrics.CounterInc("batch_processed", map[string]string{
				"database": batch.GetMetadata().Database,
				"table":    batch.GetMetadata().Table,
				"status":   "success",
			})
			c.Metrics.HistogramObserve("batch_processing_time_ms", float64(processingTime.Milliseconds()), map[string]string{
				"database": batch.GetMetadata().Database,
				"table":    batch.GetMetadata().Table,
				"status":   "success",
			})
		}
	} else {
		// Failure
		c.publishBatchEvent(BatchEvent{
			Type:           BatchEventProcessingFailed,
			Batch:          batch,
			Error:          err,
			Timestamp:      time.Now(),
			ProcessingTime: processingTime,
			RetryCount:     retryCount,
			AdditionalInfo: map[string]interface{}{
				"messages": batch.Count(),
				"bytes":    batch.Size(),
			},
		})

		// Record metrics
		if c.Metrics != nil {
			c.Metrics.CounterInc("batch_processed", map[string]string{
				"database": batch.GetMetadata().Database,
				"table":    batch.GetMetadata().Table,
				"status":   "failure",
			})
			c.Metrics.HistogramObserve("batch_processing_time_ms", float64(processingTime.Milliseconds()), map[string]string{
				"database": batch.GetMetadata().Database,
				"table":    batch.GetMetadata().Table,
				"status":   "failure",
			})
		}

		// Check if we should retry
		if retryCount < c.Config.BatchRetryCount {
			// Retry
			c.publishBatchEvent(BatchEvent{
				Type:       BatchEventRetrying,
				Batch:      batch,
				Error:      err,
				Timestamp:  time.Now(),
				RetryCount: retryCount + 1,
				AdditionalInfo: map[string]interface{}{
					"delay_ms": c.Config.BatchRetryDelay.Milliseconds(),
				},
			})

			// Update retry statistics
			c.mu.Lock()
			c.CurrentStatistics.TotalRetries++
			c.mu.Unlock()

			// Schedule retry after delay
			go func() {
				select {
				case <-time.After(c.Config.BatchRetryDelay):
					// Create new batch context
					newBatchCtx, cancel := context.WithTimeout(c.ctx, time.Hour)

					// Submit batch for retry
					err := c.SubmitBatch(batch)
					if err != nil {
						c.Logger.Warn("Failed to resubmit batch for retry",
							"error", err,
							"batchId", batch.ID,
							"retryCount", retryCount+1)

						// Send to dead letter queue if resubmission fails
						c.sendToDeadLetterQueue(batch, err)

						cancel() // Cancel the context to prevent leaks
					} else {
						// Record metrics
						if c.Metrics != nil {
							c.Metrics.CounterInc("batch_retried", map[string]string{
								"database": batch.GetMetadata().Database,
								"table":    batch.GetMetadata().Table,
								"retry":    fmt.Sprintf("%d", retryCount+1),
							})
						}
					}
				case <-c.ctx.Done():
					// Controller is stopping
					return
				}
			}()
		} else {
			// Max retries reached, send to dead letter queue
			c.sendToDeadLetterQueue(batch, err)
		}
	}
}

// sendToDeadLetterQueue sends a batch to the dead letter queue
func (c *DefaultBatchController) sendToDeadLetterQueue(batch *Batch, err error) {
	// Check if dead letter queue is enabled
	if !c.Config.DeadLetterEnabled {
		c.Logger.Warn("Discarding failed batch (dead letter queue disabled)",
			"batchId", batch.ID,
			"error", err)
		return
	}

	// Try to send to dead letter queue
	select {
	case c.DeadLetterQueue <- batch:
		// Successfully sent to dead letter queue
		c.publishBatchEvent(BatchEvent{
			Type:      BatchEventSentToDeadLetter,
			Batch:     batch,
			Error:     err,
			Timestamp: time.Now(),
			AdditionalInfo: map[string]interface{}{
				"queue_length": len(c.DeadLetterQueue),
			},
		})

		// Update dead letter statistics
		c.mu.Lock()
		c.CurrentStatistics.TotalDeadLetterMessages += int64(batch.Count())
		c.mu.Unlock()

		// Record metrics
		if c.Metrics != nil {
			c.Metrics.CounterInc("batch_sent_to_dead_letter", map[string]string{
				"database": batch.GetMetadata().Database,
				"table":    batch.GetMetadata().Table,
			})
			c.Metrics.GaugeSet("dead_letter_queue_length", float64(len(c.DeadLetterQueue)), nil)
		}

		c.Logger.Warn("Batch sent to dead letter queue",
			"batchId", batch.ID,
			"error", err,
			"messages", batch.Count(),
			"bytes", batch.Size())
	default:
		// Dead letter queue is full
		c.Logger.Error("Failed to send batch to dead letter queue (queue full)",
			"batchId", batch.ID,
			"error", err,
			"messages", batch.Count(),
			"bytes", batch.Size(),
			"queueSize", len(c.DeadLetterQueue),
			"queueCapacity", cap(c.DeadLetterQueue))
	}
}

// publishBatchEvent publishes a batch event to all listeners
func (c *DefaultBatchController) publishBatchEvent(event BatchEvent) {
	c.mu.RLock()
	listeners := c.BatchEventListeners
	c.mu.RUnlock()

	// Notify all listeners
	for _, listener := range listeners {
		go func(l BatchEventListener, e BatchEvent) {
			defer func() {
				if r := recover(); r != nil {
					c.Logger.Error("Panic in batch event listener", "panic", r, "event", e.Type)
				}
			}()
			l.OnBatchEvent(e)
		}(listener, event)
	}
}

// startFlowControl starts the flow control mechanism
func (c *DefaultBatchController) startFlowControl() {
	c.wg.Add(1)
	go c.flowControlLoop()
}

// flowControlLoop is the main loop for flow control
func (c *DefaultBatchController) flowControlLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.Config.FlowControlInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.updateFlowControlStatus()
		case <-c.ctx.Done():
			return
		}
	}
}

// updateFlowControlStatus updates the flow control status
func (c *DefaultBatchController) updateFlowControlStatus() {
	// Skip if flow control is disabled
	if !c.FlowControlEnabled {
		return
	}

	// Get compaction score
	compactionScore := 0.0
	if c.CompactionMonitor != nil {
		compactionScore = c.CompactionMonitor.GetCompactionScore()
	}

	// Check if we should throttle based on compaction score
	throttleRatio := 0.0
	isThrottling := false

	if compactionScore > c.Config.CompactionScoreThreshold {
		// Calculate throttle ratio based on how much over threshold
		overThreshold := compactionScore - c.Config.CompactionScoreThreshold
		maxOver := 100.0 - c.Config.CompactionScoreThreshold
		throttleRatio = math.Min(1.0, overThreshold/maxOver)
		isThrottling = true
	}

	// Update flow control status
	c.mu.Lock()
	defer c.mu.Unlock()

	// Store previous throttling state
	wasThrottling := c.CurrentFlowControlStatus.IsThrottling

	// Update flow control status
	c.CurrentFlowControlStatus.IsThrottling = isThrottling
	c.CurrentFlowControlStatus.CurrentThrottleRatio = throttleRatio
	c.CurrentFlowControlStatus.CurrentCompactionScore = compactionScore

	// Update worker utilization
	c.CurrentFlowControlStatus.CurrentWorkerUtilization = float64(len(c.semaphore)) / float64(cap(c.semaphore))

	// Update backlog ratio
	maxQueueSize := c.Config.MaxQueueSize
	if maxQueueSize > 0 {
		c.CurrentFlowControlStatus.CurrentBacklogRatio = float64(len(c.BatchQueue)) / float64(maxQueueSize)
	}

	// If throttling state changed, update timestamp
	if isThrottling != wasThrottling {
		c.CurrentFlowControlStatus.LastThrottleAdjustmentTime = time.Now()

		if isThrottling {
			c.Logger.Warn("Flow control throttling activated",
				"compactionScore", compactionScore,
				"threshold", c.Config.CompactionScoreThreshold,
				"throttleRatio", throttleRatio)
		} else {
			c.Logger.Info("Flow control throttling deactivated",
				"compactionScore", compactionScore,
				"threshold", c.Config.CompactionScoreThreshold)
		}
	}

	// If throttling, update throttle duration
	if isThrottling {
		c.CurrentFlowControlStatus.ThrottleDuration = time.Since(c.CurrentFlowControlStatus.LastThrottleAdjustmentTime)
	} else {
		c.CurrentFlowControlStatus.ThrottleDuration = 0
	}

	// Record metrics
	if c.Metrics != nil {
		c.Metrics.GaugeSet("flow_control_throttle_ratio", throttleRatio, nil)
		c.Metrics.GaugeSet("flow_control_is_throttling", map[bool]float64{true: 1.0, false: 0.0}[isThrottling], nil)
		c.Metrics.GaugeSet("worker_utilization", c.CurrentFlowControlStatus.CurrentWorkerUtilization, nil)
		c.Metrics.GaugeSet("backlog_ratio", c.CurrentFlowControlStatus.CurrentBacklogRatio, nil)
	}
}

// startStatsReporter starts the statistics reporter
func (c *DefaultBatchController) startStatsReporter() {
	c.wg.Add(1)
	go c.statsReporterLoop()
}

// statsReporterLoop is the main loop for reporting statistics
func (c *DefaultBatchController) statsReporterLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.Config.StatsReportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.reportStats()
		case <-c.ctx.Done():
			return
		}
	}
}

// reportStats reports processing statistics
func (c *DefaultBatchController) reportStats() {
	// Get current statistics
	stats, err := c.GetProcessingStatistics()
	if err != nil {
		c.Logger.Warn("Failed to get processing statistics", "error", err)
		return
	}

	// Log statistics
	c.Logger.Info("Batch processing statistics",
		"batchesProcessed", stats.TotalBatchesProcessed,
		"messagesProcessed", stats.TotalMessagesProcessed,
		"bytesProcessed", stats.TotalBytesProcessed,
		"successRate", stats.SuccessRate,
		"errorRate", stats.ErrorRate,
		"avgBatchSize", stats.AverageBatchSize,
		"avgMsgCount", stats.AverageBatchMessageCount,
		"avgProcessingTimeMs", stats.AverageBatchProcessingTime.Milliseconds(),
		"processingRateMsgPerSec", stats.ProcessingRate,
		"backlogCount", stats.CurrentBacklogCount)

	// Record metrics
	if c.Metrics != nil {
		c.Metrics.GaugeSet("batch_success_rate", stats.SuccessRate, nil)
		c.Metrics.GaugeSet("batch_error_rate", stats.ErrorRate, nil)
		c.Metrics.GaugeSet("batch_avg_size_bytes", stats.AverageBatchSize, nil)
		c.Metrics.GaugeSet("batch_avg_message_count", stats.AverageBatchMessageCount, nil)
		c.Metrics.GaugeSet("batch_avg_processing_time_ms", float64(stats.AverageBatchProcessingTime.Milliseconds()), nil)
		c.Metrics.GaugeSet("batch_processing_rate_msg_per_sec", stats.ProcessingRate, nil)
	}
}

// RegisterControllerMetrics registers metrics for the batch controller
func RegisterControllerMetrics(registry metrics.MetricsRecorder) {
	// Register counters
	registry.RegisterCounter("batch_submitted", "Number of batches submitted")
	registry.RegisterCounter("batch_processed", "Number of batches processed")
	registry.RegisterCounter("batch_retried", "Number of batches retried")
	registry.RegisterCounter("batch_sent_to_dead_letter", "Number of batches sent to dead letter queue")
	registry.RegisterCounter("starrocks_batch_load_attempts", "Number of StarRocks batch load attempts")
	registry.RegisterCounter("starrocks_batch_load_successes", "Number of successful StarRocks batch loads")
	registry.RegisterCounter("starrocks_batch_load_failures", "Number of failed StarRocks batch loads")

	// Register histograms
	registry.RegisterHistogram("batch_processing_time_ms", "Batch processing time in milliseconds", []float64{10, 50, 100, 500, 1000, 5000, 10000})

	// Register gauges
	registry.RegisterGauge("batch_queue_length", "Length of batch queue")
	registry.RegisterGauge("batch_queue_size_bytes", "Size of batch queue in bytes")
	registry.RegisterGauge("dead_letter_queue_length", "Length of dead letter queue")
	registry.RegisterGauge("flow_control_throttle_ratio", "Flow control throttle ratio")
	registry.RegisterGauge("flow_control_is_throttling", "Whether flow control is throttling")
	registry.RegisterGauge("worker_utilization", "Worker pool utilization")
	registry.RegisterGauge("backlog_ratio", "Backlog ratio")
	registry.RegisterGauge("batch_success_rate", "Batch success rate")
	registry.RegisterGauge("batch_error_rate", "Batch error rate")
	registry.RegisterGauge("batch_avg_size_bytes", "Average batch size in bytes")
	registry.RegisterGauge("batch_avg_message_count", "Average number of messages per batch")
	registry.RegisterGauge("batch_avg_processing_time_ms", "Average batch processing time in milliseconds")
	registry.RegisterGauge("batch_processing_rate_msg_per_sec", "Batch processing rate in messages per second")
	registry.RegisterGauge("starrocks_compaction_score", "StarRocks compaction score")
	registry.RegisterGauge("starrocks_compaction_tablets", "StarRocks compaction tablets")
	registry.RegisterGauge("starrocks_batch_load_size_bytes", "StarRocks batch load size in bytes")
	registry.RegisterGauge("starrocks_batch_load_rows", "StarRocks batch load rows")
	registry.RegisterGauge("starrocks_batch_loaded_rows", "StarRocks loaded rows")
	registry.RegisterGauge("starrocks_batch_filtered_rows", "StarRocks filtered rows")
	registry.RegisterGauge("starrocks_batch_loaded_bytes", "StarRocks loaded bytes")
	registry.RegisterGauge("starrocks_batch_load_time_ms", "StarRocks load time in milliseconds")
}

// BatchControllerFactory creates batch controllers
type BatchControllerFactory struct {
	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder

	// StarRocksClient is the StarRocks client
	StarRocksClient client.StarRocksClient

	// DefaultConfig is the default configuration
	DefaultConfig BatchControllerConfig

	// DefaultProcessorConfig is the default processor configuration
	DefaultProcessorConfig StarRocksBatchProcessorConfig
}

// NewBatchControllerFactory creates a new batch controller factory
func NewBatchControllerFactory(
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
	starRocksClient client.StarRocksClient,
	defaultConfig BatchControllerConfig,
	defaultProcessorConfig StarRocksBatchProcessorConfig,
) *BatchControllerFactory {
	return &BatchControllerFactory{
		Logger:                 logger,
		Metrics:                metricsRecorder,
		StarRocksClient:        starRocksClient,
		DefaultConfig:          defaultConfig,
		DefaultProcessorConfig: defaultProcessorConfig,
	}
}

// CreateController creates a batch controller
func (f *BatchControllerFactory) CreateController(
	config BatchControllerConfig,
	processorConfig StarRocksBatchProcessorConfig,
) (BatchController, error) {
	// Create processor
	processor := NewStarRocksBatchProcessor(
		f.StarRocksClient,
		f.Logger,
		f.Metrics,
		processorConfig,
	)

	// Create controller
	controller := NewDefaultBatchController(
		processor,
		f.StarRocksClient,
		f.Logger,
		f.Metrics,
		config,
	)

	// Create and start compaction monitor
	controller.CompactionMonitor = NewStarRocksCompactionMonitor(
		f.StarRocksClient,
		f.Logger,
		f.Metrics,
		config.FlowControlInterval,
	)

	return controller, nil
}

// CreateDefaultController creates a controller with default configuration
func (f *BatchControllerFactory) CreateDefaultController() (BatchController, error) {
	return f.CreateController(f.DefaultConfig, f.DefaultProcessorConfig)
}

// BatchEventLogger implements BatchEventListener for logging
type BatchEventLogger struct {
	// Logger is for logging
	Logger logging.Logger

	// LogLevel is the log level to use
	LogLevel string
}

// NewBatchEventLogger creates a new batch event logger
func NewBatchEventLogger(logger logging.Logger, logLevel string) *BatchEventLogger {
	return &BatchEventLogger{
		Logger:   logger,
		LogLevel: logLevel,
	}
}

// OnBatchEvent is called when a batch event occurs
func (l *BatchEventLogger) OnBatchEvent(event BatchEvent) {
	// Get batch ID and metadata
	batchID := event.Batch.ID
	metadata := event.Batch.GetMetadata()

	// Log message based on event type
	var msg string
	switch event.Type {
	case BatchEventSubmitted:
		msg = "Batch submitted for processing"
	case BatchEventProcessingStarted:
		msg = "Batch processing started"
	case BatchEventProcessingCompleted:
		msg = "Batch processing completed successfully"
	case BatchEventProcessingFailed:
		msg = "Batch processing failed"
	case BatchEventRetrying:
		msg = "Retrying batch processing"
	case BatchEventSentToDeadLetter:
		msg = "Batch sent to dead letter queue"
	default:
		msg = fmt.Sprintf("Batch event: %s", event.Type)
	}

	// Create log fields
	fields := []interface{}{
		"batchId", batchID,
		"eventType", event.Type,
		"database", metadata.Database,
		"table", metadata.Table,
	}

	// Add timing information
	if event.ProcessingTime > 0 {
		fields = append(fields, "processingTimeMs", event.ProcessingTime.Milliseconds())
	}

	// Add error information
	if event.Error != nil {
		fields = append(fields, "error", event.Error.Error())
	}

	// Add retry information
	if event.RetryCount > 0 {
		fields = append(fields, "retryCount", event.RetryCount)
	}

	// Add message count if available
	if count := event.Batch.Count(); count > 0 {
		fields = append(fields, "messageCount", count)
	}

	// Add batch size if available
	if size := event.Batch.Size(); size > 0 {
		fields = append(fields, "batchSize", size)
	}

	// Add additional info
	for k, v := range event.AdditionalInfo {
		fields = append(fields, k, v)
	}

	// Log at appropriate level
	switch l.LogLevel {
	case "debug":
		l.Logger.Debug(msg, fields...)
	case "info":
		l.Logger.Info(msg, fields...)
	case "warn":
		l.Logger.Warn(msg, fields...)
	case "error":
		l.Logger.Error(msg, fields...)
	default:
		// Default to info level
		l.Logger.Info(msg, fields...)
	}
}

// BatchMetricsCollector implements BatchEventListener for collecting metrics
type BatchMetricsCollector struct {
	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder
}

// NewBatchMetricsCollector creates a new batch metrics collector
func NewBatchMetricsCollector(metricsRecorder metrics.MetricsRecorder) *BatchMetricsCollector {
	return &BatchMetricsCollector{
		Metrics: metricsRecorder,
	}
}

// OnBatchEvent is called when a batch event occurs
func (c *BatchMetricsCollector) OnBatchEvent(event BatchEvent) {
	// Skip if metrics recorder is nil
	if c.Metrics == nil {
		return
	}

	// Get batch metadata
	metadata := event.Batch.GetMetadata()

	// Record metrics based on event type
	switch event.Type {
	case BatchEventSubmitted:
		// Metrics are recorded by the controller

	case BatchEventProcessingStarted:
		c.Metrics.CounterInc("batch_event_processing_started", map[string]string{
			"database": metadata.Database,
			"table":    metadata.Table,
		})

	case BatchEventProcessingCompleted:
		c.Metrics.CounterInc("batch_event_processing_completed", map[string]string{
			"database": metadata.Database,
			"table":    metadata.Table,
		})
		c.Metrics.HistogramObserve("batch_event_processing_time_ms", float64(event.ProcessingTime.Milliseconds()), map[string]string{
			"database": metadata.Database,
			"table":    metadata.Table,
			"status":   "success",
		})

	case BatchEventProcessingFailed:
		c.Metrics.CounterInc("batch_event_processing_failed", map[string]string{
			"database": metadata.Database,
			"table":    metadata.Table,
		})
		c.Metrics.HistogramObserve("batch_event_processing_time_ms", float64(event.ProcessingTime.Milliseconds()), map[string]string{
			"database": metadata.Database,
			"table":    metadata.Table,
			"status":   "failure",
		})

	case BatchEventRetrying:
		c.Metrics.CounterInc("batch_event_retrying", map[string]string{
			"database": metadata.Database,
			"table":    metadata.Table,
			"retry":    fmt.Sprintf("%d", event.RetryCount),
		})

	case BatchEventSentToDeadLetter:
		c.Metrics.CounterInc("batch_event_sent_to_dead_letter", map[string]string{
			"database": metadata.Database,
			"table":    metadata.Table,
		})
	}
}

// RegisterBatchEventMetrics registers metrics for batch events
func RegisterBatchEventMetrics(registry metrics.MetricsRecorder) {
	// Register counters
	registry.RegisterCounter("batch_event_processing_started", "Number of batch processing starts")
	registry.RegisterCounter("batch_event_processing_completed", "Number of batch processing completions")
	registry.RegisterCounter("batch_event_processing_failed", "Number of batch processing failures")
	registry.RegisterCounter("batch_event_retrying", "Number of batch retries")
	registry.RegisterCounter("batch_event_sent_to_dead_letter", "Number of batches sent to dead letter queue")

	// Register histograms
	registry.RegisterHistogram("batch_event_processing_time_ms", "Batch processing time in milliseconds", []float64{10, 50, 100, 500, 1000, 5000, 10000})
}

// BatchHealthChecker monitors batch processing health
type BatchHealthChecker struct {
	// Controller is the batch controller
	Controller BatchController

	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder

	// HealthyThresholds defines thresholds for healthy operation
	HealthyThresholds struct {
		// MinSuccessRate is the minimum success rate for healthy operation
		MinSuccessRate float64

		// MaxErrorRate is the maximum error rate for healthy operation
		MaxErrorRate float64

		// MaxBacklogRatio is the maximum backlog ratio for healthy operation
		MaxBacklogRatio float64

		// MaxProcessingTime is the maximum processing time for healthy operation
		MaxProcessingTime time.Duration
	}

	// LastCheckTime is when health was last checked
	LastCheckTime time.Time

	// IsHealthy indicates if the system is healthy
	IsHealthy bool

	// HealthIssues contains current health issues
	HealthIssues []string

	// Mutex protects concurrent access
	mu sync.RWMutex
}

// NewBatchHealthChecker creates a new batch health checker
func NewBatchHealthChecker(
	controller BatchController,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) *BatchHealthChecker {
	return &BatchHealthChecker{
		Controller: controller,
		Logger:     logger,
		Metrics:    metricsRecorder,
		HealthyThresholds: struct {
			MinSuccessRate    float64
			MaxErrorRate      float64
			MaxBacklogRatio   float64
			MaxProcessingTime time.Duration
		}{
			MinSuccessRate:    95.0,
			MaxErrorRate:      5.0,
			MaxBacklogRatio:   0.8,
			MaxProcessingTime: time.Second * 10,
		},
		LastCheckTime: time.Now(),
		IsHealthy:     true,
		HealthIssues:  make([]string, 0),
	}
}

// CheckHealth checks the health of batch processing
func (c *BatchHealthChecker) CheckHealth() (bool, []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get processing status
	status, err := c.Controller.GetProcessingStatus()
	if err != nil {
		c.IsHealthy = false
		c.HealthIssues = []string{fmt.Sprintf("Failed to get processing status: %v", err)}
		return c.IsHealthy, c.HealthIssues
	}

	// Check if controller is running
	if status != BatchProcessingStatusRunning {
		c.IsHealthy = false
		c.HealthIssues = []string{fmt.Sprintf("Batch controller is not running, status: %s", status)}
		return c.IsHealthy, c.HealthIssues
	}

	// Get processing statistics
	stats, err := c.Controller.GetProcessingStatistics()
	if err != nil {
		c.IsHealthy = false
		c.HealthIssues = []string{fmt.Sprintf("Failed to get processing statistics: %v", err)}
		return c.IsHealthy, c.HealthIssues
	}

	// Get flow control status
	flowStatus, err := c.Controller.GetFlowControlStatus()
	if err != nil {
		c.IsHealthy = false
		c.HealthIssues = []string{fmt.Sprintf("Failed to get flow control status: %v", err)}
		return c.IsHealthy, c.HealthIssues
	}

	// Check health issues
	issues := make([]string, 0)

	// Check success rate
	if stats.TotalBatchesProcessed > 10 && stats.SuccessRate < c.HealthyThresholds.MinSuccessRate {
		issues = append(issues, fmt.Sprintf("Success rate too low: %.2f%% (threshold: %.2f%%)",
			stats.SuccessRate, c.HealthyThresholds.MinSuccessRate))
	}

	// Check error rate
	if stats.TotalBatchesProcessed > 10 && stats.ErrorRate > c.HealthyThresholds.MaxErrorRate {
		issues = append(issues, fmt.Sprintf("Error rate too high: %.2f%% (threshold: %.2f%%)",
			stats.ErrorRate, c.HealthyThresholds.MaxErrorRate))
	}

	// Check backlog ratio
	if flowStatus.CurrentBacklogRatio > c.HealthyThresholds.MaxBacklogRatio {
		issues = append(issues, fmt.Sprintf("Backlog ratio too high: %.2f (threshold: %.2f)",
			flowStatus.CurrentBacklogRatio, c.HealthyThresholds.MaxBacklogRatio))
	}

	// Check processing time
	if stats.TotalBatchesProcessed > 10 && stats.AverageBatchProcessingTime > c.HealthyThresholds.MaxProcessingTime {
		issues = append(issues, fmt.Sprintf("Processing time too high: %v (threshold: %v)",
			stats.AverageBatchProcessingTime, c.HealthyThresholds.MaxProcessingTime))
	}

	// Check for recent errors
	if stats.LastError != nil && time.Since(stats.LastErrorTime) < time.Minute {
		issues = append(issues, fmt.Sprintf("Recent error: %v", stats.LastError))
	}

	// Update health status
	c.IsHealthy = len(issues) == 0
	c.HealthIssues = issues
	c.LastCheckTime = time.Now()

	// Record metrics
	if c.Metrics != nil {
		c.Metrics.GaugeSet("batch_health_status", map[bool]float64{true: 1.0, false: 0.0}[c.IsHealthy], nil)
		c.Metrics.GaugeSet("batch_health_issues_count", float64(len(issues)), nil)
	}

	// Log health status
	if !c.IsHealthy {
		c.Logger.Warn("Batch processing health check failed", "issues", issues)
	}

	return c.IsHealthy, c.HealthIssues
}

// BatchDeadLetterManager manages the dead letter queue
type BatchDeadLetterManager struct {
	// Controller is the batch controller
	Controller BatchController

	// DeadLetterQueue is the dead letter queue
	DeadLetterQueue chan *Batch

	// DeadLetterHandler is the handler for dead letter batches
	DeadLetterHandler func(batch *Batch, err error) error

	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder

	// Context for cancellation
	ctx context.Context

	// Cancel function
	cancel context.CancelFunc

	// Wait group for background tasks
	wg sync.WaitGroup

	// Mutex protects concurrent access
	mu sync.RWMutex

	// Running indicates if the manager is running
	running bool
}

// NewBatchDeadLetterManager creates a new batch dead letter manager
func NewBatchDeadLetterManager(
	controller BatchController,
	deadLetterQueue chan *Batch,
	deadLetterHandler func(batch *Batch, err error) error,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) *BatchDeadLetterManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &BatchDeadLetterManager{
		Controller:        controller,
		DeadLetterQueue:   deadLetterQueue,
		DeadLetterHandler: deadLetterHandler,
		Logger:            logger,
		Metrics:           metricsRecorder,
		ctx:               ctx,
		cancel:            cancel,
		running:           false,
	}
}

// Start starts the dead letter manager
func (m *BatchDeadLetterManager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if already running
	if m.running {
		return nil
	}

	// Mark as running
	m.running = true

	// Start background task
	m.wg.Add(1)
	go m.processDeadLetters()

	m.Logger.Info("Dead letter manager started")
	return nil
}

// Stop stops the dead letter manager
func (m *BatchDeadLetterManager) Stop() error {
	m.mu.Lock()
	// Check if already stopped
	if !m.running {
		m.mu.Unlock()
		return nil
	}

	// Mark as not running
	m.running = false

	// Cancel context to stop background tasks
	m.cancel()
	m.mu.Unlock()

	// Wait for background tasks to complete
	m.wg.Wait()

	m.Logger.Info("Dead letter manager stopped")
	return nil
}

// processDeadLetters processes batches in the dead letter queue
func (m *BatchDeadLetterManager) processDeadLetters() {
	defer m.wg.Done()

	for {
		select {
		case batch, ok := <-m.DeadLetterQueue:
			if !ok {
				// Channel closed
				return
			}

			// Process dead letter batch
			m.processDeadLetterBatch(batch)

		case <-m.ctx.Done():
			// Context cancelled
			return
		}
	}
}

// processDeadLetterBatch processes a dead letter batch
func (m *BatchDeadLetterManager) processDeadLetterBatch(batch *Batch) {
	// Get batch metadata
	metadata := batch.GetMetadata()

	// Record metrics
	if m.Metrics != nil {
		m.Metrics.CounterInc("dead_letter_batch_processed", map[string]string{
			"database": metadata.Database,
			"table":    metadata.Table,
		})
	}

	// Call handler if available
	if m.DeadLetterHandler != nil {
		err := m.DeadLetterHandler(batch, nil)
		if err != nil {
			m.Logger.Error("Failed to handle dead letter batch",
				"error", err,
				"batchId", batch.ID,
				"database", metadata.Database,
				"table", metadata.Table)

			// Record metrics
			if m.Metrics != nil {
				m.Metrics.CounterInc("dead_letter_batch_handler_error", map[string]string{
					"database": metadata.Database,
					"table":    metadata.Table,
				})
			}
		} else {
			m.Logger.Info("Dead letter batch handled successfully",
				"batchId", batch.ID,
				"database", metadata.Database,
				"table", metadata.Table)

			// Record metrics
			if m.Metrics != nil {
				m.Metrics.CounterInc("dead_letter_batch_handler_success", map[string]string{
					"database": metadata.Database,
					"table":    metadata.Table,
				})
			}
		}
	} else {
		// No handler, just log
		m.Logger.Warn("No handler for dead letter batch",
			"batchId", batch.ID,
			"database", metadata.Database,
			"table", metadata.Table)
	}
}

//Personal.AI order the ending
