// Package batch provides functionality for aggregating messages into batches for efficient processing.
package batch

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/turtacn/staravail/internal/common/config"
	"github.com/turtacn/staravail/internal/common/logging"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/message"
)

// BatchAggregator defines the interface for aggregating messages into batches
type BatchAggregator interface {
	// AddMessage adds a message to the aggregator
	AddMessage(msg *message.Message) error

	// GetBatch gets a ready batch (if available)
	GetBatch() (*Batch, error)

	// IsBatchReady checks if a batch is ready for processing
	IsBatchReady() bool

	// ForceBatch forces creation of a batch even if not ready
	ForceBatch() (*Batch, error)

	// GetStats gets aggregator statistics
	GetStats() BatchAggregatorStats

	// Close closes the aggregator and releases resources
	Close() error
}

// BatchStrategy defines the strategy for batch formation
type BatchStrategy int

const (
	// BatchStrategyTime forms batches based on elapsed time
	BatchStrategyTime BatchStrategy = iota

	// BatchStrategySize forms batches based on accumulated size
	BatchStrategySize

	// BatchStrategyCount forms batches based on message count
	BatchStrategyCount

	// BatchStrategyMixed forms batches based on multiple criteria
	BatchStrategyMixed
)

// BatchStatus represents the status of a batch
type BatchStatus int

const (
	// BatchStatusBuilding indicates the batch is being built
	BatchStatusBuilding BatchStatus = iota

	// BatchStatusReady indicates the batch is ready for processing
	BatchStatusReady

	// BatchStatusProcessing indicates the batch is being processed
	BatchStatusProcessing

	// BatchStatusCompleted indicates the batch has been processed
	BatchStatusCompleted

	// BatchStatusFailed indicates batch processing failed
	BatchStatusFailed

	// BatchStatusCancelled indicates the batch was cancelled
	BatchStatusCancelled

	// BatchStatusExpired indicates the batch expired
	BatchStatusExpired
)

// Batch represents a batch of messages
type Batch struct {
	// ID is the batch identifier
	ID string

	// Messages is the collection of messages in the batch
	Messages []*message.Message

	// Target is the target table for the batch
	Target string

	// Partition is the target partition for the batch
	Partition string

	// CreatedAt is when the batch was created
	CreatedAt time.Time

	// ReadyAt is when the batch became ready
	ReadyAt time.Time

	// ProcessedAt is when the batch was processed
	ProcessedAt time.Time

	// Status is the current batch status
	Status BatchStatus

	// Size is the total size of the batch in bytes
	Size int64

	// Count is the number of messages in the batch
	Count int

	// Error holds any error during batch processing
	Error error

	// Metadata is additional batch metadata
	Metadata map[string]interface{}

	// FlushReason indicates why the batch was flushed
	FlushReason string

	// mutex protects concurrent access
	mutex sync.RWMutex
}

// BatchAggregatorStats contains statistics about the aggregator
type BatchAggregatorStats struct {
	// TotalMessages is the total number of messages processed
	TotalMessages int64

	// TotalBatches is the total number of batches created
	TotalBatches int64

	// TimeBatches is the number of batches created due to time limit
	TimeBatches int64

	// SizeBatches is the number of batches created due to size limit
	SizeBatches int64

	// CountBatches is the number of batches created due to count limit
	CountBatches int64

	// ForcedBatches is the number of batches created by force
	ForcedBatches int64

	// CurrentBatchSize is the current batch size in bytes
	CurrentBatchSize int64

	// CurrentBatchCount is the current number of messages in batch
	CurrentBatchCount int

	// CurrentBatchAge is the age of the current batch in seconds
	CurrentBatchAge float64

	// AverageBatchSize is the average batch size in bytes
	AverageBatchSize float64

	// AverageBatchCount is the average number of messages per batch
	AverageBatchCount float64

	// AverageBatchTime is the average time to fill a batch in seconds
	AverageBatchTime float64
}

// BatchConfig defines configuration for a batch aggregator
type BatchConfig struct {
	// Strategy is the batch formation strategy
	Strategy BatchStrategy

	// MaxBatchSize is the maximum size of a batch in bytes
	MaxBatchSize int64

	// MaxBatchCount is the maximum number of messages in a batch
	MaxBatchCount int

	// MaxBatchTimeMS is the maximum time to wait for a batch in milliseconds
	MaxBatchTimeMS int64

	// TargetTable is the target table for the batch
	TargetTable string

	// TargetPartition is the target partition for the batch
	TargetPartition string

	// BatchPrefix is a prefix for batch IDs
	BatchPrefix string

	// EnableMetrics enables metrics collection
	EnableMetrics bool

	// ExpireAfterMS is the time after which a batch expires
	ExpireAfterMS int64
}

// DefaultBatchAggregatorConfig returns the default configuration for a batch aggregator
func DefaultBatchAggregatorConfig() BatchConfig {
	return BatchConfig{
		Strategy:        BatchStrategyMixed,
		MaxBatchSize:    10 * 1024 * 1024, // 10MB
		MaxBatchCount:   1000,
		MaxBatchTimeMS:  10000, // 10 seconds
		BatchPrefix:     "batch",
		EnableMetrics:   true,
		ExpireAfterMS:   300000, // 5 minutes
	}
}

// DefaultBatchAggregator implements BatchAggregator using time, size, and count triggers
type DefaultBatchAggregator struct {
	// Config is the aggregator configuration
	Config BatchConfig

	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder

	// Current batch being built
	currentBatch *Batch

	// Stats tracks aggregator statistics
	Stats BatchAggregatorStats

	// Batch creation time
	batchCreationTime time.Time

	// Mutex protects concurrent access
	mutex sync.Mutex

	// Batch timer for time-based batching
	batchTimer *time.Timer

	// Context for cancellation
	ctx context.Context

	// Cancel function for stopping background tasks
	cancel context.CancelFunc

	// Whether the aggregator is closed
	closed bool

	// Background tasks wait group
	wg sync.WaitGroup
}

// NewBatchAggregator creates a new batch aggregator
func NewBatchAggregator(
	config BatchConfig,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) (BatchAggregator, error) {
	// Validate config
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = DefaultBatchAggregatorConfig().MaxBatchSize
	}
	if config.MaxBatchCount <= 0 {
		config.MaxBatchCount = DefaultBatchAggregatorConfig().MaxBatchCount
	}
	if config.MaxBatchTimeMS <= 0 {
		config.MaxBatchTimeMS = DefaultBatchAggregatorConfig().MaxBatchTimeMS
	}
	if config.ExpireAfterMS <= 0 {
		config.ExpireAfterMS = DefaultBatchAggregatorConfig().ExpireAfterMS
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())

	// Create the aggregator
	aggregator := &DefaultBatchAggregator{
		Config:  config,
		Logger:  logger,
		Metrics: metricsRecorder,
		Stats: BatchAggregatorStats{
			TotalMessages:    0,
			TotalBatches:     0,
			TimeBatches:      0,
			SizeBatches:      0,
			CountBatches:     0,
			ForcedBatches:    0,
			CurrentBatchSize: 0,
			CurrentBatchCount: 0,
		},
		ctx:    ctx,
		cancel: cancel,
		closed: false,
	}

	// Start batch timer
	aggregator.resetBatchTimer()

	// Create initial batch
	aggregator.createNewBatch()

	// Start background task to handle batch expiration
	aggregator.wg.Add(1)
	go aggregator.batchExpirationTask()

	// Register metrics if enabled
	if config.EnableMetrics {
		registerBatchMetrics(metricsRecorder)
	}

	return aggregator, nil
}

// resetBatchTimer resets the batch timer
func (a *DefaultBatchAggregator) resetBatchTimer() {
	// Cancel existing timer if any
	if a.batchTimer != nil {
		a.batchTimer.Stop()
	}

	// Create new timer
	a.batchTimer = time.NewTimer(time.Duration(a.Config.MaxBatchTimeMS) * time.Millisecond)

	// Start goroutine to handle timer
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		select {
		case <-a.batchTimer.C:
			// Time limit reached, flush batch
			a.mutex.Lock()
			defer a.mutex.Unlock()

			if a.currentBatch != nil && a.currentBatch.Status == BatchStatusBuilding && a.currentBatch.Count > 0 {
				a.currentBatch.FlushReason = "time_limit"
				a.currentBatch.Status = BatchStatusReady
				a.currentBatch.ReadyAt = time.Now()
				a.Stats.TimeBatches++

				// Record metrics
				if a.Config.EnableMetrics {
					a.Metrics.CounterInc("batch_flush_total", map[string]string{
						"reason": "time_limit",
						"target": a.Config.TargetTable,
					})
					a.Metrics.GaugeSet("batch_time_seconds", time.Since(a.batchCreationTime).Seconds(), map[string]string{
						"target": a.Config.TargetTable,
					})
				}

				a.Logger.Debug("Batch ready due to time limit",
					"batch_id", a.currentBatch.ID,
					"count", a.currentBatch.Count,
					"size", a.currentBatch.Size,
					"time_ms", time.Since(a.batchCreationTime).Milliseconds())
			}
		case <-a.ctx.Done():
			return
		}
	}()
}

// createNewBatch creates a new batch
func (a *DefaultBatchAggregator) createNewBatch() {
	// Generate batch ID
	batchID := generateBatchID(a.Config.BatchPrefix)

	// Create new batch
	a.currentBatch = &Batch{
		ID:        batchID,
		Messages:  make([]*message.Message, 0, a.Config.MaxBatchCount),
		Target:    a.Config.TargetTable,
		Partition: a.Config.TargetPartition,
		CreatedAt: time.Now(),
		Status:    BatchStatusBuilding,
		Size:      0,
		Count:     0,
		Metadata:  make(map[string]interface{}),
	}

	// Record creation time
	a.batchCreationTime = time.Now()

	// Reset stats for current batch
	a.Stats.CurrentBatchSize = 0
	a.Stats.CurrentBatchCount = 0
	a.Stats.CurrentBatchAge = 0

	// Record metrics
	if a.Config.EnableMetrics {
		a.Metrics.GaugeSet("batch_current_count", float64(a.Stats.CurrentBatchCount), map[string]string{
			"target": a.Config.TargetTable,
		})
		a.Metrics.GaugeSet("batch_current_size", float64(a.Stats.CurrentBatchSize), map[string]string{
			"target": a.Config.TargetTable,
		})
	}

	// Reset batch timer
	a.resetBatchTimer()

	a.Logger.Debug("Created new batch", "batch_id", batchID, "target", a.Config.TargetTable)
}

// AddMessage adds a message to the aggregator
func (a *DefaultBatchAggregator) AddMessage(msg *message.Message) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Check if closed
	if a.closed {
		return errors.New("aggregator closed")
	}

	// Check if current batch is ready or being processed
	if a.currentBatch == nil || a.currentBatch.Status != BatchStatusBuilding {
		a.createNewBatch()
	}

	// Get message size
	msgSize := int64(len(msg.Value))
	if msg.Key != nil {
		msgSize += int64(len(msg.Key))
	}

	// Add message to batch
	a.currentBatch.Messages = append(a.currentBatch.Messages, msg)
	a.currentBatch.Size += msgSize
	a.currentBatch.Count++

	// Update stats
	a.Stats.TotalMessages++
	a.Stats.CurrentBatchSize += msgSize
	a.Stats.CurrentBatchCount++
	a.Stats.CurrentBatchAge = time.Since(a.batchCreationTime).Seconds()

	// Record metrics
	if a.Config.EnableMetrics {
		a.Metrics.CounterInc("batch_messages_total", map[string]string{
			"target": a.Config.TargetTable,
		})
		a.Metrics.GaugeSet("batch_current_count", float64(a.Stats.CurrentBatchCount), map[string]string{
			"target": a.Config.TargetTable,
		})
		a.Metrics.GaugeSet("batch_current_size", float64(a.Stats.CurrentBatchSize), map[string]string{
			"target": a.Config.TargetTable,
		})
		a.Metrics.GaugeSet("batch_current_age_seconds", a.Stats.CurrentBatchAge, map[string]string{
			"target": a.Config.TargetTable,
		})
	}

	// Check if batch is ready based on size
	if a.Config.Strategy == BatchStrategySize || a.Config.Strategy == BatchStrategyMixed {
		if a.currentBatch.Size >= a.Config.MaxBatchSize {
			a.currentBatch.FlushReason = "size_limit"
			a.currentBatch.Status = BatchStatusReady
			a.currentBatch.ReadyAt = time.Now()
			a.Stats.SizeBatches++

			// Record metrics
			if a.Config.EnableMetrics {
				a.Metrics.CounterInc("batch_flush_total", map[string]string{
					"reason": "size_limit",
					"target": a.Config.TargetTable,
				})
			}

			a.Logger.Debug("Batch ready due to size limit",
				"batch_id", a.currentBatch.ID,
				"count", a.currentBatch.Count,
				"size", a.currentBatch.Size,
				"time_ms", time.Since(a.batchCreationTime).Milliseconds())
		}
	}

	// Check if batch is ready based on count
	if a.Config.Strategy == BatchStrategyCount || a.Config.Strategy == BatchStrategyMixed {
		if a.currentBatch.Count >= a.Config.MaxBatchCount {
			a.currentBatch.FlushReason = "count_limit"
			a.currentBatch.Status = BatchStatusReady
			a.currentBatch.ReadyAt = time.Now()
			a.Stats.CountBatches++

			// Record metrics
			if a.Config.EnableMetrics {
				a.Metrics.CounterInc("batch_flush_total", map[string]string{
					"reason": "count_limit",
					"target": a.Config.TargetTable,
				})
			}

			a.Logger.Debug("Batch ready due to count limit",
				"batch_id", a.currentBatch.ID,
				"count", a.currentBatch.Count,
				"size", a.currentBatch.Size,
				"time_ms", time.Since(a.batchCreationTime).Milliseconds())
		}
	}

	return nil
}

// GetBatch gets a ready batch (if available)
func (a *DefaultBatchAggregator) GetBatch() (*Batch, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Check if closed
	if a.closed {
		return nil, errors.New("aggregator closed")
	}

	// Check if current batch is ready
	if a.currentBatch != nil && a.currentBatch.Status == BatchStatusReady {
		// Get the batch
		batch := a.currentBatch

		// Update stats
		a.Stats.TotalBatches++

		// Calculate averages
		if a.Stats.TotalBatches > 0 {
			a.Stats.AverageBatchSize = float64(a.Stats.TotalMessages*8) / float64(a.Stats.TotalBatches) // Approximation
			a.Stats.AverageBatchCount = float64(a.Stats.TotalMessages) / float64(a.Stats.TotalBatches)
			a.Stats.AverageBatchTime = (a.Stats.AverageBatchTime*(float64(a.Stats.TotalBatches)-1) + time.Since(a.batchCreationTime).Seconds()) / float64(a.Stats.TotalBatches)
		}

		// Update batch status
		batch.Status = BatchStatusProcessing

		// Create new batch for future messages
		a.createNewBatch()

		// Record metrics
		if a.Config.EnableMetrics {
			a.Metrics.CounterInc("batch_processed_total", map[string]string{
				"target": a.Config.TargetTable,
			})
			a.Metrics.GaugeSet("batch_size_bytes", float64(batch.Size), map[string]string{
				"target": a.Config.TargetTable,
			})
			a.Metrics.GaugeSet("batch_message_count", float64(batch.Count), map[string]string{
				"target": a.Config.TargetTable,
			})
		}

		return batch, nil
	}

	return nil, nil // No batch ready
}

// IsBatchReady checks if a batch is ready for processing
func (a *DefaultBatchAggregator) IsBatchReady() bool {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Check if closed
	if a.closed {
		return false
	}

	// Check if current batch is ready
	return a.currentBatch != nil && a.currentBatch.Status == BatchStatusReady
}

// ForceBatch forces creation of a batch even if not ready
func (a *DefaultBatchAggregator) ForceBatch() (*Batch, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Check if closed
	if a.closed {
		return nil, errors.New("aggregator closed")
	}

	// Check if we have a current batch with messages
	if a.currentBatch != nil && a.currentBatch.Count > 0 {
		// Mark as ready
		a.currentBatch.FlushReason = "forced"
		a.currentBatch.Status = BatchStatusReady
		a.currentBatch.ReadyAt = time.Now()
		a.Stats.ForcedBatches++

		// Record metrics
		if a.Config.EnableMetrics {
			a.Metrics.CounterInc("batch_flush_total", map[string]string{
				"reason": "forced",
				"target": a.Config.TargetTable,
			})
		}

		a.Logger.Debug("Batch ready due to forced flush",
			"batch_id", a.currentBatch.ID,
			"count", a.currentBatch.Count,
			"size", a.currentBatch.Size,
			"time_ms", time.Since(a.batchCreationTime).Milliseconds())

		// Get the batch
		batch := a.currentBatch

		// Update stats
		a.Stats.TotalBatches++

		// Calculate averages
		if a.Stats.TotalBatches > 0 {
			a.Stats.AverageBatchSize = float64(a.Stats.TotalMessages*8) / float64(a.Stats.TotalBatches) // Approximation
			a.Stats.AverageBatchCount = float64(a.Stats.TotalMessages) / float64(a.Stats.TotalBatches)
			a.Stats.AverageBatchTime = (a.Stats.AverageBatchTime*(float64(a.Stats.TotalBatches)-1) + time.Since(a.batchCreationTime).Seconds()) / float64(a.Stats.TotalBatches)
		}

		// Update batch status
		batch.Status = BatchStatusProcessing

		// Create new batch for future messages
		a.createNewBatch()

		// Record metrics
		if a.Config.EnableMetrics {
			a.Metrics.CounterInc("batch_processed_total", map[string]string{
				"target": a.Config.TargetTable,
			})
			a.Metrics.GaugeSet("batch_size_bytes", float64(batch.Size), map[string]string{
				"target": a.Config.TargetTable,
			})
			a.Metrics.GaugeSet("batch_message_count", float64(batch.Count), map[string]string{
				"target": a.Config.TargetTable,
			})
		}

		return batch, nil
	}

	return nil, errors.New("no messages in current batch")
}

// GetStats gets aggregator statistics
func (a *DefaultBatchAggregator) GetStats() BatchAggregatorStats {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Update current batch age
	if !a.closed && a.currentBatch != nil {
		a.Stats.CurrentBatchAge = time.Since(a.batchCreationTime).Seconds()
	}

	return a.Stats
}

// Close closes the aggregator and releases resources
func (a *DefaultBatchAggregator) Close() error {
	a.mutex.Lock()

	// Check if already closed
	if a.closed {
		a.mutex.Unlock()
		return nil
	}

	// Mark as closed
	a.closed = true

	// Stop timer
	if a.batchTimer != nil {
		a.batchTimer.Stop()
	}

	// Cancel context to stop background tasks
	a.cancel()

	// Check if we have a current batch with messages
	var finalBatch *Batch
	if a.currentBatch != nil && a.currentBatch.Count > 0 && a.currentBatch.Status == BatchStatusBuilding {
		// Mark as ready
		a.currentBatch.FlushReason = "shutdown"
		a.currentBatch.Status = BatchStatusReady
		a.currentBatch.ReadyAt = time.Now()
		finalBatch = a.currentBatch
		a.Stats.ForcedBatches++

		a.Logger.Debug("Final batch ready due to shutdown",
			"batch_id", a.currentBatch.ID,
			"count", a.currentBatch.Count,
			"size", a.currentBatch.Size)
	}

	a.mutex.Unlock()

	// Wait for background tasks to complete
	a.wg.Wait()

	// Return final batch if any
	if finalBatch != nil {
		// Record metrics
		if a.Config.EnableMetrics {
			a.Metrics.CounterInc("batch_flush_total", map[string]string{
				"reason": "shutdown",
				"target": a.Config.TargetTable,
			})
		}
	}

	return nil
}

// batchExpirationTask runs in the background to handle batch expiration
func (a *DefaultBatchAggregator) batchExpirationTask() {
	defer a.wg.Done()

	// Check for expired batches every second
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			a.checkExpiredBatch()
		case <-a.ctx.Done():
			return
		}
	}
}

// checkExpiredBatch checks if the current batch has expired
func (a *DefaultBatchAggregator) checkExpiredBatch() {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Skip if closed or no current batch
	if a.closed || a.currentBatch == nil {
		return
	}

	// Check if batch has expired
	expirationTime := a.currentBatch.CreatedAt.Add(time.Duration(a.Config.ExpireAfterMS) * time.Millisecond)
	if time.Now().After(expirationTime) && a.currentBatch.Status == BatchStatusBuilding && a.currentBatch.Count > 0 {
		// Mark as expired
		a.currentBatch.FlushReason = "expired"
		a.currentBatch.Status = BatchStatusExpired
		a.Logger.Warn("Batch expired",
			"batch_id", a.currentBatch.ID,
			"count", a.currentBatch.Count,
			"size", a.currentBatch.Size,
			"age_ms", time.Since(a.currentBatch.CreatedAt).Milliseconds())

		// Record metrics
		if a.Config.EnableMetrics {
			a.Metrics.CounterInc("batch_expired_total", map[string]string{
				"target": a.Config.TargetTable,
			})
		}

		// Create new batch for future messages
		a.createNewBatch()
	}
}

// SetBatchCompleted marks a batch as completed
func SetBatchCompleted(batch *Batch) {
	batch.mutex.Lock()
	defer batch.mutex.Unlock()

	batch.Status = BatchStatusCompleted
	batch.ProcessedAt = time.Now()
}

// SetBatchFailed marks a batch as failed
func SetBatchFailed(batch *Batch, err error) {
	batch.mutex.Lock()
	defer batch.mutex.Unlock()

	batch.Status = BatchStatusFailed
	batch.Error = err
	batch.ProcessedAt = time.Now()
}

// generateBatchID generates a unique batch ID
func generateBatchID(prefix string) string {
	// Generate timestamp-based ID
	return prefix + "-" + time.Now().Format("20060102-150405-000")
}

// registerBatchMetrics registers metrics for batch aggregation
func registerBatchMetrics(registry metrics.MetricsRecorder) {
	// Register counters
	registry.RegisterCounter("batch_messages_total", "Total number of messages added to batches")
	registry.RegisterCounter("batch_processed_total", "Total number of batches processed")
	registry.RegisterCounter("batch_flush_total", "Total number of batch flushes by reason")
	registry.RegisterCounter("batch_expired_total", "Total number of expired batches")

	// Register gauges
	registry.RegisterGauge("batch_current_count", "Current number of messages in batch")
	registry.RegisterGauge("batch_current_size", "Current size of batch in bytes")
	registry.RegisterGauge("batch_current_age_seconds", "Current age of batch in seconds")
	registry.RegisterGauge("batch_size_bytes", "Size of processed batch in bytes")
	registry.RegisterGauge("batch_message_count", "Number of messages in processed batch")
	registry.RegisterGauge("batch_time_seconds", "Time taken to fill batch in seconds")
}

// TablePartitionAggregator manages multiple batch aggregators by table and partition
type TablePartitionAggregator struct {
	// Factory is the function to create aggregators
	Factory func(table, partition string) (BatchAggregator, error)

	// Aggregators stores batch aggregators by table and partition
	Aggregators map[string]BatchAggregator

	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder

	// Mutex protects concurrent access
	mutex sync.RWMutex

	// Whether the aggregator is closed
	closed bool
}

// NewTablePartitionAggregator creates a new table partition aggregator
func NewTablePartitionAggregator(
	factory func(table, partition string) (BatchAggregator, error),
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) *TablePartitionAggregator {
	return &TablePartitionAggregator{
		Factory:     factory,
		Aggregators: make(map[string]BatchAggregator),
		Logger:      logger,
		Metrics:     metricsRecorder,
		closed:      false,
	}
}

// AddMessage adds a message to the appropriate aggregator
func (a *TablePartitionAggregator) AddMessage(table, partition string, msg *message.Message) error {
	a.mutex.RLock()
	// Check if closed
	if a.closed {
		a.mutex.RUnlock()
		return errors.New("aggregator closed")
	}

	// Get key for aggregator
	key := table
	if partition != "" {
		key = table + ":" + partition
	}

	// Check if aggregator exists
	aggregator, ok := a.Aggregators[key]
	a.mutex.RUnlock()

	// If not found, create a new aggregator
	if !ok {
		a.mutex.Lock()
		// Check again in case another goroutine created it
		aggregator, ok = a.Aggregators[key]
		if !ok {
			// Create new aggregator
			var err error
			aggregator, err = a.Factory(table, partition)
			if err != nil {
				a.mutex.Unlock()
				return errors.Wrapf(err, "failed to create aggregator for %s", key)
			}
			a.Aggregators[key] = aggregator
		}
		a.mutex.Unlock()
	}

	// Add message to aggregator
	return aggregator.AddMessage(msg)
}

// GetReadyBatches gets all ready batches
func (a *TablePartitionAggregator) GetReadyBatches() ([]*Batch, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	// Check if closed
	if a.closed {
		return nil, errors.New("aggregator closed")
	}

	// Collect ready batches
	batches := make([]*Batch, 0)
	for key, aggregator := range a.Aggregators {
		if aggregator.IsBatchReady() {
			batch, err := aggregator.GetBatch()
			if err != nil {
				a.Logger.Warn("Failed to get batch", "error", err, "key", key)
				continue
			}
			if batch != nil {
				batches = append(batches, batch)
			}
		}
	}

	return batches, nil
}

// ForceBatches forces creation of batches for all aggregators
func (a *TablePartitionAggregator) ForceBatches() ([]*Batch, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	// Check if closed
	if a.closed {
		return nil, errors.New("aggregator closed")
	}

	// Force batches for all aggregators
	batches := make([]*Batch, 0)
	for key, aggregator := range a.Aggregators {
		batch, err := aggregator.ForceBatch()
		if err != nil {
			// Only log warnings for errors other than "no messages"
			if err.Error() != "no messages in current batch" {
				a.Logger.Warn("Failed to force batch", "error", err, "key", key)
			}
			continue
		}
		if batch != nil {
			batches = append(batches, batch)
		}
	}

	return batches, nil
}

// Close closes all aggregators
func (a *TablePartitionAggregator) Close() error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Check if already closed
	if a.closed {
		return nil
	}

	// Mark as closed
	a.closed = true

	// Close all aggregators
	var lastErr error
	for key, aggregator := range a.Aggregators {
		if err := aggregator.Close(); err != nil {
			a.Logger.Warn("Failed to close aggregator", "error", err, "key", key)
			lastErr = err
		}
	}

	return lastErr
}

// MultiBatchAggregator implements BatchAggregator for multiple target tables
type MultiBatchAggregator struct {
	// TableAggregator is the table partition aggregator
	TableAggregator *TablePartitionAggregator

	// TableResolver resolves the target table for a message
	TableResolver func(msg *message.Message) (string, string, error)

	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder

	// DefaultConfig is the default configuration for new aggregators
	DefaultConfig BatchConfig

	// Whether the aggregator is closed
	closed bool

	// Mutex protects concurrent access
	mutex sync.RWMutex
}

// NewMultiBatchAggregator creates a new multi-batch aggregator
func NewMultiBatchAggregator(
	defaultConfig BatchConfig,
	tableResolver func(msg *message.Message) (string, string, error),
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) (BatchAggregator, error) {
	// Create factory function for table aggregators
	factory := func(table, partition string) (BatchAggregator, error) {
		// Clone config and set target table and partition
		config := defaultConfig
		config.TargetTable = table
		config.TargetPartition = partition

		// Create aggregator
		return NewBatchAggregator(config, logger, metricsRecorder)
	}

	// Create table partition aggregator
	tableAggregator := NewTablePartitionAggregator(factory, logger, metricsRecorder)

	// Create multi-batch aggregator
	aggregator := &MultiBatchAggregator{
		TableAggregator: tableAggregator,
		TableResolver:   tableResolver,
		Logger:          logger,
		Metrics:         metricsRecorder,
		DefaultConfig:   defaultConfig,
		closed:          false,
	}

	return aggregator, nil
}

// AddMessage adds a message to the aggregator
func (a *MultiBatchAggregator) AddMessage(msg *message.Message) error {
	a.mutex.RLock()
	// Check if closed
	if a.closed {
		a.mutex.RUnlock()
		return errors.New("aggregator closed")
	}
	a.mutex.RUnlock()

	// Resolve target table and partition
	table, partition, err := a.TableResolver(msg)
	if err != nil {
		return errors.Wrap(err, "failed to resolve target table")
	}

	// Add message to appropriate aggregator
	return a.TableAggregator.AddMessage(table, partition, msg)
}

// GetBatch gets a ready batch (if available)
func (a *MultiBatchAggregator) GetBatch() (*Batch, error) {
	a.mutex.RLock()
	// Check if closed
	if a.closed {
		a.mutex.RUnlock()
		return nil, errors.New("aggregator closed")
	}
	a.mutex.RUnlock()

	// Get ready batches
	batches, err := a.TableAggregator.GetReadyBatches()
	if err != nil {
		return nil, err
	}

	// Return first batch if any
	if len(batches) > 0 {
		return batches[0], nil
	}

	return nil, nil // No batch ready
}

// IsBatchReady checks if a batch is ready for processing
func (a *MultiBatchAggregator) IsBatchReady() bool {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	// Check if closed
	if a.closed {
		return false
	}

	// Check if any aggregator has a ready batch
	batches, err := a.TableAggregator.GetReadyBatches()
	if err != nil {
		return false
	}

	return len(batches) > 0
}

// ForceBatch forces creation of a batch even if not ready
func (a *MultiBatchAggregator) ForceBatch() (*Batch, error) {
	a.mutex.RLock()
	// Check if closed
	if a.closed {
		a.mutex.RUnlock()
		return nil, errors.New("aggregator closed")
	}
	a.mutex.RUnlock()

	// Force batches for all aggregators
	batches, err := a.TableAggregator.ForceBatches()
	if err != nil {
		return nil, err
	}

	// Return first batch if any
	if len(batches) > 0 {
		return batches[0], nil
	}

	return nil, errors.New("no messages in any batch")
}

// GetStats gets aggregator statistics
func (a *MultiBatchAggregator) GetStats() BatchAggregatorStats {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	// Aggregate stats from all aggregators
	stats := BatchAggregatorStats{
		TotalMessages:     0,
		TotalBatches:      0,
		TimeBatches:       0,
		SizeBatches:       0,
		CountBatches:      0,
		ForcedBatches:     0,
		CurrentBatchSize:  0,
		CurrentBatchCount: 0,
		CurrentBatchAge:   0,
		AverageBatchSize:  0,
		AverageBatchCount: 0,
		AverageBatchTime:  0,
	}

	// Collect all stats (this is an approximation as we're not handling averages properly)
	for _, aggregator := range a.TableAggregator.Aggregators {
		aggStats := aggregator.GetStats()
		stats.TotalMessages += aggStats.TotalMessages
		stats.TotalBatches += aggStats.TotalBatches
		stats.TimeBatches += aggStats.TimeBatches
		stats.SizeBatches += aggStats.SizeBatches
		stats.CountBatches += aggStats.CountBatches
		stats.ForcedBatches += aggStats.ForcedBatches

		// Current batch stats - take the max
		if aggStats.CurrentBatchSize > stats.CurrentBatchSize {
			stats.CurrentBatchSize = aggStats.CurrentBatchSize
		}
		if aggStats.CurrentBatchCount > stats.CurrentBatchCount {
			stats.CurrentBatchCount = aggStats.CurrentBatchCount
		}
		if aggStats.CurrentBatchAge > stats.CurrentBatchAge {
			stats.CurrentBatchAge = aggStats.CurrentBatchAge
		}

		// For averages, we need weighted averages
		if aggStats.TotalBatches > 0 {
			stats.AverageBatchSize += aggStats.AverageBatchSize * float64(aggStats.TotalBatches)
			stats.AverageBatchCount += aggStats.AverageBatchCount * float64(aggStats.TotalBatches)
			stats.AverageBatchTime += aggStats.AverageBatchTime * float64(aggStats.TotalBatches)
		}
	}

	// Calculate actual averages
	if stats.TotalBatches > 0 {
		stats.AverageBatchSize /= float64(stats.TotalBatches)
		stats.AverageBatchCount /= float64(stats.TotalBatches)
		stats.AverageBatchTime /= float64(stats.TotalBatches)
	}

	return stats
}

// Close closes the aggregator and releases resources
func (a *MultiBatchAggregator) Close() error {
	a.mutex.Lock()
	// Check if already closed
	if a.closed {
		a.mutex.Unlock()
		return nil
	}

	// Mark as closed
	a.closed = true
	a.mutex.Unlock()

	// Close table aggregator
	return a.TableAggregator.Close()
}

// BatchProcessor defines the interface for processing batches
type BatchProcessor interface {
	// ProcessBatch processes a batch
	ProcessBatch(batch *Batch) error

	// Close closes the processor
	Close() error
}

// BatchProcessorFunc is a function that processes a batch
type BatchProcessorFunc func(batch *Batch) error

// BatchManager manages the batch aggregation and processing lifecycle
type BatchManager struct {
	// Aggregator is the batch aggregator
	Aggregator BatchAggregator

	// Processor is the batch processor
	Processor BatchProcessor

	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder

	// Context for cancellation
	ctx context.Context

	// Cancel function for stopping background tasks
	cancel context.CancelFunc

	// Wait group for background tasks
	wg sync.WaitGroup

	// Whether the manager is running
	running bool

	// Whether the manager is closed
	closed bool

	// Mutex protects concurrent access
	mutex sync.RWMutex

	// ProcessingInterval is how often to check for ready batches
	ProcessingInterval time.Duration

	// EnableMetrics enables metrics collection
	EnableMetrics bool
}

// NewBatchManager creates a new batch manager
func NewBatchManager(
	aggregator BatchAggregator,
	processor BatchProcessor,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) *BatchManager {
	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())

	return &BatchManager{
		Aggregator:         aggregator,
		Processor:          processor,
		Logger:             logger,
		Metrics:            metricsRecorder,
		ctx:                ctx,
		cancel:             cancel,
		running:            false,
		closed:             false,
		ProcessingInterval: 100 * time.Millisecond,
		EnableMetrics:      true,
	}
}

// Start starts the batch manager
func (m *BatchManager) Start() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Check if already running
	if m.running {
		return nil
	}

	// Check if closed
	if m.closed {
		return errors.New("manager closed")
	}

	// Mark as running
	m.running = true

	// Register metrics if enabled
	if m.EnableMetrics {
		registerBatchManagerMetrics(m.Metrics)
	}

	// Start background task
	m.wg.Add(1)
	go m.processingLoop()

	m.Logger.Info("Batch manager started")
	return nil
}

// Stop stops the batch manager
func (m *BatchManager) Stop() error {
	m.mutex.Lock()
	// Check if already stopped
	if !m.running {
		m.mutex.Unlock()
		return nil
	}

	// Mark as not running
	m.running = false
	m.mutex.Unlock()

	// Process any remaining batches
	m.processReadyBatches()

	// Force processing of any remaining messages
	m.forceBatch()

	m.Logger.Info("Batch manager stopped")
	return nil
}

// Close closes the batch manager and releases resources
func (m *BatchManager) Close() error {
	m.mutex.Lock()
	// Check if already closed
	if m.closed {
		m.mutex.Unlock()
		return nil
	}

	// Mark as closed
	m.closed = true
	m.running = false

	// Cancel context to stop background tasks
	m.cancel()
	m.mutex.Unlock()

	// Wait for background tasks to complete
	m.wg.Wait()

	// Close aggregator
	if err := m.Aggregator.Close(); err != nil {
		m.Logger.Warn("Failed to close aggregator", "error", err)
	}

	// Close processor
	if err := m.Processor.Close(); err != nil {
		m.Logger.Warn("Failed to close processor", "error", err)
	}

	m.Logger.Info("Batch manager closed")
	return nil
}

// processingLoop is the main processing loop
func (m *BatchManager) processingLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.ProcessingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if m.isRunning() {
				m.processReadyBatches()
			}
		case <-m.ctx.Done():
			return
		}
	}
}

// processReadyBatches processes all ready batches
func (m *BatchManager) processReadyBatches() {
	for {
		// Check if there's a ready batch
		if !m.Aggregator.IsBatchReady() {
			break
		}

		// Get batch
		batch, err := m.Aggregator.GetBatch()
		if err != nil {
			m.Logger.Error("Failed to get batch", "error", err)
			break
		}
		if batch == nil {
			break
		}

		// Process batch
		startTime := time.Now()
		err = m.Processor.ProcessBatch(batch)
		duration := time.Since(startTime)

		// Record metrics
		if m.EnableMetrics {
			m.Metrics.TimerRecord("batch_processing_seconds", duration.Seconds(), map[string]string{
				"target": batch.Target,
				"status": err == nil ? "success" : "error",
			})
		}

		// Update batch status
		if err != nil {
			SetBatchFailed(batch, err)
			m.Logger.Error("Failed to process batch",
				"error", err,
				"batch_id", batch.ID,
				"count", batch.Count,
				"size", batch.Size,
				"target", batch.Target)
		} else {
			SetBatchCompleted(batch)
			m.Logger.Debug("Batch processed successfully",
				"batch_id", batch.ID,
				"count", batch.Count,
				"size", batch.Size,
				"target", batch.Target,
				"duration_ms", duration.Milliseconds())
		}
	}
}

// forceBatch forces processing of any remaining messages
func (m *BatchManager) forceBatch() {
	// Try to force a batch
	batch, err := m.Aggregator.ForceBatch()
	if err != nil {
		// Only log error if it's not "no messages"
		if err.Error() != "no messages in current batch" {
			m.Logger.Warn("Failed to force batch", "error", err)
		}
		return
	}
	if batch == nil {
		return
	}

	// Process batch
	startTime := time.Now()
	err = m.Processor.ProcessBatch(batch)
	duration := time.Since(startTime)

	// Record metrics
	if m.EnableMetrics {
		m.Metrics.TimerRecord("batch_processing_seconds", duration.Seconds(), map[string]string{
			"target": batch.Target,
			"status": err == nil ? "success" : "error",
		})
	}

	// Update batch status
	if err != nil {
		SetBatchFailed(batch, err)
		m.Logger.Error("Failed to process forced batch",
			"error", err,
			"batch_id", batch.ID,
			"count", batch.Count,
			"size", batch.Size,
			"target", batch.Target)
	} else {
		SetBatchCompleted(batch)
		m.Logger.Debug("Forced batch processed successfully",
			"batch_id", batch.ID,
			"count", batch.Count,
			"size", batch.Size,
			"target", batch.Target,
			"duration_ms", duration.Milliseconds())
	}
}

// isRunning checks if the manager is running
func (m *BatchManager) isRunning() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.running
}

// registerBatchManagerMetrics registers metrics for batch manager
func registerBatchManagerMetrics(registry metrics.MetricsRecorder) {
	// Register timers
	registry.RegisterTimer("batch_processing_seconds", "Time taken to process a batch")
}
//Personal.AI order the ending
