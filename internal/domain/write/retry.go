// Package write provides functionality for buffering and retrying write operations
// to StarRocks when some tablets are unavailable.
package write

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/turtacn/staravail/internal/common/config"
	"github.com/turtacn/staravail/internal/common/logging"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/domain/metadata"
	"github.com/turtacn/staravail/internal/domain/tablet"
)

// RetryStrategy defines the strategy for retrying writes
type RetryStrategy string

const (
	// RetryStrategyLinear uses a linear backoff for retries
	RetryStrategyLinear RetryStrategy = "LINEAR"
	// RetryStrategyExponential uses an exponential backoff for retries
	RetryStrategyExponential RetryStrategy = "EXPONENTIAL"
	// RetryStrategyJittered uses a jittered backoff for retries
	RetryStrategyJittered RetryStrategy = "JITTERED"
)

// RetryTrigger defines what triggers a retry
type RetryTrigger string

const (
	// RetryTriggerScheduled indicates the retry was scheduled
	RetryTriggerScheduled RetryTrigger = "SCHEDULED"
	// RetryTriggerManual indicates the retry was triggered manually
	RetryTriggerManual RetryTrigger = "MANUAL"
	// RetryTriggerHealthRecovered indicates the retry was triggered by health recovery
	RetryTriggerHealthRecovered RetryTrigger = "HEALTH_RECOVERED"
	// RetryTriggerPeriodic indicates the retry was triggered by periodic retry
	RetryTriggerPeriodic RetryTrigger = "PERIODIC"
)

// RetryStatus represents the status of a retry
type RetryStatus string

const (
	// RetryStatusPending indicates the retry is pending
	RetryStatusPending RetryStatus = "PENDING"
	// RetryStatusInProgress indicates the retry is in progress
	RetryStatusInProgress RetryStatus = "IN_PROGRESS"
	// RetryStatusSucceeded indicates the retry succeeded
	RetryStatusSucceeded RetryStatus = "SUCCEEDED"
	// RetryStatusFailed indicates the retry failed
	RetryStatusFailed RetryStatus = "FAILED"
	// RetryStatusCancelled indicates the retry was cancelled
	RetryStatusCancelled RetryStatus = "CANCELLED"
)

// RetryInfo represents information about a retry
type RetryInfo struct {
	// ID is a unique identifier for the retry
	ID string
	// WriteID is the ID of the write being retried
	WriteID string
	// Status is the status of the retry
	Status RetryStatus
	// ScheduledAt is when the retry was scheduled
	ScheduledAt time.Time
	// StartedAt is when the retry started
	StartedAt time.Time
	// CompletedAt is when the retry completed
	CompletedAt time.Time
	// Trigger is what triggered the retry
	Trigger RetryTrigger
	// AttemptNumber is the attempt number
	AttemptNumber int
	// Error is the error encountered during the retry, if any
	Error string
	// DatabaseName is the name of the database
	DatabaseName string
	// TableName is the name of the table
	TableName string
	// PartitionName is the name of the partition
	PartitionName string
	// BucketID is the ID of the bucket
	BucketID int
}

// RetryStats represents statistics about retries
type RetryStats struct {
	// TotalRetries is the total number of retries
	TotalRetries int
	// SuccessfulRetries is the number of successful retries
	SuccessfulRetries int
	// FailedRetries is the number of failed retries
	FailedRetries int
	// PendingRetries is the number of pending retries
	PendingRetries int
	// InProgressRetries is the number of in-progress retries
	InProgressRetries int
	// CancelledRetries is the number of cancelled retries
	CancelledRetries int
	// SuccessRate is the success rate of retries
	SuccessRate float64
	// AverageRetryAttempts is the average number of retry attempts
	AverageRetryAttempts float64
	// OldestPendingRetry is the oldest pending retry
	OldestPendingRetry time.Time
	// AverageRetryTime is the average time for successful retries
	AverageRetryTime time.Duration
	// RetryCountByTable is the retry count by table
	RetryCountByTable map[string]int
}

// RetryManager defines the interface for managing retries
type RetryManager interface {
	// ScheduleRetry schedules a retry for a buffered write
	ScheduleRetry(ctx context.Context, writeID string, trigger RetryTrigger) (string, error)

	// CancelRetry cancels a scheduled retry
	CancelRetry(ctx context.Context, retryID string) error

	// GetRetryStatus gets the status of a retry
	GetRetryStatus(ctx context.Context, retryID string) (RetryInfo, error)

	// GetRetryStats gets statistics about retries
	GetRetryStats(ctx context.Context) (RetryStats, error)

	// RetryAll attempts to retry all buffered writes
	RetryAll(ctx context.Context, trigger RetryTrigger) (int, error)

	// RetryForTable attempts to retry all buffered writes for a specific table
	RetryForTable(ctx context.Context, databaseName, tableName string, trigger RetryTrigger) (int, error)

	// RetryForPartition attempts to retry all buffered writes for a specific partition
	RetryForPartition(ctx context.Context, databaseName, tableName, partitionName string, trigger RetryTrigger) (int, error)

	// Start starts the retry manager
	Start() error

	// Stop stops the retry manager
	Stop() error
}

// RetryManagerConfig represents configuration for the retry manager
type RetryManagerConfig struct {
	// MaxRetryAttempts is the maximum number of retry attempts
	MaxRetryAttempts int
	// BaseRetryIntervalSeconds is the base interval for retries in seconds
	BaseRetryIntervalSeconds int
	// MaxRetryIntervalSeconds is the maximum interval for retries in seconds
	MaxRetryIntervalSeconds int
	// RetryStrategy is the strategy for retrying writes
	RetryStrategy RetryStrategy
	// PeriodicRetryIntervalMinutes is how often to retry all writes in minutes
	PeriodicRetryIntervalMinutes int
	// RetryBatchSize is how many writes to retry in a batch
	RetryBatchSize int
	// ConcurrentRetries is how many retries to execute concurrently
	ConcurrentRetries int
	// RetryOnStartup indicates if retries should be attempted on startup
	RetryOnStartup bool
}

// RetryExecutor defines the interface for executing write operations
type RetryExecutor interface {
	// ExecuteWrite executes a write operation
	ExecuteWrite(ctx context.Context, write BufferedWrite) error
}

// RetryManagerImpl implements the RetryManager interface
type RetryManagerImpl struct {
	// config is the retry manager configuration
	config RetryManagerConfig
	// writeBuffer is used to get buffered writes
	writeBuffer WriteBuffer
	// healthChecker is used to check tablet health
	healthChecker tablet.HealthChecker
	// metadataService is used to get metadata information
	metadataService metadata.MetadataService
	// retryExecutor is used to execute writes
	retryExecutor RetryExecutor
	// logger is used for logging
	logger logging.Logger
	// metrics is used to record metrics
	metrics metrics.MetricsRecorder

	// retryInfo stores information about retries
	retryInfo map[string]RetryInfo
	// retryInfoMutex protects the retryInfo map
	retryInfoMutex sync.RWMutex
	// periodicRetryTicker is a ticker for periodic retries
	periodicRetryTicker *time.Ticker
	// healthEventCh is a channel for health events
	healthEventCh chan tablet.HealthEvent
	// stopCh is a channel for stopping the retry manager
	stopCh chan struct{}
	// running indicates if the retry manager is running
	running bool
	// runningMutex protects the running flag
	runningMutex sync.RWMutex
}

// NewRetryManager creates a new RetryManager instance
func NewRetryManager(
	cfg config.RetryManagerConfig,
	writeBuffer WriteBuffer,
	healthChecker tablet.HealthChecker,
	metadataService metadata.MetadataService,
	retryExecutor RetryExecutor,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) RetryManager {
	// Convert config to internal retry manager config
	retryConfig := RetryManagerConfig{
		MaxRetryAttempts:             cfg.MaxRetryAttempts,
		BaseRetryIntervalSeconds:     cfg.BaseRetryIntervalSeconds,
		MaxRetryIntervalSeconds:      cfg.MaxRetryIntervalSeconds,
		RetryStrategy:                RetryStrategy(cfg.RetryStrategy),
		PeriodicRetryIntervalMinutes: cfg.PeriodicRetryIntervalMinutes,
		RetryBatchSize:               cfg.RetryBatchSize,
		ConcurrentRetries:            cfg.ConcurrentRetries,
		RetryOnStartup:               cfg.RetryOnStartup,
	}

	return &RetryManagerImpl{
		config:          retryConfig,
		writeBuffer:     writeBuffer,
		healthChecker:   healthChecker,
		metadataService: metadataService,
		retryExecutor:   retryExecutor,
		logger:          logger,
		metrics:         metricsRecorder,
		retryInfo:       make(map[string]RetryInfo),
		retryInfoMutex:  sync.RWMutex{},
		healthEventCh:   make(chan tablet.HealthEvent, 100),
		stopCh:          make(chan struct{}),
		running:         false,
		runningMutex:    sync.RWMutex{},
	}
}

// ScheduleRetry schedules a retry for a buffered write
func (r *RetryManagerImpl) ScheduleRetry(ctx context.Context, writeID string, trigger RetryTrigger) (string, error) {
	// Get the buffered write
	writes, err := r.writeBuffer.GetBufferedWrites(ctx, nil, 1, 0)
	if err != nil {
		return "", errors.Wrap(err, "failed to get buffered write")
	}

	var write *BufferedWrite
	for _, w := range writes {
		if w.ID == writeID {
			write = &w
			break
		}
	}

	if write == nil {
		return "", errors.Errorf("buffered write with ID %s not found", writeID)
	}

	// Check if the write can be retried based on retry count
	if write.RetryCount >= r.config.MaxRetryAttempts {
		return "", errors.Errorf("maximum retry attempts (%d) reached for write %s", r.config.MaxRetryAttempts, writeID)
	}

	// Check if the tablet is healthy
	isHealthy, err := r.isTabletHealthy(ctx, write)
	if err != nil {
		return "", errors.Wrap(err, "failed to check tablet health")
	}

	if !isHealthy {
		return "", errors.Errorf("tablet is unhealthy for write %s", writeID)
	}

	// Generate a retry ID
	retryID := fmt.Sprintf("retry_%s_%d", writeID, time.Now().UnixNano())

	// Create retry info
	retryInfo := RetryInfo{
		ID:            retryID,
		WriteID:       writeID,
		Status:        RetryStatusPending,
		ScheduledAt:   time.Now(),
		Trigger:       trigger,
		AttemptNumber: write.RetryCount + 1,
		DatabaseName:  write.DatabaseName,
		TableName:     write.TableName,
		PartitionName: write.PartitionName,
		BucketID:      write.BucketID,
	}

	// Store retry info
	r.retryInfoMutex.Lock()
	r.retryInfo[retryID] = retryInfo
	r.retryInfoMutex.Unlock()

	// Update write status
	err = r.writeBuffer.UpdateBufferedWriteStatus(ctx, writeID, BufferedWriteStatusRetrying, nil)
	if err != nil {
		return "", errors.Wrap(err, "failed to update buffered write status")
	}

	// Schedule the retry in a goroutine
	go r.executeRetry(context.Background(), retryID)

	r.logger.Info("Scheduled retry",
		"retry_id", retryID,
		"write_id", writeID,
		"trigger", trigger,
		"attempt", retryInfo.AttemptNumber)

	return retryID, nil
}

// CancelRetry cancels a scheduled retry
func (r *RetryManagerImpl) CancelRetry(ctx context.Context, retryID string) error {
	r.retryInfoMutex.Lock()
	defer r.retryInfoMutex.Unlock()

	// Check if the retry exists
	retryInfo, exists := r.retryInfo[retryID]
	if !exists {
		return errors.Errorf("retry with ID %s not found", retryID)
	}

	// Check if the retry can be cancelled
	if retryInfo.Status != RetryStatusPending && retryInfo.Status != RetryStatusInProgress {
		return errors.Errorf("retry %s cannot be cancelled, status is %s", retryID, retryInfo.Status)
	}

	// Update retry status
	retryInfo.Status = RetryStatusCancelled
	retryInfo.CompletedAt = time.Now()
	r.retryInfo[retryID] = retryInfo

	// Update metrics
	r.metrics.CounterInc("retry_manager_cancels", map[string]string{
		"database": retryInfo.DatabaseName,
		"table":    retryInfo.TableName,
	})

	r.logger.Info("Cancelled retry",
		"retry_id", retryID,
		"write_id", retryInfo.WriteID)

	return nil
}

// GetRetryStatus gets the status of a retry
func (r *RetryManagerImpl) GetRetryStatus(ctx context.Context, retryID string) (RetryInfo, error) {
	r.retryInfoMutex.RLock()
	defer r.retryInfoMutex.RUnlock()

	// Check if the retry exists
	retryInfo, exists := r.retryInfo[retryID]
	if !exists {
		return RetryInfo{}, errors.Errorf("retry with ID %s not found", retryID)
	}

	return retryInfo, nil
}

// GetRetryStats gets statistics about retries
func (r *RetryManagerImpl) GetRetryStats(ctx context.Context) (RetryStats, error) {
	r.retryInfoMutex.RLock()
	defer r.retryInfoMutex.RUnlock()

	stats := RetryStats{
		RetryCountByTable: make(map[string]int),
	}

	totalRetryTime := time.Duration(0)
	totalRetryAttempts := 0
	oldestPending := time.Now()
	hasOldestPending := false

	// Calculate statistics
	for _, info := range r.retryInfo {
		stats.TotalRetries++

		// Count by status
		switch info.Status {
		case RetryStatusSucceeded:
			stats.SuccessfulRetries++
			// Calculate retry time for successful retries
			if !info.StartedAt.IsZero() && !info.CompletedAt.IsZero() {
				totalRetryTime += info.CompletedAt.Sub(info.StartedAt)
			}
		case RetryStatusFailed:
			stats.FailedRetries++
		case RetryStatusPending:
			stats.PendingRetries++
			if !info.ScheduledAt.IsZero() && (!hasOldestPending || info.ScheduledAt.Before(oldestPending)) {
				oldestPending = info.ScheduledAt
				hasOldestPending = true
			}
		case RetryStatusInProgress:
			stats.InProgressRetries++
		case RetryStatusCancelled:
			stats.CancelledRetries++
		}

		// Count by table
		tableKey := fmt.Sprintf("%s.%s", info.DatabaseName, info.TableName)
		stats.RetryCountByTable[tableKey]++

		// Count retry attempts
		totalRetryAttempts += info.AttemptNumber
	}

	// Calculate derived statistics
	if stats.TotalRetries > 0 {
		stats.AverageRetryAttempts = float64(totalRetryAttempts) / float64(stats.TotalRetries)
	}

	if stats.SuccessfulRetries+stats.FailedRetries > 0 {
		stats.SuccessRate = float64(stats.SuccessfulRetries) / float64(stats.SuccessfulRetries+stats.FailedRetries) * 100
	}

	if stats.SuccessfulRetries > 0 {
		stats.AverageRetryTime = totalRetryTime / time.Duration(stats.SuccessfulRetries)
	}

	if hasOldestPending {
		stats.OldestPendingRetry = oldestPending
	}

	return stats, nil
}

// RetryAll attempts to retry all buffered writes
func (r *RetryManagerImpl) RetryAll(ctx context.Context, trigger RetryTrigger) (int, error) {
	// Get buffer stats to know how many writes to process
	bufferStats, err := r.writeBuffer.GetBufferStats(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get buffer stats")
	}

	// Get all buffered writes
	writes, err := r.writeBuffer.GetBufferedWrites(ctx, nil, bufferStats.TotalBufferedWrites, 0)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get buffered writes")
	}

	scheduledCount := 0

	// Process writes in batches
	batchSize := r.config.RetryBatchSize
	if batchSize <= 0 {
		batchSize = 50 // Default batch size
	}

	for i := 0; i < len(writes); i += batchSize {
		end := i + batchSize
		if end > len(writes) {
			end = len(writes)
		}

		batch := writes[i:end]
		for _, write := range batch {
			// Skip writes that are already being retried
			if write.Status == BufferedWriteStatusRetrying {
				continue
			}

			// Check if the write can be retried based on retry count
			if write.RetryCount >= r.config.MaxRetryAttempts {
				continue
			}

			// Check if the tablet is healthy
			isHealthy, err := r.isTabletHealthy(ctx, &write)
			if err != nil {
				r.logger.Error("Failed to check tablet health",
					"write_id", write.ID,
					"error", err)
				continue
			}

			if !isHealthy {
				continue
			}

			// Schedule retry
			_, err = r.ScheduleRetry(ctx, write.ID, trigger)
			if err != nil {
				r.logger.Error("Failed to schedule retry",
					"write_id", write.ID,
					"error", err)
				continue
			}

			scheduledCount++
		}
	}

	r.logger.Info("Scheduled retries for all eligible buffered writes",
		"scheduled", scheduledCount,
		"total", len(writes),
		"trigger", trigger)

	return scheduledCount, nil
}

// RetryForTable attempts to retry all buffered writes for a specific table
func (r *RetryManagerImpl) RetryForTable(ctx context.Context, databaseName, tableName string, trigger RetryTrigger) (int, error) {
	// Get all buffered writes
	bufferStats, err := r.writeBuffer.GetBufferStats(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get buffer stats")
	}

	writes, err := r.writeBuffer.GetBufferedWrites(ctx, nil, bufferStats.TotalBufferedWrites, 0)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get buffered writes")
	}

	scheduledCount := 0

	// Filter writes for the specified table
	var tableWrites []BufferedWrite
	for _, write := range writes {
		if write.DatabaseName == databaseName && write.TableName == tableName {
			tableWrites = append(tableWrites, write)
		}
	}

	// Process writes in batches
	batchSize := r.config.RetryBatchSize
	if batchSize <= 0 {
		batchSize = 50 // Default batch size
	}

	for i := 0; i < len(tableWrites); i += batchSize {
		end := i + batchSize
		if end > len(tableWrites) {
			end = len(tableWrites)
		}

		batch := tableWrites[i:end]
		for _, write := range batch {
			// Skip writes that are already being retried
			if write.Status == BufferedWriteStatusRetrying {
				continue
			}

			// Check if the write can be retried based on retry count
			if write.RetryCount >= r.config.MaxRetryAttempts {
				continue
			}

			// Check if the tablet is healthy
			isHealthy, err := r.isTabletHealthy(ctx, &write)
			if err != nil {
				r.logger.Error("Failed to check tablet health",
					"write_id", write.ID,
					"error", err)
				continue
			}

			if !isHealthy {
				continue
			}

			// Schedule retry
			_, err = r.ScheduleRetry(ctx, write.ID, trigger)
			if err != nil {
				r.logger.Error("Failed to schedule retry",
					"write_id", write.ID,
					"error", err)
				continue
			}

			scheduledCount++
		}
	}

	r.logger.Info("Scheduled retries for table",
		"database", databaseName,
		"table", tableName,
		"scheduled", scheduledCount,
		"total", len(tableWrites),
		"trigger", trigger)

	return scheduledCount, nil
}

// RetryForPartition attempts to retry all buffered writes for a specific partition
func (r *RetryManagerImpl) RetryForPartition(ctx context.Context, databaseName, tableName, partitionName string, trigger RetryTrigger) (int, error) {
	// Get all buffered writes
	bufferStats, err := r.writeBuffer.GetBufferStats(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get buffer stats")
	}

	writes, err := r.writeBuffer.GetBufferedWrites(ctx, nil, bufferStats.TotalBufferedWrites, 0)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get buffered writes")
	}

	scheduledCount := 0

	// Filter writes for the specified partition
	var partitionWrites []BufferedWrite
	for _, write := range writes {
		if write.DatabaseName == databaseName && write.TableName == tableName && write.PartitionName == partitionName {
			partitionWrites = append(partitionWrites, write)
		}
	}

	// Process writes in batches
	batchSize := r.config.RetryBatchSize
	if batchSize <= 0 {
		batchSize = 50 // Default batch size
	}

	for i := 0; i < len(partitionWrites); i += batchSize {
		end := i + batchSize
		if end > len(partitionWrites) {
			end = len(partitionWrites)
		}

		batch := partitionWrites[i:end]
		for _, write := range batch {
			// Skip writes that are already being retried
			if write.Status == BufferedWriteStatusRetrying {
				continue
			}

			// Check if the write can be retried based on retry count
			if write.RetryCount >= r.config.MaxRetryAttempts {
				continue
			}

			// Check if the tablet is healthy
			isHealthy, err := r.isTabletHealthy(ctx, &write)
			if err != nil {
				r.logger.Error("Failed to check tablet health",
					"write_id", write.ID,
					"error", err)
				continue
			}

			if !isHealthy {
				continue
			}

			// Schedule retry
			_, err = r.ScheduleRetry(ctx, write.ID, trigger)
			if err != nil {
				r.logger.Error("Failed to schedule retry",
					"write_id", write.ID,
					"error", err)
				continue
			}

			scheduledCount++
		}
	}

	r.logger.Info("Scheduled retries for partition",
		"database", databaseName,
		"table", tableName,
		"partition", partitionName,
		"scheduled", scheduledCount,
		"total", len(partitionWrites),
		"trigger", trigger)

	return scheduledCount, nil
}

// Start starts the retry manager
func (r *RetryManagerImpl) Start() error {
	r.runningMutex.Lock()
	defer r.runningMutex.Unlock()

	if r.running {
		return nil
	}

	// Subscribe to health events
	err := r.healthChecker.SubscribeToHealthEvents(r.healthEventCh)
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to health events")
	}

	// Start periodic retry ticker if configured
	if r.config.PeriodicRetryIntervalMinutes > 0 {
		r.periodicRetryTicker = time.NewTicker(time.Duration(r.config.PeriodicRetryIntervalMinutes) * time.Minute)
	}

	// Start the main loop
	go r.run()

	// Try retrying all on startup if configured
	if r.config.RetryOnStartup {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			count, err := r.RetryAll(ctx, RetryTriggerScheduled)
			if err != nil {
				r.logger.Error("Failed to retry all on startup", "error", err)
			} else {
				r.logger.Info("Retried all on startup", "scheduled", count)
			}
		}()
	}

	r.running = true
	r.logger.Info("Retry manager started")

	return nil
}

// Stop stops the retry manager
func (r *RetryManagerImpl) Stop() error {
	r.runningMutex.Lock()
	defer r.runningMutex.Unlock()

	if !r.running {
		return nil
	}

	// Stop the main loop
	close(r.stopCh)

	// Stop the periodic retry ticker if it exists
	if r.periodicRetryTicker != nil {
		r.periodicRetryTicker.Stop()
	}

	// Unsubscribe from health events
	err := r.healthChecker.UnsubscribeFromHealthEvents(r.healthEventCh)
	if err != nil {
		return errors.Wrap(err, "failed to unsubscribe from health events")
	}

	r.running = false
	r.logger.Info("Retry manager stopped")

	return nil
}

// run is the main loop of the retry manager
func (r *RetryManagerImpl) run() {
	for {
		select {
		case <-r.stopCh:
			return
		case healthEvent := <-r.healthEventCh:
			// Process health event
			r.handleHealthEvent(healthEvent)
		case <-r.periodicRetryTicker.C:
			// Perform periodic retry
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			count, err := r.RetryAll(ctx, RetryTriggerPeriodic)
			cancel()
			if err != nil {
				r.logger.Error("Failed to perform periodic retry", "error", err)
			} else {
				r.logger.Info("Performed periodic retry", "scheduled", count)
			}
		}
	}
}

// handleHealthEvent processes a health event
func (r *RetryManagerImpl) handleHealthEvent(event tablet.HealthEvent) {
	// Only process recovery events
	if event.Type != tablet.HealthEventTypeRecovered {
		return
	}

	r.logger.Info("Received health recovery event",
		"database", event.DatabaseName,
		"table", event.TableName,
		"partition", event.PartitionName,
		"bucket", event.BucketID)

	// Trigger retries based on the event
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var count int
	var err error

	if event.PartitionName != "" {
		// Retry for specific partition
		count, err = r.RetryForPartition(ctx, event.DatabaseName, event.TableName, event.PartitionName, RetryTriggerHealthRecovered)
	} else if event.TableName != "" {
		// Retry for specific table
		count, err = r.RetryForTable(ctx, event.DatabaseName, event.TableName, RetryTriggerHealthRecovered)
	} else {
		// Retry all
		count, err = r.RetryAll(ctx, RetryTriggerHealthRecovered)
	}

	if err != nil {
		r.logger.Error("Failed to trigger retries for health event",
			"event", event,
			"error", err)
	} else {
		r.logger.Info("Triggered retries for health event",
			"event", event,
			"scheduled", count)
	}
}

// executeRetry executes a retry
func (r *RetryManagerImpl) executeRetry(ctx context.Context, retryID string) {
	// Get retry info
	r.retryInfoMutex.Lock()
	retryInfo, exists := r.retryInfo[retryID]
	if !exists {
		r.retryInfoMutex.Unlock()
		r.logger.Error("Retry not found", "retry_id", retryID)
		return
	}

	// Update retry status
	retryInfo.Status = RetryStatusInProgress
	retryInfo.StartedAt = time.Now()
	r.retryInfo[retryID] = retryInfo
	r.retryInfoMutex.Unlock()

	// Calculate backoff duration
	backoffDuration := r.calculateBackoff(retryInfo.AttemptNumber)

	// Wait for backoff duration
	select {
	case <-time.After(backoffDuration):
		// Continue with retry
	case <-ctx.Done():
		// Context cancelled
		r.updateRetryStatus(retryID, RetryStatusCancelled, errors.New("retry cancelled"))
		return
	}

	// Get the buffered write
	writes, err := r.writeBuffer.GetBufferedWrites(ctx, nil, 1, 0)
	if err != nil {
		r.updateRetryStatus(retryID, RetryStatusFailed, errors.Wrap(err, "failed to get buffered write"))
		return
	}

	var write *BufferedWrite
	for _, w := range writes {
		if w.ID == retryInfo.WriteID {
			write = &w
			break
		}
	}

	if write == nil {
		r.updateRetryStatus(retryID, RetryStatusFailed, errors.Errorf("buffered write with ID %s not found", retryInfo.WriteID))
		return
	}

	// Check if the tablet is still healthy
	isHealthy, err := r.isTabletHealthy(ctx, write)
	if err != nil {
		r.updateRetryStatus(retryID, RetryStatusFailed, errors.Wrap(err, "failed to check tablet health"))
		return
	}

	if !isHealthy {
		r.updateRetryStatus(retryID, RetryStatusFailed, errors.New("tablet is unhealthy"))
		return
	}

	// Execute the write
	err = r.retryExecutor.ExecuteWrite(ctx, *write)
	if err != nil {
		// Update retry status to failed
		r.updateRetryStatus(retryID, RetryStatusFailed, err)

		// Update write status
		writeErr := r.writeBuffer.UpdateBufferedWriteStatus(ctx, write.ID, BufferedWriteStatusPending, err)
		if writeErr != nil {
			r.logger.Error("Failed to update buffered write status",
				"write_id", write.ID,
				"error", writeErr)
		}

		return
	}

	// Update retry status to succeeded
	r.updateRetryStatus(retryID, RetryStatusSucceeded, nil)

	// Remove the write from the buffer since it succeeded
	err = r.writeBuffer.RemoveBufferedWrite(ctx, write.ID)
	if err != nil {
		r.logger.Error("Failed to remove buffered write",
			"write_id", write.ID,
			"error", err)
	}

	r.logger.Info("Retry executed successfully",
		"retry_id", retryID,
		"write_id", write.ID)
}

// updateRetryStatus updates the status of a retry
func (r *RetryManagerImpl) updateRetryStatus(retryID string, status RetryStatus, err error) {
	r.retryInfoMutex.Lock()
	defer r.retryInfoMutex.Unlock()

	retryInfo, exists := r.retryInfo[retryID]
	if !exists {
		r.logger.Error("Retry not found", "retry_id", retryID)
		return
	}

	retryInfo.Status = status
	retryInfo.CompletedAt = time.Now()
	if err != nil {
		retryInfo.Error = err.Error()
	}

	r.retryInfo[retryID] = retryInfo

	// Update metrics
	r.metrics.CounterInc("retry_manager_attempts", map[string]string{
		"status":   string(status),
		"database": retryInfo.DatabaseName,
		"table":    retryInfo.TableName,
	})

	if status == RetryStatusSucceeded {
		r.metrics.HistogramObserve("retry_manager_success_time_ms",
			float64(retryInfo.CompletedAt.Sub(retryInfo.StartedAt).Milliseconds()), nil)
	}

	r.logger.Debug("Updated retry status",
		"retry_id", retryID,
		"status", status,
		"error", err)
}

// calculateBackoff calculates the backoff duration for a retry attempt
func (r *RetryManagerImpl) calculateBackoff(attemptNumber int) time.Duration {
	baseInterval := time.Duration(r.config.BaseRetryIntervalSeconds) * time.Second
	maxInterval := time.Duration(r.config.MaxRetryIntervalSeconds) * time.Second

	var backoff time.Duration

	switch r.config.RetryStrategy {
	case RetryStrategyLinear:
		// Linear backoff: baseInterval * attemptNumber
		backoff = baseInterval * time.Duration(attemptNumber)
	case RetryStrategyExponential:
		// Exponential backoff: baseInterval * 2^(attemptNumber-1)
		backoff = baseInterval * time.Duration(math.Pow(2, float64(attemptNumber-1)))
	case RetryStrategyJittered:
		// Jittered backoff: random value between baseInterval and baseInterval * 2^(attemptNumber-1)
		maxJitter := baseInterval * time.Duration(math.Pow(2, float64(attemptNumber-1)))
		jitterRange := maxJitter - baseInterval
		backoff = baseInterval + time.Duration(rand.Int63n(int64(jitterRange)+1))
	default:
		// Default to exponential backoff
		backoff = baseInterval * time.Duration(math.Pow(2, float64(attemptNumber-1)))
	}

	// Cap at max interval
	if backoff > maxInterval {
		backoff = maxInterval
	}

	return backoff
}

// isTabletHealthy checks if the tablet for a buffered write is healthy
func (r *RetryManagerImpl) isTabletHealthy(ctx context.Context, write *BufferedWrite) (bool, error) {
	// Check if the partition is healthy
	partitionHealth, err := r.healthChecker.GetPartitionHealthStatus(
		ctx,
		write.DatabaseName,
		write.TableName,
		write.PartitionName,
	)
	if err != nil {
		return false, errors.Wrap(err, "failed to get partition health status")
	}

	// Check if the bucket is healthy
	bucketHealth, err := r.healthChecker.GetBucketHealthStatus(
		ctx,
		write.DatabaseName,
		write.TableName,
		write.BucketID,
	)
	if err != nil {
		return false, errors.Wrap(err, "failed to get bucket health status")
	}

	// Both partition and bucket must be healthy
	return partitionHealth.IsHealthy && bucketHealth.IsHealthy, nil
}

//Personal.AI order the ending
