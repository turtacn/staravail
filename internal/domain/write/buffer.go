// Package write provides functionality for buffering and processing write operations
// to StarRocks when some tablets are unavailable.
package write

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	logging "github.com/turtacn/staravail/internal/common/logger"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/domain/tablet"
	"github.com/turtacn/staravail/internal/infrastructure/config"
)

// BufferedWriteStatus represents the status of a buffered write
type BufferedWriteStatus string

const (
	// BufferedWriteStatusPending indicates the write is pending
	BufferedWriteStatusPending BufferedWriteStatus = "PENDING"
	// BufferedWriteStatusRetrying indicates the write is being retried
	BufferedWriteStatusRetrying BufferedWriteStatus = "RETRYING"
	// BufferedWriteStatusFailed indicates the write has failed permanently
	BufferedWriteStatusFailed BufferedWriteStatus = "FAILED"
)

// BufferedWrite represents a write operation that has been buffered
type BufferedWrite struct {
	// ID is a unique identifier for the write
	ID string `json:"id"`
	// DatabaseName is the name of the database
	DatabaseName string `json:"database_name"`
	// TableName is the name of the table
	TableName string `json:"table_name"`
	// PartitionName is the name of the partition
	PartitionName string `json:"partition_name"`
	// BucketID is the ID of the bucket
	BucketID int `json:"bucket_id"`
	// SQL is the SQL statement for the write
	SQL string `json:"sql"`
	// Status is the status of the buffered write
	Status BufferedWriteStatus `json:"status"`
	// CreatedAt is when the write was buffered
	CreatedAt time.Time `json:"created_at"`
	// LastRetryAt is when the write was last retried
	LastRetryAt time.Time `json:"last_retry_at"`
	// RetryCount is the number of retry attempts
	RetryCount int `json:"retry_count"`
	// Error is the last error encountered
	Error string `json:"error"`
	// Priority is the priority of the write (higher values indicate higher priority)
	Priority int `json:"priority"`
	// ExpiresAt is when the buffered write expires
	ExpiresAt time.Time `json:"expires_at"`
	// Metadata is additional metadata for the write
	Metadata map[string]interface{} `json:"metadata"`
	// Payload is the actual data for the write
	Payload []byte `json:"payload"`
}

// BufferKey is used as a key for organizing buffered writes
type BufferKey struct {
	// DatabaseName is the name of the database
	DatabaseName string
	// TableName is the name of the table
	TableName string
	// PartitionName is the name of the partition
	PartitionName string
	// BucketID is the ID of the bucket
	BucketID int
}

// String returns a string representation of the buffer key
func (k BufferKey) String() string {
	return fmt.Sprintf("%s.%s.%s.%d", k.DatabaseName, k.TableName, k.PartitionName, k.BucketID)
}

// BufferStats represents statistics about the write buffer
type BufferStats struct {
	// TotalBufferedWrites is the total number of buffered writes
	TotalBufferedWrites int `json:"total_buffered_writes"`
	// PendingWrites is the number of pending writes
	PendingWrites int `json:"pending_writes"`
	// RetryingWrites is the number of retrying writes
	RetryingWrites int `json:"retrying_writes"`
	// FailedWrites is the number of failed writes
	FailedWrites int `json:"failed_writes"`
	// OldestWriteTime is the timestamp of the oldest buffered write
	OldestWriteTime time.Time `json:"oldest_write_time"`
	// TotalBufferSizeBytes is the total size of the buffer in bytes
	TotalBufferSizeBytes int64 `json:"total_buffer_size_bytes"`
	// BufferUtilizationPercent is the buffer utilization percentage
	BufferUtilizationPercent float64 `json:"buffer_utilization_percent"`
	// PartitionStats is statistics by partition
	PartitionStats map[string]int `json:"partition_stats"`
	// TableStats is statistics by table
	TableStats map[string]int `json:"table_stats"`
}

// WriteBuffer defines the interface for buffering write operations
type WriteBuffer interface {
	// BufferWrite buffers a write operation
	BufferWrite(ctx context.Context, write BufferedWrite) error

	// FlushBuffer flushes buffered writes for a specific key or all if key is nil
	FlushBuffer(ctx context.Context, key *BufferKey) (int, error)

	// GetBufferedWrites gets buffered writes for a specific key or all if key is nil
	GetBufferedWrites(ctx context.Context, key *BufferKey, limit int, offset int) ([]BufferedWrite, error)

	// GetBufferStats gets statistics about the buffer
	GetBufferStats(ctx context.Context) (BufferStats, error)

	// RemoveBufferedWrite removes a buffered write by ID
	RemoveBufferedWrite(ctx context.Context, id string) error

	// UpdateBufferedWriteStatus updates the status of a buffered write
	UpdateBufferedWriteStatus(ctx context.Context, id string, status BufferedWriteStatus, err error) error

	// CleanExpiredWrites removes expired writes from the buffer
	CleanExpiredWrites(ctx context.Context) (int, error)

	// PersistBuffer persists the buffer to disk
	PersistBuffer(ctx context.Context) error

	// RestoreBuffer restores the buffer from disk
	RestoreBuffer(ctx context.Context) error
}

// EvictionPolicy defines the policy for evicting writes from the buffer
type EvictionPolicy string

const (
	// EvictionPolicyOldest evicts the oldest writes first
	EvictionPolicyOldest EvictionPolicy = "OLDEST"
	// EvictionPolicyLeastRetried evicts the least retried writes first
	EvictionPolicyLeastRetried EvictionPolicy = "LEAST_RETRIED"
	// EvictionPolicyLowestPriority evicts the lowest priority writes first
	EvictionPolicyLowestPriority EvictionPolicy = "LOWEST_PRIORITY"
)

// WriteBufferConfig represents configuration for the write buffer
type WriteBufferConfig struct {
	// MaxBufferSizeBytes is the maximum size of the buffer in bytes
	MaxBufferSizeBytes int64
	// MaxBufferedWrites is the maximum number of writes to buffer
	MaxBufferedWrites int
	// EvictionPolicy is the policy for evicting writes from the buffer
	EvictionPolicy EvictionPolicy
	// BufferPersistPath is the path where the buffer is persisted
	BufferPersistPath string
	// PersistIntervalSeconds is how often to persist the buffer in seconds
	PersistIntervalSeconds int
	// WriteExpirationHours is when buffered writes expire in hours
	WriteExpirationHours int
	// DefaultWritePriority is the default priority for writes
	DefaultWritePriority int
	// HighPriorityTables is a list of high priority tables
	HighPriorityTables []string
}

// WriteBufferImpl implements the WriteBuffer interface
type WriteBufferImpl struct {
	// config is the buffer configuration
	config WriteBufferConfig
	// healthChecker is used to check tablet health
	healthChecker tablet.HealthChecker
	// metadataService is used to get metadata information
	metadataService metadata.MetadataService
	// logger is used for logging
	logger logging.Logger
	// metrics is used to record metrics
	metrics metrics.MetricsRecorder

	// buffer stores the buffered writes by buffer key
	buffer map[BufferKey]map[string]*BufferedWrite
	// bufferMutex protects the buffer map
	bufferMutex sync.RWMutex
	// currentBufferSizeBytes is the current size of the buffer in bytes
	currentBufferSizeBytes int64
	// lastPersistTime is when the buffer was last persisted
	lastPersistTime time.Time
	// persistTicker is a ticker for persisting the buffer
	persistTicker *time.Ticker
	// stopCh is a channel for stopping the persist ticker
	stopCh chan struct{}
}

// NewWriteBuffer creates a new WriteBuffer instance
func NewWriteBuffer(
	cfg config.WriteBufferConfig,
	healthChecker tablet.HealthChecker,
	metadataService metadata.MetadataService,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) WriteBuffer {
	// Convert config to internal buffer config
	bufferConfig := WriteBufferConfig{
		MaxBufferSizeBytes:     cfg.MaxBufferSizeBytes,
		MaxBufferedWrites:      cfg.MaxBufferedWrites,
		EvictionPolicy:         EvictionPolicy(cfg.EvictionPolicy),
		BufferPersistPath:      cfg.BufferPersistPath,
		PersistIntervalSeconds: cfg.PersistIntervalSeconds,
		WriteExpirationHours:   cfg.WriteExpirationHours,
		DefaultWritePriority:   cfg.DefaultWritePriority,
		HighPriorityTables:     cfg.HighPriorityTables,
	}

	buffer := &WriteBufferImpl{
		config:                 bufferConfig,
		healthChecker:          healthChecker,
		metadataService:        metadataService,
		logger:                 logger,
		metrics:                metricsRecorder,
		buffer:                 make(map[BufferKey]map[string]*BufferedWrite),
		bufferMutex:            sync.RWMutex{},
		currentBufferSizeBytes: 0,
		lastPersistTime:        time.Now(),
		stopCh:                 make(chan struct{}),
	}

	// Start the persist ticker if configured
	if bufferConfig.PersistIntervalSeconds > 0 {
		buffer.persistTicker = time.NewTicker(time.Duration(bufferConfig.PersistIntervalSeconds) * time.Second)
		go buffer.persistLoop()
	}

	return buffer
}

// persistLoop runs a loop to periodically persist the buffer
func (b *WriteBufferImpl) persistLoop() {
	for {
		select {
		case <-b.persistTicker.C:
			ctx := context.Background()
			err := b.PersistBuffer(ctx)
			if err != nil {
				b.logger.Error("Failed to persist buffer", "error", err)
			}
		case <-b.stopCh:
			b.persistTicker.Stop()
			return
		}
	}
}

// BufferWrite buffers a write operation
func (b *WriteBufferImpl) BufferWrite(ctx context.Context, write BufferedWrite) error {
	// Set default values if not provided
	if write.ID == "" {
		write.ID = fmt.Sprintf("write_%d", time.Now().UnixNano())
	}
	if write.CreatedAt.IsZero() {
		write.CreatedAt = time.Now()
	}
	if write.Status == "" {
		write.Status = BufferedWriteStatusPending
	}
	if write.Priority == 0 {
		write.Priority = b.getWritePriority(write.TableName)
	}
	if write.ExpiresAt.IsZero() {
		write.ExpiresAt = time.Now().Add(time.Duration(b.config.WriteExpirationHours) * time.Hour)
	}
	if write.Metadata == nil {
		write.Metadata = make(map[string]interface{})
	}

	// Create the buffer key
	key := BufferKey{
		DatabaseName:  write.DatabaseName,
		TableName:     write.TableName,
		PartitionName: write.PartitionName,
		BucketID:      write.BucketID,
	}

	// Check if we need to evict writes to make room
	writeSize := int64(len(write.SQL) + len(write.Payload))
	b.bufferMutex.Lock()
	defer b.bufferMutex.Unlock()

	// Check if we've exceeded the maximum number of writes
	totalWrites := b.countTotalWritesLocked()
	if b.config.MaxBufferedWrites > 0 && totalWrites >= b.config.MaxBufferedWrites {
		// Need to evict writes
		err := b.evictWritesLocked(1, writeSize)
		if err != nil {
			return err
		}
	}

	// Check if we've exceeded the maximum buffer size
	if b.config.MaxBufferSizeBytes > 0 && b.currentBufferSizeBytes+writeSize > b.config.MaxBufferSizeBytes {
		// Need to evict writes
		bytesToFree := b.currentBufferSizeBytes + writeSize - b.config.MaxBufferSizeBytes
		err := b.evictWritesLocked(0, bytesToFree)
		if err != nil {
			return err
		}
	}

	// Add the write to the buffer
	if _, exists := b.buffer[key]; !exists {
		b.buffer[key] = make(map[string]*BufferedWrite)
	}
	b.buffer[key][write.ID] = &write

	// Update metrics
	b.currentBufferSizeBytes += writeSize
	b.metrics.GaugeSet("write_buffer_size_bytes", float64(b.currentBufferSizeBytes), nil)
	b.metrics.GaugeSet("write_buffer_count", float64(b.countTotalWritesLocked()), nil)
	b.metrics.CounterInc("write_buffer_writes_buffered", map[string]string{
		"database": write.DatabaseName,
		"table":    write.TableName,
	})

	b.logger.Debug("Buffered write operation",
		"id", write.ID,
		"database", write.DatabaseName,
		"table", write.TableName,
		"partition", write.PartitionName,
		"bucket", write.BucketID)

	return nil
}

// FlushBuffer flushes buffered writes for a specific key or all if key is nil
func (b *WriteBufferImpl) FlushBuffer(ctx context.Context, key *BufferKey) (int, error) {
	b.bufferMutex.Lock()
	defer b.bufferMutex.Unlock()

	flushedCount := 0
	var keysToFlush []BufferKey

	if key == nil {
		// Flush all keys
		for bufferKey := range b.buffer {
			keysToFlush = append(keysToFlush, bufferKey)
		}
	} else {
		// Flush specific key
		if _, exists := b.buffer[*key]; exists {
			keysToFlush = append(keysToFlush, *key)
		}
	}

	// Process each key
	for _, k := range keysToFlush {
		// Check if the tablet for this key is healthy
		isHealthy, err := b.isTabletHealthy(ctx, k)
		if err != nil {
			b.logger.Error("Failed to check tablet health",
				"database", k.DatabaseName,
				"table", k.TableName,
				"partition", k.PartitionName,
				"bucket", k.BucketID,
				"error", err)
			continue
		}

		if !isHealthy {
			b.logger.Warn("Cannot flush buffer, tablet is unhealthy",
				"database", k.DatabaseName,
				"table", k.TableName,
				"partition", k.PartitionName,
				"bucket", k.BucketID)
			continue
		}

		// Get the writes for this key
		writes := b.buffer[k]
		flushedCount += len(writes)

		// Remove the writes from the buffer and update metrics
		for id, write := range writes {
			writeSize := int64(len(write.SQL) + len(write.Payload))
			b.currentBufferSizeBytes -= writeSize
			delete(b.buffer[k], id)

			b.metrics.CounterInc("write_buffer_writes_flushed", map[string]string{
				"database": k.DatabaseName,
				"table":    k.TableName,
			})
		}

		// Remove the key if it's empty
		if len(b.buffer[k]) == 0 {
			delete(b.buffer, k)
		}
	}

	// Update metrics
	b.metrics.GaugeSet("write_buffer_size_bytes", float64(b.currentBufferSizeBytes), nil)
	b.metrics.GaugeSet("write_buffer_count", float64(b.countTotalWritesLocked()), nil)

	b.logger.Info("Flushed buffered writes", "count", flushedCount)

	return flushedCount, nil
}

// GetBufferedWrites gets buffered writes for a specific key or all if key is nil
func (b *WriteBufferImpl) GetBufferedWrites(ctx context.Context, key *BufferKey, limit int, offset int) ([]BufferedWrite, error) {
	b.bufferMutex.RLock()
	defer b.bufferMutex.RUnlock()

	var result []BufferedWrite
	var allWrites []*BufferedWrite

	if key == nil {
		// Get all writes
		for _, writes := range b.buffer {
			for _, write := range writes {
				allWrites = append(allWrites, write)
			}
		}
	} else {
		// Get writes for specific key
		if writes, exists := b.buffer[*key]; exists {
			for _, write := range writes {
				allWrites = append(allWrites, write)
			}
		}
	}

	// Sort by creation time (newest first)
	sort.Slice(allWrites, func(i, j int) bool {
		return allWrites[i].CreatedAt.After(allWrites[j].CreatedAt)
	})

	// Apply pagination
	if offset >= len(allWrites) {
		return []BufferedWrite{}, nil
	}

	end := offset + limit
	if end > len(allWrites) || limit <= 0 {
		end = len(allWrites)
	}

	// Copy the writes to the result
	for _, write := range allWrites[offset:end] {
		result = append(result, *write)
	}

	return result, nil
}

// GetBufferStats gets statistics about the buffer
func (b *WriteBufferImpl) GetBufferStats(ctx context.Context) (BufferStats, error) {
	b.bufferMutex.RLock()
	defer b.bufferMutex.RUnlock()

	stats := BufferStats{
		PartitionStats: make(map[string]int),
		TableStats:     make(map[string]int),
	}

	oldestTime := time.Now()
	var oldestFound bool

	// Calculate statistics
	for key, writes := range b.buffer {
		for _, write := range writes {
			stats.TotalBufferedWrites++

			switch write.Status {
			case BufferedWriteStatusPending:
				stats.PendingWrites++
			case BufferedWriteStatusRetrying:
				stats.RetryingWrites++
			case BufferedWriteStatusFailed:
				stats.FailedWrites++
			}

			if !oldestFound || write.CreatedAt.Before(oldestTime) {
				oldestTime = write.CreatedAt
				oldestFound = true
			}

			// Update partition stats
			partitionKey := fmt.Sprintf("%s.%s.%s", key.DatabaseName, key.TableName, key.PartitionName)
			stats.PartitionStats[partitionKey]++

			// Update table stats
			tableKey := fmt.Sprintf("%s.%s", key.DatabaseName, key.TableName)
			stats.TableStats[tableKey]++
		}
	}

	if oldestFound {
		stats.OldestWriteTime = oldestTime
	}

	stats.TotalBufferSizeBytes = b.currentBufferSizeBytes

	if b.config.MaxBufferSizeBytes > 0 {
		stats.BufferUtilizationPercent = float64(b.currentBufferSizeBytes) / float64(b.config.MaxBufferSizeBytes) * 100
	}

	return stats, nil
}

// RemoveBufferedWrite removes a buffered write by ID
func (b *WriteBufferImpl) RemoveBufferedWrite(ctx context.Context, id string) error {
	b.bufferMutex.Lock()
	defer b.bufferMutex.Unlock()

	// Find the write by ID
	for key, writes := range b.buffer {
		if write, exists := writes[id]; exists {
			// Update metrics
			writeSize := int64(len(write.SQL) + len(write.Payload))
			b.currentBufferSizeBytes -= writeSize

			// Remove the write
			delete(b.buffer[key], id)

			// Remove the key if it's empty
			if len(b.buffer[key]) == 0 {
				delete(b.buffer, key)
			}

			// Update metrics
			b.metrics.GaugeSet("write_buffer_size_bytes", float64(b.currentBufferSizeBytes), nil)
			b.metrics.GaugeSet("write_buffer_count", float64(b.countTotalWritesLocked()), nil)
			b.metrics.CounterInc("write_buffer_writes_removed", nil)

			b.logger.Debug("Removed buffered write", "id", id)

			return nil
		}
	}

	return errors.Errorf("buffered write with ID %s not found", id)
}

// UpdateBufferedWriteStatus updates the status of a buffered write
func (b *WriteBufferImpl) UpdateBufferedWriteStatus(ctx context.Context, id string, status BufferedWriteStatus, err error) error {
	b.bufferMutex.Lock()
	defer b.bufferMutex.Unlock()

	// Find the write by ID
	for _, writes := range b.buffer {
		if write, exists := writes[id]; exists {
			// Update the write status
			write.Status = status

			if err != nil {
				write.Error = err.Error()
			}

			if status == BufferedWriteStatusRetrying {
				write.LastRetryAt = time.Now()
				write.RetryCount++
			}

			// Update metrics
			b.metrics.CounterInc("write_buffer_status_updates", map[string]string{
				"status": string(status),
			})

			b.logger.Debug("Updated buffered write status",
				"id", id,
				"status", status,
				"retry_count", write.RetryCount)

			return nil
		}
	}

	return errors.Errorf("buffered write with ID %s not found", id)
}

// CleanExpiredWrites removes expired writes from the buffer
func (b *WriteBufferImpl) CleanExpiredWrites(ctx context.Context) (int, error) {
	b.bufferMutex.Lock()
	defer b.bufferMutex.Unlock()

	now := time.Now()
	removedCount := 0

	// Find expired writes
	for key, writes := range b.buffer {
		for id, write := range writes {
			if now.After(write.ExpiresAt) {
				// Remove the write
				writeSize := int64(len(write.SQL) + len(write.Payload))
				b.currentBufferSizeBytes -= writeSize
				delete(b.buffer[key], id)
				removedCount++

				b.metrics.CounterInc("write_buffer_writes_expired", map[string]string{
					"database": key.DatabaseName,
					"table":    key.TableName,
				})
			}
		}

		// Remove the key if it's empty
		if len(b.buffer[key]) == 0 {
			delete(b.buffer, key)
		}
	}

	// Update metrics
	b.metrics.GaugeSet("write_buffer_size_bytes", float64(b.currentBufferSizeBytes), nil)
	b.metrics.GaugeSet("write_buffer_count", float64(b.countTotalWritesLocked()), nil)

	b.logger.Info("Cleaned expired buffered writes", "count", removedCount)

	return removedCount, nil
}

// PersistBuffer persists the buffer to disk
func (b *WriteBufferImpl) PersistBuffer(ctx context.Context) error {
	b.bufferMutex.RLock()
	defer b.bufferMutex.RUnlock()

	// Create the persist directory if it doesn't exist
	if err := os.MkdirAll(b.config.BufferPersistPath, 0755); err != nil {
		return errors.Wrap(err, "failed to create buffer persist directory")
	}

	// Create a snapshot of the buffer
	snapshot := make(map[string][]BufferedWrite)
	for key, writes := range b.buffer {
		keyStr := key.String()
		for _, write := range writes {
			snapshot[keyStr] = append(snapshot[keyStr], *write)
		}
	}

	// Marshal the snapshot to JSON
	data, err := json.Marshal(snapshot)
	if err != nil {
		return errors.Wrap(err, "failed to marshal buffer snapshot")
	}

	// Write the snapshot to a temporary file
	tempFile := filepath.Join(b.config.BufferPersistPath, "buffer_snapshot.tmp")
	if err := ioutil.WriteFile(tempFile, data, 0644); err != nil {
		return errors.Wrap(err, "failed to write buffer snapshot to temporary file")
	}

	// Rename the temporary file to the final file
	finalFile := filepath.Join(b.config.BufferPersistPath, "buffer_snapshot.json")
	if err := os.Rename(tempFile, finalFile); err != nil {
		return errors.Wrap(err, "failed to rename buffer snapshot file")
	}

	b.lastPersistTime = time.Now()
	b.metrics.CounterInc("write_buffer_persists", nil)
	b.logger.Debug("Persisted buffer to disk", "path", finalFile)

	return nil
}

// RestoreBuffer restores the buffer from disk
func (b *WriteBufferImpl) RestoreBuffer(ctx context.Context) error {
	b.bufferMutex.Lock()
	defer b.bufferMutex.Unlock()

	// Check if the snapshot file exists
	snapshotFile := filepath.Join(b.config.BufferPersistPath, "buffer_snapshot.json")
	if _, err := os.Stat(snapshotFile); os.IsNotExist(err) {
		b.logger.Info("No buffer snapshot found, starting with empty buffer")
		return nil
	}

	// Read the snapshot file
	data, err := ioutil.ReadFile(snapshotFile)
	if err != nil {
		return errors.Wrap(err, "failed to read buffer snapshot file")
	}

	// Unmarshal the snapshot
	snapshot := make(map[string][]BufferedWrite)
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return errors.Wrap(err, "failed to unmarshal buffer snapshot")
	}

	// Clear the current buffer
	b.buffer = make(map[BufferKey]map[string]*BufferedWrite)
	b.currentBufferSizeBytes = 0

	// Restore the buffer
	for keyStr, writes := range snapshot {
		// Parse the key
		var key BufferKey
		parts := strings.Split(keyStr, ".")
		if len(parts) >= 4 {
			key.DatabaseName = parts[0]
			key.TableName = parts[1]
			key.PartitionName = parts[2]
			bucketID, err := strconv.Atoi(parts[3])
			if err != nil {
				b.logger.Error("Failed to parse bucket ID from key", "key", keyStr, "error", err)
				continue
			}
			key.BucketID = bucketID
		} else {
			b.logger.Error("Invalid buffer key format", "key", keyStr)
			continue
		}

		// Add the writes to the buffer
		b.buffer[key] = make(map[string]*BufferedWrite)
		for i := range writes {
			write := &writes[i]
			b.buffer[key][write.ID] = write
			b.currentBufferSizeBytes += int64(len(write.SQL) + len(write.Payload))
		}
	}

	// Update metrics
	b.metrics.GaugeSet("write_buffer_size_bytes", float64(b.currentBufferSizeBytes), nil)
	b.metrics.GaugeSet("write_buffer_count", float64(b.countTotalWritesLocked()), nil)
	b.metrics.CounterInc("write_buffer_restores", nil)

	b.logger.Info("Restored buffer from disk",
		"writes", b.countTotalWritesLocked(),
		"size", b.currentBufferSizeBytes)

	return nil
}

// evictWritesLocked evicts writes from the buffer according to the eviction policy
// The caller must hold the buffer mutex.
func (b *WriteBufferImpl) evictWritesLocked(countToEvict int, bytesToFree int64) error {
	if countToEvict == 0 && bytesToFree == 0 {
		return nil
	}

	// Collect all writes for eviction evaluation
	type evictionCandidate struct {
		key   BufferKey
		id    string
		write *BufferedWrite
		size  int64
	}

	var candidates []evictionCandidate
	for key, writes := range b.buffer {
		for id, write := range writes {
			size := int64(len(write.SQL) + len(write.Payload))
			candidates = append(candidates, evictionCandidate{
				key:   key,
				id:    id,
				write: write,
				size:  size,
			})
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	// Sort candidates according to the eviction policy
	switch b.config.EvictionPolicy {
	case EvictionPolicyOldest:
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].write.CreatedAt.Before(candidates[j].write.CreatedAt)
		})
	case EvictionPolicyLeastRetried:
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].write.RetryCount < candidates[j].write.RetryCount
		})
	case EvictionPolicyLowestPriority:
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].write.Priority == candidates[j].write.Priority {
				// If priorities are equal, use creation time as a tiebreaker
				return candidates[i].write.CreatedAt.Before(candidates[j].write.CreatedAt)
			}
			return candidates[i].write.Priority < candidates[j].write.Priority
		})
	default:
		// Default to oldest
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].write.CreatedAt.Before(candidates[j].write.CreatedAt)
		})
	}

	// Evict candidates until we've freed enough space or count
	evictedCount := 0
	freedBytes := int64(0)
	for _, candidate := range candidates {
		// Remove the write from the buffer
		delete(b.buffer[candidate.key], candidate.id)
		b.currentBufferSizeBytes -= candidate.size
		evictedCount++
		freedBytes += candidate.size

		// Remove the key if it's empty
		if len(b.buffer[candidate.key]) == 0 {
			delete(b.buffer, candidate.key)
		}

		// Update metrics
		b.metrics.CounterInc("write_buffer_writes_evicted", map[string]string{
			"database": candidate.key.DatabaseName,
			"table":    candidate.key.TableName,
		})

		// Check if we've evicted enough
		if (countToEvict > 0 && evictedCount >= countToEvict) ||
			(bytesToFree > 0 && freedBytes >= bytesToFree) {
			break
		}
	}

	b.logger.Info("Evicted buffered writes",
		"count", evictedCount,
		"bytes", freedBytes,
		"policy", b.config.EvictionPolicy)

	return nil
}

// isTabletHealthy checks if the tablet for a buffer key is healthy
func (b *WriteBufferImpl) isTabletHealthy(ctx context.Context, key BufferKey) (bool, error) {
	// Check if the partition is healthy
	partitionHealth, err := b.healthChecker.GetPartitionHealthStatus(
		ctx,
		key.DatabaseName,
		key.TableName,
		key.PartitionName,
	)
	if err != nil {
		return false, errors.Wrap(err, "failed to get partition health status")
	}

	// Check if the bucket is healthy
	bucketHealth, err := b.healthChecker.GetBucketHealthStatus(
		ctx,
		key.DatabaseName,
		key.TableName,
		key.BucketID,
	)
	if err != nil {
		return false, errors.Wrap(err, "failed to get bucket health status")
	}

	// Both partition and bucket must be healthy
	return partitionHealth.IsHealthy && bucketHealth.IsHealthy, nil
}

// countTotalWritesLocked counts the total number of buffered writes
// The caller must hold the buffer mutex.
func (b *WriteBufferImpl) countTotalWritesLocked() int {
	count := 0
	for _, writes := range b.buffer {
		count += len(writes)
	}
	return count
}

// getWritePriority gets the priority for a write based on the table name
func (b *WriteBufferImpl) getWritePriority(tableName string) int {
	// Check if the table is in the high priority list
	for _, highPriorityTable := range b.config.HighPriorityTables {
		if strings.EqualFold(tableName, highPriorityTable) {
			return b.config.DefaultWritePriority + 100
		}
	}
	return b.config.DefaultWritePriority
}

// Close stops the persist ticker and performs a final persist
func (b *WriteBufferImpl) Close() error {
	// Stop the persist ticker
	close(b.stopCh)

	// Perform a final persist
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return b.PersistBuffer(ctx)
}

//Personal.AI order the ending
