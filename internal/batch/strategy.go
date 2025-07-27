// Package batch provides functionality for managing and processing data in batches.
package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/turtacn/staravail/internal/common/config"
	"github.com/turtacn/staravail/internal/common/logging"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/message"
)

// BatchStrategy defines the interface for batch formation strategies
type BatchStrategy interface {
	// ShouldCreateBatch determines if a batch should be created based on current conditions
	ShouldCreateBatch(batchInfo BatchInfo) bool

	// GetMaxBatchSize returns the maximum batch size in bytes
	GetMaxBatchSize() int64

	// GetMaxBatchCount returns the maximum number of messages in a batch
	GetMaxBatchCount() int

	// GetMaxBatchDelay returns the maximum delay before a batch is created
	GetMaxBatchDelay() time.Duration

	// GetName returns the strategy name
	GetName() string

	// GetConfig returns the strategy configuration
	GetConfig() map[string]interface{}

	// Reset resets any internal state
	Reset()

	// Close releases any resources
	Close() error

	// OnBatchCreated is called when a batch is created
	OnBatchCreated(batchInfo BatchInfo)

	// Clone creates a copy of the strategy with the same configuration
	Clone() BatchStrategy
}

// BatchInfo contains information about the current batch state
type BatchInfo struct {
	// CurrentSize is the current size of the batch in bytes
	CurrentSize int64

	// CurrentCount is the current number of messages in the batch
	CurrentCount int

	// ElapsedTime is the time since the batch was started
	ElapsedTime time.Duration

	// FirstMessageTime is when the first message was added
	FirstMessageTime time.Time

	// LastMessageTime is when the last message was added
	LastMessageTime time.Time

	// TargetTable is the target table for the batch
	TargetTable string

	// TargetPartition is the target partition for the batch
	TargetPartition string

	// SystemLoad contains information about system load
	SystemLoad SystemLoadInfo

	// BatchCount is the number of batches created so far
	BatchCount int64

	// MessageRate is the rate of messages being processed (messages per second)
	MessageRate float64

	// AdditionalInfo contains any additional information
	AdditionalInfo map[string]interface{}
}

// SystemLoadInfo contains information about system load
type SystemLoadInfo struct {
	// CPUUtilization is the current CPU utilization (0-1)
	CPUUtilization float64

	// MemoryUtilization is the current memory utilization (0-1)
	MemoryUtilization float64

	// NetworkUtilization is the current network utilization (0-1)
	NetworkUtilization float64

	// DiskUtilization is the current disk utilization (0-1)
	DiskUtilization float64

	// QueueSize is the current queue size
	QueueSize int

	// ProcessingDelay is the current processing delay
	ProcessingDelay time.Duration

	// ErrorRate is the current error rate (0-1)
	ErrorRate float64
}

// BaseStrategy provides common functionality for all strategies
type BaseStrategy struct {
	// Name is the strategy name
	Name string

	// MaxBatchSize is the maximum batch size in bytes
	MaxBatchSize int64

	// MaxBatchCount is the maximum number of messages in a batch
	MaxBatchCount int

	// MaxBatchDelay is the maximum delay before a batch is created
	MaxBatchDelay time.Duration

	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder

	// EnableMetrics enables metrics collection
	EnableMetrics bool

	// lock protects concurrent access
	lock sync.RWMutex

	// closed indicates if the strategy is closed
	closed bool
}

// NewBaseStrategy creates a new base strategy
func NewBaseStrategy(
	name string,
	maxBatchSize int64,
	maxBatchCount int,
	maxBatchDelay time.Duration,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
	enableMetrics bool,
) *BaseStrategy {
	return &BaseStrategy{
		Name:          name,
		MaxBatchSize:  maxBatchSize,
		MaxBatchCount: maxBatchCount,
		MaxBatchDelay: maxBatchDelay,
		Logger:        logger,
		Metrics:       metricsRecorder,
		EnableMetrics: enableMetrics,
	}
}

// GetMaxBatchSize returns the maximum batch size in bytes
func (s *BaseStrategy) GetMaxBatchSize() int64 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.MaxBatchSize
}

// GetMaxBatchCount returns the maximum number of messages in a batch
func (s *BaseStrategy) GetMaxBatchCount() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.MaxBatchCount
}

// GetMaxBatchDelay returns the maximum delay before a batch is created
func (s *BaseStrategy) GetMaxBatchDelay() time.Duration {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.MaxBatchDelay
}

// GetName returns the strategy name
func (s *BaseStrategy) GetName() string {
	return s.Name
}

// Reset resets any internal state
func (s *BaseStrategy) Reset() {
	// Base implementation does nothing
}

// Close releases any resources
func (s *BaseStrategy) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.closed = true
	return nil
}

// OnBatchCreated is called when a batch is created
func (s *BaseStrategy) OnBatchCreated(batchInfo BatchInfo) {
	// Base implementation does nothing
}

// GetConfig returns the strategy configuration
func (s *BaseStrategy) GetConfig() map[string]interface{} {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return map[string]interface{}{
		"name":          s.Name,
		"maxBatchSize":  s.MaxBatchSize,
		"maxBatchCount": s.MaxBatchCount,
		"maxBatchDelay": s.MaxBatchDelay.Milliseconds(),
		"enableMetrics": s.EnableMetrics,
	}
}

// Clone creates a copy of the base strategy
func (s *BaseStrategy) Clone() *BaseStrategy {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return &BaseStrategy{
		Name:          s.Name,
		MaxBatchSize:  s.MaxBatchSize,
		MaxBatchCount: s.MaxBatchCount,
		MaxBatchDelay: s.MaxBatchDelay,
		Logger:        s.Logger,
		Metrics:       s.Metrics,
		EnableMetrics: s.EnableMetrics,
	}
}

// TimeIntervalStrategy implements BatchStrategy based on time intervals
type TimeIntervalStrategy struct {
	*BaseStrategy
	// MinBatchDelay is the minimum delay before a batch is created
	MinBatchDelay time.Duration
}

// NewTimeIntervalStrategy creates a new time interval strategy
func NewTimeIntervalStrategy(
	maxBatchDelay time.Duration,
	minBatchDelay time.Duration,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
	enableMetrics bool,
) *TimeIntervalStrategy {
	// Ensure maxBatchDelay is greater than minBatchDelay
	if maxBatchDelay < minBatchDelay {
		maxBatchDelay = minBatchDelay
	}

	return &TimeIntervalStrategy{
		BaseStrategy: NewBaseStrategy(
			"TimeIntervalStrategy",
			math.MaxInt64, // No size limit
			math.MaxInt,   // No count limit
			maxBatchDelay,
			logger,
			metricsRecorder,
			enableMetrics,
		),
		MinBatchDelay: minBatchDelay,
	}
}

// ShouldCreateBatch determines if a batch should be created based on time
func (s *TimeIntervalStrategy) ShouldCreateBatch(batchInfo BatchInfo) bool {
	// Check if batch is empty
	if batchInfo.CurrentCount == 0 {
		return false
	}

	// Check if the elapsed time exceeds the max delay
	if batchInfo.ElapsedTime >= s.GetMaxBatchDelay() {
		if s.EnableMetrics {
			s.Metrics.CounterInc("batch_strategy_triggered", map[string]string{
				"strategy": s.GetName(),
				"reason":   "max_delay",
				"target":   batchInfo.TargetTable,
			})
		}
		return true
	}

	// Check if the elapsed time exceeds the min delay and there are messages
	if batchInfo.ElapsedTime >= s.MinBatchDelay && batchInfo.CurrentCount > 0 {
		// Only create batch if no new messages for a while
		timeSinceLastMessage := time.Since(batchInfo.LastMessageTime)
		if timeSinceLastMessage >= s.MinBatchDelay/2 {
			if s.EnableMetrics {
				s.Metrics.CounterInc("batch_strategy_triggered", map[string]string{
					"strategy": s.GetName(),
					"reason":   "min_delay_no_messages",
					"target":   batchInfo.TargetTable,
				})
			}
			return true
		}
	}

	return false
}

// GetConfig returns the strategy configuration
func (s *TimeIntervalStrategy) GetConfig() map[string]interface{} {
	config := s.BaseStrategy.GetConfig()
	config["minBatchDelay"] = s.MinBatchDelay.Milliseconds()
	return config
}

// Clone creates a copy of the strategy
func (s *TimeIntervalStrategy) Clone() BatchStrategy {
	baseClone := s.BaseStrategy.Clone()
	return &TimeIntervalStrategy{
		BaseStrategy:  baseClone,
		MinBatchDelay: s.MinBatchDelay,
	}
}

// BatchSizeStrategy implements BatchStrategy based on batch size
type BatchSizeStrategy struct {
	*BaseStrategy
	// MinBatchSize is the minimum batch size before creation
	MinBatchSize int64
}

// NewBatchSizeStrategy creates a new batch size strategy
func NewBatchSizeStrategy(
	maxBatchSize int64,
	minBatchSize int64,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
	enableMetrics bool,
) *BatchSizeStrategy {
	// Ensure maxBatchSize is greater than minBatchSize
	if maxBatchSize < minBatchSize {
		maxBatchSize = minBatchSize
	}

	return &BatchSizeStrategy{
		BaseStrategy: NewBaseStrategy(
			"BatchSizeStrategy",
			maxBatchSize,
			math.MaxInt,  // No count limit
			time.Hour*24, // Very long delay (effectively no time limit)
			logger,
			metricsRecorder,
			enableMetrics,
		),
		MinBatchSize: minBatchSize,
	}
}

// ShouldCreateBatch determines if a batch should be created based on size
func (s *BatchSizeStrategy) ShouldCreateBatch(batchInfo BatchInfo) bool {
	// Check if batch is empty
	if batchInfo.CurrentCount == 0 {
		return false
	}

	// Check if the current size exceeds the max size
	if batchInfo.CurrentSize >= s.GetMaxBatchSize() {
		if s.EnableMetrics {
			s.Metrics.CounterInc("batch_strategy_triggered", map[string]string{
				"strategy": s.GetName(),
				"reason":   "max_size",
				"target":   batchInfo.TargetTable,
			})
		}
		return true
	}

	// Check if the current size exceeds the min size and there hasn't been a new message for a while
	if batchInfo.CurrentSize >= s.MinBatchSize && batchInfo.CurrentCount > 0 {
		timeSinceLastMessage := time.Since(batchInfo.LastMessageTime)
		if timeSinceLastMessage >= time.Second {
			if s.EnableMetrics {
				s.Metrics.CounterInc("batch_strategy_triggered", map[string]string{
					"strategy": s.GetName(),
					"reason":   "min_size_no_messages",
					"target":   batchInfo.TargetTable,
				})
			}
			return true
		}
	}

	return false
}

// GetConfig returns the strategy configuration
func (s *BatchSizeStrategy) GetConfig() map[string]interface{} {
	config := s.BaseStrategy.GetConfig()
	config["minBatchSize"] = s.MinBatchSize
	return config
}

// Clone creates a copy of the strategy
func (s *BatchSizeStrategy) Clone() BatchStrategy {
	baseClone := s.BaseStrategy.Clone()
	return &BatchSizeStrategy{
		BaseStrategy: baseClone,
		MinBatchSize: s.MinBatchSize,
	}
}

// BatchCountStrategy implements BatchStrategy based on message count
type BatchCountStrategy struct {
	*BaseStrategy
	// MinBatchCount is the minimum number of messages before creation
	MinBatchCount int
}

// NewBatchCountStrategy creates a new batch count strategy
func NewBatchCountStrategy(
	maxBatchCount int,
	minBatchCount int,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
	enableMetrics bool,
) *BatchCountStrategy {
	// Ensure maxBatchCount is greater than minBatchCount
	if maxBatchCount < minBatchCount {
		maxBatchCount = minBatchCount
	}

	return &BatchCountStrategy{
		BaseStrategy: NewBaseStrategy(
			"BatchCountStrategy",
			math.MaxInt64, // No size limit
			maxBatchCount,
			time.Hour*24, // Very long delay (effectively no time limit)
			logger,
			metricsRecorder,
			enableMetrics,
		),
		MinBatchCount: minBatchCount,
	}
}

// ShouldCreateBatch determines if a batch should be created based on count
func (s *BatchCountStrategy) ShouldCreateBatch(batchInfo BatchInfo) bool {
	// Check if batch is empty
	if batchInfo.CurrentCount == 0 {
		return false
	}

	// Check if the current count exceeds the max count
	if batchInfo.CurrentCount >= s.GetMaxBatchCount() {
		if s.EnableMetrics {
			s.Metrics.CounterInc("batch_strategy_triggered", map[string]string{
				"strategy": s.GetName(),
				"reason":   "max_count",
				"target":   batchInfo.TargetTable,
			})
		}
		return true
	}

	// Check if the current count exceeds the min count and there hasn't been a new message for a while
	if batchInfo.CurrentCount >= s.MinBatchCount {
		timeSinceLastMessage := time.Since(batchInfo.LastMessageTime)
		if timeSinceLastMessage >= time.Second {
			if s.EnableMetrics {
				s.Metrics.CounterInc("batch_strategy_triggered", map[string]string{
					"strategy": s.GetName(),
					"reason":   "min_count_no_messages",
					"target":   batchInfo.TargetTable,
				})
			}
			return true
		}
	}

	return false
}

// GetConfig returns the strategy configuration
func (s *BatchCountStrategy) GetConfig() map[string]interface{} {
	config := s.BaseStrategy.GetConfig()
	config["minBatchCount"] = s.MinBatchCount
	return config
}

// Clone creates a copy of the strategy
func (s *BatchCountStrategy) Clone() BatchStrategy {
	baseClone := s.BaseStrategy.Clone()
	return &BatchCountStrategy{
		BaseStrategy:  baseClone,
		MinBatchCount: s.MinBatchCount,
	}
}

// HybridStrategy implements BatchStrategy by combining multiple strategies
type HybridStrategy struct {
	*BaseStrategy
	// Strategies is the list of strategies to combine
	Strategies []BatchStrategy
	// RequireAll requires all strategies to agree (AND logic) if true,
	// otherwise any strategy can trigger (OR logic)
	RequireAll bool
}

// NewHybridStrategy creates a new hybrid strategy
func NewHybridStrategy(
	strategies []BatchStrategy,
	requireAll bool,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
	enableMetrics bool,
) *HybridStrategy {
	// Find the minimum values across all strategies
	var maxBatchSize int64 = math.MaxInt64
	var maxBatchCount int = math.MaxInt
	var maxBatchDelay time.Duration = time.Hour * 24

	for _, strategy := range strategies {
		if strategy.GetMaxBatchSize() < maxBatchSize {
			maxBatchSize = strategy.GetMaxBatchSize()
		}
		if strategy.GetMaxBatchCount() < maxBatchCount {
			maxBatchCount = strategy.GetMaxBatchCount()
		}
		if strategy.GetMaxBatchDelay() < maxBatchDelay {
			maxBatchDelay = strategy.GetMaxBatchDelay()
		}
	}

	return &HybridStrategy{
		BaseStrategy: NewBaseStrategy(
			"HybridStrategy",
			maxBatchSize,
			maxBatchCount,
			maxBatchDelay,
			logger,
			metricsRecorder,
			enableMetrics,
		),
		Strategies: strategies,
		RequireAll: requireAll,
	}
}

// ShouldCreateBatch determines if a batch should be created based on all strategies
func (s *HybridStrategy) ShouldCreateBatch(batchInfo BatchInfo) bool {
	// Check if batch is empty
	if batchInfo.CurrentCount == 0 {
		return false
	}

	if s.RequireAll {
		// All strategies must agree (AND logic)
		for _, strategy := range s.Strategies {
			if !strategy.ShouldCreateBatch(batchInfo) {
				return false
			}
		}
		if s.EnableMetrics {
			s.Metrics.CounterInc("batch_strategy_triggered", map[string]string{
				"strategy": s.GetName(),
				"reason":   "all_strategies",
				"target":   batchInfo.TargetTable,
			})
		}
		return true
	} else {
		// Any strategy can trigger (OR logic)
		for _, strategy := range s.Strategies {
			if strategy.ShouldCreateBatch(batchInfo) {
				if s.EnableMetrics {
					s.Metrics.CounterInc("batch_strategy_triggered", map[string]string{
						"strategy": s.GetName(),
						"reason":   "any_strategy_" + strategy.GetName(),
						"target":   batchInfo.TargetTable,
					})
				}
				return true
			}
		}
	}

	return false
}

// Reset resets all strategies
func (s *HybridStrategy) Reset() {
	for _, strategy := range s.Strategies {
		strategy.Reset()
	}
}

// Close releases all resources
func (s *HybridStrategy) Close() error {
	var lastError error
	for _, strategy := range s.Strategies {
		if err := strategy.Close(); err != nil {
			lastError = err
		}
	}
	return lastError
}

// OnBatchCreated notifies all strategies
func (s *HybridStrategy) OnBatchCreated(batchInfo BatchInfo) {
	for _, strategy := range s.Strategies {
		strategy.OnBatchCreated(batchInfo)
	}
}

// GetConfig returns the strategy configuration
func (s *HybridStrategy) GetConfig() map[string]interface{} {
	config := s.BaseStrategy.GetConfig()

	strategiesConfig := make([]map[string]interface{}, len(s.Strategies))
	for i, strategy := range s.Strategies {
		strategiesConfig[i] = strategy.GetConfig()
	}

	config["strategies"] = strategiesConfig
	config["requireAll"] = s.RequireAll
	return config
}

// Clone creates a copy of the strategy
func (s *HybridStrategy) Clone() BatchStrategy {
	baseClone := s.BaseStrategy.Clone()

	// Clone all strategies
	strategies := make([]BatchStrategy, len(s.Strategies))
	for i, strategy := range s.Strategies {
		strategies[i] = strategy.Clone()
	}

	return &HybridStrategy{
		BaseStrategy: baseClone,
		Strategies:   strategies,
		RequireAll:   s.RequireAll,
	}
}

// AdaptiveStrategy implements BatchStrategy that adapts based on system load
type AdaptiveStrategy struct {
	*BaseStrategy
	// BaseStrategy is the underlying strategy to adapt
	UnderlyingStrategy BatchStrategy
	// LoadThresholds defines thresholds for different load levels
	LoadThresholds []float64
	// SizeFactors defines size adjustment factors for different load levels
	SizeFactors []float64
	// CountFactors defines count adjustment factors for different load levels
	CountFactors []float64
	// DelayFactors defines delay adjustment factors for different load levels
	DelayFactors []float64
	// AdaptInterval is how often to adapt the strategy
	AdaptInterval time.Duration
	// LastAdaptTime is when the strategy was last adapted
	LastAdaptTime time.Time
	// BaseMaxBatchSize is the base maximum batch size
	BaseMaxBatchSize int64
	// BaseMaxBatchCount is the base maximum batch count
	BaseMaxBatchCount int
	// BaseMaxBatchDelay is the base maximum batch delay
	BaseMaxBatchDelay time.Duration
	// AdaptationEnabled controls whether adaptation is enabled
	AdaptationEnabled bool
	// Mutex protects concurrent access
	mu sync.RWMutex
}

// NewAdaptiveStrategy creates a new adaptive strategy
func NewAdaptiveStrategy(
	underlyingStrategy BatchStrategy,
	loadThresholds []float64,
	sizeFactors []float64,
	countFactors []float64,
	delayFactors []float64,
	adaptInterval time.Duration,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
	enableMetrics bool,
) (*AdaptiveStrategy, error) {
	// Validate inputs
	if len(loadThresholds) != len(sizeFactors) || len(loadThresholds) != len(countFactors) || len(loadThresholds) != len(delayFactors) {
		return nil, errors.New("load thresholds and adjustment factors must have the same length")
	}

	// Create strategy
	strategy := &AdaptiveStrategy{
		BaseStrategy: NewBaseStrategy(
			"AdaptiveStrategy",
			underlyingStrategy.GetMaxBatchSize(),
			underlyingStrategy.GetMaxBatchCount(),
			underlyingStrategy.GetMaxBatchDelay(),
			logger,
			metricsRecorder,
			enableMetrics,
		),
		UnderlyingStrategy: underlyingStrategy,
		LoadThresholds:     loadThresholds,
		SizeFactors:        sizeFactors,
		CountFactors:       countFactors,
		DelayFactors:       delayFactors,
		AdaptInterval:      adaptInterval,
		LastAdaptTime:      time.Now(),
		BaseMaxBatchSize:   underlyingStrategy.GetMaxBatchSize(),
		BaseMaxBatchCount:  underlyingStrategy.GetMaxBatchCount(),
		BaseMaxBatchDelay:  underlyingStrategy.GetMaxBatchDelay(),
		AdaptationEnabled:  true,
	}

	return strategy, nil
}

// ShouldCreateBatch determines if a batch should be created
func (s *AdaptiveStrategy) ShouldCreateBatch(batchInfo BatchInfo) bool {
	// Check if batch is empty
	if batchInfo.CurrentCount == 0 {
		return false
	}

	// Adapt strategy if needed
	s.adaptIfNeeded(batchInfo)

	// Use underlying strategy
	return s.UnderlyingStrategy.ShouldCreateBatch(batchInfo)
}

// adaptIfNeeded adapts the strategy based on system load if needed
func (s *AdaptiveStrategy) adaptIfNeeded(batchInfo BatchInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if adaptation is enabled
	if !s.AdaptationEnabled {
		return
	}

	// Check if it's time to adapt
	if time.Since(s.LastAdaptTime) < s.AdaptInterval {
		return
	}

	// Adapt based on system load
	load := batchInfo.SystemLoad.CPUUtilization
	level := 0

	// Find the appropriate level based on load
	for i, threshold := range s.LoadThresholds {
		if load >= threshold {
			level = i + 1
		} else {
			break
		}
	}

	// Apply adjustment factors
	var sizeFactor, countFactor, delayFactor float64

	if level < len(s.SizeFactors) {
		sizeFactor = s.SizeFactors[level]
	} else {
		sizeFactor = s.SizeFactors[len(s.SizeFactors)-1]
	}

	if level < len(s.CountFactors) {
		countFactor = s.CountFactors[level]
	} else {
		countFactor = s.CountFactors[len(s.CountFactors)-1]
	}

	if level < len(s.DelayFactors) {
		delayFactor = s.DelayFactors[level]
	} else {
		delayFactor = s.DelayFactors[len(s.DelayFactors)-1]
	}

	// Calculate new values
	newMaxBatchSize := int64(float64(s.BaseMaxBatchSize) * sizeFactor)
	newMaxBatchCount := int(float64(s.BaseMaxBatchCount) * countFactor)
	newMaxBatchDelay := time.Duration(float64(s.BaseMaxBatchDelay) * delayFactor)

	// Update strategy values
	s.MaxBatchSize = newMaxBatchSize
	s.MaxBatchCount = newMaxBatchCount
	s.MaxBatchDelay = newMaxBatchDelay

	// Update underlying strategy if it supports it
	if adaptable, ok := s.UnderlyingStrategy.(interface {
		SetMaxBatchSize(int64)
		SetMaxBatchCount(int)
		SetMaxBatchDelay(time.Duration)
	}); ok {
		adaptable.SetMaxBatchSize(newMaxBatchSize)
		adaptable.SetMaxBatchCount(newMaxBatchCount)
		adaptable.SetMaxBatchDelay(newMaxBatchDelay)
	}

	// Record metrics
	if s.EnableMetrics {
		s.Metrics.GaugeSet("batch_adaptive_level", float64(level), map[string]string{
			"target": batchInfo.TargetTable,
		})
		s.Metrics.GaugeSet("batch_adaptive_size", float64(newMaxBatchSize), map[string]string{
			"target": batchInfo.TargetTable,
		})
		s.Metrics.GaugeSet("batch_adaptive_count", float64(newMaxBatchCount), map[string]string{
			"target": batchInfo.TargetTable,
		})
		s.Metrics.GaugeSet("batch_adaptive_delay_ms", float64(newMaxBatchDelay.Milliseconds()), map[string]string{
			"target": batchInfo.TargetTable,
		})
	}

	// Log adaptation
	s.Logger.Debug("Adapted batch strategy",
		"load", load,
		"level", level,
		"size_factor", sizeFactor,
		"count_factor", countFactor,
		"delay_factor", delayFactor,
		"new_max_size", newMaxBatchSize,
		"new_max_count", newMaxBatchCount,
		"new_max_delay_ms", newMaxBatchDelay.Milliseconds())

	// Update last adapt time
	s.LastAdaptTime = time.Now()
}

// Reset resets the strategy
func (s *AdaptiveStrategy) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Reset underlying strategy
	s.UnderlyingStrategy.Reset()

	// Reset adaptation
	s.LastAdaptTime = time.Now()
}

// Close releases resources
func (s *AdaptiveStrategy) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.UnderlyingStrategy.Close()
}

// OnBatchCreated is called when a batch is created
func (s *AdaptiveStrategy) OnBatchCreated(batchInfo BatchInfo) {
	s.UnderlyingStrategy.OnBatchCreated(batchInfo)
}

// GetConfig returns the strategy configuration
func (s *AdaptiveStrategy) GetConfig() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	config := s.BaseStrategy.GetConfig()
	config["underlyingStrategy"] = s.UnderlyingStrategy.GetConfig()
	config["loadThresholds"] = s.LoadThresholds
	config["sizeFactors"] = s.SizeFactors
	config["countFactors"] = s.CountFactors
	config["delayFactors"] = s.DelayFactors
	config["adaptInterval"] = s.AdaptInterval.Milliseconds()
	config["adaptationEnabled"] = s.AdaptationEnabled
	return config
}

// EnableAdaptation enables or disables adaptation
func (s *AdaptiveStrategy) EnableAdaptation(enabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.AdaptationEnabled = enabled
}

// IsAdaptationEnabled returns whether adaptation is enabled
func (s *AdaptiveStrategy) IsAdaptationEnabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.AdaptationEnabled
}

// Clone creates a copy of the strategy
func (s *AdaptiveStrategy) Clone() BatchStrategy {
	s.mu.RLock()
	defer s.mu.RUnlock()

	baseClone := s.BaseStrategy.Clone()

	// Clone load thresholds and factors
	loadThresholds := make([]float64, len(s.LoadThresholds))
	copy(loadThresholds, s.LoadThresholds)

	sizeFactors := make([]float64, len(s.SizeFactors))
	copy(sizeFactors, s.SizeFactors)

	countFactors := make([]float64, len(s.CountFactors))
	copy(countFactors, s.CountFactors)

	delayFactors := make([]float64, len(s.DelayFactors))
	copy(delayFactors, s.DelayFactors)

	return &AdaptiveStrategy{
		BaseStrategy:       baseClone,
		UnderlyingStrategy: s.UnderlyingStrategy.Clone(),
		LoadThresholds:     loadThresholds,
		SizeFactors:        sizeFactors,
		CountFactors:       countFactors,
		DelayFactors:       delayFactors,
		AdaptInterval:      s.AdaptInterval,
		LastAdaptTime:      s.LastAdaptTime,
		BaseMaxBatchSize:   s.BaseMaxBatchSize,
		BaseMaxBatchCount:  s.BaseMaxBatchCount,
		BaseMaxBatchDelay:  s.BaseMaxBatchDelay,
		AdaptationEnabled:  s.AdaptationEnabled,
	}
}

// BatchStrategyType defines the type of batch strategy
type BatchStrategyType string

const (
	// TimeIntervalStrategyType represents a time-based strategy
	TimeIntervalStrategyType BatchStrategyType = "time_interval"

	// BatchSizeStrategyType represents a size-based strategy
	BatchSizeStrategyType BatchStrategyType = "batch_size"

	// BatchCountStrategyType represents a count-based strategy
	BatchCountStrategyType BatchStrategyType = "batch_count"

	// HybridStrategyType represents a hybrid strategy
	HybridStrategyType BatchStrategyType = "hybrid"

	// AdaptiveStrategyType represents an adaptive strategy
	AdaptiveStrategyType BatchStrategyType = "adaptive"
)

// BatchStrategyConfig defines configuration for a batch strategy
type BatchStrategyConfig struct {
	// Type is the strategy type
	Type BatchStrategyType `json:"type"`

	// MaxBatchSize is the maximum batch size in bytes
	MaxBatchSize int64 `json:"maxBatchSize"`

	// MinBatchSize is the minimum batch size in bytes
	MinBatchSize int64 `json:"minBatchSize"`

	// MaxBatchCount is the maximum number of messages in a batch
	MaxBatchCount int `json:"maxBatchCount"`

	// MinBatchCount is the minimum number of messages in a batch
	MinBatchCount int `json:"minBatchCount"`

	// MaxBatchDelayMS is the maximum delay before a batch is created in milliseconds
	MaxBatchDelayMS int64 `json:"maxBatchDelayMS"`

	// MinBatchDelayMS is the minimum delay before a batch is created in milliseconds
	MinBatchDelayMS int64 `json:"minBatchDelayMS"`

	// RequireAll requires all strategies to agree (AND logic) if true for hybrid strategy
	RequireAll bool `json:"requireAll"`

	// Strategies is the list of strategies for hybrid strategy
	Strategies []BatchStrategyConfig `json:"strategies"`

	// UnderlyingStrategy is the underlying strategy for adaptive strategy
	UnderlyingStrategy *BatchStrategyConfig `json:"underlyingStrategy"`

	// LoadThresholds defines thresholds for different load levels for adaptive strategy
	LoadThresholds []float64 `json:"loadThresholds"`

	// SizeFactors defines size adjustment factors for different load levels for adaptive strategy
	SizeFactors []float64 `json:"sizeFactors"`

	// CountFactors defines count adjustment factors for different load levels for adaptive strategy
	CountFactors []float64 `json:"countFactors"`

	// DelayFactors defines delay adjustment factors for different load levels for adaptive strategy
	DelayFactors []float64 `json:"delayFactors"`

	// AdaptIntervalMS is how often to adapt the strategy in milliseconds for adaptive strategy
	AdaptIntervalMS int64 `json:"adaptIntervalMS"`

	// AdaptationEnabled controls whether adaptation is enabled for adaptive strategy
	AdaptationEnabled bool `json:"adaptationEnabled"`

	// EnableMetrics enables metrics collection
	EnableMetrics bool `json:"enableMetrics"`
}

// DefaultTimeIntervalStrategyConfig returns the default time interval strategy configuration
func DefaultTimeIntervalStrategyConfig() BatchStrategyConfig {
	return BatchStrategyConfig{
		Type:            TimeIntervalStrategyType,
		MaxBatchDelayMS: 10000, // 10 seconds
		MinBatchDelayMS: 1000,  // 1 second
		EnableMetrics:   true,
	}
}

// DefaultBatchSizeStrategyConfig returns the default batch size strategy configuration
func DefaultBatchSizeStrategyConfig() BatchStrategyConfig {
	return BatchStrategyConfig{
		Type:          BatchSizeStrategyType,
		MaxBatchSize:  10 * 1024 * 1024, // 10MB
		MinBatchSize:  1 * 1024 * 1024,  // 1MB
		EnableMetrics: true,
	}
}

// DefaultBatchCountStrategyConfig returns the default batch count strategy configuration
func DefaultBatchCountStrategyConfig() BatchStrategyConfig {
	return BatchStrategyConfig{
		Type:          BatchCountStrategyType,
		MaxBatchCount: 1000,
		MinBatchCount: 100,
		EnableMetrics: true,
	}
}

// DefaultHybridStrategyConfig returns the default hybrid strategy configuration
func DefaultHybridStrategyConfig() BatchStrategyConfig {
	return BatchStrategyConfig{
		Type:          HybridStrategyType,
		RequireAll:    false,
		EnableMetrics: true,
		Strategies: []BatchStrategyConfig{
			DefaultTimeIntervalStrategyConfig(),
			DefaultBatchSizeStrategyConfig(),
			DefaultBatchCountStrategyConfig(),
		},
	}
}

// DefaultAdaptiveStrategyConfig returns the default adaptive strategy configuration
func DefaultAdaptiveStrategyConfig() BatchStrategyConfig {
	return BatchStrategyConfig{
		Type:              AdaptiveStrategyType,
		EnableMetrics:     true,
		AdaptIntervalMS:   30000, // 30 seconds
		AdaptationEnabled: true,
		LoadThresholds:    []float64{0.3, 0.6, 0.8},
		SizeFactors:       []float64{1.0, 0.8, 0.5, 0.3},
		CountFactors:      []float64{1.0, 0.8, 0.5, 0.3},
		DelayFactors:      []float64{1.0, 1.2, 1.5, 2.0},
		UnderlyingStrategy: &BatchStrategyConfig{
			Type:          HybridStrategyType,
			RequireAll:    false,
			EnableMetrics: true,
			Strategies: []BatchStrategyConfig{
				DefaultTimeIntervalStrategyConfig(),
				DefaultBatchSizeStrategyConfig(),
				DefaultBatchCountStrategyConfig(),
			},
		},
	}
}

// BatchStrategyFactory creates batch strategies from configuration
type BatchStrategyFactory struct {
	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder
}

// NewBatchStrategyFactory creates a new batch strategy factory
func NewBatchStrategyFactory(
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) *BatchStrategyFactory {
	return &BatchStrategyFactory{
		Logger:  logger,
		Metrics: metricsRecorder,
	}
}

// CreateStrategy creates a batch strategy from configuration
func (f *BatchStrategyFactory) CreateStrategy(config BatchStrategyConfig) (BatchStrategy, error) {
	switch config.Type {
	case TimeIntervalStrategyType:
		return f.createTimeIntervalStrategy(config)

	case BatchSizeStrategyType:
		return f.createBatchSizeStrategy(config)

	case BatchCountStrategyType:
		return f.createBatchCountStrategy(config)

	case HybridStrategyType:
		return f.createHybridStrategy(config)

	case AdaptiveStrategyType:
		return f.createAdaptiveStrategy(config)

	default:
		return nil, errors.Errorf("unknown strategy type: %s", config.Type)
	}
}

// createTimeIntervalStrategy creates a time interval strategy
func (f *BatchStrategyFactory) createTimeIntervalStrategy(config BatchStrategyConfig) (BatchStrategy, error) {
	// Validate configuration
	if config.MaxBatchDelayMS <= 0 {
		config.MaxBatchDelayMS = DefaultTimeIntervalStrategyConfig().MaxBatchDelayMS
	}
	if config.MinBatchDelayMS <= 0 {
		config.MinBatchDelayMS = DefaultTimeIntervalStrategyConfig().MinBatchDelayMS
	}

	// Create strategy
	return NewTimeIntervalStrategy(
		time.Duration(config.MaxBatchDelayMS)*time.Millisecond,
		time.Duration(config.MinBatchDelayMS)*time.Millisecond,
		f.Logger,
		f.Metrics,
		config.EnableMetrics,
	), nil
}

// createBatchSizeStrategy creates a batch size strategy
func (f *BatchStrategyFactory) createBatchSizeStrategy(config BatchStrategyConfig) (BatchStrategy, error) {
	// Validate configuration
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = DefaultBatchSizeStrategyConfig().MaxBatchSize
	}
	if config.MinBatchSize <= 0 {
		config.MinBatchSize = DefaultBatchSizeStrategyConfig().MinBatchSize
	}

	// Create strategy
	return NewBatchSizeStrategy(
		config.MaxBatchSize,
		config.MinBatchSize,
		f.Logger,
		f.Metrics,
		config.EnableMetrics,
	), nil
}

// createBatchCountStrategy creates a batch count strategy
func (f *BatchStrategyFactory) createBatchCountStrategy(config BatchStrategyConfig) (BatchStrategy, error) {
	// Validate configuration
	if config.MaxBatchCount <= 0 {
		config.MaxBatchCount = DefaultBatchCountStrategyConfig().MaxBatchCount
	}
	if config.MinBatchCount <= 0 {
		config.MinBatchCount = DefaultBatchCountStrategyConfig().MinBatchCount
	}

	// Create strategy
	return NewBatchCountStrategy(
		config.MaxBatchCount,
		config.MinBatchCount,
		f.Logger,
		f.Metrics,
		config.EnableMetrics,
	), nil
}

// createHybridStrategy creates a hybrid strategy
func (f *BatchStrategyFactory) createHybridStrategy(config BatchStrategyConfig) (BatchStrategy, error) {
	// Validate configuration
	if len(config.Strategies) == 0 {
		return nil, errors.New("hybrid strategy requires at least one underlying strategy")
	}

	// Create underlying strategies
	strategies := make([]BatchStrategy, len(config.Strategies))
	for i, strategyConfig := range config.Strategies {
		strategy, err := f.CreateStrategy(strategyConfig)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create underlying strategy")
		}
		strategies[i] = strategy
	}

	// Create strategy
	return NewHybridStrategy(
		strategies,
		config.RequireAll,
		f.Logger,
		f.Metrics,
		config.EnableMetrics,
	), nil
}

// createAdaptiveStrategy creates an adaptive strategy
func (f *BatchStrategyFactory) createAdaptiveStrategy(config BatchStrategyConfig) (BatchStrategy, error) {
	// Validate configuration
	if config.UnderlyingStrategy == nil {
		return nil, errors.New("adaptive strategy requires an underlying strategy")
	}
	if len(config.LoadThresholds) == 0 {
		config.LoadThresholds = DefaultAdaptiveStrategyConfig().LoadThresholds
	}
	if len(config.SizeFactors) == 0 {
		config.SizeFactors = DefaultAdaptiveStrategyConfig().SizeFactors
	}
	if len(config.CountFactors) == 0 {
		config.CountFactors = DefaultAdaptiveStrategyConfig().CountFactors
	}
	if len(config.DelayFactors) == 0 {
		config.DelayFactors = DefaultAdaptiveStrategyConfig().DelayFactors
	}
	if config.AdaptIntervalMS <= 0 {
		config.AdaptIntervalMS = DefaultAdaptiveStrategyConfig().AdaptIntervalMS
	}

	// Create underlying strategy
	underlyingStrategy, err := f.CreateStrategy(*config.UnderlyingStrategy)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create underlying strategy")
	}

	// Create strategy
	return NewAdaptiveStrategy(
		underlyingStrategy,
		config.LoadThresholds,
		config.SizeFactors,
		config.CountFactors,
		config.DelayFactors,
		time.Duration(config.AdaptIntervalMS)*time.Millisecond,
		f.Logger,
		f.Metrics,
		config.EnableMetrics,
	)
}

// StrategyEvaluator evaluates batch strategies
type StrategyEvaluator struct {
	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder

	// Start time for the current batch
	startTime time.Time

	// First message time
	firstMessageTime time.Time

	// Last message time
	lastMessageTime time.Time

	// Current size in bytes
	currentSize int64

	// Current message count
	currentCount int

	// Target table
	targetTable string

	// Target partition
	targetPartition string

	// System load provider
	systemLoadProvider func() SystemLoadInfo

	// Batch count
	batchCount int64

	// Message rate calculator
	messageRateCalculator *messageRateCalculator

	// Additional info
	additionalInfo map[string]interface{}

	// Mutex protects concurrent access
	mu sync.RWMutex
}

// NewStrategyEvaluator creates a new strategy evaluator
func NewStrategyEvaluator(
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
	targetTable string,
	targetPartition string,
	systemLoadProvider func() SystemLoadInfo,
) *StrategyEvaluator {
	now := time.Now()
	return &StrategyEvaluator{
		Logger:                logger,
		Metrics:               metricsRecorder,
		startTime:             now,
		firstMessageTime:      time.Time{}, // Zero time until first message
		lastMessageTime:       now,
		currentSize:           0,
		currentCount:          0,
		targetTable:           targetTable,
		targetPartition:       targetPartition,
		systemLoadProvider:    systemLoadProvider,
		batchCount:            0,
		messageRateCalculator: newMessageRateCalculator(10, 1*time.Second),
		additionalInfo:        make(map[string]interface{}),
	}
}

// AddMessage adds a message to the evaluator
func (e *StrategyEvaluator) AddMessage(messageSize int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()

	// Update first message time if this is the first message
	if e.currentCount == 0 {
		e.firstMessageTime = now
	}

	// Update stats
	e.currentSize += messageSize
	e.currentCount++
	e.lastMessageTime = now

	// Update message rate
	e.messageRateCalculator.AddMessage(now)
}

// ShouldCreateBatch evaluates if a batch should be created using the given strategy
func (e *StrategyEvaluator) ShouldCreateBatch(strategy BatchStrategy) bool {
	// Get current batch info
	batchInfo := e.GetBatchInfo()

	// Evaluate strategy
	return strategy.ShouldCreateBatch(batchInfo)
}

// GetBatchInfo gets current batch information
func (e *StrategyEvaluator) GetBatchInfo() BatchInfo {
	e.mu.RLock()
	defer e.mu.RUnlock()

	now := time.Now()
	var elapsedTime time.Duration
	if e.currentCount > 0 {
		elapsedTime = now.Sub(e.startTime)
	}

	// Get system load if provider available
	var systemLoad SystemLoadInfo
	if e.systemLoadProvider != nil {
		systemLoad = e.systemLoadProvider()
	}

	return BatchInfo{
		CurrentSize:      e.currentSize,
		CurrentCount:     e.currentCount,
		ElapsedTime:      elapsedTime,
		FirstMessageTime: e.firstMessageTime,
		LastMessageTime:  e.lastMessageTime,
		TargetTable:      e.targetTable,
		TargetPartition:  e.targetPartition,
		SystemLoad:       systemLoad,
		BatchCount:       e.batchCount,
		MessageRate:      e.messageRateCalculator.GetRate(),
		AdditionalInfo:   e.additionalInfo,
	}
}

// Reset resets the evaluator for a new batch
func (e *StrategyEvaluator) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	e.startTime = now
	e.firstMessageTime = time.Time{} // Zero time until first message
	e.lastMessageTime = now
	e.currentSize = 0
	e.currentCount = 0
	e.batchCount++
	e.additionalInfo = make(map[string]interface{})
}

// SetAdditionalInfo sets additional information
func (e *StrategyEvaluator) SetAdditionalInfo(key string, value interface{}) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.additionalInfo[key] = value
}

// messageRateCalculator calculates message rate
type messageRateCalculator struct {
	// messageTimestamps tracks recent message timestamps
	messageTimestamps []time.Time

	// position is the current position in the ring buffer
	position int

	// windowSize is the number of messages to track
	windowSize int

	// windowDuration is the time window for rate calculation
	windowDuration time.Duration

	// mutex protects concurrent access
	mu sync.RWMutex
}

// newMessageRateCalculator creates a new message rate calculator
func newMessageRateCalculator(windowSize int, windowDuration time.Duration) *messageRateCalculator {
	return &messageRateCalculator{
		messageTimestamps: make([]time.Time, windowSize),
		position:          0,
		windowSize:        windowSize,
		windowDuration:    windowDuration,
	}
}

// AddMessage adds a message timestamp
func (c *messageRateCalculator) AddMessage(timestamp time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.messageTimestamps[c.position] = timestamp
	c.position = (c.position + 1) % c.windowSize
}

// GetRate calculates the current message rate (messages per second)
func (c *messageRateCalculator) GetRate() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	now := time.Now()
	count := 0

	// Count messages within window duration
	for _, timestamp := range c.messageTimestamps {
		if !timestamp.IsZero() && now.Sub(timestamp) <= c.windowDuration {
			count++
		}
	}

	// Calculate rate
	return float64(count) / c.windowDuration.Seconds()
}

// PerformanceMonitor monitors system performance for adaptive strategies
type PerformanceMonitor struct {
	// CPUUtilizationProvider provides CPU utilization
	CPUUtilizationProvider func() (float64, error)

	// MemoryUtilizationProvider provides memory utilization
	MemoryUtilizationProvider func() (float64, error)

	// NetworkUtilizationProvider provides network utilization
	NetworkUtilizationProvider func() (float64, error)

	// DiskUtilizationProvider provides disk utilization
	DiskUtilizationProvider func() (float64, error)

	// QueueSizeProvider provides queue size
	QueueSizeProvider func() (int, error)

	// ProcessingDelayProvider provides processing delay
	ProcessingDelayProvider func() (time.Duration, error)

	// ErrorRateProvider provides error rate
	ErrorRateProvider func() (float64, error)

	// UpdateInterval is how often to update metrics
	UpdateInterval time.Duration

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

	// Current system load
	currentSystemLoad SystemLoadInfo

	// Mutex protects concurrent access
	mu sync.RWMutex

	// Running indicates if the monitor is running
	running bool
}

// NewPerformanceMonitor creates a new performance monitor
func NewPerformanceMonitor(
	updateInterval time.Duration,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) *PerformanceMonitor {
	ctx, cancel := context.WithCancel(context.Background())

	return &PerformanceMonitor{
		UpdateInterval: updateInterval,
		Logger:         logger,
		Metrics:        metricsRecorder,
		ctx:            ctx,
		cancel:         cancel,
		currentSystemLoad: SystemLoadInfo{
			CPUUtilization:     0,
			MemoryUtilization:  0,
			NetworkUtilization: 0,
			DiskUtilization:    0,
			QueueSize:          0,
			ProcessingDelay:    0,
			ErrorRate:          0,
		},
		running: false,
	}
}

// Start starts the performance monitor
func (m *PerformanceMonitor) Start() error {
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

	m.Logger.Info("Performance monitor started")
	return nil
}

// Stop stops the performance monitor
func (m *PerformanceMonitor) Stop() error {
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

	m.Logger.Info("Performance monitor stopped")
	return nil
}

// GetSystemLoad gets the current system load
func (m *PerformanceMonitor) GetSystemLoad() SystemLoadInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentSystemLoad
}

// monitorLoop is the main monitoring loop
func (m *PerformanceMonitor) monitorLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.UpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.updateMetrics()
		case <-m.ctx.Done():
			return
		}
	}
}

// updateMetrics updates performance metrics
func (m *PerformanceMonitor) updateMetrics() {
	systemLoad := SystemLoadInfo{
		CPUUtilization:     0,
		MemoryUtilization:  0,
		NetworkUtilization: 0,
		DiskUtilization:    0,
		QueueSize:          0,
		ProcessingDelay:    0,
		ErrorRate:          0,
	}

	// Get CPU utilization
	if m.CPUUtilizationProvider != nil {
		if cpuUtil, err := m.CPUUtilizationProvider(); err == nil {
			systemLoad.CPUUtilization = cpuUtil
			m.Metrics.GaugeSet("system_cpu_utilization", cpuUtil, nil)
		} else {
			m.Logger.Warn("Failed to get CPU utilization", "error", err)
		}
	}

	// Get memory utilization
	if m.MemoryUtilizationProvider != nil {
		if memUtil, err := m.MemoryUtilizationProvider(); err == nil {
			systemLoad.MemoryUtilization = memUtil
			m.Metrics.GaugeSet("system_memory_utilization", memUtil, nil)
		} else {
			m.Logger.Warn("Failed to get memory utilization", "error", err)
		}
	}

	// Get network utilization
	if m.NetworkUtilizationProvider != nil {
		if netUtil, err := m.NetworkUtilizationProvider(); err == nil {
			systemLoad.NetworkUtilization = netUtil
			m.Metrics.GaugeSet("system_network_utilization", netUtil, nil)
		} else {
			m.Logger.Warn("Failed to get network utilization", "error", err)
		}
	}

	// Get disk utilization
	if m.DiskUtilizationProvider != nil {
		if diskUtil, err := m.DiskUtilizationProvider(); err == nil {
			systemLoad.DiskUtilization = diskUtil
			m.Metrics.GaugeSet("system_disk_utilization", diskUtil, nil)
		} else {
			m.Logger.Warn("Failed to get disk utilization", "error", err)
		}
	}

	// Get queue size
	if m.QueueSizeProvider != nil {
		if queueSize, err := m.QueueSizeProvider(); err == nil {
			systemLoad.QueueSize = queueSize
			m.Metrics.GaugeSet("system_queue_size", float64(queueSize), nil)
		} else {
			m.Logger.Warn("Failed to get queue size", "error", err)
		}
	}

	// Get processing delay
	if m.ProcessingDelayProvider != nil {
		if procDelay, err := m.ProcessingDelayProvider(); err == nil {
			systemLoad.ProcessingDelay = procDelay
			m.Metrics.GaugeSet("system_processing_delay_ms", float64(procDelay.Milliseconds()), nil)
		} else {
			m.Logger.Warn("Failed to get processing delay", "error", err)
		}
	}

	// Get error rate
	if m.ErrorRateProvider != nil {
		if errRate, err := m.ErrorRateProvider(); err == nil {
			systemLoad.ErrorRate = errRate
			m.Metrics.GaugeSet("system_error_rate", errRate, nil)
		} else {
			m.Logger.Warn("Failed to get error rate", "error", err)
		}
	}

	// Update current system load
	m.mu.Lock()
	m.currentSystemLoad = systemLoad
	m.mu.Unlock()
}

// RegisterMetrics registers metrics for batch strategies
func RegisterMetrics(registry metrics.MetricsRecorder) {
	// Register counters
	registry.RegisterCounter("batch_strategy_triggered", "Number of times a batch strategy was triggered")

	// Register gauges
	registry.RegisterGauge("batch_adaptive_level", "Current adaptation level")
	registry.RegisterGauge("batch_adaptive_size", "Current adaptive batch size")
	registry.RegisterGauge("batch_adaptive_count", "Current adaptive batch count")
	registry.RegisterGauge("batch_adaptive_delay_ms", "Current adaptive batch delay in ms")

	// Register system metrics
	registry.RegisterGauge("system_cpu_utilization", "System CPU utilization")
	registry.RegisterGauge("system_memory_utilization", "System memory utilization")
	registry.RegisterGauge("system_network_utilization", "System network utilization")
	registry.RegisterGauge("system_disk_utilization", "System disk utilization")
	registry.RegisterGauge("system_queue_size", "System queue size")
	registry.RegisterGauge("system_processing_delay_ms", "System processing delay in ms")
	registry.RegisterGauge("system_error_rate", "System error rate")
}

// BatchStrategyManager manages batch strategies for multiple targets
type BatchStrategyManager struct {
	// Factory is the strategy factory
	Factory *BatchStrategyFactory

	// DefaultConfig is the default configuration
	DefaultConfig BatchStrategyConfig

	// Strategies maps target to strategy
	Strategies map[string]BatchStrategy

	// PerformanceMonitor monitors system performance
	PerformanceMonitor *PerformanceMonitor

	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder

	// Mutex protects concurrent access
	mu sync.RWMutex
}

// NewBatchStrategyManager creates a new batch strategy manager
func NewBatchStrategyManager(
	factory *BatchStrategyFactory,
	defaultConfig BatchStrategyConfig,
	performanceMonitor *PerformanceMonitor,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) *BatchStrategyManager {
	return &BatchStrategyManager{
		Factory:            factory,
		DefaultConfig:      defaultConfig,
		Strategies:         make(map[string]BatchStrategy),
		PerformanceMonitor: performanceMonitor,
		Logger:             logger,
		Metrics:            metricsRecorder,
	}
}

// GetStrategy gets a strategy for a target
func (m *BatchStrategyManager) GetStrategy(target string) (BatchStrategy, error) {
	m.mu.RLock()
	strategy, ok := m.Strategies[target]
	m.mu.RUnlock()

	// If strategy exists, return it
	if ok {
		return strategy, nil
	}

	// Create new strategy
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check again in case another goroutine created it
	strategy, ok = m.Strategies[target]
	if ok {
		return strategy, nil
	}

	// Create new strategy
	var err error
	strategy, err = m.Factory.CreateStrategy(m.DefaultConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create strategy for target %s", target)
	}

	// Store strategy
	m.Strategies[target] = strategy

	return strategy, nil
}

// SetStrategyConfig sets the configuration for a target
func (m *BatchStrategyManager) SetStrategyConfig(target string, config BatchStrategyConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Create new strategy
	strategy, err := m.Factory.CreateStrategy(config)
	if err != nil {
		return errors.Wrapf(err, "failed to create strategy for target %s", target)
	}

	// Close existing strategy if any
	if existingStrategy, ok := m.Strategies[target]; ok {
		if err := existingStrategy.Close(); err != nil {
			m.Logger.Warn("Failed to close existing strategy", "error", err, "target", target)
		}
	}

	// Store new strategy
	m.Strategies[target] = strategy

	return nil
}

// Close closes all strategies
func (m *BatchStrategyManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for target, strategy := range m.Strategies {
		if err := strategy.Close(); err != nil {
			m.Logger.Warn("Failed to close strategy", "error", err, "target", target)
			lastErr = err
		}
	}

	// Clear strategies
	m.Strategies = make(map[string]BatchStrategy)

	return lastErr
}

//Personal.AI order the ending
