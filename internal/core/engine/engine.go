// Package engine provides the core execution engine for the system.
package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	logging "github.com/turtacn/staravail/internal/common/logger"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/domain/health"
	"github.com/turtacn/staravail/internal/infrastructure/config"
)

// ExecutionStrategy defines different strategies for query execution.
type ExecutionStrategy string

const (
	// StrategyFastest executes on the fastest available node.
	StrategyFastest ExecutionStrategy = "fastest"
	// StrategyParallel executes in parallel on multiple nodes and uses the first result.
	StrategyParallel ExecutionStrategy = "parallel"
	// StrategyBalanced distributes load evenly across nodes.
	StrategyBalanced ExecutionStrategy = "balanced"
	// StrategyConsistent always uses the same node for the same query.
	StrategyConsistent ExecutionStrategy = "consistent"
	// StrategyCached uses cached results when available.
	StrategyCached ExecutionStrategy = "cached"
)

// QueryResult represents the result of a query execution.
type QueryResult struct {
	Data           interface{}   `json:"data"`
	Columns        []string      `json:"columns"`
	RowCount       int           `json:"row_count"`
	ExecutionTime  time.Duration `json:"execution_time"`
	ExecutedOn     string        `json:"executed_on"`
	CacheHit       bool          `json:"cache_hit"`
	Strategy       string        `json:"strategy"`
	ErrorMessage   string        `json:"error_message,omitempty"`
	WarningMessage string        `json:"warning_message,omitempty"`
	Metadata       interface{}   `json:"metadata,omitempty"`
}

// WriteResult represents the result of a write operation.
type WriteResult struct {
	Success        bool          `json:"success"`
	RowsAffected   int64         `json:"rows_affected"`
	ExecutionTime  time.Duration `json:"execution_time"`
	ExecutedOn     string        `json:"executed_on"`
	Strategy       string        `json:"strategy"`
	ErrorMessage   string        `json:"error_message,omitempty"`
	WarningMessage string        `json:"warning_message,omitempty"`
	Metadata       interface{}   `json:"metadata,omitempty"`
}

// ExecutionRequest represents a generic execution request.
type ExecutionRequest struct {
	Statement     string            `json:"statement"`
	Database      string            `json:"database"`
	User          string            `json:"user"`
	Timeout       time.Duration     `json:"timeout"`
	Priority      int               `json:"priority"`
	RequestID     string            `json:"request_id"`
	Parameters    []interface{}     `json:"parameters,omitempty"`
	Hints         map[string]string `json:"hints,omitempty"`
	MaxRetries    int               `json:"max_retries"`
	TargetNodeIDs []string          `json:"target_node_ids,omitempty"`
	ExecutionMode string            `json:"execution_mode,omitempty"`
}

// ExecutionStats represents execution statistics.
type ExecutionStats struct {
	TotalQueries         int64            `json:"total_queries"`
	TotalWrites          int64            `json:"total_writes"`
	SuccessfulQueries    int64            `json:"successful_queries"`
	SuccessfulWrites     int64            `json:"successful_writes"`
	FailedQueries        int64            `json:"failed_queries"`
	FailedWrites         int64            `json:"failed_writes"`
	AverageQueryTime     time.Duration    `json:"average_query_time"`
	AverageWriteTime     time.Duration    `json:"average_write_time"`
	QueryTimePercentiles map[int]int64    `json:"query_time_percentiles"`
	WriteTimePercentiles map[int]int64    `json:"write_time_percentiles"`
	CacheHitRate         float64          `json:"cache_hit_rate"`
	QueryConcurrency     int64            `json:"query_concurrency"`
	WriteConcurrency     int64            `json:"write_concurrency"`
	QueryErrors          map[string]int64 `json:"query_errors"`
	WriteErrors          map[string]int64 `json:"write_errors"`
	StrategyDistribution map[string]int64 `json:"strategy_distribution"`
	ResourceUtilization  float64          `json:"resource_utilization"`
	AvgQueueTime         time.Duration    `json:"avg_queue_time"`
	MaxQueueTime         time.Duration    `json:"max_queue_time"`
	CurrentQueueSize     int              `json:"current_queue_size"`
	TotalExecutions      int64            `json:"total_executions"`
	ExecutionsPerSecond  float64          `json:"executions_per_second"`
}

// ExecutionPlan represents a plan for executing a request.
type ExecutionPlan struct {
	Strategy       ExecutionStrategy `json:"strategy"`
	TargetNodeIDs  []string          `json:"target_node_ids"`
	Timeout        time.Duration     `json:"timeout"`
	MaxRetries     int               `json:"max_retries"`
	ParallelDegree int               `json:"parallel_degree"`
	Priority       int               `json:"priority"`
	CacheEnabled   bool              `json:"cache_enabled"`
	CacheTTL       time.Duration     `json:"cache_ttl"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// Engine defines the interface for the execution engine.
type Engine interface {
	// ExecuteQuery executes a query and returns the result.
	ExecuteQuery(ctx context.Context, req *ExecutionRequest) (*QueryResult, error)

	// ExecuteWrite executes a write operation and returns the result.
	ExecuteWrite(ctx context.Context, req *ExecutionRequest) (*WriteResult, error)

	// GetExecutionStats returns statistics about executions.
	GetExecutionStats() *ExecutionStats

	// Start starts the engine and prepares it for handling requests.
	Start() error

	// Stop stops the engine and releases resources.
	Stop() error

	// IsHealthy returns whether the engine is healthy.
	IsHealthy() bool
}

// StarRocksEngine implements the Engine interface.
type StarRocksEngine struct {
	queryService  query.QueryService
	writeService  write.WriteService
	healthService health.HealthService
	metrics       *metrics.Registry
	config        *config.EngineConfig
	logger        *zap.Logger

	// Execution control
	querySemaphore *semaphore.Weighted
	writeSemaphore *semaphore.Weighted
	currentQueries int64
	currentWrites  int64

	// Stats tracking
	totalQueries      int64
	totalWrites       int64
	successfulQueries int64
	successfulWrites  int64
	failedQueries     int64
	failedWrites      int64
	totalQueryTime    int64 // in nanoseconds
	totalWriteTime    int64 // in nanoseconds
	cacheHits         int64
	cacheMisses       int64
	queueTimeTotal    int64 // in nanoseconds
	queueTimeCount    int64
	maxQueueTime      int64 // in nanoseconds

	// Query execution times for percentile calculation
	queryTimes      []int64 // in nanoseconds
	writeTimes      []int64 // in nanoseconds
	queryTimesMutex sync.Mutex
	writeTimesMutex sync.Mutex

	// Error tracking
	queryErrors map[string]int64
	writeErrors map[string]int64
	errorsMutex sync.RWMutex

	// Strategy usage tracking
	strategyUsage map[string]int64
	strategyMutex sync.RWMutex

	// Execution concurrency tracking
	concurrencyHistogram []int64 // index is concurrency level, value is count
	concurrencyMutex     sync.Mutex

	// Prometheus metrics
	queryLatency              prometheus.Histogram
	writeLatency              prometheus.Histogram
	queueTimeHistogram        prometheus.Histogram
	executionsTotal           prometheus.Counter
	executionErrors           *prometheus.CounterVec
	concurrencyGauge          prometheus.Gauge
	cacheHitRatio             prometheus.Gauge
	resourceUtilization       prometheus.Gauge
	executionsPerSecond       prometheus.Gauge
	queryStrategyDistribution *prometheus.CounterVec

	// State
	healthy   bool
	started   bool
	startTime time.Time
	mu        sync.RWMutex

	// Execution queue
	queryQueue chan *executionTask
	writeQueue chan *executionTask
	queueWg    sync.WaitGroup

	// Shutdown
	stopCh   chan struct{}
	workerWg sync.WaitGroup
}

// executionTask represents a task in the execution queue.
type executionTask struct {
	req       *ExecutionRequest
	ctx       context.Context
	plan      *ExecutionPlan
	resultCh  chan interface{}
	errCh     chan error
	queueTime time.Time
	strategy  ExecutionStrategy
	isWrite   bool
}

// NewStarRocksEngine creates a new StarRocksEngine.
func NewStarRocksEngine(
	queryService query.QueryService,
	writeService write.WriteService,
	healthService health.HealthService,
	metricsRegistry *metrics.Registry,
	config *config.EngineConfig,
) *StarRocksEngine {
	logger := logging.GetLogger().Named("engine")

	// Initialize execution semaphores
	querySemaphore := semaphore.NewWeighted(int64(config.MaxConcurrentQueries))
	writeSemaphore := semaphore.NewWeighted(int64(config.MaxConcurrentWrites))

	// Initialize query and write queues
	queryQueue := make(chan *executionTask, config.QueryQueueSize)
	writeQueue := make(chan *executionTask, config.WriteQueueSize)

	engine := &StarRocksEngine{
		queryService:         queryService,
		writeService:         writeService,
		healthService:        healthService,
		metrics:              metricsRegistry,
		config:               config,
		logger:               logger,
		querySemaphore:       querySemaphore,
		writeSemaphore:       writeSemaphore,
		queryErrors:          make(map[string]int64),
		writeErrors:          make(map[string]int64),
		strategyUsage:        make(map[string]int64),
		concurrencyHistogram: make([]int64, config.MaxConcurrentQueries+config.MaxConcurrentWrites+1),
		queryQueue:           queryQueue,
		writeQueue:           writeQueue,
		stopCh:               make(chan struct{}),
		healthy:              true,
	}

	// Initialize metrics
	engine.initializeMetrics(metricsRegistry)

	return engine
}

// initializeMetrics initializes Prometheus metrics.
func (e *StarRocksEngine) initializeMetrics(registry *metrics.Registry) {
	// Query latency histogram
	e.queryLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "starrocks_proxy",
		Subsystem: "engine",
		Name:      "query_execution_time_seconds",
		Help:      "Time taken to execute queries",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15), // from 1ms to ~16s
	})

	// Write latency histogram
	e.writeLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "starrocks_proxy",
		Subsystem: "engine",
		Name:      "write_execution_time_seconds",
		Help:      "Time taken to execute write operations",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15), // from 1ms to ~16s
	})

	// Queue time histogram
	e.queueTimeHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "starrocks_proxy",
		Subsystem: "engine",
		Name:      "queue_time_seconds",
		Help:      "Time spent in the execution queue",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 10), // from 1ms to ~1s
	})

	// Total executions counter
	e.executionsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "starrocks_proxy",
		Subsystem: "engine",
		Name:      "executions_total",
		Help:      "Total number of executed operations",
	})

	// Execution errors counter
	e.executionErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "starrocks_proxy",
			Subsystem: "engine",
			Name:      "execution_errors_total",
			Help:      "Total number of execution errors by type",
		},
		[]string{"error_type", "operation_type"},
	)

	// Concurrency gauge
	e.concurrencyGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "starrocks_proxy",
		Subsystem: "engine",
		Name:      "current_concurrency",
		Help:      "Current number of concurrent executions",
	})

	// Cache hit ratio
	e.cacheHitRatio = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "starrocks_proxy",
		Subsystem: "engine",
		Name:      "cache_hit_ratio",
		Help:      "Ratio of cache hits to total executions",
	})

	// Resource utilization
	e.resourceUtilization = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "starrocks_proxy",
		Subsystem: "engine",
		Name:      "resource_utilization",
		Help:      "Resource utilization as a ratio of used to available",
	})

	// Executions per second
	e.executionsPerSecond = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "starrocks_proxy",
		Subsystem: "engine",
		Name:      "executions_per_second",
		Help:      "Number of executions per second",
	})

	// Strategy distribution
	e.queryStrategyDistribution = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "starrocks_proxy",
			Subsystem: "engine",
			Name:      "strategy_usage_total",
			Help:      "Total usage of each execution strategy",
		},
		[]string{"strategy"},
	)

	// Register metrics with the registry
	registry.RegisterMetric("query_execution_time", e.queryLatency)
	registry.RegisterMetric("write_execution_time", e.writeLatency)
	registry.RegisterMetric("queue_time", e.queueTimeHistogram)
	registry.RegisterMetric("executions_total", e.executionsTotal)
	registry.RegisterMetric("execution_errors", e.executionErrors)
	registry.RegisterMetric("current_concurrency", e.concurrencyGauge)
	registry.RegisterMetric("cache_hit_ratio", e.cacheHitRatio)
	registry.RegisterMetric("resource_utilization", e.resourceUtilization)
	registry.RegisterMetric("executions_per_second", e.executionsPerSecond)
	registry.RegisterMetric("strategy_usage", e.queryStrategyDistribution)
}

// Start starts the execution engine.
func (e *StarRocksEngine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.started {
		return nil
	}

	e.logger.Info("Starting StarRocks execution engine",
		zap.Int("max_concurrent_queries", e.config.MaxConcurrentQueries),
		zap.Int("max_concurrent_writes", e.config.MaxConcurrentWrites),
		zap.Int("query_queue_size", e.config.QueryQueueSize),
		zap.Int("write_queue_size", e.config.WriteQueueSize),
	)

	// Start worker pools
	e.startWorkerPools()

	// Start metrics updater
	e.startMetricsUpdater()

	e.started = true
	e.startTime = time.Now()
	e.healthy = true

	e.logger.Info("StarRocks execution engine started")

	return nil
}

// startWorkerPools starts the query and write worker pools.
func (e *StarRocksEngine) startWorkerPools() {
	// Start query workers
	for i := 0; i < e.config.QueryWorkerCount; i++ {
		e.workerWg.Add(1)
		go func(workerID int) {
			defer e.workerWg.Done()
			e.queryWorker(workerID)
		}(i)
	}

	// Start write workers
	for i := 0; i < e.config.WriteWorkerCount; i++ {
		e.workerWg.Add(1)
		go func(workerID int) {
			defer e.workerWg.Done()
			e.writeWorker(workerID)
		}(i)
	}
}

// queryWorker processes tasks from the query queue.
func (e *StarRocksEngine) queryWorker(workerID int) {
	e.logger.Debug("Query worker started", zap.Int("worker_id", workerID))

	for {
		select {
		case <-e.stopCh:
			e.logger.Debug("Query worker stopping", zap.Int("worker_id", workerID))
			return
		case task, ok := <-e.queryQueue:
			if !ok {
				return
			}

			// Calculate queue time
			queueDuration := time.Since(task.queueTime)
			atomic.AddInt64(&e.queueTimeTotal, int64(queueDuration))
			atomic.AddInt64(&e.queueTimeCount, 1)
			current := int64(queueDuration)
			for {
				old := atomic.LoadInt64(&e.maxQueueTime)
				if current <= old || atomic.CompareAndSwapInt64(&e.maxQueueTime, old, current) {
					break
				}
			}
			e.queueTimeHistogram.Observe(queueDuration.Seconds())

			// Execute the query
			result, err := e.executeQueryTask(task)

			// Send result or error back
			if err != nil {
				task.errCh <- err
				close(task.resultCh)
			} else {
				task.resultCh <- result
				close(task.errCh)
			}
		}
	}
}

// writeWorker processes tasks from the write queue.
func (e *StarRocksEngine) writeWorker(workerID int) {
	e.logger.Debug("Write worker started", zap.Int("worker_id", workerID))

	for {
		select {
		case <-e.stopCh:
			e.logger.Debug("Write worker stopping", zap.Int("worker_id", workerID))
			return
		case task, ok := <-e.writeQueue:
			if !ok {
				return
			}

			// Calculate queue time
			queueDuration := time.Since(task.queueTime)
			atomic.AddInt64(&e.queueTimeTotal, int64(queueDuration))
			atomic.AddInt64(&e.queueTimeCount, 1)
			current := int64(queueDuration)
			for {
				old := atomic.LoadInt64(&e.maxQueueTime)
				if current <= old || atomic.CompareAndSwapInt64(&e.maxQueueTime, old, current) {
					break
				}
			}
			e.queueTimeHistogram.Observe(queueDuration.Seconds())

			// Execute the write
			result, err := e.executeWriteTask(task)

			// Send result or error back
			if err != nil {
				task.errCh <- err
				close(task.resultCh)
			} else {
				task.resultCh <- result
				close(task.errCh)
			}
		}
	}
}

// executeQueryTask executes a query task.
func (e *StarRocksEngine) executeQueryTask(task *executionTask) (*QueryResult, error) {
	// Increment concurrency counter
	atomic.AddInt64(&e.currentQueries, 1)
	defer atomic.AddInt64(&e.currentQueries, -1)

	// Update concurrency metrics
	totalConcurrency := atomic.LoadInt64(&e.currentQueries) + atomic.LoadInt64(&e.currentWrites)
	e.concurrencyGauge.Set(float64(totalConcurrency))
	e.updateConcurrencyHistogram(int(totalConcurrency))

	// Acquire semaphore
	ctx, cancel := context.WithTimeout(task.ctx, task.req.Timeout)
	defer cancel()

	if err := e.querySemaphore.Acquire(ctx, 1); err != nil {
		e.recordQueryError("SEMAPHORE_ACQUISITION_FAILED", task.req)
		return nil, fmt.Errorf("failed to acquire query semaphore: %w", err)
	}
	defer e.querySemaphore.Release(1)

	// Execute according to the plan
	startTime := time.Now()

	var result *query.Result
	var err error

	// Record the strategy usage
	e.recordStrategyUsage(string(task.strategy))

	switch task.strategy {
	case StrategyFastest:
		result, err = e.executeQueryFastest(ctx, task.req, task.plan)
	case StrategyParallel:
		result, err = e.executeQueryParallel(ctx, task.req, task.plan)
	case StrategyBalanced:
		result, err = e.executeQueryBalanced(ctx, task.req, task.plan)
	case StrategyConsistent:
		result, err = e.executeQueryConsistent(ctx, task.req, task.plan)
	case StrategyCached:
		result, err = e.executeQueryCached(ctx, task.req, task.plan)
	default:
		// Default to balanced
		result, err = e.executeQueryBalanced(ctx, task.req, task.plan)
	}

	executionTime := time.Since(startTime)
	e.recordQueryTime(executionTime)

	// Update metrics
	e.queryLatency.Observe(executionTime.Seconds())
	atomic.AddInt64(&e.totalQueries, 1)

	if err != nil {
		atomic.AddInt64(&e.failedQueries, 1)
		e.recordQueryError(e.classifyError(err), task.req)
		return nil, err
	}

	atomic.AddInt64(&e.successfulQueries, 1)

	// Prepare result
	queryResult := &QueryResult{
		Data:           result.Data,
		Columns:        result.Columns,
		RowCount:       result.RowCount,
		ExecutionTime:  executionTime,
		ExecutedOn:     result.NodeID,
		CacheHit:       result.CacheHit,
		Strategy:       string(task.strategy),
		WarningMessage: result.WarningMessage,
		Metadata:       result.Metadata,
	}

	// Update cache hit stats
	if result.CacheHit {
		atomic.AddInt64(&e.cacheHits, 1)
	} else {
		atomic.AddInt64(&e.cacheMisses, 1)
	}

	return queryResult, nil
}

// executeWriteTask executes a write task.
func (e *StarRocksEngine) executeWriteTask(task *executionTask) (*WriteResult, error) {
	// Increment concurrency counter
	atomic.AddInt64(&e.currentWrites, 1)
	defer atomic.AddInt64(&e.currentWrites, -1)

	// Update concurrency metrics
	totalConcurrency := atomic.LoadInt64(&e.currentQueries) + atomic.LoadInt64(&e.currentWrites)
	e.concurrencyGauge.Set(float64(totalConcurrency))
	e.updateConcurrencyHistogram(int(totalConcurrency))

	// Acquire semaphore
	ctx, cancel := context.WithTimeout(task.ctx, task.req.Timeout)
	defer cancel()

	if err := e.writeSemaphore.Acquire(ctx, 1); err != nil {
		e.recordWriteError("SEMAPHORE_ACQUISITION_FAILED", task.req)
		return nil, fmt.Errorf("failed to acquire write semaphore: %w", err)
	}
	defer e.writeSemaphore.Release(1)

	// Execute according to the plan
	startTime := time.Now()

	var result *write.Result
	var err error

	// Record the strategy usage
	e.recordStrategyUsage(string(task.strategy))

	// For writes, we typically just have one strategy because consistency is important
	result, err = e.executeWrite(ctx, task.req, task.plan)

	executionTime := time.Since(startTime)
	e.recordWriteTime(executionTime)

	// Update metrics
	e.writeLatency.Observe(executionTime.Seconds())
	atomic.AddInt64(&e.totalWrites, 1)

	if err != nil {
		atomic.AddInt64(&e.failedWrites, 1)
		e.recordWriteError(e.classifyError(err), task.req)
		return nil, err
	}

	atomic.AddInt64(&e.successfulWrites, 1)

	// Prepare result
	writeResult := &WriteResult{
		Success:        true,
		RowsAffected:   result.RowsAffected,
		ExecutionTime:  executionTime,
		ExecutedOn:     result.NodeID,
		Strategy:       string(task.strategy),
		WarningMessage: result.WarningMessage,
		Metadata:       result.Metadata,
	}

	return writeResult, nil
}

// startMetricsUpdater starts a goroutine to periodically update metrics.
func (e *StarRocksEngine) startMetricsUpdater() {
	e.workerWg.Add(1)
	go func() {
		defer e.workerWg.Done()

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		var lastExecutionCount int64
		var lastTime time.Time = time.Now()

		for {
			select {
			case <-e.stopCh:
				return
			case <-ticker.C:
				// Update cache hit ratio
				e.updateCacheHitRatio()

				// Update resource utilization
				e.updateResourceUtilization()

				// Update executions per second
				now := time.Now()
				totalExecutions := atomic.LoadInt64(&e.totalQueries) + atomic.LoadInt64(&e.totalWrites)
				elapsed := now.Sub(lastTime).Seconds()
				if elapsed > 0 {
					eps := float64(totalExecutions-lastExecutionCount) / elapsed
					e.executionsPerSecond.Set(eps)
					lastExecutionCount = totalExecutions
					lastTime = now
				}

				// Check health with health service
				if e.healthService != nil {
					healthy, _ := e.healthService.IsHealthy("engine")
					e.mu.Lock()
					e.healthy = healthy
					e.mu.Unlock()
				}
			}
		}
	}()
}

// Stop stops the execution engine.
func (e *StarRocksEngine) Stop() error {
	e.mu.Lock()
	if !e.started {
		e.mu.Unlock()
		return nil
	}
	e.started = false
	e.mu.Unlock()

	e.logger.Info("Stopping StarRocks execution engine")

	// Signal all workers to stop
	close(e.stopCh)

	// Wait for all workers to finish
	done := make(chan struct{})
	go func() {
		e.workerWg.Wait()
		close(done)
	}()

	// Wait with timeout
	select {
	case <-done:
		e.logger.Info("All workers stopped successfully")
	case <-time.After(30 * time.Second):
		e.logger.Warn("Timeout waiting for workers to stop")
	}

	// Close queues
	close(e.queryQueue)
	close(e.writeQueue)

	e.logger.Info("StarRocks execution engine stopped")

	return nil
}

// IsHealthy returns whether the engine is healthy.
func (e *StarRocksEngine) IsHealthy() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.started && e.healthy
}

// ExecuteQuery executes a query.
func (e *StarRocksEngine) ExecuteQuery(ctx context.Context, req *ExecutionRequest) (*QueryResult, error) {
	e.mu.RLock()
	if !e.started {
		e.mu.RUnlock()
		return nil, errors.New("engine not started")
	}
	e.mu.RUnlock()

	// Validate request
	if req == nil || req.Statement == "" {
		return nil, errors.New("invalid query request")
	}

	// Set default timeout if not specified
	if req.Timeout <= 0 {
		req.Timeout = time.Duration(e.config.DefaultQueryTimeoutSeconds) * time.Second
	}

	// Create execution plan
	plan, err := e.createQueryExecutionPlan(req)
	if err != nil {
		return nil, fmt.Errorf("failed to create execution plan: %w", err)
	}

	// Create execution task
	task := &executionTask{
		req:       req,
		ctx:       ctx,
		plan:      plan,
		resultCh:  make(chan interface{}, 1),
		errCh:     make(chan error, 1),
		queueTime: time.Now(),
		strategy:  plan.Strategy,
		isWrite:   false,
	}

	// Submit task to queue
	select {
	case e.queryQueue <- task:
		// Task submitted successfully
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Queue is full
		return nil, errors.New("query queue is full")
	}

	// Wait for result or error
	select {
	case result := <-task.resultCh:
		if result == nil {
			// This shouldn't happen if the channels are used correctly
			return nil, errors.New("received nil result")
		}
		return result.(*QueryResult), nil
	case err := <-task.errCh:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ExecuteWrite executes a write operation.
func (e *StarRocksEngine) ExecuteWrite(ctx context.Context, req *ExecutionRequest) (*WriteResult, error) {
	e.mu.RLock()
	if !e.started {
		e.mu.RUnlock()
		return nil, errors.New("engine not started")
	}
	e.mu.RUnlock()

	// Validate request
	if req == nil || req.Statement == "" {
		return nil, errors.New("invalid write request")
	}

	// Set default timeout if not specified
	if req.Timeout <= 0 {
		req.Timeout = time.Duration(e.config.DefaultWriteTimeoutSeconds) * time.Second
	}

	// Create execution plan
	plan, err := e.createWriteExecutionPlan(req)
	if err != nil {
		return nil, fmt.Errorf("failed to create execution plan: %w", err)
	}

	// Create execution task
	task := &executionTask{
		req:       req,
		ctx:       ctx,
		plan:      plan,
		resultCh:  make(chan interface{}, 1),
		errCh:     make(chan error, 1),
		queueTime: time.Now(),
		strategy:  plan.Strategy,
		isWrite:   true,
	}

	// Submit task to queue
	select {
	case e.writeQueue <- task:
		// Task submitted successfully
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Queue is full
		return nil, errors.New("write queue is full")
	}

	// Wait for result or error
	select {
	case result := <-task.resultCh:
		if result == nil {
			// This shouldn't happen if the channels are used correctly
			return nil, errors.New("received nil result")
		}
		return result.(*WriteResult), nil
	case err := <-task.errCh:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// GetExecutionStats returns statistics about executions.
func (e *StarRocksEngine) GetExecutionStats() *ExecutionStats {
	// Calculate average query time
	var avgQueryTime time.Duration
	totalQueries := atomic.LoadInt64(&e.totalQueries)
	if totalQueries > 0 {
		totalQueryTimeNanos := atomic.LoadInt64(&e.totalQueryTime)
		avgQueryTime = time.Duration(totalQueryTimeNanos / totalQueries)
	}

	// Calculate average write time
	var avgWriteTime time.Duration
	totalWrites := atomic.LoadInt64(&e.totalWrites)
	if totalWrites > 0 {
		totalWriteTimeNanos := atomic.LoadInt64(&e.totalWriteTime)
		avgWriteTime = time.Duration(totalWriteTimeNanos / totalWrites)
	}

	// Calculate cache hit rate
	cacheHits := atomic.LoadInt64(&e.cacheHits)
	cacheMisses := atomic.LoadInt64(&e.cacheMisses)
	cacheHitRate := 0.0
	if cacheHits+cacheMisses > 0 {
		cacheHitRate = float64(cacheHits) / float64(cacheHits+cacheMisses)
	}

	// Calculate query and write time percentiles
	queryPercentiles := e.calculateQueryTimePercentiles()
	writePercentiles := e.calculateWriteTimePercentiles()

	// Get query and write errors
	queryErrors := e.getQueryErrors()
	writeErrors := e.getWriteErrors()

	// Get strategy distribution
	strategyDistribution := e.getStrategyDistribution()

	// Calculate average queue time
	var avgQueueTime time.Duration
	queueTimeCount := atomic.LoadInt64(&e.queueTimeCount)
	if queueTimeCount > 0 {
		queueTimeTotal := atomic.LoadInt64(&e.queueTimeTotal)
		avgQueueTime = time.Duration(queueTimeTotal / queueTimeCount)
	}

	// Calculate max queue time
	maxQueueTime := time.Duration(atomic.LoadInt64(&e.maxQueueTime))

	// Calculate resource utilization
	queryConcurrency := atomic.LoadInt64(&e.currentQueries)
	writeConcurrency := atomic.LoadInt64(&e.currentWrites)
	resourceUtilization := e.calculateResourceUtilization()

	// Calculate total executions and EPS
	totalExecutions := totalQueries + totalWrites
	var executionsPerSecond float64
	uptime := time.Since(e.startTime).Seconds()
	if uptime > 0 {
		executionsPerSecond = float64(totalExecutions) / uptime
	}

	return &ExecutionStats{
		TotalQueries:         totalQueries,
		TotalWrites:          totalWrites,
		SuccessfulQueries:    atomic.LoadInt64(&e.successfulQueries),
		SuccessfulWrites:     atomic.LoadInt64(&e.successfulWrites),
		FailedQueries:        atomic.LoadInt64(&e.failedQueries),
		FailedWrites:         atomic.LoadInt64(&e.failedWrites),
		AverageQueryTime:     avgQueryTime,
		AverageWriteTime:     avgWriteTime,
		QueryTimePercentiles: queryPercentiles,
		WriteTimePercentiles: writePercentiles,
		CacheHitRate:         cacheHitRate,
		QueryConcurrency:     queryConcurrency,
		WriteConcurrency:     writeConcurrency,
		QueryErrors:          queryErrors,
		WriteErrors:          writeErrors,
		StrategyDistribution: strategyDistribution,
		ResourceUtilization:  resourceUtilization,
		AvgQueueTime:         avgQueueTime,
		MaxQueueTime:         maxQueueTime,
		CurrentQueueSize:     len(e.queryQueue) + len(e.writeQueue),
		TotalExecutions:      totalExecutions,
		ExecutionsPerSecond:  executionsPerSecond,
	}
}

// createQueryExecutionPlan creates an execution plan for a query.
func (e *StarRocksEngine) createQueryExecutionPlan(req *ExecutionRequest) (*ExecutionPlan, error) {
	// Parse hints for strategy
	strategy := ExecutionStrategy(e.config.DefaultQueryStrategy)
	if strategyHint, ok := req.Hints["strategy"]; ok {
		switch ExecutionStrategy(strategyHint) {
		case StrategyFastest, StrategyParallel, StrategyBalanced, StrategyConsistent, StrategyCached:
			strategy = ExecutionStrategy(strategyHint)
		}
	}

	// Check if the query suggests a specific strategy
	if strategy == "" {
		strategy = e.determineQueryStrategy(req)
	}

	// Determine target nodes
	targetNodeIDs := req.TargetNodeIDs
	if len(targetNodeIDs) == 0 {
		// If not specified, use all healthy nodes
		targetNodeIDs = e.getHealthyNodeIDs()

		if len(targetNodeIDs) == 0 {
			return nil, errors.New("no healthy nodes available for query execution")
		}
	}

	// Determine parallel degree for parallel strategy
	parallelDegree := e.config.DefaultParallelDegree
	if parallelHint, ok := req.Hints["parallel_degree"]; ok {
		if pd, err := parseInt(parallelHint); err == nil && pd > 0 {
			parallelDegree = pd
		}
	}

	// Determine if caching should be enabled
	cacheEnabled := e.config.QueryCacheEnabled
	if cacheHint, ok := req.Hints["cache"]; ok {
		cacheEnabled = cacheHint == "true" || cacheHint == "1" || cacheHint == "yes"
	}

	// Determine cache TTL
	cacheTTL := time.Duration(e.config.QueryCacheTTLSeconds) * time.Second
	if ttlHint, ok := req.Hints["cache_ttl"]; ok {
		if ttl, err := time.ParseDuration(ttlHint); err == nil && ttl > 0 {
			cacheTTL = ttl
		}
	}

	// Create execution plan
	plan := &ExecutionPlan{
		Strategy:       strategy,
		TargetNodeIDs:  targetNodeIDs,
		Timeout:        req.Timeout,
		MaxRetries:     req.MaxRetries,
		ParallelDegree: parallelDegree,
		Priority:       req.Priority,
		CacheEnabled:   cacheEnabled,
		CacheTTL:       cacheTTL,
		Metadata:       make(map[string]string),
	}

	// Add query classification metadata
	plan.Metadata["query_type"] = e.classifyQuery(req.Statement)

	return plan, nil
}

// createWriteExecutionPlan creates an execution plan for a write operation.
func (e *StarRocksEngine) createWriteExecutionPlan(req *ExecutionRequest) (*ExecutionPlan, error) {
	// For writes, we typically want consistency, so we'll default to consistent strategy
	strategy := ExecutionStrategy(e.config.DefaultWriteStrategy)

	// But we'll still honor explicit hints
	if strategyHint, ok := req.Hints["strategy"]; ok {
		switch ExecutionStrategy(strategyHint) {
		case StrategyFastest, StrategyParallel, StrategyBalanced, StrategyConsistent:
			strategy = ExecutionStrategy(strategyHint)
		}
	}

	// Determine target nodes
	targetNodeIDs := req.TargetNodeIDs
	if len(targetNodeIDs) == 0 {
		// For writes, prefer primary nodes
		targetNodeIDs = e.getPrimaryNodeIDs()

		// If no primaries available, fall back to any writable node
		if len(targetNodeIDs) == 0 {
			targetNodeIDs = e.getWritableNodeIDs()
		}

		if len(targetNodeIDs) == 0 {
			return nil, errors.New("no writable nodes available for write execution")
		}
	}

	// Create execution plan
	plan := &ExecutionPlan{
		Strategy:       strategy,
		TargetNodeIDs:  targetNodeIDs,
		Timeout:        req.Timeout,
		MaxRetries:     req.MaxRetries,
		ParallelDegree: 1, // Typically no parallelism for writes
		Priority:       req.Priority,
		CacheEnabled:   false, // No caching for writes
		CacheTTL:       0,
		Metadata:       make(map[string]string),
	}

	// Add write classification metadata
	plan.Metadata["write_type"] = e.classifyWrite(req.Statement)

	return plan, nil
}

// determineQueryStrategy determines the best strategy for a query based on its characteristics.
func (e *StarRocksEngine) determineQueryStrategy(req *ExecutionRequest) ExecutionStrategy {
	queryType := e.classifyQuery(req.Statement)

	switch queryType {
	case "select":
		// For read-only queries, prefer cached if possible
		if e.config.QueryCacheEnabled {
			return StrategyCached
		}
		// Otherwise balance load
		return StrategyBalanced
	case "analytics":
		// For complex analytical queries, prefer fastest node
		return StrategyFastest
	case "system":
		// For system queries, use a consistent node
		return StrategyConsistent
	case "explain":
		// For explain plans, use a consistent node
		return StrategyConsistent
	case "show":
		// For show commands, use a consistent node
		return StrategyConsistent
	default:
		// Default to balanced strategy
		return StrategyBalanced
	}
}

// classifyQuery analyzes a query to determine its type.
func (e *StarRocksEngine) classifyQuery(query string) string {
	// This is a simple classification based on keywords
	// A production implementation would use a proper SQL parser

	query = strings.ToUpper(strings.TrimSpace(query))

	if strings.HasPrefix(query, "SELECT") {
		if strings.Contains(query, "SUM(") ||
			strings.Contains(query, "COUNT(") ||
			strings.Contains(query, "AVG(") ||
			strings.Contains(query, "GROUP BY") ||
			strings.Contains(query, "ORDER BY") {
			return "analytics"
		}
		return "select"
	}

	if strings.HasPrefix(query, "SHOW") ||
		strings.HasPrefix(query, "DESCRIBE") ||
		strings.HasPrefix(query, "DESC") {
		return "show"
	}

	if strings.HasPrefix(query, "EXPLAIN") {
		return "explain"
	}

	// Other system-related queries
	if strings.Contains(query, "INFORMATION_SCHEMA") ||
		strings.Contains(query, "PERFORMANCE_SCHEMA") {
		return "system"
	}

	return "other"
}

// classifyWrite analyzes a write statement to determine its type.
func (e *StarRocksEngine) classifyWrite(statement string) string {
	statement = strings.ToUpper(strings.TrimSpace(statement))

	if strings.HasPrefix(statement, "INSERT") {
		return "insert"
	}

	if strings.HasPrefix(statement, "UPDATE") {
		return "update"
	}

	if strings.HasPrefix(statement, "DELETE") {
		return "delete"
	}

	if strings.HasPrefix(statement, "CREATE") {
		return "ddl"
	}

	if strings.HasPrefix(statement, "ALTER") {
		return "ddl"
	}

	if strings.HasPrefix(statement, "DROP") {
		return "ddl"
	}

	return "other"
}

// executeQueryFastest executes a query on the fastest available node.
func (e *StarRocksEngine) executeQueryFastest(ctx context.Context, req *ExecutionRequest, plan *ExecutionPlan) (*query.Result, error) {
	// Get healthy target nodes
	targetNodeIDs := e.filterHealthyNodes(plan.TargetNodeIDs)
	if len(targetNodeIDs) == 0 {
		return nil, errors.New("no healthy target nodes available")
	}

	// Create a query request for the service
	queryReq := &query.Request{
		Query:      req.Statement,
		Database:   req.Database,
		User:       req.User,
		Parameters: req.Parameters,
		Timeout:    plan.Timeout,
		Priority:   plan.Priority,
		RequestID:  req.RequestID,
		NodeIDs:    targetNodeIDs,
		UseCache:   plan.CacheEnabled,
		CacheTTL:   plan.CacheTTL,
	}

	// Execute the query
	return e.queryService.ExecuteQueryFastest(ctx, queryReq)
}

// executeQueryParallel executes a query in parallel on multiple nodes and uses the first result.
func (e *StarRocksEngine) executeQueryParallel(ctx context.Context, req *ExecutionRequest, plan *ExecutionPlan) (*query.Result, error) {
	// Get healthy target nodes
	targetNodeIDs := e.filterHealthyNodes(plan.TargetNodeIDs)
	if len(targetNodeIDs) == 0 {
		return nil, errors.New("no healthy target nodes available")
	}

	// Limit parallel degree to available nodes
	parallelDegree := plan.ParallelDegree
	if parallelDegree > len(targetNodeIDs) {
		parallelDegree = len(targetNodeIDs)
	}

	// Create a query request for the service
	queryReq := &query.Request{
		Query:          req.Statement,
		Database:       req.Database,
		User:           req.User,
		Parameters:     req.Parameters,
		Timeout:        plan.Timeout,
		Priority:       plan.Priority,
		RequestID:      req.RequestID,
		NodeIDs:        targetNodeIDs,
		UseCache:       plan.CacheEnabled,
		CacheTTL:       plan.CacheTTL,
		ParallelDegree: parallelDegree,
	}

	// Execute the query in parallel
	return e.queryService.ExecuteQueryParallel(ctx, queryReq)
}

// executeQueryBalanced executes a query on a balanced node.
func (e *StarRocksEngine) executeQueryBalanced(ctx context.Context, req *ExecutionRequest, plan *ExecutionPlan) (*query.Result, error) {
	// Get healthy target nodes
	targetNodeIDs := e.filterHealthyNodes(plan.TargetNodeIDs)
	if len(targetNodeIDs) == 0 {
		return nil, errors.New("no healthy target nodes available")
	}

	// Create a query request for the service
	queryReq := &query.Request{
		Query:      req.Statement,
		Database:   req.Database,
		User:       req.User,
		Parameters: req.Parameters,
		Timeout:    plan.Timeout,
		Priority:   plan.Priority,
		RequestID:  req.RequestID,
		NodeIDs:    targetNodeIDs,
		UseCache:   plan.CacheEnabled,
		CacheTTL:   plan.CacheTTL,
	}

	// Execute the query
	return e.queryService.ExecuteQueryBalanced(ctx, queryReq)
}

// executeQueryConsistent executes a query on a consistent node.
func (e *StarRocksEngine) executeQueryConsistent(ctx context.Context, req *ExecutionRequest, plan *ExecutionPlan) (*query.Result, error) {
	// Get healthy target nodes
	targetNodeIDs := e.filterHealthyNodes(plan.TargetNodeIDs)
	if len(targetNodeIDs) == 0 {
		return nil, errors.New("no healthy target nodes available")
	}

	// Create a query request for the service
	queryReq := &query.Request{
		Query:      req.Statement,
		Database:   req.Database,
		User:       req.User,
		Parameters: req.Parameters,
		Timeout:    plan.Timeout,
		Priority:   plan.Priority,
		RequestID:  req.RequestID,
		NodeIDs:    targetNodeIDs,
		UseCache:   plan.CacheEnabled,
		CacheTTL:   plan.CacheTTL,
		Consistent: true,
	}

	// Execute the query
	return e.queryService.ExecuteQueryConsistent(ctx, queryReq)
}

// executeQueryCached executes a query with caching enabled.
func (e *StarRocksEngine) executeQueryCached(ctx context.Context, req *ExecutionRequest, plan *ExecutionPlan) (*query.Result, error) {
	// Get healthy target nodes
	targetNodeIDs := e.filterHealthyNodes(plan.TargetNodeIDs)
	if len(targetNodeIDs) == 0 {
		return nil, errors.New("no healthy target nodes available")
	}

	// Create a query request for the service
	queryReq := &query.Request{
		Query:      req.Statement,
		Database:   req.Database,
		User:       req.User,
		Parameters: req.Parameters,
		Timeout:    plan.Timeout,
		Priority:   plan.Priority,
		RequestID:  req.RequestID,
		NodeIDs:    targetNodeIDs,
		UseCache:   true, // Always use cache for this strategy
		CacheTTL:   plan.CacheTTL,
	}

	// Execute the query
	return e.queryService.ExecuteQueryCached(ctx, queryReq)
}

// executeWrite executes a write operation.
func (e *StarRocksEngine) executeWrite(ctx context.Context, req *ExecutionRequest, plan *ExecutionPlan) (*write.Result, error) {
	// Get healthy and writable target nodes
	targetNodeIDs := e.filterWritableNodes(plan.TargetNodeIDs)
	if len(targetNodeIDs) == 0 {
		return nil, errors.New("no healthy writable nodes available")
	}

	// Create a write request for the service
	writeReq := &write.Request{
		Statement:  req.Statement,
		Database:   req.Database,
		User:       req.User,
		Parameters: req.Parameters,
		Timeout:    plan.Timeout,
		Priority:   plan.Priority,
		RequestID:  req.RequestID,
		NodeIDs:    targetNodeIDs,
	}

	// Determine the write execution method based on strategy
	switch plan.Strategy {
	case StrategyConsistent:
		return e.writeService.ExecuteWriteConsistent(ctx, writeReq)
	case StrategyParallel:
		return e.writeService.ExecuteWriteParallel(ctx, writeReq)
	default:
		// Default to balanced for writes
		return e.writeService.ExecuteWriteBalanced(ctx, writeReq)
	}
}

// filterHealthyNodes filters out unhealthy nodes from the given list.
func (e *StarRocksEngine) filterHealthyNodes(nodeIDs []string) []string {
	if len(nodeIDs) == 0 {
		return e.getHealthyNodeIDs()
	}

	healthyNodes := make([]string, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		isHealthy, err := e.healthService.IsHealthy(nodeID)
		if err == nil && isHealthy {
			healthyNodes = append(healthyNodes, nodeID)
		}
	}

	return healthyNodes
}

// filterWritableNodes filters out nodes that cannot accept writes.
func (e *StarRocksEngine) filterWritableNodes(nodeIDs []string) []string {
	if len(nodeIDs) == 0 {
		return e.getWritableNodeIDs()
	}

	writableNodes := make([]string, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		isHealthy, err := e.healthService.IsHealthy(nodeID)
		isWritable, _ := e.healthService.IsWritable(nodeID)

		if err == nil && isHealthy && isWritable {
			writableNodes = append(writableNodes, nodeID)
		}
	}

	return writableNodes
}

// getHealthyNodeIDs returns a list of all healthy node IDs.
func (e *StarRocksEngine) getHealthyNodeIDs() []string {
	nodes, err := e.healthService.GetAllNodes()
	if err != nil {
		e.logger.Error("Failed to get all nodes", zap.Error(err))
		return nil
	}

	healthyNodes := make([]string, 0, len(nodes))
	for _, node := range nodes {
		isHealthy, err := e.healthService.IsHealthy(node.ID)
		if err == nil && isHealthy {
			healthyNodes = append(healthyNodes, node.ID)
		}
	}

	return healthyNodes
}

// getPrimaryNodeIDs returns a list of primary node IDs.
func (e *StarRocksEngine) getPrimaryNodeIDs() []string {
	nodes, err := e.healthService.GetAllNodes()
	if err != nil {
		e.logger.Error("Failed to get all nodes", zap.Error(err))
		return nil
	}

	primaryNodes := make([]string, 0, len(nodes))
	for _, node := range nodes {
		isHealthy, err := e.healthService.IsHealthy(node.ID)
		isPrimary, _ := e.healthService.IsPrimary(node.ID)

		if err == nil && isHealthy && isPrimary {
			primaryNodes = append(primaryNodes, node.ID)
		}
	}

	return primaryNodes
}

// getWritableNodeIDs returns a list of writable node IDs.
func (e *StarRocksEngine) getWritableNodeIDs() []string {
	nodes, err := e.healthService.GetAllNodes()
	if err != nil {
		e.logger.Error("Failed to get all nodes", zap.Error(err))
		return nil
	}

	writableNodes := make([]string, 0, len(nodes))
	for _, node := range nodes {
		isHealthy, err := e.healthService.IsHealthy(node.ID)
		isWritable, _ := e.healthService.IsWritable(node.ID)

		if err == nil && isHealthy && isWritable {
			writableNodes = append(writableNodes, node.ID)
		}
	}

	return writableNodes
}

// updateConcurrencyHistogram updates the concurrency histogram.
func (e *StarRocksEngine) updateConcurrencyHistogram(concurrency int) {
	e.concurrencyMutex.Lock()
	defer e.concurrencyMutex.Unlock()

	if concurrency >= 0 && concurrency < len(e.concurrencyHistogram) {
		e.concurrencyHistogram[concurrency]++
	}
}

// recordQueryTime records a query execution time.
func (e *StarRocksEngine) recordQueryTime(duration time.Duration) {
	// Update total query time
	atomic.AddInt64(&e.totalQueryTime, int64(duration))

	// Store the individual time for percentile calculation
	e.queryTimesMutex.Lock()
	e.queryTimes = append(e.queryTimes, int64(duration))
	// Limit the size of the slice to avoid memory issues
	if len(e.queryTimes) > 10000 {
		e.queryTimes = e.queryTimes[1000:] // Keep the most recent values
	}
	e.queryTimesMutex.Unlock()
}

// recordWriteTime records a write execution time.
func (e *StarRocksEngine) recordWriteTime(duration time.Duration) {
	// Update total write time
	atomic.AddInt64(&e.totalWriteTime, int64(duration))

	// Store the individual time for percentile calculation
	e.writeTimesMutex.Lock()
	e.writeTimes = append(e.writeTimes, int64(duration))
	// Limit the size of the slice to avoid memory issues
	if len(e.writeTimes) > 10000 {
		e.writeTimes = e.writeTimes[1000:] // Keep the most recent values
	}
	e.writeTimesMutex.Unlock()
}

// recordQueryError records a query error.
func (e *StarRocksEngine) recordQueryError(errorType string, req *ExecutionRequest) {
	e.errorsMutex.Lock()
	e.queryErrors[errorType]++
	e.errorsMutex.Unlock()

	// Update Prometheus metrics
	e.executionErrors.WithLabelValues(errorType, "query").Inc()

	e.logger.Warn("Query execution error",
		zap.String("error_type", errorType),
		zap.String("request_id", req.RequestID),
		zap.String("query", req.Statement))
}

// recordWriteError records a write error.
func (e *StarRocksEngine) recordWriteError(errorType string, req *ExecutionRequest) {
	e.errorsMutex.Lock()
	e.writeErrors[errorType]++
	e.errorsMutex.Unlock()

	// Update Prometheus metrics
	e.executionErrors.WithLabelValues(errorType, "write").Inc()

	e.logger.Warn("Write execution error",
		zap.String("error_type", errorType),
		zap.String("request_id", req.RequestID),
		zap.String("statement", req.Statement))
}

// recordStrategyUsage records the usage of an execution strategy.
func (e *StarRocksEngine) recordStrategyUsage(strategy string) {
	e.strategyMutex.Lock()
	e.strategyUsage[strategy]++
	e.strategyMutex.Unlock()

	// Update Prometheus metrics
	e.queryStrategyDistribution.WithLabelValues(strategy).Inc()
}

// classifyError classifies an error into a standard type.
func (e *StarRocksEngine) classifyError(err error) string {
	if err == nil {
		return "NONE"
	}

	errMsg := err.Error()

	// Timeout errors
	if errors.Is(err, context.DeadlineExceeded) || strings.Contains(errMsg, "timeout") || strings.Contains(errMsg, "deadline") {
		return "TIMEOUT"
	}

	// Cancellation errors
	if errors.Is(err, context.Canceled) || strings.Contains(errMsg, "canceled") || strings.Contains(errMsg, "cancelled") {
		return "CANCELLED"
	}

	// Connection errors
	if strings.Contains(errMsg, "connection") || strings.Contains(errMsg, "network") || strings.Contains(errMsg, "dial") {
		return "CONNECTION"
	}

	// SQL syntax errors
	if strings.Contains(errMsg, "syntax") || strings.Contains(errMsg, "parse") {
		return "SYNTAX"
	}

	// Permission errors
	if strings.Contains(errMsg, "permission") || strings.Contains(errMsg, "access") || strings.Contains(errMsg, "denied") {
		return "PERMISSION"
	}

	// Resource errors
	if strings.Contains(errMsg, "resource") || strings.Contains(errMsg, "memory") || strings.Contains(errMsg, "disk") {
		return "RESOURCE"
	}

	// Database errors
	if strings.Contains(errMsg, "database") || strings.Contains(errMsg, "table") || strings.Contains(errMsg, "schema") {
		return "DATABASE"
	}

	// Default
	return "OTHER"
}

// calculateQueryTimePercentiles calculates percentiles of query execution times.
func (e *StarRocksEngine) calculateQueryTimePercentiles() map[int]int64 {
	e.queryTimesMutex.Lock()
	times := make([]int64, len(e.queryTimes))
	copy(times, e.queryTimes)
	e.queryTimesMutex.Unlock()

	if len(times) == 0 {
		return map[int]int64{}
	}

	sort.Slice(times, func(i, j int) bool {
		return times[i] < times[j]
	})

	percentiles := map[int]int64{}

	// Calculate p50, p90, p95, p99
	percentiles[50] = times[len(times)*50/100]
	percentiles[90] = times[len(times)*90/100]
	percentiles[95] = times[len(times)*95/100]
	percentiles[99] = times[len(times)*99/100]

	return percentiles
}

// calculateWriteTimePercentiles calculates percentiles of write execution times.
func (e *StarRocksEngine) calculateWriteTimePercentiles() map[int]int64 {
	e.writeTimesMutex.Lock()
	times := make([]int64, len(e.writeTimes))
	copy(times, e.writeTimes)
	e.writeTimesMutex.Unlock()

	if len(times) == 0 {
		return map[int]int64{}
	}

	sort.Slice(times, func(i, j int) bool {
		return times[i] < times[j]
	})

	percentiles := map[int]int64{}

	// Calculate p50, p90, p95, p99
	percentiles[50] = times[len(times)*50/100]
	percentiles[90] = times[len(times)*90/100]
	percentiles[95] = times[len(times)*95/100]
	percentiles[99] = times[len(times)*99/100]

	return percentiles
}

// getQueryErrors returns a copy of the query errors map.
func (e *StarRocksEngine) getQueryErrors() map[string]int64 {
	e.errorsMutex.RLock()
	defer e.errorsMutex.RUnlock()

	errors := make(map[string]int64, len(e.queryErrors))
	for k, v := range e.queryErrors {
		errors[k] = v
	}

	return errors
}

// getWriteErrors returns a copy of the write errors map.
func (e *StarRocksEngine) getWriteErrors() map[string]int64 {
	e.errorsMutex.RLock()
	defer e.errorsMutex.RUnlock()

	errors := make(map[string]int64, len(e.writeErrors))
	for k, v := range e.writeErrors {
		errors[k] = v
	}

	return errors
}

// getStrategyDistribution returns a copy of the strategy usage map.
func (e *StarRocksEngine) getStrategyDistribution() map[string]int64 {
	e.strategyMutex.RLock()
	defer e.strategyMutex.RUnlock()

	distribution := make(map[string]int64, len(e.strategyUsage))
	for k, v := range e.strategyUsage {
		distribution[k] = v
	}

	return distribution
}

// updateCacheHitRatio updates the cache hit ratio metric.
func (e *StarRocksEngine) updateCacheHitRatio() {
	cacheHits := atomic.LoadInt64(&e.cacheHits)
	cacheMisses := atomic.LoadInt64(&e.cacheMisses)

	ratio := 0.0
	if cacheHits+cacheMisses > 0 {
		ratio = float64(cacheHits) / float64(cacheHits+cacheMisses)
	}

	e.cacheHitRatio.Set(ratio)
}

// updateResourceUtilization updates the resource utilization metric.
func (e *StarRocksEngine) updateResourceUtilization() {
	currentQueries := atomic.LoadInt64(&e.currentQueries)
	currentWrites := atomic.LoadInt64(&e.currentWrites)
	maxConcurrent := int64(e.config.MaxConcurrentQueries + e.config.MaxConcurrentWrites)

	utilization := 0.0
	if maxConcurrent > 0 {
		utilization = float64(currentQueries+currentWrites) / float64(maxConcurrent)
	}

	e.resourceUtilization.Set(utilization)
}

// calculateResourceUtilization calculates the current resource utilization.
func (e *StarRocksEngine) calculateResourceUtilization() float64 {
	currentQueries := atomic.LoadInt64(&e.currentQueries)
	currentWrites := atomic.LoadInt64(&e.currentWrites)
	maxConcurrent := int64(e.config.MaxConcurrentQueries + e.config.MaxConcurrentWrites)

	if maxConcurrent > 0 {
		return float64(currentQueries+currentWrites) / float64(maxConcurrent)
	}

	return 0.0
}

// parseInt parses a string into an integer with error handling.
func parseInt(s string) (int, error) {
	i, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	return i, nil
}

// EngineFactory defines a factory for creating engine instances.
type EngineFactory interface {
	CreateEngine(config *config.Config) (Engine, error)
}

// DefaultEngineFactory is the default implementation of EngineFactory.
type DefaultEngineFactory struct {
	queryServiceFactory  query.ServiceFactory
	writeServiceFactory  write.ServiceFactory
	healthServiceFactory health.ServiceFactory
	metricsRegistry      *metrics.Registry
}

// NewDefaultEngineFactory creates a new DefaultEngineFactory.
func NewDefaultEngineFactory(
	queryServiceFactory query.ServiceFactory,
	writeServiceFactory write.ServiceFactory,
	healthServiceFactory health.ServiceFactory,
	metricsRegistry *metrics.Registry,
) *DefaultEngineFactory {
	return &DefaultEngineFactory{
		queryServiceFactory:  queryServiceFactory,
		writeServiceFactory:  writeServiceFactory,
		healthServiceFactory: healthServiceFactory,
		metricsRegistry:      metricsRegistry,
	}
}

// CreateEngine creates a new Engine instance.
func (f *DefaultEngineFactory) CreateEngine(config *config.Config) (Engine, error) {
	// Create required services
	queryService, err := f.queryServiceFactory.CreateService(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create query service: %w", err)
	}

	writeService, err := f.writeServiceFactory.CreateService(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create write service: %w", err)
	}

	healthService, err := f.healthServiceFactory.CreateService(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create health service: %w", err)
	}

	// Create the engine
	engine := NewStarRocksEngine(
		queryService,
		writeService,
		healthService,
		f.metricsRegistry,
		&config.Engine,
	)

	// Start the engine
	if err := engine.Start(); err != nil {
		return nil, fmt.Errorf("failed to start engine: %w", err)
	}

	return engine, nil
}

//Personal.AI order the ending
