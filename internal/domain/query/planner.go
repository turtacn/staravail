// Package query provides functionality for SQL query parsing, pruning, planning, and execution.
package query

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/turtacn/staravail/internal/common/config"
	"github.com/turtacn/staravail/internal/common/logging"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/domain/metadata"
	"github.com/turtacn/staravail/internal/domain/tablet"
)

// ExecutionStrategy defines the strategy for query execution
type ExecutionStrategy string

const (
	// ExecutionStrategyOriginal executes the original query
	ExecutionStrategyOriginal ExecutionStrategy = "ORIGINAL"
	// ExecutionStrategyPruned executes a pruned version of the query
	ExecutionStrategyPruned ExecutionStrategy = "PRUNED"
	// ExecutionStrategyPartitioned executes the query on each partition separately and combines results
	ExecutionStrategyPartitioned ExecutionStrategy = "PARTITIONED"
	// ExecutionStrategyRejected rejects the query because it cannot be executed
	ExecutionStrategyRejected ExecutionStrategy = "REJECTED"
)

// QueryPlan represents a plan for executing a query
type QueryPlan struct {
	// OriginalSQL is the original SQL query
	OriginalSQL string
	// ExecutionSQL is the SQL to be executed
	ExecutionSQL string
	// Strategy is the execution strategy
	Strategy ExecutionStrategy
	// IsExecutable indicates if the query can be executed
	IsExecutable bool
	// ExpectedResults describes what to expect from the results
	ExpectedResults string
	// PotentialDataLoss describes potential data loss
	PotentialDataLoss string
	// EstimatedDataLossPercentage is the estimated percentage of data loss
	EstimatedDataLossPercentage float64
	// WarningMessage contains a warning message about potential issues
	WarningMessage string
	// TablesHealthStatus contains health status information for tables
	TablesHealthStatus map[string]float64
	// PruningInfo contains information about query pruning
	PruningInfo *PruningInfo
	// CreatedAt is when the plan was created
	CreatedAt time.Time
	// ExecutionTimeout is the timeout for query execution
	ExecutionTimeout time.Duration
	// PlannerMetadata contains additional metadata set by the planner
	PlannerMetadata map[string]interface{}
}

// QueryPlanner defines the interface for planning queries
type QueryPlanner interface {
	// PlanQuery plans a query and returns a query plan
	PlanQuery(ctx context.Context, sql string) (QueryPlan, error)

	// GetQueryPlan gets a cached query plan, if available
	GetQueryPlan(ctx context.Context, sql string) (QueryPlan, bool, error)

	// IsQueryExecutable checks if a query can be executed
	IsQueryExecutable(ctx context.Context, sql string) (bool, string, error)

	// InvalidateCache invalidates the cache for a specific SQL or all SQL if empty
	InvalidateCache(sql string)

	// SetExecutionTimeoutFunc sets a function to determine execution timeout based on query complexity
	SetExecutionTimeoutFunc(func(QueryInfo) time.Duration)
}

// QueryPlannerConfig represents configuration for the query planner
type QueryPlannerConfig struct {
	// MinTableAvailabilityThreshold is the minimum availability threshold for tables
	MinTableAvailabilityThreshold float64
	// PreferPrunedQueries indicates if pruned queries should be preferred
	PreferPrunedQueries bool
	// MaxDataLossPercentage is the maximum acceptable data loss percentage
	MaxDataLossPercentage float64
	// EnablePartitionedStrategy enables the partitioned execution strategy
	EnablePartitionedStrategy bool
	// DefaultExecutionTimeout is the default timeout for query execution
	DefaultExecutionTimeout time.Duration
	// CacheEnabled indicates if caching is enabled
	CacheEnabled bool
	// CacheTTL is the cache time-to-live
	CacheTTL time.Duration
	// CacheMaxSize is the maximum size of the cache
	CacheMaxSize int
}

// cachedQueryPlan represents a cached query plan
type cachedQueryPlan struct {
	// Plan is the query plan
	Plan QueryPlan
	// LastAccessed is when the cache entry was last accessed
	LastAccessed time.Time
}

// QueryPlannerImpl implements the QueryPlanner interface
type QueryPlannerImpl struct {
	// config is the planner configuration
	config QueryPlannerConfig
	// queryParser is used to parse SQL queries
	queryParser QueryParser
	// queryPruner is used to prune queries
	queryPruner QueryPruner
	// healthChecker is used to check health status
	healthChecker tablet.HealthChecker
	// metadataService is used to get metadata information
	metadataService metadata.MetadataService
	// logger is used for logging
	logger logging.Logger
	// metrics is used to record metrics
	metrics metrics.MetricsRecorder
	// executionTimeoutFunc is a function to determine execution timeout based on query complexity
	executionTimeoutFunc func(QueryInfo) time.Duration

	// planCache caches query plans
	planCache map[string]cachedQueryPlan
	// cacheMutex protects the cache map
	cacheMutex sync.RWMutex
	// lastCacheCleanup is when the cache was last cleaned up
	lastCacheCleanup time.Time
}

// NewQueryPlanner creates a new QueryPlanner instance
func NewQueryPlanner(
	cfg config.QueryPlannerConfig,
	queryParser QueryParser,
	queryPruner QueryPruner,
	healthChecker tablet.HealthChecker,
	metadataService metadata.MetadataService,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) QueryPlanner {
	// Convert config to internal planner config
	plannerConfig := QueryPlannerConfig{
		MinTableAvailabilityThreshold: cfg.MinTableAvailabilityThreshold,
		PreferPrunedQueries:           cfg.PreferPrunedQueries,
		MaxDataLossPercentage:         cfg.MaxDataLossPercentage,
		EnablePartitionedStrategy:     cfg.EnablePartitionedStrategy,
		DefaultExecutionTimeout:       cfg.DefaultExecutionTimeout,
		CacheEnabled:                  cfg.CacheEnabled,
		CacheTTL:                      cfg.CacheTTL,
		CacheMaxSize:                  cfg.CacheMaxSize,
	}

	return &QueryPlannerImpl{
		config:          plannerConfig,
		queryParser:     queryParser,
		queryPruner:     queryPruner,
		healthChecker:   healthChecker,
		metadataService: metadataService,
		logger:          logger,
		metrics:         metricsRecorder,
		executionTimeoutFunc: func(QueryInfo) time.Duration {
			return cfg.DefaultExecutionTimeout
		},
		planCache:        make(map[string]cachedQueryPlan),
		lastCacheCleanup: time.Now(),
	}
}

// PlanQuery plans a query and returns a query plan
func (p *QueryPlannerImpl) PlanQuery(ctx context.Context, sql string) (QueryPlan, error) {
	// Check if cached plan is available
	if p.config.CacheEnabled {
		if cachedPlan, found, _ := p.GetQueryPlan(ctx, sql); found {
			// Update metrics for cache hit
			p.metrics.CounterInc("query_planner_cache_hit", nil)
			return cachedPlan, nil
		}
	}

	// Update metrics for cache miss
	p.metrics.CounterInc("query_planner_cache_miss", nil)

	// Start planning time measurement
	startTime := time.Now()
	defer func() {
		// Record planning time
		p.metrics.HistogramObserve("query_planner_time_ms", float64(time.Since(startTime).Milliseconds()), nil)
	}()

	// Parse the query
	queryInfo, err := p.queryParser.ParseQuery(ctx, sql)
	if err != nil {
		p.logger.Error("Failed to parse query", "sql", sql, "error", err)
		return QueryPlan{}, errors.Wrap(err, "failed to parse query")
	}

	// Initialize query plan
	plan := QueryPlan{
		OriginalSQL:        sql,
		ExecutionSQL:       sql,
		Strategy:           ExecutionStrategyOriginal,
		IsExecutable:       true,
		TablesHealthStatus: make(map[string]float64),
		CreatedAt:          time.Now(),
		ExecutionTimeout:   p.executionTimeoutFunc(queryInfo),
		PlannerMetadata:    make(map[string]interface{}),
	}

	// Get health status for tables
	tablesAvailability, err := p.getTablesAvailability(ctx, queryInfo)
	if err != nil {
		p.logger.Error("Failed to get tables availability", "sql", sql, "error", err)
		// Continue with planning but mark as potentially unreliable
		plan.WarningMessage = fmt.Sprintf("Health status check failed: %v", err)
	}
	plan.TablesHealthStatus = tablesAvailability

	// Check if the query is executable based on health status
	isExecutable, reason := p.isQueryExecutableInternal(tablesAvailability)
	if !isExecutable {
		plan.IsExecutable = false
		plan.Strategy = ExecutionStrategyRejected
		plan.WarningMessage = reason
		plan.ExpectedResults = "Query cannot be executed due to unavailable tables"

		// Cache the plan if caching is enabled
		if p.config.CacheEnabled {
			p.cacheQueryPlan(sql, plan)
		}

		return plan, nil
	}

	// Check if query pruning is needed and possible
	if p.shouldAttemptPruning(tablesAvailability) {
		canPrune, err := p.queryPruner.CanPrune(ctx, sql)
		if err != nil {
			p.logger.Error("Failed to check if query can be pruned", "sql", sql, "error", err)
			// Continue with original query
		} else if canPrune {
			// Try to prune the query
			prunedSQL, pruningInfo, err := p.queryPruner.PruneQuery(ctx, sql, "")
			if err != nil {
				p.logger.Error("Failed to prune query", "sql", sql, "error", err)
				// Continue with original query
			} else if pruningInfo.IsQueryPruned {
				// Query was successfully pruned
				plan.ExecutionSQL = prunedSQL
				plan.Strategy = ExecutionStrategyPruned
				plan.PruningInfo = &pruningInfo
				plan.EstimatedDataLossPercentage = pruningInfo.EstimatedDataLossPercentage
				plan.PotentialDataLoss = fmt.Sprintf(
					"Approximately %.2f%% of data may be excluded due to unavailable partitions/buckets",
					pruningInfo.EstimatedDataLossPercentage,
				)
				plan.WarningMessage = pruningInfo.WarningMessage
				plan.ExpectedResults = "Results may be incomplete due to query pruning"

				// Check if the data loss is acceptable
				if pruningInfo.EstimatedDataLossPercentage > p.config.MaxDataLossPercentage {
					// Data loss is too high, reject the query
					if !p.config.PreferPrunedQueries {
						plan.IsExecutable = false
						plan.Strategy = ExecutionStrategyRejected
						plan.WarningMessage = fmt.Sprintf(
							"Estimated data loss (%.2f%%) exceeds maximum allowed (%.2f%%)",
							pruningInfo.EstimatedDataLossPercentage,
							p.config.MaxDataLossPercentage,
						)
						plan.ExpectedResults = "Query rejected due to excessive potential data loss"
					}
				}
			}
		}
	}

	// Check if partitioned execution strategy should be used
	if p.config.EnablePartitionedStrategy && p.shouldUsePartitionedStrategy(queryInfo, tablesAvailability) {
		plan.Strategy = ExecutionStrategyPartitioned
		plan.ExpectedResults = "Results will be combined from multiple partition-specific queries"
		plan.PlannerMetadata["requiresPostProcessing"] = true
		plan.PlannerMetadata["partitionQueries"] = p.generatePartitionQueries(ctx, queryInfo)
	}

	// Cache the plan if caching is enabled
	if p.config.CacheEnabled {
		p.cacheQueryPlan(sql, plan)
	}

	return plan, nil
}

// getTablesAvailability gets availability information for tables in a query
func (p *QueryPlannerImpl) getTablesAvailability(ctx context.Context, queryInfo QueryInfo) (map[string]float64, error) {
	availability := make(map[string]float64)

	for _, table := range queryInfo.Tables {
		// Get the health status for the table
		tableHealth, err := p.healthChecker.GetTableHealthStatus(ctx, table.DatabaseName, table.TableName)
		if err != nil {
			p.logger.Error("Failed to get table health status",
				"database", table.DatabaseName,
				"table", table.TableName,
				"error", err)
			availability[fmt.Sprintf("%s.%s", table.DatabaseName, table.TableName)] = 0.0
			continue
		}

		availability[fmt.Sprintf("%s.%s", table.DatabaseName, table.TableName)] = tableHealth.AvailabilityRatio
	}

	return availability, nil
}

// isQueryExecutableInternal checks if a query can be executed based on health status
func (p *QueryPlannerImpl) isQueryExecutableInternal(tablesAvailability map[string]float64) (bool, string) {
	// Check if any table is below the minimum availability threshold
	for table, availability := range tablesAvailability {
		if availability < p.config.MinTableAvailabilityThreshold {
			return false, fmt.Sprintf(
				"Table %s availability (%.2f%%) is below minimum threshold (%.2f%%)",
				table,
				availability*100,
				p.config.MinTableAvailabilityThreshold*100,
			)
		}
	}

	return true, ""
}

// shouldAttemptPruning checks if query pruning should be attempted based on health status
func (p *QueryPlannerImpl) shouldAttemptPruning(tablesAvailability map[string]float64) bool {
	// Check if any table has less than 100% availability
	for _, availability := range tablesAvailability {
		if availability < 1.0 {
			return true
		}
	}

	return false
}

// shouldUsePartitionedStrategy checks if the partitioned execution strategy should be used
func (p *QueryPlannerImpl) shouldUsePartitionedStrategy(queryInfo QueryInfo, tablesAvailability map[string]float64) bool {
	// Check if the query has partitionable conditions
	if len(queryInfo.PartitionConditions) == 0 {
		return false
	}

	// Check if any table has less than 100% availability
	hasPartialAvailability := false
	for _, availability := range tablesAvailability {
		if availability < 1.0 && availability > 0.0 {
			hasPartialAvailability = true
			break
		}
	}

	// Only use partitioned strategy if there's partial availability
	return hasPartialAvailability
}

// generatePartitionQueries generates partition-specific queries for partitioned execution
func (p *QueryPlannerImpl) generatePartitionQueries(ctx context.Context, queryInfo QueryInfo) []string {
	// This is a placeholder implementation
	// In a real implementation, you would generate a query for each healthy partition

	// For simplicity, we'll just return a stub
	return []string{"/* Partition queries would be generated here */"}
}

// GetQueryPlan gets a cached query plan, if available
func (p *QueryPlannerImpl) GetQueryPlan(ctx context.Context, sql string) (QueryPlan, bool, error) {
	if !p.config.CacheEnabled {
		return QueryPlan{}, false, nil
	}

	p.cacheMutex.RLock()
	defer p.cacheMutex.RUnlock()

	// Check if we need to clean up the cache
	if time.Since(p.lastCacheCleanup) > 10*time.Minute {
		go p.cleanupCache()
	}

	// Check if the query is in the cache
	if entry, found := p.planCache[sql]; found {
		// Check if the cache entry is still valid
		if time.Since(entry.LastAccessed) <= p.config.CacheTTL {
			// Update last accessed time
			entry.LastAccessed = time.Now()
			p.planCache[sql] = entry
			return entry.Plan, true, nil
		}
	}

	return QueryPlan{}, false, nil
}

// IsQueryExecutable checks if a query can be executed
func (p *QueryPlannerImpl) IsQueryExecutable(ctx context.Context, sql string) (bool, string, error) {
	// Parse the query
	queryInfo, err := p.queryParser.ParseQuery(ctx, sql)
	if err != nil {
		p.logger.Error("Failed to parse query", "sql", sql, "error", err)
		return false, "Failed to parse query", errors.Wrap(err, "failed to parse query")
	}

	// Get health status for tables
	tablesAvailability, err := p.getTablesAvailability(ctx, queryInfo)
	if err != nil {
		p.logger.Error("Failed to get tables availability", "sql", sql, "error", err)
		return false, fmt.Sprintf("Failed to check health status: %v", err), err
	}

	// Check if the query is executable based on health status
	isExecutable, reason := p.isQueryExecutableInternal(tablesAvailability)

	// If not executable directly, check if it could be executable with pruning
	if !isExecutable && p.shouldAttemptPruning(tablesAvailability) {
		canPrune, err := p.queryPruner.CanPrune(ctx, sql)
		if err != nil {
			p.logger.Error("Failed to check if query can be pruned", "sql", sql, "error", err)
		} else if canPrune {
			// Get pruning info to check estimated data loss
			pruningInfo, err := p.queryPruner.GetPruningInfo(ctx, sql, "")
			if err != nil {
				p.logger.Error("Failed to get pruning info", "sql", sql, "error", err)
			} else if pruningInfo.EstimatedDataLossPercentage <= p.config.MaxDataLossPercentage {
				return true, "Executable with pruning", nil
			} else {
				reason = fmt.Sprintf(
					"Estimated data loss (%.2f%%) exceeds maximum allowed (%.2f%%)",
					pruningInfo.EstimatedDataLossPercentage,
					p.config.MaxDataLossPercentage,
				)
			}
		}
	}

	return isExecutable, reason, nil
}

// InvalidateCache invalidates the cache for a specific SQL or all SQL if empty
func (p *QueryPlannerImpl) InvalidateCache(sql string) {
	p.cacheMutex.Lock()
	defer p.cacheMutex.Unlock()

	if sql == "" {
		// Invalidate entire cache
		p.planCache = make(map[string]cachedQueryPlan)
		p.metrics.CounterInc("query_planner_cache_invalidate_all", nil)
	} else {
		// Invalidate specific entry
		delete(p.planCache, sql)
		p.metrics.CounterInc("query_planner_cache_invalidate_entry", nil)
	}
}

// SetExecutionTimeoutFunc sets a function to determine execution timeout based on query complexity
func (p *QueryPlannerImpl) SetExecutionTimeoutFunc(timeoutFunc func(QueryInfo) time.Duration) {
	p.executionTimeoutFunc = timeoutFunc
}

// cacheQueryPlan caches a query plan
func (p *QueryPlannerImpl) cacheQueryPlan(sql string, plan QueryPlan) {
	p.cacheMutex.Lock()
	defer p.cacheMutex.Unlock()

	// Check if the cache is full
	if len(p.planCache) >= p.config.CacheMaxSize {
		// If full, remove the oldest entries
		p.evictOldestEntries(p.config.CacheMaxSize / 10) // Remove 10% of entries
	}

	// Add the new entry
	p.planCache[sql] = cachedQueryPlan{
		Plan:         plan,
		LastAccessed: time.Now(),
	}
}

// evictOldestEntries evicts the oldest entries from the cache
func (p *QueryPlannerImpl) evictOldestEntries(count int) {
	if count <= 0 || len(p.planCache) == 0 {
		return
	}

	// Collect all entries with their last accessed time
	type cacheEntry struct {
		sql          string
		lastAccessed time.Time
	}
	entries := make([]cacheEntry, 0, len(p.planCache))

	for sql, entry := range p.planCache {
		entries = append(entries, cacheEntry{
			sql:          sql,
			lastAccessed: entry.LastAccessed,
		})
	}

	// Sort by last accessed time (oldest first)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].lastAccessed.Before(entries[j].lastAccessed)
	})

	// Remove the oldest entries
	numToRemove := count
	if numToRemove > len(entries) {
		numToRemove = len(entries)
	}

	for i := 0; i < numToRemove; i++ {
		delete(p.planCache, entries[i].sql)
	}

	p.metrics.CounterAdd("query_planner_cache_entries_evicted", float64(numToRemove), nil)
}

// cleanupCache cleans up expired entries from the cache
func (p *QueryPlannerImpl) cleanupCache() {
	p.cacheMutex.Lock()
	defer p.cacheMutex.Unlock()

	now := time.Now()
	p.lastCacheCleanup = now
	expiredCount := 0

	// Remove expired entries
	for sql, entry := range p.planCache {
		if now.Sub(entry.LastAccessed) > p.config.CacheTTL {
			delete(p.planCache, sql)
			expiredCount++
		}
	}

	p.metrics.CounterAdd("query_planner_cache_entries_expired", float64(expiredCount), nil)
}

//Personal.AI order the ending
