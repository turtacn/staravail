// Package query provides functionality for SQL query parsing, pruning, and execution.
package query

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/turtacn/staravail/internal/common/config"
	"github.com/turtacn/staravail/internal/common/logging"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/domain/metadata"
	"github.com/turtacn/staravail/internal/domain/tablet"
	"github.com/xwb1989/sqlparser"
)

// PruningStrategy defines the strategy for query pruning
type PruningStrategy string

const (
	// PruningStrategyMaxData maximizes data availability, potentially increasing query complexity
	PruningStrategyMaxData PruningStrategy = "MAX_DATA"
	// PruningStrategyMinComplexity minimizes query complexity, potentially sacrificing some data
	PruningStrategyMinComplexity PruningStrategy = "MIN_COMPLEXITY"
	// PruningStrategyBalanced balances between data availability and query complexity
	PruningStrategyBalanced PruningStrategy = "BALANCED"
)

// PruningInfo represents information about a pruned query
type PruningInfo struct {
	// OriginalSQL is the original SQL query
	OriginalSQL string
	// PrunedSQL is the pruned SQL query
	PrunedSQL string
	// IsQueryPruned indicates if the query was pruned
	IsQueryPruned bool
	// PrunedPartitions contains information about the pruned partitions
	PrunedPartitions []PrunedPartitionInfo
	// PrunedBuckets contains information about the pruned buckets
	PrunedBuckets []PrunedBucketInfo
	// EstimatedDataLossPercentage is the estimated percentage of data lost due to pruning
	EstimatedDataLossPercentage float64
	// PruningStrategy is the strategy used for pruning
	PruningStrategy PruningStrategy
	// PruningTime is the time it took to prune the query
	PruningTime time.Duration
	// WarningMessage contains a warning message about potential data loss
	WarningMessage string
}

// PrunedPartitionInfo represents information about a pruned partition
type PrunedPartitionInfo struct {
	// TableName is the name of the table
	TableName string
	// PartitionName is the name of the partition
	PartitionName string
	// PartitionValue is the value of the partition
	PartitionValue string
	// PruningReason is the reason for pruning the partition
	PruningReason string
}

// PrunedBucketInfo represents information about a pruned bucket
type PrunedBucketInfo struct {
	// TableName is the name of the table
	TableName string
	// PartitionName is the name of the partition
	PartitionName string
	// BucketID is the ID of the bucket
	BucketID int
	// PruningReason is the reason for pruning the bucket
	PruningReason string
}

// QueryPruner defines the interface for pruning queries
type QueryPruner interface {
	// PruneQuery prunes a query to avoid unavailable tablets
	PruneQuery(ctx context.Context, sql string, strategy PruningStrategy) (string, PruningInfo, error)

	// CanPrune checks if a query can be pruned
	CanPrune(ctx context.Context, sql string) (bool, error)

	// GetPruningInfo gets pruning information for a query without actually pruning it
	GetPruningInfo(ctx context.Context, sql string, strategy PruningStrategy) (PruningInfo, error)

	// GetAvailabilityInfo gets availability information for tables and partitions in a query
	GetAvailabilityInfo(ctx context.Context, sql string) (map[string]float64, error)

	// SetPruningStrategy sets the default pruning strategy
	SetPruningStrategy(strategy PruningStrategy)
}

// QueryPrunerConfig represents configuration for the query pruner
type QueryPrunerConfig struct {
	// DefaultPruningStrategy is the default pruning strategy
	DefaultPruningStrategy PruningStrategy
	// EnablePartitionPruning enables partition-based pruning
	EnablePartitionPruning bool
	// EnableBucketPruning enables bucket-based pruning
	EnableBucketPruning bool
	// MinAvailabilityThreshold is the minimum availability threshold for pruning
	MinAvailabilityThreshold float64
	// CacheEnabled enables caching of pruning results
	CacheEnabled bool
	// CacheTTL is the cache time-to-live
	CacheTTL time.Duration
	// CacheMaxSize is the maximum size of the cache
	CacheMaxSize int
}

// cachedPruningInfo represents cached pruning information
type cachedPruningInfo struct {
	// Info is the pruning information
	Info PruningInfo
	// LastAccessed is when the cache entry was last accessed
	LastAccessed time.Time
}

// QueryPrunerImpl implements the QueryPruner interface
type QueryPrunerImpl struct {
	// config is the pruner configuration
	config QueryPrunerConfig
	// queryParser is used to parse SQL queries
	queryParser QueryParser
	// tabletManager is used to get tablet information
	tabletManager tablet.TabletManager
	// healthChecker is used to check tablet health
	healthChecker tablet.HealthChecker
	// metadataService is used to get metadata information
	metadataService metadata.MetadataService
	// logger is used for logging
	logger logging.Logger
	// metrics is used to record metrics
	metrics metrics.MetricsRecorder

	// pruningCache caches pruning information
	pruningCache map[string]cachedPruningInfo
	// cacheMutex protects the cache map
	cacheMutex sync.RWMutex
	// lastCacheCleanup is when the cache was last cleaned up
	lastCacheCleanup time.Time
}

// NewQueryPruner creates a new QueryPruner instance
func NewQueryPruner(
	cfg config.QueryPrunerConfig,
	queryParser QueryParser,
	tabletManager tablet.TabletManager,
	healthChecker tablet.HealthChecker,
	metadataService metadata.MetadataService,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) QueryPruner {
	// Convert config to internal pruner config
	prunerConfig := QueryPrunerConfig{
		DefaultPruningStrategy:   PruningStrategy(cfg.DefaultPruningStrategy),
		EnablePartitionPruning:   cfg.EnablePartitionPruning,
		EnableBucketPruning:      cfg.EnableBucketPruning,
		MinAvailabilityThreshold: cfg.MinAvailabilityThreshold,
		CacheEnabled:             cfg.CacheEnabled,
		CacheTTL:                 cfg.CacheTTL,
		CacheMaxSize:             cfg.CacheMaxSize,
	}

	return &QueryPrunerImpl{
		config:           prunerConfig,
		queryParser:      queryParser,
		tabletManager:    tabletManager,
		healthChecker:    healthChecker,
		metadataService:  metadataService,
		logger:           logger,
		metrics:          metricsRecorder,
		pruningCache:     make(map[string]cachedPruningInfo),
		lastCacheCleanup: time.Now(),
	}
}

// PruneQuery prunes a query to avoid unavailable tablets
func (p *QueryPrunerImpl) PruneQuery(ctx context.Context, sql string, strategy PruningStrategy) (string, PruningInfo, error) {
	// Check if the strategy is empty, use default if it is
	if strategy == "" {
		strategy = p.config.DefaultPruningStrategy
	}

	// Check if cached result is available
	if p.config.CacheEnabled {
		cacheKey := fmt.Sprintf("%s:%s", sql, strategy)
		if cachedInfo, found := p.getCachedPruningInfo(cacheKey); found {
			// Update metrics for cache hit
			p.metrics.CounterInc("query_pruner_cache_hit", nil)
			return cachedInfo.PrunedSQL, cachedInfo, nil
		}
	}

	// Update metrics for cache miss
	p.metrics.CounterInc("query_pruner_cache_miss", nil)

	// Start pruning time measurement
	startTime := time.Now()
	defer func() {
		// Record pruning time
		p.metrics.HistogramObserve("query_pruner_time_ms", float64(time.Since(startTime).Milliseconds()), nil)
	}()

	// Initialize pruning info
	pruningInfo := PruningInfo{
		OriginalSQL:     sql,
		PrunedSQL:       sql,
		IsQueryPruned:   false,
		PruningStrategy: strategy,
		PruningTime:     0,
	}

	// Check if pruning is possible
	canPrune, err := p.CanPrune(ctx, sql)
	if err != nil {
		p.logger.Error("Failed to check if query can be pruned", "sql", sql, "error", err)
		return sql, pruningInfo, err
	}

	if !canPrune {
		p.logger.Debug("Query cannot be pruned", "sql", sql)
		return sql, pruningInfo, nil
	}

	// Parse the query
	queryInfo, err := p.queryParser.ParseQuery(ctx, sql)
	if err != nil {
		p.logger.Error("Failed to parse query", "sql", sql, "error", err)
		return sql, pruningInfo, err
	}

	// Get the pruned SQL and pruning info
	prunedSQL, info, err := p.pruneQueryInternal(ctx, sql, queryInfo, strategy)
	if err != nil {
		p.logger.Error("Failed to prune query", "sql", sql, "error", err)
		return sql, pruningInfo, err
	}

	// Update pruning time
	info.PruningTime = time.Since(startTime)

	// Cache the result
	if p.config.CacheEnabled {
		cacheKey := fmt.Sprintf("%s:%s", sql, strategy)
		p.cachePruningInfo(cacheKey, info)
	}

	return prunedSQL, info, nil
}

// pruneQueryInternal performs the actual query pruning
func (p *QueryPrunerImpl) pruneQueryInternal(
	ctx context.Context,
	sql string,
	queryInfo QueryInfo,
	strategy PruningStrategy,
) (string, PruningInfo, error) {
	// Initialize pruning info
	pruningInfo := PruningInfo{
		OriginalSQL:      sql,
		PrunedSQL:        sql,
		IsQueryPruned:    false,
		PrunedPartitions: []PrunedPartitionInfo{},
		PrunedBuckets:    []PrunedBucketInfo{},
		PruningStrategy:  strategy,
	}

	// Parse the SQL statement
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		p.logger.Error("Failed to parse SQL", "sql", sql, "error", err)
		return sql, pruningInfo, errors.Wrap(err, "failed to parse SQL")
	}

	// Only prune SELECT, UPDATE, and DELETE queries with WHERE clauses
	var whereExpr sqlparser.Expr
	var whereClause *sqlparser.Where
	switch s := stmt.(type) {
	case *sqlparser.Select:
		if s.Where != nil {
			whereExpr = s.Where.Expr
			whereClause = s.Where
		}
	case *sqlparser.Update:
		if s.Where != nil {
			whereExpr = s.Where.Expr
			whereClause = s.Where
		}
	case *sqlparser.Delete:
		if s.Where != nil {
			whereExpr = s.Where.Expr
			whereClause = s.Where
		}
	default:
		return sql, pruningInfo, nil
	}

	if whereExpr == nil {
		// No WHERE clause to modify
		return sql, pruningInfo, nil
	}

	// Get the unavailable partitions and buckets for each table
	unavailablePartitions := make(map[string]map[string]string) // map[tableName]map[partitionName]reason
	unavailableBuckets := make(map[string]map[int]string)       // map[tableName]map[bucketID]reason

	totalPartitions := 0
	totalBuckets := 0
	unavailablePartitionCount := 0
	unavailableBucketCount := 0

	for _, table := range queryInfo.Tables {
		// Get the health status for the table
		tableHealth, err := p.healthChecker.GetTableHealthStatus(ctx, table.DatabaseName, table.TableName)
		if err != nil {
			p.logger.Error("Failed to get table health status",
				"database", table.DatabaseName,
				"table", table.TableName,
				"error", err)
			continue
		}

		// Initialize maps for this table
		unavailablePartitions[table.TableName] = make(map[string]string)
		unavailableBuckets[table.TableName] = make(map[int]string)

		// Process partitions
		if p.config.EnablePartitionPruning {
			partitions, err := p.metadataService.GetTablePartitions(ctx, table.DatabaseName, table.TableName)
			if err != nil {
				p.logger.Error("Failed to get table partitions",
					"database", table.DatabaseName,
					"table", table.TableName,
					"error", err)
				continue
			}

			totalPartitions += len(partitions)

			for _, partition := range partitions {
				partitionHealth, err := p.healthChecker.GetPartitionHealthStatus(
					ctx,
					table.DatabaseName,
					table.TableName,
					partition.Name,
				)
				if err != nil {
					p.logger.Error("Failed to get partition health status",
						"database", table.DatabaseName,
						"table", table.TableName,
						"partition", partition.Name,
						"error", err)
					continue
				}

				// Check if partition is unhealthy and should be pruned
				if partitionHealth.AvailabilityRatio < p.config.MinAvailabilityThreshold {
					unavailablePartitions[table.TableName][partition.Name] = fmt.Sprintf(
						"Availability ratio %.2f%% below threshold %.2f%%",
						partitionHealth.AvailabilityRatio*100,
						p.config.MinAvailabilityThreshold*100,
					)
					unavailablePartitionCount++

					// Add to pruning info
					pruningInfo.PrunedPartitions = append(pruningInfo.PrunedPartitions, PrunedPartitionInfo{
						TableName:      table.TableName,
						PartitionName:  partition.Name,
						PartitionValue: partition.Value,
						PruningReason:  unavailablePartitions[table.TableName][partition.Name],
					})
				}
			}
		}

		// Process buckets
		if p.config.EnableBucketPruning {
			buckets, err := p.metadataService.GetTableBuckets(ctx, table.DatabaseName, table.TableName)
			if err != nil {
				p.logger.Error("Failed to get table buckets",
					"database", table.DatabaseName,
					"table", table.TableName,
					"error", err)
				continue
			}

			totalBuckets += len(buckets)

			for _, bucket := range buckets {
				bucketHealth, err := p.healthChecker.GetBucketHealthStatus(
					ctx,
					table.DatabaseName,
					table.TableName,
					bucket.ID,
				)
				if err != nil {
					p.logger.Error("Failed to get bucket health status",
						"database", table.DatabaseName,
						"table", table.TableName,
						"bucket", bucket.ID,
						"error", err)
					continue
				}

				// Check if bucket is unhealthy and should be pruned
				if bucketHealth.IsHealthy == false {
					unavailableBuckets[table.TableName][bucket.ID] = "Unhealthy bucket"
					unavailableBucketCount++

					// Add to pruning info
					pruningInfo.PrunedBuckets = append(pruningInfo.PrunedBuckets, PrunedBucketInfo{
						TableName:     table.TableName,
						PartitionName: bucket.PartitionName,
						BucketID:      bucket.ID,
						PruningReason: unavailableBuckets[table.TableName][bucket.ID],
					})
				}
			}
		}
	}

	// If nothing is unavailable, no need to prune
	if unavailablePartitionCount == 0 && unavailableBucketCount == 0 {
		return sql, pruningInfo, nil
	}

	// Estimate data loss percentage
	var estimatedDataLossPercentage float64
	if totalPartitions > 0 {
		estimatedDataLossPercentage = float64(unavailablePartitionCount) / float64(totalPartitions) * 100
	}
	if totalBuckets > 0 {
		bucketLossPercentage := float64(unavailableBucketCount) / float64(totalBuckets) * 100
		// Take the max of partition and bucket loss percentages
		if bucketLossPercentage > estimatedDataLossPercentage {
			estimatedDataLossPercentage = bucketLossPercentage
		}
	}
	pruningInfo.EstimatedDataLossPercentage = estimatedDataLossPercentage

	// Generate warning message
	warningMessage := fmt.Sprintf(
		"Query pruned to exclude %d unavailable partitions and %d unavailable buckets. "+
			"Estimated data loss: %.2f%%",
		unavailablePartitionCount,
		unavailableBucketCount,
		estimatedDataLossPercentage,
	)
	pruningInfo.WarningMessage = warningMessage

	// Build new WHERE clause that excludes unavailable partitions and buckets
	newWhereExpr, modified := p.buildPrunedWhereClause(
		whereExpr,
		queryInfo,
		unavailablePartitions,
		unavailableBuckets,
		strategy,
	)

	// If WHERE clause was not modified, return original SQL
	if !modified {
		return sql, pruningInfo, nil
	}

	// Update the WHERE clause in the statement
	whereClause.Expr = newWhereExpr

	// Generate the pruned SQL
	prunedSQL := sqlparser.String(stmt)
	pruningInfo.PrunedSQL = prunedSQL
	pruningInfo.IsQueryPruned = true

	return prunedSQL, pruningInfo, nil
}

// buildPrunedWhereClause builds a new WHERE clause that excludes unavailable partitions and buckets
func (p *QueryPrunerImpl) buildPrunedWhereClause(
	expr sqlparser.Expr,
	queryInfo QueryInfo,
	unavailablePartitions map[string]map[string]string,
	unavailableBuckets map[string]map[int]string,
	strategy PruningStrategy,
) (sqlparser.Expr, bool) {
	// Clone the original expression
	newExpr := sqlparser.CloneExpr(expr)

	// If there are no unavailable partitions or buckets, no need to modify
	if len(unavailablePartitions) == 0 && len(unavailableBuckets) == 0 {
		return newExpr, false
	}

	// Build exclusion conditions for partitions
	var partitionExclusions []sqlparser.Expr
	if len(unavailablePartitions) > 0 && len(queryInfo.PartitionConditions) > 0 {
		for _, partCond := range queryInfo.PartitionConditions {
			tableName := ""
			// Find the table for this partition column
			for _, table := range queryInfo.Tables {
				// Check if the partition column belongs to this table
				columns, err := p.metadataService.GetPartitionColumns(context.Background(), table.DatabaseName, table.TableName)
				if err != nil {
					p.logger.Error("Failed to get partition columns",
						"database", table.DatabaseName,
						"table", table.TableName,
						"error", err)
					continue
				}

				for _, col := range columns {
					if col == partCond.Column {
						tableName = table.TableName
						break
					}
				}

				if tableName != "" {
					break
				}
			}

			if tableName == "" || len(unavailablePartitions[tableName]) == 0 {
				continue
			}

			// Get partition values to exclude
			partitionValues := make([]string, 0, len(unavailablePartitions[tableName]))
			for partName := range unavailablePartitions[tableName] {
				// Get the partition value from metadata
				partInfo, err := p.metadataService.GetPartitionInfo(
					context.Background(),
					"", // We don't have database name here, but tableName should be unique in this context
					tableName,
					partName,
				)
				if err != nil {
					p.logger.Error("Failed to get partition info",
						"table", tableName,
						"partition", partName,
						"error", err)
					continue
				}
				partitionValues = append(partitionValues, partInfo.Value)
			}

			if len(partitionValues) == 0 {
				continue
			}

			// Build exclusion condition based on partition column and values
			var exclusionExpr sqlparser.Expr
			if len(partitionValues) == 1 {
				// Single value exclusion
				exclusionExpr = &sqlparser.ComparisonExpr{
					Operator: "!=",
					Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent(partCond.Column)},
					Right:    sqlparser.NewStrVal([]byte(partitionValues[0])),
				}
			} else {
				// Multiple values exclusion
				valTuple := sqlparser.ValTuple{}
				for _, val := range partitionValues {
					valTuple = append(valTuple, sqlparser.NewStrVal([]byte(val)))
				}
				exclusionExpr = &sqlparser.ComparisonExpr{
					Operator: "not in",
					Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent(partCond.Column)},
					Right:    valTuple,
				}
			}
			partitionExclusions = append(partitionExclusions, exclusionExpr)
		}
	}

	// Build exclusion conditions for buckets
	var bucketExclusions []sqlparser.Expr
	if len(unavailableBuckets) > 0 && len(queryInfo.BucketConditions) > 0 {
		for _, bucketCond := range queryInfo.BucketConditions {
			tableName := ""
			// Find the table for this bucket column
			for _, table := range queryInfo.Tables {
				// Check if the bucket column belongs to this table
				columns, err := p.metadataService.GetBucketColumns(context.Background(), table.DatabaseName, table.TableName)
				if err != nil {
					p.logger.Error("Failed to get bucket columns",
						"database", table.DatabaseName,
						"table", table.TableName,
						"error", err)
					continue
				}

				for _, col := range columns {
					if col == bucketCond.Column {
						tableName = table.TableName
						break
					}
				}

				if tableName != "" {
					break
				}
			}

			if tableName == "" || len(unavailableBuckets[tableName]) == 0 {
				continue
			}

			// Get bucket IDs to exclude
			bucketIDs := make([]int, 0, len(unavailableBuckets[tableName]))
			for bucketID := range unavailableBuckets[tableName] {
				bucketIDs = append(bucketIDs, bucketID)
			}

			if len(bucketIDs) == 0 {
				continue
			}

			// Build exclusion condition based on bucket hash function
			// Note: This is a simplified example. The actual implementation would depend on
			// how StarRocks implements bucket hashing.
			var exclusionExpr sqlparser.Expr
			if len(bucketIDs) == 1 {
				// Single bucket exclusion
				exclusionExpr = &sqlparser.ComparisonExpr{
					Operator: "!=",
					Left: &sqlparser.FuncExpr{
						Name: sqlparser.NewColIdent("MOD"),
						Exprs: sqlparser.SelectExprs{
							&sqlparser.AliasedExpr{
								Expr: &sqlparser.FuncExpr{
									Name: sqlparser.NewColIdent("CRC32"),
									Exprs: sqlparser.SelectExprs{
										&sqlparser.AliasedExpr{
											Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent(bucketCond.Column)},
										},
									},
								},
							},
							&sqlparser.AliasedExpr{
								Expr: sqlparser.NewIntVal([]byte("16")), // Assuming 16 buckets, adjust as needed
							},
						},
					},
					Right: sqlparser.NewIntVal([]byte(fmt.Sprintf("%d", bucketIDs[0]))),
				}
			} else {
				// Multiple buckets exclusion
				valTuple := sqlparser.ValTuple{}
				for _, id := range bucketIDs {
					valTuple = append(valTuple, sqlparser.NewIntVal([]byte(fmt.Sprintf("%d", id))))
				}
				exclusionExpr = &sqlparser.ComparisonExpr{
					Operator: "not in",
					Left: &sqlparser.FuncExpr{
						Name: sqlparser.NewColIdent("MOD"),
						Exprs: sqlparser.SelectExprs{
							&sqlparser.AliasedExpr{
								Expr: &sqlparser.FuncExpr{
									Name: sqlparser.NewColIdent("CRC32"),
									Exprs: sqlparser.SelectExprs{
										&sqlparser.AliasedExpr{
											Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent(bucketCond.Column)},
										},
									},
								},
							},
							&sqlparser.AliasedExpr{
								Expr: sqlparser.NewIntVal([]byte("16")), // Assuming 16 buckets, adjust as needed
							},
						},
					},
					Right: valTuple,
				}
			}
			bucketExclusions = append(bucketExclusions, exclusionExpr)
		}
	}

	// If no exclusions were built, no need to modify the WHERE clause
	if len(partitionExclusions) == 0 && len(bucketExclusions) == 0 {
		return newExpr, false
	}

	// Combine all exclusions based on the pruning strategy
	var combinedExclusions sqlparser.Expr
	var allExclusions []sqlparser.Expr

	allExclusions = append(allExclusions, partitionExclusions...)
	allExclusions = append(allExclusions, bucketExclusions...)

	if len(allExclusions) == 1 {
		combinedExclusions = allExclusions[0]
	} else {
		// Combine exclusions based on strategy
		switch strategy {
		case PruningStrategyMaxData:
			// For maximum data, use OR between exclusions
			// (exclude only if data is unavailable in all specified dimensions)
			combinedExclusions = allExclusions[0]
			for i := 1; i < len(allExclusions); i++ {
				combinedExclusions = &sqlparser.OrExpr{
					Left:  combinedExclusions,
					Right: allExclusions[i],
				}
			}
		case PruningStrategyMinComplexity:
			// For minimum complexity, use AND between exclusions
			// (exclude if data is unavailable in any specified dimension)
			combinedExclusions = allExclusions[0]
			for i := 1; i < len(allExclusions); i++ {
				combinedExclusions = &sqlparser.AndExpr{
					Left:  combinedExclusions,
					Right: allExclusions[i],
				}
			}
		case PruningStrategyBalanced:
			// For balanced approach, group exclusions by table
			// This is a simplified implementation
			combinedExclusions = allExclusions[0]
			for i := 1; i < len(allExclusions); i++ {
				if i < len(partitionExclusions) {
					// AND between partition exclusions
					combinedExclusions = &sqlparser.AndExpr{
						Left:  combinedExclusions,
						Right: allExclusions[i],
					}
				} else {
					// OR between bucket exclusions
					combinedExclusions = &sqlparser.OrExpr{
						Left:  combinedExclusions,
						Right: allExclusions[i],
					}
				}
			}
		default:
			// Default to balanced approach
			combinedExclusions = allExclusions[0]
			for i := 1; i < len(allExclusions); i++ {
				combinedExclusions = &sqlparser.AndExpr{
					Left:  combinedExclusions,
					Right: allExclusions[i],
				}
			}
		}
	}

	// Combine the original WHERE clause with the exclusions
	return &sqlparser.AndExpr{
		Left:  newExpr,
		Right: combinedExclusions,
	}, true
}

// CanPrune checks if a query can be pruned
func (p *QueryPrunerImpl) CanPrune(ctx context.Context, sql string) (bool, error) {
	// Parse the query
	queryInfo, err := p.queryParser.ParseQuery(ctx, sql)
	if err != nil {
		p.logger.Error("Failed to parse query", "sql", sql, "error", err)
		return false, err
	}

	// Check if the query is a type that can be pruned
	if queryInfo.Type != QueryTypeSelect &&
		queryInfo.Type != QueryTypeUpdate &&
		queryInfo.Type != QueryTypeDelete {
		return false, nil
	}

	// Check if the query has tables
	if len(queryInfo.Tables) == 0 {
		return false, nil
	}

	// For SELECT queries, we need a WHERE clause to prune
	if queryInfo.Type == QueryTypeSelect {
		stmt, err := sqlparser.Parse(sql)
		if err != nil {
			p.logger.Error("Failed to parse SQL", "sql", sql, "error", err)
			return false, errors.Wrap(err, "failed to parse SQL")
		}

		selectStmt, ok := stmt.(*sqlparser.Select)
		if !ok {
			return false, nil
		}

		if selectStmt.Where == nil {
			return false, nil
		}
	}

	// Check if the query has partition or bucket conditions
	hasPartitionConditions := len(queryInfo.PartitionConditions) > 0
	hasBucketConditions := len(queryInfo.BucketConditions) > 0

	// Can prune if either partition or bucket conditions are present
	return hasPartitionConditions || hasBucketConditions, nil
}

// GetPruningInfo gets pruning information for a query without actually pruning it
func (p *QueryPrunerImpl) GetPruningInfo(ctx context.Context, sql string, strategy PruningStrategy) (PruningInfo, error) {
	// Check if the strategy is empty, use default if it is
	if strategy == "" {
		strategy = p.config.DefaultPruningStrategy
	}

	// Check if cached result is available
	if p.config.CacheEnabled {
		cacheKey := fmt.Sprintf("%s:%s:info", sql, strategy)
		if cachedInfo, found := p.getCachedPruningInfo(cacheKey); found {
			// Update metrics for cache hit
			p.metrics.CounterInc("query_pruner_info_cache_hit", nil)
			return cachedInfo, nil
		}
	}

	// Update metrics for cache miss
	p.metrics.CounterInc("query_pruner_info_cache_miss", nil)

	// Start pruning time measurement
	startTime := time.Now()

	// Parse the query
	queryInfo, err := p.queryParser.ParseQuery(ctx, sql)
	if err != nil {
		p.logger.Error("Failed to parse query", "sql", sql, "error", err)
		return PruningInfo{}, err
	}

	// Get the pruning info without actually pruning the query
	_, info, err := p.pruneQueryInternal(ctx, sql, queryInfo, strategy)
	if err != nil {
		p.logger.Error("Failed to get pruning info", "sql", sql, "error", err)
		return PruningInfo{}, err
	}

	// Update pruning time
	info.PruningTime = time.Since(startTime)

	// Cache the result
	if p.config.CacheEnabled {
		cacheKey := fmt.Sprintf("%s:%s:info", sql, strategy)
		p.cachePruningInfo(cacheKey, info)
	}

	return info, nil
}

// GetAvailabilityInfo gets availability information for tables and partitions in a query
func (p *QueryPrunerImpl) GetAvailabilityInfo(ctx context.Context, sql string) (map[string]float64, error) {
	// Parse the query
	queryInfo, err := p.queryParser.ParseQuery(ctx, sql)
	if err != nil {
		p.logger.Error("Failed to parse query", "sql", sql, "error", err)
		return nil, err
	}

	// Initialize availability map
	availabilityMap := make(map[string]float64)

	// Get availability for each table
	for _, table := range queryInfo.Tables {
		// Get the health status for the table
		tableHealth, err := p.healthChecker.GetTableHealthStatus(ctx, table.DatabaseName, table.TableName)
		if err != nil {
			p.logger.Error("Failed to get table health status",
				"database", table.DatabaseName,
				"table", table.TableName,
				"error", err)
			availabilityMap[table.TableName] = 0.0
			continue
		}

		availabilityMap[table.TableName] = tableHealth.AvailabilityRatio

		// Get availability for each partition
		partitions, err := p.metadataService.GetTablePartitions(ctx, table.DatabaseName, table.TableName)
		if err != nil {
			p.logger.Error("Failed to get table partitions",
				"database", table.DatabaseName,
				"table", table.TableName,
				"error", err)
			continue
		}

		for _, partition := range partitions {
			partitionHealth, err := p.healthChecker.GetPartitionHealthStatus(
				ctx,
				table.DatabaseName,
				table.TableName,
				partition.Name,
			)
			if err != nil {
				p.logger.Error("Failed to get partition health status",
					"database", table.DatabaseName,
					"table", table.TableName,
					"partition", partition.Name,
					"error", err)
				availabilityMap[fmt.Sprintf("%s.%s", table.TableName, partition.Name)] = 0.0
				continue
			}

			availabilityMap[fmt.Sprintf("%s.%s", table.TableName, partition.Name)] = partitionHealth.AvailabilityRatio
		}
	}

	return availabilityMap, nil
}

// SetPruningStrategy sets the default pruning strategy
func (p *QueryPrunerImpl) SetPruningStrategy(strategy PruningStrategy) {
	p.config.DefaultPruningStrategy = strategy
	p.logger.Info("Set default pruning strategy", "strategy", strategy)
}

// getCachedPruningInfo tries to get cached pruning info
func (p *QueryPrunerImpl) getCachedPruningInfo(cacheKey string) (PruningInfo, bool) {
	p.cacheMutex.RLock()
	defer p.cacheMutex.RUnlock()

	// Check if we need to clean up the cache
	if time.Since(p.lastCacheCleanup) > 10*time.Minute {
		go p.cleanupCache()
	}

	// Check if the query is in the cache
	if entry, found := p.pruningCache[cacheKey]; found {
		// Check if the cache entry is still valid
		if time.Since(entry.LastAccessed) <= p.config.CacheTTL {
			// Update last accessed time
			entry.LastAccessed = time.Now()
			p.pruningCache[cacheKey] = entry
			return entry.Info, true
		}
	}

	return PruningInfo{}, false
}

// cachePruningInfo caches pruning info
func (p *QueryPrunerImpl) cachePruningInfo(cacheKey string, info PruningInfo) {
	p.cacheMutex.Lock()
	defer p.cacheMutex.Unlock()

	// Check if the cache is full
	if len(p.pruningCache) >= p.config.CacheMaxSize {
		// If full, remove the oldest entries
		p.evictOldestEntries(p.config.CacheMaxSize / 10) // Remove 10% of entries
	}

	// Add the new entry
	p.pruningCache[cacheKey] = cachedPruningInfo{
		Info:         info,
		LastAccessed: time.Now(),
	}
}

// evictOldestEntries evicts the oldest entries from the cache
func (p *QueryPrunerImpl) evictOldestEntries(count int) {
	if count <= 0 || len(p.pruningCache) == 0 {
		return
	}

	// Collect all entries with their last accessed time
	type cacheEntry struct {
		key          string
		lastAccessed time.Time
	}
	entries := make([]cacheEntry, 0, len(p.pruningCache))

	for key, info := range p.pruningCache {
		entries = append(entries, cacheEntry{
			key:          key,
			lastAccessed: info.LastAccessed,
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
		delete(p.pruningCache, entries[i].key)
	}

	p.metrics.CounterAdd("query_pruner_cache_entries_evicted", float64(numToRemove), nil)
}

// cleanupCache cleans up expired entries from the cache
func (p *QueryPrunerImpl) cleanupCache() {
	p.cacheMutex.Lock()
	defer p.cacheMutex.Unlock()

	now := time.Now()
	p.lastCacheCleanup = now
	expiredCount := 0

	// Remove expired entries
	for key, info := range p.pruningCache {
		if now.Sub(info.LastAccessed) > p.config.CacheTTL {
			delete(p.pruningCache, key)
			expiredCount++
		}
	}

	p.metrics.CounterAdd("query_pruner_cache_entries_expired", float64(expiredCount), nil)
}

//Personal.AI order the ending
