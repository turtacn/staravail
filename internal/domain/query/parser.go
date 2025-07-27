// Package query provides functionality for SQL query parsing and execution.
package query

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/turtacn/staravail/internal/common/config"
	"github.com/turtacn/staravail/internal/common/logging"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/domain/metadata"
	"github.com/xwb1989/sqlparser"
)

// QueryType represents the type of SQL query
type QueryType string

const (
	// QueryTypeSelect represents a SELECT query
	QueryTypeSelect QueryType = "SELECT"
	// QueryTypeInsert represents an INSERT query
	QueryTypeInsert QueryType = "INSERT"
	// QueryTypeUpdate represents an UPDATE query
	QueryTypeUpdate QueryType = "UPDATE"
	// QueryTypeDelete represents a DELETE query
	QueryTypeDelete QueryType = "DELETE"
	// QueryTypeCreate represents a CREATE query
	QueryTypeCreate QueryType = "CREATE"
	// QueryTypeAlter represents an ALTER query
	QueryTypeAlter QueryType = "ALTER"
	// QueryTypeDrop represents a DROP query
	QueryTypeDrop QueryType = "DROP"
	// QueryTypeShow represents a SHOW query
	QueryTypeShow QueryType = "SHOW"
	// QueryTypeExplain represents an EXPLAIN query
	QueryTypeExplain QueryType = "EXPLAIN"
	// QueryTypeDescribe represents a DESCRIBE query
	QueryTypeDescribe QueryType = "DESCRIBE"
	// QueryTypeUse represents a USE query
	QueryTypeUse QueryType = "USE"
	// QueryTypeOther represents other query types
	QueryTypeOther QueryType = "OTHER"
)

// PartitionCondition represents a condition on a partition column
type PartitionCondition struct {
	// Column is the name of the partition column
	Column string
	// Operator is the comparison operator (=, >, <, >=, <=, IN, BETWEEN)
	Operator string
	// Values contains the value(s) for the condition
	Values []interface{}
}

// BucketCondition represents a condition on a bucket column
type BucketCondition struct {
	// Column is the name of the bucket column
	Column string
	// Operator is the comparison operator (=, >, <, >=, <=, IN, BETWEEN)
	Operator string
	// Values contains the value(s) for the condition
	Values []interface{}
}

// TableReference represents a table referenced in a SQL query
type TableReference struct {
	// DatabaseName is the name of the database
	DatabaseName string
	// TableName is the name of the table
	TableName string
	// Alias is the alias of the table in the query, if any
	Alias string
}

// QueryInfo represents information about a parsed query
type QueryInfo struct {
	// OriginalSQL is the original SQL query
	OriginalSQL string
	// Type is the type of query
	Type QueryType
	// Tables is the list of tables referenced in the query
	Tables []TableReference
	// PartitionConditions is the list of partition conditions in the query
	PartitionConditions []PartitionCondition
	// BucketConditions is the list of bucket conditions in the query
	BucketConditions []BucketCondition
	// HasAggregation indicates if the query has aggregation
	HasAggregation bool
	// HasGroupBy indicates if the query has GROUP BY
	HasGroupBy bool
	// HasOrderBy indicates if the query has ORDER BY
	HasOrderBy bool
	// HasLimit indicates if the query has LIMIT
	HasLimit bool
	// LimitValue is the value of LIMIT, if any
	LimitValue int64
	// HasJoin indicates if the query has JOIN
	HasJoin bool
	// JoinTables is the list of tables involved in JOINs
	JoinTables []TableReference
	// HasSubquery indicates if the query has subqueries
	HasSubquery bool
	// IsValid indicates if the query is valid
	IsValid bool
	// ValidationErrors contains validation errors, if any
	ValidationErrors []string
	// ParsedAt is when the query was parsed
	ParsedAt time.Time
}

// QueryParser defines the interface for parsing SQL queries
type QueryParser interface {
	// ParseQuery parses a SQL query and returns information about it
	ParseQuery(ctx context.Context, sql string) (QueryInfo, error)

	// ExtractTables extracts tables referenced in a SQL query
	ExtractTables(ctx context.Context, sql string) ([]TableReference, error)

	// ExtractPartitionConditions extracts partition conditions from a SQL query
	ExtractPartitionConditions(ctx context.Context, sql string, tableName string, partitionColumns []string) ([]PartitionCondition, error)

	// ExtractBucketConditions extracts bucket conditions from a SQL query
	ExtractBucketConditions(ctx context.Context, sql string, tableName string, bucketColumns []string) ([]BucketCondition, error)

	// ValidateQuery validates a SQL query against StarRocks syntax
	ValidateQuery(ctx context.Context, sql string) (bool, []string, error)

	// GetQueryType gets the type of a SQL query
	GetQueryType(ctx context.Context, sql string) (QueryType, error)

	// ClearCache clears the parser cache
	ClearCache()
}

// QueryParserConfig represents configuration for the query parser
type QueryParserConfig struct {
	// CacheEnabled indicates if caching is enabled
	CacheEnabled bool
	// CacheTTL is the cache time-to-live
	CacheTTL time.Duration
	// CacheMaxSize is the maximum size of the cache
	CacheMaxSize int
	// EnableStrictValidation enables strict SQL validation
	EnableStrictValidation bool
}

// cachedQueryInfo represents cached query information
type cachedQueryInfo struct {
	// Info is the query information
	Info QueryInfo
	// LastAccessed is when the cache entry was last accessed
	LastAccessed time.Time
}

// QueryParserImpl implements the QueryParser interface
type QueryParserImpl struct {
	// config is the parser configuration
	config QueryParserConfig
	// metadataService is used to get metadata information
	metadataService metadata.MetadataService
	// logger is used for logging
	logger logging.Logger
	// metrics is used to record metrics
	metrics metrics.MetricsRecorder

	// queryCache caches parsed query information
	queryCache map[string]cachedQueryInfo
	// cacheMutex protects the cache map
	cacheMutex sync.RWMutex
	// lastCacheCleanup is when the cache was last cleaned up
	lastCacheCleanup time.Time
}

// NewQueryParser creates a new QueryParser instance
func NewQueryParser(
	cfg config.QueryParserConfig,
	metadataService metadata.MetadataService,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) QueryParser {
	// Convert config to internal parser config
	parserConfig := QueryParserConfig{
		CacheEnabled:           cfg.CacheEnabled,
		CacheTTL:               cfg.CacheTTL,
		CacheMaxSize:           cfg.CacheMaxSize,
		EnableStrictValidation: cfg.EnableStrictValidation,
	}

	return &QueryParserImpl{
		config:           parserConfig,
		metadataService:  metadataService,
		logger:           logger,
		metrics:          metricsRecorder,
		queryCache:       make(map[string]cachedQueryInfo),
		lastCacheCleanup: time.Now(),
	}
}

// ParseQuery parses a SQL query and returns information about it
func (p *QueryParserImpl) ParseQuery(ctx context.Context, sql string) (QueryInfo, error) {
	// Check if cached result is available
	if p.config.CacheEnabled {
		if cachedInfo, found := p.getCachedQueryInfo(sql); found {
			// Update metrics for cache hit
			p.metrics.CounterInc("query_parser_cache_hit", nil)
			return cachedInfo, nil
		}
	}

	// Update metrics for cache miss
	p.metrics.CounterInc("query_parser_cache_miss", nil)

	// Initialize query info
	info := QueryInfo{
		OriginalSQL: sql,
		IsValid:     true,
		ParsedAt:    time.Now(),
	}

	// Start parsing time measurement
	startTime := time.Now()
	defer func() {
		// Record parsing time
		p.metrics.HistogramObserve("query_parser_parse_time_ms", float64(time.Since(startTime).Milliseconds()), nil)
	}()

	// Validate and get query type
	valid, validationErrors, err := p.ValidateQuery(ctx, sql)
	if err != nil {
		p.logger.Error("Failed to validate query", "sql", sql, "error", err)
		return QueryInfo{}, err
	}

	info.IsValid = valid
	info.ValidationErrors = validationErrors

	// If query is not valid, return the validation errors
	if !valid {
		if p.config.CacheEnabled {
			p.cacheQueryInfo(sql, info)
		}
		return info, nil
	}

	// Get query type
	queryType, err := p.GetQueryType(ctx, sql)
	if err != nil {
		p.logger.Error("Failed to get query type", "sql", sql, "error", err)
		return QueryInfo{}, err
	}
	info.Type = queryType

	// Extract tables
	tables, err := p.ExtractTables(ctx, sql)
	if err != nil {
		p.logger.Error("Failed to extract tables", "sql", sql, "error", err)
		return QueryInfo{}, err
	}
	info.Tables = tables

	// If there are no tables, we can't extract partition or bucket conditions
	if len(tables) == 0 {
		if p.config.CacheEnabled {
			p.cacheQueryInfo(sql, info)
		}
		return info, nil
	}

	// Parse the SQL statement
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		p.logger.Error("Failed to parse SQL", "sql", sql, "error", err)
		return QueryInfo{}, errors.Wrap(err, "failed to parse SQL")
	}

	// Extract additional information based on query type
	switch queryType {
	case QueryTypeSelect:
		p.extractSelectInfo(ctx, stmt, &info)
	case QueryTypeInsert:
		p.extractInsertInfo(ctx, stmt, &info)
	case QueryTypeUpdate:
		p.extractUpdateInfo(ctx, stmt, &info)
	case QueryTypeDelete:
		p.extractDeleteInfo(ctx, stmt, &info)
	}

	// Cache the result
	if p.config.CacheEnabled {
		p.cacheQueryInfo(sql, info)
	}

	return info, nil
}

// extractSelectInfo extracts information from a SELECT statement
func (p *QueryParserImpl) extractSelectInfo(ctx context.Context, stmt sqlparser.Statement, info *QueryInfo) {
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return
	}

	// Check for aggregation
	visitor := newAggregationVisitor()
	sqlparser.Walk(visitor, selectStmt.SelectExprs)
	info.HasAggregation = visitor.hasAggregation

	// Check for GROUP BY
	info.HasGroupBy = len(selectStmt.GroupBy) > 0

	// Check for ORDER BY
	info.HasOrderBy = len(selectStmt.OrderBy) > 0

	// Check for LIMIT
	if selectStmt.Limit != nil && selectStmt.Limit.Rowcount != nil {
		info.HasLimit = true
		switch rowcount := selectStmt.Limit.Rowcount.(type) {
		case *sqlparser.SQLVal:
			if rowcount.Type == sqlparser.IntVal {
				val, err := strconv.ParseInt(string(rowcount.Val), 10, 64)
				if err == nil {
					info.LimitValue = val
				}
			}
		}
	}

	// Check for JOINs
	joinVisitor := newJoinVisitor()
	sqlparser.Walk(joinVisitor, selectStmt.From)
	info.HasJoin = joinVisitor.hasJoin
	info.JoinTables = joinVisitor.joinTables

	// Check for subqueries
	subqueryVisitor := newSubqueryVisitor()
	sqlparser.Walk(subqueryVisitor, selectStmt)
	info.HasSubquery = subqueryVisitor.hasSubquery

	// Extract partition and bucket conditions for each table
	for _, table := range info.Tables {
		// Get partition columns for this table
		partitionColumns, err := p.metadataService.GetPartitionColumns(ctx, table.DatabaseName, table.TableName)
		if err != nil {
			p.logger.Error("Failed to get partition columns",
				"database", table.DatabaseName,
				"table", table.TableName,
				"error", err)
			continue
		}

		// Extract partition conditions
		if selectStmt.Where != nil && len(partitionColumns) > 0 {
			partitionConds, err := p.extractConditionsFromExpr(
				selectStmt.Where.Expr,
				table.TableName,
				table.Alias,
				partitionColumns,
			)
			if err != nil {
				p.logger.Error("Failed to extract partition conditions",
					"database", table.DatabaseName,
					"table", table.TableName,
					"error", err)
				continue
			}
			info.PartitionConditions = append(info.PartitionConditions, partitionConds...)
		}

		// Get bucket columns for this table
		bucketColumns, err := p.metadataService.GetBucketColumns(ctx, table.DatabaseName, table.TableName)
		if err != nil {
			p.logger.Error("Failed to get bucket columns",
				"database", table.DatabaseName,
				"table", table.TableName,
				"error", err)
			continue
		}

		// Extract bucket conditions
		if selectStmt.Where != nil && len(bucketColumns) > 0 {
			bucketConds, err := p.extractConditionsFromExpr(
				selectStmt.Where.Expr,
				table.TableName,
				table.Alias,
				bucketColumns,
			)
			if err != nil {
				p.logger.Error("Failed to extract bucket conditions",
					"database", table.DatabaseName,
					"table", table.TableName,
					"error", err)
				continue
			}
			for _, cond := range bucketConds {
				info.BucketConditions = append(info.BucketConditions, BucketCondition{
					Column:   cond.Column,
					Operator: cond.Operator,
					Values:   cond.Values,
				})
			}
		}
	}
}

// extractInsertInfo extracts information from an INSERT statement
func (p *QueryParserImpl) extractInsertInfo(ctx context.Context, stmt sqlparser.Statement, info *QueryInfo) {
	insertStmt, ok := stmt.(*sqlparser.Insert)
	if !ok {
		return
	}

	// Check for subqueries in the VALUES clause
	subqueryVisitor := newSubqueryVisitor()
	sqlparser.Walk(subqueryVisitor, insertStmt.Rows)
	info.HasSubquery = subqueryVisitor.hasSubquery

	// For INSERT statements, we've already extracted the target table
	// We don't need to extract partition or bucket conditions as those
	// are not applicable for INSERT statements in the same way as SELECT
}

// extractUpdateInfo extracts information from an UPDATE statement
func (p *QueryParserImpl) extractUpdateInfo(ctx context.Context, stmt sqlparser.Statement, info *QueryInfo) {
	updateStmt, ok := stmt.(*sqlparser.Update)
	if !ok {
		return
	}

	// Check for subqueries
	subqueryVisitor := newSubqueryVisitor()
	sqlparser.Walk(subqueryVisitor, updateStmt)
	info.HasSubquery = subqueryVisitor.hasSubquery

	// Extract partition and bucket conditions for the table
	if len(info.Tables) > 0 {
		table := info.Tables[0]

		// Get partition columns for this table
		partitionColumns, err := p.metadataService.GetPartitionColumns(ctx, table.DatabaseName, table.TableName)
		if err != nil {
			p.logger.Error("Failed to get partition columns",
				"database", table.DatabaseName,
				"table", table.TableName,
				"error", err)
			return
		}

		// Extract partition conditions
		if updateStmt.Where != nil && len(partitionColumns) > 0 {
			partitionConds, err := p.extractConditionsFromExpr(
				updateStmt.Where.Expr,
				table.TableName,
				table.Alias,
				partitionColumns,
			)
			if err != nil {
				p.logger.Error("Failed to extract partition conditions",
					"database", table.DatabaseName,
					"table", table.TableName,
					"error", err)
				return
			}
			info.PartitionConditions = append(info.PartitionConditions, partitionConds...)
		}

		// Get bucket columns for this table
		bucketColumns, err := p.metadataService.GetBucketColumns(ctx, table.DatabaseName, table.TableName)
		if err != nil {
			p.logger.Error("Failed to get bucket columns",
				"database", table.DatabaseName,
				"table", table.TableName,
				"error", err)
			return
		}

		// Extract bucket conditions
		if updateStmt.Where != nil && len(bucketColumns) > 0 {
			bucketConds, err := p.extractConditionsFromExpr(
				updateStmt.Where.Expr,
				table.TableName,
				table.Alias,
				bucketColumns,
			)
			if err != nil {
				p.logger.Error("Failed to extract bucket conditions",
					"database", table.DatabaseName,
					"table", table.TableName,
					"error", err)
				return
			}
			for _, cond := range bucketConds {
				info.BucketConditions = append(info.BucketConditions, BucketCondition{
					Column:   cond.Column,
					Operator: cond.Operator,
					Values:   cond.Values,
				})
			}
		}
	}
}

// extractDeleteInfo extracts information from a DELETE statement
func (p *QueryParserImpl) extractDeleteInfo(ctx context.Context, stmt sqlparser.Statement, info *QueryInfo) {
	deleteStmt, ok := stmt.(*sqlparser.Delete)
	if !ok {
		return
	}

	// Check for subqueries
	subqueryVisitor := newSubqueryVisitor()
	sqlparser.Walk(subqueryVisitor, deleteStmt)
	info.HasSubquery = subqueryVisitor.hasSubquery

	// Extract partition and bucket conditions for the table
	if len(info.Tables) > 0 {
		table := info.Tables[0]

		// Get partition columns for this table
		partitionColumns, err := p.metadataService.GetPartitionColumns(ctx, table.DatabaseName, table.TableName)
		if err != nil {
			p.logger.Error("Failed to get partition columns",
				"database", table.DatabaseName,
				"table", table.TableName,
				"error", err)
			return
		}

		// Extract partition conditions
		if deleteStmt.Where != nil && len(partitionColumns) > 0 {
			partitionConds, err := p.extractConditionsFromExpr(
				deleteStmt.Where.Expr,
				table.TableName,
				table.Alias,
				partitionColumns,
			)
			if err != nil {
				p.logger.Error("Failed to extract partition conditions",
					"database", table.DatabaseName,
					"table", table.TableName,
					"error", err)
				return
			}
			info.PartitionConditions = append(info.PartitionConditions, partitionConds...)
		}

		// Get bucket columns for this table
		bucketColumns, err := p.metadataService.GetBucketColumns(ctx, table.DatabaseName, table.TableName)
		if err != nil {
			p.logger.Error("Failed to get bucket columns",
				"database", table.DatabaseName,
				"table", table.TableName,
				"error", err)
			return
		}

		// Extract bucket conditions
		if deleteStmt.Where != nil && len(bucketColumns) > 0 {
			bucketConds, err := p.extractConditionsFromExpr(
				deleteStmt.Where.Expr,
				table.TableName,
				table.Alias,
				bucketColumns,
			)
			if err != nil {
				p.logger.Error("Failed to extract bucket conditions",
					"database", table.DatabaseName,
					"table", table.TableName,
					"error", err)
				return
			}
			for _, cond := range bucketConds {
				info.BucketConditions = append(info.BucketConditions, BucketCondition{
					Column:   cond.Column,
					Operator: cond.Operator,
					Values:   cond.Values,
				})
			}
		}
	}
}

// ExtractTables extracts tables referenced in a SQL query
func (p *QueryParserImpl) ExtractTables(ctx context.Context, sql string) ([]TableReference, error) {
	// Parse the SQL statement
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		p.logger.Error("Failed to parse SQL", "sql", sql, "error", err)
		return nil, errors.Wrap(err, "failed to parse SQL")
	}

	// Extract tables
	tableVisitor := newTableVisitor()
	sqlparser.Walk(tableVisitor, stmt)

	// Default database if tables don't have a database specified
	defaultDB, err := p.getDefaultDatabase(ctx)
	if err != nil {
		p.logger.Warn("Failed to get default database", "error", err)
		// Continue without default database
	}

	tables := make([]TableReference, 0, len(tableVisitor.tables))
	for _, table := range tableVisitor.tables {
		dbName := table.Qualifier.String()
		if dbName == "" {
			dbName = defaultDB
		}
		tables = append(tables, TableReference{
			DatabaseName: dbName,
			TableName:    table.Name.String(),
			Alias:        table.As.String(),
		})
	}

	return tables, nil
}

// ExtractPartitionConditions extracts partition conditions from a SQL query
func (p *QueryParserImpl) ExtractPartitionConditions(
	ctx context.Context,
	sql string,
	tableName string,
	partitionColumns []string,
) ([]PartitionCondition, error) {
	// Parse the SQL statement
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		p.logger.Error("Failed to parse SQL", "sql", sql, "error", err)
		return nil, errors.Wrap(err, "failed to parse SQL")
	}

	// Extract tables to get aliases
	tables, err := p.ExtractTables(ctx, sql)
	if err != nil {
		p.logger.Error("Failed to extract tables", "sql", sql, "error", err)
		return nil, err
	}

	// Find the alias for the specified table
	var tableAlias string
	for _, table := range tables {
		if table.TableName == tableName {
			tableAlias = table.Alias
			break
		}
	}

	// Extract conditions from WHERE clause
	var whereExpr sqlparser.Expr
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		if stmt.Where != nil {
			whereExpr = stmt.Where.Expr
		}
	case *sqlparser.Update:
		if stmt.Where != nil {
			whereExpr = stmt.Where.Expr
		}
	case *sqlparser.Delete:
		if stmt.Where != nil {
			whereExpr = stmt.Where.Expr
		}
	default:
		return nil, fmt.Errorf("unsupported statement type for extracting partition conditions")
	}

	if whereExpr == nil {
		return []PartitionCondition{}, nil
	}

	// Extract conditions
	conditions, err := p.extractConditionsFromExpr(whereExpr, tableName, tableAlias, partitionColumns)
	if err != nil {
		p.logger.Error("Failed to extract conditions from expression",
			"sql", sql,
			"table", tableName,
			"error", err)
		return nil, err
	}

	return conditions, nil
}

// ExtractBucketConditions extracts bucket conditions from a SQL query
func (p *QueryParserImpl) ExtractBucketConditions(
	ctx context.Context,
	sql string,
	tableName string,
	bucketColumns []string,
) ([]BucketCondition, error) {
	// Parse the SQL statement
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		p.logger.Error("Failed to parse SQL", "sql", sql, "error", err)
		return nil, errors.Wrap(err, "failed to parse SQL")
	}

	// Extract tables to get aliases
	tables, err := p.ExtractTables(ctx, sql)
	if err != nil {
		p.logger.Error("Failed to extract tables", "sql", sql, "error", err)
		return nil, err
	}

	// Find the alias for the specified table
	var tableAlias string
	for _, table := range tables {
		if table.TableName == tableName {
			tableAlias = table.Alias
			break
		}
	}

	// Extract conditions from WHERE clause
	var whereExpr sqlparser.Expr
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		if stmt.Where != nil {
			whereExpr = stmt.Where.Expr
		}
	case *sqlparser.Update:
		if stmt.Where != nil {
			whereExpr = stmt.Where.Expr
		}
	case *sqlparser.Delete:
		if stmt.Where != nil {
			whereExpr = stmt.Where.Expr
		}
	default:
		return nil, fmt.Errorf("unsupported statement type for extracting bucket conditions")
	}

	if whereExpr == nil {
		return []BucketCondition{}, nil
	}

	// Extract conditions
	partitionConditions, err := p.extractConditionsFromExpr(whereExpr, tableName, tableAlias, bucketColumns)
	if err != nil {
		p.logger.Error("Failed to extract conditions from expression",
			"sql", sql,
			"table", tableName,
			"error", err)
		return nil, err
	}

	// Convert partition conditions to bucket conditions
	bucketConditions := make([]BucketCondition, len(partitionConditions))
	for i, cond := range partitionConditions {
		bucketConditions[i] = BucketCondition{
			Column:   cond.Column,
			Operator: cond.Operator,
			Values:   cond.Values,
		}
	}

	return bucketConditions, nil
}

// extractConditionsFromExpr extracts conditions from a WHERE expression
func (p *QueryParserImpl) extractConditionsFromExpr(
	expr sqlparser.Expr,
	tableName string,
	tableAlias string,
	targetColumns []string,
) ([]PartitionCondition, error) {
	conditions := make([]PartitionCondition, 0)

	// Create a map of target columns for faster lookup
	targetColumnsMap := make(map[string]bool)
	for _, col := range targetColumns {
		targetColumnsMap[strings.ToLower(col)] = true
	}

	// Helper function to check if a column belongs to the target table
	isTargetColumn := func(colName string, tableRef string) bool {
		// If there's a table reference, check if it matches the table name or alias
		if tableRef != "" {
			if tableRef == tableName || (tableAlias != "" && tableRef == tableAlias) {
				return targetColumnsMap[strings.ToLower(colName)]
			}
			return false
		}
		// If there's no table reference, assume it's from the target table
		return targetColumnsMap[strings.ToLower(colName)]
	}

	// Recursively extract conditions
	var extractConditions func(expr sqlparser.Expr) error
	extractConditions = func(expr sqlparser.Expr) error {
		switch expr := expr.(type) {
		case *sqlparser.ComparisonExpr:
			// Handle comparison expressions (=, <, >, <=, >=, !=, etc.)
			col, ok := expr.Left.(*sqlparser.ColName)
			if !ok {
				// Check if the right side is a column and left is a value
				col, ok = expr.Right.(*sqlparser.ColName)
				if !ok {
					return nil
				}
				// Swap operators for reverse comparisons (value < col becomes col > value)
				op := expr.Operator
				switch op {
				case "<":
					op = ">"
				case ">":
					op = "<"
				case "<=":
					op = ">="
				case ">=":
					op = "<="
				}
				return p.extractComparisonCondition(col, expr.Left, op, isTargetColumn, &conditions)
			}
			return p.extractComparisonCondition(col, expr.Right, expr.Operator, isTargetColumn, &conditions)

		case *sqlparser.RangeCond:
			// Handle BETWEEN expressions
			col, ok := expr.Left.(*sqlparser.ColName)
			if !ok {
				return nil
			}
			if !isTargetColumn(col.Name.String(), col.Qualifier.String()) {
				return nil
			}

			// Extract values from the range
			from, err := p.extractValue(expr.From)
			if err != nil {
				return err
			}
			to, err := p.extractValue(expr.To)
			if err != nil {
				return err
			}

			operator := "BETWEEN"
			if expr.Operator == "not between" {
				operator = "NOT BETWEEN"
			}

			conditions = append(conditions, PartitionCondition{
				Column:   col.Name.String(),
				Operator: operator,
				Values:   []interface{}{from, to},
			})

		case *sqlparser.IsExpr:
			// Handle IS NULL and IS NOT NULL
			col, ok := expr.Expr.(*sqlparser.ColName)
			if !ok {
				return nil
			}
			if !isTargetColumn(col.Name.String(), col.Qualifier.String()) {
				return nil
			}

			operator := "IS"
			if expr.Operator == "is not" {
				operator = "IS NOT"
			}

			conditions = append(conditions, PartitionCondition{
				Column:   col.Name.String(),
				Operator: operator,
				Values:   []interface{}{nil},
			})

		case *sqlparser.AndExpr:
			// Handle AND expressions by recursively extracting from both sides
			if err := extractConditions(expr.Left); err != nil {
				return err
			}
			if err := extractConditions(expr.Right); err != nil {
				return err
			}

		case *sqlparser.OrExpr:
			// Handle OR expressions by recursively extracting from both sides
			if err := extractConditions(expr.Left); err != nil {
				return err
			}
			if err := extractConditions(expr.Right); err != nil {
				return err
			}

		case *sqlparser.ParenExpr:
			// Handle parenthesized expressions
			if err := extractConditions(expr.Expr); err != nil {
				return err
			}

		case *sqlparser.NotExpr:
			// Handle NOT expressions
			if err := extractConditions(expr.Expr); err != nil {
				return err
			}

		case *sqlparser.Subquery:
			// We don't extract conditions from subqueries at this point
			return nil
		}

		return nil
	}

	if err := extractConditions(expr); err != nil {
		return nil, err
	}

	return conditions, nil
}

// extractComparisonCondition extracts a condition from a comparison expression
func (p *QueryParserImpl) extractComparisonCondition(
	col *sqlparser.ColName,
	valueExpr sqlparser.Expr,
	operator string,
	isTargetColumn func(string, string) bool,
	conditions *[]PartitionCondition,
) error {
	if !isTargetColumn(col.Name.String(), col.Qualifier.String()) {
		return nil
	}

	switch valueExpr := valueExpr.(type) {
	case *sqlparser.SQLVal, *sqlparser.Subquery:
		// Handle simple value comparison
		value, err := p.extractValue(valueExpr)
		if err != nil {
			return err
		}

		*conditions = append(*conditions, PartitionCondition{
			Column:   col.Name.String(),
			Operator: operator,
			Values:   []interface{}{value},
		})

	case sqlparser.ValTuple:
		// Handle IN expressions
		if operator != "in" && operator != "not in" {
			return nil
		}

		values := make([]interface{}, 0, len(valueExpr))
		for _, val := range valueExpr {
			value, err := p.extractValue(val)
			if err != nil {
				return err
			}
			values = append(values, value)
		}

		op := "IN"
		if operator == "not in" {
			op = "NOT IN"
		}

		*conditions = append(*conditions, PartitionCondition{
			Column:   col.Name.String(),
			Operator: op,
			Values:   values,
		})

	case *sqlparser.FuncExpr:
		// Handle function calls
		// For now, we don't extract conditions from function calls
		return nil
	}

	return nil
}

// extractValue extracts a value from a SQL expression
func (p *QueryParserImpl) extractValue(expr sqlparser.Expr) (interface{}, error) {
	switch expr := expr.(type) {
	case *sqlparser.SQLVal:
		switch expr.Type {
		case sqlparser.StrVal:
			return string(expr.Val), nil
		case sqlparser.IntVal:
			val, err := strconv.ParseInt(string(expr.Val), 10, 64)
			if err != nil {
				return nil, err
			}
			return val, nil
		case sqlparser.FloatVal:
			val, err := strconv.ParseFloat(string(expr.Val), 64)
			if err != nil {
				return nil, err
			}
			return val, nil
		case sqlparser.HexVal:
			return "0x" + string(expr.Val), nil
		case sqlparser.BitVal:
			return "b'" + string(expr.Val) + "'", nil
		default:
			return string(expr.Val), nil
		}
	case sqlparser.ValTuple:
		values := make([]interface{}, 0, len(expr))
		for _, val := range expr {
			v, err := p.extractValue(val)
			if err != nil {
				return nil, err
			}
			values = append(values, v)
		}
		return values, nil
	case *sqlparser.Subquery:
		// For subqueries, we just return a placeholder since we can't evaluate it here
		return "[SUBQUERY]", nil
	case *sqlparser.FuncExpr:
		return fmt.Sprintf("[FUNCTION:%s]", expr.Name.String()), nil
	case sqlparser.BoolVal:
		return bool(expr), nil
	case *sqlparser.NullVal:
		return nil, nil
	default:
		return "[UNKNOWN]", nil
	}
}

// ValidateQuery validates a SQL query against StarRocks syntax
func (p *QueryParserImpl) ValidateQuery(ctx context.Context, sql string) (bool, []string, error) {
	errors := []string{}

	// Basic validation - try to parse the SQL
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		errors = append(errors, fmt.Sprintf("SQL syntax error: %v", err))
		return false, errors, nil
	}

	// Additional StarRocks-specific validation
	if p.config.EnableStrictValidation {
		// Perform StarRocks-specific validation
		// This is a placeholder for more specific validation logic
		if err := p.validateStarRocksSyntax(stmt); err != nil {
			errors = append(errors, err.Error())
		}
	}

	return len(errors) == 0, errors, nil
}

// validateStarRocksSyntax performs StarRocks-specific syntax validation
func (p *QueryParserImpl) validateStarRocksSyntax(stmt sqlparser.Statement) error {
	// This is a placeholder for more specific validation logic
	// In a real implementation, this would check StarRocks-specific rules

	// Example validation: check for unsupported features
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		// Check for unsupported JOIN types
		if err := p.validateJoins(stmt); err != nil {
			return err
		}

		// Check for other unsupported features
		if err := p.validateSelectFeatures(stmt); err != nil {
			return err
		}
	}

	return nil
}

// validateJoins validates JOIN clauses in a SELECT statement
func (p *QueryParserImpl) validateJoins(stmt *sqlparser.Select) error {
	// Placeholder for JOIN validation
	// In a real implementation, this would check for StarRocks-specific JOIN limitations
	return nil
}

// validateSelectFeatures validates features in a SELECT statement
func (p *QueryParserImpl) validateSelectFeatures(stmt *sqlparser.Select) error {
	// Placeholder for SELECT feature validation
	// In a real implementation, this would check for StarRocks-specific SELECT limitations
	return nil
}

// GetQueryType gets the type of a SQL query
func (p *QueryParserImpl) GetQueryType(ctx context.Context, sql string) (QueryType, error) {
	// Parse the SQL statement
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		p.logger.Error("Failed to parse SQL", "sql", sql, "error", err)
		return QueryTypeOther, errors.Wrap(err, "failed to parse SQL")
	}

	// Determine the query type
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		return QueryTypeSelect, nil
	case *sqlparser.Insert:
		return QueryTypeInsert, nil
	case *sqlparser.Update:
		return QueryTypeUpdate, nil
	case *sqlparser.Delete:
		return QueryTypeDelete, nil
	case *sqlparser.DDL:
		switch stmt.Action {
		case "create":
			return QueryTypeCreate, nil
		case "alter":
			return QueryTypeAlter, nil
		case "drop":
			return QueryTypeDrop, nil
		}
	case *sqlparser.Show:
		return QueryTypeShow, nil
	case *sqlparser.Explain:
		return QueryTypeExplain, nil
	case *sqlparser.OtherRead:
		if strings.HasPrefix(strings.ToUpper(sql), "DESCRIBE") ||
			strings.HasPrefix(strings.ToUpper(sql), "DESC") {
			return QueryTypeDescribe, nil
		}
	case *sqlparser.Use:
		return QueryTypeUse, nil
	}

	return QueryTypeOther, nil
}

// ClearCache clears the parser cache
func (p *QueryParserImpl) ClearCache() {
	p.cacheMutex.Lock()
	defer p.cacheMutex.Unlock()

	p.queryCache = make(map[string]cachedQueryInfo)
	p.lastCacheCleanup = time.Now()
	p.metrics.CounterInc("query_parser_cache_clear", nil)
}

// getCachedQueryInfo tries to get cached query info
func (p *QueryParserImpl) getCachedQueryInfo(sql string) (QueryInfo, bool) {
	p.cacheMutex.RLock()
	defer p.cacheMutex.RUnlock()

	// Check if we need to clean up the cache
	if time.Since(p.lastCacheCleanup) > 10*time.Minute {
		go p.cleanupCache()
	}

	// Check if the query is in the cache
	if entry, found := p.queryCache[sql]; found {
		// Check if the cache entry is still valid
		if time.Since(entry.LastAccessed) <= p.config.CacheTTL {
			// Update last accessed time
			entry.LastAccessed = time.Now()
			p.queryCache[sql] = entry
			return entry.Info, true
		}
	}

	return QueryInfo{}, false
}

// cacheQueryInfo caches query info
func (p *QueryParserImpl) cacheQueryInfo(sql string, info QueryInfo) {
	p.cacheMutex.Lock()
	defer p.cacheMutex.Unlock()

	// Check if the cache is full
	if len(p.queryCache) >= p.config.CacheMaxSize {
		// If full, remove the oldest entries
		p.evictOldestEntries(p.config.CacheMaxSize / 10) // Remove 10% of entries
	}

	// Add the new entry
	p.queryCache[sql] = cachedQueryInfo{
		Info:         info,
		LastAccessed: time.Now(),
	}
}

// evictOldestEntries evicts the oldest entries from the cache
func (p *QueryParserImpl) evictOldestEntries(count int) {
	if count <= 0 || len(p.queryCache) == 0 {
		return
	}

	// Collect all entries with their last accessed time
	entries := make([]struct {
		sql          string
		lastAccessed time.Time
	}, 0, len(p.queryCache))

	for sql, info := range p.queryCache {
		entries = append(entries, struct {
			sql          string
			lastAccessed time.Time
		}{
			sql:          sql,
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
		delete(p.queryCache, entries[i].sql)
	}

	p.metrics.CounterAdd("query_parser_cache_entries_evicted", float64(numToRemove), nil)
}

// cleanupCache cleans up expired entries from the cache
func (p *QueryParserImpl) cleanupCache() {
	p.cacheMutex.Lock()
	defer p.cacheMutex.Unlock()

	now := time.Now()
	p.lastCacheCleanup = now
	expiredCount := 0

	// Remove expired entries
	for sql, info := range p.queryCache {
		if now.Sub(info.LastAccessed) > p.config.CacheTTL {
			delete(p.queryCache, sql)
			expiredCount++
		}
	}

	p.metrics.CounterAdd("query_parser_cache_entries_expired", float64(expiredCount), nil)
}

// getDefaultDatabase gets the default database from the metadata service
func (p *QueryParserImpl) getDefaultDatabase(ctx context.Context) (string, error) {
	defaultDB, err := p.metadataService.GetDefaultDatabase(ctx)
	if err != nil {
		return "", err
	}
	return defaultDB, nil
}

// Helper types for SQL parsing visitors

// aggregationVisitor checks if a SELECT statement has aggregation functions
type aggregationVisitor struct {
	hasAggregation bool
}

// newAggregationVisitor creates a new aggregation visitor
func newAggregationVisitor() *aggregationVisitor {
	return &aggregationVisitor{
		hasAggregation: false,
	}
}

// Visit implements the sqlparser.Visitor interface
func (v *aggregationVisitor) Visit(node sqlparser.SQLNode) (kontinue bool, err error) {
	switch node := node.(type) {
	case *sqlparser.FuncExpr:
		funcName := strings.ToUpper(node.Name.String())
		// Check for common aggregation functions
		if funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" ||
			funcName == "MIN" || funcName == "MAX" || funcName == "GROUP_CONCAT" {
			v.hasAggregation = true
			return false, nil
		}
	}
	return true, nil
}

// joinVisitor checks if a FROM clause has JOINs and collects the join tables
type joinVisitor struct {
	hasJoin    bool
	joinTables []TableReference
}

// newJoinVisitor creates a new join visitor
func newJoinVisitor() *joinVisitor {
	return &joinVisitor{
		hasJoin:    false,
		joinTables: make([]TableReference, 0),
	}
}

// Visit implements the sqlparser.Visitor interface
func (v *joinVisitor) Visit(node sqlparser.SQLNode) (kontinue bool, err error) {
	switch node := node.(type) {
	case *sqlparser.JoinTableExpr:
		v.hasJoin = true

		// Get the right table
		rightTable, ok := node.RightExpr.(*sqlparser.AliasedTableExpr)
		if ok {
			tableName, ok := rightTable.Expr.(sqlparser.TableName)
			if ok {
				v.joinTables = append(v.joinTables, TableReference{
					DatabaseName: tableName.Qualifier.String(),
					TableName:    tableName.Name.String(),
					Alias:        rightTable.As.String(),
				})
			}
		}
	}
	return true, nil
}

// subqueryVisitor checks if a statement has subqueries
type subqueryVisitor struct {
	hasSubquery bool
}

// newSubqueryVisitor creates a new subquery visitor
func newSubqueryVisitor() *subqueryVisitor {
	return &subqueryVisitor{
		hasSubquery: false,
	}
}

// Visit implements the sqlparser.Visitor interface
func (v *subqueryVisitor) Visit(node sqlparser.SQLNode) (kontinue bool, err error) {
	if _, ok := node.(*sqlparser.Subquery); ok {
		v.hasSubquery = true
		return false, nil
	}
	return true, nil
}

// tableVisitor collects all tables referenced in a statement
type tableVisitor struct {
	tables []sqlparser.TableName
}

// newTableVisitor creates a new table visitor
func newTableVisitor() *tableVisitor {
	return &tableVisitor{
		tables: make([]sqlparser.TableName, 0),
	}
}

// Visit implements the sqlparser.Visitor interface
func (v *tableVisitor) Visit(node sqlparser.SQLNode) (kontinue bool, err error) {
	switch node := node.(type) {
	case *sqlparser.AliasedTableExpr:
		if tableName, ok := node.Expr.(sqlparser.TableName); ok {
			v.tables = append(v.tables, tableName)
		}
	}
	return true, nil
}

// Personal.AI order the ending
