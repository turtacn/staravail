// Package health provides components for health checking and monitoring.
package health

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/turtacn/staravail/internal/common/config"
	"github.com/turtacn/staravail/internal/common/errors"
	"github.com/turtacn/staravail/internal/common/logging"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/domain/tablet"
)

// HealthStatus represents the health status of a component
type HealthStatus string

const (
	// HealthStatusUnknown indicates the health status is unknown
	HealthStatusUnknown HealthStatus = "UNKNOWN"
	// HealthStatusHealthy indicates the component is healthy
	HealthStatusHealthy HealthStatus = "HEALTHY"
	// HealthStatusDegraded indicates the component is degraded but still functional
	HealthStatusDegraded HealthStatus = "DEGRADED"
	// HealthStatusUnhealthy indicates the component is unhealthy
	HealthStatusUnhealthy HealthStatus = "UNHEALTHY"
)

// AccessLevel represents the access level for a component
type AccessLevel string

const (
	// AccessLevelFull indicates full access (read and write)
	AccessLevelFull AccessLevel = "FULL"
	// AccessLevelReadOnly indicates read-only access
	AccessLevelReadOnly AccessLevel = "READ_ONLY"
	// AccessLevelPartial indicates partial access (some operations may fail)
	AccessLevelPartial AccessLevel = "PARTIAL"
	// AccessLevelNone indicates no access
	AccessLevelNone AccessLevel = "NONE"
)

// OperationType represents the type of operation
type OperationType string

const (
	// OperationTypeRead represents a read operation
	OperationTypeRead OperationType = "READ"
	// OperationTypeWrite represents a write operation
	OperationTypeWrite OperationType = "WRITE"
	// OperationTypeDelete represents a delete operation
	OperationTypeDelete OperationType = "DELETE"
	// OperationTypeAlter represents an alter operation
	OperationTypeAlter OperationType = "ALTER"
)

// PartitionAccessibility represents the accessibility of a partition
type PartitionAccessibility struct {
	// PartitionID is the ID of the partition
	PartitionID int64
	// PartitionName is the name of the partition
	PartitionName string
	// Status is the health status of the partition
	Status HealthStatus
	// AccessLevel is the access level of the partition
	AccessLevel AccessLevel
	// ReadOperationSupported indicates if read operations are supported
	ReadOperationSupported bool
	// WriteOperationSupported indicates if write operations are supported
	WriteOperationSupported bool
	// HealthyTabletCount is the number of healthy tablets in the partition
	HealthyTabletCount int
	// TotalTabletCount is the total number of tablets in the partition
	TotalTabletCount int
	// HealthyReplicas is the number of healthy replicas
	HealthyReplicas int
	// TotalReplicas is the total number of replicas
	TotalReplicas int
	// LastUpdated is when this information was last updated
	LastUpdated time.Time
	// Issues contains any issues with the partition
	Issues []string
}

// TableAccessibility represents the accessibility of a table
type TableAccessibility struct {
	// DatabaseID is the ID of the database
	DatabaseID int64
	// DatabaseName is the name of the database
	DatabaseName string
	// TableID is the ID of the table
	TableID int64
	// TableName is the name of the table
	TableName string
	// Status is the health status of the table
	Status HealthStatus
	// AccessLevel is the access level of the table
	AccessLevel AccessLevel
	// ReadOperationSupported indicates if read operations are supported
	ReadOperationSupported bool
	// WriteOperationSupported indicates if write operations are supported
	WriteOperationSupported bool
	// PartitionAccessibility contains the accessibility of each partition
	PartitionAccessibility map[int64]PartitionAccessibility
	// HealthyPartitions is the number of healthy partitions
	HealthyPartitions int
	// TotalPartitions is the total number of partitions
	TotalPartitions int
	// LastUpdated is when this information was last updated
	LastUpdated time.Time
	// Issues contains any issues with the table
	Issues []string
}

// BackendHealthDetail represents detailed health information for a backend
type BackendHealthDetail struct {
	// BackendID is the ID of the backend
	BackendID int64
	// Host is the hostname of the backend
	Host string
	// Status is the health status of the backend
	Status HealthStatus
	// IsAlive indicates if the backend is alive
	IsAlive bool
	// LastHeartbeat is when the last heartbeat was received
	LastHeartbeat time.Time
	// DiskUsagePercent is the disk usage percentage
	DiskUsagePercent float64
	// MemoryUsagePercent is the memory usage percentage
	MemoryUsagePercent float64
	// CPUUsagePercent is the CPU usage percentage
	CPUUsagePercent float64
	// TabletCount is the number of tablets on this backend
	TabletCount int
	// LastUpdated is when this information was last updated
	LastUpdated time.Time
	// Issues contains any issues with the backend
	Issues []string
}

// QueryExecutability represents the executability of a query
type QueryExecutability struct {
	// QueryID is a unique identifier for the query
	QueryID string
	// QueryType is the type of the query (SELECT, INSERT, etc.)
	QueryType string
	// Tables is the list of tables involved in the query
	Tables []TableAccessibility
	// IsExecutable indicates if the query is executable
	IsExecutable bool
	// ExecutionLevel indicates the level of execution possible
	ExecutionLevel AccessLevel
	// EstimatedImpact is the estimated impact of any degraded performance
	EstimatedImpact float64
	// LastUpdated is when this information was last updated
	LastUpdated time.Time
	// Issues contains any issues that may affect query execution
	Issues []string
}

// WriteExecutability represents the executability of a write operation
type WriteExecutability struct {
	// OperationID is a unique identifier for the write operation
	OperationID string
	// OperationType is the type of write operation
	OperationType OperationType
	// Tables is the list of tables involved in the write operation
	Tables []TableAccessibility
	// IsExecutable indicates if the write operation is executable
	IsExecutable bool
	// ExecutionLevel indicates the level of execution possible
	ExecutionLevel AccessLevel
	// EstimatedImpact is the estimated impact of any degraded performance
	EstimatedImpact float64
	// LastUpdated is when this information was last updated
	LastUpdated time.Time
	// Issues contains any issues that may affect write execution
	Issues []string
}

// HealthCheckReport represents a comprehensive health check report
type HealthCheckReport struct {
	// OverallStatus is the overall health status
	OverallStatus HealthStatus
	// Backends contains the health details of all backends
	Backends []BackendHealthDetail
	// Tables contains the accessibility of all tables
	Tables []TableAccessibility
	// UnhealthyComponents contains all unhealthy components
	UnhealthyComponents []string
	// IssuesSummary contains a summary of all issues
	IssuesSummary []string
	// GeneratedAt is when this report was generated
	GeneratedAt time.Time
	// GeneratedBy is who generated this report
	GeneratedBy string
}

// HealthCheckerConfig represents configuration for the health checker
type HealthCheckerConfig struct {
	// CacheEnabled indicates if caching is enabled
	CacheEnabled bool
	// CacheTTL is the cache time-to-live
	CacheTTL time.Duration
	// TabletHealthThreshold is the percentage of healthy tablets required for a partition to be healthy
	TabletHealthThreshold float64
	// ReplicaHealthThreshold is the percentage of healthy replicas required for a tablet to be healthy
	ReplicaHealthThreshold float64
	// BackendHealthThreshold is the percentage of healthy backends required for the system to be healthy
	BackendHealthThreshold float64
	// CheckTimeout is the timeout for health checks
	CheckTimeout time.Duration
}

// HealthChecker defines the interface for health checking
type HealthChecker interface {
	// CheckBackendHealth checks the health of a specific backend
	CheckBackendHealth(ctx context.Context, backendID int64) (BackendHealthDetail, error)

	// CheckAllBackendsHealth checks the health of all backends
	CheckAllBackendsHealth(ctx context.Context) ([]BackendHealthDetail, error)

	// CheckTabletHealth checks the health of a specific tablet
	CheckTabletHealth(ctx context.Context, tabletID int64) (HealthStatus, []string, error)

	// CheckPartitionHealth checks the health of a specific partition
	CheckPartitionHealth(ctx context.Context, databaseID, tableID, partitionID int64) (PartitionAccessibility, error)

	// CheckTableHealth checks the health of a specific table
	CheckTableHealth(ctx context.Context, databaseID, tableID int64) (TableAccessibility, error)

	// CheckQueryExecutability checks if a query can be executed
	CheckQueryExecutability(ctx context.Context, query string, tables []int64) (QueryExecutability, error)

	// CheckWriteExecutability checks if a write operation can be executed
	CheckWriteExecutability(ctx context.Context, operation OperationType, tables []int64) (WriteExecutability, error)

	// GenerateHealthReport generates a comprehensive health report
	GenerateHealthReport(ctx context.Context) (HealthCheckReport, error)

	// ClearCache clears the health check cache
	ClearCache()
}

// HealthCheckerImpl implements the HealthChecker interface
type HealthCheckerImpl struct {
	// config is the health checker configuration
	config HealthCheckerConfig
	// tabletManager is used to get tablet information
	tabletManager tablet.TabletManager
	// monitor is used to get health monitoring information
	monitor HealthMonitor
	// logger is used for logging
	logger logging.Logger
	// metrics is used to record metrics
	metrics metrics.MetricsRecorder

	// backendHealthCache caches backend health details
	backendHealthCache map[int64]BackendHealthDetail
	// tabletHealthCache caches tablet health status
	tabletHealthCache map[int64]cachedTabletHealth
	// partitionHealthCache caches partition accessibility
	partitionHealthCache map[int64]PartitionAccessibility
	// tableHealthCache caches table accessibility
	tableHealthCache map[int64]TableAccessibility

	// cacheMutex protects the cache maps
	cacheMutex sync.RWMutex
	// lastCacheCleanup is when the cache was last cleaned up
	lastCacheCleanup time.Time
}

// cachedTabletHealth represents cached tablet health information
type cachedTabletHealth struct {
	// Status is the health status of the tablet
	Status HealthStatus
	// Issues contains any issues with the tablet
	Issues []string
	// LastUpdated is when this information was last updated
	LastUpdated time.Time
}

// NewHealthChecker creates a new HealthChecker instance
func NewHealthChecker(
	cfg config.HealthCheckerConfig,
	tabletManager tablet.TabletManager,
	monitor HealthMonitor,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) HealthChecker {
	// Convert config to internal checker config
	checkerConfig := HealthCheckerConfig{
		CacheEnabled:           cfg.CacheEnabled,
		CacheTTL:               cfg.CacheTTL,
		TabletHealthThreshold:  cfg.TabletHealthThreshold,
		ReplicaHealthThreshold: cfg.ReplicaHealthThreshold,
		BackendHealthThreshold: cfg.BackendHealthThreshold,
		CheckTimeout:           cfg.CheckTimeout,
	}

	return &HealthCheckerImpl{
		config:               checkerConfig,
		tabletManager:        tabletManager,
		monitor:              monitor,
		logger:               logger,
		metrics:              metricsRecorder,
		backendHealthCache:   make(map[int64]BackendHealthDetail),
		tabletHealthCache:    make(map[int64]cachedTabletHealth),
		partitionHealthCache: make(map[int64]PartitionAccessibility),
		tableHealthCache:     make(map[int64]TableAccessibility),
		lastCacheCleanup:     time.Now(),
	}
}

// CheckBackendHealth checks the health of a specific backend
func (hc *HealthCheckerImpl) CheckBackendHealth(ctx context.Context, backendID int64) (BackendHealthDetail, error) {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(ctx, hc.config.CheckTimeout)
	defer cancel()

	// Check if cached result is available
	if hc.config.CacheEnabled {
		if cachedResult, found := hc.getCachedBackendHealth(backendID); found {
			return cachedResult, nil
		}
	}

	// Get backend health from the monitor
	backendHealth, err := hc.monitor.GetBackendHealth(backendID)
	if err != nil {
		hc.logger.Error("Failed to get backend health", "backendID", backendID, "error", err)
		return BackendHealthDetail{}, err
	}

	// Analyze backend health
	detail := hc.analyzeBackendHealth(backendHealth)

	// Cache the result
	if hc.config.CacheEnabled {
		hc.cacheBackendHealth(backendID, detail)
	}

	// Record metrics
	hc.recordBackendHealthMetrics(detail)

	return detail, nil
}

// analyzeBackendHealth analyzes the backend health
func (hc *HealthCheckerImpl) analyzeBackendHealth(backendHealth BackendHealth) BackendHealthDetail {
	issues := make([]string, 0)
	status := HealthStatusHealthy

	// Initialize the detail
	detail := BackendHealthDetail{
		BackendID:        backendHealth.BackendID,
		Host:             backendHealth.Host,
		IsAlive:          backendHealth.Alive,
		LastHeartbeat:    backendHealth.LastHeartbeat,
		DiskUsagePercent: backendHealth.UsedPct,
		TabletCount:      backendHealth.TabletCount,
		LastUpdated:      time.Now(),
	}

	// Check if the backend is alive
	if !backendHealth.Alive {
		status = HealthStatusUnhealthy
		issues = append(issues, "Backend is not alive")
	}

	// Check if the backend is decommissioned
	if backendHealth.SystemDecommissioned || backendHealth.ClusterDecommissioned {
		status = HealthStatusUnhealthy
		issues = append(issues, "Backend is decommissioned")
	}

	// Check disk usage
	if backendHealth.UsedPct > 90 {
		status = HealthStatusDegraded
		issues = append(issues, fmt.Sprintf("Disk usage is high: %.2f%%", backendHealth.UsedPct))
	}

	// Check last heartbeat
	if time.Since(backendHealth.LastHeartbeat) > 5*time.Minute {
		status = HealthStatusDegraded
		issues = append(issues, fmt.Sprintf("Last heartbeat was %v ago", time.Since(backendHealth.LastHeartbeat)))
	}

	// If there's an error message, add it to the issues
	if backendHealth.ErrMsg != "" {
		issues = append(issues, fmt.Sprintf("Backend error: %s", backendHealth.ErrMsg))
	}

	detail.Status = status
	detail.Issues = issues

	return detail
}

// CheckAllBackendsHealth checks the health of all backends
func (hc *HealthCheckerImpl) CheckAllBackendsHealth(ctx context.Context) ([]BackendHealthDetail, error) {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(ctx, hc.config.CheckTimeout)
	defer cancel()

	// Get all backend health from the monitor
	backends, err := hc.monitor.GetAllBackendHealth()
	if err != nil {
		hc.logger.Error("Failed to get all backend health", "error", err)
		return nil, err
	}

	// Analyze each backend's health
	results := make([]BackendHealthDetail, 0, len(backends))
	for _, be := range backends {
		detail := hc.analyzeBackendHealth(be)
		results = append(results, detail)

		// Cache the result
		if hc.config.CacheEnabled {
			hc.cacheBackendHealth(be.BackendID, detail)
		}

		// Record metrics
		hc.recordBackendHealthMetrics(detail)
	}

	return results, nil
}

// CheckTabletHealth checks the health of a specific tablet
func (hc *HealthCheckerImpl) CheckTabletHealth(ctx context.Context, tabletID int64) (HealthStatus, []string, error) {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(ctx, hc.config.CheckTimeout)
	defer cancel()

	// Check if cached result is available
	if hc.config.CacheEnabled {
		if cachedResult, found := hc.getCachedTabletHealth(tabletID); found {
			return cachedResult.Status, cachedResult.Issues, nil
		}
	}

	// Get tablet information from the tablet manager
	tabletInfo, err := hc.tabletManager.GetTabletInfo(ctx, tabletID)
	if err != nil {
		hc.logger.Error("Failed to get tablet info", "tabletID", tabletID, "error", err)
		return HealthStatusUnknown, []string{fmt.Sprintf("Failed to get tablet info: %v", err)}, err
	}

	// Get tablet replicas
	replicas, err := hc.tabletManager.GetTabletReplicas(ctx, tabletID)
	if err != nil {
		hc.logger.Error("Failed to get tablet replicas", "tabletID", tabletID, "error", err)
		return HealthStatusUnknown, []string{fmt.Sprintf("Failed to get tablet replicas: %v", err)}, err
	}

	// Analyze tablet health
	status, issues := hc.analyzeTabletHealth(tabletInfo, replicas)

	// Cache the result
	if hc.config.CacheEnabled {
		hc.cacheTabletHealth(tabletID, status, issues)
	}

	// Record metrics
	hc.metrics.GaugeInc("health_check_tablet_status", map[string]string{
		"tablet_id": fmt.Sprintf("%d", tabletID),
		"status":    string(status),
	})

	return status, issues, nil
}

// analyzeTabletHealth analyzes the health of a tablet
func (hc *HealthCheckerImpl) analyzeTabletHealth(tabletInfo tablet.TabletInfo, replicas []tablet.TabletReplica) (HealthStatus, []string) {
	issues := make([]string, 0)
	status := HealthStatusHealthy

	// Check the number of replicas
	if len(replicas) == 0 {
		status = HealthStatusUnhealthy
		issues = append(issues, "No replicas found for tablet")
		return status, issues
	}

	// Count healthy replicas
	healthyReplicaCount := 0
	for _, replica := range replicas {
		if replica.Status == "NORMAL" || replica.Status == "HEALTHY" {
			healthyReplicaCount++
		} else {
			issues = append(issues, fmt.Sprintf("Replica %d on backend %d is not healthy: %s",
				replica.ReplicaID, replica.BackendID, replica.Status))
		}
	}

	// Check if enough replicas are healthy
	healthyPercentage := float64(healthyReplicaCount) / float64(len(replicas)) * 100
	if healthyPercentage < hc.config.ReplicaHealthThreshold {
		if healthyReplicaCount == 0 {
			status = HealthStatusUnhealthy
			issues = append(issues, "No healthy replicas found")
		} else {
			status = HealthStatusDegraded
			issues = append(issues, fmt.Sprintf("Only %.2f%% of replicas are healthy (threshold: %.2f%%)",
				healthyPercentage, hc.config.ReplicaHealthThreshold))
		}
	}

	// Check tablet state
	if tabletInfo.State != "NORMAL" && tabletInfo.State != "HEALTHY" {
		if status != HealthStatusUnhealthy {
			status = HealthStatusDegraded
		}
		issues = append(issues, fmt.Sprintf("Tablet state is not normal: %s", tabletInfo.State))
	}

	return status, issues
}

// CheckPartitionHealth checks the health of a specific partition
func (hc *HealthCheckerImpl) CheckPartitionHealth(ctx context.Context, databaseID, tableID, partitionID int64) (PartitionAccessibility, error) {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(ctx, hc.config.CheckTimeout)
	defer cancel()

	// Check if cached result is available
	if hc.config.CacheEnabled {
		if cachedResult, found := hc.getCachedPartitionHealth(partitionID); found {
			return cachedResult, nil
		}
	}

	// Get partition information from the tablet manager
	partitionInfo, err := hc.tabletManager.GetPartitionInfo(ctx, databaseID, tableID, partitionID)
	if err != nil {
		hc.logger.Error("Failed to get partition info",
			"databaseID", databaseID,
			"tableID", tableID,
			"partitionID", partitionID,
			"error", err)
		return PartitionAccessibility{}, err
	}

	// Get tablets for this partition
	tablets, err := hc.tabletManager.GetTabletsForPartition(ctx, databaseID, tableID, partitionID)
	if err != nil {
		hc.logger.Error("Failed to get tablets for partition",
			"databaseID", databaseID,
			"tableID", tableID,
			"partitionID", partitionID,
			"error", err)
		return PartitionAccessibility{}, err
	}

	// Check health of each tablet
	totalTablets := len(tablets)
	healthyTablets := 0
	totalReplicas := 0
	healthyReplicas := 0
	tabletIssues := make(map[int64][]string)

	for _, tablet := range tablets {
		status, issues, err := hc.CheckTabletHealth(ctx, tablet.TabletID)
		if err != nil {
			hc.logger.Error("Failed to check tablet health",
				"tabletID", tablet.TabletID,
				"error", err)
			continue
		}

		// Get replica count for this tablet
		replicas, err := hc.tabletManager.GetTabletReplicas(ctx, tablet.TabletID)
		if err != nil {
			hc.logger.Error("Failed to get tablet replicas",
				"tabletID", tablet.TabletID,
				"error", err)
			continue
		}

		totalReplicas += len(replicas)
		for _, replica := range replicas {
			if replica.Status == "NORMAL" || replica.Status == "HEALTHY" {
				healthyReplicas++
			}
		}

		if status == HealthStatusHealthy {
			healthyTablets++
		} else {
			tabletIssues[tablet.TabletID] = issues
		}
	}

	// Analyze partition health
	accessibility := hc.analyzePartitionHealth(
		partitionInfo,
		healthyTablets,
		totalTablets,
		healthyReplicas,
		totalReplicas,
		tabletIssues)

	// Cache the result
	if hc.config.CacheEnabled {
		hc.cachePartitionHealth(partitionID, accessibility)
	}

	// Record metrics
	hc.metrics.GaugeSet("health_check_partition_healthy_tablets", float64(accessibility.HealthyTabletCount), map[string]string{
		"partition_id": fmt.Sprintf("%d", partitionID),
	})
	hc.metrics.GaugeSet("health_check_partition_total_tablets", float64(accessibility.TotalTabletCount), map[string]string{
		"partition_id": fmt.Sprintf("%d", partitionID),
	})
	hc.metrics.GaugeSet("health_check_partition_healthy_replicas", float64(accessibility.HealthyReplicas), map[string]string{
		"partition_id": fmt.Sprintf("%d", partitionID),
	})
	hc.metrics.GaugeSet("health_check_partition_total_replicas", float64(accessibility.TotalReplicas), map[string]string{
		"partition_id": fmt.Sprintf("%d", partitionID),
	})

	return accessibility, nil
}

// analyzePartitionHealth analyzes the health of a partition
func (hc *HealthCheckerImpl) analyzePartitionHealth(
	partitionInfo tablet.PartitionInfo,
	healthyTablets int,
	totalTablets int,
	healthyReplicas int,
	totalReplicas int,
	tabletIssues map[int64][]string,
) PartitionAccessibility {
	issues := make([]string, 0)
	status := HealthStatusHealthy
	accessLevel := AccessLevelFull
	readSupported := true
	writeSupported := true

	// Initialize accessibility
	accessibility := PartitionAccessibility{
		PartitionID:             partitionInfo.PartitionID,
		PartitionName:           partitionInfo.PartitionName,
		HealthyTabletCount:      healthyTablets,
		TotalTabletCount:        totalTablets,
		HealthyReplicas:         healthyReplicas,
		TotalReplicas:           totalReplicas,
		Status:                  HealthStatusHealthy,
		AccessLevel:             AccessLevelFull,
		ReadOperationSupported:  true,
		WriteOperationSupported: true,
		LastUpdated:             time.Now(),
	}

	// Check if we have any tablets
	if totalTablets == 0 {
		status = HealthStatusUnhealthy
		accessLevel = AccessLevelNone
		readSupported = false
		writeSupported = false
		issues = append(issues, "No tablets found for partition")
	} else {
		// Check tablet health percentage
		tabletHealthPercentage := 0.0
		if totalTablets > 0 {
			tabletHealthPercentage = float64(healthyTablets) / float64(totalTablets) * 100
		}

		// Check replica health percentage
		replicaHealthPercentage := 0.0
		if totalReplicas > 0 {
			replicaHealthPercentage = float64(healthyReplicas) / float64(totalReplicas) * 100
		}

		// Determine status based on health percentages
		if tabletHealthPercentage < 50 || replicaHealthPercentage < 50 {
			status = HealthStatusUnhealthy
			accessLevel = AccessLevelNone
			readSupported = false
			writeSupported = false
			issues = append(issues, fmt.Sprintf("Only %.2f%% of tablets and %.2f%% of replicas are healthy",
				tabletHealthPercentage, replicaHealthPercentage))
		} else if tabletHealthPercentage < hc.config.TabletHealthThreshold ||
			replicaHealthPercentage < hc.config.ReplicaHealthThreshold {
			status = HealthStatusDegraded
			accessLevel = AccessLevelReadOnly
			writeSupported = false
			issues = append(issues, fmt.Sprintf("Only %.2f%% of tablets and %.2f%% of replicas are healthy",
				tabletHealthPercentage, replicaHealthPercentage))
		}

		// Add tablet issues
		for tabletID, tabletIssueList := range tabletIssues {
			for _, issue := range tabletIssueList {
				issues = append(issues, fmt.Sprintf("Tablet %d: %s", tabletID, issue))
			}
		}
	}

	// Check partition state
	if partitionInfo.State != "NORMAL" && partitionInfo.State != "HEALTHY" {
		if status != HealthStatusUnhealthy {
			status = HealthStatusDegraded
		}
		if accessLevel == AccessLevelFull {
			accessLevel = AccessLevelPartial
		}
		issues = append(issues, fmt.Sprintf("Partition state is not normal: %s", partitionInfo.State))
	}

	accessibility.Status = status
	accessibility.AccessLevel = accessLevel
	accessibility.ReadOperationSupported = readSupported
	accessibility.WriteOperationSupported = writeSupported
	accessibility.Issues = issues

	return accessibility
}

// CheckTableHealth checks the health of a specific table
func (hc *HealthCheckerImpl) CheckTableHealth(ctx context.Context, databaseID, tableID int64) (TableAccessibility, error) {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(ctx, hc.config.CheckTimeout)
	defer cancel()

	// Check if cached result is available
	if hc.config.CacheEnabled {
		if cachedResult, found := hc.getCachedTableHealth(tableID); found {
			return cachedResult, nil
		}
	}

	// Get table information from the tablet manager
	tableInfo, err := hc.tabletManager.GetTableInfo(ctx, databaseID, tableID)
	if err != nil {
		hc.logger.Error("Failed to get table info",
			"databaseID", databaseID,
			"tableID", tableID,
			"error", err)
		return TableAccessibility{}, err
	}

	// Get partitions for this table
	partitions, err := hc.tabletManager.GetPartitionsForTable(ctx, databaseID, tableID)
	if err != nil {
		hc.logger.Error("Failed to get partitions for table",
			"databaseID", databaseID,
			"tableID", tableID,
			"error", err)
		return TableAccessibility{}, err
	}

	// Check health of each partition
	partitionAccessibility := make(map[int64]PartitionAccessibility)
	healthyPartitions := 0

	for _, partition := range partitions {
		accessibility, err := hc.CheckPartitionHealth(ctx, databaseID, tableID, partition.PartitionID)
		if err != nil {
			hc.logger.Error("Failed to check partition health",
				"partitionID", partition.PartitionID,
				"error", err)
			continue
		}

		partitionAccessibility[partition.PartitionID] = accessibility
		if accessibility.Status == HealthStatusHealthy {
			healthyPartitions++
		}
	}

	// Analyze table health
	accessibility := hc.analyzeTableHealth(tableInfo, partitionAccessibility, healthyPartitions, len(partitions))

	// Cache the result
	if hc.config.CacheEnabled {
		hc.cacheTableHealth(tableID, accessibility)
	}

	// Record metrics
	hc.metrics.GaugeSet("health_check_table_healthy_partitions", float64(accessibility.HealthyPartitions), map[string]string{
		"table_id": fmt.Sprintf("%d", tableID),
	})
	hc.metrics.GaugeSet("health_check_table_total_partitions", float64(accessibility.TotalPartitions), map[string]string{
		"table_id": fmt.Sprintf("%d", tableID),
	})

	return accessibility, nil
}

// analyzeTableHealth analyzes the health of a table
func (hc *HealthCheckerImpl) analyzeTableHealth(
	tableInfo tablet.TableInfo,
	partitionAccessibility map[int64]PartitionAccessibility,
	healthyPartitions int,
	totalPartitions int,
) TableAccessibility {
	issues := make([]string, 0)
	status := HealthStatusHealthy
	accessLevel := AccessLevelFull
	readSupported := true
	writeSupported := true

	// Initialize accessibility
	accessibility := TableAccessibility{
		DatabaseID:              tableInfo.DatabaseID,
		DatabaseName:            tableInfo.DatabaseName,
		TableID:                 tableInfo.TableID,
		TableName:               tableInfo.TableName,
		PartitionAccessibility:  partitionAccessibility,
		HealthyPartitions:       healthyPartitions,
		TotalPartitions:         totalPartitions,
		Status:                  HealthStatusHealthy,
		AccessLevel:             AccessLevelFull,
		ReadOperationSupported:  true,
		WriteOperationSupported: true,
		LastUpdated:             time.Now(),
	}

	// Check if we have any partitions
	if totalPartitions == 0 {
		status = HealthStatusUnhealthy
		accessLevel = AccessLevelNone
		readSupported = false
		writeSupported = false
		issues = append(issues, "No partitions found for table")
	} else {
		// Count partitions by access level
		partitionsByAccessLevel := make(map[AccessLevel]int)
		for _, partition := range partitionAccessibility {
			partitionsByAccessLevel[partition.AccessLevel]++

			// Collect issues from partitions
			for _, issue := range partition.Issues {
				issues = append(issues, fmt.Sprintf("Partition %d: %s", partition.PartitionID, issue))
			}
		}

		// Determine table access level based on partition access levels
		if partitionsByAccessLevel[AccessLevelNone] == totalPartitions {
			// All partitions are inaccessible
			status = HealthStatusUnhealthy
			accessLevel = AccessLevelNone
			readSupported = false
			writeSupported = false
		} else if partitionsByAccessLevel[AccessLevelNone] > 0 {
			// Some partitions are inaccessible
			status = HealthStatusDegraded
			accessLevel = AccessLevelPartial
			issues = append(issues, fmt.Sprintf("%d out of %d partitions are completely inaccessible",
				partitionsByAccessLevel[AccessLevelNone], totalPartitions))
		}

		// Check if any partitions are read-only
		if partitionsByAccessLevel[AccessLevelReadOnly] > 0 {
			if status != HealthStatusUnhealthy {
				status = HealthStatusDegraded
			}
			if accessLevel != AccessLevelNone {
				accessLevel = AccessLevelReadOnly
				writeSupported = false
			}
			issues = append(issues, fmt.Sprintf("%d out of %d partitions are read-only",
				partitionsByAccessLevel[AccessLevelReadOnly], totalPartitions))
		}

		// Check if any partitions have partial access
		if partitionsByAccessLevel[AccessLevelPartial] > 0 {
			if status != HealthStatusUnhealthy && status != HealthStatusDegraded {
				status = HealthStatusDegraded
			}
			if accessLevel == AccessLevelFull {
				accessLevel = AccessLevelPartial
			}
			issues = append(issues, fmt.Sprintf("%d out of %d partitions have partial access",
				partitionsByAccessLevel[AccessLevelPartial], totalPartitions))
		}

		// Check partition health percentage
		partitionHealthPercentage := 0.0
		if totalPartitions > 0 {
			partitionHealthPercentage = float64(healthyPartitions) / float64(totalPartitions) * 100
		}

		if partitionHealthPercentage < 50 {
			status = HealthStatusUnhealthy
			if accessLevel != AccessLevelNone {
				accessLevel = AccessLevelPartial
			}
			issues = append(issues, fmt.Sprintf("Only %.2f%% of partitions are healthy", partitionHealthPercentage))
		} else if partitionHealthPercentage < hc.config.TabletHealthThreshold {
			if status != HealthStatusUnhealthy {
				status = HealthStatusDegraded
			}
			if accessLevel == AccessLevelFull {
				accessLevel = AccessLevelPartial
			}
			issues = append(issues, fmt.Sprintf("Only %.2f%% of partitions are healthy", partitionHealthPercentage))
		}
	}

	// Check table state
	if tableInfo.State != "NORMAL" && tableInfo.State != "HEALTHY" {
		if status != HealthStatusUnhealthy {
			status = HealthStatusDegraded
		}
		if accessLevel == AccessLevelFull {
			accessLevel = AccessLevelPartial
		}
		issues = append(issues, fmt.Sprintf("Table state is not normal: %s", tableInfo.State))
	}

	accessibility.Status = status
	accessibility.AccessLevel = accessLevel
	accessibility.ReadOperationSupported = readSupported
	accessibility.WriteOperationSupported = writeSupported
	accessibility.Issues = issues

	return accessibility
}

// CheckQueryExecutability checks if a query can be executed
func (hc *HealthCheckerImpl) CheckQueryExecutability(ctx context.Context, query string, tableIDs []int64) (QueryExecutability, error) {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(ctx, hc.config.CheckTimeout)
	defer cancel()

	// In a real implementation, you would parse the query to identify the tables involved
	// For simplicity, we'll use the provided table IDs

	// Check health of each table
	tables := make([]TableAccessibility, 0, len(tableIDs))
	var databaseID int64 = 1 // Default database ID, in a real implementation you'd get this from the query context

	for _, tableID := range tableIDs {
		tableAccessibility, err := hc.CheckTableHealth(ctx, databaseID, tableID)
		if err != nil {
			hc.logger.Error("Failed to check table health", "tableID", tableID, "error", err)
			continue
		}
		tables = append(tables, tableAccessibility)
	}

	// Analyze query executability
	executability := hc.analyzeQueryExecutability(query, tables)

	// Record metrics
	hc.metrics.CounterInc("health_check_query_executability", map[string]string{
		"executable": fmt.Sprintf("%t", executability.IsExecutable),
		"level":      string(executability.ExecutionLevel),
	})

	return executability, nil
}

// analyzeQueryExecutability analyzes if a query can be executed
func (hc *HealthCheckerImpl) analyzeQueryExecutability(query string, tables []TableAccessibility) QueryExecutability {
	issues := make([]string, 0)
	isExecutable := true
	executionLevel := AccessLevelFull
	estimatedImpact := 0.0

	// Initialize executability
	executability := QueryExecutability{
		QueryID:         fmt.Sprintf("q-%d", time.Now().UnixNano()),
		QueryType:       "SELECT", // Assuming SELECT, in a real implementation you'd parse the query
		Tables:          tables,
		IsExecutable:    true,
		ExecutionLevel:  AccessLevelFull,
		EstimatedImpact: 0.0,
		LastUpdated:     time.Now(),
	}

	// Check if we have any tables
	if len(tables) == 0 {
		isExecutable = false
		executionLevel = AccessLevelNone
		issues = append(issues, "No table health information available")

		executability.IsExecutable = isExecutable
		executability.ExecutionLevel = executionLevel
		executability.Issues = issues
		return executability
	}

	// Determine overall executability based on table health
	for _, table := range tables {
		// Collect issues from tables
		for _, issue := range table.Issues {
			issues = append(issues, fmt.Sprintf("Table %s: %s", table.TableName, issue))
		}

		// If any table is completely inaccessible, the query can't be executed
		if table.AccessLevel == AccessLevelNone {
			isExecutable = false
			executionLevel = AccessLevelNone
			issues = append(issues, fmt.Sprintf("Table %s is completely inaccessible", table.TableName))
			break
		}

		// If any table is read-only or partial, adjust the execution level
		if table.AccessLevel == AccessLevelReadOnly && executionLevel != AccessLevelNone {
			executionLevel = AccessLevelReadOnly
		} else if table.AccessLevel == AccessLevelPartial &&
			executionLevel != AccessLevelNone &&
			executionLevel != AccessLevelReadOnly {
			executionLevel = AccessLevelPartial
		}

		// Calculate impact based on table health
		tableImpact := 0.0
		switch table.Status {
		case HealthStatusDegraded:
			tableImpact = 0.3
		case HealthStatusUnhealthy:
			tableImpact = 0.7
		}

		// Accumulate impact
		if tableImpact > estimatedImpact {
			estimatedImpact = tableImpact
		}
	}

	// For write queries, check if all tables support writes
	if executability.QueryType != "SELECT" {
		for _, table := range tables {
			if !table.WriteOperationSupported {
				isExecutable = false
				executionLevel = AccessLevelNone
				issues = append(issues, fmt.Sprintf("Write operations not supported on table %s", table.TableName))
				break
			}
		}
	}

	executability.IsExecutable = isExecutable
	executability.ExecutionLevel = executionLevel
	executability.EstimatedImpact = estimatedImpact
	executability.Issues = issues

	return executability
}

// CheckWriteExecutability checks if a write operation can be executed
func (hc *HealthCheckerImpl) CheckWriteExecutability(ctx context.Context, operation OperationType, tableIDs []int64) (WriteExecutability, error) {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(ctx, hc.config.CheckTimeout)
	defer cancel()

	// Check health of each table
	tables := make([]TableAccessibility, 0, len(tableIDs))
	var databaseID int64 = 1 // Default database ID, in a real implementation you'd get this from the operation context

	for _, tableID := range tableIDs {
		tableAccessibility, err := hc.CheckTableHealth(ctx, databaseID, tableID)
		if err != nil {
			hc.logger.Error("Failed to check table health", "tableID", tableID, "error", err)
			continue
		}
		tables = append(tables, tableAccessibility)
	}

	// Analyze write executability
	executability := hc.analyzeWriteExecutability(operation, tables)

	// Record metrics
	hc.metrics.CounterInc("health_check_write_executability", map[string]string{
		"operation":  string(operation),
		"executable": fmt.Sprintf("%t", executability.IsExecutable),
		"level":      string(executability.ExecutionLevel),
	})

	return executability, nil
}

// analyzeWriteExecutability analyzes if a write operation can be executed
func (hc *HealthCheckerImpl) analyzeWriteExecutability(operation OperationType, tables []TableAccessibility) WriteExecutability {
	issues := make([]string, 0)
	isExecutable := true
	executionLevel := AccessLevelFull
	estimatedImpact := 0.0

	// Initialize executability
	executability := WriteExecutability{
		OperationID:     fmt.Sprintf("op-%d", time.Now().UnixNano()),
		OperationType:   operation,
		Tables:          tables,
		IsExecutable:    true,
		ExecutionLevel:  AccessLevelFull,
		EstimatedImpact: 0.0,
		LastUpdated:     time.Now(),
	}

	// Check if we have any tables
	if len(tables) == 0 {
		isExecutable = false
		executionLevel = AccessLevelNone
		issues = append(issues, "No table health information available")

		executability.IsExecutable = isExecutable
		executability.ExecutionLevel = executionLevel
		executability.Issues = issues
		return executability
	}

	// Determine overall executability based on table health
	for _, table := range tables {
		// Collect issues from tables
		for _, issue := range table.Issues {
			issues = append(issues, fmt.Sprintf("Table %s: %s", table.TableName, issue))
		}

		// For write operations, all tables must support writes
		if !table.WriteOperationSupported {
			isExecutable = false
			executionLevel = AccessLevelNone
			issues = append(issues, fmt.Sprintf("Write operations not supported on table %s", table.TableName))
			break
		}

		// If any table is completely inaccessible, the operation can't be executed
		if table.AccessLevel == AccessLevelNone {
			isExecutable = false
			executionLevel = AccessLevelNone
			issues = append(issues, fmt.Sprintf("Table %s is completely inaccessible", table.TableName))
			break
		}

		// If any table is read-only, the write operation can't be executed
		if table.AccessLevel == AccessLevelReadOnly {
			isExecutable = false
			executionLevel = AccessLevelNone
			issues = append(issues, fmt.Sprintf("Table %s is read-only", table.TableName))
			break
		}

		// If any table has partial access, adjust the execution level
		if table.AccessLevel == AccessLevelPartial && executionLevel != AccessLevelNone {
			executionLevel = AccessLevelPartial
			issues = append(issues, fmt.Sprintf("Table %s has partial access, write operation may be partially successful", table.TableName))
		}

		// Calculate impact based on table health
		tableImpact := 0.0
		switch table.Status {
		case HealthStatusDegraded:
			tableImpact = 0.3
		case HealthStatusUnhealthy:
			tableImpact = 0.7
		}

		// Accumulate impact
		if tableImpact > estimatedImpact {
			estimatedImpact = tableImpact
		}
	}

	executability.IsExecutable = isExecutable
	executability.ExecutionLevel = executionLevel
	executability.EstimatedImpact = estimatedImpact
	executability.Issues = issues

	return executability
}

// GenerateHealthReport generates a comprehensive health report
func (hc *HealthCheckerImpl) GenerateHealthReport(ctx context.Context) (HealthCheckReport, error) {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(ctx, hc.config.CheckTimeout)
	defer cancel()

	// Check all backends health
	backends, err := hc.CheckAllBackendsHealth(ctx)
	if err != nil {
		hc.logger.Error("Failed to check all backends health", "error", err)
		return HealthCheckReport{}, err
	}

	// Get all tables
	// In a real implementation, you would get this from a metadata service
	// For simplicity, we'll use a dummy list
	tableIDs := []int64{1, 2, 3}
	var databaseID int64 = 1

	// Check health of each table
	tables := make([]TableAccessibility, 0, len(tableIDs))
	for _, tableID := range tableIDs {
		tableAccessibility, err := hc.CheckTableHealth(ctx, databaseID, tableID)
		if err != nil {
			hc.logger.Error("Failed to check table health", "tableID", tableID, "error", err)
			continue
		}
		tables = append(tables, tableAccessibility)
	}

	// Generate the report
	report := hc.compileHealthReport(backends, tables)

	return report, nil
}

// compileHealthReport compiles a health report from backend and table health information
func (hc *HealthCheckerImpl) compileHealthReport(backends []BackendHealthDetail, tables []TableAccessibility) HealthCheckReport {
	unhealthyComponents := make([]string, 0)
	issuesSummary := make([]string, 0)
	overallStatus := HealthStatusHealthy

	// Check backends health
	unhealthyBackends := 0
	for _, backend := range backends {
		if backend.Status != HealthStatusHealthy {
			unhealthyComponents = append(unhealthyComponents,
				fmt.Sprintf("Backend %d (%s): %s", backend.BackendID, backend.Host, backend.Status))

			for _, issue := range backend.Issues {
				issuesSummary = append(issuesSummary,
					fmt.Sprintf("Backend %d (%s): %s", backend.BackendID, backend.Host, issue))
			}

			unhealthyBackends++
		}
	}

	// Check tables health
	unhealthyTables := 0
	for _, table := range tables {
		if table.Status != HealthStatusHealthy {
			unhealthyComponents = append(unhealthyComponents,
				fmt.Sprintf("Table %s (%d): %s", table.TableName, table.TableID, table.Status))

			for _, issue := range table.Issues {
				issuesSummary = append(issuesSummary,
					fmt.Sprintf("Table %s (%d): %s", table.TableName, table.TableID, issue))
			}

			unhealthyTables++
		}
	}

	// Determine overall status
	if len(backends) > 0 {
		backendHealthPercentage := float64(len(backends)-unhealthyBackends) / float64(len(backends)) * 100
		if backendHealthPercentage < 50 {
			overallStatus = HealthStatusUnhealthy
		} else if backendHealthPercentage < hc.config.BackendHealthThreshold {
			overallStatus = HealthStatusDegraded
		}
	}

	if len(tables) > 0 && overallStatus != HealthStatusUnhealthy {
		tableHealthPercentage := float64(len(tables)-unhealthyTables) / float64(len(tables)) * 100
		if tableHealthPercentage < 50 {
			overallStatus = HealthStatusUnhealthy
		} else if tableHealthPercentage < hc.config.TabletHealthThreshold {
			if overallStatus != HealthStatusUnhealthy {
				overallStatus = HealthStatusDegraded
			}
		}
	}

	// Create the report
	report := HealthCheckReport{
		OverallStatus:       overallStatus,
		Backends:            backends,
		Tables:              tables,
		UnhealthyComponents: unhealthyComponents,
		IssuesSummary:       issuesSummary,
		GeneratedAt:         time.Now(),
		GeneratedBy:         "HealthChecker",
	}

	return report
}

// ClearCache clears the health check cache
func (hc *HealthCheckerImpl) ClearCache() {
	hc.cacheMutex.Lock()
	defer hc.cacheMutex.Unlock()

	hc.backendHealthCache = make(map[int64]BackendHealthDetail)
	hc.tabletHealthCache = make(map[int64]cachedTabletHealth)
	hc.partitionHealthCache = make(map[int64]PartitionAccessibility)
	hc.tableHealthCache = make(map[int64]TableAccessibility)
	hc.lastCacheCleanup = time.Now()

	hc.logger.Info("Health check cache cleared")
}

// Cache management methods

// getCachedBackendHealth gets a cached backend health result
func (hc *HealthCheckerImpl) getCachedBackendHealth(backendID int64) (BackendHealthDetail, bool) {
	hc.cacheMutex.RLock()
	defer hc.cacheMutex.RUnlock()

	if cached, found := hc.backendHealthCache[backendID]; found {
		if time.Since(cached.LastUpdated) < hc.config.CacheTTL {
			return cached, true
		}
	}

	return BackendHealthDetail{}, false
}

// cacheBackendHealth caches a backend health result
func (hc *HealthCheckerImpl) cacheBackendHealth(backendID int64, health BackendHealthDetail) {
	hc.cacheMutex.Lock()
	defer hc.cacheMutex.Unlock()

	hc.backendHealthCache[backendID] = health
	hc.cleanupCacheIfNeeded()
}

// getCachedTabletHealth gets a cached tablet health result
func (hc *HealthCheckerImpl) getCachedTabletHealth(tabletID int64) (cachedTabletHealth, bool) {
	hc.cacheMutex.RLock()
	defer hc.cacheMutex.RUnlock()

	if cached, found := hc.tabletHealthCache[tabletID]; found {
		if time.Since(cached.LastUpdated) < hc.config.CacheTTL {
			return cached, true
		}
	}

	return cachedTabletHealth{}, false
}

// cacheTabletHealth caches a tablet health result
func (hc *HealthCheckerImpl) cacheTabletHealth(tabletID int64, status HealthStatus, issues []string) {
	hc.cacheMutex.Lock()
	defer hc.cacheMutex.Unlock()

	hc.tabletHealthCache[tabletID] = cachedTabletHealth{
		Status:      status,
		Issues:      issues,
		LastUpdated: time.Now(),
	}
	hc.cleanupCacheIfNeeded()
}

// getCachedPartitionHealth gets a cached partition health result
func (hc *HealthCheckerImpl) getCachedPartitionHealth(partitionID int64) (PartitionAccessibility, bool) {
	hc.cacheMutex.RLock()
	defer hc.cacheMutex.RUnlock()

	if cached, found := hc.partitionHealthCache[partitionID]; found {
		if time.Since(cached.LastUpdated) < hc.config.CacheTTL {
			return cached, true
		}
	}

	return PartitionAccessibility{}, false
}

// cachePartitionHealth caches a partition health result
func (hc *HealthCheckerImpl) cachePartitionHealth(partitionID int64, accessibility PartitionAccessibility) {
	hc.cacheMutex.Lock()
	defer hc.cacheMutex.Unlock()

	hc.partitionHealthCache[partitionID] = accessibility
	hc.cleanupCacheIfNeeded()
}

// getCachedTableHealth gets a cached table health result
func (hc *HealthCheckerImpl) getCachedTableHealth(tableID int64) (TableAccessibility, bool) {
	hc.cacheMutex.RLock()
	defer hc.cacheMutex.RUnlock()

	if cached, found := hc.tableHealthCache[tableID]; found {
		if time.Since(cached.LastUpdated) < hc.config.CacheTTL {
			return cached, true
		}
	}

	return TableAccessibility{}, false
}

// cacheTableHealth caches a table health result
func (hc *HealthCheckerImpl) cacheTableHealth(tableID int64, accessibility TableAccessibility) {
	hc.cacheMutex.Lock()
	defer hc.cacheMutex.Unlock()

	hc.tableHealthCache[tableID] = accessibility
	hc.cleanupCacheIfNeeded()
}

// cleanupCacheIfNeeded cleans up expired cache entries
func (hc *HealthCheckerImpl) cleanupCacheIfNeeded() {
	// Only clean up once per hour to avoid excessive cleanup operations
	if time.Since(hc.lastCacheCleanup) < time.Hour {
		return
	}

	// Clean up backend health cache
	for id, health := range hc.backendHealthCache {
		if time.Since(health.LastUpdated) > hc.config.CacheTTL {
			delete(hc.backendHealthCache, id)
		}
	}

	// Clean up tablet health cache
	for id, health := range hc.tabletHealthCache {
		if time.Since(health.LastUpdated) > hc.config.CacheTTL {
			delete(hc.tabletHealthCache, id)
		}
	}

	// Clean up partition health cache
	for id, health := range hc.partitionHealthCache {
		if time.Since(health.LastUpdated) > hc.config.CacheTTL {
			delete(hc.partitionHealthCache, id)
		}
	}

	// Clean up table health cache
	for id, health := range hc.tableHealthCache {
		if time.Since(health.LastUpdated) > hc.config.CacheTTL {
			delete(hc.tableHealthCache, id)
		}
	}

	hc.lastCacheCleanup = time.Now()
}

// recordBackendHealthMetrics records backend health metrics
func (hc *HealthCheckerImpl) recordBackendHealthMetrics(detail BackendHealthDetail) {
	hc.metrics.GaugeSet("health_check_backend_status", float64(getStatusNumericValue(detail.Status)), map[string]string{
		"backend_id": fmt.Sprintf("%d", detail.BackendID),
		"host":       detail.Host,
	})
	hc.metrics.GaugeSet("health_check_backend_disk_usage", detail.DiskUsagePercent, map[string]string{
		"backend_id": fmt.Sprintf("%d", detail.BackendID),
		"host":       detail.Host,
	})
	hc.metrics.GaugeSet("health_check_backend_is_alive", boolToFloat64(detail.IsAlive), map[string]string{
		"backend_id": fmt.Sprintf("%d", detail.BackendID),
		"host":       detail.Host,
	})
}

// getStatusNumericValue converts a health status to a numeric value for metrics
func getStatusNumericValue(status HealthStatus) int {
	switch status {
	case HealthStatusHealthy:
		return 0
	case HealthStatusDegraded:
		return 1
	case HealthStatusUnhealthy:
		return 2
	default:
		return 3 // Unknown
	}
}

// boolToFloat64 converts a bool to a float64 (1.0 for true, 0.0 for false)
func boolToFloat64(b bool) float64 {
	if b {
		return 1.0
	}
	return 0.0
}

//Personal.AI order the ending
