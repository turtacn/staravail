// Package services provides application-level services for the StarRocks proxy.
package services

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	logging "github.com/turtacn/staravail/internal/common/logger"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/domain/health"
	"github.com/turtacn/staravail/internal/domain/tablet"
	"github.com/turtacn/staravail/internal/infrastructure/config"
)

// HealthLevel represents the severity level of a health status
type HealthLevel string

const (
	// HealthLevelNormal indicates normal operation
	HealthLevelNormal HealthLevel = "NORMAL"

	// HealthLevelWarning indicates minor issues that don't affect functionality
	HealthLevelWarning HealthLevel = "WARNING"

	// HealthLevelDegraded indicates functionality is degraded but still operational
	HealthLevelDegraded HealthLevel = "DEGRADED"

	// HealthLevelCritical indicates severe issues that affect functionality
	HealthLevelCritical HealthLevel = "CRITICAL"

	// HealthLevelUnknown indicates the health status is unknown
	HealthLevelUnknown HealthLevel = "UNKNOWN"
)

// HealthScope defines the scope of a health check
type HealthScope string

const (
	// HealthScopeCluster indicates a cluster-wide health check
	HealthScopeCluster HealthScope = "CLUSTER"

	// HealthScopeDatabase indicates a database-specific health check
	HealthScopeDatabase HealthScope = "DATABASE"

	// HealthScopeTable indicates a table-specific health check
	HealthScopeTable HealthScope = "TABLE"

	// HealthScopePartition indicates a partition-specific health check
	HealthScopePartition HealthScope = "PARTITION"

	// HealthScopeTablet indicates a tablet-specific health check
	HealthScopeTablet HealthScope = "TABLET"

	// HealthScopeBackend indicates a backend-specific health check
	HealthScopeBackend HealthScope = "BACKEND"

	// HealthScopeComponent indicates a component-specific health check
	HealthScopeComponent HealthScope = "COMPONENT"
)

// HealthStatus represents a detailed health status
type HealthStatus struct {
	// Level is the overall health level
	Level HealthLevel `json:"level"`

	// Scope is the scope of the health status
	Scope HealthScope `json:"scope"`

	// Target is the specific target of the health status (e.g., table name)
	Target string `json:"target"`

	// Message is a human-readable message describing the health status
	Message string `json:"message"`

	// Components is a map of component names to their health status
	Components map[string]*ComponentHealth `json:"components,omitempty"`

	// Metrics contains relevant metrics for this health status
	Metrics map[string]float64 `json:"metrics,omitempty"`

	// Details contains additional details about the health status
	Details map[string]interface{} `json:"details,omitempty"`

	// Timestamp is when this health status was generated
	Timestamp time.Time `json:"timestamp"`

	// Duration is how long the check took
	Duration time.Duration `json:"duration"`

	// Issues is a list of specific issues found
	Issues []HealthIssue `json:"issues,omitempty"`

	// Recommendations is a list of recommendations for resolving issues
	Recommendations []string `json:"recommendations,omitempty"`
}

// ComponentHealth represents the health status of a specific component
type ComponentHealth struct {
	// Name is the name of the component
	Name string `json:"name"`

	// Level is the health level of the component
	Level HealthLevel `json:"level"`

	// Message is a human-readable message describing the component's health
	Message string `json:"message"`

	// Details contains additional details about the component's health
	Details map[string]interface{} `json:"details,omitempty"`

	// Timestamp is when this component health was last updated
	Timestamp time.Time `json:"timestamp"`
}

// HealthIssue represents a specific health issue
type HealthIssue struct {
	// ID is a unique identifier for this issue
	ID string `json:"id"`

	// Level is the severity level of the issue
	Level HealthLevel `json:"level"`

	// Message is a human-readable message describing the issue
	Message string `json:"message"`

	// Component is the component where the issue was found
	Component string `json:"component"`

	// FirstDetected is when the issue was first detected
	FirstDetected time.Time `json:"first_detected"`

	// LastDetected is when the issue was last detected
	LastDetected time.Time `json:"last_detected"`

	// Count is how many times this issue has been detected
	Count int `json:"count"`

	// Status indicates whether the issue is new, ongoing, or resolved
	Status string `json:"status"`

	// Resolution is a message describing how the issue was resolved, if applicable
	Resolution string `json:"resolution,omitempty"`
}

// HealthHistory represents the history of health status changes
type HealthHistory struct {
	// Target is the specific target (e.g., table name)
	Target string `json:"target"`

	// Scope is the scope of the health history
	Scope HealthScope `json:"scope"`

	// Entries is a list of historical health statuses
	Entries []HealthHistoryEntry `json:"entries"`
}

// HealthHistoryEntry represents a single health status change
type HealthHistoryEntry struct {
	// Timestamp is when the health status changed
	Timestamp time.Time `json:"timestamp"`

	// Level is the health level at this point in time
	Level HealthLevel `json:"level"`

	// Message is a human-readable message describing the health status
	Message string `json:"message"`

	// Duration is how long this health status lasted
	Duration time.Duration `json:"duration,omitempty"`
}

// HealthDiagnosis represents a diagnosis of health issues
type HealthDiagnosis struct {
	// Target is the specific target (e.g., table name)
	Target string `json:"target"`

	// Scope is the scope of the diagnosis
	Scope HealthScope `json:"scope"`

	// Level is the overall health level
	Level HealthLevel `json:"level"`

	// Summary is a human-readable summary of the diagnosis
	Summary string `json:"summary"`

	// Issues is a list of identified issues
	Issues []HealthIssue `json:"issues"`

	// Recommendations is a list of recommendations for resolving issues
	Recommendations []string `json:"recommendations"`

	// RootCauses is a list of potential root causes
	RootCauses []string `json:"root_causes"`

	// RelatedComponents is a list of related components that may be affected
	RelatedComponents []string `json:"related_components"`

	// Timestamp is when this diagnosis was generated
	Timestamp time.Time `json:"timestamp"`
}

// HealthEvent represents a health status change event
type HealthEvent struct {
	// ID is a unique identifier for this event
	ID string `json:"id"`

	// Type is the type of event (e.g., "status_change", "issue_detected")
	Type string `json:"type"`

	// Level is the health level associated with this event
	Level HealthLevel `json:"level"`

	// PreviousLevel is the previous health level, if applicable
	PreviousLevel HealthLevel `json:"previous_level,omitempty"`

	// Scope is the scope of the event
	Scope HealthScope `json:"scope"`

	// Target is the specific target of the event (e.g., table name)
	Target string `json:"target"`

	// Message is a human-readable message describing the event
	Message string `json:"message"`

	// Timestamp is when this event occurred
	Timestamp time.Time `json:"timestamp"`

	// Details contains additional details about the event
	Details map[string]interface{} `json:"details,omitempty"`
}

// HealthSubscription represents a subscription to health events
type HealthSubscription struct {
	// ID is a unique identifier for this subscription
	ID string `json:"id"`

	// Scopes is a list of scopes to subscribe to
	Scopes []HealthScope `json:"scopes"`

	// Targets is a list of specific targets to subscribe to
	Targets []string `json:"targets"`

	// Levels is a list of health levels to subscribe to
	Levels []HealthLevel `json:"levels"`

	// EventTypes is a list of event types to subscribe to
	EventTypes []string `json:"event_types"`

	// CallbackFunc is a function to call when an event occurs
	CallbackFunc func(HealthEvent)

	// Channel is a channel to send events to
	Channel chan HealthEvent

	// Created is when this subscription was created
	Created time.Time
}

// HealthService defines the interface for health services
type HealthService interface {
	// GetClusterHealth returns the overall health of the cluster
	GetClusterHealth(ctx context.Context) (*HealthStatus, error)

	// GetDatabaseHealth returns the health of a specific database
	GetDatabaseHealth(ctx context.Context, database string) (*HealthStatus, error)

	// GetTableHealth returns the health of a specific table
	GetTableHealth(ctx context.Context, database, table string) (*HealthStatus, error)

	// GetPartitionHealth returns the health of a specific partition
	GetPartitionHealth(ctx context.Context, database, table, partition string) (*HealthStatus, error)

	// GetTabletHealth returns the health of a specific tablet
	GetTabletHealth(ctx context.Context, tabletID string) (*HealthStatus, error)

	// GetBackendHealth returns the health of a specific backend
	GetBackendHealth(ctx context.Context, backendID string) (*HealthStatus, error)

	// GetComponentHealth returns the health of a specific component
	GetComponentHealth(ctx context.Context, component string) (*ComponentHealth, error)

	// GetHealthHistory returns the health history for a target
	GetHealthHistory(ctx context.Context, scope HealthScope, target string, limit int, since time.Time) (*HealthHistory, error)

	// GetHealthDiagnosis returns a diagnosis of health issues for a target
	GetHealthDiagnosis(ctx context.Context, scope HealthScope, target string) (*HealthDiagnosis, error)

	// SubscribeHealthUpdates subscribes to health updates
	SubscribeHealthUpdates(ctx context.Context, scopes []HealthScope, targets []string, levels []HealthLevel, eventTypes []string) (*HealthSubscription, error)

	// UnsubscribeHealthUpdates unsubscribes from health updates
	UnsubscribeHealthUpdates(ctx context.Context, subscriptionID string) error

	// ReportHealthIssue reports a health issue
	ReportHealthIssue(ctx context.Context, scope HealthScope, target string, level HealthLevel, message string, details map[string]interface{}) error
}

// StarRocksHealthService implements HealthService for StarRocks
type StarRocksHealthService struct {
	// Config is the configuration for the service
	Config *config.Config

	// Logger is the logger for the service
	Logger logging.Logger

	// Metrics is the metrics recorder for the service
	Metrics metrics.MetricsRecorder

	// HealthMonitor monitors overall health
	HealthMonitor health.HealthMonitor

	// HealthChecker checks detailed component health
	HealthChecker health.HealthChecker

	// TabletManager manages tablet information
	TabletManager tablet.TabletManager

	// healthHistory stores historical health statuses
	healthHistory map[string][]HealthHistoryEntry

	// issues stores current health issues
	issues map[string]HealthIssue

	// subscriptions stores active subscriptions
	subscriptions map[string]*HealthSubscription

	// subscriptionsMutex protects subscriptions
	subscriptionsMutex sync.RWMutex

	// historyMutex protects healthHistory
	historyMutex sync.RWMutex

	// issuesMutex protects issues
	issuesMutex sync.RWMutex

	// healthStatusGauge is a metric for the current health status
	healthStatusGauge *prometheus.GaugeVec

	// healthIssuesGauge is a metric for the number of health issues
	healthIssuesGauge *prometheus.GaugeVec

	// healthCheckDuration is a metric for the duration of health checks
	healthCheckDuration *prometheus.HistogramVec

	// lastHealthStatus stores the last health status for each target
	lastHealthStatus map[string]*HealthStatus

	// lastHealthStatusMutex protects lastHealthStatus
	lastHealthStatusMutex sync.RWMutex
}

// NewStarRocksHealthService creates a new StarRocksHealthService
func NewStarRocksHealthService(
	config *config.Config,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
	healthMonitor health.HealthMonitor,
	healthChecker health.HealthChecker,
	tabletManager tablet.TabletManager,
) *StarRocksHealthService {
	// Create metrics
	healthStatusGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "health_status",
			Help: "Current health status (0=Unknown, 1=Normal, 2=Warning, 3=Degraded, 4=Critical)",
		},
		[]string{"scope", "target"},
	)

	healthIssuesGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "health_issues",
			Help: "Number of health issues by level",
		},
		[]string{"scope", "target", "level"},
	)

	healthCheckDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "health_check_duration_seconds",
			Help:    "Duration of health checks",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10},
		},
		[]string{"scope", "target"},
	)

	// Register metrics if a registry is provided
	if metricsRecorder != nil {
		if registry, ok := metricsRecorder.GetRegistry().(*prometheus.Registry); ok {
			registry.MustRegister(healthStatusGauge, healthIssuesGauge, healthCheckDuration)
		}
	}

	service := &StarRocksHealthService{
		Config:                config,
		Logger:                logger,
		Metrics:               metricsRecorder,
		HealthMonitor:         healthMonitor,
		HealthChecker:         healthChecker,
		TabletManager:         tabletManager,
		healthHistory:         make(map[string][]HealthHistoryEntry),
		issues:                make(map[string]HealthIssue),
		subscriptions:         make(map[string]*HealthSubscription),
		subscriptionsMutex:    sync.RWMutex{},
		historyMutex:          sync.RWMutex{},
		issuesMutex:           sync.RWMutex{},
		healthStatusGauge:     healthStatusGauge,
		healthIssuesGauge:     healthIssuesGauge,
		healthCheckDuration:   healthCheckDuration,
		lastHealthStatus:      make(map[string]*HealthStatus),
		lastHealthStatusMutex: sync.RWMutex{},
	}

	// Start background health monitoring
	service.startHealthMonitoring()

	return service
}

// GetClusterHealth returns the overall health of the cluster
func (s *StarRocksHealthService) GetClusterHealth(ctx context.Context) (*HealthStatus, error) {
	startTime := time.Now()

	s.Logger.Debug("Getting cluster health")

	// Get overall health result from HealthChecker
	healthResult := s.HealthChecker.Check(ctx)

	// Convert health result to HealthStatus
	status := s.convertHealthResultToStatus(healthResult, HealthScopeCluster, "cluster")

	// Set timestamp and duration
	status.Timestamp = time.Now()
	status.Duration = time.Since(startTime)

	// Log status
	s.Logger.Info("Cluster health check completed",
		"level", status.Level,
		"duration", status.Duration,
	)

	// Update metrics
	s.recordHealthMetrics(status)

	// Update health history
	s.updateHealthHistory(status)

	// Check for issues
	s.detectAndUpdateIssues(status)

	// Store last health status
	s.updateLastHealthStatus(status)

	// Send health event to subscribers
	s.notifyHealthSubscribers(status, "status_update")

	return status, nil
}

// GetDatabaseHealth returns the health of a specific database
func (s *StarRocksHealthService) GetDatabaseHealth(ctx context.Context, database string) (*HealthStatus, error) {
	startTime := time.Now()

	s.Logger.Debug("Getting database health", "database", database)

	// Get backend information for the database
	dbInfo, err := s.TabletManager.GetDatabaseInfo(ctx, database)
	if err != nil {
		s.Logger.Error("Failed to get database info", "database", database, "error", err)
		return nil, fmt.Errorf("failed to get database info: %w", err)
	}

	// Get health result from HealthChecker
	healthResult := s.HealthChecker.Check(ctx, health.WithScope("database"), health.WithTarget(database))

	// Convert health result to HealthStatus
	status := s.convertHealthResultToStatus(healthResult, HealthScopeDatabase, database)

	// Add database-specific details
	status.Details["tables_count"] = dbInfo.TablesCount
	status.Details["partitions_count"] = dbInfo.PartitionsCount
	status.Details["tablets_count"] = dbInfo.TabletsCount
	status.Details["replicas_count"] = dbInfo.ReplicasCount
	status.Details["size_bytes"] = dbInfo.SizeBytes

	// Set timestamp and duration
	status.Timestamp = time.Now()
	status.Duration = time.Since(startTime)

	// Log status
	s.Logger.Info("Database health check completed",
		"database", database,
		"level", status.Level,
		"duration", status.Duration,
	)

	// Update metrics
	s.recordHealthMetrics(status)

	// Update health history
	s.updateHealthHistory(status)

	// Check for issues
	s.detectAndUpdateIssues(status)

	// Store last health status
	s.updateLastHealthStatus(status)

	// Send health event to subscribers
	s.notifyHealthSubscribers(status, "status_update")

	return status, nil
}

// GetTableHealth returns the health of a specific table
func (s *StarRocksHealthService) GetTableHealth(ctx context.Context, database, table string) (*HealthStatus, error) {
	startTime := time.Now()

	s.Logger.Debug("Getting table health", "database", database, "table", table)

	// Get tablet information for the table
	tableInfo, err := s.TabletManager.GetTableInfo(ctx, database, table)
	if err != nil {
		s.Logger.Error("Failed to get table info",
			"database", database,
			"table", table,
			"error", err,
		)
		return nil, fmt.Errorf("failed to get table info: %w", err)
	}

	// Create target string (database.table)
	target := fmt.Sprintf("%s.%s", database, table)

	// Get health result from HealthChecker
	healthResult := s.HealthChecker.Check(ctx,
		health.WithScope("table"),
		health.WithTarget(target),
	)

	// Convert health result to HealthStatus
	status := s.convertHealthResultToStatus(healthResult, HealthScopeTable, target)

	// Add table-specific details
	status.Details["partitions_count"] = tableInfo.PartitionsCount
	status.Details["tablets_count"] = tableInfo.TabletsCount
	status.Details["replicas_count"] = tableInfo.ReplicasCount
	status.Details["size_bytes"] = tableInfo.SizeBytes
	status.Details["row_count"] = tableInfo.RowCount
	status.Details["partition_type"] = tableInfo.PartitionType
	status.Details["distribution_type"] = tableInfo.DistributionType

	// Check tablet health
	tabletIssues := 0
	unavailableTablets := 0

	for _, tablet := range tableInfo.Tablets {
		if tablet.Status != "NORMAL" {
			tabletIssues++
		}

		if tablet.Status == "UNAVAILABLE" {
			unavailableTablets++
		}
	}

	status.Details["tablet_issues"] = tabletIssues
	status.Details["unavailable_tablets"] = unavailableTablets

	// Adjust health level based on tablet issues
	if unavailableTablets > 0 {
		status.Level = HealthLevelCritical
		status.Message = fmt.Sprintf("Table has %d unavailable tablets", unavailableTablets)
	} else if tabletIssues > 0 {
		if status.Level != HealthLevelCritical {
			status.Level = HealthLevelDegraded
			status.Message = fmt.Sprintf("Table has %d tablets with issues", tabletIssues)
		}
	}

	// Set timestamp and duration
	status.Timestamp = time.Now()
	status.Duration = time.Since(startTime)

	// Log status
	s.Logger.Info("Table health check completed",
		"database", database,
		"table", table,
		"level", status.Level,
		"duration", status.Duration,
	)

	// Update metrics
	s.recordHealthMetrics(status)

	// Update health history
	s.updateHealthHistory(status)

	// Check for issues
	s.detectAndUpdateIssues(status)

	// Store last health status
	s.updateLastHealthStatus(status)

	// Send health event to subscribers
	s.notifyHealthSubscribers(status, "status_update")

	return status, nil
}

// GetPartitionHealth returns the health of a specific partition
func (s *StarRocksHealthService) GetPartitionHealth(ctx context.Context, database, table, partition string) (*HealthStatus, error) {
	startTime := time.Now()

	s.Logger.Debug("Getting partition health",
		"database", database,
		"table", table,
		"partition", partition,
	)

	// Get tablet information for the partition
	partitionInfo, err := s.TabletManager.GetPartitionInfo(ctx, database, table, partition)
	if err != nil {
		s.Logger.Error("Failed to get partition info",
			"database", database,
			"table", table,
			"partition", partition,
			"error", err,
		)
		return nil, fmt.Errorf("failed to get partition info: %w", err)
	}

	// Create target string (database.table.partition)
	target := fmt.Sprintf("%s.%s.%s", database, table, partition)

	// Get health result from HealthChecker
	healthResult := s.HealthChecker.Check(ctx,
		health.WithScope("partition"),
		health.WithTarget(target),
	)

	// Convert health result to HealthStatus
	status := s.convertHealthResultToStatus(healthResult, HealthScopePartition, target)

	// Add partition-specific details
	status.Details["tablets_count"] = partitionInfo.TabletsCount
	status.Details["replicas_count"] = partitionInfo.ReplicasCount
	status.Details["size_bytes"] = partitionInfo.SizeBytes
	status.Details["row_count"] = partitionInfo.RowCount

	// Check tablet health
	tabletIssues := 0
	unavailableTablets := 0

	for _, tablet := range partitionInfo.Tablets {
		if tablet.Status != "NORMAL" {
			tabletIssues++
		}

		if tablet.Status == "UNAVAILABLE" {
			unavailableTablets++
		}
	}

	status.Details["tablet_issues"] = tabletIssues
	status.Details["unavailable_tablets"] = unavailableTablets

	// Adjust health level based on tablet issues
	if unavailableTablets > 0 {
		status.Level = HealthLevelCritical
		status.Message = fmt.Sprintf("Partition has %d unavailable tablets", unavailableTablets)
	} else if tabletIssues > 0 {
		if status.Level != HealthLevelCritical {
			status.Level = HealthLevelDegraded
			status.Message = fmt.Sprintf("Partition has %d tablets with issues", tabletIssues)
		}
	}

	// Set timestamp and duration
	status.Timestamp = time.Now()
	status.Duration = time.Since(startTime)

	// Log status
	s.Logger.Info("Partition health check completed",
		"database", database,
		"table", table,
		"partition", partition,
		"level", status.Level,
		"duration", status.Duration,
	)

	// Update metrics
	s.recordHealthMetrics(status)

	// Update health history
	s.updateHealthHistory(status)

	// Check for issues
	s.detectAndUpdateIssues(status)

	// Store last health status
	s.updateLastHealthStatus(status)

	// Send health event to subscribers
	s.notifyHealthSubscribers(status, "status_update")

	return status, nil
}

// GetTabletHealth returns the health of a specific tablet
func (s *StarRocksHealthService) GetTabletHealth(ctx context.Context, tabletID string) (*HealthStatus, error) {
	startTime := time.Now()

	s.Logger.Debug("Getting tablet health", "tablet_id", tabletID)

	// Get tablet information
	tabletInfo, err := s.TabletManager.GetTabletInfoByID(ctx, tabletID)
	if err != nil {
		s.Logger.Error("Failed to get tablet info", "tablet_id", tabletID, "error", err)
		return nil, fmt.Errorf("failed to get tablet info: %w", err)
	}

	// Create target string
	target := tabletID

	// Get health result from HealthChecker
	healthResult := s.HealthChecker.Check(ctx,
		health.WithScope("tablet"),
		health.WithTarget(target),
	)

	// Convert health result to HealthStatus
	status := s.convertHealthResultToStatus(healthResult, HealthScopeTablet, target)

	// Add tablet-specific details
	status.Details["database"] = tabletInfo.Database
	status.Details["table"] = tabletInfo.Table
	status.Details["partition"] = tabletInfo.Partition
	status.Details["replica_count"] = len(tabletInfo.Replicas)
	status.Details["size_bytes"] = tabletInfo.SizeBytes
	status.Details["row_count"] = tabletInfo.RowCount
	status.Details["status"] = tabletInfo.Status

	// Check replica health
	replicaIssues := 0
	unavailableReplicas := 0

	for _, replica := range tabletInfo.Replicas {
		if replica.Status != "NORMAL" {
			replicaIssues++
		}

		if replica.Status == "UNAVAILABLE" {
			unavailableReplicas++
		}
	}

	status.Details["replica_issues"] = replicaIssues
	status.Details["unavailable_replicas"] = unavailableReplicas

	// Adjust health level based on replica issues
	if unavailableReplicas > 0 {
		status.Level = HealthLevelCritical
		status.Message = fmt.Sprintf("Tablet has %d unavailable replicas", unavailableReplicas)
	} else if replicaIssues > 0 {
		if status.Level != HealthLevelCritical {
			status.Level = HealthLevelDegraded
			status.Message = fmt.Sprintf("Tablet has %d replicas with issues", replicaIssues)
		}
	}

	// If tablet status is not NORMAL, adjust health level
	if tabletInfo.Status != "NORMAL" {
		if tabletInfo.Status == "UNAVAILABLE" {
			status.Level = HealthLevelCritical
			status.Message = "Tablet is unavailable"
		} else {
			if status.Level != HealthLevelCritical {
				status.Level = HealthLevelDegraded
				status.Message = fmt.Sprintf("Tablet status is %s", tabletInfo.Status)
			}
		}
	}

	// Set timestamp and duration
	status.Timestamp = time.Now()
	status.Duration = time.Since(startTime)

	// Log status
	s.Logger.Info("Tablet health check completed",
		"tablet_id", tabletID,
		"level", status.Level,
		"duration", status.Duration,
	)

	// Update metrics
	s.recordHealthMetrics(status)

	// Update health history
	s.updateHealthHistory(status)

	// Check for issues
	s.detectAndUpdateIssues(status)

	// Store last health status
	s.updateLastHealthStatus(status)

	// Send health event to subscribers
	s.notifyHealthSubscribers(status, "status_update")

	return status, nil
}

// GetBackendHealth returns the health of a specific backend
func (s *StarRocksHealthService) GetBackendHealth(ctx context.Context, backendID string) (*HealthStatus, error) {
	startTime := time.Now()

	s.Logger.Debug("Getting backend health", "backend_id", backendID)

	// Get backend information
	backendInfo, err := s.TabletManager.GetBackendInfo(ctx, backendID)
	if err != nil {
		s.Logger.Error("Failed to get backend info", "backend_id", backendID, "error", err)
		return nil, fmt.Errorf("failed to get backend info: %w", err)
	}

	// Create target string
	target := backendID

	// Get health result from HealthChecker
	healthResult := s.HealthChecker.Check(ctx,
		health.WithScope("backend"),
		health.WithTarget(target),
	)

	// Convert health result to HealthStatus
	status := s.convertHealthResultToStatus(healthResult, HealthScopeBackend, target)

	// Add backend-specific details
	status.Details["host"] = backendInfo.Host
	status.Details["port"] = backendInfo.Port
	status.Details["alive"] = backendInfo.Alive
	status.Details["last_heartbeat"] = backendInfo.LastHeartbeat
	status.Details["tablet_count"] = backendInfo.TabletCount
	status.Details["capacity_bytes"] = backendInfo.CapacityBytes
	status.Details["used_bytes"] = backendInfo.UsedBytes
	status.Details["available_bytes"] = backendInfo.AvailableBytes
	status.Details["cpu_usage_percent"] = backendInfo.CPUUsagePercent
	status.Details["mem_usage_percent"] = backendInfo.MemUsagePercent
	status.Details["disk_usage_percent"] = backendInfo.DiskUsagePercent
	status.Details["tags"] = backendInfo.Tags

	// Calculate disk usage percentage
	diskUsagePercent := float64(0)
	if backendInfo.CapacityBytes > 0 {
		diskUsagePercent = float64(backendInfo.UsedBytes) / float64(backendInfo.CapacityBytes) * 100
	}

	// Adjust health level based on backend health
	if !backendInfo.Alive {
		status.Level = HealthLevelCritical
		status.Message = "Backend is not alive"
	} else if diskUsagePercent > 90 {
		status.Level = HealthLevelCritical
		status.Message = fmt.Sprintf("Backend disk usage is very high (%.2f%%)", diskUsagePercent)
	} else if diskUsagePercent > 80 {
		if status.Level != HealthLevelCritical {
			status.Level = HealthLevelDegraded
			status.Message = fmt.Sprintf("Backend disk usage is high (%.2f%%)", diskUsagePercent)
		}
	} else if diskUsagePercent > 70 {
		if status.Level != HealthLevelCritical && status.Level != HealthLevelDegraded {
			status.Level = HealthLevelWarning
			status.Message = fmt.Sprintf("Backend disk usage is elevated (%.2f%%)", diskUsagePercent)
		}
	}

	// Check CPU and memory usage
	if backendInfo.CPUUsagePercent > 90 {
		if status.Level != HealthLevelCritical {
			status.Level = HealthLevelDegraded
			status.Message = fmt.Sprintf("Backend CPU usage is very high (%.2f%%)", backendInfo.CPUUsagePercent)
		}
	} else if backendInfo.CPUUsagePercent > 80 {
		if status.Level != HealthLevelCritical && status.Level != HealthLevelDegraded {
			status.Level = HealthLevelWarning
			status.Message = fmt.Sprintf("Backend CPU usage is high (%.2f%%)", backendInfo.CPUUsagePercent)
		}
	}

	if backendInfo.MemUsagePercent > 90 {
		if status.Level != HealthLevelCritical {
			status.Level = HealthLevelDegraded
			status.Message = fmt.Sprintf("Backend memory usage is very high (%.2f%%)", backendInfo.MemUsagePercent)
		}
	} else if backendInfo.MemUsagePercent > 80 {
		if status.Level != HealthLevelCritical && status.Level != HealthLevelDegraded {
			status.Level = HealthLevelWarning
			status.Message = fmt.Sprintf("Backend memory usage is high (%.2f%%)", backendInfo.MemUsagePercent)
		}
	}

	// Set timestamp and duration
	status.Timestamp = time.Now()
	status.Duration = time.Since(startTime)

	// Log status
	s.Logger.Info("Backend health check completed",
		"backend_id", backendID,
		"level", status.Level,
		"duration", status.Duration,
	)

	// Update metrics
	s.recordHealthMetrics(status)

	// Update health history
	s.updateHealthHistory(status)

	// Check for issues
	s.detectAndUpdateIssues(status)

	// Store last health status
	s.updateLastHealthStatus(status)

	// Send health event to subscribers
	s.notifyHealthSubscribers(status, "status_update")

	return status, nil
}

// GetComponentHealth returns the health of a specific component
func (s *StarRocksHealthService) GetComponentHealth(ctx context.Context, component string) (*ComponentHealth, error) {
	s.Logger.Debug("Getting component health", "component", component)

	// Get health result from HealthChecker
	healthResult := s.HealthChecker.Check(ctx)

	// Find component health
	componentHealth, ok := healthResult.Components[component]
	if !ok {
		return nil, fmt.Errorf("component not found: %s", component)
	}

	// Convert to ComponentHealth
	health := &ComponentHealth{
		Name:      component,
		Level:     convertHealthStatusToLevel(componentHealth.Status),
		Message:   componentHealth.Message,
		Details:   componentHealth.Details,
		Timestamp: time.Now(),
	}

	return health, nil
}

// GetHealthHistory returns the health history for a target
func (s *StarRocksHealthService) GetHealthHistory(
	ctx context.Context,
	scope HealthScope,
	target string,
	limit int,
	since time.Time,
) (*HealthHistory, error) {
	s.Logger.Debug("Getting health history",
		"scope", scope,
		"target", target,
		"limit", limit,
		"since", since,
	)

	// Create history key
	historyKey := fmt.Sprintf("%s:%s", scope, target)

	// Get history entries
	s.historyMutex.RLock()
	entries, ok := s.healthHistory[historyKey]
	s.historyMutex.RUnlock()

	if !ok {
		// Return empty history
		return &HealthHistory{
			Target:  target,
			Scope:   scope,
			Entries: []HealthHistoryEntry{},
		}, nil
	}

	// Filter entries by time
	var filteredEntries []HealthHistoryEntry
	for _, entry := range entries {
		if entry.Timestamp.After(since) {
			filteredEntries = append(filteredEntries, entry)
		}
	}

	// Sort entries by timestamp (newest first)
	sort.Slice(filteredEntries, func(i, j int) bool {
		return filteredEntries[i].Timestamp.After(filteredEntries[j].Timestamp)
	})

	// Apply limit
	if limit > 0 && len(filteredEntries) > limit {
		filteredEntries = filteredEntries[:limit]
	}

	return &HealthHistory{
		Target:  target,
		Scope:   scope,
		Entries: filteredEntries,
	}, nil
}

// GetHealthDiagnosis returns a diagnosis of health issues for a target
func (s *StarRocksHealthService) GetHealthDiagnosis(
	ctx context.Context,
	scope HealthScope,
	target string,
) (*HealthDiagnosis, error) {
	s.Logger.Debug("Getting health diagnosis", "scope", scope, "target", target)

	// Get current health status
	var status *HealthStatus
	var err error

	switch scope {
	case HealthScopeCluster:
		status, err = s.GetClusterHealth(ctx)
	case HealthScopeDatabase:
		status, err = s.GetDatabaseHealth(ctx, target)
	case HealthScopeTable:
		parts := strings.Split(target, ".")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid table target format: %s", target)
		}
		status, err = s.GetTableHealth(ctx, parts[0], parts[1])
	case HealthScopePartition:
		parts := strings.Split(target, ".")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid partition target format: %s", target)
		}
		status, err = s.GetPartitionHealth(ctx, parts[0], parts[1], parts[2])
	case HealthScopeTablet:
		status, err = s.GetTabletHealth(ctx, target)
	case HealthScopeBackend:
		status, err = s.GetBackendHealth(ctx, target)
	default:
		return nil, fmt.Errorf("unsupported scope: %s", scope)
	}

	if err != nil {
		return nil, err
	}

	// Get health history
	history, err := s.GetHealthHistory(ctx, scope, target, 10, time.Now().Add(-24*time.Hour))
	if err != nil {
		s.Logger.Warn("Failed to get health history for diagnosis",
			"scope", scope,
			"target", target,
			"error", err,
		)
		// Continue with diagnosis anyway
	}

	// Create diagnosis
	diagnosis := &HealthDiagnosis{
		Target:            target,
		Scope:             scope,
		Level:             status.Level,
		Summary:           status.Message,
		Issues:            status.Issues,
		Recommendations:   status.Recommendations,
		RootCauses:        []string{},
		RelatedComponents: []string{},
		Timestamp:         time.Now(),
	}

	// Analyze issues and generate recommendations
	s.analyzeDiagnosis(diagnosis, status, history)

	return diagnosis, nil
}

// SubscribeHealthUpdates subscribes to health updates
func (s *StarRocksHealthService) SubscribeHealthUpdates(
	ctx context.Context,
	scopes []HealthScope,
	targets []string,
	levels []HealthLevel,
	eventTypes []string,
) (*HealthSubscription, error) {
	// Create subscription
	subscription := &HealthSubscription{
		ID:         generateSubscriptionID(),
		Scopes:     scopes,
		Targets:    targets,
		Levels:     levels,
		EventTypes: eventTypes,
		Channel:    make(chan HealthEvent, 100), // Buffered channel
		Created:    time.Now(),
	}

	// Store subscription
	s.subscriptionsMutex.Lock()
	s.subscriptions[subscription.ID] = subscription
	s.subscriptionsMutex.Unlock()

	s.Logger.Info("Health subscription created",
		"subscription_id", subscription.ID,
		"scopes", scopes,
		"targets", targets,
		"levels", levels,
		"event_types", eventTypes,
	)

	// Handle subscription cleanup when context is done
	go func() {
		<-ctx.Done()
		s.UnsubscribeHealthUpdates(context.Background(), subscription.ID)
	}()

	return subscription, nil
}

// UnsubscribeHealthUpdates unsubscribes from health updates
func (s *StarRocksHealthService) UnsubscribeHealthUpdates(
	ctx context.Context,
	subscriptionID string,
) error {
	s.subscriptionsMutex.Lock()
	defer s.subscriptionsMutex.Unlock()

	// Check if subscription exists
	subscription, ok := s.subscriptions[subscriptionID]
	if !ok {
		return fmt.Errorf("subscription not found: %s", subscriptionID)
	}

	// Close channel and delete subscription
	close(subscription.Channel)
	delete(s.subscriptions, subscriptionID)

	s.Logger.Info("Health subscription removed", "subscription_id", subscriptionID)

	return nil
}

// ReportHealthIssue reports a health issue
func (s *StarRocksHealthService) ReportHealthIssue(
	ctx context.Context,
	scope HealthScope,
	target string,
	level HealthLevel,
	message string,
	details map[string]interface{},
) error {
	s.Logger.Info("Health issue reported",
		"scope", scope,
		"target", target,
		"level", level,
		"message", message,
	)

	// Create issue ID
	issueID := generateIssueID(scope, target, message)

	// Check if issue already exists
	s.issuesMutex.Lock()
	defer s.issuesMutex.Unlock()

	existingIssue, exists := s.issues[issueID]

	if exists {
		// Update existing issue
		existingIssue.LastDetected = time.Now()
		existingIssue.Count++
		existingIssue.Level = level
		existingIssue.Message = message
		existingIssue.Status = "ongoing"
		s.issues[issueID] = existingIssue

		// Send health event
		event := HealthEvent{
			ID:        generateEventID(),
			Type:      "issue_updated",
			Level:     level,
			Scope:     scope,
			Target:    target,
			Message:   message,
			Timestamp: time.Now(),
			Details:   details,
		}

		s.notifyHealthSubscribersForEvent(event)
	} else {
		// Create new issue
		issue := HealthIssue{
			ID:            issueID,
			Level:         level,
			Message:       message,
			Component:     scope.String() + ":" + target,
			FirstDetected: time.Now(),
			LastDetected:  time.Now(),
			Count:         1,
			Status:        "new",
		}

		s.issues[issueID] = issue

		// Send health event
		event := HealthEvent{
			ID:        generateEventID(),
			Type:      "issue_detected",
			Level:     level,
			Scope:     scope,
			Target:    target,
			Message:   message,
			Timestamp: time.Now(),
			Details:   details,
		}

		s.notifyHealthSubscribersForEvent(event)
	}

	// Update issue count metrics
	if s.healthIssuesGauge != nil {
		s.updateIssueCountMetrics()
	}

	return nil
}

// convertHealthResultToStatus converts a health.HealthResult to a HealthStatus
func (s *StarRocksHealthService) convertHealthResultToStatus(
	healthResult health.HealthResult,
	scope HealthScope,
	target string,
) *HealthStatus {
	// Create components map
	components := make(map[string]*ComponentHealth)
	for name, component := range healthResult.Components {
		components[name] = &ComponentHealth{
			Name:      name,
			Level:     convertHealthStatusToLevel(component.Status),
			Message:   component.Message,
			Details:   component.Details,
			Timestamp: time.Now(),
		}
	}

	// Determine overall health level
	level := convertHealthStatusToLevel(healthResult.Status)

	// Create HealthStatus
	status := &HealthStatus{
		Level:           level,
		Scope:           scope,
		Target:          target,
		Message:         healthResult.Message,
		Components:      components,
		Metrics:         make(map[string]float64),
		Details:         make(map[string]interface{}),
		Timestamp:       time.Now(),
		Duration:        0, // Will be set by caller
		Issues:          []HealthIssue{},
		Recommendations: []string{},
	}

	// Add relevant issues
	s.addRelevantIssues(status)

	// Generate recommendations
	s.generateRecommendations(status)

	return status
}

// addRelevantIssues adds relevant issues to a health status
func (s *StarRocksHealthService) addRelevantIssues(status *HealthStatus) {
	// Get issues that are relevant to this scope and target
	s.issuesMutex.RLock()
	defer s.issuesMutex.RUnlock()

	for _, issue := range s.issues {
		// Skip resolved issues
		if issue.Status == "resolved" {
			continue
		}

		// Check if issue is relevant to this status
		if strings.HasPrefix(issue.Component, string(status.Scope)+":"+status.Target) {
			status.Issues = append(status.Issues, issue)
		}
	}
}

// generateRecommendations generates recommendations for a health status
func (s *StarRocksHealthService) generateRecommendations(status *HealthStatus) {
	// Generate recommendations based on health level and issues
	recommendations := []string{}

	switch status.Level {
	case HealthLevelCritical:
		recommendations = append(recommendations, "Investigate and resolve critical issues immediately")

		// Add component-specific recommendations
		for name, component := range status.Components {
			if component.Level == HealthLevelCritical {
				recommendations = append(recommendations, fmt.Sprintf("Check component %s: %s", name, component.Message))
			}
		}

		// Add issue-specific recommendations
		for _, issue := range status.Issues {
			if issue.Level == HealthLevelCritical {
				recommendations = append(recommendations, fmt.Sprintf("Resolve issue: %s", issue.Message))
			}
		}

		// Add scope-specific recommendations
		switch status.Scope {
		case HealthScopeCluster:
			recommendations = append(recommendations, "Check cluster configuration and resource allocation")
		case HealthScopeDatabase:
			recommendations = append(recommendations, "Check database schema and configuration")
		case HealthScopeTable:
			recommendations = append(recommendations, "Check table partitioning and distribution")
		case HealthScopePartition:
			recommendations = append(recommendations, "Check partition balance across backends")
		case HealthScopeTablet:
			recommendations = append(recommendations, "Check tablet replicas and consistency")
		case HealthScopeBackend:
			recommendations = append(recommendations, "Check backend resources and connectivity")
		}

	case HealthLevelDegraded:
		recommendations = append(recommendations, "Address degraded performance issues")

		// Add component-specific recommendations
		for name, component := range status.Components {
			if component.Level == HealthLevelDegraded {
				recommendations = append(recommendations, fmt.Sprintf("Improve component %s: %s", name, component.Message))
			}
		}

	case HealthLevelWarning:
		recommendations = append(recommendations, "Monitor warnings for potential issues")

	case HealthLevelNormal:
		recommendations = append(recommendations, "Continue monitoring for optimal performance")
	}

	// Add general recommendations
	if len(status.Issues) > 0 {
		recommendations = append(recommendations, fmt.Sprintf("Address %d outstanding issues", len(status.Issues)))
	}

	status.Recommendations = recommendations
}

// analyzeDiagnosis analyzes health status and history to enhance a diagnosis
func (s *StarRocksHealthService) analyzeDiagnosis(
	diagnosis *HealthDiagnosis,
	status *HealthStatus,
	history *HealthHistory,
) {
	// Add root causes based on component health
	for name, component := range status.Components {
		if component.Level == HealthLevelCritical || component.Level == HealthLevelDegraded {
			diagnosis.RelatedComponents = append(diagnosis.RelatedComponents, name)
			diagnosis.RootCauses = append(diagnosis.RootCauses,
				fmt.Sprintf("%s component issue: %s", name, component.Message))
		}
	}

	// Analyze health history for patterns
	if history != nil && len(history.Entries) > 0 {
		// Check for recurring issues
		levelCounts := make(map[HealthLevel]int)
		for _, entry := range history.Entries {
			levelCounts[entry.Level]++
		}

		// If there are multiple non-normal entries in history, there might be a recurring issue
		if levelCounts[HealthLevelCritical] > 1 || levelCounts[HealthLevelDegraded] > 1 {
			diagnosis.RootCauses = append(diagnosis.RootCauses,
				"Recurring health issues detected, may indicate systemic problems")
		}

		// Check for recent degradation
		if len(history.Entries) >= 2 {
			latest := history.Entries[0]
			previous := history.Entries[1]

			if levelIsWorse(latest.Level, previous.Level) {
				diagnosis.RootCauses = append(diagnosis.RootCauses,
					fmt.Sprintf("Recent degradation from %s to %s", previous.Level, latest.Level))
			}
		}
	}

	// Add scope-specific analysis
	switch diagnosis.Scope {
	case HealthScopeCluster:
		// Check for backend issues
		backendIssues := false
		for name, component := range status.Components {
			if strings.HasPrefix(name, "backend") &&
				(component.Level == HealthLevelCritical || component.Level == HealthLevelDegraded) {
				backendIssues = true
				break
			}
		}

		if backendIssues {
			diagnosis.RootCauses = append(diagnosis.RootCauses,
				"One or more backends are experiencing issues")
			diagnosis.Recommendations = append(diagnosis.Recommendations,
				"Check backend servers for resource constraints or connectivity issues")
		}

	case HealthScopeTable:
		// Check for partition balance issues
		if partitionCount, ok := status.Details["partitions_count"].(int); ok && partitionCount > 0 {
			if tabletIssues, ok := status.Details["tablet_issues"].(int); ok && tabletIssues > 0 {
				diagnosis.RootCauses = append(diagnosis.RootCauses,
					fmt.Sprintf("Table has %d tablets with issues across its partitions", tabletIssues))
				diagnosis.Recommendations = append(diagnosis.Recommendations,
					"Check partition distribution and balance")
			}
		}

	case HealthScopeBackend:
		// Check for resource issues
		if diskUsage, ok := status.Details["disk_usage_percent"].(float64); ok && diskUsage > 80 {
			diagnosis.RootCauses = append(diagnosis.RootCauses,
				fmt.Sprintf("High disk usage (%.2f%%)", diskUsage))
			diagnosis.Recommendations = append(diagnosis.Recommendations,
				"Free up disk space or add more storage")
		}

		if cpuUsage, ok := status.Details["cpu_usage_percent"].(float64); ok && cpuUsage > 80 {
			diagnosis.RootCauses = append(diagnosis.RootCauses,
				fmt.Sprintf("High CPU usage (%.2f%%)", cpuUsage))
			diagnosis.Recommendations = append(diagnosis.Recommendations,
				"Check for CPU-intensive queries or add more CPU resources")
		}

		if memUsage, ok := status.Details["mem_usage_percent"].(float64); ok && memUsage > 80 {
			diagnosis.RootCauses = append(diagnosis.RootCauses,
				fmt.Sprintf("High memory usage (%.2f%%)", memUsage))
			diagnosis.Recommendations = append(diagnosis.Recommendations,
				"Check for memory leaks or add more memory")
		}
	}

	// Add general recommendations based on health level
	switch diagnosis.Level {
	case HealthLevelCritical:
		diagnosis.Recommendations = append(diagnosis.Recommendations,
			"Investigate and resolve critical issues immediately",
			"Consider reaching out to StarRocks support for assistance")

	case HealthLevelDegraded:
		diagnosis.Recommendations = append(diagnosis.Recommendations,
			"Address performance issues to restore optimal operation",
			"Monitor metrics closely to track improvements")

	case HealthLevelWarning:
		diagnosis.Recommendations = append(diagnosis.Recommendations,
			"Monitor the situation for potential degradation",
			"Proactively address warnings before they become issues")
	}

	// Remove duplicates from recommendations and root causes
	diagnosis.Recommendations = removeDuplicates(diagnosis.Recommendations)
	diagnosis.RootCauses = removeDuplicates(diagnosis.RootCauses)
	diagnosis.RelatedComponents = removeDuplicates(diagnosis.RelatedComponents)
}

// startHealthMonitoring starts background health monitoring
func (s *StarRocksHealthService) startHealthMonitoring() {
	// Get monitoring interval from config
	interval := s.Config.Health.MonitoringInterval
	if interval <= 0 {
		interval = 60 * time.Second // Default to 1 minute
	}

	s.Logger.Info("Starting health monitoring", "interval", interval)

	// Start monitoring goroutine
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Create background context
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

				// Check cluster health
				_, err := s.GetClusterHealth(ctx)
				if err != nil {
					s.Logger.Error("Failed to check cluster health", "error", err)
				}

				// Check databases health
				// Note: In a real implementation, you would get the list of databases
				// For now, we'll use a placeholder
				// databases := []string{"db1", "db2"}
				// for _, db := range databases {
				//     _, err := s.GetDatabaseHealth(ctx, db)
				//     if err != nil {
				//         s.Logger.Error("Failed to check database health", "database", db, "error", err)
				//     }
				// }

				cancel()
			}
		}
	}()
}

// updateHealthHistory updates the health history for a target
func (s *StarRocksHealthService) updateHealthHistory(status *HealthStatus) {
	// Create history key
	historyKey := fmt.Sprintf("%s:%s", status.Scope, status.Target)

	// Create history entry
	entry := HealthHistoryEntry{
		Timestamp: status.Timestamp,
		Level:     status.Level,
		Message:   status.Message,
	}

	// Update history
	s.historyMutex.Lock()
	defer s.historyMutex.Unlock()

	// Get existing entries or create new slice
	entries, ok := s.healthHistory[historyKey]
	if !ok {
		entries = []HealthHistoryEntry{}
	}

	// Set duration for previous entry if it exists
	if len(entries) > 0 {
		entries[0].Duration = status.Timestamp.Sub(entries[0].Timestamp)
	}

	// Add new entry at the beginning
	entries = append([]HealthHistoryEntry{entry}, entries...)

	// Limit history size
	maxHistorySize := 100
	if len(entries) > maxHistorySize {
		entries = entries[:maxHistorySize]
	}

	// Update history
	s.healthHistory[historyKey] = entries
}

// detectAndUpdateIssues detects and updates health issues
func (s *StarRocksHealthService) detectAndUpdateIssues(status *HealthStatus) {
	// Skip if health is normal
	if status.Level == HealthLevelNormal {
		return
	}

	// Report issue
	s.ReportHealthIssue(
		context.Background(),
		status.Scope,
		status.Target,
		status.Level,
		status.Message,
		status.Details,
	)

	// Check components for issues
	for name, component := range status.Components {
		if component.Level != HealthLevelNormal {
			s.ReportHealthIssue(
				context.Background(),
				HealthScopeComponent,
				name,
				component.Level,
				component.Message,
				component.Details,
			)
		}
	}
}

// notifyHealthSubscribers notifies health subscribers about a status change
func (s *StarRocksHealthService) notifyHealthSubscribers(status *HealthStatus, eventType string) {
	// Get previous status
	previousStatus := s.getLastHealthStatus(status.Scope, status.Target)

	// Create event
	event := HealthEvent{
		ID:        generateEventID(),
		Type:      eventType,
		Level:     status.Level,
		Scope:     status.Scope,
		Target:    status.Target,
		Message:   status.Message,
		Timestamp: status.Timestamp,
		Details:   status.Details,
	}

	// Add previous level if it exists and is different
	if previousStatus != nil && previousStatus.Level != status.Level {
		event.PreviousLevel = previousStatus.Level

		// Change event type to status_change if the level changed
		event.Type = "status_change"
	}

	// Notify subscribers
	s.notifyHealthSubscribersForEvent(event)
}

// notifyHealthSubscribersForEvent notifies health subscribers about an event
func (s *StarRocksHealthService) notifyHealthSubscribersForEvent(event HealthEvent) {
	s.subscriptionsMutex.RLock()
	defer s.subscriptionsMutex.RUnlock()

	// Send event to matching subscribers
	for _, subscription := range s.subscriptions {
		// Check if subscription matches event
		if s.eventMatchesSubscription(event, subscription) {
			// Send event to subscriber's channel or callback
			if subscription.Channel != nil {
				select {
				case subscription.Channel <- event:
					// Event sent
				default:
					// Channel full, log warning
					s.Logger.Warn("Health event channel full, dropping event",
						"subscription_id", subscription.ID,
						"event_type", event.Type,
					)
				}
			}

			if subscription.CallbackFunc != nil {
				// Call callback in a goroutine to avoid blocking
				go subscription.CallbackFunc(event)
			}
		}
	}
}

// eventMatchesSubscription checks if an event matches a subscription
func (s *StarRocksHealthService) eventMatchesSubscription(
	event HealthEvent,
	subscription *HealthSubscription,
) bool {
	// Check scopes
	if len(subscription.Scopes) > 0 {
		scopeMatches := false
		for _, scope := range subscription.Scopes {
			if scope == event.Scope {
				scopeMatches = true
				break
			}
		}
		if !scopeMatches {
			return false
		}
	}

	// Check targets
	if len(subscription.Targets) > 0 {
		targetMatches := false
		for _, target := range subscription.Targets {
			if target == event.Target || target == "*" {
				targetMatches = true
				break
			}
		}
		if !targetMatches {
			return false
		}
	}

	// Check levels
	if len(subscription.Levels) > 0 {
		levelMatches := false
		for _, level := range subscription.Levels {
			if level == event.Level {
				levelMatches = true
				break
			}
		}
		if !levelMatches {
			return false
		}
	}

	// Check event types
	if len(subscription.EventTypes) > 0 {
		typeMatches := false
		for _, eventType := range subscription.EventTypes {
			if eventType == event.Type || eventType == "*" {
				typeMatches = true
				break
			}
		}
		if !typeMatches {
			return false
		}
	}

	return true
}

// recordHealthMetrics records metrics for a health status
func (s *StarRocksHealthService) recordHealthMetrics(status *HealthStatus) {
	// Convert health level to a numeric value
	var levelValue float64
	switch status.Level {
	case HealthLevelNormal:
		levelValue = 1
	case HealthLevelWarning:
		levelValue = 2
	case HealthLevelDegraded:
		levelValue = 3
	case HealthLevelCritical:
		levelValue = 4
	default: // HealthLevelUnknown
		levelValue = 0
	}

	// Record health status metric
	if s.healthStatusGauge != nil {
		s.healthStatusGauge.WithLabelValues(string(status.Scope), status.Target).Set(levelValue)
	}

	// Record health check duration
	if s.healthCheckDuration != nil {
		s.healthCheckDuration.WithLabelValues(string(status.Scope), status.Target).Observe(status.Duration.Seconds())
	}

	// Record through metrics recorder interface
	if s.Metrics != nil {
		s.Metrics.GaugeSet("health_status", levelValue, map[string]string{
			"scope":  string(status.Scope),
			"target": status.Target,
		})

		s.Metrics.HistogramObserve("health_check_duration_ms",
			float64(status.Duration.Milliseconds()),
			map[string]string{
				"scope":  string(status.Scope),
				"target": status.Target,
			})
	}
}

// updateIssueCountMetrics updates metrics for issue counts
func (s *StarRocksHealthService) updateIssueCountMetrics() {
	// Reset issue count metrics
	if s.healthIssuesGauge != nil {
		s.healthIssuesGauge.Reset()
	}

	// Count issues by scope, target, and level
	issueCounts := make(map[string]map[string]map[HealthLevel]int)

	s.issuesMutex.RLock()
	defer s.issuesMutex.RUnlock()

	for _, issue := range s.issues {
		// Skip resolved issues
		if issue.Status == "resolved" {
			continue
		}

		// Parse component to get scope and target
		parts := strings.SplitN(issue.Component, ":", 2)
		if len(parts) != 2 {
			continue
		}

		scope := parts[0]
		target := parts[1]

		// Initialize maps if needed
		if _, ok := issueCounts[scope]; !ok {
			issueCounts[scope] = make(map[string]map[HealthLevel]int)
		}

		if _, ok := issueCounts[scope][target]; !ok {
			issueCounts[scope][target] = make(map[HealthLevel]int)
		}

		// Increment count
		issueCounts[scope][target][issue.Level]++
	}

	// Update metrics
	if s.healthIssuesGauge != nil {
		for scope, targets := range issueCounts {
			for target, levels := range targets {
				for level, count := range levels {
					s.healthIssuesGauge.WithLabelValues(scope, target, string(level)).Set(float64(count))
				}
			}
		}
	}

	// Record through metrics recorder interface
	if s.Metrics != nil {
		for scope, targets := range issueCounts {
			for target, levels := range targets {
				for level, count := range levels {
					s.Metrics.GaugeSet("health_issues", float64(count), map[string]string{
						"scope":  scope,
						"target": target,
						"level":  string(level),
					})
				}
			}
		}
	}
}

// updateLastHealthStatus updates the last known health status for a target
func (s *StarRocksHealthService) updateLastHealthStatus(status *HealthStatus) {
	// Create key
	key := fmt.Sprintf("%s:%s", status.Scope, status.Target)

	// Update last status
	s.lastHealthStatusMutex.Lock()
	defer s.lastHealthStatusMutex.Unlock()

	s.lastHealthStatus[key] = status
}

// getLastHealthStatus gets the last known health status for a target
func (s *StarRocksHealthService) getLastHealthStatus(scope HealthScope, target string) *HealthStatus {
	// Create key
	key := fmt.Sprintf("%s:%s", scope, target)

	// Get last status
	s.lastHealthStatusMutex.RLock()
	defer s.lastHealthStatusMutex.RUnlock()

	return s.lastHealthStatus[key]
}

// Helper functions

// convertHealthStatusToLevel converts a health.Status to a HealthLevel
func convertHealthStatusToLevel(status health.Status) HealthLevel {
	switch status {
	case health.StatusHealthy:
		return HealthLevelNormal
	case health.StatusDegraded:
		return HealthLevelDegraded
	case health.StatusUnhealthy:
		return HealthLevelCritical
	default:
		return HealthLevelUnknown
	}
}

// levelIsWorse checks if level1 is worse than level2
func levelIsWorse(level1 HealthLevel, level2 HealthLevel) bool {
	levelValues := map[HealthLevel]int{
		HealthLevelNormal:   1,
		HealthLevelWarning:  2,
		HealthLevelDegraded: 3,
		HealthLevelCritical: 4,
		HealthLevelUnknown:  0,
	}

	return levelValues[level1] > levelValues[level2]
}

// String returns the string representation of a HealthScope
func (h HealthScope) String() string {
	return string(h)
}

// generateSubscriptionID generates a unique subscription ID
func generateSubscriptionID() string {
	return fmt.Sprintf("sub-%d-%x", time.Now().UnixNano(), randomBytes(4))
}

// generateEventID generates a unique event ID
func generateEventID() string {
	return fmt.Sprintf("evt-%d-%x", time.Now().UnixNano(), randomBytes(4))
}

// generateIssueID generates a unique issue ID
func generateIssueID(scope HealthScope, target string, message string) string {
	// Create a unique identifier based on scope, target, and message
	hash := hash(fmt.Sprintf("%s:%s:%s", scope, target, message))
	return fmt.Sprintf("iss-%s", hash)
}

// hash creates a simple hash of a string
func hash(s string) string {
	h := 0
	for _, c := range s {
		h = 31*h + int(c)
	}
	return fmt.Sprintf("%x", h)
}

// randomBytes generates random bytes
func randomBytes(n int) []byte {
	b := make([]byte, n)
	// In a real implementation, use crypto/rand
	// For simplicity, we're using a placeholder
	for i := range b {
		b[i] = byte(time.Now().Nanosecond() % 256)
		time.Sleep(1 * time.Nanosecond)
	}
	return b
}

// removeDuplicates removes duplicate strings from a slice
func removeDuplicates(slice []string) []string {
	keys := make(map[string]bool)
	result := []string{}

	for _, entry := range slice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			result = append(result, entry)
		}
	}

	return result
}

//Personal.AI order the ending
