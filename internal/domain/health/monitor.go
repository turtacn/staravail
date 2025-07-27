// Package health defines interfaces and implementations for health monitoring.
package health

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/turtacn/staravail/internal/common/errors"
	"github.com/turtacn/staravail/internal/common/logger"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/domain/tablet"
	"github.com/turtacn/staravail/internal/infrastructure/config"
)

// HealthState represents the overall health state of the StarRocks cluster
type HealthState string

const (
	// HealthStateUnknown indicates the health state is unknown
	HealthStateUnknown HealthState = "UNKNOWN"
	// HealthStateHealthy indicates the cluster is healthy
	HealthStateHealthy HealthState = "HEALTHY"
	// HealthStateDegraded indicates the cluster is degraded but still functional
	HealthStateDegraded HealthState = "DEGRADED"
	// HealthStateUnhealthy indicates the cluster is unhealthy
	HealthStateUnhealthy HealthState = "UNHEALTHY"
	// HealthStateCritical indicates the cluster is in a critical state
	HealthStateCritical HealthState = "CRITICAL"
)

// BackendHealth represents the health information for a backend node
type BackendHealth struct {
	// BackendID is the unique ID of the backend
	BackendID int64
	// Host is the hostname or IP address of the backend
	Host string
	// HeartbeatPort is the heartbeat port of the backend
	HeartbeatPort int
	// BePort is the backend port
	BePort int
	// HttpPort is the HTTP port of the backend
	HttpPort int
	// BrpcPort is the brpc port of the backend
	BrpcPort int
	// LastStartTime is when the backend was last started
	LastStartTime time.Time
	// LastHeartbeat is when the last heartbeat was received
	LastHeartbeat time.Time
	// Alive indicates if the backend is alive
	Alive bool
	// SystemDecommissioned indicates if the backend is decommissioned by the system
	SystemDecommissioned bool
	// ClusterDecommissioned indicates if the backend is decommissioned by the cluster
	ClusterDecommissioned bool
	// TabletCount is the number of tablets on this backend
	TabletCount int
	// DataUsedCapacity is the used data capacity in bytes
	DataUsedCapacity int64
	// AvailCapacity is the available capacity in bytes
	AvailCapacity int64
	// TotalCapacity is the total capacity in bytes
	TotalCapacity int64
	// UsedPct is the percentage of capacity used
	UsedPct float64
	// MaxDiskUsedPct is the maximum disk usage percentage
	MaxDiskUsedPct float64
	// Tag is the tag of the backend
	Tag string
	// ErrMsg is any error message associated with this backend
	ErrMsg string
	// Status is the status of the backend
	Status string
	// NodeRole is the role of the node
	NodeRole string
}

// TabletDistribution represents the distribution of tablets
type TabletDistribution struct {
	// DatabaseID is the ID of the database
	DatabaseID int64
	// DatabaseName is the name of the database
	DatabaseName string
	// TableID is the ID of the table
	TableID int64
	// TableName is the name of the table
	TableName string
	// PartitionID is the ID of the partition
	PartitionID int64
	// PartitionName is the name of the partition
	PartitionName string
	// IndexID is the ID of the index
	IndexID int64
	// IndexName is the name of the index
	IndexName string
	// TabletID is the ID of the tablet
	TabletID int64
	// ReplicaID is the ID of the replica
	ReplicaID int64
	// BackendID is the ID of the backend
	BackendID int64
	// Schema is the schema hash
	SchemaHash int
	// Version is the version
	Version int
	// VersionHash is the version hash
	VersionHash int64
	// RowCount is the number of rows
	RowCount int64
	// DataSize is the size of data in bytes
	DataSize int64
	// State is the state of the tablet
	State string
	// LastFailure is the last failure message
	LastFailure string
	// LastSuccess is the last success message
	LastSuccess string
}

// ClusterHealthMetrics represents health metrics for the entire cluster
type ClusterHealthMetrics struct {
	// TotalBackends is the total number of backends
	TotalBackends int
	// HealthyBackends is the number of healthy backends
	HealthyBackends int
	// UnhealthyBackends is the number of unhealthy backends
	UnhealthyBackends int
	// DecommissionedBackends is the number of decommissioned backends
	DecommissionedBackends int
	// TotalFrontends is the total number of frontends
	TotalFrontends int
	// HealthyFrontends is the number of healthy frontends
	HealthyFrontends int
	// TotalTablets is the total number of tablets
	TotalTablets int
	// HealthyTablets is the number of healthy tablets
	HealthyTablets int
	// UnhealthyTablets is the number of unhealthy tablets
	UnhealthyTablets int
	// AverageBackendUsage is the average backend disk usage percentage
	AverageBackendUsage float64
	// MaxBackendUsage is the maximum backend disk usage percentage
	MaxBackendUsage float64
	// AverageTabletReplicas is the average number of replicas per tablet
	AverageTabletReplicas float64
	// UnderReplicatedTablets is the number of tablets with fewer than desired replicas
	UnderReplicatedTablets int
	// OverReplicatedTablets is the number of tablets with more than desired replicas
	OverReplicatedTablets int
	// BalanceScore is a score indicating how well tablets are balanced across backends
	BalanceScore float64
	// LastUpdated is when these metrics were last updated
	LastUpdated time.Time
}

// MonitorConfig represents configuration for the health monitor
type MonitorConfig struct {
	// BackendCheckInterval is how often to check backend health
	BackendCheckInterval time.Duration
	// TabletCheckInterval is how often to check tablet distribution
	TabletCheckInterval time.Duration
	// RequestTimeout is the timeout for StarRocks API requests
	RequestTimeout time.Duration
	// RetryCount is the number of times to retry failed requests
	RetryCount int
	// RetryInterval is the interval between retries
	RetryInterval time.Duration
	// UnhealthyBackendThreshold is the percentage of backends that can be unhealthy before the cluster is considered degraded
	UnhealthyBackendThreshold float64
	// CriticalBackendThreshold is the percentage of backends that can be unhealthy before the cluster is considered critical
	CriticalBackendThreshold float64
	// TabletSampleSize is the number of tablets to sample for distribution checks
	TabletSampleSize int
	// EnableAlerts indicates whether to enable alerts
	EnableAlerts bool
	// AlertCooldown is the cooldown period between alerts
	AlertCooldown time.Duration
}

// HealthAlert represents a health alert
type HealthAlert struct {
	// Level is the alert level
	Level string
	// Message is the alert message
	Message string
	// Source is what component triggered the alert
	Source string
	// Timestamp is when the alert was generated
	Timestamp time.Time
	// Metrics contains any metrics related to the alert
	Metrics map[string]interface{}
}

// HealthStateEvent represents an event for health state changes
type HealthStateEvent struct {
	// PreviousState is the previous health state
	PreviousState HealthState
	// CurrentState is the current health state
	CurrentState HealthState
	// Reason explains why the state changed
	Reason string
	// Timestamp is when the state change occurred
	Timestamp time.Time
	// Metrics contains health metrics at the time of the event
	Metrics ClusterHealthMetrics
}

// BackendStateEvent represents an event for backend state changes
type BackendStateEvent struct {
	// BackendID is the ID of the backend
	BackendID int64
	// Host is the hostname of the backend
	Host string
	// PreviousAlive indicates if the backend was previously alive
	PreviousAlive bool
	// CurrentAlive indicates if the backend is currently alive
	CurrentAlive bool
	// Timestamp is when the state change occurred
	Timestamp time.Time
}

// HealthMonitor defines the interface for health monitoring
type HealthMonitor interface {
	// StartMonitoring starts the health monitoring process
	StartMonitoring(ctx context.Context) error

	// StopMonitoring stops the health monitoring process
	StopMonitoring() error

	// GetHealthState returns the current health state of the cluster
	GetHealthState() HealthState

	// GetClusterHealthMetrics returns the current health metrics of the cluster
	GetClusterHealthMetrics() ClusterHealthMetrics

	// GetBackendHealth returns the health information for a specific backend
	GetBackendHealth(backendID int64) (BackendHealth, error)

	// GetAllBackendHealth returns the health information for all backends
	GetAllBackendHealth() ([]BackendHealth, error)

	// GetTabletDistribution returns the distribution of tablets
	GetTabletDistribution(ctx context.Context, databaseID, tableID int64) ([]TabletDistribution, error)

	// GetRecentAlerts returns recent health alerts
	GetRecentAlerts(maxCount int) []HealthAlert

	// SubscribeToHealthStateChanges subscribes to health state change events
	SubscribeToHealthStateChanges() (string, <-chan HealthStateEvent)

	// UnsubscribeFromHealthStateChanges unsubscribes from health state change events
	UnsubscribeFromHealthStateChanges(subscriptionID string) bool

	// SubscribeToBackendStateChanges subscribes to backend state change events
	SubscribeToBackendStateChanges() (string, <-chan BackendStateEvent)

	// UnsubscribeFromBackendStateChanges unsubscribes from backend state change events
	UnsubscribeFromBackendStateChanges(subscriptionID string) bool

	// ForceRefresh forces an immediate refresh of health data
	ForceRefresh(ctx context.Context) error
}

// HealthMonitorImpl implements the HealthMonitor interface
type HealthMonitorImpl struct {
	// config is the monitor configuration
	config MonitorConfig
	// starrocksClient is the client for interacting with StarRocks
	starrocksClient client.StarRocksClient
	// tabletManager is used to update tablet states
	tabletManager tablet.TabletManager
	// eventBus is used to publish events
	eventBus events.EventBus
	// logger is used for logging
	logger logging.Logger
	// metrics is used to record metrics
	metrics metrics.MetricsRecorder
	// alertProvider is used to send alerts
	alertProvider AlertProvider

	// currentState is the current health state of the cluster
	currentState HealthState
	// currentMetrics is the current health metrics of the cluster
	currentMetrics ClusterHealthMetrics
	// backends is the current backend health information
	backends []BackendHealth
	// lastBackendStates maps backend IDs to their last known alive state
	lastBackendStates map[int64]bool
	// recentAlerts contains recent health alerts
	recentAlerts []HealthAlert
	// lastAlertTime is when the last alert was sent
	lastAlertTime time.Time

	// healthStateSubscriptions maps subscription IDs to health state channels
	healthStateSubscriptions map[string]chan HealthStateEvent
	// backendStateSubscriptions maps subscription IDs to backend state channels
	backendStateSubscriptions map[string]chan BackendStateEvent

	// feAddresses is the list of FE addresses to try
	feAddresses []string
	// currentFEIndex is the index of the current FE being used
	currentFEIndex int

	// monitoring indicates if monitoring is currently active
	monitoring bool
	// cancel is used to cancel the monitoring context
	cancel context.CancelFunc
	// wg is used to wait for all monitoring goroutines to finish
	wg sync.WaitGroup
	// mutex protects the shared state
	mutex sync.RWMutex
	// subMutex protects the subscription maps
	subMutex sync.RWMutex
	// alertMutex protects the alerts state
	alertMutex sync.RWMutex
}

// AlertProvider defines the interface for sending alerts
type AlertProvider interface {
	// SendAlert sends an alert
	SendAlert(alert HealthAlert) error
}

// NewHealthMonitor creates a new HealthMonitor instance
func NewHealthMonitor(
	cfg config.HealthMonitorConfig,
	starrocksClient client.StarRocksClient,
	tabletManager tablet.TabletManager,
	eventBus events.EventBus,
	logger logging.Logger,
	metrics metrics.MetricsRecorder,
	alertProvider AlertProvider,
) HealthMonitor {
	// Convert config to internal monitor config
	monitorConfig := MonitorConfig{
		BackendCheckInterval:      cfg.BackendCheckInterval,
		TabletCheckInterval:       cfg.TabletCheckInterval,
		RequestTimeout:            cfg.RequestTimeout,
		RetryCount:                cfg.RetryCount,
		RetryInterval:             cfg.RetryInterval,
		UnhealthyBackendThreshold: cfg.UnhealthyBackendThreshold,
		CriticalBackendThreshold:  cfg.CriticalBackendThreshold,
		TabletSampleSize:          cfg.TabletSampleSize,
		EnableAlerts:              cfg.EnableAlerts,
		AlertCooldown:             cfg.AlertCooldown,
	}

	return &HealthMonitorImpl{
		config:                    monitorConfig,
		starrocksClient:           starrocksClient,
		tabletManager:             tabletManager,
		eventBus:                  eventBus,
		logger:                    logger,
		metrics:                   metrics,
		alertProvider:             alertProvider,
		currentState:              HealthStateUnknown,
		lastBackendStates:         make(map[int64]bool),
		recentAlerts:              make([]HealthAlert, 0, 100),
		healthStateSubscriptions:  make(map[string]chan HealthStateEvent),
		backendStateSubscriptions: make(map[string]chan BackendStateEvent),
		feAddresses:               cfg.FrontendAddresses,
		monitoring:                false,
	}
}

// StartMonitoring starts the health monitoring process
func (hm *HealthMonitorImpl) StartMonitoring(ctx context.Context) error {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	if hm.monitoring {
		return errors.NewInvalidStateError("Health monitoring is already running")
	}

	if len(hm.feAddresses) == 0 {
		return errors.NewInvalidArgumentError("No FE addresses provided for health monitoring")
	}

	// Start with a random FE to avoid all clients hitting the same one
	hm.currentFEIndex = rand.Intn(len(hm.feAddresses))

	// Create a context with cancel function
	monitoringCtx, cancel := context.WithCancel(context.Background())
	hm.cancel = cancel

	// Start backend check goroutine
	hm.wg.Add(1)
	go hm.runBackendChecks(monitoringCtx)

	// Start tablet check goroutine
	hm.wg.Add(1)
	go hm.runTabletChecks(monitoringCtx)

	hm.monitoring = true
	hm.logger.Info("Health monitoring started", "feAddresses", hm.feAddresses)

	return nil
}

// StopMonitoring stops the health monitoring process
func (hm *HealthMonitorImpl) StopMonitoring() error {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	if !hm.monitoring {
		return errors.NewInvalidStateError("Health monitoring is not running")
	}

	// Cancel the context to signal all goroutines to stop
	if hm.cancel != nil {
		hm.cancel()
		hm.cancel = nil
	}

	// Wait for all goroutines to finish
	hm.wg.Wait()

	hm.monitoring = false
	hm.logger.Info("Health monitoring stopped")

	return nil
}

// GetHealthState returns the current health state of the cluster
func (hm *HealthMonitorImpl) GetHealthState() HealthState {
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()
	return hm.currentState
}

// GetClusterHealthMetrics returns the current health metrics of the cluster
func (hm *HealthMonitorImpl) GetClusterHealthMetrics() ClusterHealthMetrics {
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()
	return hm.currentMetrics
}

// GetBackendHealth returns the health information for a specific backend
func (hm *HealthMonitorImpl) GetBackendHealth(backendID int64) (BackendHealth, error) {
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()

	for _, be := range hm.backends {
		if be.BackendID == backendID {
			return be, nil
		}
	}

	return BackendHealth{}, errors.NewNotFoundError(fmt.Sprintf("Backend with ID %d not found", backendID))
}

// GetAllBackendHealth returns the health information for all backends
func (hm *HealthMonitorImpl) GetAllBackendHealth() ([]BackendHealth, error) {
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()

	// Return a copy to prevent external modification
	result := make([]BackendHealth, len(hm.backends))
	copy(result, hm.backends)
	return result, nil
}

// GetTabletDistribution returns the distribution of tablets
func (hm *HealthMonitorImpl) GetTabletDistribution(ctx context.Context, databaseID, tableID int64) ([]TabletDistribution, error) {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(ctx, hm.config.RequestTimeout)
	defer cancel()

	// Set up query parameters
	params := map[string]interface{}{
		"database_id": databaseID,
	}
	if tableID > 0 {
		params["table_id"] = tableID
	}

	// Query StarRocks for tablet distribution
	distribution, err := hm.executeWithRetry(ctx, func(ctx context.Context) ([]TabletDistribution, error) {
		return hm.queryTabletDistribution(ctx, params)
	})

	if err != nil {
		hm.logger.Error("Failed to get tablet distribution",
			"databaseID", databaseID,
			"tableID", tableID,
			"error", err)
		return nil, err
	}

	return distribution, nil
}

// queryTabletDistribution queries StarRocks for tablet distribution
func (hm *HealthMonitorImpl) queryTabletDistribution(ctx context.Context, params map[string]interface{}) ([]TabletDistribution, error) {
	// Build query based on parameters
	var query string
	var args []interface{}

	if databaseID, ok := params["database_id"]; ok {
		if tableID, ok := params["table_id"]; ok {
			query = "SHOW TABLETS FROM $1.$2"
			// We need to convert IDs to actual names using a separate query
			// For simplicity, we're using dummy placeholders here
			args = []interface{}{"db_name", "table_name"}
		} else {
			query = "SHOW TABLETS FROM $1"
			args = []interface{}{"db_name"}
		}
	} else {
		query = "SHOW TABLETS"
	}

	// For this example, we'll simulate the result
	// In a real implementation, you would execute the query using starrocksClient
	// result, err := hm.starrocksClient.ExecuteQuery(ctx, query, args...)

	// Simulate some tablet distribution data
	result := []TabletDistribution{
		{
			DatabaseID:    1,
			DatabaseName:  "db1",
			TableID:       101,
			TableName:     "table1",
			PartitionID:   1001,
			PartitionName: "p1",
			IndexID:       10001,
			IndexName:     "idx1",
			TabletID:      100001,
			ReplicaID:     1000001,
			BackendID:     1,
			SchemaHash:    123456,
			Version:       1,
			VersionHash:   987654321,
			RowCount:      10000,
			DataSize:      1024 * 1024 * 10, // 10MB
			State:         "NORMAL",
		},
		// Add more simulated data as needed
	}

	return result, nil
}

// GetRecentAlerts returns recent health alerts
func (hm *HealthMonitorImpl) GetRecentAlerts(maxCount int) []HealthAlert {
	hm.alertMutex.RLock()
	defer hm.alertMutex.RUnlock()

	if maxCount <= 0 || maxCount > len(hm.recentAlerts) {
		maxCount = len(hm.recentAlerts)
	}

	// Return the most recent alerts
	result := make([]HealthAlert, maxCount)
	startIdx := len(hm.recentAlerts) - maxCount
	copy(result, hm.recentAlerts[startIdx:])

	return result
}

// SubscribeToHealthStateChanges subscribes to health state change events
func (hm *HealthMonitorImpl) SubscribeToHealthStateChanges() (string, <-chan HealthStateEvent) {
	hm.subMutex.Lock()
	defer hm.subMutex.Unlock()

	// Generate a unique subscription ID
	subscriptionID := fmt.Sprintf("health-%d", time.Now().UnixNano())

	// Create a buffered channel
	ch := make(chan HealthStateEvent, 10)

	hm.healthStateSubscriptions[subscriptionID] = ch

	return subscriptionID, ch
}

// UnsubscribeFromHealthStateChanges unsubscribes from health state change events
func (hm *HealthMonitorImpl) UnsubscribeFromHealthStateChanges(subscriptionID string) bool {
	hm.subMutex.Lock()
	defer hm.subMutex.Unlock()

	if ch, exists := hm.healthStateSubscriptions[subscriptionID]; exists {
		close(ch)
		delete(hm.healthStateSubscriptions, subscriptionID)
		return true
	}

	return false
}

// SubscribeToBackendStateChanges subscribes to backend state change events
func (hm *HealthMonitorImpl) SubscribeToBackendStateChanges() (string, <-chan BackendStateEvent) {
	hm.subMutex.Lock()
	defer hm.subMutex.Unlock()

	// Generate a unique subscription ID
	subscriptionID := fmt.Sprintf("backend-%d", time.Now().UnixNano())

	// Create a buffered channel
	ch := make(chan BackendStateEvent, 10)

	hm.backendStateSubscriptions[subscriptionID] = ch

	return subscriptionID, ch
}

// UnsubscribeFromBackendStateChanges unsubscribes from backend state change events
func (hm *HealthMonitorImpl) UnsubscribeFromBackendStateChanges(subscriptionID string) bool {
	hm.subMutex.Lock()
	defer hm.subMutex.Unlock()

	if ch, exists := hm.backendStateSubscriptions[subscriptionID]; exists {
		close(ch)
		delete(hm.backendStateSubscriptions, subscriptionID)
		return true
	}

	return false
}

// ForceRefresh forces an immediate refresh of health data
func (hm *HealthMonitorImpl) ForceRefresh(ctx context.Context) error {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(ctx, hm.config.RequestTimeout)
	defer cancel()

	// Check if monitoring is active
	hm.mutex.RLock()
	if !hm.monitoring {
		hm.mutex.RUnlock()
		return errors.NewInvalidStateError("Health monitoring is not running")
	}
	hm.mutex.RUnlock()

	// Refresh backend health
	if err := hm.checkBackendHealth(ctx); err != nil {
		return err
	}

	// Refresh tablet distribution
	if err := hm.checkTabletDistribution(ctx); err != nil {
		return err
	}

	// Recalculate health state and metrics
	hm.updateHealthState()

	return nil
}

// runBackendChecks runs the backend health checks
func (hm *HealthMonitorImpl) runBackendChecks(ctx context.Context) {
	defer hm.wg.Done()

	ticker := time.NewTicker(hm.config.BackendCheckInterval)
	defer ticker.Stop()

	// Run an initial check immediately
	if err := hm.checkBackendHealth(ctx); err != nil {
		hm.logger.Error("Initial backend health check failed", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			hm.logger.Info("Backend health check goroutine stopping")
			return
		case <-ticker.C:
			if err := hm.checkBackendHealth(ctx); err != nil {
				hm.logger.Error("Backend health check failed", "error", err)
				// Switch to a different FE on error
				hm.rotateFeAddress()
			}
		}
	}
}

// runTabletChecks runs the tablet distribution checks
func (hm *HealthMonitorImpl) runTabletChecks(ctx context.Context) {
	defer hm.wg.Done()

	ticker := time.NewTicker(hm.config.TabletCheckInterval)
	defer ticker.Stop()

	// Don't run an initial check immediately, give the system time to stabilize

	for {
		select {
		case <-ctx.Done():
			hm.logger.Info("Tablet distribution check goroutine stopping")
			return
		case <-ticker.C:
			if err := hm.checkTabletDistribution(ctx); err != nil {
				hm.logger.Error("Tablet distribution check failed", "error", err)
				// Switch to a different FE on error
				hm.rotateFeAddress()
			}
		}
	}
}

// checkBackendHealth checks the health of all backends
func (hm *HealthMonitorImpl) checkBackendHealth(ctx context.Context) error {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(ctx, hm.config.RequestTimeout)
	defer cancel()

	// Query StarRocks for backend health
	backends, err := hm.executeWithRetry(ctx, func(ctx context.Context) ([]BackendHealth, error) {
		return hm.queryBackendHealth(ctx)
	})

	if err != nil {
		hm.logger.Error("Failed to query backend health", "error", err)
		return err
	}

	// Update backend health information
	hm.updateBackendHealth(backends)

	// Recalculate health state
	hm.updateHealthState()

	return nil
}

// queryBackendHealth queries StarRocks for backend health information
func (hm *HealthMonitorImpl) queryBackendHealth(ctx context.Context) ([]BackendHealth, error) {
	// In a real implementation, you would call the StarRocks API
	// For example: response, err := hm.starrocksClient.GetBackends(ctx)

	// For this example, we'll simulate the API response
	backends := []BackendHealth{
		{
			BackendID:             1,
			Host:                  "be-1.example.com",
			HeartbeatPort:         9050,
			BePort:                9060,
			HttpPort:              8040,
			BrpcPort:              8060,
			LastStartTime:         time.Now().Add(-24 * time.Hour),
			LastHeartbeat:         time.Now(),
			Alive:                 true,
			SystemDecommissioned:  false,
			ClusterDecommissioned: false,
			TabletCount:           1000,
			DataUsedCapacity:      1024 * 1024 * 1024 * 100,  // 100GB
			AvailCapacity:         1024 * 1024 * 1024 * 900,  // 900GB
			TotalCapacity:         1024 * 1024 * 1024 * 1000, // 1TB
			UsedPct:               10.0,
			MaxDiskUsedPct:        15.0,
			Tag:                   "zone1",
			Status:                "OK",
			NodeRole:              "compute",
		},
		{
			BackendID:             2,
			Host:                  "be-2.example.com",
			HeartbeatPort:         9050,
			BePort:                9060,
			HttpPort:              8040,
			BrpcPort:              8060,
			LastStartTime:         time.Now().Add(-48 * time.Hour),
			LastHeartbeat:         time.Now(),
			Alive:                 true,
			SystemDecommissioned:  false,
			ClusterDecommissioned: false,
			TabletCount:           1200,
			DataUsedCapacity:      1024 * 1024 * 1024 * 200,  // 200GB
			AvailCapacity:         1024 * 1024 * 1024 * 800,  // 800GB
			TotalCapacity:         1024 * 1024 * 1024 * 1000, // 1TB
			UsedPct:               20.0,
			MaxDiskUsedPct:        25.0,
			Tag:                   "zone2",
			Status:                "OK",
			NodeRole:              "compute",
		},
		// Simulate an unhealthy backend
		{
			BackendID:             3,
			Host:                  "be-3.example.com",
			HeartbeatPort:         9050,
			BePort:                9060,
			HttpPort:              8040,
			BrpcPort:              8060,
			LastStartTime:         time.Now().Add(-72 * time.Hour),
			LastHeartbeat:         time.Now().Add(-10 * time.Minute),
			Alive:                 false,
			SystemDecommissioned:  false,
			ClusterDecommissioned: false,
			TabletCount:           800,
			DataUsedCapacity:      1024 * 1024 * 1024 * 150,  // 150GB
			AvailCapacity:         1024 * 1024 * 1024 * 850,  // 850GB
			TotalCapacity:         1024 * 1024 * 1024 * 1000, // 1TB
			UsedPct:               15.0,
			MaxDiskUsedPct:        20.0,
			Tag:                   "zone3",
			Status:                "Error",
			ErrMsg:                "Node is down",
			NodeRole:              "compute",
		},
	}

	return backends, nil
}

// updateBackendHealth updates the backend health information
func (hm *HealthMonitorImpl) updateBackendHealth(backends []BackendHealth) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	// Check for backend state changes
	backendChanges := make([]BackendStateEvent, 0)

	// Create a map of current backends by ID for easy lookup
	currentBackends := make(map[int64]bool)
	for _, be := range backends {
		currentBackends[be.BackendID] = be.Alive

		// Check if this backend existed before and if its state changed
		if previousState, exists := hm.lastBackendStates[be.BackendID]; exists && previousState != be.Alive {
			// State changed - create an event
			backendChanges = append(backendChanges, BackendStateEvent{
				BackendID:     be.BackendID,
				Host:          be.Host,
				PreviousAlive: previousState,
				CurrentAlive:  be.Alive,
				Timestamp:     time.Now(),
			})
		}
	}

	// Check for backends that were removed
	for id, wasAlive := range hm.lastBackendStates {
		if _, exists := currentBackends[id]; !exists {
			// Backend was removed - create an event
			backendChanges = append(backendChanges, BackendStateEvent{
				BackendID:     id,
				Host:          "unknown", // We don't have the host anymore
				PreviousAlive: wasAlive,
				CurrentAlive:  false,
				Timestamp:     time.Now(),
			})
		}
	}

	// Update backend list and last states map
	hm.backends = backends
	hm.lastBackendStates = currentBackends

	// Process backend state changes
	for _, event := range backendChanges {
		hm.handleBackendStateChange(event)
	}

	// Update metrics
	hm.updateBackendMetrics()
}

// handleBackendStateChange handles a backend state change
func (hm *HealthMonitorImpl) handleBackendStateChange(event BackendStateEvent) {
	// Log the state change
	hm.logger.Info("Backend state changed",
		"backendID", event.BackendID,
		"host", event.Host,
		"previousAlive", event.PreviousAlive,
		"currentAlive", event.CurrentAlive)

	// Publish the event to subscribers
	hm.publishBackendStateEvent(event)

	// Notify the tablet manager of the state change
	if hm.tabletManager != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), hm.config.RequestTimeout)
			defer cancel()

			if err := hm.tabletManager.HandleBackendStateChange(ctx, event.BackendID, event.CurrentAlive); err != nil {
				hm.logger.Error("Failed to notify tablet manager of backend state change",
					"backendID", event.BackendID,
					"error", err)
			}
		}()
	}

	// Record metrics
	hm.metrics.CounterInc("backend_state_changes_total", map[string]string{
		"backend_id": fmt.Sprintf("%d", event.BackendID),
		"host":       event.Host,
		"is_alive":   fmt.Sprintf("%t", event.CurrentAlive),
	})

	// Generate an alert if a backend went down
	if !event.CurrentAlive && event.PreviousAlive {
		alert := HealthAlert{
			Level:     "WARNING",
			Message:   fmt.Sprintf("Backend %d (%s) is down", event.BackendID, event.Host),
			Source:    "HealthMonitor",
			Timestamp: event.Timestamp,
			Metrics: map[string]interface{}{
				"backend_id": event.BackendID,
				"host":       event.Host,
			},
		}
		hm.addAlert(alert)
	}

	// Generate an alert if a backend came back up
	if event.CurrentAlive && !event.PreviousAlive {
		alert := HealthAlert{
			Level:     "INFO",
			Message:   fmt.Sprintf("Backend %d (%s) is back online", event.BackendID, event.Host),
			Source:    "HealthMonitor",
			Timestamp: event.Timestamp,
			Metrics: map[string]interface{}{
				"backend_id": event.BackendID,
				"host":       event.Host,
			},
		}
		hm.addAlert(alert)
	}
}

// updateBackendMetrics updates metrics related to backends
func (hm *HealthMonitorImpl) updateBackendMetrics() {
	totalBackends := len(hm.backends)
	healthyBackends := 0
	unhealthyBackends := 0
	decommissionedBackends := 0
	totalDiskUsage := 0.0
	maxDiskUsage := 0.0

	for _, be := range hm.backends {
		if be.Alive {
			healthyBackends++
		} else {
			unhealthyBackends++
		}

		if be.SystemDecommissioned || be.ClusterDecommissioned {
			decommissionedBackends++
		}

		totalDiskUsage += be.UsedPct
		if be.UsedPct > maxDiskUsage {
			maxDiskUsage = be.UsedPct
		}
	}

	averageDiskUsage := 0.0
	if totalBackends > 0 {
		averageDiskUsage = totalDiskUsage / float64(totalBackends)
	}

	// Update metrics gauges
	hm.metrics.GaugeSet("total_backends", float64(totalBackends), nil)
	hm.metrics.GaugeSet("healthy_backends", float64(healthyBackends), nil)
	hm.metrics.GaugeSet("unhealthy_backends", float64(unhealthyBackends), nil)
	hm.metrics.GaugeSet("decommissioned_backends", float64(decommissionedBackends), nil)
	hm.metrics.GaugeSet("average_backend_disk_usage", averageDiskUsage, nil)
	hm.metrics.GaugeSet("max_backend_disk_usage", maxDiskUsage, nil)
}

// checkTabletDistribution checks the distribution of tablets
func (hm *HealthMonitorImpl) checkTabletDistribution(ctx context.Context) error {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(ctx, hm.config.RequestTimeout)
	defer cancel()

	// In a real implementation, you would query for all databases and tables
	// For simplicity, we'll just query the overall tablet distribution
	distribution, err := hm.executeWithRetry(ctx, func(ctx context.Context) ([]TabletDistribution, error) {
		return hm.queryTabletDistribution(ctx, nil)
	})

	if err != nil {
		hm.logger.Error("Failed to query tablet distribution", "error", err)
		return err
	}

	// Analyze the tablet distribution
	hm.analyzeTabletDistribution(distribution)

	// Recalculate health state
	hm.updateHealthState()

	return nil
}

// analyzeTabletDistribution analyzes the tablet distribution
func (hm *HealthMonitorImpl) analyzeTabletDistribution(distribution []TabletDistribution) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	totalTablets := len(distribution)
	healthyTablets := 0
	unhealthyTablets := 0
	tabletReplicas := make(map[int64]int)
	desiredReplicas := 3 // Typically, StarRocks uses 3 replicas per tablet
	underReplicatedTablets := 0
	overReplicatedTablets := 0

	// Analyze tablet health and replication
	for _, tablet := range distribution {
		// Count the tablet replica
		tabletReplicas[tablet.TabletID]++

		// Check tablet health
		if tablet.State == "NORMAL" {
			healthyTablets++
		} else {
			unhealthyTablets++
		}
	}

	// Check for under/over-replicated tablets
	totalReplicaCount := 0
	for tabletID, replicaCount := range tabletReplicas {
		totalReplicaCount += replicaCount
		if replicaCount < desiredReplicas {
			underReplicatedTablets++
		} else if replicaCount > desiredReplicas {
			overReplicatedTablets++
		}
	}

	// Calculate average replicas per tablet
	averageReplicas := 0.0
	if len(tabletReplicas) > 0 {
		averageReplicas = float64(totalReplicaCount) / float64(len(tabletReplicas))
	}

	// Calculate a tablet balance score
	// In a real implementation, you would calculate this based on the distribution
	// of tablets across backends
	balanceScore := 0.0
	if healthyTablets > 0 {
		balanceScore = 100.0 * (1.0 - float64(unhealthyTablets)/float64(totalTablets))
	}

	// Update the current metrics
	metrics := &hm.currentMetrics
	metrics.TotalTablets = totalTablets
	metrics.HealthyTablets = healthyTablets
	metrics.UnhealthyTablets = unhealthyTablets
	metrics.AverageTabletReplicas = averageReplicas
	metrics.UnderReplicatedTablets = underReplicatedTablets
	metrics.OverReplicatedTablets = overReplicatedTablets
	metrics.BalanceScore = balanceScore
	metrics.LastUpdated = time.Now()

	// Update metrics gauges
	hm.metrics.GaugeSet("total_tablets", float64(totalTablets), nil)
	hm.metrics.GaugeSet("healthy_tablets", float64(healthyTablets), nil)
	hm.metrics.GaugeSet("unhealthy_tablets", float64(unhealthyTablets), nil)
	hm.metrics.GaugeSet("average_tablet_replicas", averageReplicas, nil)
	hm.metrics.GaugeSet("under_replicated_tablets", float64(underReplicatedTablets), nil)
	hm.metrics.GaugeSet("over_replicated_tablets", float64(overReplicatedTablets), nil)
	hm.metrics.GaugeSet("tablet_balance_score", balanceScore, nil)

	// Generate alerts for under-replicated tablets if needed
	if underReplicatedTablets > 0 {
		alert := HealthAlert{
			Level:     "WARNING",
			Message:   fmt.Sprintf("%d tablets are under-replicated", underReplicatedTablets),
			Source:    "HealthMonitor",
			Timestamp: time.Now(),
			Metrics: map[string]interface{}{
				"under_replicated_tablets": underReplicatedTablets,
				"total_tablets":            totalTablets,
			},
		}
		hm.addAlert(alert)
	}

	// Generate alerts for unhealthy tablets if needed
	if unhealthyTablets > 0 {
		alert := HealthAlert{
			Level:     "WARNING",
			Message:   fmt.Sprintf("%d tablets are unhealthy", unhealthyTablets),
			Source:    "HealthMonitor",
			Timestamp: time.Now(),
			Metrics: map[string]interface{}{
				"unhealthy_tablets": unhealthyTablets,
				"total_tablets":     totalTablets,
			},
		}
		hm.addAlert(alert)
	}
}

// updateHealthState updates the overall health state based on metrics
func (hm *HealthMonitorImpl) updateHealthState() {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	// Calculate metrics for total backends
	totalBackends := len(hm.backends)
	healthyBackends := 0
	for _, be := range hm.backends {
		if be.Alive {
			healthyBackends++
		}
	}

	// Calculate backend health percentage
	backendHealthPercentage := 0.0
	if totalBackends > 0 {
		backendHealthPercentage = float64(healthyBackends) / float64(totalBackends) * 100.0
	}

	// Update metrics
	metrics := &hm.currentMetrics
	metrics.TotalBackends = totalBackends
	metrics.HealthyBackends = healthyBackends
	metrics.UnhealthyBackends = totalBackends - healthyBackends
	metrics.LastUpdated = time.Now()

	// Determine the health state
	previousState := hm.currentState
	var newState HealthState
	var stateReason string

	if totalBackends == 0 {
		newState = HealthStateUnknown
		stateReason = "No backends found"
	} else if backendHealthPercentage < hm.config.CriticalBackendThreshold {
		newState = HealthStateCritical
		stateReason = fmt.Sprintf("Only %.1f%% of backends are healthy (below critical threshold of %.1f%%)",
			backendHealthPercentage, hm.config.CriticalBackendThreshold)
	} else if backendHealthPercentage < hm.config.UnhealthyBackendThreshold {
		newState = HealthStateUnhealthy
		stateReason = fmt.Sprintf("Only %.1f%% of backends are healthy (below unhealthy threshold of %.1f%%)",
			backendHealthPercentage, hm.config.UnhealthyBackendThreshold)
	} else if healthyBackends < totalBackends {
		newState = HealthStateDegraded
		stateReason = fmt.Sprintf("%d out of %d backends are healthy", healthyBackends, totalBackends)
	} else if metrics.UnhealthyTablets > 0 || metrics.UnderReplicatedTablets > 0 {
		newState = HealthStateDegraded
		stateReason = fmt.Sprintf("All backends are healthy but %d tablets are unhealthy and %d are under-replicated",
			metrics.UnhealthyTablets, metrics.UnderReplicatedTablets)
	} else {
		newState = HealthStateHealthy
		stateReason = "All components are healthy"
	}

	// If the state changed, publish an event
	if newState != previousState {
		hm.currentState = newState

		// Log the state change
		hm.logger.Info("Cluster health state changed",
			"previousState", previousState,
			"newState", newState,
			"reason", stateReason)

		// Create an event
		event := HealthStateEvent{
			PreviousState: previousState,
			CurrentState:  newState,
			Reason:        stateReason,
			Timestamp:     time.Now(),
			Metrics:       hm.currentMetrics,
		}

		// Publish the event
		hm.publishHealthStateEvent(event)

		// Generate an alert if the state worsened
		if isWorsened(previousState, newState) {
			alert := HealthAlert{
				Level:     getAlertLevelForState(newState),
				Message:   fmt.Sprintf("Cluster health state degraded from %s to %s: %s", previousState, newState, stateReason),
				Source:    "HealthMonitor",
				Timestamp: time.Now(),
				Metrics: map[string]interface{}{
					"previous_state":   string(previousState),
					"current_state":    string(newState),
					"healthy_backends": healthyBackends,
					"total_backends":   totalBackends,
				},
			}
			hm.addAlert(alert)
		}

		// Generate an alert if the state improved
		if isImproved(previousState, newState) {
			alert := HealthAlert{
				Level:     "INFO",
				Message:   fmt.Sprintf("Cluster health state improved from %s to %s: %s", previousState, newState, stateReason),
				Source:    "HealthMonitor",
				Timestamp: time.Now(),
				Metrics: map[string]interface{}{
					"previous_state":   string(previousState),
					"current_state":    string(newState),
					"healthy_backends": healthyBackends,
					"total_backends":   totalBackends,
				},
			}
			hm.addAlert(alert)
		}
	}

	// Update metrics gauge
	hm.metrics.GaugeSet("cluster_health_state", float64(getStateNumericValue(newState)), map[string]string{
		"state": string(newState),
	})
}

// getStateNumericValue converts a health state to a numeric value for metrics
func getStateNumericValue(state HealthState) int {
	switch state {
	case HealthStateHealthy:
		return 0
	case HealthStateDegraded:
		return 1
	case HealthStateUnhealthy:
		return 2
	case HealthStateCritical:
		return 3
	default:
		return 4 // Unknown
	}
}

// isWorsened checks if the health state has worsened
func isWorsened(old, new HealthState) bool {
	return getStateNumericValue(new) > getStateNumericValue(old)
}

// isImproved checks if the health state has improved
func isImproved(old, new HealthState) bool {
	return getStateNumericValue(new) < getStateNumericValue(old)
}

// getAlertLevelForState returns the appropriate alert level for a health state
func getAlertLevelForState(state HealthState) string {
	switch state {
	case HealthStateHealthy:
		return "INFO"
	case HealthStateDegraded:
		return "WARNING"
	case HealthStateUnhealthy:
		return "ERROR"
	case HealthStateCritical:
		return "CRITICAL"
	default:
		return "INFO"
	}
}

// publishHealthStateEvent publishes a health state event to all subscribers
func (hm *HealthMonitorImpl) publishHealthStateEvent(event HealthStateEvent) {
	hm.subMutex.RLock()
	defer hm.subMutex.RUnlock()

	for _, ch := range hm.healthStateSubscriptions {
		// Non-blocking send
		select {
		case ch <- event:
			// Successfully sent
		default:
			// Channel is full, log and continue
			hm.logger.Warn("Failed to send health state event to subscriber (buffer full)")
		}
	}

	// Also publish to the general event bus
	if hm.eventBus != nil {
		hm.eventBus.Publish("health.state.changed", event)
	}
}

// publishBackendStateEvent publishes a backend state event to all subscribers
func (hm *HealthMonitorImpl) publishBackendStateEvent(event BackendStateEvent) {
	hm.subMutex.RLock()
	defer hm.subMutex.RUnlock()

	for _, ch := range hm.backendStateSubscriptions {
		// Non-blocking send
		select {
		case ch <- event:
			// Successfully sent
		default:
			// Channel is full, log and continue
			hm.logger.Warn("Failed to send backend state event to subscriber (buffer full)")
		}
	}

	// Also publish to the general event bus
	if hm.eventBus != nil {
		hm.eventBus.Publish("backend.state.changed", event)
	}
}

// addAlert adds an alert to the recent alerts list and optionally sends it
func (hm *HealthMonitorImpl) addAlert(alert HealthAlert) {
	hm.alertMutex.Lock()
	defer hm.alertMutex.Unlock()

	// Add to recent alerts
	hm.recentAlerts = append(hm.recentAlerts, alert)

	// Limit the size of the recent alerts list
	if len(hm.recentAlerts) > 100 {
		hm.recentAlerts = hm.recentAlerts[len(hm.recentAlerts)-100:]
	}

	// Log the alert
	hm.logger.Log(alert.Level, alert.Message, "source", alert.Source)

	// Send the alert if enabled and not in cooldown
	if hm.config.EnableAlerts && hm.alertProvider != nil {
		if time.Since(hm.lastAlertTime) > hm.config.AlertCooldown {
			go func() {
				if err := hm.alertProvider.SendAlert(alert); err != nil {
					hm.logger.Error("Failed to send alert", "error", err)
				} else {
					hm.lastAlertTime = time.Now()
				}
			}()
		}
	}

	// Record metrics
	hm.metrics.CounterInc("health_alerts_total", map[string]string{
		"level":  alert.Level,
		"source": alert.Source,
	})
}

// rotateFeAddress rotates to the next FE address
func (hm *HealthMonitorImpl) rotateFeAddress() {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	if len(hm.feAddresses) <= 1 {
		return
	}

	// Move to the next FE
	hm.currentFEIndex = (hm.currentFEIndex + 1) % len(hm.feAddresses)
	currentFE := hm.feAddresses[hm.currentFEIndex]

	// Update the client to use the new FE
	if hm.starrocksClient != nil {
		if err := hm.starrocksClient.SetActiveEndpoint(currentFE); err != nil {
			hm.logger.Error("Failed to set active endpoint", "endpoint", currentFE, "error", err)
		} else {
			hm.logger.Info("Switched to new frontend", "endpoint", currentFE)
		}
	}
}

// executeWithRetry executes a function with retry logic
func (hm *HealthMonitorImpl) executeWithRetry(ctx context.Context, fn func(context.Context) (interface{}, error)) (interface{}, error) {
	var result interface{}
	var err error

	for attempt := 0; attempt <= hm.config.RetryCount; attempt++ {
		// Check if context is cancelled
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Execute the function
		result, err = fn(ctx)
		if err == nil {
			return result, nil
		}

		// Log the error
		hm.logger.Error("Operation failed, will retry",
			"attempt", attempt+1,
			"maxAttempts", hm.config.RetryCount+1,
			"error", err)

		// If this was the last attempt, return the error
		if attempt >= hm.config.RetryCount {
			break
		}

		// Wait before retrying
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(hm.config.RetryInterval):
			// Continue with retry
		}

		// Rotate to a different FE for the next attempt
		hm.rotateFeAddress()
	}

	return nil, err
}

//Personal.AI order the ending
