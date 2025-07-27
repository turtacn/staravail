// Package tablet defines the domain entities and services for tablet management.
package tablet

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/turtacn/staravail/internal/common/errors"
	logging "github.com/turtacn/staravail/internal/common/logger"
	"github.com/turtacn/staravail/internal/common/metrics"
)

// TabletStateEvent represents an event related to tablet state changes
type TabletStateEvent struct {
	// TabletID is the ID of the tablet that changed state
	TabletID int64
	// OldState is the previous state of the tablet
	OldState TabletState
	// NewState is the new state of the tablet
	NewState TabletState
	// Timestamp is when the state change occurred
	Timestamp time.Time
	// Reason describes why the state changed
	Reason string
}

// PartitionStatus represents the status of a partition
type PartitionStatus struct {
	// PartitionID is the ID of the partition
	PartitionID int64
	// TableID is the ID of the table this partition belongs to
	TableID int64
	// DatabaseID is the ID of the database this partition belongs to
	DatabaseID int64
	// TotalTablets is the total number of tablets in this partition
	TotalTablets int
	// AvailableTablets is the number of available tablets in this partition
	AvailableTablets int
	// UnavailableTablets is the number of unavailable tablets in this partition
	UnavailableTablets int
	// MovingTablets is the number of tablets being moved in this partition
	MovingTablets int
	// CloningTablets is the number of tablets being cloned in this partition
	CloningTablets int
	// RestoringTablets is the number of tablets being restored in this partition
	RestoringTablets int
	// IsAvailable indicates if the partition as a whole is available
	IsAvailable bool
	// LastUpdated is when this status was last updated
	LastUpdated time.Time
}

// TableStatus represents the status of a table
type TableStatus struct {
	// TableID is the ID of the table
	TableID int64
	// DatabaseID is the ID of the database this table belongs to
	DatabaseID int64
	// TotalPartitions is the total number of partitions in this table
	TotalPartitions int
	// AvailablePartitions is the number of available partitions in this table
	AvailablePartitions int
	// UnavailablePartitions is the number of unavailable partitions in this table
	UnavailablePartitions int
	// TotalTablets is the total number of tablets in this table
	TotalTablets int
	// AvailableTablets is the number of available tablets in this table
	AvailableTablets int
	// UnavailableTablets is the number of unavailable tablets in this table
	UnavailableTablets int
	// IsAvailable indicates if the table as a whole is available
	IsAvailable bool
	// LastUpdated is when this status was last updated
	LastUpdated time.Time
}

// DatabaseStatus represents the status of a database
type DatabaseStatus struct {
	// DatabaseID is the ID of the database
	DatabaseID int64
	// TotalTables is the total number of tables in this database
	TotalTables int
	// AvailableTables is the number of available tables in this database
	AvailableTables int
	// UnavailableTables is the number of unavailable tables in this database
	UnavailableTables int
	// TotalPartitions is the total number of partitions in this database
	TotalPartitions int
	// AvailablePartitions is the number of available partitions in this database
	AvailablePartitions int
	// UnavailablePartitions is the number of unavailable partitions in this database
	UnavailablePartitions int
	// TotalTablets is the total number of tablets in this database
	TotalTablets int
	// AvailableTablets is the number of available tablets in this database
	AvailableTablets int
	// UnavailableTablets is the number of unavailable tablets in this database
	UnavailableTablets int
	// IsAvailable indicates if the database as a whole is available
	IsAvailable bool
	// LastUpdated is when this status was last updated
	LastUpdated time.Time
}

// TabletManager defines the interface for tablet management operations
type TabletManager interface {
	// GetTablet retrieves a tablet by its ID
	// Returns nil if the tablet is not found
	// Returns an error if the operation fails
	GetTablet(ctx context.Context, tabletID int64) (*Tablet, error)

	// GetTablets retrieves multiple tablets by their IDs
	// Returns an empty collection if no tablets are found
	// Returns an error if the operation fails
	GetTablets(ctx context.Context, tabletIDs []int64) (*TabletCollection, error)

	// GetTabletStatus retrieves the status of a tablet
	// Returns an error if the tablet is not found or the operation fails
	GetTabletStatus(ctx context.Context, tabletID int64) (TabletState, error)

	// UpdateTabletStatus updates the status of a tablet
	// Returns an error if the tablet is not found or the operation fails
	UpdateTabletStatus(ctx context.Context, tabletID int64, state TabletState, reason string) error

	// GetPartitionTablets retrieves all tablets in a partition
	// Returns an empty collection if no tablets are found
	// Returns an error if the operation fails
	GetPartitionTablets(ctx context.Context, partitionID int64) (*TabletCollection, error)

	// GetPartitionStatus retrieves the status of a partition
	// Returns an error if the partition is not found or the operation fails
	GetPartitionStatus(ctx context.Context, partitionID int64) (*PartitionStatus, error)

	// GetTableTablets retrieves all tablets in a table
	// Returns an empty collection if no tablets are found
	// Returns an error if the operation fails
	GetTableTablets(ctx context.Context, tableID int64) (*TabletCollection, error)

	// GetTableStatus retrieves the status of a table
	// Returns an error if the table is not found or the operation fails
	GetTableStatus(ctx context.Context, tableID int64) (*TableStatus, error)

	// GetDatabaseTablets retrieves all tablets in a database
	// Returns an empty collection if no tablets are found
	// Returns an error if the operation fails
	GetDatabaseTablets(ctx context.Context, databaseID int64) (*TabletCollection, error)

	// GetDatabaseStatus retrieves the status of a database
	// Returns an error if the database is not found or the operation fails
	GetDatabaseStatus(ctx context.Context, databaseID int64) (*DatabaseStatus, error)

	// GetBackendTablets retrieves all tablets on a backend
	// Returns an empty collection if no tablets are found
	// Returns an error if the operation fails
	GetBackendTablets(ctx context.Context, backendID int64) (*TabletCollection, error)

	// HandleBackendStateChange handles a change in backend state
	// Updates the status of all tablets on the backend accordingly
	// Returns an error if the operation fails
	HandleBackendStateChange(ctx context.Context, backendID int64, isAlive bool) error

	// RefreshTabletStatus refreshes the status of a tablet by querying its backends
	// Returns an error if the operation fails
	RefreshTabletStatus(ctx context.Context, tabletID int64) error

	// RefreshPartitionStatus refreshes the status of all tablets in a partition
	// Returns an error if the operation fails
	RefreshPartitionStatus(ctx context.Context, partitionID int64) error

	// RefreshTableStatus refreshes the status of all tablets in a table
	// Returns an error if the operation fails
	RefreshTableStatus(ctx context.Context, tableID int64) error

	// RefreshDatabaseStatus refreshes the status of all tablets in a database
	// Returns an error if the operation fails
	RefreshDatabaseStatus(ctx context.Context, databaseID int64) error

	// RefreshAllStatus refreshes the status of all tablets
	// Returns an error if the operation fails
	RefreshAllStatus(ctx context.Context) error

	// SubscribeToTabletStateChanges subscribes to tablet state change events
	// Returns a subscription ID that can be used to unsubscribe
	SubscribeToTabletStateChanges() (string, <-chan TabletStateEvent)

	// UnsubscribeFromTabletStateChanges unsubscribes from tablet state change events
	// Returns true if the subscription was found and removed, false otherwise
	UnsubscribeFromTabletStateChanges(subscriptionID string) bool

	// GetAvailableTablets retrieves all available tablets
	// Returns an empty collection if no tablets are found
	// Returns an error if the operation fails
	GetAvailableTablets(ctx context.Context) (*TabletCollection, error)

	// GetUnavailableTablets retrieves all unavailable tablets
	// Returns an empty collection if no tablets are found
	// Returns an error if the operation fails
	GetUnavailableTablets(ctx context.Context) (*TabletCollection, error)
}

// TabletManagerImpl implements the TabletManager interface
type TabletManagerImpl struct {
	// tabletRepo is the repository for tablet operations
	tabletRepo TabletRepository
	// partitionRepo is the repository for partition operations
	partitionRepo PartitionRepository
	// backendRepo is the repository for backend operations
	backendRepo BackendRepository
	// eventBus is used to publish tablet state change events
	eventBus events.EventBus

	// tabletCache caches tablet information by ID
	tabletCache map[int64]*Tablet
	// partitionStatusCache caches partition status by ID
	partitionStatusCache map[int64]*PartitionStatus
	// tableStatusCache caches table status by ID
	tableStatusCache map[int64]*TableStatus
	// databaseStatusCache caches database status by ID
	databaseStatusCache map[int64]*DatabaseStatus

	// cacheMutex protects the caches
	cacheMutex sync.RWMutex
	// tabletStateLock protects tablet state updates
	tabletStateLock sync.Mutex

	// stateChangeSubscriptions holds the subscriptions to tablet state changes
	stateChangeSubscriptions map[string]chan TabletStateEvent
	// subscriptionsMutex protects the subscriptions map
	subscriptionsMutex sync.RWMutex

	// logger is the logger for tablet manager operations
	logger logging.Logger
	// metrics is used to record metrics
	metrics metrics.MetricsRecorder
	// cacheExpiryDuration is how long cache entries are valid
	cacheExpiryDuration time.Duration
}

// NewTabletManager creates a new TabletManager instance
func NewTabletManager(
	tabletRepo TabletRepository,
	partitionRepo PartitionRepository,
	backendRepo BackendRepository,
	eventBus events.EventBus,
	logger logging.Logger,
	metrics metrics.MetricsRecorder,
	cacheExpiryDuration time.Duration,
) TabletManager {
	if cacheExpiryDuration <= 0 {
		cacheExpiryDuration = 5 * time.Minute // Default cache expiry
	}

	manager := &TabletManagerImpl{
		tabletRepo:               tabletRepo,
		partitionRepo:            partitionRepo,
		backendRepo:              backendRepo,
		eventBus:                 eventBus,
		logger:                   logger,
		metrics:                  metrics,
		tabletCache:              make(map[int64]*Tablet),
		partitionStatusCache:     make(map[int64]*PartitionStatus),
		tableStatusCache:         make(map[int64]*TableStatus),
		databaseStatusCache:      make(map[int64]*DatabaseStatus),
		stateChangeSubscriptions: make(map[string]chan TabletStateEvent),
		cacheExpiryDuration:      cacheExpiryDuration,
	}

	// Start background cache cleanup
	go manager.startCacheCleanupTask()

	return manager
}

// startCacheCleanupTask periodically cleans up expired cache entries
func (tm *TabletManagerImpl) startCacheCleanupTask() {
	ticker := time.NewTicker(tm.cacheExpiryDuration / 2)
	defer ticker.Stop()

	for range ticker.C {
		tm.cleanupExpiredCacheEntries()
	}
}

// cleanupExpiredCacheEntries removes expired entries from the caches
func (tm *TabletManagerImpl) cleanupExpiredCacheEntries() {
	now := time.Now()
	tm.cacheMutex.Lock()
	defer tm.cacheMutex.Unlock()

	// Cleanup partition status cache
	for id, status := range tm.partitionStatusCache {
		if now.Sub(status.LastUpdated) > tm.cacheExpiryDuration {
			delete(tm.partitionStatusCache, id)
		}
	}

	// Cleanup table status cache
	for id, status := range tm.tableStatusCache {
		if now.Sub(status.LastUpdated) > tm.cacheExpiryDuration {
			delete(tm.tableStatusCache, id)
		}
	}

	// Cleanup database status cache
	for id, status := range tm.databaseStatusCache {
		if now.Sub(status.LastUpdated) > tm.cacheExpiryDuration {
			delete(tm.databaseStatusCache, id)
		}
	}

	// Note: We don't automatically clean up tablet cache entries
	// as they may be frequently accessed. Instead, they're managed
	// directly by the GetTablet and related methods.
}

// GetTablet retrieves a tablet by its ID
func (tm *TabletManagerImpl) GetTablet(ctx context.Context, tabletID int64) (*Tablet, error) {
	// Try to get from cache first
	tm.cacheMutex.RLock()
	if tablet, exists := tm.tabletCache[tabletID]; exists {
		tm.cacheMutex.RUnlock()
		return tablet, nil
	}
	tm.cacheMutex.RUnlock()

	// If not in cache, get from repository
	tablet, err := tm.tabletRepo.FindByID(ctx, tabletID)
	if err != nil {
		tm.logger.Error("Failed to get tablet", "tabletID", tabletID, "error", err)
		return nil, err
	}

	if tablet == nil {
		return nil, errors.NewNotFoundError(fmt.Sprintf("Tablet with ID %d not found", tabletID))
	}

	// Update cache
	tm.cacheMutex.Lock()
	tm.tabletCache[tabletID] = tablet
	tm.cacheMutex.Unlock()

	return tablet, nil
}

// GetTablets retrieves multiple tablets by their IDs
func (tm *TabletManagerImpl) GetTablets(ctx context.Context, tabletIDs []int64) (*TabletCollection, error) {
	if len(tabletIDs) == 0 {
		return NewTabletCollection(), nil
	}

	// Create a list of IDs that need to be fetched from the repository
	needToFetch := make([]int64, 0, len(tabletIDs))
	result := NewTabletCollection()

	// Try to get from cache first
	tm.cacheMutex.RLock()
	for _, id := range tabletIDs {
		if tablet, exists := tm.tabletCache[id]; exists {
			result.Add(tablet)
		} else {
			needToFetch = append(needToFetch, id)
		}
	}
	tm.cacheMutex.RUnlock()

	// If all tablets were in cache, return early
	if len(needToFetch) == 0 {
		return result, nil
	}

	// Fetch missing tablets from repository
	fetched, err := tm.tabletRepo.FindByIDs(ctx, needToFetch)
	if err != nil {
		tm.logger.Error("Failed to get tablets", "tabletIDs", needToFetch, "error", err)
		return nil, err
	}

	// Update cache and add to result
	if fetched.Size() > 0 {
		tm.cacheMutex.Lock()
		for _, tablet := range fetched.GetAll() {
			tm.tabletCache[tablet.TabletID()] = tablet
			result.Add(tablet)
		}
		tm.cacheMutex.Unlock()
	}

	return result, nil
}

// GetTabletStatus retrieves the status of a tablet
func (tm *TabletManagerImpl) GetTabletStatus(ctx context.Context, tabletID int64) (TabletState, error) {
	tablet, err := tm.GetTablet(ctx, tabletID)
	if err != nil {
		return TabletStateUnknown, err
	}

	return tablet.GetCurrentState(), nil
}

// UpdateTabletStatus updates the status of a tablet
func (tm *TabletManagerImpl) UpdateTabletStatus(ctx context.Context, tabletID int64, state TabletState, reason string) error {
	// Use a dedicated lock for tablet state updates to ensure consistency
	tm.tabletStateLock.Lock()
	defer tm.tabletStateLock.Unlock()

	// Get the tablet
	tablet, err := tm.GetTablet(ctx, tabletID)
	if err != nil {
		return err
	}

	// Get the current state before updating
	oldState := tablet.GetCurrentState()

	// If the state is not changing, no need to update
	if oldState == state {
		return nil
	}

	// Update the tablet state based on the new state
	switch state {
	case TabletStateNormal:
		tablet.MarkAvailable()
	case TabletStateUnavailable:
		tablet.MarkUnavailable(fmt.Errorf(reason))
	case TabletStateMoving:
		tablet.MarkMoving()
	case TabletStateCloning:
		tablet.MarkCloning()
	case TabletStateRestoring:
		tablet.MarkRestoring()
	default:
		return errors.NewInvalidArgumentError(fmt.Sprintf("Invalid tablet state: %s", state))
	}

	// Persist the tablet
	err = tm.tabletRepo.Save(ctx, tablet)
	if err != nil {
		tm.logger.Error("Failed to save tablet state", "tabletID", tabletID, "state", state, "error", err)
		return err
	}

	// Update the cache
	tm.cacheMutex.Lock()
	tm.tabletCache[tabletID] = tablet
	tm.cacheMutex.Unlock()

	// Clear affected caches
	tm.invalidateRelatedCaches(ctx, tablet)

	// Create and publish the tablet state change event
	event := TabletStateEvent{
		TabletID:  tabletID,
		OldState:  oldState,
		NewState:  state,
		Timestamp: time.Now(),
		Reason:    reason,
	}

	// Notify subscribers
	tm.publishTabletStateEvent(event)

	// Record metrics
	tm.metrics.CounterInc("tablet_state_changes_total", map[string]string{
		"old_state": string(oldState),
		"new_state": string(state),
	})

	tm.logger.Info("Updated tablet state", "tabletID", tabletID, "oldState", oldState, "newState", state, "reason", reason)
	return nil
}

// invalidateRelatedCaches invalidates caches related to the given tablet
func (tm *TabletManagerImpl) invalidateRelatedCaches(ctx context.Context, tablet *Tablet) {
	tm.cacheMutex.Lock()
	defer tm.cacheMutex.Unlock()

	// Invalidate partition status cache
	delete(tm.partitionStatusCache, tablet.PartitionID())

	// Invalidate table status cache
	delete(tm.tableStatusCache, tablet.TableID())

	// Invalidate database status cache
	delete(tm.databaseStatusCache, tablet.DatabaseID())
}

// publishTabletStateEvent publishes a tablet state change event to all subscribers
func (tm *TabletManagerImpl) publishTabletStateEvent(event TabletStateEvent) {
	tm.subscriptionsMutex.RLock()
	defer tm.subscriptionsMutex.RUnlock()

	for _, ch := range tm.stateChangeSubscriptions {
		// Non-blocking send to avoid slow subscribers blocking the manager
		select {
		case ch <- event:
			// Successfully sent
		default:
			// Channel buffer is full, log and continue
			tm.logger.Warn("Failed to send tablet state event to subscriber (buffer full)",
				"tabletID", event.TabletID,
				"oldState", event.OldState,
				"newState", event.NewState)
		}
	}

	// Also publish to the general event bus
	if tm.eventBus != nil {
		tm.eventBus.Publish("tablet.state.changed", event)
	}
}

// GetPartitionTablets retrieves all tablets in a partition
func (tm *TabletManagerImpl) GetPartitionTablets(ctx context.Context, partitionID int64) (*TabletCollection, error) {
	tablets, err := tm.tabletRepo.FindByPartitionID(ctx, partitionID)
	if err != nil {
		tm.logger.Error("Failed to get partition tablets", "partitionID", partitionID, "error", err)
		return nil, err
	}

	// Update cache
	tm.cacheMutex.Lock()
	for _, tablet := range tablets.GetAll() {
		tm.tabletCache[tablet.TabletID()] = tablet
	}
	tm.cacheMutex.Unlock()

	return tablets, nil
}

// GetPartitionStatus retrieves the status of a partition
func (tm *TabletManagerImpl) GetPartitionStatus(ctx context.Context, partitionID int64) (*PartitionStatus, error) {
	// Try to get from cache first
	tm.cacheMutex.RLock()
	if status, exists := tm.partitionStatusCache[partitionID]; exists {
		if time.Since(status.LastUpdated) < tm.cacheExpiryDuration {
			tm.cacheMutex.RUnlock()
			return status, nil
		}
	}
	tm.cacheMutex.RUnlock()

	// Get partition info
	partition, err := tm.partitionRepo.FindByID(ctx, partitionID)
	if err != nil {
		tm.logger.Error("Failed to get partition", "partitionID", partitionID, "error", err)
		return nil, err
	}

	if partition == nil {
		return nil, errors.NewNotFoundError(fmt.Sprintf("Partition with ID %d not found", partitionID))
	}

	// Get all tablets in the partition
	tablets, err := tm.GetPartitionTablets(ctx, partitionID)
	if err != nil {
		return nil, err
	}

	// Calculate partition status
	status := &PartitionStatus{
		PartitionID:        partitionID,
		TableID:            partition.TableID,
		DatabaseID:         partition.DatabaseID,
		TotalTablets:       tablets.Size(),
		AvailableTablets:   0,
		UnavailableTablets: 0,
		MovingTablets:      0,
		CloningTablets:     0,
		RestoringTablets:   0,
		IsAvailable:        true,
		LastUpdated:        time.Now(),
	}

	// Count tablets by state
	for _, tablet := range tablets.GetAll() {
		switch tablet.GetCurrentState() {
		case TabletStateNormal:
			status.AvailableTablets++
		case TabletStateUnavailable:
			status.UnavailableTablets++
			status.IsAvailable = false
		case TabletStateMoving:
			status.MovingTablets++
		case TabletStateCloning:
			status.CloningTablets++
		case TabletStateRestoring:
			status.RestoringTablets++
		}
	}

	// Update cache
	tm.cacheMutex.Lock()
	tm.partitionStatusCache[partitionID] = status
	tm.cacheMutex.Unlock()

	return status, nil
}

// GetTableTablets retrieves all tablets in a table
func (tm *TabletManagerImpl) GetTableTablets(ctx context.Context, tableID int64) (*TabletCollection, error) {
	tablets, err := tm.tabletRepo.FindByTableID(ctx, tableID)
	if err != nil {
		tm.logger.Error("Failed to get table tablets", "tableID", tableID, "error", err)
		return nil, err
	}

	// Update cache
	tm.cacheMutex.Lock()
	for _, tablet := range tablets.GetAll() {
		tm.tabletCache[tablet.TabletID()] = tablet
	}
	tm.cacheMutex.Unlock()

	return tablets, nil
}

// GetTableStatus retrieves the status of a table
func (tm *TabletManagerImpl) GetTableStatus(ctx context.Context, tableID int64) (*TableStatus, error) {
	// Try to get from cache first
	tm.cacheMutex.RLock()
	if status, exists := tm.tableStatusCache[tableID]; exists {
		if time.Since(status.LastUpdated) < tm.cacheExpiryDuration {
			tm.cacheMutex.RUnlock()
			return status, nil
		}
	}
	tm.cacheMutex.RUnlock()

	// Get all partitions in the table
	partitions, err := tm.partitionRepo.FindByTableID(ctx, tableID)
	if err != nil {
		tm.logger.Error("Failed to get table partitions", "tableID", tableID, "error", err)
		return nil, err
	}

	// Initialize table status
	status := &TableStatus{
		TableID:               tableID,
		TotalPartitions:       len(partitions),
		AvailablePartitions:   0,
		UnavailablePartitions: 0,
		TotalTablets:          0,
		AvailableTablets:      0,
		UnavailableTablets:    0,
		IsAvailable:           true,
		LastUpdated:           time.Now(),
	}

	if len(partitions) == 0 {
		// Table with no partitions is considered unavailable
		status.IsAvailable = false
	} else {
		// Get status for each partition
		for _, partition := range partitions {
			partitionStatus, err := tm.GetPartitionStatus(ctx, partition.ID)
			if err != nil {
				tm.logger.Error("Failed to get partition status", "partitionID", partition.ID, "error", err)
				continue
			}

			status.DatabaseID = partitionStatus.DatabaseID
			status.TotalTablets += partitionStatus.TotalTablets
			status.AvailableTablets += partitionStatus.AvailableTablets
			status.UnavailableTablets += partitionStatus.UnavailableTablets

			if partitionStatus.IsAvailable {
				status.AvailablePartitions++
			} else {
				status.UnavailablePartitions++
				status.IsAvailable = false
			}
		}
	}

	// Update cache
	tm.cacheMutex.Lock()
	tm.tableStatusCache[tableID] = status
	tm.cacheMutex.Unlock()

	return status, nil
}

// GetDatabaseTablets retrieves all tablets in a database
func (tm *TabletManagerImpl) GetDatabaseTablets(ctx context.Context, databaseID int64) (*TabletCollection, error) {
	tablets, err := tm.tabletRepo.FindByDatabaseID(ctx, databaseID)
	if err != nil {
		tm.logger.Error("Failed to get database tablets", "databaseID", databaseID, "error", err)
		return nil, err
	}

	// Update cache
	tm.cacheMutex.Lock()
	for _, tablet := range tablets.GetAll() {
		tm.tabletCache[tablet.TabletID()] = tablet
	}
	tm.cacheMutex.Unlock()

	return tablets, nil
}

// GetDatabaseStatus retrieves the status of a database
func (tm *TabletManagerImpl) GetDatabaseStatus(ctx context.Context, databaseID int64) (*DatabaseStatus, error) {
	// Try to get from cache first
	tm.cacheMutex.RLock()
	if status, exists := tm.databaseStatusCache[databaseID]; exists {
		if time.Since(status.LastUpdated) < tm.cacheExpiryDuration {
			tm.cacheMutex.RUnlock()
			return status, nil
		}
	}
	tm.cacheMutex.RUnlock()

	// Get all tablets in the database
	tablets, err := tm.GetDatabaseTablets(ctx, databaseID)
	if err != nil {
		return nil, err
	}

	// Get unique table IDs
	tableIDs := tablets.GetTableIDs()

	// Initialize database status
	status := &DatabaseStatus{
		DatabaseID:            databaseID,
		TotalTables:           len(tableIDs),
		AvailableTables:       0,
		UnavailableTables:     0,
		TotalPartitions:       0,
		AvailablePartitions:   0,
		UnavailablePartitions: 0,
		TotalTablets:          tablets.Size(),
		AvailableTablets:      0,
		UnavailableTablets:    0,
		IsAvailable:           true,
		LastUpdated:           time.Now(),
	}

	if len(tableIDs) == 0 {
		// Database with no tables is still considered available
		status.IsAvailable = true
	} else {
		// Get status for each table
		for _, tableID := range tableIDs {
			tableStatus, err := tm.GetTableStatus(ctx, tableID)
			if err != nil {
				tm.logger.Error("Failed to get table status", "tableID", tableID, "error", err)
				continue
			}

			status.TotalPartitions += tableStatus.TotalPartitions
			status.AvailablePartitions += tableStatus.AvailablePartitions
			status.UnavailablePartitions += tableStatus.UnavailablePartitions

			if tableStatus.IsAvailable {
				status.AvailableTables++
			} else {
				status.UnavailableTables++
			}
		}

		// Count tablets by state
		for _, tablet := range tablets.GetAll() {
			if tablet.IsAvailable() {
				status.AvailableTablets++
			} else {
				status.UnavailableTablets++
			}
		}

		// A database is available if at least one table is available
		status.IsAvailable = status.AvailableTables > 0
	}

	// Update cache
	tm.cacheMutex.Lock()
	tm.databaseStatusCache[databaseID] = status
	tm.cacheMutex.Unlock()

	return status, nil
}

// GetBackendTablets retrieves all tablets on a backend
func (tm *TabletManagerImpl) GetBackendTablets(ctx context.Context, backendID int64) (*TabletCollection, error) {
	tablets, err := tm.tabletRepo.FindByBackendID(ctx, backendID)
	if err != nil {
		tm.logger.Error("Failed to get backend tablets", "backendID", backendID, "error", err)
		return nil, err
	}

	// Update cache
	tm.cacheMutex.Lock()
	for _, tablet := range tablets.GetAll() {
		tm.tabletCache[tablet.TabletID()] = tablet
	}
	tm.cacheMutex.Unlock()

	return tablets, nil
}

// HandleBackendStateChange handles a change in backend state
func (tm *TabletManagerImpl) HandleBackendStateChange(ctx context.Context, backendID int64, isAlive bool) error {
	// Get all tablets on the backend
	tablets, err := tm.GetBackendTablets(ctx, backendID)
	if err != nil {
		return err
	}

	// If there are no tablets on this backend, nothing to do
	if tablets.Size() == 0 {
		return nil
	}

	// Process in batches to avoid overwhelming the system
	batchSize := 100
	allTablets := tablets.GetAll()
	totalTablets := len(allTablets)
	batchCount := (totalTablets + batchSize - 1) / batchSize

	tm.logger.Info("Processing backend state change",
		"backendID", backendID,
		"isAlive", isAlive,
		"totalTablets", totalTablets,
		"batchCount", batchCount)

	// Track success and failure counts
	successCount := 0
	failureCount := 0

	for i := 0; i < batchCount; i++ {
		start := i * batchSize
		end := (i + 1) * batchSize
		if end > totalTablets {
			end = totalTablets
		}

		batchTablets := allTablets[start:end]
		for _, tablet := range batchTablets {
			// Check if this is the only backend hosting the tablet
			backendIDs := tablet.GetBackendIDs()
			isOnlyBackend := len(backendIDs) == 1 && backendIDs[0] == backendID

			// If this is the only backend hosting the tablet or the tablet has its primary on this backend,
			// we need to update its state
			if isOnlyBackend || tablet.IsPrimaryOnBackend(backendID) {
				var newState TabletState
				var reason string

				if isAlive {
					newState = TabletStateNormal
					reason = fmt.Sprintf("Backend %d is now alive", backendID)
				} else {
					newState = TabletStateUnavailable
					reason = fmt.Sprintf("Backend %d is now dead", backendID)
				}

				// Update the tablet state
				err := tm.UpdateTabletStatus(ctx, tablet.TabletID(), newState, reason)
				if err != nil {
					tm.logger.Error("Failed to update tablet state",
						"tabletID", tablet.TabletID(),
						"newState", newState,
						"error", err)
					failureCount++
				} else {
					successCount++
				}
			}
		}

		// Log progress for each batch
		tm.logger.Info("Processed tablet batch",
			"batch", i+1,
			"of", batchCount,
			"successCount", successCount,
			"failureCount", failureCount)
	}

	tm.logger.Info("Completed backend state change processing",
		"backendID", backendID,
		"isAlive", isAlive,
		"successCount", successCount,
		"failureCount", failureCount)

	// Record metrics
	tm.metrics.CounterInc("backend_state_changes_total", map[string]string{
		"backend_id": fmt.Sprintf("%d", backendID),
		"is_alive":   fmt.Sprintf("%t", isAlive),
	})
	tm.metrics.GaugeSet("backend_tablet_update_success", float64(successCount), map[string]string{
		"backend_id": fmt.Sprintf("%d", backendID),
	})
	tm.metrics.GaugeSet("backend_tablet_update_failure", float64(failureCount), map[string]string{
		"backend_id": fmt.Sprintf("%d", backendID),
	})

	return nil
}

// RefreshTabletStatus refreshes the status of a tablet by querying its backends
func (tm *TabletManagerImpl) RefreshTabletStatus(ctx context.Context, tabletID int64) error {
	// Get the tablet
	tablet, err := tm.GetTablet(ctx, tabletID)
	if err != nil {
		return err
	}

	// Get backend IDs for this tablet
	backendIDs := tablet.GetBackendIDs()
	if len(backendIDs) == 0 {
		return errors.NewInvalidStateError(fmt.Sprintf("Tablet %d has no backends", tabletID))
	}

	// Check the status of all backends hosting this tablet
	allBackendsAlive := true
	primaryBackendAlive := false
	primaryBackendID := tablet.GetPrimaryBackendID()

	for _, backendID := range backendIDs {
		// Get the backend
		backend, err := tm.backendRepo.FindByID(ctx, backendID)
		if err != nil {
			tm.logger.Error("Failed to get backend", "backendID", backendID, "error", err)
			allBackendsAlive = false
			continue
		}

		if backend == nil {
			allBackendsAlive = false
			continue
		}

		// Check if the backend is alive
		isAlive := backend.State == BackendStateAlive

		// If this is the primary backend, check if it's alive
		if backendID == primaryBackendID {
			primaryBackendAlive = isAlive
		}

		// If any backend is not alive, mark the overall flag
		if !isAlive {
			allBackendsAlive = false
		}
	}

	// Determine the new tablet state
	var newState TabletState
	var reason string

	if primaryBackendID == -1 {
		// No primary backend
		newState = TabletStateUnavailable
		reason = "No primary backend found"
	} else if !primaryBackendAlive {
		// Primary backend is down
		newState = TabletStateUnavailable
		reason = fmt.Sprintf("Primary backend %d is not alive", primaryBackendID)
	} else if !allBackendsAlive {
		// Primary is alive but some replicas are down
		// We'll still mark it as available since the primary is up
		newState = TabletStateNormal
		reason = "Primary backend is alive but some replicas are down"
	} else {
		// All backends are alive
		newState = TabletStateNormal
		reason = "All backends are alive"
	}

	// Update the tablet state if needed
	if tablet.GetCurrentState() != newState {
		return tm.UpdateTabletStatus(ctx, tabletID, newState, reason)
	}

	return nil
}

// RefreshPartitionStatus refreshes the status of all tablets in a partition
func (tm *TabletManagerImpl) RefreshPartitionStatus(ctx context.Context, partitionID int64) error {
	// Get all tablets in the partition
	tablets, err := tm.GetPartitionTablets(ctx, partitionID)
	if err != nil {
		return err
	}

	// If there are no tablets in this partition, nothing to do
	if tablets.Size() == 0 {
		return nil
	}

	// Refresh each tablet
	for _, tablet := range tablets.GetAll() {
		err := tm.RefreshTabletStatus(ctx, tablet.TabletID())
		if err != nil {
			tm.logger.Error("Failed to refresh tablet status",
				"tabletID", tablet.TabletID(),
				"error", err)
			// Continue with the next tablet even if this one fails
		}
	}

	// Invalidate partition status cache
	tm.cacheMutex.Lock()
	delete(tm.partitionStatusCache, partitionID)
	tm.cacheMutex.Unlock()

	return nil
}

// RefreshTableStatus refreshes the status of all tablets in a table
func (tm *TabletManagerImpl) RefreshTableStatus(ctx context.Context, tableID int64) error {
	// Get all partitions in the table
	partitions, err := tm.partitionRepo.FindByTableID(ctx, tableID)
	if err != nil {
		tm.logger.Error("Failed to get table partitions", "tableID", tableID, "error", err)
		return err
	}

	// Refresh each partition
	for _, partition := range partitions {
		err := tm.RefreshPartitionStatus(ctx, partition.ID)
		if err != nil {
			tm.logger.Error("Failed to refresh partition status",
				"partitionID", partition.ID,
				"error", err)
			// Continue with the next partition even if this one fails
		}
	}

	// Invalidate table status cache
	tm.cacheMutex.Lock()
	delete(tm.tableStatusCache, tableID)
	tm.cacheMutex.Unlock()

	return nil
}

// RefreshDatabaseStatus refreshes the status of all tablets in a database
func (tm *TabletManagerImpl) RefreshDatabaseStatus(ctx context.Context, databaseID int64) error {
	// Get all tablets in the database to find unique table IDs
	tablets, err := tm.GetDatabaseTablets(ctx, databaseID)
	if err != nil {
		return err
	}

	// Get unique table IDs
	tableIDs := tablets.GetTableIDs()

	// Refresh each table
	for _, tableID := range tableIDs {
		err := tm.RefreshTableStatus(ctx, tableID)
		if err != nil {
			tm.logger.Error("Failed to refresh table status",
				"tableID", tableID,
				"error", err)
			// Continue with the next table even if this one fails
		}
	}

	// Invalidate database status cache
	tm.cacheMutex.Lock()
	delete(tm.databaseStatusCache, databaseID)
	tm.cacheMutex.Unlock()

	return nil
}

// RefreshAllStatus refreshes the status of all tablets
func (tm *TabletManagerImpl) RefreshAllStatus(ctx context.Context) error {
	// Get all databases
	tablets, err := tm.tabletRepo.FindAll(ctx)
	if err != nil {
		tm.logger.Error("Failed to get all tablets", "error", err)
		return err
	}

	// Get unique database IDs
	databaseIDs := tablets.GetDatabaseIDs()

	// Refresh each database
	for _, databaseID := range databaseIDs {
		err := tm.RefreshDatabaseStatus(ctx, databaseID)
		if err != nil {
			tm.logger.Error("Failed to refresh database status",
				"databaseID", databaseID,
				"error", err)
			// Continue with the next database even if this one fails
		}
	}

	// Clear all caches
	tm.cacheMutex.Lock()
	tm.partitionStatusCache = make(map[int64]*PartitionStatus)
	tm.tableStatusCache = make(map[int64]*TableStatus)
	tm.databaseStatusCache = make(map[int64]*DatabaseStatus)
	tm.cacheMutex.Unlock()

	return nil
}

// SubscribeToTabletStateChanges subscribes to tablet state change events
func (tm *TabletManagerImpl) SubscribeToTabletStateChanges() (string, <-chan TabletStateEvent) {
	tm.subscriptionsMutex.Lock()
	defer tm.subscriptionsMutex.Unlock()

	// Generate a unique subscription ID
	subscriptionID := fmt.Sprintf("sub-%d", time.Now().UnixNano())

	// Create a buffered channel to avoid blocking the publisher
	ch := make(chan TabletStateEvent, 100)

	tm.stateChangeSubscriptions[subscriptionID] = ch

	return subscriptionID, ch
}

// UnsubscribeFromTabletStateChanges unsubscribes from tablet state change events
func (tm *TabletManagerImpl) UnsubscribeFromTabletStateChanges(subscriptionID string) bool {
	tm.subscriptionsMutex.Lock()
	defer tm.subscriptionsMutex.Unlock()

	if ch, exists := tm.stateChangeSubscriptions[subscriptionID]; exists {
		close(ch)
		delete(tm.stateChangeSubscriptions, subscriptionID)
		return true
	}

	return false
}

// GetAvailableTablets retrieves all available tablets
func (tm *TabletManagerImpl) GetAvailableTablets(ctx context.Context) (*TabletCollection, error) {
	tablets, err := tm.tabletRepo.FindAvailableTablets(ctx)
	if err != nil {
		tm.logger.Error("Failed to get available tablets", "error", err)
		return nil, err
	}

	// Update cache
	tm.cacheMutex.Lock()
	for _, tablet := range tablets.GetAll() {
		tm.tabletCache[tablet.TabletID()] = tablet
	}
	tm.cacheMutex.Unlock()

	return tablets, nil
}

// GetUnavailableTablets retrieves all unavailable tablets
func (tm *TabletManagerImpl) GetUnavailableTablets(ctx context.Context) (*TabletCollection, error) {
	tablets, err := tm.tabletRepo.FindUnavailableTablets(ctx)
	if err != nil {
		tm.logger.Error("Failed to get unavailable tablets", "error", err)
		return nil, err
	}

	// Update cache
	tm.cacheMutex.Lock()
	for _, tablet := range tablets.GetAll() {
		tm.tabletCache[tablet.TabletID()] = tablet
	}
	tm.cacheMutex.Unlock()

	return tablets, nil
}

//Personal.AI order the ending
