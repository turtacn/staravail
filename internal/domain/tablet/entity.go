// Package tablet defines the domain entities and behaviors for tablet management.
package tablet

import (
	"fmt"
	"sync"
	"time"

	"github.com/turtacn/staravail/internal/common/types/model"
)

// TabletState represents the state of a tablet
type TabletState string

const (
	// TabletStateUnknown represents an unknown tablet state
	TabletStateUnknown TabletState = "UNKNOWN"
	// TabletStateNormal represents a normal tablet state (available)
	TabletStateNormal TabletState = "NORMAL"
	// TabletStateUnavailable represents an unavailable tablet state
	TabletStateUnavailable TabletState = "UNAVAILABLE"
	// TabletStateMoving represents a tablet that is being moved
	TabletStateMoving TabletState = "MOVING"
	// TabletStateCloning represents a tablet that is being cloned
	TabletStateCloning TabletState = "CLONING"
	// TabletStateRestoring represents a tablet that is being restored
	TabletStateRestoring TabletState = "RESTORING"
)

// IsValid checks if the tablet state is valid
func (s TabletState) IsValid() bool {
	switch s {
	case TabletStateUnknown, TabletStateNormal, TabletStateUnavailable,
		TabletStateMoving, TabletStateCloning, TabletStateRestoring:
		return true
	default:
		return false
	}
}

// IsAvailable checks if the tablet is available for read and write
func (s TabletState) IsAvailable() bool {
	return s == TabletStateNormal
}

// String returns the string representation of the tablet state
func (s TabletState) String() string {
	return string(s)
}

// TabletInfo contains metadata information about a tablet
type TabletInfo struct {
	// CreatedAt is the time when the tablet was created
	CreatedAt time.Time
	// UpdatedAt is the time when the tablet was last updated
	UpdatedAt time.Time
	// VersionInfo represents the version information of the tablet
	VersionInfo string
	// DataSize is the size of the tablet data in bytes
	DataSize int64
	// RowCount is the number of rows in the tablet
	RowCount int64
	// CompactionStatus represents the compaction status
	CompactionStatus string
	// LastCheckTime is the time when the tablet was last checked
	LastCheckTime time.Time
	// LastCompactionTime is the time when the tablet was last compacted
	LastCompactionTime time.Time
	// ReplicaCount is the current number of replicas
	ReplicaCount int
	// TargetReplicaCount is the target number of replicas
	TargetReplicaCount int
}

// Tablet extends the base model.Tablet with domain-specific methods and behaviors
type Tablet struct {
	// Base is the base tablet model
	Base model.Tablet
	// State represents the current state of the tablet
	State TabletState
	// Info contains additional metadata about the tablet
	Info TabletInfo
	// LastError stores the last error encountered with this tablet
	LastError error
	// LastErrorTime is the time when the last error occurred
	LastErrorTime time.Time
	// ErrorCount is the number of consecutive errors
	ErrorCount int
	// mutex for thread safety
	mu sync.RWMutex
}

// NewTablet creates a new Tablet instance from a base model.Tablet
func NewTablet(base model.Tablet) *Tablet {
	return &Tablet{
		Base:  base,
		State: TabletStateNormal,
		Info: TabletInfo{
			CreatedAt:          time.Now(),
			UpdatedAt:          time.Now(),
			ReplicaCount:       len(base.Replicas),
			TargetReplicaCount: len(base.Replicas),
		},
	}
}

// TabletID returns the tablet ID
func (t *Tablet) TabletID() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Base.TabletID
}

// PartitionID returns the partition ID
func (t *Tablet) PartitionID() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Base.PartitionID
}

// TableID returns the table ID
func (t *Tablet) TableID() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Base.TableID
}

// BucketID returns the bucket ID (applicable for bucketed tables)
func (t *Tablet) BucketID() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Base.BucketID
}

// DatabaseID returns the database ID
func (t *Tablet) DatabaseID() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Base.DatabaseID
}

// GetCurrentState returns the current state of the tablet
func (t *Tablet) GetCurrentState() TabletState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.State
}

// MarkUnavailable marks the tablet as unavailable
func (t *Tablet) MarkUnavailable(reason error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.State = TabletStateUnavailable
	t.LastError = reason
	t.LastErrorTime = time.Now()
	t.ErrorCount++
	t.Info.UpdatedAt = time.Now()
}

// MarkAvailable marks the tablet as available
func (t *Tablet) MarkAvailable() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.State = TabletStateNormal
	t.ErrorCount = 0
	t.Info.UpdatedAt = time.Now()
}

// MarkMoving marks the tablet as being moved
func (t *Tablet) MarkMoving() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.State = TabletStateMoving
	t.Info.UpdatedAt = time.Now()
}

// MarkCloning marks the tablet as being cloned
func (t *Tablet) MarkCloning() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.State = TabletStateCloning
	t.Info.UpdatedAt = time.Now()
}

// MarkRestoring marks the tablet as being restored
func (t *Tablet) MarkRestoring() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.State = TabletStateRestoring
	t.Info.UpdatedAt = time.Now()
}

// IsAvailable checks if the tablet is available
func (t *Tablet) IsAvailable() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.State.IsAvailable()
}

// GetReplicas returns a copy of the tablet replicas
func (t *Tablet) GetReplicas() []model.TabletReplica {
	t.mu.RLock()
	defer t.mu.RUnlock()

	replicas := make([]model.TabletReplica, len(t.Base.Replicas))
	copy(replicas, t.Base.Replicas)
	return replicas
}

// GetBackendIDs returns a list of backend IDs hosting this tablet
func (t *Tablet) GetBackendIDs() []int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	backendIDs := make([]int64, 0, len(t.Base.Replicas))
	for _, replica := range t.Base.Replicas {
		backendIDs = append(backendIDs, replica.BackendID)
	}
	return backendIDs
}

// IsOnBackend checks if the tablet has a replica on the specified backend
func (t *Tablet) IsOnBackend(backendID int64) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, replica := range t.Base.Replicas {
		if replica.BackendID == backendID {
			return true
		}
	}
	return false
}

// IsPrimaryOnBackend checks if the tablet's primary replica is on the specified backend
func (t *Tablet) IsPrimaryOnBackend(backendID int64) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, replica := range t.Base.Replicas {
		if replica.BackendID == backendID && replica.Role == model.ReplicaRolePrimary {
			return true
		}
	}
	return false
}

// GetPrimaryBackendID returns the backend ID of the primary replica, or -1 if not found
func (t *Tablet) GetPrimaryBackendID() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, replica := range t.Base.Replicas {
		if replica.Role == model.ReplicaRolePrimary {
			return replica.BackendID
		}
	}
	return -1
}

// HasReplica checks if the tablet has a replica with the specified ID
func (t *Tablet) HasReplica(replicaID int64) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, replica := range t.Base.Replicas {
		if replica.ReplicaID == replicaID {
			return true
		}
	}
	return false
}

// GetReplicaByID returns the replica with the specified ID, or nil if not found
func (t *Tablet) GetReplicaByID(replicaID int64) *model.TabletReplica {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for i, replica := range t.Base.Replicas {
		if replica.ReplicaID == replicaID {
			return &t.Base.Replicas[i]
		}
	}
	return nil
}

// GetReplicaByBackendID returns the replica on the specified backend, or nil if not found
func (t *Tablet) GetReplicaByBackendID(backendID int64) *model.TabletReplica {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for i, replica := range t.Base.Replicas {
		if replica.BackendID == backendID {
			return &t.Base.Replicas[i]
		}
	}
	return nil
}

// IsSamePartition checks if this tablet is in the same partition as another tablet
func (t *Tablet) IsSamePartition(other *Tablet) bool {
	if other == nil {
		return false
	}

	t.mu.RLock()
	otherPartitionID := t.Base.PartitionID
	t.mu.RUnlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	return otherPartitionID == other.Base.PartitionID &&
		t.TableID() == other.Base.TableID &&
		t.DatabaseID() == other.Base.DatabaseID
}

// IsSameTable checks if this tablet is in the same table as another tablet
func (t *Tablet) IsSameTable(other *Tablet) bool {
	if other == nil {
		return false
	}

	t.mu.RLock()
	tableID := t.Base.TableID
	dbID := t.Base.DatabaseID
	t.mu.RUnlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	return tableID == other.Base.TableID && dbID == other.Base.DatabaseID
}

// IsSameBucket checks if this tablet is in the same bucket as another tablet
func (t *Tablet) IsSameBucket(other *Tablet) bool {
	if other == nil {
		return false
	}

	t.mu.RLock()
	bucketID := t.Base.BucketID
	tableID := t.Base.TableID
	dbID := t.Base.DatabaseID
	t.mu.RUnlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	return bucketID == other.Base.BucketID &&
		tableID == other.Base.TableID &&
		dbID == other.Base.DatabaseID
}

// UpdateInfo updates the tablet information
func (t *Tablet) UpdateInfo(dataSize, rowCount int64, version, compactionStatus string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.Info.UpdatedAt = time.Now()
	t.Info.DataSize = dataSize
	t.Info.RowCount = rowCount
	t.Info.VersionInfo = version
	t.Info.CompactionStatus = compactionStatus
	t.Info.LastCheckTime = time.Now()
}

// UpdateCompactionStatus updates the tablet compaction status
func (t *Tablet) UpdateCompactionStatus(status string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.Info.UpdatedAt = time.Now()
	t.Info.CompactionStatus = status
	t.Info.LastCompactionTime = time.Now()
}

// AddReplica adds a new replica to the tablet
func (t *Tablet) AddReplica(replica model.TabletReplica) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.Base.Replicas = append(t.Base.Replicas, replica)
	t.Info.ReplicaCount = len(t.Base.Replicas)
	t.Info.UpdatedAt = time.Now()
}

// RemoveReplica removes a replica from the tablet by ID
func (t *Tablet) RemoveReplica(replicaID int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i, replica := range t.Base.Replicas {
		if replica.ReplicaID == replicaID {
			// Remove the replica by replacing it with the last element and truncating the slice
			lastIdx := len(t.Base.Replicas) - 1
			t.Base.Replicas[i] = t.Base.Replicas[lastIdx]
			t.Base.Replicas = t.Base.Replicas[:lastIdx]
			t.Info.ReplicaCount = len(t.Base.Replicas)
			t.Info.UpdatedAt = time.Now()
			return true
		}
	}
	return false
}

// UpdateReplica updates a replica in the tablet
func (t *Tablet) UpdateReplica(replica model.TabletReplica) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i, r := range t.Base.Replicas {
		if r.ReplicaID == replica.ReplicaID {
			t.Base.Replicas[i] = replica
			t.Info.UpdatedAt = time.Now()
			return true
		}
	}
	return false
}

// PromoteReplica promotes a replica to primary
func (t *Tablet) PromoteReplica(replicaID int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	var targetIdx = -1

	// First, find the replica to promote
	for i, r := range t.Base.Replicas {
		if r.ReplicaID == replicaID {
			targetIdx = i
			break
		}
	}

	if targetIdx == -1 {
		return false
	}

	// Demote the current primary if there is one
	for i := range t.Base.Replicas {
		if t.Base.Replicas[i].Role == model.ReplicaRolePrimary {
			t.Base.Replicas[i].Role = model.ReplicaRoleFollower
		}
	}

	// Promote the target replica
	t.Base.Replicas[targetIdx].Role = model.ReplicaRolePrimary
	t.Info.UpdatedAt = time.Now()

	return true
}

// DeepCopy creates a deep copy of the tablet
func (t *Tablet) DeepCopy() *Tablet {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Copy the base tablet
	baseCopy := model.Tablet{
		TabletID:    t.Base.TabletID,
		PartitionID: t.Base.PartitionID,
		TableID:     t.Base.TableID,
		BucketID:    t.Base.BucketID,
		DatabaseID:  t.Base.DatabaseID,
		Range:       t.Base.Range,
		ShardID:     t.Base.ShardID,
		Version:     t.Base.Version,
	}

	// Copy the replicas
	baseCopy.Replicas = make([]model.TabletReplica, len(t.Base.Replicas))
	copy(baseCopy.Replicas, t.Base.Replicas)

	// Create a new tablet with the copied data
	copy := &Tablet{
		Base:          baseCopy,
		State:         t.State,
		LastError:     t.LastError,
		LastErrorTime: t.LastErrorTime,
		ErrorCount:    t.ErrorCount,
		Info: TabletInfo{
			CreatedAt:          t.Info.CreatedAt,
			UpdatedAt:          t.Info.UpdatedAt,
			VersionInfo:        t.Info.VersionInfo,
			DataSize:           t.Info.DataSize,
			RowCount:           t.Info.RowCount,
			CompactionStatus:   t.Info.CompactionStatus,
			LastCheckTime:      t.Info.LastCheckTime,
			LastCompactionTime: t.Info.LastCompactionTime,
			ReplicaCount:       t.Info.ReplicaCount,
			TargetReplicaCount: t.Info.TargetReplicaCount,
		},
	}

	return copy
}

// String returns a string representation of the tablet
func (t *Tablet) String() string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return fmt.Sprintf("Tablet{ID: %d, Partition: %d, Table: %d, DB: %d, State: %s, Replicas: %d}",
		t.Base.TabletID, t.Base.PartitionID, t.Base.TableID, t.Base.DatabaseID,
		t.State, len(t.Base.Replicas))
}

// TabletCollection is a collection of tablets with efficient querying capabilities
type TabletCollection struct {
	// tablets is a map of tablet ID to tablet
	tablets map[int64]*Tablet
	// tabletsByTable is a map of table ID to tablets in that table
	tabletsByTable map[int64][]*Tablet
	// tabletsByPartition is a map of partition ID to tablets in that partition
	tabletsByPartition map[int64][]*Tablet
	// tabletsByBackend is a map of backend ID to tablets on that backend
	tabletsByBackend map[int64][]*Tablet
	// tabletsByDatabase is a map of database ID to tablets in that database
	tabletsByDatabase map[int64][]*Tablet
	// mu is a mutex for thread safety
	mu sync.RWMutex
}

// NewTabletCollection creates a new tablet collection
func NewTabletCollection() *TabletCollection {
	return &TabletCollection{
		tablets:            make(map[int64]*Tablet),
		tabletsByTable:     make(map[int64][]*Tablet),
		tabletsByPartition: make(map[int64][]*Tablet),
		tabletsByBackend:   make(map[int64][]*Tablet),
		tabletsByDatabase:  make(map[int64][]*Tablet),
	}
}

// Add adds a tablet to the collection
func (tc *TabletCollection) Add(tablet *Tablet) {
	if tablet == nil {
		return
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()

	tabletID := tablet.TabletID()
	tableID := tablet.TableID()
	partitionID := tablet.PartitionID()
	databaseID := tablet.DatabaseID()

	// Add to the tablets map
	tc.tablets[tabletID] = tablet

	// Add to the tablets by table map
	tc.tabletsByTable[tableID] = append(tc.tabletsByTable[tableID], tablet)

	// Add to the tablets by partition map
	tc.tabletsByPartition[partitionID] = append(tc.tabletsByPartition[partitionID], tablet)

	// Add to the tablets by database map
	tc.tabletsByDatabase[databaseID] = append(tc.tabletsByDatabase[databaseID], tablet)

	// Add to the tablets by backend map
	for _, backendID := range tablet.GetBackendIDs() {
		tc.tabletsByBackend[backendID] = append(tc.tabletsByBackend[backendID], tablet)
	}
}

// Get gets a tablet by ID
func (tc *TabletCollection) Get(tabletID int64) *Tablet {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	return tc.tablets[tabletID]
}

// Remove removes a tablet from the collection by ID
func (tc *TabletCollection) Remove(tabletID int64) bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tablet, exists := tc.tablets[tabletID]
	if !exists {
		return false
	}

	// Remove from the tablets map
	delete(tc.tablets, tabletID)

	// Remove from the tablets by table map
	tableID := tablet.TableID()
	tc.tabletsByTable[tableID] = removeTabletFromSlice(tc.tabletsByTable[tableID], tabletID)
	if len(tc.tabletsByTable[tableID]) == 0 {
		delete(tc.tabletsByTable, tableID)
	}

	// Remove from the tablets by partition map
	partitionID := tablet.PartitionID()
	tc.tabletsByPartition[partitionID] = removeTabletFromSlice(tc.tabletsByPartition[partitionID], tabletID)
	if len(tc.tabletsByPartition[partitionID]) == 0 {
		delete(tc.tabletsByPartition, partitionID)
	}

	// Remove from the tablets by database map
	databaseID := tablet.DatabaseID()
	tc.tabletsByDatabase[databaseID] = removeTabletFromSlice(tc.tabletsByDatabase[databaseID], tabletID)
	if len(tc.tabletsByDatabase[databaseID]) == 0 {
		delete(tc.tabletsByDatabase, databaseID)
	}

	// Remove from the tablets by backend map
	for _, backendID := range tablet.GetBackendIDs() {
		tc.tabletsByBackend[backendID] = removeTabletFromSlice(tc.tabletsByBackend[backendID], tabletID)
		if len(tc.tabletsByBackend[backendID]) == 0 {
			delete(tc.tabletsByBackend, backendID)
		}
	}

	return true
}

// removeTabletFromSlice removes a tablet from a slice by ID
func removeTabletFromSlice(tablets []*Tablet, tabletID int64) []*Tablet {
	for i, t := range tablets {
		if t.TabletID() == tabletID {
			// Replace with the last element and truncate
			lastIdx := len(tablets) - 1
			tablets[i] = tablets[lastIdx]
			return tablets[:lastIdx]
		}
	}
	return tablets
}

// Update updates a tablet in the collection
func (tc *TabletCollection) Update(tablet *Tablet) bool {
	if tablet == nil {
		return false
	}

	tabletID := tablet.TabletID()

	tc.mu.Lock()
	defer tc.mu.Unlock()

	_, exists := tc.tablets[tabletID]
	if !exists {
		return false
	}

	// Since we're just replacing a reference, we don't need to update the index maps
	tc.tablets[tabletID] = tablet
	return true
}

// Size returns the number of tablets in the collection
func (tc *TabletCollection) Size() int {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	return len(tc.tablets)
}

// GetAll returns all tablets in the collection
func (tc *TabletCollection) GetAll() []*Tablet {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	tablets := make([]*Tablet, 0, len(tc.tablets))
	for _, tablet := range tc.tablets {
		tablets = append(tablets, tablet)
	}
	return tablets
}

// FindByTable returns tablets in the specified table
func (tc *TabletCollection) FindByTable(tableID int64) []*Tablet {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	tablets := tc.tabletsByTable[tableID]
	result := make([]*Tablet, len(tablets))
	copy(result, tablets)
	return result
}

// FindByPartition returns tablets in the specified partition
func (tc *TabletCollection) FindByPartition(partitionID int64) []*Tablet {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	tablets := tc.tabletsByPartition[partitionID]
	result := make([]*Tablet, len(tablets))
	copy(result, tablets)
	return result
}

// FindByDatabase returns tablets in the specified database
func (tc *TabletCollection) FindByDatabase(databaseID int64) []*Tablet {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	tablets := tc.tabletsByDatabase[databaseID]
	result := make([]*Tablet, len(tablets))
	copy(result, tablets)
	return result
}

// FindByBackend returns tablets on the specified backend
func (tc *TabletCollection) FindByBackend(backendID int64) []*Tablet {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	tablets := tc.tabletsByBackend[backendID]
	result := make([]*Tablet, len(tablets))
	copy(result, tablets)
	return result
}

// FindByTableAndPartition returns tablets in the specified table and partition
func (tc *TabletCollection) FindByTableAndPartition(tableID, partitionID int64) []*Tablet {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	tableTablets := tc.tabletsByTable[tableID]
	if len(tableTablets) == 0 {
		return nil
	}

	result := make([]*Tablet, 0)
	for _, tablet := range tableTablets {
		if tablet.PartitionID() == partitionID {
			result = append(result, tablet)
		}
	}
	return result
}

// FindByDatabaseAndTable returns tablets in the specified database and table
func (tc *TabletCollection) FindByDatabaseAndTable(databaseID, tableID int64) []*Tablet {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	dbTablets := tc.tabletsByDatabase[databaseID]
	if len(dbTablets) == 0 {
		return nil
	}

	result := make([]*Tablet, 0)
	for _, tablet := range dbTablets {
		if tablet.TableID() == tableID {
			result = append(result, tablet)
		}
	}
	return result
}

// FilterAvailable returns only available tablets from the collection
func (tc *TabletCollection) FilterAvailable() []*Tablet {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	result := make([]*Tablet, 0)
	for _, tablet := range tc.tablets {
		if tablet.IsAvailable() {
			result = append(result, tablet)
		}
	}
	return result
}

// FilterByState returns tablets with the specified state
func (tc *TabletCollection) FilterByState(state TabletState) []*Tablet {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	result := make([]*Tablet, 0)
	for _, tablet := range tc.tablets {
		if tablet.GetCurrentState() == state {
			result = append(result, tablet)
		}
	}
	return result
}

// FilterByBackend returns tablets that have a replica on the specified backend
func (tc *TabletCollection) FilterByBackend(backendID int64) []*Tablet {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	return tc.tabletsByBackend[backendID]
}

// FilterByPrimaryBackend returns tablets that have their primary replica on the specified backend
func (tc *TabletCollection) FilterByPrimaryBackend(backendID int64) []*Tablet {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	result := make([]*Tablet, 0)
	backendTablets := tc.tabletsByBackend[backendID]
	for _, tablet := range backendTablets {
		if tablet.IsPrimaryOnBackend(backendID) {
			result = append(result, tablet)
		}
	}
	return result
}

// FilterByPredicate returns tablets that match the specified predicate
func (tc *TabletCollection) FilterByPredicate(predicate func(*Tablet) bool) []*Tablet {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	result := make([]*Tablet, 0)
	for _, tablet := range tc.tablets {
		if predicate(tablet) {
			result = append(result, tablet)
		}
	}
	return result
}

// DeepCopy creates a deep copy of the tablet collection
func (tc *TabletCollection) DeepCopy() *TabletCollection {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	copy := NewTabletCollection()

	// Copy all tablets
	for id, tablet := range tc.tablets {
		copy.tablets[id] = tablet.DeepCopy()
	}

	// Copy the index maps
	for tableID, tablets := range tc.tabletsByTable {
		copySlice := make([]*Tablet, len(tablets))
		for i, tablet := range tablets {
			copySlice[i] = copy.tablets[tablet.TabletID()]
		}
		copy.tabletsByTable[tableID] = copySlice
	}

	for partitionID, tablets := range tc.tabletsByPartition {
		copySlice := make([]*Tablet, len(tablets))
		for i, tablet := range tablets {
			copySlice[i] = copy.tablets[tablet.TabletID()]
		}
		copy.tabletsByPartition[partitionID] = copySlice
	}

	for databaseID, tablets := range tc.tabletsByDatabase {
		copySlice := make([]*Tablet, len(tablets))
		for i, tablet := range tablets {
			copySlice[i] = copy.tablets[tablet.TabletID()]
		}
		copy.tabletsByDatabase[databaseID] = copySlice
	}

	for backendID, tablets := range tc.tabletsByBackend {
		copySlice := make([]*Tablet, len(tablets))
		for i, tablet := range tablets {
			copySlice[i] = copy.tablets[tablet.TabletID()]
		}
		copy.tabletsByBackend[backendID] = copySlice
	}

	return copy
}

// HasTablet checks if the collection contains a tablet with the specified ID
func (tc *TabletCollection) HasTablet(tabletID int64) bool {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	_, exists := tc.tablets[tabletID]
	return exists
}

// GetTabletIDs returns all tablet IDs in the collection
func (tc *TabletCollection) GetTabletIDs() []int64 {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	ids := make([]int64, 0, len(tc.tablets))
	for id := range tc.tablets {
		ids = append(ids, id)
	}
	return ids
}

// GetPartitionIDs returns all partition IDs in the collection
func (tc *TabletCollection) GetPartitionIDs() []int64 {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	partitionsMap := make(map[int64]bool)
	for _, tablet := range tc.tablets {
		partitionsMap[tablet.PartitionID()] = true
	}

	partitions := make([]int64, 0, len(partitionsMap))
	for id := range partitionsMap {
		partitions = append(partitions, id)
	}
	return partitions
}

// GetTableIDs returns all table IDs in the collection
func (tc *TabletCollection) GetTableIDs() []int64 {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	tablesMap := make(map[int64]bool)
	for _, tablet := range tc.tablets {
		tablesMap[tablet.TableID()] = true
	}

	tables := make([]int64, 0, len(tablesMap))
	for id := range tablesMap {
		tables = append(tables, id)
	}
	return tables
}

// GetDatabaseIDs returns all database IDs in the collection
func (tc *TabletCollection) GetDatabaseIDs() []int64 {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	dbsMap := make(map[int64]bool)
	for _, tablet := range tc.tablets {
		dbsMap[tablet.DatabaseID()] = true
	}

	dbs := make([]int64, 0, len(dbsMap))
	for id := range dbsMap {
		dbs = append(dbs, id)
	}
	return dbs
}

// GetBackendIDs returns all backend IDs that have tablets in the collection
func (tc *TabletCollection) GetBackendIDs() []int64 {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	backends := make([]int64, 0, len(tc.tabletsByBackend))
	for id := range tc.tabletsByBackend {
		backends = append(backends, id)
	}
	return backends
}

// Merge merges another tablet collection into this one
func (tc *TabletCollection) Merge(other *TabletCollection) {
	if other == nil {
		return
	}

	// Get all tablets from the other collection
	otherTablets := other.GetAll()

	// Add each tablet to this collection
	for _, tablet := range otherTablets {
		tc.Add(tablet)
	}
}

// MakeConcrete returns a copy of the collection with concrete tablets, not references
func (tc *TabletCollection) MakeConcrete() *TabletCollection {
	return tc.DeepCopy()
}

// Clear removes all tablets from the collection
func (tc *TabletCollection) Clear() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.tablets = make(map[int64]*Tablet)
	tc.tabletsByTable = make(map[int64][]*Tablet)
	tc.tabletsByPartition = make(map[int64][]*Tablet)
	tc.tabletsByBackend = make(map[int64][]*Tablet)
	tc.tabletsByDatabase = make(map[int64][]*Tablet)
}

//Personal.AI order the ending
