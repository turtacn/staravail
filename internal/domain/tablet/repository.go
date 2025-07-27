// Package tablet defines domain entities and repository interfaces for tablet management.
package tablet

import (
	"context"
	"time"
)

// TabletRepository defines the interface for tablet persistence operations.
type TabletRepository interface {
	// FindByID retrieves a tablet by its ID.
	// Returns nil if the tablet is not found.
	// Returns an error if the operation fails.
	FindByID(ctx context.Context, tabletID int64) (*Tablet, error)

	// FindByIDs retrieves tablets by their IDs.
	// Returns an empty collection if no tablets are found.
	// Returns an error if the operation fails.
	FindByIDs(ctx context.Context, tabletIDs []int64) (*TabletCollection, error)

	// FindByPartitionID retrieves all tablets in a partition.
	// Returns an empty collection if no tablets are found.
	// Returns an error if the operation fails.
	FindByPartitionID(ctx context.Context, partitionID int64) (*TabletCollection, error)

	// FindByTableID retrieves all tablets in a table.
	// Returns an empty collection if no tablets are found.
	// Returns an error if the operation fails.
	FindByTableID(ctx context.Context, tableID int64) (*TabletCollection, error)

	// FindByDatabaseID retrieves all tablets in a database.
	// Returns an empty collection if no tablets are found.
	// Returns an error if the operation fails.
	FindByDatabaseID(ctx context.Context, databaseID int64) (*TabletCollection, error)

	// FindByBackendID retrieves all tablets hosted on a specific backend.
	// Returns an empty collection if no tablets are found.
	// Returns an error if the operation fails.
	FindByBackendID(ctx context.Context, backendID int64) (*TabletCollection, error)

	// FindByBackendIDs retrieves all tablets hosted on the specified backends.
	// Returns an empty collection if no tablets are found.
	// Returns an error if the operation fails.
	FindByBackendIDs(ctx context.Context, backendIDs []int64) (*TabletCollection, error)

	// FindByBucketID retrieves all tablets in a specific bucket.
	// Returns an empty collection if no tablets are found.
	// Returns an error if the operation fails.
	FindByBucketID(ctx context.Context, tableID, bucketID int64) (*TabletCollection, error)

	// FindByShardID retrieves all tablets in a specific shard.
	// Returns an empty collection if no tablets are found.
	// Returns an error if the operation fails.
	FindByShardID(ctx context.Context, shardID string) (*TabletCollection, error)

	// FindByRange retrieves all tablets that cover a specific range.
	// Returns an empty collection if no tablets are found.
	// Returns an error if the operation fails.
	FindByRange(ctx context.Context, tableID int64, startKey, endKey []byte) (*TabletCollection, error)

	// FindByState retrieves all tablets in a specific state.
	// Returns an empty collection if no tablets are found.
	// Returns an error if the operation fails.
	FindByState(ctx context.Context, state TabletState) (*TabletCollection, error)

	// FindAll retrieves all tablets.
	// Returns an empty collection if no tablets are found.
	// Returns an error if the operation fails.
	FindAll(ctx context.Context) (*TabletCollection, error)

	// FindAvailableTablets retrieves all available tablets.
	// Returns an empty collection if no tablets are found.
	// Returns an error if the operation fails.
	FindAvailableTablets(ctx context.Context) (*TabletCollection, error)

	// FindUnavailableTablets retrieves all unavailable tablets.
	// Returns an empty collection if no tablets are found.
	// Returns an error if the operation fails.
	FindUnavailableTablets(ctx context.Context) (*TabletCollection, error)

	// FindMovingTablets retrieves all tablets that are currently being moved.
	// Returns an empty collection if no tablets are found.
	// Returns an error if the operation fails.
	FindMovingTablets(ctx context.Context) (*TabletCollection, error)

	// FindCloningTablets retrieves all tablets that are currently being cloned.
	// Returns an empty collection if no tablets are found.
	// Returns an error if the operation fails.
	FindCloningTablets(ctx context.Context) (*TabletCollection, error)

	// FindRestoringTablets retrieves all tablets that are currently being restored.
	// Returns an empty collection if no tablets are found.
	// Returns an error if the operation fails.
	FindRestoringTablets(ctx context.Context) (*TabletCollection, error)

	// FindTabletsByVersion retrieves all tablets with a specific version.
	// Returns an empty collection if no tablets are found.
	// Returns an error if the operation fails.
	FindTabletsByVersion(ctx context.Context, version int64) (*TabletCollection, error)

	// FindReplicasByBackendID retrieves all tablet replicas hosted on a specific backend.
	// Returns an empty map if no replicas are found.
	// The map keys are tablet IDs and values are the corresponding replicas.
	// Returns an error if the operation fails.
	FindReplicasByBackendID(ctx context.Context, backendID int64) (map[int64][]TabletReplica, error)

	// Save persists a tablet.
	// Returns an error if the operation fails.
	Save(ctx context.Context, tablet *Tablet) error

	// SaveAll persists multiple tablets.
	// Returns an error if the operation fails.
	SaveAll(ctx context.Context, tablets []*Tablet) error

	// SaveCollection persists a tablet collection.
	// Returns an error if the operation fails.
	SaveCollection(ctx context.Context, collection *TabletCollection) error

	// Delete removes a tablet by its ID.
	// Returns true if the tablet was deleted, false if it was not found.
	// Returns an error if the operation fails.
	Delete(ctx context.Context, tabletID int64) (bool, error)

	// DeleteAll removes multiple tablets by their IDs.
	// Returns the number of tablets deleted.
	// Returns an error if the operation fails.
	DeleteAll(ctx context.Context, tabletIDs []int64) (int, error)

	// UpdateState updates the state of a tablet.
	// Returns an error if the operation fails.
	UpdateState(ctx context.Context, tabletID int64, state TabletState) error

	// UpdateReplicas updates the replicas of a tablet.
	// Returns an error if the operation fails.
	UpdateReplicas(ctx context.Context, tabletID int64, replicas []TabletReplica) error

	// AddReplica adds a replica to a tablet.
	// Returns an error if the operation fails.
	AddReplica(ctx context.Context, tabletID int64, replica TabletReplica) error

	// RemoveReplica removes a replica from a tablet.
	// Returns true if the replica was removed, false if it was not found.
	// Returns an error if the operation fails.
	RemoveReplica(ctx context.Context, tabletID, replicaID int64) (bool, error)

	// CountByTableID counts the number of tablets in a table.
	// Returns an error if the operation fails.
	CountByTableID(ctx context.Context, tableID int64) (int, error)

	// CountByPartitionID counts the number of tablets in a partition.
	// Returns an error if the operation fails.
	CountByPartitionID(ctx context.Context, partitionID int64) (int, error)

	// CountByDatabaseID counts the number of tablets in a database.
	// Returns an error if the operation fails.
	CountByDatabaseID(ctx context.Context, databaseID int64) (int, error)

	// CountByBackendID counts the number of tablets hosted on a specific backend.
	// Returns an error if the operation fails.
	CountByBackendID(ctx context.Context, backendID int64) (int, error)

	// CountByState counts the number of tablets in a specific state.
	// Returns an error if the operation fails.
	CountByState(ctx context.Context, state TabletState) (int, error)

	// CountAll counts the total number of tablets.
	// Returns an error if the operation fails.
	CountAll(ctx context.Context) (int, error)
}

// TabletReplica represents a replica of a tablet on a specific backend.
type TabletReplica struct {
	// ReplicaID is the unique ID of the replica
	ReplicaID int64
	// TabletID is the ID of the tablet that this replica belongs to
	TabletID int64
	// BackendID is the ID of the backend hosting this replica
	BackendID int64
	// Role is the role of this replica (primary, follower, etc.)
	Role string
	// State is the state of this replica
	State string
	// Version is the version of this replica
	Version int64
	// LastHeartbeat is the timestamp of the last heartbeat received from this replica
	LastHeartbeat time.Time
	// Path is the storage path of this replica on the backend
	Path string
	// Size is the size of this replica in bytes
	Size int64
	// RowCount is the number of rows in this replica
	RowCount int64
}

// PartitionRepository defines the interface for partition persistence operations.
type PartitionRepository interface {
	// FindByID retrieves a partition by its ID.
	// Returns nil if the partition is not found.
	// Returns an error if the operation fails.
	FindByID(ctx context.Context, partitionID int64) (*Partition, error)

	// FindByIDs retrieves partitions by their IDs.
	// Returns an empty slice if no partitions are found.
	// Returns an error if the operation fails.
	FindByIDs(ctx context.Context, partitionIDs []int64) ([]*Partition, error)

	// FindByTableID retrieves all partitions in a table.
	// Returns an empty slice if no partitions are found.
	// Returns an error if the operation fails.
	FindByTableID(ctx context.Context, tableID int64) ([]*Partition, error)

	// FindByDatabaseID retrieves all partitions in a database.
	// Returns an empty slice if no partitions are found.
	// Returns an error if the operation fails.
	FindByDatabaseID(ctx context.Context, databaseID int64) ([]*Partition, error)

	// FindByKey retrieves the partition that contains the specified key.
	// Returns nil if no partition is found.
	// Returns an error if the operation fails.
	FindByKey(ctx context.Context, tableID int64, key []byte) (*Partition, error)

	// FindByRange retrieves all partitions that overlap with the specified range.
	// Returns an empty slice if no partitions are found.
	// Returns an error if the operation fails.
	FindByRange(ctx context.Context, tableID int64, startKey, endKey []byte) ([]*Partition, error)

	// FindByName retrieves a partition by its name.
	// Returns nil if the partition is not found.
	// Returns an error if the operation fails.
	FindByName(ctx context.Context, tableID int64, name string) (*Partition, error)

	// FindAll retrieves all partitions.
	// Returns an empty slice if no partitions are found.
	// Returns an error if the operation fails.
	FindAll(ctx context.Context) ([]*Partition, error)

	// Save persists a partition.
	// Returns an error if the operation fails.
	Save(ctx context.Context, partition *Partition) error

	// SaveAll persists multiple partitions.
	// Returns an error if the operation fails.
	SaveAll(ctx context.Context, partitions []*Partition) error

	// Delete removes a partition by its ID.
	// Returns true if the partition was deleted, false if it was not found.
	// Returns an error if the operation fails.
	Delete(ctx context.Context, partitionID int64) (bool, error)

	// DeleteByTableID removes all partitions in a table.
	// Returns the number of partitions deleted.
	// Returns an error if the operation fails.
	DeleteByTableID(ctx context.Context, tableID int64) (int, error)

	// CountByTableID counts the number of partitions in a table.
	// Returns an error if the operation fails.
	CountByTableID(ctx context.Context, tableID int64) (int, error)

	// CountByDatabaseID counts the number of partitions in a database.
	// Returns an error if the operation fails.
	CountByDatabaseID(ctx context.Context, databaseID int64) (int, error)

	// CountAll counts the total number of partitions.
	// Returns an error if the operation fails.
	CountAll(ctx context.Context) (int, error)
}

// Partition represents a partition of a table.
type Partition struct {
	// ID is the unique ID of the partition
	ID int64
	// TableID is the ID of the table that this partition belongs to
	TableID int64
	// DatabaseID is the ID of the database that this partition belongs to
	DatabaseID int64
	// Name is the name of the partition
	Name string
	// Type is the type of the partition (range, hash, list, etc.)
	Type string
	// StartKey is the inclusive start key of the partition
	StartKey []byte
	// EndKey is the exclusive end key of the partition
	EndKey []byte
	// Value is the partition value (for list partitions)
	Value string
	// CreateTime is the time when the partition was created
	CreateTime time.Time
	// UpdateTime is the time when the partition was last updated
	UpdateTime time.Time
	// TabletCount is the number of tablets in this partition
	TabletCount int
	// DataSize is the total size of data in this partition in bytes
	DataSize int64
	// RowCount is the total number of rows in this partition
	RowCount int64
}

// BackendRepository defines the interface for backend persistence operations.
type BackendRepository interface {
	// FindByID retrieves a backend by its ID.
	// Returns nil if the backend is not found.
	// Returns an error if the operation fails.
	FindByID(ctx context.Context, backendID int64) (*Backend, error)

	// FindByIDs retrieves backends by their IDs.
	// Returns an empty slice if no backends are found.
	// Returns an error if the operation fails.
	FindByIDs(ctx context.Context, backendIDs []int64) ([]*Backend, error)

	// FindByHost retrieves a backend by its host address.
	// Returns nil if the backend is not found.
	// Returns an error if the operation fails.
	FindByHost(ctx context.Context, host string) (*Backend, error)

	// FindByHostPort retrieves a backend by its host and port.
	// Returns nil if the backend is not found.
	// Returns an error if the operation fails.
	FindByHostPort(ctx context.Context, host string, port int) (*Backend, error)

	// FindAll retrieves all backends.
	// Returns an empty slice if no backends are found.
	// Returns an error if the operation fails.
	FindAll(ctx context.Context) ([]*Backend, error)

	// FindAlive retrieves all alive backends.
	// Returns an empty slice if no backends are found.
	// Returns an error if the operation fails.
	FindAlive(ctx context.Context) ([]*Backend, error)

	// FindDead retrieves all dead backends.
	// Returns an empty slice if no backends are found.
	// Returns an error if the operation fails.
	FindDead(ctx context.Context) ([]*Backend, error)

	// FindByState retrieves all backends in a specific state.
	// Returns an empty slice if no backends are found.
	// Returns an error if the operation fails.
	FindByState(ctx context.Context, state BackendState) ([]*Backend, error)

	// FindByCluster retrieves all backends in a specific cluster.
	// Returns an empty slice if no backends are found.
	// Returns an error if the operation fails.
	FindByCluster(ctx context.Context, clusterID string) ([]*Backend, error)

	// FindByTag retrieves all backends with a specific tag.
	// Returns an empty slice if no backends are found.
	// Returns an error if the operation fails.
	FindByTag(ctx context.Context, tag string) ([]*Backend, error)

	// FindByCapacity retrieves all backends with available capacity.
	// The availableBytes parameter specifies the minimum available bytes required.
	// Returns an empty slice if no backends are found.
	// Returns an error if the operation fails.
	FindByCapacity(ctx context.Context, availableBytes int64) ([]*Backend, error)

	// FindByTabletCount retrieves backends ordered by tablet count.
	// If ascending is true, returns backends with the least tablets first.
	// If ascending is false, returns backends with the most tablets first.
	// The limit parameter specifies the maximum number of backends to return.
	// Returns an empty slice if no backends are found.
	// Returns an error if the operation fails.
	FindByTabletCount(ctx context.Context, ascending bool, limit int) ([]*Backend, error)

	// Save persists a backend.
	// Returns an error if the operation fails.
	Save(ctx context.Context, backend *Backend) error

	// SaveAll persists multiple backends.
	// Returns an error if the operation fails.
	SaveAll(ctx context.Context, backends []*Backend) error

	// Delete removes a backend by its ID.
	// Returns true if the backend was deleted, false if it was not found.
	// Returns an error if the operation fails.
	Delete(ctx context.Context, backendID int64) (bool, error)

	// UpdateState updates the state of a backend.
	// Returns an error if the operation fails.
	UpdateState(ctx context.Context, backendID int64, state BackendState) error

	// UpdateHeartbeat updates the last heartbeat time of a backend.
	// Returns an error if the operation fails.
	UpdateHeartbeat(ctx context.Context, backendID int64, heartbeatTime time.Time) error

	// UpdateCapacity updates the capacity information of a backend.
	// Returns an error if the operation fails.
	UpdateCapacity(ctx context.Context, backendID int64, capacity BackendCapacity) error

	// UpdateTabletCount updates the tablet count of a backend.
	// Returns an error if the operation fails.
	UpdateTabletCount(ctx context.Context, backendID int64, tabletCount int) error

	// CountAll counts the total number of backends.
	// Returns an error if the operation fails.
	CountAll(ctx context.Context) (int, error)

	// CountByState counts the number of backends in a specific state.
	// Returns an error if the operation fails.
	CountByState(ctx context.Context, state BackendState) (int, error)

	// CountByCluster counts the number of backends in a specific cluster.
	// Returns an error if the operation fails.
	CountByCluster(ctx context.Context, clusterID string) (int, error)
}

// BackendState represents the state of a backend.
type BackendState string

const (
	// BackendStateUnknown represents an unknown backend state
	BackendStateUnknown BackendState = "UNKNOWN"
	// BackendStateAlive represents an alive backend
	BackendStateAlive BackendState = "ALIVE"
	// BackendStateDead represents a dead backend
	BackendStateDead BackendState = "DEAD"
	// BackendStateDecommissioned represents a decommissioned backend
	BackendStateDecommissioned BackendState = "DECOMMISSIONED"
	// BackendStateMaintenance represents a backend in maintenance mode
	BackendStateMaintenance BackendState = "MAINTENANCE"
)

// IsValid checks if the backend state is valid
func (s BackendState) IsValid() bool {
	switch s {
	case BackendStateUnknown, BackendStateAlive, BackendStateDead,
		BackendStateDecommissioned, BackendStateMaintenance:
		return true
	default:
		return false
	}
}

// String returns the string representation of the backend state
func (s BackendState) String() string {
	return string(s)
}

// BackendCapacity represents the capacity information of a backend.
type BackendCapacity struct {
	// TotalBytes is the total capacity in bytes
	TotalBytes int64
	// UsedBytes is the used capacity in bytes
	UsedBytes int64
	// AvailableBytes is the available capacity in bytes
	AvailableBytes int64
	// DiskUtilization is the disk utilization percentage (0-100)
	DiskUtilization float64
	// MemoryTotalBytes is the total memory in bytes
	MemoryTotalBytes int64
	// MemoryUsedBytes is the used memory in bytes
	MemoryUsedBytes int64
	// MemoryAvailableBytes is the available memory in bytes
	MemoryAvailableBytes int64
	// MemoryUtilization is the memory utilization percentage (0-100)
	MemoryUtilization float64
	// CPUUtilization is the CPU utilization percentage (0-100)
	CPUUtilization float64
}

// Backend represents a storage backend.
type Backend struct {
	// ID is the unique ID of the backend
	ID int64
	// Host is the hostname or IP address of the backend
	Host string
	// Port is the port number of the backend
	Port int
	// HTTPPort is the HTTP port number of the backend
	HTTPPort int
	// State is the state of the backend
	State BackendState
	// ClusterID is the ID of the cluster that this backend belongs to
	ClusterID string
	// Tags are the tags associated with this backend
	Tags []string
	// LastHeartbeat is the timestamp of the last heartbeat received from this backend
	LastHeartbeat time.Time
	// Capacity is the capacity information of the backend
	Capacity BackendCapacity
	// TabletCount is the number of tablets hosted on this backend
	TabletCount int
	// StartTime is the time when the backend was started
	StartTime time.Time
	// Version is the version of the backend software
	Version string
	// Zone is the availability zone of the backend
	Zone string
	// Region is the region of the backend
	Region string
}

// ShardRepository defines the interface for shard persistence operations.
type ShardRepository interface {
	// FindByID retrieves a shard by its ID.
	// Returns nil if the shard is not found.
	// Returns an error if the operation fails.
	FindByID(ctx context.Context, shardID string) (*Shard, error)

	// FindByIDs retrieves shards by their IDs.
	// Returns an empty slice if no shards are found.
	// Returns an error if the operation fails.
	FindByIDs(ctx context.Context, shardIDs []string) ([]*Shard, error)

	// FindByTableID retrieves all shards in a table.
	// Returns an empty slice if no shards are found.
	// Returns an error if the operation fails.
	FindByTableID(ctx context.Context, tableID int64) ([]*Shard, error)

	// FindByPartitionID retrieves all shards in a partition.
	// Returns an empty slice if no shards are found.
	// Returns an error if the operation fails.
	FindByPartitionID(ctx context.Context, partitionID int64) ([]*Shard, error)

	// FindByDatabaseID retrieves all shards in a database.
	// Returns an empty slice if no shards are found.
	// Returns an error if the operation fails.
	FindByDatabaseID(ctx context.Context, databaseID int64) ([]*Shard, error)

	// FindByKey retrieves the shard that contains the specified key.
	// Returns nil if no shard is found.
	// Returns an error if the operation fails.
	FindByKey(ctx context.Context, tableID int64, key []byte) (*Shard, error)

	// FindByRange retrieves all shards that overlap with the specified range.
	// Returns an empty slice if no shards are found.
	// Returns an error if the operation fails.
	FindByRange(ctx context.Context, tableID int64, startKey, endKey []byte) ([]*Shard, error)

	// FindAll retrieves all shards.
	// Returns an empty slice if no shards are found.
	// Returns an error if the operation fails.
	FindAll(ctx context.Context) ([]*Shard, error)

	// Save persists a shard.
	// Returns an error if the operation fails.
	Save(ctx context.Context, shard *Shard) error

	// SaveAll persists multiple shards.
	// Returns an error if the operation fails.
	SaveAll(ctx context.Context, shards []*Shard) error

	// Delete removes a shard by its ID.
	// Returns true if the shard was deleted, false if it was not found.
	// Returns an error if the operation fails.
	Delete(ctx context.Context, shardID string) (bool, error)

	// DeleteByTableID removes all shards in a table.
	// Returns the number of shards deleted.
	// Returns an error if the operation fails.
	DeleteByTableID(ctx context.Context, tableID int64) (int, error)

	// DeleteByPartitionID removes all shards in a partition.
	// Returns the number of shards deleted.
	// Returns an error if the operation fails.
	DeleteByPartitionID(ctx context.Context, partitionID int64) (int, error)

	// CountByTableID counts the number of shards in a table.
	// Returns an error if the operation fails.
	CountByTableID(ctx context.Context, tableID int64) (int, error)

	// CountByPartitionID counts the number of shards in a partition.
	// Returns an error if the operation fails.
	CountByPartitionID(ctx context.Context, partitionID int64) (int, error)

	// CountByDatabaseID counts the number of shards in a database.
	// Returns an error if the operation fails.
	CountByDatabaseID(ctx context.Context, databaseID int64) (int, error)

	// CountAll counts the total number of shards.
	// Returns an error if the operation fails.
	CountAll(ctx context.Context) (int, error)
}

// Shard represents a shard of a table or partition.
type Shard struct {
	// ID is the unique ID of the shard
	ID string
	// TableID is the ID of the table that this shard belongs to
	TableID int64
	// PartitionID is the ID of the partition that this shard belongs to
	PartitionID int64
	// DatabaseID is the ID of the database that this shard belongs to
	DatabaseID int64
	// StartKey is the inclusive start key of the shard
	StartKey []byte
	// EndKey is the exclusive end key of the shard
	EndKey []byte
	// CreateTime is the time when the shard was created
	CreateTime time.Time
	// UpdateTime is the time when the shard was last updated
	UpdateTime time.Time
	// TabletCount is the number of tablets in this shard
	TabletCount int
	// Status is the status of the shard
	Status string
	// Version is the version of the shard
	Version int64
}

//Personal.AI order the ending
