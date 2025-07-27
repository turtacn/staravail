// Package enum defines global enumeration types used across the project.
package enum

// ServiceStatus represents the operational status of a service.
type ServiceStatus int

const (
	// ServiceStatusRunning indicates the service is operating normally.
	ServiceStatusRunning ServiceStatus = iota
	// ServiceStatusDegraded indicates the service is operational but with reduced performance.
	ServiceStatusDegraded
	// ServiceStatusFailed indicates the service has failed and is not operational.
	ServiceStatusFailed
)

// String returns the string representation of ServiceStatus.
func (s ServiceStatus) String() string {
	switch s {
	case ServiceStatusRunning:
		return "Running"
	case ServiceStatusDegraded:
		return "Degraded"
	case ServiceStatusFailed:
		return "Failed"
	default:
		return "Unknown"
	}
}

// BackendStatus represents the health status of a backend node.
type BackendStatus int

const (
	// BackendStatusHealthy indicates the backend node is healthy and fully operational.
	BackendStatusHealthy BackendStatus = iota
	// BackendStatusUnhealthy indicates the backend node is experiencing issues.
	BackendStatusUnhealthy
	// BackendStatusUnknown indicates the status of the backend node cannot be determined.
	BackendStatusUnknown
)

// String returns the string representation of BackendStatus.
func (s BackendStatus) String() string {
	switch s {
	case BackendStatusHealthy:
		return "Healthy"
	case BackendStatusUnhealthy:
		return "Unhealthy"
	case BackendStatusUnknown:
		return "Unknown"
	default:
		return "Unknown"
	}
}

// TabletStatus represents the availability status of a tablet.
type TabletStatus int

const (
	// TabletStatusAvailable indicates the tablet is available for read/write operations.
	TabletStatusAvailable TabletStatus = iota
	// TabletStatusUnavailable indicates the tablet is not available for operations.
	TabletStatusUnavailable
	// TabletStatusUnknown indicates the status of the tablet cannot be determined.
	TabletStatusUnknown
)

// String returns the string representation of TabletStatus.
func (s TabletStatus) String() string {
	switch s {
	case TabletStatusAvailable:
		return "Available"
	case TabletStatusUnavailable:
		return "Unavailable"
	case TabletStatusUnknown:
		return "Unknown"
	default:
		return "Unknown"
	}
}

// RequestType represents the type of request being processed.
type RequestType int

const (
	// RequestTypeQuery indicates a read/query request.
	RequestTypeQuery RequestType = iota
	// RequestTypeWrite indicates a write/modification request.
	RequestTypeWrite
	// RequestTypeAdmin indicates an administrative request.
	RequestTypeAdmin
)

// String returns the string representation of RequestType.
func (r RequestType) String() string {
	switch r {
	case RequestTypeQuery:
		return "Query"
	case RequestTypeWrite:
		return "Write"
	case RequestTypeAdmin:
		return "Admin"
	default:
		return "Unknown"
	}
}

// QueryHandlingStrategy represents the strategy for handling query requests.
type QueryHandlingStrategy int

const (
	// QueryHandlingStrategyFullPass indicates the query should be processed completely.
	QueryHandlingStrategyFullPass QueryHandlingStrategy = iota
	// QueryHandlingStrategyPrune indicates the query should be pruned for optimization.
	QueryHandlingStrategyPrune
	// QueryHandlingStrategyReject indicates the query should be rejected.
	QueryHandlingStrategyReject
)

// String returns the string representation of QueryHandlingStrategy.
func (q QueryHandlingStrategy) String() string {
	switch q {
	case QueryHandlingStrategyFullPass:
		return "FullPass"
	case QueryHandlingStrategyPrune:
		return "Prune"
	case QueryHandlingStrategyReject:
		return "Reject"
	default:
		return "Unknown"
	}
}

// WriteHandlingStrategy represents the strategy for handling write requests.
type WriteHandlingStrategy int

const (
	// WriteHandlingStrategyDirectWrite indicates writes should be executed directly.
	WriteHandlingStrategyDirectWrite WriteHandlingStrategy = iota
	// WriteHandlingStrategyBufferAndRetry indicates writes should be buffered and retried.
	WriteHandlingStrategyBufferAndRetry
	// WriteHandlingStrategyReject indicates writes should be rejected.
	WriteHandlingStrategyReject
)

// String returns the string representation of WriteHandlingStrategy.
func (w WriteHandlingStrategy) String() string {
	switch w {
	case WriteHandlingStrategyDirectWrite:
		return "DirectWrite"
	case WriteHandlingStrategyBufferAndRetry:
		return "BufferAndRetry"
	case WriteHandlingStrategyReject:
		return "Reject"
	default:
		return "Unknown"
	}
}

// DataFormat represents the format of data being processed.
type DataFormat int

const (
	// DataFormatAvroBinary indicates data in Avro binary format.
	DataFormatAvroBinary DataFormat = iota
	// DataFormatAvroOCF indicates data in Avro Object Container File format.
	DataFormatAvroOCF
	// DataFormatJSON indicates data in JSON format.
	DataFormatJSON
	// DataFormatCSV indicates data in CSV format.
	DataFormatCSV
)

// String returns the string representation of DataFormat.
func (d DataFormat) String() string {
	switch d {
	case DataFormatAvroBinary:
		return "AvroBinary"
	case DataFormatAvroOCF:
		return "AvroOCF"
	case DataFormatJSON:
		return "JSON"
	case DataFormatCSV:
		return "CSV"
	default:
		return "Unknown"
	}
}

// BatchStrategy represents the strategy for batching operations.
type BatchStrategy int

const (
	// BatchStrategyTimeInterval indicates batching based on time intervals.
	BatchStrategyTimeInterval BatchStrategy = iota
	// BatchStrategyBatchSize indicates batching based on data size.
	BatchStrategyBatchSize
	// BatchStrategyBatchCount indicates batching based on count of items.
	BatchStrategyBatchCount
	// BatchStrategyHybrid indicates batching using a hybrid approach.
	BatchStrategyHybrid
)

// String returns the string representation of BatchStrategy.
func (b BatchStrategy) String() string {
	switch b {
	case BatchStrategyTimeInterval:
		return "TimeInterval"
	case BatchStrategyBatchSize:
		return "BatchSize"
	case BatchStrategyBatchCount:
		return "BatchCount"
	case BatchStrategyHybrid:
		return "Hybrid"
	default:
		return "Unknown"
	}
}

//Personal.AI order the ending**
