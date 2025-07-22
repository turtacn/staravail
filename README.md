# StarAvail - StarRocks High Availability Proxy

[![Go Version](https://img.shields.io/badge/Go-1.20.2+-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/Build-Passing-brightgreen.svg)](https://github.com/turtacn/staravail)

[中文文档](README-zh.md) | English

StarAvail is an intelligent proxy service designed to provide "partial availability" capabilities for StarRocks clusters running in single replica mode (num_replicas=1). When Backend (BE) nodes fail, StarAvail ensures business continuity by intelligently routing queries and writes around failed tablets while maintaining data consistency.

## Core Pain Points & Value Proposition

### The Challenge
In StarRocks single replica deployments, a user table may be split into dozens or hundreds of tablets. When any BE node fails, all tablets on that node become unavailable. StarRocks' "all-or-nothing" approach causes entire table queries/writes to fail when scanning missing tablets, leading to:

- **Service Interruption**: Complete table unavailability even for queries targeting healthy tablets
- **Data Loss Risk**: Permanent data loss potential in single replica scenarios  
- **Business Impact**: Large-scale query and write failures affecting operations

### Our Solution
StarAvail acts as an intelligent proxy layer that:
- **Maintains Service Continuity**: Routes requests only to healthy tablets
- **Prevents Data Loss**: Intelligently caches writes to failed partitions for retry
- **Zero Kernel Modification**: Works without modifying StarRocks core
- **Performance Optimized**: Built-in performance tuning framework with extensible optimizations

## Key Features

### 🚀 Intelligent Availability Management
- Real-time BE node health monitoring via FE HTTP APIs
- Dynamic tablet-to-BE mapping maintenance with smart caching
- Automatic query pruning to healthy partitions only

### 🛡️ Data Protection
- Write buffering for failed partitions with automatic retry
- Configurable degradation strategies (reject vs. partial results)
- Transaction-aware write coordination

### ⚡ Performance Excellence  
- High-performance reverse proxy for normal operations
- Extensible performance optimization framework
- Connection pooling and query result caching
- Intelligent load balancing across healthy FE nodes

### 🔧 Operations Friendly
- Comprehensive observability with metrics, logging, and tracing
- Configurable alerting for partition failures
- Health check endpoints for monitoring integration
- Graceful degradation with clear error messaging

## Architecture Overview

StarAvail operates as a stateless proxy layer between applications and StarRocks FE nodes:

```

\[Applications] → \[StarAvail Proxy] → \[StarRocks FE Cluster]
↓
\[Configuration & State Management]
↓
\[StarRocks BE Cluster]

````

For detailed architecture documentation, see [docs/architecture.md](docs/architecture.md).

## Quick Start

### Prerequisites
- Go 1.20.2 or later
- StarRocks cluster (single replica mode)
- Access to StarRocks FE HTTP APIs

### Installation

```bash
# Clone the repository
git clone https://github.com/turtacn/staravail.git
cd staravail

# Build the project
make build

# Or install directly
go install github.com/turtacn/staravail/cmd/staravail@latest
````

### Basic Usage

```bash
# Start StarAvail proxy
./staravail --config config.yaml

# Or with environment variables
export STARAVAIL_FE_HOSTS="fe1:9030,fe2:9030,fe3:9030"
export STARAVAIL_LISTEN_PORT="8030"
./staravail
```

### Configuration Example

```yaml
# config.yaml
server:
  listen_port: 8030
  read_timeout: 30s
  write_timeout: 30s

starrocks:
  fe_hosts:
    - "fe1:9030"
    - "fe2:9030"  
    - "fe3:9030"
  health_check_interval: 10s
  tablet_mapping_refresh: 60s

availability:
  degradation_mode: "partial_results" # or "reject"
  write_buffer_size: 1000
  retry_attempts: 3
  retry_backoff: "exponential"

observability:
  metrics_enabled: true
  tracing_enabled: true
  log_level: "info"
```

### Example Usage

```go
// Connect through StarAvail proxy instead of direct FE connection
db, err := sql.Open("mysql", "user:password@tcp(staravail-host:8030)/database")
if err != nil {
    log.Fatal(err)
}

// Queries automatically route around failed tablets
rows, err := db.Query(`
    SELECT * FROM user_events 
    WHERE event_date >= '2025-01-01' 
    AND event_date < '2025-01-02'
`)

// Writes are intelligently buffered if targeting failed partitions
_, err = db.Exec(`
    INSERT INTO user_events (user_id, event_date, event_type) 
    VALUES (?, ?, ?)`, userID, eventDate, eventType)
```

## Performance Optimizations

StarAvail includes several built-in optimizations:

### Connection Management

```go
// Smart connection pooling
config := &PoolConfig{
    MaxIdleConns:    100,
    MaxOpenConns:    200,
    ConnMaxLifetime: time.Hour,
    HealthCheck:     true,
}
```

### Query Optimization

```go
// Automatic query plan caching
cache := NewQueryPlanCache(1000) // Cache 1000 plans
proxy.WithQueryPlanCache(cache)

// Result set caching for repeated queries
resultCache := NewResultCache(time.Minute * 5)
proxy.WithResultCache(resultCache)
```

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Development Setup

```bash
# Clone and setup development environment
git clone https://github.com/turtacn/staravail.git
cd staravail

# Install development dependencies
make dev-setup

# Run tests
make test

# Run integration tests (requires StarRocks cluster)
make test-integration
```

## Documentation

* [Architecture Guide](docs/architecture.md)
* [Configuration Reference](docs/configuration.md)
* [Performance Tuning](docs/performance.md)
* [Troubleshooting](docs/troubleshooting.md)
* [API Reference](docs/api.md)

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Community

* GitHub Issues: [Report bugs and feature requests](https://github.com/turtacn/staravail/issues)
* Discussions: [Join our community discussions](https://github.com/turtacn/staravail/discussions)

---

**Note**: This project is designed specifically for StarRocks single replica deployments. For production multi-replica clusters, native StarRocks availability features should be preferred.