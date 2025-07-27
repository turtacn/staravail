// Package config provides configuration structures and loading functions for the application.
package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/turtacn/staravail/internal/common/logger"
)

var (
	// log is the logger for the config package
	log = logger.GetLogger("config")

	// globalConfig is the global configuration instance
	globalConfig     *Config
	globalConfigLock sync.RWMutex
)

// Config is the root configuration structure containing all configuration sections.
type Config struct {
	// Server contains server-related configuration
	Server ServerConfig `json:"server" yaml:"server" mapstructure:"server"`
	// Starrocks contains StarRocks-related configuration
	Starrocks StarrocksConfig `json:"starrocks" yaml:"starrocks" mapstructure:"starrocks"`
	// Monitor contains monitoring-related configuration
	Monitor MonitorConfig `json:"monitor" yaml:"monitor" mapstructure:"monitor"`
	// Log contains logging-related configuration
	Log LogConfig `json:"log" yaml:"log" mapstructure:"log"`
	// WriteBuffer contains write buffer configuration
	WriteBuffer WriteBufferConfig `json:"write_buffer" yaml:"write_buffer" mapstructure:"write_buffer"`
	// Batch contains batch processing configuration
	Batch BatchConfig `json:"batch" yaml:"batch" mapstructure:"batch"`
	// Auth contains authentication and authorization configuration
	Auth AuthConfig `json:"auth" yaml:"auth" mapstructure:"auth"`
	// Quota contains resource quota configuration
	Quota QuotaConfig `json:"quota" yaml:"quota" mapstructure:"quota"`
	// configPath is the path to the config file (not exported to JSON/YAML)
	configPath string `json:"-" yaml:"-" mapstructure:"-"`
	// configWatcher is used for config hot reload (not exported to JSON/YAML)
	configWatcher *fsnotify.Watcher `json:"-" yaml:"-" mapstructure:"-"`
	// onConfigReload is a callback to be called when config is reloaded
	onConfigReload []func(old, new *Config) error `json:"-" yaml:"-" mapstructure:"-"`
	// reloadLock is used to synchronize config reloads
	reloadLock sync.Mutex `json:"-" yaml:"-" mapstructure:"-"`
}

// ServerConfig contains server-related configuration.
type ServerConfig struct {
	// Host is the host to bind to
	Host string `json:"host" yaml:"host" mapstructure:"host" validate:"required"`
	// HTTPPort is the HTTP port to listen on
	HTTPPort int `json:"http_port" yaml:"http_port" mapstructure:"http_port" validate:"required,min=1,max=65535"`
	// MySQLPort is the MySQL protocol port to listen on
	MySQLPort int `json:"mysql_port" yaml:"mysql_port" mapstructure:"mysql_port" validate:"required,min=1,max=65535"`
	// AdminPort is the admin HTTP port to listen on
	AdminPort int `json:"admin_port" yaml:"admin_port" mapstructure:"admin_port" validate:"required,min=1,max=65535"`
	// MaxConnections is the maximum number of connections to allow
	MaxConnections int `json:"max_connections" yaml:"max_connections" mapstructure:"max_connections" validate:"min=1"`
	// ConnectionTimeout is the timeout for connections in seconds
	ConnectionTimeout int `json:"connection_timeout" yaml:"connection_timeout" mapstructure:"connection_timeout" validate:"min=1"`
	// ReadTimeout is the timeout for read operations in seconds
	ReadTimeout int `json:"read_timeout" yaml:"read_timeout" mapstructure:"read_timeout" validate:"min=1"`
	// WriteTimeout is the timeout for write operations in seconds
	WriteTimeout int `json:"write_timeout" yaml:"write_timeout" mapstructure:"write_timeout" validate:"min=1"`
	// IdleTimeout is the timeout for idle connections in seconds
	IdleTimeout int `json:"idle_timeout" yaml:"idle_timeout" mapstructure:"idle_timeout" validate:"min=1"`
	// ShutdownTimeout is the timeout for graceful shutdown in seconds
	ShutdownTimeout int `json:"shutdown_timeout" yaml:"shutdown_timeout" mapstructure:"shutdown_timeout" validate:"min=1"`
	// EnableHTTPS indicates whether to enable HTTPS
	EnableHTTPS bool `json:"enable_https" yaml:"enable_https" mapstructure:"enable_https"`
	// TLSCertFile is the path to the TLS certificate file
	TLSCertFile string `json:"tls_cert_file" yaml:"tls_cert_file" mapstructure:"tls_cert_file"`
	// TLSKeyFile is the path to the TLS key file
	TLSKeyFile string `json:"tls_key_file" yaml:"tls_key_file" mapstructure:"tls_key_file"`
	// MaxRequestBodySize is the maximum size of the request body in bytes
	MaxRequestBodySize int64 `json:"max_request_body_size" yaml:"max_request_body_size" mapstructure:"max_request_body_size" validate:"min=1"`
	// EnableCORS indicates whether to enable Cross-Origin Resource Sharing
	EnableCORS bool `json:"enable_cors" yaml:"enable_cors" mapstructure:"enable_cors"`
	// AllowedOrigins is the list of allowed origins for CORS
	AllowedOrigins []string `json:"allowed_origins" yaml:"allowed_origins" mapstructure:"allowed_origins"`
	// EnableGzip indicates whether to enable Gzip compression
	EnableGzip bool `json:"enable_gzip" yaml:"enable_gzip" mapstructure:"enable_gzip"`
}

// StarrocksConfig contains StarRocks-related configuration.
type StarrocksConfig struct {
	// FEAddresses is the list of StarRocks Frontend addresses
	FEAddresses []string `json:"fe_addresses" yaml:"fe_addresses" mapstructure:"fe_addresses" validate:"required,min=1"`
	// FEHTTPPort is the HTTP port of the Frontend
	FEHTTPPort int `json:"fe_http_port" yaml:"fe_http_port" mapstructure:"fe_http_port" validate:"required,min=1,max=65535"`
	// FEMySQLPort is the MySQL port of the Frontend
	FEMySQLPort int `json:"fe_mysql_port" yaml:"fe_mysql_port" mapstructure:"fe_mysql_port" validate:"required,min=1,max=65535"`
	// BEAddresses is the list of StarRocks Backend addresses
	BEAddresses []string `json:"be_addresses" yaml:"be_addresses" mapstructure:"be_addresses"`
	// BEHTTPPort is the HTTP port of the Backend
	BEHTTPPort int `json:"be_http_port" yaml:"be_http_port" mapstructure:"be_http_port" validate:"required,min=1,max=65535"`
	// Username is the username for StarRocks authentication
	Username string `json:"username" yaml:"username" mapstructure:"username" validate:"required"`
	// Password is the password for StarRocks authentication
	Password string `json:"password" yaml:"password" mapstructure:"password" validate:"required"`
	// Database is the default database to use
	Database string `json:"database" yaml:"database" mapstructure:"database"`
	// QueryTimeout is the timeout for queries in seconds
	QueryTimeout int `json:"query_timeout" yaml:"query_timeout" mapstructure:"query_timeout" validate:"min=1"`
	// MaxConnections is the maximum number of connections to StarRocks
	MaxConnections int `json:"max_connections" yaml:"max_connections" mapstructure:"max_connections" validate:"min=1"`
	// ConnectionPoolSize is the size of the connection pool per FE
	ConnectionPoolSize int `json:"connection_pool_size" yaml:"connection_pool_size" mapstructure:"connection_pool_size" validate:"min=1"`
	// RetryCount is the number of times to retry failed operations
	RetryCount int `json:"retry_count" yaml:"retry_count" mapstructure:"retry_count" validate:"min=0"`
	// RetryInterval is the interval between retries in milliseconds
	RetryInterval int `json:"retry_interval" yaml:"retry_interval" mapstructure:"retry_interval" validate:"min=1"`
	// HeartbeatInterval is the interval for heartbeat checks in seconds
	HeartbeatInterval int `json:"heartbeat_interval" yaml:"heartbeat_interval" mapstructure:"heartbeat_interval" validate:"min=1"`
	// LoadBalanceStrategy is the strategy for load balancing across FEs
	LoadBalanceStrategy string `json:"load_balance_strategy" yaml:"load_balance_strategy" mapstructure:"load_balance_strategy" validate:"oneof=round_robin random least_connections"`
	// EnableAutoFailover indicates whether to enable automatic failover
	EnableAutoFailover bool `json:"enable_auto_failover" yaml:"enable_auto_failover" mapstructure:"enable_auto_failover"`
	// FailoverRetryCount is the number of times to retry during failover
	FailoverRetryCount int `json:"failover_retry_count" yaml:"failover_retry_count" mapstructure:"failover_retry_count" validate:"min=0"`
}

// MonitorConfig contains monitoring-related configuration.
type MonitorConfig struct {
	// Enable indicates whether to enable monitoring
	Enable bool `json:"enable" yaml:"enable" mapstructure:"enable"`
	// MetricsPort is the port for exposing metrics
	MetricsPort int `json:"metrics_port" yaml:"metrics_port" mapstructure:"metrics_port" validate:"min=1,max=65535"`
	// MetricsPath is the HTTP path for exposing metrics
	MetricsPath string `json:"metrics_path" yaml:"metrics_path" mapstructure:"metrics_path"`
	// MetricsInterval is the interval for collecting metrics in seconds
	MetricsInterval int `json:"metrics_interval" yaml:"metrics_interval" mapstructure:"metrics_interval" validate:"min=1"`
	// HealthCheckPath is the HTTP path for health checks
	HealthCheckPath string `json:"health_check_path" yaml:"health_check_path" mapstructure:"health_check_path"`
	// HealthCheckInterval is the interval for health checks in seconds
	HealthCheckInterval int `json:"health_check_interval" yaml:"health_check_interval" mapstructure:"health_check_interval" validate:"min=1"`
	// EnableTracing indicates whether to enable distributed tracing
	EnableTracing bool `json:"enable_tracing" yaml:"enable_tracing" mapstructure:"enable_tracing"`
	// TracingProvider is the provider for distributed tracing
	TracingProvider string `json:"tracing_provider" yaml:"tracing_provider" mapstructure:"tracing_provider" validate:"oneof=jaeger zipkin opentelemetry"`
	// TracingEndpoint is the endpoint for the tracing provider
	TracingEndpoint string `json:"tracing_endpoint" yaml:"tracing_endpoint" mapstructure:"tracing_endpoint"`
	// TracingSampleRate is the sampling rate for tracing (0.0-1.0)
	TracingSampleRate float64 `json:"tracing_sample_rate" yaml:"tracing_sample_rate" mapstructure:"tracing_sample_rate" validate:"min=0,max=1"`
	// AlertThresholds contains thresholds for alerts
	AlertThresholds AlertThresholds `json:"alert_thresholds" yaml:"alert_thresholds" mapstructure:"alert_thresholds"`
	// EnableAlerting indicates whether to enable alerting
	EnableAlerting bool `json:"enable_alerting" yaml:"enable_alerting" mapstructure:"enable_alerting"`
	// AlertingEndpoint is the endpoint for sending alerts
	AlertingEndpoint string `json:"alerting_endpoint" yaml:"alerting_endpoint" mapstructure:"alerting_endpoint"`
	// Profiling indicates whether to enable profiling
	EnableProfiling bool `json:"enable_profiling" yaml:"enable_profiling" mapstructure:"enable_profiling"`
	// ProfilingPort is the port for profiling
	ProfilingPort int `json:"profiling_port" yaml:"profiling_port" mapstructure:"profiling_port" validate:"min=1,max=65535"`
}

// AlertThresholds contains thresholds for alerts.
type AlertThresholds struct {
	// CPUUsagePercent is the threshold for CPU usage alerts
	CPUUsagePercent float64 `json:"cpu_usage_percent" yaml:"cpu_usage_percent" mapstructure:"cpu_usage_percent" validate:"min=0,max=100"`
	// MemoryUsagePercent is the threshold for memory usage alerts
	MemoryUsagePercent float64 `json:"memory_usage_percent" yaml:"memory_usage_percent" mapstructure:"memory_usage_percent" validate:"min=0,max=100"`
	// DiskUsagePercent is the threshold for disk usage alerts
	DiskUsagePercent float64 `json:"disk_usage_percent" yaml:"disk_usage_percent" mapstructure:"disk_usage_percent" validate:"min=0,max=100"`
	// ErrorRatePercent is the threshold for error rate alerts
	ErrorRatePercent float64 `json:"error_rate_percent" yaml:"error_rate_percent" mapstructure:"error_rate_percent" validate:"min=0,max=100"`
	// SlowQueryLatencyMs is the threshold for slow query alerts in milliseconds
	SlowQueryLatencyMs int `json:"slow_query_latency_ms" yaml:"slow_query_latency_ms" mapstructure:"slow_query_latency_ms" validate:"min=1"`
	// HighQPS is the threshold for high QPS alerts
	HighQPS int `json:"high_qps" yaml:"high_qps" mapstructure:"high_qps" validate:"min=1"`
	// BackendUnhealthyCount is the threshold for backend unhealthy alerts
	BackendUnhealthyCount int `json:"backend_unhealthy_count" yaml:"backend_unhealthy_count" mapstructure:"backend_unhealthy_count" validate:"min=1"`
	// TabletUnhealthyPercent is the threshold for tablet unhealthy alerts
	TabletUnhealthyPercent float64 `json:"tablet_unhealthy_percent" yaml:"tablet_unhealthy_percent" mapstructure:"tablet_unhealthy_percent" validate:"min=0,max=100"`
}

// LogConfig contains logging-related configuration.
type LogConfig struct {
	// Level is the logging level
	Level string `json:"level" yaml:"level" mapstructure:"level" validate:"oneof=debug info warn error fatal"`
	// Format is the logging format
	Format string `json:"format" yaml:"format" mapstructure:"format" validate:"oneof=json text console"`
	// OutputPath is the path to the log file
	OutputPath string `json:"output_path" yaml:"output_path" mapstructure:"output_path"`
	// EnableConsole indicates whether to log to console
	EnableConsole bool `json:"enable_console" yaml:"enable_console" mapstructure:"enable_console"`
	// EnableFile indicates whether to log to file
	EnableFile bool `json:"enable_file" yaml:"enable_file" mapstructure:"enable_file"`
	// MaxSize is the maximum size of the log file before rotation in MB
	MaxSize int `json:"max_size" yaml:"max_size" mapstructure:"max_size" validate:"min=1"`
	// MaxAge is the maximum number of days to retain old log files
	MaxAge int `json:"max_age" yaml:"max_age" mapstructure:"max_age" validate:"min=1"`
	// MaxBackups is the maximum number of old log files to retain
	MaxBackups int `json:"max_backups" yaml:"max_backups" mapstructure:"max_backups" validate:"min=1"`
	// Compress indicates whether to compress rotated log files
	Compress bool `json:"compress" yaml:"compress" mapstructure:"compress"`
	// EnableSampling indicates whether to enable log sampling
	EnableSampling bool `json:"enable_sampling" yaml:"enable_sampling" mapstructure:"enable_sampling"`
	// SamplingInitial is the initial number of messages to log at each level
	SamplingInitial int `json:"sampling_initial" yaml:"sampling_initial" mapstructure:"sampling_initial" validate:"min=1"`
	// SamplingThereafter is the sampling rate after the initial messages
	SamplingThereafter int `json:"sampling_thereafter" yaml:"sampling_thereafter" mapstructure:"sampling_thereafter" validate:"min=1"`
	// RequestLogFormat is the format for request logging
	RequestLogFormat string `json:"request_log_format" yaml:"request_log_format" mapstructure:"request_log_format"`
	// SlowQueryLogEnable indicates whether to enable slow query logging
	SlowQueryLogEnable bool `json:"slow_query_log_enable" yaml:"slow_query_log_enable" mapstructure:"slow_query_log_enable"`
	// SlowQueryThresholdMs is the threshold for slow query logging in milliseconds
	SlowQueryThresholdMs int `json:"slow_query_threshold_ms" yaml:"slow_query_threshold_ms" mapstructure:"slow_query_threshold_ms" validate:"min=1"`
}

// WriteBufferConfig contains write buffer configuration.
type WriteBufferConfig struct {
	// Enable indicates whether to enable write buffering
	Enable bool `json:"enable" yaml:"enable" mapstructure:"enable"`
	// BufferSize is the size of the write buffer in bytes
	BufferSize int64 `json:"buffer_size" yaml:"buffer_size" mapstructure:"buffer_size" validate:"min=1"`
	// FlushInterval is the interval for flushing the buffer in milliseconds
	FlushInterval int `json:"flush_interval" yaml:"flush_interval" mapstructure:"flush_interval" validate:"min=1"`
	// FlushSize is the size threshold for flushing the buffer in bytes
	FlushSize int64 `json:"flush_size" yaml:"flush_size" mapstructure:"flush_size" validate:"min=1"`
	// PersistEnable indicates whether to persist the buffer
	PersistEnable bool `json:"persist_enable" yaml:"persist_enable" mapstructure:"persist_enable"`
	// PersistPath is the path for persisting the buffer
	PersistPath string `json:"persist_path" yaml:"persist_path" mapstructure:"persist_path"`
	// PersistInterval is the interval for persisting the buffer in seconds
	PersistInterval int `json:"persist_interval" yaml:"persist_interval" mapstructure:"persist_interval" validate:"min=1"`
	// MaxQueueSize is the maximum size of the write queue
	MaxQueueSize int `json:"max_queue_size" yaml:"max_queue_size" mapstructure:"max_queue_size" validate:"min=1"`
	// RetryCount is the number of times to retry failed writes
	RetryCount int `json:"retry_count" yaml:"retry_count" mapstructure:"retry_count" validate:"min=0"`
	// RetryInterval is the interval between retries in milliseconds
	RetryInterval int `json:"retry_interval" yaml:"retry_interval" mapstructure:"retry_interval" validate:"min=1"`
	// EnableCompression indicates whether to enable compression
	EnableCompression bool `json:"enable_compression" yaml:"enable_compression" mapstructure:"enable_compression"`
	// CompressionLevel is the compression level (1-9)
	CompressionLevel int `json:"compression_level" yaml:"compression_level" mapstructure:"compression_level" validate:"min=1,max=9"`
	// EnableEncryption indicates whether to enable encryption
	EnableEncryption bool `json:"enable_encryption" yaml:"enable_encryption" mapstructure:"enable_encryption"`
	// EncryptionKey is the key for encryption
	EncryptionKey string `json:"encryption_key" yaml:"encryption_key" mapstructure:"encryption_key"`
}

// BatchConfig contains batch processing configuration.
type BatchConfig struct {
	// Enable indicates whether to enable batch processing
	Enable bool `json:"enable" yaml:"enable" mapstructure:"enable"`
	// BatchSize is the size of a batch
	BatchSize int `json:"batch_size" yaml:"batch_size" mapstructure:"batch_size" validate:"min=1"`
	// BatchInterval is the interval for processing batches in milliseconds
	BatchInterval int `json:"batch_interval" yaml:"batch_interval" mapstructure:"batch_interval" validate:"min=1"`
	// MaxBatchSize is the maximum size of a batch
	MaxBatchSize int `json:"max_batch_size" yaml:"max_batch_size" mapstructure:"max_batch_size" validate:"min=1"`
	// MaxBatchInterval is the maximum interval for processing batches in milliseconds
	MaxBatchInterval int `json:"max_batch_interval" yaml:"max_batch_interval" mapstructure:"max_batch_interval" validate:"min=1"`
	// ConcurrentBatches is the number of concurrent batches to process
	ConcurrentBatches int `json:"concurrent_batches" yaml:"concurrent_batches" mapstructure:"concurrent_batches" validate:"min=1"`
	// RetryCount is the number of times to retry failed batches
	RetryCount int `json:"retry_count" yaml:"retry_count" mapstructure:"retry_count" validate:"min=0"`
	// RetryInterval is the interval between retries in milliseconds
	RetryInterval int `json:"retry_interval" yaml:"retry_interval" mapstructure:"retry_interval" validate:"min=1"`
	// QueueSize is the size of the batch queue
	QueueSize int `json:"queue_size" yaml:"queue_size" mapstructure:"queue_size" validate:"min=1"`
	// Strategy is the strategy for batch processing
	Strategy string `json:"strategy" yaml:"strategy" mapstructure:"strategy" validate:"oneof=size time hybrid adaptive"`
	// AdaptiveConfig contains configuration for adaptive batching
	AdaptiveConfig AdaptiveBatchConfig `json:"adaptive_config" yaml:"adaptive_config" mapstructure:"adaptive_config"`
}

// AdaptiveBatchConfig contains configuration for adaptive batching.
type AdaptiveBatchConfig struct {
	// MinBatchSize is the minimum batch size
	MinBatchSize int `json:"min_batch_size" yaml:"min_batch_size" mapstructure:"min_batch_size" validate:"min=1"`
	// MaxBatchSize is the maximum batch size
	MaxBatchSize int `json:"max_batch_size" yaml:"max_batch_size" mapstructure:"max_batch_size" validate:"min=1"`
	// MinBatchInterval is the minimum batch interval in milliseconds
	MinBatchInterval int `json:"min_batch_interval" yaml:"min_batch_interval" mapstructure:"min_batch_interval" validate:"min=1"`
	// MaxBatchInterval is the maximum batch interval in milliseconds
	MaxBatchInterval int `json:"max_batch_interval" yaml:"max_batch_interval" mapstructure:"max_batch_interval" validate:"min=1"`
	// ScalingFactor is the factor for scaling batch size based on load
	ScalingFactor float64 `json:"scaling_factor" yaml:"scaling_factor" mapstructure:"scaling_factor" validate:"min=0.1,max=10"`
	// TargetLatency is the target latency in milliseconds
	TargetLatency int `json:"target_latency" yaml:"target_latency" mapstructure:"target_latency" validate:"min=1"`
	// EvaluationInterval is the interval for evaluating batch parameters in seconds
	EvaluationInterval int `json:"evaluation_interval" yaml:"evaluation_interval" mapstructure:"evaluation_interval" validate:"min=1"`
}

// AuthConfig contains authentication and authorization configuration.
type AuthConfig struct {
	// Enable indicates whether to enable authentication
	Enable bool `json:"enable" yaml:"enable" mapstructure:"enable"`
	// Type is the type of authentication
	Type string `json:"type" yaml:"type" mapstructure:"type" validate:"oneof=basic jwt ldap oauth"`
	// JWTSecret is the secret for JWT authentication
	JWTSecret string `json:"jwt_secret" yaml:"jwt_secret" mapstructure:"jwt_secret"`
	// JWTExpiration is the expiration time for JWT tokens in minutes
	JWTExpiration int `json:"jwt_expiration" yaml:"jwt_expiration" mapstructure:"jwt_expiration" validate:"min=1"`
	// LDAPServer is the LDAP server address
	LDAPServer string `json:"ldap_server" yaml:"ldap_server" mapstructure:"ldap_server"`
	// LDAPBindDN is the LDAP bind DN
	LDAPBindDN string `json:"ldap_bind_dn" yaml:"ldap_bind_dn" mapstructure:"ldap_bind_dn"`
	// LDAPBindPassword is the LDAP bind password
	LDAPBindPassword string `json:"ldap_bind_password" yaml:"ldap_bind_password" mapstructure:"ldap_bind_password"`
	// LDAPSearchBase is the LDAP search base
	LDAPSearchBase string `json:"ldap_search_base" yaml:"ldap_search_base" mapstructure:"ldap_search_base"`
	// OAuthProvider is the OAuth provider
	OAuthProvider string `json:"oauth_provider" yaml:"oauth_provider" mapstructure:"oauth_provider"`
	// OAuthClientID is the OAuth client ID
	OAuthClientID string `json:"oauth_client_id" yaml:"oauth_client_id" mapstructure:"oauth_client_id"`
	// OAuthClientSecret is the OAuth client secret
	OAuthClientSecret string `json:"oauth_client_secret" yaml:"oauth_client_secret" mapstructure:"oauth_client_secret"`
	// OAuthRedirectURL is the OAuth redirect URL
	OAuthRedirectURL string `json:"oauth_redirect_url" yaml:"oauth_redirect_url" mapstructure:"oauth_redirect_url"`
	// OAuthScopes is the list of OAuth scopes
	OAuthScopes []string `json:"oauth_scopes" yaml:"oauth_scopes" mapstructure:"oauth_scopes"`
	// StaticUsers is a map of username to password hash for static authentication
	StaticUsers map[string]string `json:"static_users" yaml:"static_users" mapstructure:"static_users"`
	// EnableAuthorization indicates whether to enable authorization
	EnableAuthorization bool `json:"enable_authorization" yaml:"enable_authorization" mapstructure:"enable_authorization"`
	// AuthorizationRules is a list of authorization rules
	AuthorizationRules []AuthorizationRule `json:"authorization_rules" yaml:"authorization_rules" mapstructure:"authorization_rules"`
}

// AuthorizationRule defines an authorization rule.
type AuthorizationRule struct {
	// User is the username or pattern to match
	User string `json:"user" yaml:"user" mapstructure:"user"`
	// Database is the database or pattern to match
	Database string `json:"database" yaml:"database" mapstructure:"database"`
	// Table is the table or pattern to match
	Table string `json:"table" yaml:"table" mapstructure:"table"`
	// Permissions is the list of permissions
	Permissions []string `json:"permissions" yaml:"permissions" mapstructure:"permissions" validate:"min=1"`
}

// QuotaConfig contains resource quota configuration.
type QuotaConfig struct {
	// Enable indicates whether to enable quota enforcement
	Enable bool `json:"enable" yaml:"enable" mapstructure:"enable"`
	// MaxQueriesPerSecond is the maximum number of queries per second
	MaxQueriesPerSecond int `json:"max_queries_per_second" yaml:"max_queries_per_second" mapstructure:"max_queries_per_second" validate:"min=1"`
	// MaxWritesPerSecond is the maximum number of writes per second
	MaxWritesPerSecond int `json:"max_writes_per_second" yaml:"max_writes_per_second" mapstructure:"max_writes_per_second" validate:"min=1"`
	// MaxConcurrentQueries is the maximum number of concurrent queries
	MaxConcurrentQueries int `json:"max_concurrent_queries" yaml:"max_concurrent_queries" mapstructure:"max_concurrent_queries" validate:"min=1"`
	// MaxConcurrentWrites is the maximum number of concurrent writes
	MaxConcurrentWrites int `json:"max_concurrent_writes" yaml:"max_concurrent_writes" mapstructure:"max_concurrent_writes" validate:"min=1"`
	// MaxRowsPerQuery is the maximum number of rows per query
	MaxRowsPerQuery int64 `json:"max_rows_per_query" yaml:"max_rows_per_query" mapstructure:"max_rows_per_query" validate:"min=1"`
	// MaxBytesPerQuery is the maximum number of bytes per query
	MaxBytesPerQuery int64 `json:"max_bytes_per_query" yaml:"max_bytes_per_query" mapstructure:"max_bytes_per_query" validate:"min=1"`
	// MaxExecutionTimeSeconds is the maximum execution time in seconds
	MaxExecutionTimeSeconds int `json:"max_execution_time_seconds" yaml:"max_execution_time_seconds" mapstructure:"max_execution_time_seconds" validate:"min=1"`
	// UserQuotas is a map of username to user-specific quotas
	UserQuotas map[string]UserQuota `json:"user_quotas" yaml:"user_quotas" mapstructure:"user_quotas"`
}

// UserQuota defines quotas for a specific user.
type UserQuota struct {
	// MaxQueriesPerSecond is the maximum number of queries per second
	MaxQueriesPerSecond int `json:"max_queries_per_second" yaml:"max_queries_per_second" mapstructure:"max_queries_per_second" validate:"min=1"`
	// MaxWritesPerSecond is the maximum number of writes per second
	MaxWritesPerSecond int `json:"max_writes_per_second" yaml:"max_writes_per_second" mapstructure:"max_writes_per_second" validate:"min=1"`
	// MaxConcurrentQueries is the maximum number of concurrent queries
	MaxConcurrentQueries int `json:"max_concurrent_queries" yaml:"max_concurrent_queries" mapstructure:"max_concurrent_queries" validate:"min=1"`
	// MaxConcurrentWrites is the maximum number of concurrent writes
	MaxConcurrentWrites int `json:"max_concurrent_writes" yaml:"max_concurrent_writes" mapstructure:"max_concurrent_writes" validate:"min=1"`
	// MaxRowsPerQuery is the maximum number of rows per query
	MaxRowsPerQuery int64 `json:"max_rows_per_query" yaml:"max_rows_per_query" mapstructure:"max_rows_per_query" validate:"min=1"`
	// MaxBytesPerQuery is the maximum number of bytes per query
	MaxBytesPerQuery int64 `json:"max_bytes_per_query" yaml:"max_bytes_per_query" mapstructure:"max_bytes_per_query" validate:"min=1"`
	// MaxExecutionTimeSeconds is the maximum execution time in seconds
	MaxExecutionTimeSeconds int `json:"max_execution_time_seconds" yaml:"max_execution_time_seconds" mapstructure:"max_execution_time_seconds" validate:"min=1"`
}

// DefaultConfig returns the default configuration.
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Host:               "0.0.0.0",
			HTTPPort:           8080,
			MySQLPort:          9030,
			AdminPort:          8081,
			MaxConnections:     1000,
			ConnectionTimeout:  30,
			ReadTimeout:        30,
			WriteTimeout:       30,
			IdleTimeout:        60,
			ShutdownTimeout:    30,
			EnableHTTPS:        false,
			MaxRequestBodySize: 10 * 1024 * 1024, // 10 MB
			EnableCORS:         true,
			AllowedOrigins:     []string{"*"},
			EnableGzip:         true,
		},
		Starrocks: StarrocksConfig{
			FEAddresses:         []string{"localhost"},
			FEHTTPPort:          8030,
			FEMySQLPort:         9030,
			BEHTTPPort:          8040,
			Username:            "root",
			Password:            "",
			QueryTimeout:        300,
			MaxConnections:      100,
			ConnectionPoolSize:  10,
			RetryCount:          3,
			RetryInterval:       1000,
			HeartbeatInterval:   10,
			LoadBalanceStrategy: "round_robin",
			EnableAutoFailover:  true,
			FailoverRetryCount:  3,
		},
		Monitor: MonitorConfig{
			Enable:              true,
			MetricsPort:         9090,
			MetricsPath:         "/metrics",
			MetricsInterval:     15,
			HealthCheckPath:     "/health",
			HealthCheckInterval: 30,
			EnableTracing:       false,
			TracingProvider:     "jaeger",
			TracingSampleRate:   0.1,
			AlertThresholds: AlertThresholds{
				CPUUsagePercent:        80.0,
				MemoryUsagePercent:     80.0,
				DiskUsagePercent:       80.0,
				ErrorRatePercent:       5.0,
				SlowQueryLatencyMs:     1000,
				HighQPS:                1000,
				BackendUnhealthyCount:  1,
				TabletUnhealthyPercent: 10.0,
			},
			EnableAlerting:  false,
			EnableProfiling: false,
			ProfilingPort:   6060,
		},
		Log: LogConfig{
			Level:                "info",
			Format:               "json",
			OutputPath:           "logs/starrocks-proxy.log",
			EnableConsole:        true,
			EnableFile:           true,
			MaxSize:              100,
			MaxAge:               30,
			MaxBackups:           10,
			Compress:             true,
			EnableSampling:       false,
			SamplingInitial:      100,
			SamplingThereafter:   100,
			RequestLogFormat:     "${remote_ip} - ${user} [${time_local}] \"${method} ${path} ${protocol}\" ${status} ${body_bytes_sent} \"${referer}\" \"${user_agent}\" ${request_time}ms",
			SlowQueryLogEnable:   true,
			SlowQueryThresholdMs: 1000,
		},
		WriteBuffer: WriteBufferConfig{
			Enable:            false,
			BufferSize:        100 * 1024 * 1024, // 100 MB
			FlushInterval:     1000,
			FlushSize:         10 * 1024 * 1024, // 10 MB
			PersistEnable:     false,
			PersistPath:       "data/buffer",
			PersistInterval:   60,
			MaxQueueSize:      10000,
			RetryCount:        3,
			RetryInterval:     1000,
			EnableCompression: true,
			CompressionLevel:  6,
			EnableEncryption:  false,
		},
		Batch: BatchConfig{
			Enable:            false,
			BatchSize:         1000,
			BatchInterval:     1000,
			MaxBatchSize:      10000,
			MaxBatchInterval:  10000,
			ConcurrentBatches: 5,
			RetryCount:        3,
			RetryInterval:     1000,
			QueueSize:         10000,
			Strategy:          "hybrid",
			AdaptiveConfig: AdaptiveBatchConfig{
				MinBatchSize:       100,
				MaxBatchSize:       10000,
				MinBatchInterval:   100,
				MaxBatchInterval:   10000,
				ScalingFactor:      1.5,
				TargetLatency:      100,
				EvaluationInterval: 60,
			},
		},
		Auth: AuthConfig{
			Enable:              false,
			Type:                "basic",
			JWTExpiration:       60,
			EnableAuthorization: false,
			AuthorizationRules:  []AuthorizationRule{},
		},
		Quota: QuotaConfig{
			Enable:                  false,
			MaxQueriesPerSecond:     100,
			MaxWritesPerSecond:      100,
			MaxConcurrentQueries:    50,
			MaxConcurrentWrites:     50,
			MaxRowsPerQuery:         1000000,
			MaxBytesPerQuery:        100 * 1024 * 1024, // 100 MB
			MaxExecutionTimeSeconds: 300,
			UserQuotas:              map[string]UserQuota{},
		},
	}
}

// LoadConfig loads the configuration from the specified file path.
func LoadConfig(configPath string) (*Config, error) {
	config := DefaultConfig()
	config.configPath = configPath

	// Use viper to load the configuration
	v := viper.New()
	v.SetConfigFile(configPath)

	// Set environment variable prefix
	v.SetEnvPrefix("STARROCKS_PROXY")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Read the config file
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	// Unmarshal the config into the Config struct
	if err := v.Unmarshal(config); err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}

	// Set up environment variable overrides for all config fields
	configureEnvVarOverrides(v, config)

	// Parse command-line flags
	parseCommandLineFlags(config)

	// Validate the configuration
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("config validation error: %w", err)
	}

	// Set up hot reload if configured
	if err := config.setupHotReload(); err != nil {
		log.Warnf("Failed to set up config hot reload: %v", err)
	}

	// Set the global config instance
	setGlobalConfig(config)

	return config, nil
}

// configureEnvVarOverrides sets up environment variable overrides for all config fields
func configureEnvVarOverrides(v *viper.Viper, config *Config) {
	// Recursively process all fields in the config struct
	configureEnvVarOverridesRecursive(v, reflect.ValueOf(config).Elem(), "")
}

// configureEnvVarOverridesRecursive recursively processes all fields in a struct
func configureEnvVarOverridesRecursive(v *viper.Viper, val reflect.Value, prefix string) {
	t := val.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		value := val.Field(i)

		// Skip unexported fields
		if field.PkgPath != "" {
			continue
		}

		// Get the field name from mapstructure tag or use the field name
		name := field.Tag.Get("mapstructure")
		if name == "" || name == "-" {
			continue
		}

		// Build the full path to this field
		fullPath := name
		if prefix != "" {
			fullPath = prefix + "." + name
		}

		// For struct fields, recursively process them
		if value.Kind() == reflect.Struct {
			configureEnvVarOverridesRecursive(v, value, fullPath)
			continue
		}

		// For other fields, set up environment variable binding
		envVar := "STARROCKS_PROXY_" + strings.ToUpper(strings.ReplaceAll(fullPath, ".", "_"))
		if err := v.BindEnv(fullPath, envVar); err != nil {
			log.Warnf("Failed to bind environment variable for %s: %v", fullPath, err)
		}
	}
}

// parseCommandLineFlags defines and parses command-line flags for configuration overrides
func parseCommandLineFlags(config *Config) {
	// Set up flags
	pflag.String("server.host", config.Server.Host, "Server host")
	pflag.Int("server.http_port", config.Server.HTTPPort, "HTTP port")
	pflag.Int("server.mysql_port", config.Server.MySQLPort, "MySQL port")
	pflag.StringSlice("starrocks.fe_addresses", config.Starrocks.FEAddresses, "StarRocks Frontend addresses")
	pflag.String("starrocks.username", config.Starrocks.Username, "StarRocks username")
	pflag.String("starrocks.password", config.Starrocks.Password, "StarRocks password")
	pflag.String("log.level", config.Log.Level, "Log level")
	pflag.Bool("monitor.enable", config.Monitor.Enable, "Enable monitoring")

	// Parse flags
	pflag.Parse()

	// Apply flag values to config
	viper.BindPFlags(pflag.CommandLine)
	if viper.IsSet("server.host") {
		config.Server.Host = viper.GetString("server.host")
	}
	if viper.IsSet("server.http_port") {
		config.Server.HTTPPort = viper.GetInt("server.http_port")
	}
	if viper.IsSet("server.mysql_port") {
		config.Server.MySQLPort = viper.GetInt("server.mysql_port")
	}
	if viper.IsSet("starrocks.fe_addresses") {
		config.Starrocks.FEAddresses = viper.GetStringSlice("starrocks.fe_addresses")
	}
	if viper.IsSet("starrocks.username") {
		config.Starrocks.Username = viper.GetString("starrocks.username")
	}
	if viper.IsSet("starrocks.password") {
		config.Starrocks.Password = viper.GetString("starrocks.password")
	}
	if viper.IsSet("log.level") {
		config.Log.Level = viper.GetString("log.level")
	}
	if viper.IsSet("monitor.enable") {
		config.Monitor.Enable = viper.GetBool("monitor.enable")
	}
}

// validateConfig performs validation on the configuration values
func validateConfig(config *Config) error {
	// Validate server config
	if config.Server.HTTPPort == config.Server.MySQLPort ||
		config.Server.HTTPPort == config.Server.AdminPort ||
		config.Server.MySQLPort == config.Server.AdminPort {
		return fmt.Errorf("server ports must be different: http_port=%d, mysql_port=%d, admin_port=%d",
			config.Server.HTTPPort, config.Server.MySQLPort, config.Server.AdminPort)
	}

	if config.Server.EnableHTTPS {
		if config.Server.TLSCertFile == "" || config.Server.TLSKeyFile == "" {
			return fmt.Errorf("TLS cert file and key file must be specified when HTTPS is enabled")
		}
		if _, err := os.Stat(config.Server.TLSCertFile); os.IsNotExist(err) {
			return fmt.Errorf("TLS cert file does not exist: %s", config.Server.TLSCertFile)
		}
		if _, err := os.Stat(config.Server.TLSKeyFile); os.IsNotExist(err) {
			return fmt.Errorf("TLS key file does not exist: %s", config.Server.TLSKeyFile)
		}
	}

	// Validate StarRocks config
	if len(config.Starrocks.FEAddresses) == 0 {
		return fmt.Errorf("at least one StarRocks Frontend address must be specified")
	}

	if config.Starrocks.Username == "" {
		return fmt.Errorf("StarRocks username must be specified")
	}

	// Validate log config
	if config.Log.EnableFile {
		logDir := filepath.Dir(config.Log.OutputPath)
		if _, err := os.Stat(logDir); os.IsNotExist(err) {
			if err := os.MkdirAll(logDir, 0755); err != nil {
				return fmt.Errorf("failed to create log directory: %w", err)
			}
		}
	}

	// Validate write buffer config
	if config.WriteBuffer.Enable && config.WriteBuffer.PersistEnable {
		if config.WriteBuffer.PersistPath == "" {
			return fmt.Errorf("persist path must be specified when write buffer persistence is enabled")
		}
		bufferDir := filepath.Dir(config.WriteBuffer.PersistPath)
		if _, err := os.Stat(bufferDir); os.IsNotExist(err) {
			if err := os.MkdirAll(bufferDir, 0755); err != nil {
				return fmt.Errorf("failed to create buffer directory: %w", err)
			}
		}
	}

	// Validate batch config
	if config.Batch.Enable {
		if config.Batch.BatchSize <= 0 {
			return fmt.Errorf("batch size must be greater than 0")
		}
		if config.Batch.BatchInterval <= 0 {
			return fmt.Errorf("batch interval must be greater than 0")
		}
		if config.Batch.Strategy == "adaptive" {
			if config.Batch.AdaptiveConfig.MinBatchSize >= config.Batch.AdaptiveConfig.MaxBatchSize {
				return fmt.Errorf("min batch size must be less than max batch size")
			}
			if config.Batch.AdaptiveConfig.MinBatchInterval >= config.Batch.AdaptiveConfig.MaxBatchInterval {
				return fmt.Errorf("min batch interval must be less than max batch interval")
			}
		}
	}

	// Validate auth config
	if config.Auth.Enable {
		switch config.Auth.Type {
		case "jwt":
			if config.Auth.JWTSecret == "" {
				return fmt.Errorf("JWT secret must be specified when JWT authentication is enabled")
			}
		case "ldap":
			if config.Auth.LDAPServer == "" {
				return fmt.Errorf("LDAP server must be specified when LDAP authentication is enabled")
			}
		case "oauth":
			if config.Auth.OAuthClientID == "" || config.Auth.OAuthClientSecret == "" {
				return fmt.Errorf("OAuth client ID and secret must be specified when OAuth authentication is enabled")
			}
		}
	}

	return nil
}

// setupHotReload sets up a file watcher to reload the config when the file changes
func (c *Config) setupHotReload() error {
	if c.configPath == "" {
		return fmt.Errorf("config path is empty, hot reload not set up")
	}

	// Create a new file watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create file watcher: %w", err)
	}

	// Get the absolute path to the config file
	absPath, err := filepath.Abs(c.configPath)
	if err != nil {
		watcher.Close()
		return fmt.Errorf("failed to get absolute path: %w", err)
	}

	// Add the config file to the watcher
	if err := watcher.Add(filepath.Dir(absPath)); err != nil {
		watcher.Close()
		return fmt.Errorf("failed to add config file to watcher: %w", err)
	}

	c.configWatcher = watcher

	// Start watching for changes
	go c.watchConfigFile(absPath)

	return nil
}

// watchConfigFile watches for changes to the config file and reloads the config
func (c *Config) watchConfigFile(configPath string) {
	for {
		select {
		case event, ok := <-c.configWatcher.Events:
			if !ok {
				return
			}

			// Check if the event is for our config file
			if filepath.Base(event.Name) != filepath.Base(configPath) {
				continue
			}

			// Check if the event is a write or create event
			if event.Op&(fsnotify.Write|fsnotify.Create) != 0 {
				log.Infof("Config file changed, reloading: %s", configPath)
				if err := c.Reload(); err != nil {
					log.Errorf("Failed to reload config: %v", err)
				}
			}
		case err, ok := <-c.configWatcher.Errors:
			if !ok {
				return
			}
			log.Errorf("Config file watcher error: %v", err)
		}
	}
}

// Reload reloads the configuration from the file
func (c *Config) Reload() error {
	c.reloadLock.Lock()
	defer c.reloadLock.Unlock()

	// Save a copy of the old config
	oldConfig := *c

	// Read the config file
	data, err := ioutil.ReadFile(c.configPath)
	if err != nil {
		return fmt.Errorf("error reading config file: %w", err)
	}

	// Determine the config format and unmarshal
	var newConfig Config
	ext := strings.ToLower(filepath.Ext(c.configPath))
	switch ext {
	case ".json":
		if err := json.Unmarshal(data, &newConfig); err != nil {
			return fmt.Errorf("error unmarshaling JSON config: %w", err)
		}
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &newConfig); err != nil {
			return fmt.Errorf("error unmarshaling YAML config: %w", err)
		}
	default:
		return fmt.Errorf("unsupported config file format: %s", ext)
	}

	// Apply environment variable overrides
	applyEnvVarOverrides(&newConfig)

	// Validate the new config
	if err := validateConfig(&newConfig); err != nil {
		return fmt.Errorf("config validation error: %w", err)
	}

	// Preserve fields that should not be changed by reload
	newConfig.configPath = c.configPath
	newConfig.configWatcher = c.configWatcher
	newConfig.onConfigReload = c.onConfigReload

	// Update the global config
	setGlobalConfig(&newConfig)

	// Copy new values to the current config
	*c = newConfig

	// Call registered reload callbacks
	for _, callback := range c.onConfigReload {
		if err := callback(&oldConfig, c); err != nil {
			log.Warnf("Config reload callback error: %v", err)
		}
	}

	log.Info("Configuration reloaded successfully")
	return nil
}

// applyEnvVarOverrides applies environment variable overrides to the config
func applyEnvVarOverrides(config *Config) {
	// Recursively process all fields in the config struct
	applyEnvVarOverridesRecursive(reflect.ValueOf(config).Elem(), "", "STARROCKS_PROXY_")
}

// applyEnvVarOverridesRecursive recursively processes all fields in a struct
func applyEnvVarOverridesRecursive(val reflect.Value, prefix, envPrefix string) {
	t := val.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		value := val.Field(i)

		// Skip unexported fields
		if field.PkgPath != "" {
			continue
		}

		// Get the field name from mapstructure tag or use the field name
		name := field.Tag.Get("mapstructure")
		if name == "" || name == "-" {
			continue
		}

		// Build the full path to this field
		fullPath := name
		if prefix != "" {
			fullPath = prefix + "." + name
		}

		// For struct fields, recursively process them
		if value.Kind() == reflect.Struct {
			applyEnvVarOverridesRecursive(value, fullPath, envPrefix)
			continue
		}

		// For other fields, check for environment variable override
		envVar := envPrefix + strings.ToUpper(strings.ReplaceAll(fullPath, ".", "_"))
		envValue, exists := os.LookupEnv(envVar)
		if !exists {
			continue
		}

		// Apply the environment variable value to the field
		switch value.Kind() {
		case reflect.String:
			value.SetString(envValue)
		case reflect.Bool:
			boolValue, err := strconv.ParseBool(envValue)
			if err == nil {
				value.SetBool(boolValue)
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			intValue, err := strconv.ParseInt(envValue, 10, 64)
			if err == nil {
				value.SetInt(intValue)
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			uintValue, err := strconv.ParseUint(envValue, 10, 64)
			if err == nil {
				value.SetUint(uintValue)
			}
		case reflect.Float32, reflect.Float64:
			floatValue, err := strconv.ParseFloat(envValue, 64)
			if err == nil {
				value.SetFloat(floatValue)
			}
		case reflect.Slice:
			// Handle slice of strings
			if value.Type().Elem().Kind() == reflect.String {
				strSlice := strings.Split(envValue, ",")
				value.Set(reflect.ValueOf(strSlice))
			}
		}
	}
}

// RegisterReloadCallback registers a callback to be called when the config is reloaded
func (c *Config) RegisterReloadCallback(callback func(old, new *Config) error) {
	c.reloadLock.Lock()
	defer c.reloadLock.Unlock()
	c.onConfigReload = append(c.onConfigReload, callback)
}

// Close closes any resources used by the config
func (c *Config) Close() error {
	if c.configWatcher != nil {
		return c.configWatcher.Close()
	}
	return nil
}

// SaveToFile saves the current configuration to a file
func (c *Config) SaveToFile(filePath string) error {
	// Create a copy of the config without non-serializable fields
	configCopy := *c
	configCopy.configWatcher = nil
	configCopy.onConfigReload = nil

	// Determine the format based on the file extension
	ext := strings.ToLower(filepath.Ext(filePath))
	var data []byte
	var err error

	switch ext {
	case ".json":
		data, err = json.MarshalIndent(configCopy, "", "  ")
		if err != nil {
			return fmt.Errorf("error marshaling config to JSON: %w", err)
		}
	case ".yaml", ".yml":
		data, err = yaml.Marshal(configCopy)
		if err != nil {
			return fmt.Errorf("error marshaling config to YAML: %w", err)
		}
	default:
		return fmt.Errorf("unsupported file format: %s", ext)
	}

	// Create the directory if it doesn't exist
	dir := filepath.Dir(filePath)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
	}

	// Write the config to the file
	if err := ioutil.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("error writing config to file: %w", err)
	}

	return nil
}

// GetGlobalConfig returns the global configuration instance
func GetGlobalConfig() *Config {
	globalConfigLock.RLock()
	defer globalConfigLock.RUnlock()
	return globalConfig
}

// setGlobalConfig sets the global configuration instance
func setGlobalConfig(config *Config) {
	globalConfigLock.Lock()
	defer globalConfigLock.Unlock()
	globalConfig = config
}

// ApplyLogConfig applies the log configuration to the logger
func (c *Config) ApplyLogConfig() error {
	// Convert log level string to logger.Level
	var level logger.Level
	switch strings.ToLower(c.Log.Level) {
	case "debug":
		level = logger.DebugLevel
	case "info":
		level = logger.InfoLevel
	case "warn", "warning":
		level = logger.WarnLevel
	case "error":
		level = logger.ErrorLevel
	case "fatal":
		level = logger.FatalLevel
	default:
		level = logger.InfoLevel
	}

	// Create logger config
	logConfig := logger.Config{
		Level:              level,
		Format:             c.Log.Format,
		EnableConsole:      c.Log.EnableConsole,
		ConsoleLevel:       level,
		EnableFile:         c.Log.EnableFile,
		FileLevel:          level,
		FilePath:           c.Log.OutputPath,
		FileMaxSize:        c.Log.MaxSize,
		FileMaxBackups:     c.Log.MaxBackups,
		FileMaxAge:         c.Log.MaxAge,
		FileCompress:       c.Log.Compress,
		EnableSampling:     c.Log.EnableSampling,
		SamplingInitial:    c.Log.SamplingInitial,
		SamplingThereafter: c.Log.SamplingThereafter,
		ContextKeys:        []string{"request_id", "user_id", "session_id"},
		DevelopmentMode:    false,
		DisableCaller:      false,
		DisableStacktrace:  false,
	}

	// Update logger configuration
	if err := logger.UpdateConfig(logConfig); err != nil {
		return fmt.Errorf("failed to update logger config: %w", err)
	}

	return nil
}

// WaitForChanges blocks until a configuration change is detected
func (c *Config) WaitForChanges() <-chan struct{} {
	ch := make(chan struct{})

	// Register a callback to signal when config is reloaded
	c.RegisterReloadCallback(func(old, new *Config) error {
		select {
		case ch <- struct{}{}:
		default:
			// Channel is full or closed, ignore
		}
		return nil
	})

	return ch
}

// LoadConfigFile is a helper function to load the configuration from a file
func LoadConfigFile(filePath string) (*Config, error) {
	return LoadConfig(filePath)
}

// LoadDefaultConfig loads the default configuration
func LoadDefaultConfig() *Config {
	config := DefaultConfig()
	setGlobalConfig(config)
	return config
}

// init initializes the package by loading default config
func init() {
	globalConfig = DefaultConfig()
}

//Personal.AI order the ending
