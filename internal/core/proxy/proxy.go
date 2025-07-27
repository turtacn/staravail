// Package proxy provides the core proxy component for the system.
package proxy

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	logging "github.com/turtacn/staravail/internal/common/logger"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/domain/health"
	"github.com/turtacn/staravail/internal/infrastructure/config"
	"github.com/turtacn/staravail/internal/interfaces/api"
	"github.com/turtacn/staravail/internal/interfaces/mysql"
)

// ProxyStatus represents the status of the proxy.
type ProxyStatus string

const (
	// StatusInitializing indicates the proxy is initializing.
	StatusInitializing ProxyStatus = "initializing"
	// StatusStarting indicates the proxy is starting.
	StatusStarting ProxyStatus = "starting"
	// StatusRunning indicates the proxy is running.
	StatusRunning ProxyStatus = "running"
	// StatusStopping indicates the proxy is stopping.
	StatusStopping ProxyStatus = "stopping"
	// StatusStopped indicates the proxy is stopped.
	StatusStopped ProxyStatus = "stopped"
	// StatusError indicates the proxy encountered an error.
	StatusError ProxyStatus = "error"
)

// ComponentStatus represents the status of a component in the system.
type ComponentStatus struct {
	Name        string      `json:"name"`
	Status      string      `json:"status"`
	StartTime   time.Time   `json:"start_time,omitempty"`
	LastError   string      `json:"last_error,omitempty"`
	Metrics     interface{} `json:"metrics,omitempty"`
	Description string      `json:"description,omitempty"`
}

// SystemMetrics represents system-wide metrics.
type SystemMetrics struct {
	TotalRequests        int64   `json:"total_requests"`
	RequestsPerSecond    float64 `json:"requests_per_second"`
	ErrorRate            float64 `json:"error_rate"`
	AverageResponseTime  float64 `json:"average_response_time_ms"`
	CPUUsage             float64 `json:"cpu_usage"`
	MemoryUsage          float64 `json:"memory_usage_mb"`
	TotalConnections     int64   `json:"total_connections"`
	ActiveConnections    int64   `json:"active_connections"`
	TotalQueriesExecuted int64   `json:"total_queries_executed"`
	CacheHitRatio        float64 `json:"cache_hit_ratio"`
}

// SystemStatus represents the overall system status.
type SystemStatus struct {
	Status            ProxyStatus                `json:"status"`
	Version           string                     `json:"version"`
	Uptime            string                     `json:"uptime"`
	StartTime         time.Time                  `json:"start_time"`
	ComponentStatuses map[string]ComponentStatus `json:"component_statuses"`
	Metrics           SystemMetrics              `json:"metrics"`
}

// Proxy defines the interface for the core proxy component.
type Proxy interface {
	// Start starts the proxy and all its components.
	Start() error

	// Stop stops the proxy and all its components gracefully.
	Stop() error

	// GetStatus returns the current status of the proxy.
	GetStatus() SystemStatus

	// WaitForShutdown blocks until the proxy is shut down.
	WaitForShutdown() error

	// GetMetrics returns the current system metrics.
	GetMetrics() SystemMetrics
}

// StarRocksProxy implements the Proxy interface.
type StarRocksProxy struct {
	config        *config.Config
	logger        *zap.Logger
	httpServer    *http.Server
	apiRouter     api.Router
	mysqlServer   mysql.Server
	healthMonitor health.Monitor
	metrics       *metrics.Registry

	status     ProxyStatus
	startTime  time.Time
	components map[string]interface{}
	statusMu   sync.RWMutex

	shutdownCh chan struct{}
	shutdownWg sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc

	// Metrics
	totalRequests        prometheus.Counter
	requestsPerSecond    prometheus.Gauge
	errorRate            prometheus.Gauge
	averageResponseTime  prometheus.Gauge
	totalConnections     prometheus.Counter
	activeConnections    prometheus.Gauge
	totalQueriesExecuted prometheus.Counter
	cacheHitRatio        prometheus.Gauge
}

// NewStarRocksProxy creates a new StarRocksProxy instance.
func NewStarRocksProxy(
	config *config.Config,
	apiRouter api.Router,
	mysqlServer mysql.Server,
	healthMonitor health.Monitor,
	metricsRegistry *metrics.Registry,
) *StarRocksProxy {
	ctx, cancel := context.WithCancel(context.Background())

	proxy := &StarRocksProxy{
		config:        config,
		logger:        logging.GetLogger().Named("proxy"),
		apiRouter:     apiRouter,
		mysqlServer:   mysqlServer,
		healthMonitor: healthMonitor,
		metrics:       metricsRegistry,
		status:        StatusInitializing,
		components:    make(map[string]interface{}),
		shutdownCh:    make(chan struct{}),
		ctx:           ctx,
		cancel:        cancel,
	}

	// Register components
	proxy.components["api_router"] = apiRouter
	proxy.components["mysql_server"] = mysqlServer
	proxy.components["health_monitor"] = healthMonitor
	proxy.components["metrics_registry"] = metricsRegistry

	// Initialize metrics
	proxy.initializeMetrics()

	return proxy
}

// initializeMetrics sets up the system-level metrics.
func (p *StarRocksProxy) initializeMetrics() {
	p.totalRequests = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "starrocks_proxy",
		Name:      "total_requests",
		Help:      "Total number of requests received",
	})

	p.requestsPerSecond = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "starrocks_proxy",
		Name:      "requests_per_second",
		Help:      "Number of requests per second",
	})

	p.errorRate = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "starrocks_proxy",
		Name:      "error_rate",
		Help:      "Error rate as a percentage of total requests",
	})

	p.averageResponseTime = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "starrocks_proxy",
		Name:      "average_response_time_ms",
		Help:      "Average response time in milliseconds",
	})

	p.totalConnections = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "starrocks_proxy",
		Name:      "total_connections",
		Help:      "Total number of connections established",
	})

	p.activeConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "starrocks_proxy",
		Name:      "active_connections",
		Help:      "Number of currently active connections",
	})

	p.totalQueriesExecuted = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "starrocks_proxy",
		Name:      "total_queries_executed",
		Help:      "Total number of SQL queries executed",
	})

	p.cacheHitRatio = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "starrocks_proxy",
		Name:      "cache_hit_ratio",
		Help:      "Cache hit ratio as a percentage",
	})

	// Register metrics with the registry
	p.metrics.RegisterMetric("total_requests", p.totalRequests)
	p.metrics.RegisterMetric("requests_per_second", p.requestsPerSecond)
	p.metrics.RegisterMetric("error_rate", p.errorRate)
	p.metrics.RegisterMetric("average_response_time", p.averageResponseTime)
	p.metrics.RegisterMetric("total_connections", p.totalConnections)
	p.metrics.RegisterMetric("active_connections", p.activeConnections)
	p.metrics.RegisterMetric("total_queries_executed", p.totalQueriesExecuted)
	p.metrics.RegisterMetric("cache_hit_ratio", p.cacheHitRatio)
}

// Start starts the proxy and all its components.
func (p *StarRocksProxy) Start() error {
	p.statusMu.Lock()
	p.status = StatusStarting
	p.startTime = time.Now()
	p.statusMu.Unlock()

	p.logger.Info("Starting StarRocks Proxy",
		zap.String("version", p.config.Version),
		zap.Any("config", p.config.Sanitized()))

	// Start components in the correct order
	if err := p.startComponents(); err != nil {
		p.setStatus(StatusError)
		return fmt.Errorf("failed to start components: %w", err)
	}

	// Start the HTTP server
	if err := p.startHTTPServer(); err != nil {
		p.setStatus(StatusError)
		return fmt.Errorf("failed to start HTTP server: %w", err)
	}

	// Start the MySQL server
	if err := p.startMySQLServer(); err != nil {
		p.setStatus(StatusError)
		return fmt.Errorf("failed to start MySQL server: %w", err)
	}

	// Start the health monitor
	if err := p.startHealthMonitor(); err != nil {
		p.setStatus(StatusError)
		return fmt.Errorf("failed to start health monitor: %w", err)
	}

	// Start metrics collection
	p.startMetricsCollection()

	// Set up signal handling
	p.setupSignalHandling()

	p.setStatus(StatusRunning)
	p.logger.Info("StarRocks Proxy started successfully",
		zap.String("http_address", p.config.API.Address),
		zap.String("mysql_address", p.config.MySQL.Address))

	return nil
}

// startComponents starts all registered components.
func (p *StarRocksProxy) startComponents() error {
	// Define the startup order
	startupOrder := []string{
		"metrics_registry",
		"health_monitor",
		"api_router",
		"mysql_server",
	}

	for _, componentName := range startupOrder {
		component := p.components[componentName]
		p.logger.Info("Starting component", zap.String("component", componentName))

		// Start the component based on its type
		var err error
		switch c := component.(type) {
		case api.Router:
			err = c.Initialize()
		case mysql.Server:
			// MySQL server is started separately
		case health.Monitor:
			err = c.Start()
		case *metrics.Registry:
			err = c.Start()
		default:
			p.logger.Warn("Unknown component type", zap.String("component", componentName))
		}

		if err != nil {
			return fmt.Errorf("failed to start component %s: %w", componentName, err)
		}
	}

	return nil
}

// startHTTPServer starts the HTTP server.
func (p *StarRocksProxy) startHTTPServer() error {
	// Create a new HTTP server
	mux := http.NewServeMux()

	// Register the API router's handler
	mux.Handle("/api/", p.apiRouter.GetHandler())

	// Register the metrics handler
	mux.Handle("/metrics", p.metrics.GetHandler())

	// Register the health check handler
	mux.Handle("/health", p.healthMonitor.GetHandler())

	// Create the HTTP server
	p.httpServer = &http.Server{
		Addr:              p.config.API.Address,
		Handler:           mux,
		ReadTimeout:       time.Duration(p.config.API.ReadTimeoutSec) * time.Second,
		WriteTimeout:      time.Duration(p.config.API.WriteTimeoutSec) * time.Second,
		IdleTimeout:       time.Duration(p.config.API.IdleTimeoutSec) * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Start the HTTP server in a goroutine
	p.shutdownWg.Add(1)
	go func() {
		defer p.shutdownWg.Done()
		p.logger.Info("Starting HTTP server", zap.String("address", p.config.API.Address))

		if err := p.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			p.logger.Error("HTTP server error", zap.Error(err))
		}
	}()

	return nil
}

// startMySQLServer starts the MySQL server.
func (p *StarRocksProxy) startMySQLServer() error {
	// Start the MySQL server in a goroutine
	p.shutdownWg.Add(1)
	go func() {
		defer p.shutdownWg.Done()
		p.logger.Info("Starting MySQL server", zap.String("address", p.config.MySQL.Address))

		if err := p.mysqlServer.Start(); err != nil {
			p.logger.Error("MySQL server error", zap.Error(err))
		}
	}()

	return nil
}

// startHealthMonitor initializes the health monitor.
func (p *StarRocksProxy) startHealthMonitor() error {
	// The health monitor was already started in startComponents
	// This method is for any additional configuration

	// Register the proxy itself with the health monitor
	p.healthMonitor.RegisterCheck("proxy", func() (bool, error) {
		status := p.GetStatus()
		return status.Status == StatusRunning, nil
	})

	return nil
}

// startMetricsCollection starts the metrics collection process.
func (p *StarRocksProxy) startMetricsCollection() {
	// Start a goroutine to update metrics periodically
	p.shutdownWg.Add(1)
	go func() {
		defer p.shutdownWg.Done()

		ticker := time.NewTicker(time.Second * 5)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				p.updateMetrics()
			case <-p.ctx.Done():
				return
			}
		}
	}()
}

// updateMetrics updates the system metrics.
func (p *StarRocksProxy) updateMetrics() {
	// This would typically gather metrics from various components
	// For demonstration, we'll use some sample calculations

	// Update requests per second (simple calculation based on total requests)
	elapsed := time.Since(p.startTime).Seconds()
	if elapsed > 0 {
		rps := float64(p.totalRequests.(*prometheus.CounterVec).WithLabelValues().Value()) / elapsed
		p.requestsPerSecond.Set(rps)
	}

	// Update active connections from MySQL server
	if mysqlMetrics, ok := p.mysqlServer.(interface{ GetActiveConnections() int }); ok {
		p.activeConnections.Set(float64(mysqlMetrics.GetActiveConnections()))
	}

	// Other metric updates would go here, typically calling into component-specific metrics
}

// setupSignalHandling sets up handlers for OS signals.
func (p *StarRocksProxy) setupSignalHandling() {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	p.shutdownWg.Add(1)
	go func() {
		defer p.shutdownWg.Done()

		for {
			select {
			case sig := <-signalCh:
				p.logger.Info("Received signal", zap.String("signal", sig.String()))

				switch sig {
				case syscall.SIGHUP:
					// Reload configuration
					p.reloadConfiguration()
				case syscall.SIGINT, syscall.SIGTERM:
					// Initiate graceful shutdown
					p.logger.Info("Initiating graceful shutdown...")
					p.Stop()
					return
				}
			case <-p.ctx.Done():
				return
			}
		}
	}()
}

// reloadConfiguration reloads the configuration.
func (p *StarRocksProxy) reloadConfiguration() {
	p.logger.Info("Reloading configuration")

	// In a real implementation, this would reload configuration from disk
	// and apply changes to components that support dynamic reconfiguration

	// For this example, we'll just log that we would reload
	p.logger.Info("Configuration reload complete")
}

// Stop stops the proxy and all its components gracefully.
func (p *StarRocksProxy) Stop() error {
	p.statusMu.Lock()
	if p.status == StatusStopping || p.status == StatusStopped {
		p.statusMu.Unlock()
		return nil
	}
	p.status = StatusStopping
	p.statusMu.Unlock()

	p.logger.Info("Stopping StarRocks Proxy")

	// Signal all goroutines to stop
	p.cancel()

	// Stop components in reverse order
	if err := p.stopComponents(); err != nil {
		p.logger.Error("Error stopping components", zap.Error(err))
	}

	// Close the shutdown channel to signal we're done
	close(p.shutdownCh)

	p.setStatus(StatusStopped)
	p.logger.Info("StarRocks Proxy stopped")

	return nil
}

// stopComponents stops all registered components in reverse order.
func (p *StarRocksProxy) stopComponents() error {
	// Define the shutdown order (reverse of startup order)
	shutdownOrder := []string{
		"mysql_server",
		"api_router",
		"health_monitor",
		"metrics_registry",
	}

	// Create a context with timeout for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Stop the HTTP server first
	if p.httpServer != nil {
		p.logger.Info("Stopping HTTP server")
		if err := p.httpServer.Shutdown(ctx); err != nil {
			p.logger.Error("Error shutting down HTTP server", zap.Error(err))
		}
	}

	// Stop other components
	for _, componentName := range shutdownOrder {
		component := p.components[componentName]
		p.logger.Info("Stopping component", zap.String("component", componentName))

		// Stop the component based on its type
		var err error
		switch c := component.(type) {
		case api.Router:
			// API router doesn't need explicit stopping
		case mysql.Server:
			err = c.Stop()
		case health.Monitor:
			err = c.Stop()
		case *metrics.Registry:
			err = c.Stop()
		default:
			p.logger.Warn("Unknown component type", zap.String("component", componentName))
		}

		if err != nil {
			p.logger.Error("Error stopping component",
				zap.String("component", componentName),
				zap.Error(err))
		}
	}

	return nil
}

// WaitForShutdown blocks until the proxy is shut down.
func (p *StarRocksProxy) WaitForShutdown() error {
	<-p.shutdownCh
	p.shutdownWg.Wait()
	return nil
}

// GetStatus returns the current status of the proxy.
func (p *StarRocksProxy) GetStatus() SystemStatus {
	p.statusMu.RLock()
	defer p.statusMu.RUnlock()

	status := SystemStatus{
		Status:            p.status,
		Version:           p.config.Version,
		Uptime:            time.Since(p.startTime).String(),
		StartTime:         p.startTime,
		ComponentStatuses: make(map[string]ComponentStatus),
		Metrics:           p.GetMetrics(),
	}

	// Collect status from each component
	for name, component := range p.components {
		componentStatus := ComponentStatus{
			Name:   name,
			Status: "unknown",
		}

		// Get component-specific status if available
		switch c := component.(type) {
		case interface{ GetStatus() string }:
			componentStatus.Status = c.GetStatus()
		}

		status.ComponentStatuses[name] = componentStatus
	}

	return status
}

// GetMetrics returns the current system metrics.
func (p *StarRocksProxy) GetMetrics() SystemMetrics {
	totalReqs := p.totalRequests.(*prometheus.CounterVec).WithLabelValues().Value()

	metrics := SystemMetrics{
		TotalRequests:        int64(totalReqs),
		RequestsPerSecond:    p.requestsPerSecond.(*prometheus.GaugeVec).WithLabelValues().Value(),
		ErrorRate:            p.errorRate.(*prometheus.GaugeVec).WithLabelValues().Value(),
		AverageResponseTime:  p.averageResponseTime.(*prometheus.GaugeVec).WithLabelValues().Value(),
		TotalConnections:     int64(p.totalConnections.(*prometheus.CounterVec).WithLabelValues().Value()),
		ActiveConnections:    int64(p.activeConnections.(*prometheus.GaugeVec).WithLabelValues().Value()),
		TotalQueriesExecuted: int64(p.totalQueriesExecuted.(*prometheus.CounterVec).WithLabelValues().Value()),
		CacheHitRatio:        p.cacheHitRatio.(*prometheus.GaugeVec).WithLabelValues().Value(),
	}

	// Calculate CPU and memory usage
	// In a real implementation, this would use actual system metrics
	metrics.CPUUsage = 0.0    // Placeholder
	metrics.MemoryUsage = 0.0 // Placeholder

	return metrics
}

// setStatus updates the proxy status with thread safety.
func (p *StarRocksProxy) setStatus(status ProxyStatus) {
	p.statusMu.Lock()
	defer p.statusMu.Unlock()
	p.status = status
}

// Helper methods for dependency injection during testing

// SetAPIRouter sets the API router.
func (p *StarRocksProxy) SetAPIRouter(router api.Router) {
	p.apiRouter = router
	p.components["api_router"] = router
}

// SetMySQLServer sets the MySQL server.
func (p *StarRocksProxy) SetMySQLServer(server mysql.Server) {
	p.mysqlServer = server
	p.components["mysql_server"] = server
}

// SetHealthMonitor sets the health monitor.
func (p *StarRocksProxy) SetHealthMonitor(monitor health.Monitor) {
	p.healthMonitor = monitor
	p.components["health_monitor"] = monitor
}

// SetMetricsRegistry sets the metrics registry.
func (p *StarRocksProxy) SetMetricsRegistry(registry *metrics.Registry) {
	p.metrics = registry
	p.components["metrics_registry"] = registry
}

// ProxyFactory defines a factory for creating proxy instances.
type ProxyFactory interface {
	CreateProxy(config *config.Config) (Proxy, error)
}

// DefaultProxyFactory is the default implementation of ProxyFactory.
type DefaultProxyFactory struct {
	apiRouterFactory     api.RouterFactory
	mysqlServerFactory   mysql.ServerFactory
	healthMonitorFactory health.MonitorFactory
	metricsRegistry      *metrics.Registry
}

// NewDefaultProxyFactory creates a new DefaultProxyFactory.
func NewDefaultProxyFactory(
	apiRouterFactory api.RouterFactory,
	mysqlServerFactory mysql.ServerFactory,
	healthMonitorFactory health.MonitorFactory,
	metricsRegistry *metrics.Registry,
) *DefaultProxyFactory {
	return &DefaultProxyFactory{
		apiRouterFactory:     apiRouterFactory,
		mysqlServerFactory:   mysqlServerFactory,
		healthMonitorFactory: healthMonitorFactory,
		metricsRegistry:      metricsRegistry,
	}
}

// CreateProxy creates a new proxy instance with all dependencies initialized.
func (f *DefaultProxyFactory) CreateProxy(config *config.Config) (Proxy, error) {
	// Create the API router
	apiRouter, err := f.apiRouterFactory.CreateRouter(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create API router: %w", err)
	}

	// Create the MySQL server
	mysqlServer, err := f.mysqlServerFactory.CreateServer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create MySQL server: %w", err)
	}

	// Create the health monitor
	healthMonitor, err := f.healthMonitorFactory.CreateMonitor(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create health monitor: %w", err)
	}

	// Create the proxy
	proxy := NewStarRocksProxy(
		config,
		apiRouter,
		mysqlServer,
		healthMonitor,
		f.metricsRegistry,
	)

	return proxy, nil
}

// ProxyServer is a helper struct that manages the proxy lifecycle.
type ProxyServer struct {
	proxy  Proxy
	logger *zap.Logger
}

// NewProxyServer creates a new ProxyServer.
func NewProxyServer(proxy Proxy) *ProxyServer {
	return &ProxyServer{
		proxy:  proxy,
		logger: logging.GetLogger().Named("proxy-server"),
	}
}

// Run starts the proxy and waits for it to shut down.
func (s *ProxyServer) Run() error {
	// Start the proxy
	if err := s.proxy.Start(); err != nil {
		return fmt.Errorf("failed to start proxy: %w", err)
	}

	// Wait for shutdown
	s.logger.Info("Proxy is running, press Ctrl+C to stop")
	if err := s.proxy.WaitForShutdown(); err != nil {
		return fmt.Errorf("error during proxy shutdown: %w", err)
	}

	return nil
}

// CreateTestListener creates a network listener for testing.
func CreateTestListener(network, address string) (net.Listener, error) {
	return net.Listen(network, address)
}

// CreateAndRunProxy is a convenience function to create and run a proxy.
func CreateAndRunProxy(configPath string) error {
	// Load configuration
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Initialize logging
	if err := logging.InitializeLogger(cfg.Logging); err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}

	logger := logging.GetLogger()
	defer logger.Sync()

	// Initialize metrics registry
	metricsRegistry := metrics.NewRegistry()

	// Create factories
	apiRouterFactory := api.NewDefaultRouterFactory()
	mysqlServerFactory := mysql.NewDefaultServerFactory()
	healthMonitorFactory := health.NewDefaultMonitorFactory()

	// Create proxy factory
	proxyFactory := NewDefaultProxyFactory(
		apiRouterFactory,
		mysqlServerFactory,
		healthMonitorFactory,
		metricsRegistry,
	)

	// Create the proxy
	proxy, err := proxyFactory.CreateProxy(cfg)
	if err != nil {
		return fmt.Errorf("failed to create proxy: %w", err)
	}

	// Create and run the proxy server
	server := NewProxyServer(proxy)
	return server.Run()
}

//Personal.AI order the ending
