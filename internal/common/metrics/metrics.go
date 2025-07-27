package metrics

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/prometheus"
	api "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"

	logging "github.com/turtacn/staravail/internal/common/logger"
	"github.com/turtacn/staravail/internal/infrastructure/config"
)

// Registry provides metrics collection and reporting functionality.
type Registry struct {
	// Prometheus metrics components
	registry      *prometheus.Registry
	otelExporter  *prometheus.Exporter
	meterProvider *metric.MeterProvider
	meter         api.Meter

	// HTTP server for metrics exposure
	server   *http.Server
	httpAddr string

	// Metrics collections by type
	counters       map[string]api.Int64Counter
	gauges         map[string]api.Int64ObservableGauge
	histograms     map[string]api.Float64Histogram
	upDownCounters map[string]api.Int64UpDownCounter

	// Logger
	logger *zap.Logger

	// Mutex for concurrent access to metrics maps
	mu sync.RWMutex

	// Metrics configuration
	config *config.MonitoringConfig
}

// NewRegistry creates a new metrics registry
func NewRegistry(cfg config.MonitoringConfig) (*Registry, error) {
	logger := logging.GetLogger().Named("metrics")

	// Create a Prometheus registry
	promRegistry := prometheus.NewRegistry()

	// Create a Prometheus exporter
	exporter, err := prometheus.New(prometheus.WithRegisterer(promRegistry))
	if err != nil {
		return nil, fmt.Errorf("failed to create Prometheus exporter: %w", err)
	}

	// Create a meter provider
	meterProvider := metric.NewMeterProvider(
		metric.WithReader(exporter),
	)

	// Set the global meter provider
	otel.SetMeterProvider(meterProvider)

	// Create a meter
	meter := meterProvider.Meter("github.com/turtacn/staravail")

	// Create the metrics registry
	registry := &Registry{
		registry:       promRegistry,
		otelExporter:   exporter,
		meterProvider:  meterProvider,
		meter:          meter,
		counters:       make(map[string]api.Int64Counter),
		gauges:         make(map[string]api.Int64ObservableGauge),
		histograms:     make(map[string]api.Float64Histogram),
		upDownCounters: make(map[string]api.Int64UpDownCounter),
		logger:         logger,
		config:         &cfg,
	}

	// Set up HTTP server for metrics if enabled
	if cfg.Enabled {
		httpAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
		registry.httpAddr = httpAddr

		// Create the HTTP server
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(promRegistry, promhttp.HandlerOpts{}))
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		})

		server := &http.Server{
			Addr:    httpAddr,
			Handler: mux,
		}
		registry.server = server

		// Start the HTTP server in a goroutine
		go func() {
			logger.Info("Starting metrics HTTP server", zap.String("address", httpAddr))
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Error("Metrics HTTP server failed", zap.Error(err))
			}
		}()
	} else {
		logger.Info("Metrics HTTP server is disabled")
	}

	// Register default metrics
	registry.registerDefaultMetrics()

	return registry, nil
}

// Close stops the metrics registry and any associated servers
func (r *Registry) Close() error {
	r.logger.Info("Closing metrics registry")

	// Stop the HTTP server if it's running
	if r.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		r.logger.Debug("Stopping metrics HTTP server")
		if err := r.server.Shutdown(ctx); err != nil {
			r.logger.Error("Error shutting down metrics HTTP server", zap.Error(err))
			return err
		}
	}

	// Shutdown the meter provider
	if r.meterProvider != nil {
		r.logger.Debug("Shutting down meter provider")
		if err := r.meterProvider.Shutdown(context.Background()); err != nil {
			r.logger.Error("Error shutting down meter provider", zap.Error(err))
			return err
		}
	}

	return nil
}

// registerDefaultMetrics registers the default metrics
func (r *Registry) registerDefaultMetrics() {
	// Up metric (always 1, indicates the service is up)
	upGauge, err := r.meter.Int64ObservableGauge("up",
		api.WithDescription("Indicates whether the service is up (1) or down (0)"),
	)
	if err != nil {
		r.logger.Error("Failed to create up gauge", zap.Error(err))
	} else {
		r.mu.Lock()
		r.gauges["up"] = upGauge
		r.mu.Unlock()

		_, err = r.meter.RegisterCallback(
			func(_ context.Context, o api.Observer) error {
				o.ObserveInt64(upGauge, 1)
				return nil
			},
			upGauge,
		)
		if err != nil {
			r.logger.Error("Failed to register callback for up gauge", zap.Error(err))
		}
	}

	// Start time gauge
	startTimeGauge, err := r.meter.Int64ObservableGauge("start_time_seconds",
		api.WithDescription("Unix timestamp when the service started"),
	)
	if err != nil {
		r.logger.Error("Failed to create start time gauge", zap.Error(err))
	} else {
		startTime := time.Now().Unix()
		r.mu.Lock()
		r.gauges["start_time_seconds"] = startTimeGauge
		r.mu.Unlock()

		_, err = r.meter.RegisterCallback(
			func(_ context.Context, o api.Observer) error {
				o.ObserveInt64(startTimeGauge, startTime)
				return nil
			},
			startTimeGauge,
		)
		if err != nil {
			r.logger.Error("Failed to register callback for start time gauge", zap.Error(err))
		}
	}
}

// RegisterBuildInfo registers build information metrics
func (r *Registry) RegisterBuildInfo(version, gitCommit, buildDate string) {
	buildInfoGauge, err := r.meter.Int64ObservableGauge("build_info",
		api.WithDescription("Build information"),
	)
	if err != nil {
		r.logger.Error("Failed to create build info gauge", zap.Error(err))
		return
	}

	r.mu.Lock()
	r.gauges["build_info"] = buildInfoGauge
	r.mu.Unlock()

	_, err = r.meter.RegisterCallback(
		func(_ context.Context, o api.Observer) error {
			o.ObserveInt64(buildInfoGauge, 1,
				api.WithAttributes(
					prometheus.Labels{
						"version":    version,
						"git_commit": gitCommit,
						"build_date": buildDate,
					}...,
				),
			)
			return nil
		},
		buildInfoGauge,
	)
	if err != nil {
		r.logger.Error("Failed to register callback for build info gauge", zap.Error(err))
	}
}

// Counter operations

// IncrementCounter increments a counter by 1
func (r *Registry) IncrementCounter(name string) {
	r.AddToCounter(name, 1)
}

// AddToCounter adds the given value to a counter
func (r *Registry) AddToCounter(name string, value int64) {
	counter, err := r.getOrCreateCounter(name)
	if err != nil {
		r.logger.Error("Failed to increment counter", zap.String("name", name), zap.Error(err))
		return
	}
	counter.Add(context.Background(), value)
}

// IncrementCounterWithLabels increments a counter with labels by 1
func (r *Registry) IncrementCounterWithLabels(name string, labels map[string]string) {
	r.AddToCounterWithLabels(name, 1, labels)
}

// AddToCounterWithLabels adds the given value to a counter with labels
func (r *Registry) AddToCounterWithLabels(name string, value int64, labels map[string]string) {
	counter, err := r.getOrCreateCounter(name)
	if err != nil {
		r.logger.Error("Failed to increment counter with labels",
			zap.String("name", name),
			zap.Any("labels", labels),
			zap.Error(err))
		return
	}

	// Convert map to attribute slice
	attrs := make([]prometheus.Label, 0, len(labels))
	for k, v := range labels {
		attrs = append(attrs, prometheus.Label{Key: k, Value: v})
	}

	counter.Add(context.Background(), value, api.WithAttributes(attrs...))
}

// getOrCreateCounter gets or creates a counter
func (r *Registry) getOrCreateCounter(name string) (api.Int64Counter, error) {
	r.mu.RLock()
	counter, exists := r.counters[name]
	r.mu.RUnlock()

	if exists {
		return counter, nil
	}

	// Create a new counter
	counter, err := r.meter.Int64Counter(name,
		api.WithDescription(fmt.Sprintf("%s counter", name)),
	)
	if err != nil {
		return nil, err
	}

	// Store the counter
	r.mu.Lock()
	r.counters[name] = counter
	r.mu.Unlock()

	return counter, nil
}

// Gauge operations

// SetGauge sets a gauge to the given value
func (r *Registry) SetGauge(name string, value float64) {
	// Convert to int64 for the API
	intValue := int64(value)

	r.mu.RLock()
	_, exists := r.gauges[name]
	r.mu.RUnlock()

	if !exists {
		// Create a new gauge
		gauge, err := r.meter.Int64ObservableGauge(name,
			api.WithDescription(fmt.Sprintf("%s gauge", name)),
		)
		if err != nil {
			r.logger.Error("Failed to create gauge", zap.String("name", name), zap.Error(err))
			return
		}

		r.mu.Lock()
		r.gauges[name] = gauge
		r.mu.Unlock()

		// Create a callback for this gauge
		_, err = r.meter.RegisterCallback(
			func(_ context.Context, o api.Observer) error {
				o.ObserveInt64(gauge, intValue)
				return nil
			},
			gauge,
		)
		if err != nil {
			r.logger.Error("Failed to register callback for gauge", zap.String("name", name), zap.Error(err))
		}
	} else {
		r.logger.Warn("Cannot directly set value for existing gauge, use AddToGauge instead",
			zap.String("name", name))
	}
}

// AddToGauge adds the given value to a gauge
func (r *Registry) AddToGauge(name string, value float64) {
	// For gauges, we use UpDownCounters instead since gauges don't support direct addition
	counter, err := r.getOrCreateUpDownCounter(name)
	if err != nil {
		r.logger.Error("Failed to add to gauge", zap.String("name", name), zap.Error(err))
		return
	}

	// Convert to int64 for the API
	intValue := int64(value)
	counter.Add(context.Background(), intValue)
}

// getOrCreateUpDownCounter gets or creates an up-down counter
func (r *Registry) getOrCreateUpDownCounter(name string) (api.Int64UpDownCounter, error) {
	r.mu.RLock()
	counter, exists := r.upDownCounters[name]
	r.mu.RUnlock()

	if exists {
		return counter, nil
	}

	// Create a new up-down counter
	counter, err := r.meter.Int64UpDownCounter(name,
		api.WithDescription(fmt.Sprintf("%s gauge", name)),
	)
	if err != nil {
		return nil, err
	}

	// Store the counter
	r.mu.Lock()
	r.upDownCounters[name] = counter
	r.mu.Unlock()

	return counter, nil
}

// Histogram operations

// ObserveHistogram records a value in a histogram
func (r *Registry) ObserveHistogram(name string, value float64) {
	histogram, err := r.getOrCreateHistogram(name)
	if err != nil {
		r.logger.Error("Failed to observe histogram", zap.String("name", name), zap.Error(err))
		return
	}
	histogram.Record(context.Background(), value)
}

// ObserveHistogramWithLabels records a value in a histogram with labels
func (r *Registry) ObserveHistogramWithLabels(name string, value float64, labels map[string]string) {
	histogram, err := r.getOrCreateHistogram(name)
	if err != nil {
		r.logger.Error("Failed to observe histogram with labels",
			zap.String("name", name),
			zap.Any("labels", labels),
			zap.Error(err))
		return
	}

	// Convert map to attribute slice
	attrs := make([]prometheus.Label, 0, len(labels))
	for k, v := range labels {
		attrs = append(attrs, prometheus.Label{Key: k, Value: v})
	}

	histogram.Record(context.Background(), value, api.WithAttributes(attrs...))
}

// getOrCreateHistogram gets or creates a histogram
func (r *Registry) getOrCreateHistogram(name string) (api.Float64Histogram, error) {
	r.mu.RLock()
	histogram, exists := r.histograms[name]
	r.mu.RUnlock()

	if exists {
		return histogram, nil
	}

	// Create a new histogram
	histogram, err := r.meter.Float64Histogram(name,
		api.WithDescription(fmt.Sprintf("%s histogram", name)),
	)
	if err != nil {
		return nil, err
	}

	// Store the histogram
	r.mu.Lock()
	r.histograms[name] = histogram
	r.mu.Unlock()

	return histogram, nil
}

// DecrementCounter decrements a counter (implemented as incrementing with negative value)
func (r *Registry) DecrementCounter(name string) {
	counter, err := r.getOrCreateUpDownCounter(name)
	if err != nil {
		r.logger.Error("Failed to decrement counter", zap.String("name", name), zap.Error(err))
		return
	}
	counter.Add(context.Background(), -1)
}

// GetHTTPHandler returns the HTTP handler for the metrics endpoint
func (r *Registry) GetHTTPHandler() http.Handler {
	return promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{})
}

// GetHTTPAddress returns the HTTP address where metrics are being served
func (r *Registry) GetHTTPAddress() string {
	return r.httpAddr
}

// RecordLatency records a latency in a histogram
func (r *Registry) RecordLatency(name string, duration time.Duration) {
	histogramName := fmt.Sprintf("%s_seconds", name)
	r.ObserveHistogram(histogramName, duration.Seconds())
}

// RecordLatencyWithLabels records a latency in a histogram with labels
func (r *Registry) RecordLatencyWithLabels(name string, duration time.Duration, labels map[string]string) {
	histogramName := fmt.Sprintf("%s_seconds", name)
	r.ObserveHistogramWithLabels(histogramName, duration.Seconds(), labels)
}

// RecordQueryMetrics records metrics for a query
func (r *Registry) RecordQueryMetrics(success bool, duration time.Duration, queryType string) {
	// Record query count
	labels := map[string]string{
		"success": fmt.Sprintf("%t", success),
		"type":    queryType,
	}
	r.IncrementCounterWithLabels("queries_total", labels)

	// Record query latency
	r.ObserveHistogramWithLabels("query_duration_seconds", duration.Seconds(), labels)
}

// RecordRowsProcessed records the number of rows processed
func (r *Registry) RecordRowsProcessed(count int64, operation string) {
	labels := map[string]string{
		"operation": operation,
	}
	r.AddToCounterWithLabels("rows_processed_total", count, labels)
}

// RecordBytesProcessed records the number of bytes processed
func (r *Registry) RecordBytesProcessed(bytes int64, operation string) {
	labels := map[string]string{
		"operation": operation,
	}
	r.AddToCounterWithLabels("bytes_processed_total", bytes, labels)
}

// RecordConnectionMetrics records metrics for a connection
func (r *Registry) RecordConnectionMetrics(event string, protocol string) {
	labels := map[string]string{
		"event":    event,
		"protocol": protocol,
	}

	switch event {
	case "open":
		r.IncrementCounterWithLabels("connections_opened_total", labels)
		r.AddToGauge("connections_current", 1)
	case "close":
		r.IncrementCounterWithLabels("connections_closed_total", labels)
		r.AddToGauge("connections_current", -1)
	case "error":
		r.IncrementCounterWithLabels("connection_errors_total", labels)
	}
}

// RecordErrorMetrics records metrics for an error
func (r *Registry) RecordErrorMetrics(errorType string, component string) {
	labels := map[string]string{
		"type":      errorType,
		"component": component,
	}
	r.IncrementCounterWithLabels("errors_total", labels)
}

// RecordCacheMetrics records metrics for cache operations
func (r *Registry) RecordCacheMetrics(operation string, hit bool) {
	labels := map[string]string{
		"operation": operation,
		"hit":       fmt.Sprintf("%t", hit),
	}
	r.IncrementCounterWithLabels("cache_operations_total", labels)
}

// RegisterRuntimeMetrics registers metrics related to Go runtime
func (r *Registry) RegisterRuntimeMetrics() {
	// Number of goroutines
	goRoutinesGauge, err := r.meter.Int64ObservableGauge("go_goroutines",
		api.WithDescription("Number of goroutines that currently exist"),
	)
	if err != nil {
		r.logger.Error("Failed to create goroutines gauge", zap.Error(err))
	} else {
		r.mu.Lock()
		r.gauges["go_goroutines"] = goRoutinesGauge
		r.mu.Unlock()

		_, err = r.meter.RegisterCallback(
			func(_ context.Context, o api.Observer) error {
				o.ObserveInt64(goRoutinesGauge, int64(runtime.NumGoroutine()))
				return nil
			},
			goRoutinesGauge,
		)
		if err != nil {
			r.logger.Error("Failed to register callback for goroutines gauge", zap.Error(err))
		}
	}

	// GC metrics
	var memStats runtime.MemStats

	// Total allocations
	allocsTotalGauge, err := r.meter.Int64ObservableGauge("go_allocs_total",
		api.WithDescription("Total bytes allocated (even if freed)"),
	)
	if err != nil {
		r.logger.Error("Failed to create allocs total gauge", zap.Error(err))
	} else {
		r.mu.Lock()
		r.gauges["go_allocs_total"] = allocsTotalGauge
		r.mu.Unlock()

		_, err = r.meter.RegisterCallback(
			func(_ context.Context, o api.Observer) error {
				runtime.ReadMemStats(&memStats)
				o.ObserveInt64(allocsTotalGauge, int64(memStats.TotalAlloc))
				return nil
			},
			allocsTotalGauge,
		)
		if err != nil {
			r.logger.Error("Failed to register callback for allocs total gauge", zap.Error(err))
		}
	}

	// System memory
	sysGauge, err := r.meter.Int64ObservableGauge("go_sys",
		api.WithDescription("Total bytes obtained from system"),
	)
	if err != nil {
		r.logger.Error("Failed to create sys gauge", zap.Error(err))
	} else {
		r.mu.Lock()
		r.gauges["go_sys"] = sysGauge
		r.mu.Unlock()

		_, err = r.meter.RegisterCallback(
			func(_ context.Context, o api.Observer) error {
				runtime.ReadMemStats(&memStats)
				o.ObserveInt64(sysGauge, int64(memStats.Sys))
				return nil
			},
			sysGauge,
		)
		if err != nil {
			r.logger.Error("Failed to register callback for sys gauge", zap.Error(err))
		}
	}

	// Heap in use
	heapInUseGauge, err := r.meter.Int64ObservableGauge("go_heap_inuse",
		api.WithDescription("Bytes in use by the heap"),
	)
	if err != nil {
		r.logger.Error("Failed to create heap in use gauge", zap.Error(err))
	} else {
		r.mu.Lock()
		r.gauges["go_heap_inuse"] = heapInUseGauge
		r.mu.Unlock()

		_, err = r.meter.RegisterCallback(
			func(_ context.Context, o api.Observer) error {
				runtime.ReadMemStats(&memStats)
				o.ObserveInt64(heapInUseGauge, int64(memStats.HeapInuse))
				return nil
			},
			heapInUseGauge,
		)
		if err != nil {
			r.logger.Error("Failed to register callback for heap in use gauge", zap.Error(err))
		}
	}

	// GC count
	gcCountGauge, err := r.meter.Int64ObservableGauge("go_gc_count",
		api.WithDescription("Number of completed GC cycles"),
	)
	if err != nil {
		r.logger.Error("Failed to create GC count gauge", zap.Error(err))
	} else {
		r.mu.Lock()
		r.gauges["go_gc_count"] = gcCountGauge
		r.mu.Unlock()

		_, err = r.meter.RegisterCallback(
			func(_ context.Context, o api.Observer) error {
				runtime.ReadMemStats(&memStats)
				o.ObserveInt64(gcCountGauge, int64(memStats.NumGC))
				return nil
			},
			gcCountGauge,
		)
		if err != nil {
			r.logger.Error("Failed to register callback for GC count gauge", zap.Error(err))
		}
	}
}

// RecordStarRocksMetrics records metrics for StarRocks operations
func (r *Registry) RecordStarRocksMetrics(operation string, success bool, duration time.Duration) {
	labels := map[string]string{
		"operation": operation,
		"success":   fmt.Sprintf("%t", success),
	}

	// Record operation count
	r.IncrementCounterWithLabels("starrocks_operations_total", labels)

	// Record operation latency
	r.ObserveHistogramWithLabels("starrocks_operation_duration_seconds", duration.Seconds(), labels)
}

// RecordConnectionPoolMetrics records metrics for connection pool
func (r *Registry) RecordConnectionPoolMetrics(poolSize int, activeConnections int, waitingRequests int) {
	// Set current pool size
	poolSizeGauge, err := r.meter.Int64ObservableGauge("connection_pool_size",
		api.WithDescription("Current size of the connection pool"),
	)
	if err != nil {
		r.logger.Error("Failed to create connection pool size gauge", zap.Error(err))
	} else {
		r.mu.Lock()
		r.gauges["connection_pool_size"] = poolSizeGauge
		r.mu.Unlock()

		_, err = r.meter.RegisterCallback(
			func(_ context.Context, o api.Observer) error {
				o.ObserveInt64(poolSizeGauge, int64(poolSize))
				return nil
			},
			poolSizeGauge,
		)
		if err != nil {
			r.logger.Error("Failed to register callback for connection pool size gauge", zap.Error(err))
		}
	}

	// Set active connections
	activeConnectionsGauge, err := r.meter.Int64ObservableGauge("connection_pool_active",
		api.WithDescription("Number of active connections in the pool"),
	)
	if err != nil {
		r.logger.Error("Failed to create active connections gauge", zap.Error(err))
	} else {
		r.mu.Lock()
		r.gauges["connection_pool_active"] = activeConnectionsGauge
		r.mu.Unlock()

		_, err = r.meter.RegisterCallback(
			func(_ context.Context, o api.Observer) error {
				o.ObserveInt64(activeConnectionsGauge, int64(activeConnections))
				return nil
			},
			activeConnectionsGauge,
		)
		if err != nil {
			r.logger.Error("Failed to register callback for active connections gauge", zap.Error(err))
		}
	}

	// Set waiting requests
	waitingRequestsGauge, err := r.meter.Int64ObservableGauge("connection_pool_waiting",
		api.WithDescription("Number of requests waiting for a connection"),
	)
	if err != nil {
		r.logger.Error("Failed to create waiting requests gauge", zap.Error(err))
	} else {
		r.mu.Lock()
		r.gauges["connection_pool_waiting"] = waitingRequestsGauge
		r.mu.Unlock()

		_, err = r.meter.RegisterCallback(
			func(_ context.Context, o api.Observer) error {
				o.ObserveInt64(waitingRequestsGauge, int64(waitingRequests))
				return nil
			},
			waitingRequestsGauge,
		)
		if err != nil {
			r.logger.Error("Failed to register callback for waiting requests gauge", zap.Error(err))
		}
	}
}

// RecordProxyMetrics records metrics specific to the proxy operations
func (r *Registry) RecordProxyMetrics(operation string, status string, duration time.Duration) {
	labels := map[string]string{
		"operation": operation,
		"status":    status,
	}

	// Record operation count
	r.IncrementCounterWithLabels("proxy_operations_total", labels)

	// Record operation latency
	r.ObserveHistogramWithLabels("proxy_operation_duration_seconds", duration.Seconds(), labels)
}

// RegisterBuildTime registers when the application was built
func (r *Registry) RegisterBuildTime(buildTimeStr string) {
	buildTime, err := time.Parse(time.RFC3339, buildTimeStr)
	if err != nil {
		r.logger.Error("Failed to parse build time", zap.Error(err), zap.String("time_str", buildTimeStr))
		return
	}

	buildTimeGauge, err := r.meter.Int64ObservableGauge("build_timestamp",
		api.WithDescription("Unix timestamp when this binary was built"),
	)
	if err != nil {
		r.logger.Error("Failed to create build time gauge", zap.Error(err))
		return
	}

	r.mu.Lock()
	r.gauges["build_timestamp"] = buildTimeGauge
	r.mu.Unlock()

	buildTimeUnix := buildTime.Unix()
	_, err = r.meter.RegisterCallback(
		func(_ context.Context, o api.Observer) error {
			o.ObserveInt64(buildTimeGauge, buildTimeUnix)
			return nil
		},
		buildTimeGauge,
	)
	if err != nil {
		r.logger.Error("Failed to register callback for build time gauge", zap.Error(err))
	}
}

// NewTimer creates a timer that can be used to measure execution time
// and automatically record it to the metrics registry
func (r *Registry) NewTimer(name string, labels map[string]string) *Timer {
	return &Timer{
		start:    time.Now(),
		name:     name,
		labels:   labels,
		registry: r,
	}
}

// Timer represents a timer that can measure execution time
type Timer struct {
	start    time.Time
	name     string
	labels   map[string]string
	registry *Registry
}

// ObserveDuration observes the duration since the timer was created
func (t *Timer) ObserveDuration() time.Duration {
	duration := time.Since(t.start)
	if t.labels != nil {
		t.registry.ObserveHistogramWithLabels(t.name, duration.Seconds(), t.labels)
	} else {
		t.registry.ObserveHistogram(t.name, duration.Seconds())
	}
	return duration
}

// Reset resets the timer to the current time
func (t *Timer) Reset() {
	t.start = time.Now()
}

// Elapsed returns the elapsed time since the timer was created
// or last reset without recording it
func (t *Timer) Elapsed() time.Duration {
	return time.Since(t.start)
}

// GetStatus returns a snapshot of the current metrics state
// This is useful for diagnostic purposes
func (r *Registry) GetStatus() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	status := make(map[string]interface{})

	countersMap := make(map[string]string)
	for name := range r.counters {
		countersMap[name] = "counter"
	}

	gaugesMap := make(map[string]string)
	for name := range r.gauges {
		gaugesMap[name] = "gauge"
	}

	histogramsMap := make(map[string]string)
	for name := range r.histograms {
		histogramsMap[name] = "histogram"
	}

	upDownCountersMap := make(map[string]string)
	for name := range r.upDownCounters {
		upDownCountersMap[name] = "up_down_counter"
	}

	status["counters"] = countersMap
	status["gauges"] = gaugesMap
	status["histograms"] = histogramsMap
	status["up_down_counters"] = upDownCountersMap
	status["http_address"] = r.httpAddr
	status["config"] = r.config

	return status
}

// RegisterSystemInfo registers metrics about the system
func (r *Registry) RegisterSystemInfo() {
	hostname, err := os.Hostname()
	if err != nil {
		r.logger.Error("Failed to get hostname", zap.Error(err))
		hostname = "unknown"
	}

	systemInfoGauge, err := r.meter.Int64ObservableGauge("system_info",
		api.WithDescription("System information"),
	)
	if err != nil {
		r.logger.Error("Failed to create system info gauge", zap.Error(err))
		return
	}

	r.mu.Lock()
	r.gauges["system_info"] = systemInfoGauge
	r.mu.Unlock()

	_, err = r.meter.RegisterCallback(
		func(_ context.Context, o api.Observer) error {
			o.ObserveInt64(systemInfoGauge, 1,
				api.WithAttributes(
					prometheus.Labels{
						"hostname": hostname,
						"os":       runtime.GOOS,
						"arch":     runtime.GOARCH,
						"go":       runtime.Version(),
						"cpus":     fmt.Sprintf("%d", runtime.NumCPU()),
					}...,
				),
			)
			return nil
		},
		systemInfoGauge,
	)
	if err != nil {
		r.logger.Error("Failed to register callback for system info gauge", zap.Error(err))
	}
}

// RecordRequestMetrics records metrics for API requests
func (r *Registry) RecordRequestMetrics(method, path, status string, duration time.Duration, bytes int64) {
	labels := map[string]string{
		"method": method,
		"path":   path,
		"status": status,
	}

	// Record request count
	r.IncrementCounterWithLabels("http_requests_total", labels)

	// Record request duration
	r.ObserveHistogramWithLabels("http_request_duration_seconds", duration.Seconds(), labels)

	// Record response size
	if bytes > 0 {
		r.ObserveHistogramWithLabels("http_response_size_bytes", float64(bytes), labels)
	}
}

// RecordDatabaseOperationMetrics records metrics for database operations
func (r *Registry) RecordDatabaseOperationMetrics(operation, database, table string, success bool, duration time.Duration) {
	labels := map[string]string{
		"operation": operation,
		"database":  database,
		"table":     table,
		"success":   fmt.Sprintf("%t", success),
	}

	// Record operation count
	r.IncrementCounterWithLabels("database_operations_total", labels)

	// Record operation duration
	r.ObserveHistogramWithLabels("database_operation_duration_seconds", duration.Seconds(), labels)
}

// RecordCacheStats records cache statistics
func (r *Registry) RecordCacheStats(name string, hits, misses, size, capacity int64) {
	labels := map[string]string{
		"cache": name,
	}

	// Record hits
	hitGauge, err := r.getOrCreateGauge(fmt.Sprintf("%s_cache_hits", name))
	if err == nil {
		r.mu.Lock()
		r.meter.RegisterCallback(
			func(_ context.Context, o api.Observer) error {
				o.ObserveInt64(hitGauge, hits)
				return nil
			},
			hitGauge,
		)
		r.mu.Unlock()
	}

	// Record misses
	missGauge, err := r.getOrCreateGauge(fmt.Sprintf("%s_cache_misses", name))
	if err == nil {
		r.mu.Lock()
		r.meter.RegisterCallback(
			func(_ context.Context, o api.Observer) error {
				o.ObserveInt64(missGauge, misses)
				return nil
			},
			missGauge,
		)
		r.mu.Unlock()
	}

	// Record size
	sizeGauge, err := r.getOrCreateGauge(fmt.Sprintf("%s_cache_size", name))
	if err == nil {
		r.mu.Lock()
		r.meter.RegisterCallback(
			func(_ context.Context, o api.Observer) error {
				o.ObserveInt64(sizeGauge, size)
				return nil
			},
			sizeGauge,
		)
		r.mu.Unlock()
	}

	// Record capacity
	capacityGauge, err := r.getOrCreateGauge(fmt.Sprintf("%s_cache_capacity", name))
	if err == nil {
		r.mu.Lock()
		r.meter.RegisterCallback(
			func(_ context.Context, o api.Observer) error {
				o.ObserveInt64(capacityGauge, capacity)
				return nil
			},
			capacityGauge,
		)
		r.mu.Unlock()
	}

	// Calculate and record hit ratio
	if hits+misses > 0 {
		hitRatio := float64(hits) / float64(hits+misses)
		r.ObserveHistogramWithLabels("cache_hit_ratio", hitRatio, labels)
	}
}

// getOrCreateGauge gets or creates a gauge
func (r *Registry) getOrCreateGauge(name string) (api.Int64ObservableGauge, error) {
	r.mu.RLock()
	gauge, exists := r.gauges[name]
	r.mu.RUnlock()

	if exists {
		return gauge, nil
	}

	// Create a new gauge
	gauge, err := r.meter.Int64ObservableGauge(name,
		api.WithDescription(fmt.Sprintf("%s gauge", name)),
	)
	if err != nil {
		return nil, err
	}

	// Store the gauge
	r.mu.Lock()
	r.gauges[name] = gauge
	r.mu.Unlock()

	return gauge, nil
}

// RecordResourceUtilization records resource utilization metrics
func (r *Registry) RecordResourceUtilization(cpuPercent, memoryPercent float64, memoryUsedBytes int64) {
	// CPU utilization
	cpuGauge, err := r.getOrCreateGauge("cpu_utilization_percent")
	if err == nil {
		r.mu.Lock()
		r.meter.RegisterCallback(
			func(_ context.Context, o api.Observer) error {
				o.ObserveInt64(cpuGauge, int64(cpuPercent))
				return nil
			},
			cpuGauge,
		)
		r.mu.Unlock()
	}

	// Memory utilization percentage
	memPercentGauge, err := r.getOrCreateGauge("memory_utilization_percent")
	if err == nil {
		r.mu.Lock()
		r.meter.RegisterCallback(
			func(_ context.Context, o api.Observer) error {
				o.ObserveInt64(memPercentGauge, int64(memoryPercent))
				return nil
			},
			memPercentGauge,
		)
		r.mu.Unlock()
	}

	// Memory utilization bytes
	memBytesGauge, err := r.getOrCreateGauge("memory_utilization_bytes")
	if err == nil {
		r.mu.Lock()
		r.meter.RegisterCallback(
			func(_ context.Context, o api.Observer) error {
				o.ObserveInt64(memBytesGauge, memoryUsedBytes)
				return nil
			},
			memBytesGauge,
		)
		r.mu.Unlock()
	}
}

// SetComponentStatus sets the status of a component (0=down, 1=degraded, 2=up)
func (r *Registry) SetComponentStatus(component string, status int) {
	statusGauge, err := r.getOrCreateGauge(fmt.Sprintf("component_status_%s", component))
	if err == nil {
		r.mu.Lock()
		r.meter.RegisterCallback(
			func(_ context.Context, o api.Observer) error {
				o.ObserveInt64(statusGauge, int64(status))
				return nil
			},
			statusGauge,
		)
		r.mu.Unlock()
	}
}

// RecordQuerySizeMetrics records query size metrics
func (r *Registry) RecordQuerySizeMetrics(queryLength int, numTables int, numJoins int, numFilters int) {
	// Query text length
	r.ObserveHistogram("query_length_chars", float64(queryLength))

	// Number of tables
	r.ObserveHistogram("query_table_count", float64(numTables))

	// Number of joins
	r.ObserveHistogram("query_join_count", float64(numJoins))

	// Number of filters
	r.ObserveHistogram("query_filter_count", float64(numFilters))
}

// GetPrometheusRegistry returns the underlying Prometheus registry
func (r *Registry) GetPrometheusRegistry() *prometheus.Registry {
	return r.registry
}

// GetMeterProvider returns the underlying meter provider
func (r *Registry) GetMeterProvider() *metric.MeterProvider {
	return r.meterProvider
}

// MustCounter gets or creates a counter, panicking on error
func (r *Registry) MustCounter(name string) api.Int64Counter {
	counter, err := r.getOrCreateCounter(name)
	if err != nil {
		panic(fmt.Sprintf("Failed to create counter %s: %v", name, err))
	}
	return counter
}

// MustHistogram gets or creates a histogram, panicking on error
func (r *Registry) MustHistogram(name string) api.Float64Histogram {
	histogram, err := r.getOrCreateHistogram(name)
	if err != nil {
		panic(fmt.Sprintf("Failed to create histogram %s: %v", name, err))
	}
	return histogram
}

// IsEnabled returns whether metrics collection is enabled
func (r *Registry) IsEnabled() bool {
	if r.config == nil {
		return false
	}
	return r.config.Enabled
}

//Personal.AI order the ending
