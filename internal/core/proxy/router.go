// Package proxy provides the core proxy component for the system.
package proxy

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	logging "github.com/turtacn/staravail/internal/common/logger"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/domain/health"
	"github.com/turtacn/staravail/internal/domain/query"
	"github.com/turtacn/staravail/internal/domain/write"
	"github.com/turtacn/staravail/internal/infrastructure/config"
)

// RoutingStrategy defines the routing strategy to use.
type RoutingStrategy string

const (
	// StrategyPreferAvailability prioritizes availability over consistency.
	StrategyPreferAvailability RoutingStrategy = "prefer_availability"
	// StrategyPreferConsistency prioritizes consistency over availability.
	StrategyPreferConsistency RoutingStrategy = "prefer_consistency"
	// StrategyBalanced balances between availability and consistency.
	StrategyBalanced RoutingStrategy = "balanced"
)

// RouteTarget defines a destination for routing.
type RouteTarget struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Address     string            `json:"address"`
	Weight      int               `json:"weight"`
	Tags        map[string]string `json:"tags"`
	IsReadOnly  bool              `json:"is_read_only"`
	IsPreferred bool              `json:"is_preferred"`
}

// QueryRouteRequest represents a request to route a query.
type QueryRouteRequest struct {
	Query           string            `json:"query"`
	User            string            `json:"user"`
	Database        string            `json:"database"`
	Timeout         time.Duration     `json:"timeout"`
	ReadConsistency string            `json:"read_consistency"`
	Hints           map[string]string `json:"hints"`
	IsAnalytical    bool              `json:"is_analytical"`
	Priority        int               `json:"priority"`
	RequestID       string            `json:"request_id"`
}

// WriteRouteRequest represents a request to route a write operation.
type WriteRouteRequest struct {
	Statement        string            `json:"statement"`
	User             string            `json:"user"`
	Database         string            `json:"database"`
	Timeout          time.Duration     `json:"timeout"`
	WriteConsistency string            `json:"write_consistency"`
	Hints            map[string]string `json:"hints"`
	IsBatch          bool              `json:"is_batch"`
	Priority         int               `json:"priority"`
	RequestID        string            `json:"request_id"`
}

// RouteDecision represents the result of a routing decision.
type RouteDecision struct {
	Target          *RouteTarget      `json:"target"`
	FallbackTargets []*RouteTarget    `json:"fallback_targets,omitempty"`
	Strategy        RoutingStrategy   `json:"strategy"`
	DecisionTime    time.Time         `json:"decision_time"`
	CacheHit        bool              `json:"cache_hit"`
	Metadata        map[string]string `json:"metadata,omitempty"`
	TTL             time.Duration     `json:"ttl,omitempty"`
}

// RoutingStats represents statistics about routing decisions.
type RoutingStats struct {
	TotalQueries          int64            `json:"total_queries"`
	TotalWrites           int64            `json:"total_writes"`
	AverageQueryLatency   time.Duration    `json:"average_query_latency"`
	AverageWriteLatency   time.Duration    `json:"average_write_latency"`
	CacheHitRate          float64          `json:"cache_hit_rate"`
	TargetUsageCounts     map[string]int64 `json:"target_usage_counts"`
	ErrorCounts           map[string]int64 `json:"error_counts"`
	StrategyUsage         map[string]int64 `json:"strategy_usage"`
	QueryTypeDistribution map[string]int64 `json:"query_type_distribution"`
	WriteTypeDistribution map[string]int64 `json:"write_type_distribution"`
	RecentDecisions       []*RouteDecision `json:"recent_decisions"`
	HealthyTargets        []*RouteTarget   `json:"healthy_targets"`
	UnhealthyTargets      []*RouteTarget   `json:"unhealthy_targets"`
}

// RoutingError represents an error that occurred during routing.
type RoutingError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

// Error implements the error interface.
func (e *RoutingError) Error() string {
	if e.Details != "" {
		return fmt.Sprintf("%s: %s (%s)", e.Code, e.Message, e.Details)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Common routing error codes
var (
	ErrNoHealthyTargets     = &RoutingError{Code: "NO_HEALTHY_TARGETS", Message: "No healthy targets available for routing"}
	ErrInvalidRequest       = &RoutingError{Code: "INVALID_REQUEST", Message: "Invalid routing request"}
	ErrUnsupportedOperation = &RoutingError{Code: "UNSUPPORTED_OPERATION", Message: "Operation not supported by available targets"}
	ErrTimeout              = &RoutingError{Code: "TIMEOUT", Message: "Routing decision timed out"}
	ErrInternalError        = &RoutingError{Code: "INTERNAL_ERROR", Message: "Internal routing error"}
)

// RequestRouter defines the interface for routing requests.
type RequestRouter interface {
	// RouteQuery routes a query request to an appropriate target.
	RouteQuery(ctx context.Context, req *QueryRouteRequest) (*RouteDecision, error)

	// RouteWrite routes a write request to an appropriate target.
	RouteWrite(ctx context.Context, req *WriteRouteRequest) (*RouteDecision, error)

	// GetRoutingStats returns statistics about routing decisions.
	GetRoutingStats() *RoutingStats

	// RefreshTargets refreshes the list of available targets.
	RefreshTargets(ctx context.Context) error

	// SetDefaultStrategy sets the default routing strategy.
	SetDefaultStrategy(strategy RoutingStrategy)

	// ClearCache clears the routing decision cache.
	ClearCache()
}

// StarRocksRouter implements the RequestRouter interface.
type StarRocksRouter struct {
	queryService    query.QueryService
	writeService    write.WriteService
	healthChecker   health.HealthChecker
	metricsRegistry *metrics.Registry
	config          *config.RoutingConfig
	logger          *zap.Logger

	targets      []*RouteTarget
	targetsMutex sync.RWMutex

	decisionCache   cache.Cache
	defaultStrategy RoutingStrategy

	// Stats tracking
	statsMutex            sync.RWMutex
	totalQueries          int64
	totalWrites           int64
	totalQueryLatency     time.Duration
	totalWriteLatency     time.Duration
	cacheHits             int64
	cacheMisses           int64
	targetUsageCounts     map[string]int64
	errorCounts           map[string]int64
	strategyUsage         map[string]int64
	queryTypeDistribution map[string]int64
	writeTypeDistribution map[string]int64
	recentDecisions       []*RouteDecision

	// Metrics
	queryLatency       prometheus.Histogram
	writeLatency       prometheus.Histogram
	cacheHitRatio      prometheus.Gauge
	routingErrors      *prometheus.CounterVec
	targetSelections   *prometheus.CounterVec
	strategySelections *prometheus.CounterVec
}

// NewStarRocksRouter creates a new StarRocksRouter instance.
func NewStarRocksRouter(
	queryService query.QueryService,
	writeService write.WriteService,
	healthChecker health.HealthChecker,
	metricsRegistry *metrics.Registry,
	config *config.RoutingConfig,
) *StarRocksRouter {
	logger := logging.GetLogger().Named("router")

	// Create decision cache
	cacheConfig := cache.CacheConfig{
		MaxSize:   config.CacheMaxSize,
		TTL:       time.Duration(config.CacheTTLSeconds) * time.Second,
		Algorithm: cache.LRU,
	}
	decisionCache := cache.NewLRUCache(cacheConfig)

	router := &StarRocksRouter{
		queryService:          queryService,
		writeService:          writeService,
		healthChecker:         healthChecker,
		metricsRegistry:       metricsRegistry,
		config:                config,
		logger:                logger,
		decisionCache:         decisionCache,
		defaultStrategy:       RoutingStrategy(config.DefaultStrategy),
		targetUsageCounts:     make(map[string]int64),
		errorCounts:           make(map[string]int64),
		strategyUsage:         make(map[string]int64),
		queryTypeDistribution: make(map[string]int64),
		writeTypeDistribution: make(map[string]int64),
		recentDecisions:       make([]*RouteDecision, 0, 100),
	}

	// Initialize metrics
	router.initializeMetrics()

	return router
}

// initializeMetrics sets up the router metrics.
func (r *StarRocksRouter) initializeMetrics() {
	// Query latency histogram
	r.queryLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "starrocks_proxy",
		Subsystem: "router",
		Name:      "query_routing_latency_seconds",
		Help:      "Time taken to make query routing decisions",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 10), // from 1ms to ~1s
	})

	// Write latency histogram
	r.writeLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "starrocks_proxy",
		Subsystem: "router",
		Name:      "write_routing_latency_seconds",
		Help:      "Time taken to make write routing decisions",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 10), // from 1ms to ~1s
	})

	// Cache hit ratio gauge
	r.cacheHitRatio = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "starrocks_proxy",
		Subsystem: "router",
		Name:      "cache_hit_ratio",
		Help:      "Ratio of cache hits to total routing requests",
	})

	// Routing errors counter vector
	r.routingErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "starrocks_proxy",
			Subsystem: "router",
			Name:      "routing_errors_total",
			Help:      "Total number of routing errors by error code",
		},
		[]string{"error_code"},
	)

	// Target selections counter vector
	r.targetSelections = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "starrocks_proxy",
			Subsystem: "router",
			Name:      "target_selections_total",
			Help:      "Total number of times each target was selected",
		},
		[]string{"target_id", "operation_type"},
	)

	// Strategy selections counter vector
	r.strategySelections = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "starrocks_proxy",
			Subsystem: "router",
			Name:      "strategy_selections_total",
			Help:      "Total number of times each routing strategy was selected",
		},
		[]string{"strategy"},
	)

	// Register metrics with the registry
	r.metricsRegistry.RegisterMetric("query_routing_latency", r.queryLatency)
	r.metricsRegistry.RegisterMetric("write_routing_latency", r.writeLatency)
	r.metricsRegistry.RegisterMetric("cache_hit_ratio", r.cacheHitRatio)
	r.metricsRegistry.RegisterMetric("routing_errors", r.routingErrors)
	r.metricsRegistry.RegisterMetric("target_selections", r.targetSelections)
	r.metricsRegistry.RegisterMetric("strategy_selections", r.strategySelections)
}

// Start initializes the router.
func (r *StarRocksRouter) Start() error {
	// Initialize targets
	if err := r.RefreshTargets(context.Background()); err != nil {
		return fmt.Errorf("failed to refresh targets: %w", err)
	}

	// Start periodic target refresh
	go r.startPeriodicTargetRefresh()

	// Start periodic stats update
	go r.startPeriodicStatsUpdate()

	r.logger.Info("Router started", zap.Int("targets", len(r.targets)))
	return nil
}

// startPeriodicTargetRefresh periodically refreshes the list of targets.
func (r *StarRocksRouter) startPeriodicTargetRefresh() {
	ticker := time.NewTicker(time.Duration(r.config.RefreshIntervalSeconds) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := r.RefreshTargets(ctx); err != nil {
			r.logger.Error("Failed to refresh targets", zap.Error(err))
		}
		cancel()
	}
}

// startPeriodicStatsUpdate periodically updates statistics.
func (r *StarRocksRouter) startPeriodicStatsUpdate() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		r.updateCacheHitRatio()
	}
}

// updateCacheHitRatio updates the cache hit ratio metric.
func (r *StarRocksRouter) updateCacheHitRatio() {
	r.statsMutex.RLock()
	total := r.cacheHits + r.cacheMisses
	hits := r.cacheHits
	r.statsMutex.RUnlock()

	ratio := 0.0
	if total > 0 {
		ratio = float64(hits) / float64(total)
	}

	r.cacheHitRatio.Set(ratio)
}

// RouteQuery routes a query request to an appropriate target.
func (r *StarRocksRouter) RouteQuery(ctx context.Context, req *QueryRouteRequest) (*RouteDecision, error) {
	if req == nil {
		return nil, ErrInvalidRequest
	}

	startTime := time.Now()
	cacheKey := r.generateQueryCacheKey(req)

	// Try to get from cache
	if cached, found := r.decisionCache.Get(cacheKey); found {
		if decision, ok := cached.(*RouteDecision); ok {
			// Update stats
			r.updateQueryStats(decision, time.Since(startTime), true)
			return decision, nil
		}
	}

	// Determine routing strategy
	strategy := r.determineQueryRoutingStrategy(req)

	// Get healthy targets
	healthyTargets, err := r.getHealthyTargets()
	if err != nil {
		routingErr := &RoutingError{
			Code:    "HEALTH_CHECK_ERROR",
			Message: "Failed to get healthy targets",
			Details: err.Error(),
		}
		r.recordError(routingErr)
		return nil, routingErr
	}

	if len(healthyTargets) == 0 {
		r.recordError(ErrNoHealthyTargets)
		return nil, ErrNoHealthyTargets
	}

	// Filter targets based on query characteristics
	eligibleTargets := r.filterTargetsForQuery(healthyTargets, req)
	if len(eligibleTargets) == 0 {
		routingErr := &RoutingError{
			Code:    "NO_ELIGIBLE_TARGETS",
			Message: "No eligible targets available for this query",
		}
		r.recordError(routingErr)
		return nil, routingErr
	}

	// Select target based on strategy
	selectedTarget, fallbackTargets := r.selectQueryTarget(eligibleTargets, req, strategy)

	// Create decision
	decision := &RouteDecision{
		Target:          selectedTarget,
		FallbackTargets: fallbackTargets,
		Strategy:        strategy,
		DecisionTime:    time.Now(),
		CacheHit:        false,
		Metadata:        make(map[string]string),
		TTL:             time.Duration(r.config.CacheTTLSeconds) * time.Second,
	}

	// Add metadata
	decision.Metadata["query_type"] = r.classifyQuery(req.Query)
	if req.IsAnalytical {
		decision.Metadata["analytical"] = "true"
	}

	// Cache the decision
	r.decisionCache.Set(cacheKey, decision, decision.TTL)

	// Update stats
	r.updateQueryStats(decision, time.Since(startTime), false)

	return decision, nil
}

// RouteWrite routes a write request to an appropriate target.
func (r *StarRocksRouter) RouteWrite(ctx context.Context, req *WriteRouteRequest) (*RouteDecision, error) {
	if req == nil {
		return nil, ErrInvalidRequest
	}

	startTime := time.Now()
	cacheKey := r.generateWriteCacheKey(req)

	// Write operations are less cacheable, but we can still cache for very short periods
	// for batched operations to reuse the same target
	if req.IsBatch {
		if cached, found := r.decisionCache.Get(cacheKey); found {
			if decision, ok := cached.(*RouteDecision); ok {
				// Update stats
				r.updateWriteStats(decision, time.Since(startTime), true)
				return decision, nil
			}
		}
	}

	// Determine routing strategy
	strategy := r.determineWriteRoutingStrategy(req)

	// Get healthy targets
	healthyTargets, err := r.getHealthyTargets()
	if err != nil {
		routingErr := &RoutingError{
			Code:    "HEALTH_CHECK_ERROR",
			Message: "Failed to get healthy targets",
			Details: err.Error(),
		}
		r.recordError(routingErr)
		return nil, routingErr
	}

	if len(healthyTargets) == 0 {
		r.recordError(ErrNoHealthyTargets)
		return nil, ErrNoHealthyTargets
	}

	// Filter targets that can accept writes
	writableTargets := r.filterTargetsForWrite(healthyTargets, req)
	if len(writableTargets) == 0 {
		routingErr := &RoutingError{
			Code:    "NO_WRITABLE_TARGETS",
			Message: "No writable targets available for this operation",
		}
		r.recordError(routingErr)
		return nil, routingErr
	}

	// Select target based on strategy
	selectedTarget, fallbackTargets := r.selectWriteTarget(writableTargets, req, strategy)

	// Create decision
	decision := &RouteDecision{
		Target:          selectedTarget,
		FallbackTargets: fallbackTargets,
		Strategy:        strategy,
		DecisionTime:    time.Now(),
		CacheHit:        false,
		Metadata:        make(map[string]string),
		TTL:             10 * time.Second, // Shorter TTL for write operations
	}

	// Add metadata
	decision.Metadata["write_type"] = r.classifyWrite(req.Statement)
	if req.IsBatch {
		decision.Metadata["batch"] = "true"
		// Cache batch write decisions for a short time
		r.decisionCache.Set(cacheKey, decision, decision.TTL)
	}

	// Update stats
	r.updateWriteStats(decision, time.Since(startTime), false)

	return decision, nil
}

// GetRoutingStats returns statistics about routing decisions.
func (r *StarRocksRouter) GetRoutingStats() *RoutingStats {
	r.statsMutex.RLock()
	defer r.statsMutex.RUnlock()

	// Get healthy/unhealthy targets
	healthyTargets, unhealthyTargets := r.getTargetsByHealth()

	// Calculate average latencies
	var avgQueryLatency, avgWriteLatency time.Duration
	if r.totalQueries > 0 {
		avgQueryLatency = time.Duration(int64(r.totalQueryLatency) / r.totalQueries)
	}
	if r.totalWrites > 0 {
		avgWriteLatency = time.Duration(int64(r.totalWriteLatency) / r.totalWrites)
	}

	// Calculate cache hit rate
	cacheHitRate := 0.0
	totalRequests := r.cacheHits + r.cacheMisses
	if totalRequests > 0 {
		cacheHitRate = float64(r.cacheHits) / float64(totalRequests)
	}

	// Copy maps to prevent concurrent access
	targetUsageCounts := make(map[string]int64, len(r.targetUsageCounts))
	for k, v := range r.targetUsageCounts {
		targetUsageCounts[k] = v
	}

	errorCounts := make(map[string]int64, len(r.errorCounts))
	for k, v := range r.errorCounts {
		errorCounts[k] = v
	}

	strategyUsage := make(map[string]int64, len(r.strategyUsage))
	for k, v := range r.strategyUsage {
		strategyUsage[k] = v
	}

	queryTypeDistribution := make(map[string]int64, len(r.queryTypeDistribution))
	for k, v := range r.queryTypeDistribution {
		queryTypeDistribution[k] = v
	}

	writeTypeDistribution := make(map[string]int64, len(r.writeTypeDistribution))
	for k, v := range r.writeTypeDistribution {
		writeTypeDistribution[k] = v
	}

	// Copy recent decisions (limited to last 100)
	recentDecisions := make([]*RouteDecision, len(r.recentDecisions))
	copy(recentDecisions, r.recentDecisions)

	return &RoutingStats{
		TotalQueries:          r.totalQueries,
		TotalWrites:           r.totalWrites,
		AverageQueryLatency:   avgQueryLatency,
		AverageWriteLatency:   avgWriteLatency,
		CacheHitRate:          cacheHitRate,
		TargetUsageCounts:     targetUsageCounts,
		ErrorCounts:           errorCounts,
		StrategyUsage:         strategyUsage,
		QueryTypeDistribution: queryTypeDistribution,
		WriteTypeDistribution: writeTypeDistribution,
		RecentDecisions:       recentDecisions,
		HealthyTargets:        healthyTargets,
		UnhealthyTargets:      unhealthyTargets,
	}
}

// RefreshTargets refreshes the list of available targets.
func (r *StarRocksRouter) RefreshTargets(ctx context.Context) error {
	// This would typically fetch targets from service discovery or configuration
	// For this example, we'll use the static config

	targets := make([]*RouteTarget, 0, len(r.config.Targets))
	for _, targetConfig := range r.config.Targets {
		target := &RouteTarget{
			ID:          targetConfig.ID,
			Name:        targetConfig.Name,
			Address:     targetConfig.Address,
			Weight:      targetConfig.Weight,
			Tags:        targetConfig.Tags,
			IsReadOnly:  targetConfig.IsReadOnly,
			IsPreferred: targetConfig.IsPreferred,
		}
		targets = append(targets, target)
	}

	// Update targets with thread safety
	r.targetsMutex.Lock()
	r.targets = targets
	r.targetsMutex.Unlock()

	r.logger.Info("Refreshed routing targets", zap.Int("count", len(targets)))

	// Since targets changed, clear the decision cache
	r.ClearCache()

	return nil
}

// SetDefaultStrategy sets the default routing strategy.
func (r *StarRocksRouter) SetDefaultStrategy(strategy RoutingStrategy) {
	r.defaultStrategy = strategy
	r.logger.Info("Default routing strategy set", zap.String("strategy", string(strategy)))

	// Clear cache as routing decisions may change with the new strategy
	r.ClearCache()
}

// ClearCache clears the routing decision cache.
func (r *StarRocksRouter) ClearCache() {
	r.decisionCache.Clear()
	r.logger.Info("Routing decision cache cleared")
}

// getHealthyTargets returns a list of currently healthy targets.
func (r *StarRocksRouter) getHealthyTargets() ([]*RouteTarget, error) {
	r.targetsMutex.RLock()
	allTargets := r.targets
	r.targetsMutex.RUnlock()

	healthyTargets := make([]*RouteTarget, 0, len(allTargets))

	for _, target := range allTargets {
		isHealthy, err := r.healthChecker.IsHealthy(target.ID)
		if err != nil {
			r.logger.Warn("Health check error",
				zap.String("target_id", target.ID),
				zap.Error(err))
			continue
		}

		if isHealthy {
			healthyTargets = append(healthyTargets, target)
		}
	}

	return healthyTargets, nil
}

// getTargetsByHealth returns separate lists of healthy and unhealthy targets.
func (r *StarRocksRouter) getTargetsByHealth() ([]*RouteTarget, []*RouteTarget) {
	r.targetsMutex.RLock()
	allTargets := r.targets
	r.targetsMutex.RUnlock()

	healthyTargets := make([]*RouteTarget, 0, len(allTargets))
	unhealthyTargets := make([]*RouteTarget, 0, len(allTargets))

	for _, target := range allTargets {
		isHealthy, err := r.healthChecker.IsHealthy(target.ID)
		if err != nil || !isHealthy {
			unhealthyTargets = append(unhealthyTargets, target)
		} else {
			healthyTargets = append(healthyTargets, target)
		}
	}

	return healthyTargets, unhealthyTargets
}

// filterTargetsForQuery filters targets based on query characteristics.
func (r *StarRocksRouter) filterTargetsForQuery(targets []*RouteTarget, req *QueryRouteRequest) []*RouteTarget {
	// Start with all targets
	eligibleTargets := make([]*RouteTarget, 0, len(targets))

	// Process query hints
	if targetHint, ok := req.Hints["target"]; ok {
		// If a specific target is hinted, try to use only that one
		for _, target := range targets {
			if target.ID == targetHint || target.Name == targetHint {
				return []*RouteTarget{target}
			}
		}
		// If hinted target not found, fall through to normal selection
	}

	queryType := r.classifyQuery(req.Query)
	isAnalytical := req.IsAnalytical

	for _, target := range targets {
		// Check if the target is suitable for this query type
		if isAnalytical {
			// For analytical queries, prefer targets with higher capacity
			if weight := target.Weight; weight > r.config.AnalyticalQueryMinWeight {
				eligibleTargets = append(eligibleTargets, target)
			}
		} else if queryType == "read" {
			// Any target can handle read queries
			eligibleTargets = append(eligibleTargets, target)
		} else if queryType == "system" {
			// For system queries, prefer primary nodes
			if target.IsPreferred {
				eligibleTargets = append(eligibleTargets, target)
			}
		}
	}

	// If no targets were eligible based on query type, fall back to any target
	if len(eligibleTargets) == 0 {
		return targets
	}

	return eligibleTargets
}

// filterTargetsForWrite filters targets that can accept writes.
func (r *StarRocksRouter) filterTargetsForWrite(targets []*RouteTarget, req *WriteRouteRequest) []*RouteTarget {
	// Start with all targets
	writableTargets := make([]*RouteTarget, 0, len(targets))

	// Process write hints
	if targetHint, ok := req.Hints["target"]; ok {
		// If a specific target is hinted, try to use only that one
		for _, target := range targets {
			if target.ID == targetHint || target.Name == targetHint {
				if !target.IsReadOnly {
					return []*RouteTarget{target}
				}
				// If hinted target is read-only, it can't be used for writes
				break
			}
		}
		// If hinted target not found or is read-only, fall through to normal selection
	}

	for _, target := range targets {
		// Skip read-only targets for write operations
		if target.IsReadOnly {
			continue
		}

		// Preferred targets get priority for writes
		if target.IsPreferred {
			writableTargets = append(writableTargets, target)
		} else {
			// If consistency is strict, only use preferred targets
			if req.WriteConsistency == "strict" && len(writableTargets) > 0 {
				continue
			}
			writableTargets = append(writableTargets, target)
		}
	}

	return writableTargets
}

// selectQueryTarget selects a target for a query based on the routing strategy.
func (r *StarRocksRouter) selectQueryTarget(
	eligibleTargets []*RouteTarget,
	req *QueryRouteRequest,
	strategy RoutingStrategy,
) (*RouteTarget, []*RouteTarget) {
	if len(eligibleTargets) == 0 {
		return nil, nil
	}

	if len(eligibleTargets) == 1 {
		return eligibleTargets[0], nil
	}

	// Create a slice for fallback targets
	fallbackTargets := make([]*RouteTarget, 0, len(eligibleTargets)-1)

	// Default to the first target
	selectedTarget := eligibleTargets[0]

	switch strategy {
	case StrategyPreferAvailability:
		// Select based on weight (higher weight = more capacity)
		highestWeight := eligibleTargets[0].Weight
		for _, target := range eligibleTargets {
			if target.Weight > highestWeight {
				fallbackTargets = append(fallbackTargets, selectedTarget)
				selectedTarget = target
				highestWeight = target.Weight
			} else {
				fallbackTargets = append(fallbackTargets, target)
			}
		}

	case StrategyPreferConsistency:
		// Prefer targets marked as preferred (typically primaries)
		preferredFound := false
		for _, target := range eligibleTargets {
			if target.IsPreferred {
				if !preferredFound {
					selectedTarget = target
					preferredFound = true
				} else {
					fallbackTargets = append(fallbackTargets, target)
				}
			} else {
				fallbackTargets = append(fallbackTargets, target)
			}
		}

	case StrategyBalanced:
		// Load balancing - select based on current usage and weight
		// This is a simplified version - a real implementation would track load
		lowestUsage := int64(^uint64(0) >> 1) // Max int64
		r.statsMutex.RLock()
		for _, target := range eligibleTargets {
			usage := r.targetUsageCounts[target.ID]
			adjustedUsage := usage / int64(target.Weight+1) // Adjust for weight

			if adjustedUsage < lowestUsage {
				if selectedTarget != nil {
					fallbackTargets = append(fallbackTargets, selectedTarget)
				}
				selectedTarget = target
				lowestUsage = adjustedUsage
			} else {
				fallbackTargets = append(fallbackTargets, target)
			}
		}
		r.statsMutex.RUnlock()

	default:
		// Unknown strategy, just use the first target
		for i, target := range eligibleTargets {
			if i > 0 {
				fallbackTargets = append(fallbackTargets, target)
			}
		}
	}

	return selectedTarget, fallbackTargets
}

// selectWriteTarget selects a target for a write operation based on the routing strategy.
func (r *StarRocksRouter) selectWriteTarget(
	writableTargets []*RouteTarget,
	req *WriteRouteRequest,
	strategy RoutingStrategy,
) (*RouteTarget, []*RouteTarget) {
	if len(writableTargets) == 0 {
		return nil, nil
	}

	if len(writableTargets) == 1 {
		return writableTargets[0], nil
	}

	// For write operations, we typically want to prioritize consistency
	// So we'll prefer the "primary" (preferred) nodes

	preferredTargets := make([]*RouteTarget, 0, len(writableTargets))
	nonPreferredTargets := make([]*RouteTarget, 0, len(writableTargets))

	for _, target := range writableTargets {
		if target.IsPreferred {
			preferredTargets = append(preferredTargets, target)
		} else {
			nonPreferredTargets = append(nonPreferredTargets, target)
		}
	}

	// If we have preferred targets, select from those
	if len(preferredTargets) > 0 {
		// For writes, we'll use the same strategy selection regardless of the strategy
		// This ensures writes go to appropriate targets

		// Select the preferred target with the lowest current usage
		selectedTarget := preferredTargets[0]
		lowestUsage := int64(^uint64(0) >> 1) // Max int64

		r.statsMutex.RLock()
		for _, target := range preferredTargets {
			usage := r.targetUsageCounts[target.ID]
			if usage < lowestUsage {
				selectedTarget = target
				lowestUsage = usage
			}
		}
		r.statsMutex.RUnlock()

		// Create fallback targets list
		fallbackTargets := make([]*RouteTarget, 0, len(writableTargets)-1)
		for _, target := range writableTargets {
			if target != selectedTarget {
				fallbackTargets = append(fallbackTargets, target)
			}
		}

		return selectedTarget, fallbackTargets
	}

	// If no preferred targets, fall back to all writable targets
	// Select the one with the lowest usage
	selectedTarget := writableTargets[0]
	lowestUsage := int64(^uint64(0) >> 1) // Max int64

	r.statsMutex.RLock()
	for _, target := range writableTargets {
		usage := r.targetUsageCounts[target.ID]
		if usage < lowestUsage {
			selectedTarget = target
			lowestUsage = usage
		}
	}
	r.statsMutex.RUnlock()

	// Create fallback targets list
	fallbackTargets := make([]*RouteTarget, 0, len(writableTargets)-1)
	for _, target := range writableTargets {
		if target != selectedTarget {
			fallbackTargets = append(fallbackTargets, target)
		}
	}

	return selectedTarget, fallbackTargets
}

// determineQueryRoutingStrategy determines the routing strategy for a query.
func (r *StarRocksRouter) determineQueryRoutingStrategy(req *QueryRouteRequest) RoutingStrategy {
	// Check if strategy is specified in hints
	if strategyHint, ok := req.Hints["routing_strategy"]; ok {
		switch RoutingStrategy(strategyHint) {
		case StrategyPreferAvailability, StrategyPreferConsistency, StrategyBalanced:
			return RoutingStrategy(strategyHint)
		}
	}

	// Check read consistency level
	switch req.ReadConsistency {
	case "strong":
		return StrategyPreferConsistency
	case "weak":
		return StrategyPreferAvailability
	case "balanced":
		return StrategyBalanced
	}

	// For analytical queries, prefer availability
	if req.IsAnalytical {
		return StrategyPreferAvailability
	}

	// Fall back to default strategy
	return r.defaultStrategy
}

// determineWriteRoutingStrategy determines the routing strategy for a write operation.
func (r *StarRocksRouter) determineWriteRoutingStrategy(req *WriteRouteRequest) RoutingStrategy {
	// Check if strategy is specified in hints
	if strategyHint, ok := req.Hints["routing_strategy"]; ok {
		switch RoutingStrategy(strategyHint) {
		case StrategyPreferAvailability, StrategyPreferConsistency, StrategyBalanced:
			return RoutingStrategy(strategyHint)
		}
	}

	// Check write consistency level
	switch req.WriteConsistency {
	case "strong", "strict":
		return StrategyPreferConsistency
	case "relaxed":
		return StrategyBalanced
	}

	// For batch operations, balance between nodes
	if req.IsBatch {
		return StrategyBalanced
	}

	// For writes, default to consistency
	return StrategyPreferConsistency
}

// classifyQuery analyzes a query to determine its type.
func (r *StarRocksRouter) classifyQuery(query string) string {
	// A real implementation would use SQL parsing
	// This is a simple example

	query = strings.ToUpper(strings.TrimSpace(query))

	if strings.HasPrefix(query, "SELECT") {
		if strings.Contains(query, "SYS.") {
			return "system"
		}
		return "read"
	}

	if strings.HasPrefix(query, "SHOW") ||
		strings.HasPrefix(query, "DESCRIBE") ||
		strings.HasPrefix(query, "DESC") {
		return "system"
	}

	// Consider any query that doesn't modify data as a read query
	return "read"
}

// classifyWrite analyzes a write statement to determine its type.
func (r *StarRocksRouter) classifyWrite(statement string) string {
	// A real implementation would use SQL parsing
	// This is a simple example

	statement = strings.ToUpper(strings.TrimSpace(statement))

	if strings.HasPrefix(statement, "INSERT") {
		return "insert"
	}

	if strings.HasPrefix(statement, "UPDATE") {
		return "update"
	}

	if strings.HasPrefix(statement, "DELETE") {
		return "delete"
	}

	if strings.HasPrefix(statement, "CREATE") {
		return "ddl"
	}

	if strings.HasPrefix(statement, "ALTER") {
		return "ddl"
	}

	if strings.HasPrefix(statement, "DROP") {
		return "ddl"
	}

	// Default
	return "other"
}

// generateQueryCacheKey generates a cache key for a query.
func (r *StarRocksRouter) generateQueryCacheKey(req *QueryRouteRequest) string {
	// A simplified cache key - real implementation would be more sophisticated
	// and would include relevant parts of the query that affect routing
	return fmt.Sprintf("q:%s:db:%s:u:%s:a:%v",
		hash(req.Query),
		req.Database,
		req.User,
		req.IsAnalytical,
	)
}

// generateWriteCacheKey generates a cache key for a write operation.
func (r *StarRocksRouter) generateWriteCacheKey(req *WriteRouteRequest) string {
	// For writes, we include fewer elements in the key
	// as most writes should not be cached for long
	return fmt.Sprintf("w:%s:db:%s:u:%s:b:%v",
		hash(req.Statement),
		req.Database,
		req.User,
		req.IsBatch,
	)
}

// hash creates a simple hash of a string.
func hash(s string) string {
	h := fnv.New32a()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum32())
}

// updateQueryStats updates statistics for a query routing operation.
func (r *StarRocksRouter) updateQueryStats(decision *RouteDecision, latency time.Duration, cacheHit bool) {
	r.statsMutex.Lock()
	defer r.statsMutex.Unlock()

	r.totalQueries++
	r.totalQueryLatency += latency

	if cacheHit {
		r.cacheHits++
	} else {
		r.cacheMisses++
	}

	if decision.Target != nil {
		r.targetUsageCounts[decision.Target.ID]++
		r.targetSelections.WithLabelValues(decision.Target.ID, "query").Inc()
	}

	r.strategyUsage[string(decision.Strategy)]++
	r.strategySelections.WithLabelValues(string(decision.Strategy)).Inc()

	if queryType, ok := decision.Metadata["query_type"]; ok {
		r.queryTypeDistribution[queryType]++
	}

	// Update recent decisions (keep only the last 100)
	r.recentDecisions = append(r.recentDecisions, decision)
	if len(r.recentDecisions) > 100 {
		r.recentDecisions = r.recentDecisions[1:]
	}

	// Update prometheus metrics
	r.queryLatency.Observe(latency.Seconds())
}

// updateWriteStats updates statistics for a write routing operation.
func (r *StarRocksRouter) updateWriteStats(decision *RouteDecision, latency time.Duration, cacheHit bool) {
	r.statsMutex.Lock()
	defer r.statsMutex.Unlock()

	r.totalWrites++
	r.totalWriteLatency += latency

	if cacheHit {
		r.cacheHits++
	} else {
		r.cacheMisses++
	}

	if decision.Target != nil {
		r.targetUsageCounts[decision.Target.ID]++
		r.targetSelections.WithLabelValues(decision.Target.ID, "write").Inc()
	}

	r.strategyUsage[string(decision.Strategy)]++
	r.strategySelections.WithLabelValues(string(decision.Strategy)).Inc()

	if writeType, ok := decision.Metadata["write_type"]; ok {
		r.writeTypeDistribution[writeType]++
	}

	// Update recent decisions (keep only the last 100)
	r.recentDecisions = append(r.recentDecisions, decision)
	if len(r.recentDecisions) > 100 {
		r.recentDecisions = r.recentDecisions[1:]
	}

	// Update prometheus metrics
	r.writeLatency.Observe(latency.Seconds())
}

// recordError records a routing error.
func (r *StarRocksRouter) recordError(err error) {
	if err == nil {
		return
	}

	r.statsMutex.Lock()
	defer r.statsMutex.Unlock()

	var code string
	if routingErr, ok := err.(*RoutingError); ok {
		code = routingErr.Code
	} else {
		code = "UNKNOWN_ERROR"
	}

	r.errorCounts[code]++
	r.routingErrors.WithLabelValues(code).Inc()
}

// RouterFactory defines a factory for creating router instances.
type RouterFactory interface {
	CreateRouter(config *config.Config) (RequestRouter, error)
}

// DefaultRouterFactory is the default implementation of RouterFactory.
type DefaultRouterFactory struct {
	queryServiceFactory  query.ServiceFactory
	writeServiceFactory  write.ServiceFactory
	healthCheckerFactory health.CheckerFactory
	metricsRegistry      *metrics.Registry
}

// NewDefaultRouterFactory creates a new DefaultRouterFactory.
func NewDefaultRouterFactory(
	queryServiceFactory query.ServiceFactory,
	writeServiceFactory write.ServiceFactory,
	healthCheckerFactory health.CheckerFactory,
	metricsRegistry *metrics.Registry,
) *DefaultRouterFactory {
	return &DefaultRouterFactory{
		queryServiceFactory:  queryServiceFactory,
		writeServiceFactory:  writeServiceFactory,
		healthCheckerFactory: healthCheckerFactory,
		metricsRegistry:      metricsRegistry,
	}
}

// CreateRouter creates a new RequestRouter instance.
func (f *DefaultRouterFactory) CreateRouter(config *config.Config) (RequestRouter, error) {
	queryService, err := f.queryServiceFactory.CreateService(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create query service: %w", err)
	}

	writeService, err := f.writeServiceFactory.CreateService(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create write service: %w", err)
	}

	healthChecker, err := f.healthCheckerFactory.CreateChecker(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create health checker: %w", err)
	}

	router := NewStarRocksRouter(
		queryService,
		writeService,
		healthChecker,
		f.metricsRegistry,
		&config.Routing,
	)

	// Start the router
	if err := router.Start(); err != nil {
		return nil, fmt.Errorf("failed to start router: %w", err)
	}

	return router, nil
}

//Personal.AI order the ending
