// Package router provides HTTP routing functionality for StarRocks proxy.
// It defines and implements the Router interface to set up and manage HTTP routes.
package router

import (
	"net/http"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/turtacn/staravail/internal/application/interfaces"
	logging "github.com/turtacn/staravail/internal/common/logger"
	"github.com/turtacn/staravail/internal/infrastructure/config"
	handlers "github.com/turtacn/staravail/internal/interfaces/http/handlers"
	"github.com/turtacn/staravail/internal/interfaces/http/middleware"
)

// Router defines the interface for HTTP routing.
type Router interface {
	// SetupRoutes configures all routes for the HTTP server.
	SetupRoutes() error

	// GetHandler returns the HTTP handler for the router.
	GetHandler() http.Handler

	// AddMiddleware adds a middleware to the router.
	AddMiddleware(middleware gin.HandlerFunc)

	// Group creates a new route group with the given path prefix and middlewares.
	Group(relativePath string, handlers ...gin.HandlerFunc) *gin.RouterGroup

	// GetEngine returns the underlying router engine.
	GetEngine() *gin.Engine
}

// GinRouter implements the Router interface using the Gin web framework.
type GinRouter struct {
	engine           *gin.Engine
	serviceDeps      interfaces.ServiceDependencies
	config           *config.HTTPConfig
	logger           *zap.Logger
	queryHandler     *handlers.QueryHandler
	writeHandler     *handlers.WriteHandler
	metadataHandler  *handlers.MetadataHandler
	healthHandler    *handlers.HealthHandler
	adminHandler     *handlers.AdminHandler
	userHandler      *handlers.UserHandler
	streamHandler    *handlers.StreamHandler
	metricsCollector *metrics.MetricsCollector
}

// NewGinRouter creates a new GinRouter instance.
func NewGinRouter(
	serviceDeps interfaces.ServiceDependencies,
	config *config.HTTPConfig,
	metricsCollector *metrics.MetricsCollector,
) (Router, error) {
	logger := logging.GetLogger().Named("http.router")

	// Set Gin mode based on configuration
	if config.Debug {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	engine := gin.New()

	// Create handler instances
	queryHandler, err := handlers.NewQueryHandler(serviceDeps, metricsCollector)
	if err != nil {
		return nil, err
	}

	writeHandler, err := handlers.NewWriteHandler(serviceDeps, metricsCollector)
	if err != nil {
		return nil, err
	}

	metadataHandler, err := handlers.NewMetadataHandler(serviceDeps, metricsCollector)
	if err != nil {
		return nil, err
	}

	healthHandler, err := handlers.NewHealthHandler(serviceDeps, metricsCollector)
	if err != nil {
		return nil, err
	}

	adminHandler, err := handlers.NewAdminHandler(serviceDeps, metricsCollector)
	if err != nil {
		return nil, err
	}

	userHandler, err := handlers.NewUserHandler(serviceDeps, metricsCollector)
	if err != nil {
		return nil, err
	}

	streamHandler, err := handlers.NewStreamHandler(serviceDeps, metricsCollector)
	if err != nil {
		return nil, err
	}

	return &GinRouter{
		engine:           engine,
		serviceDeps:      serviceDeps,
		config:           config,
		logger:           logger,
		queryHandler:     queryHandler,
		writeHandler:     writeHandler,
		metadataHandler:  metadataHandler,
		healthHandler:    healthHandler,
		adminHandler:     adminHandler,
		userHandler:      userHandler,
		streamHandler:    streamHandler,
		metricsCollector: metricsCollector,
	}, nil
}

// SetupRoutes configures all routes for the HTTP server.
func (r *GinRouter) SetupRoutes() error {
	r.setupGlobalMiddleware()
	r.setupHealthRoutes()
	r.setupQueryRoutes()
	r.setupWriteRoutes()
	r.setupMetadataRoutes()
	r.setupAdminRoutes()
	r.setupUserRoutes()
	r.setupStreamRoutes()
	r.setupMetricsRoutes()
	r.setupDebugRoutes()
	r.setupSwaggerRoutes()
	r.setupStaticRoutes()

	return nil
}

// GetHandler returns the HTTP handler for the router.
func (r *GinRouter) GetHandler() http.Handler {
	return r.engine
}

// AddMiddleware adds a middleware to the router.
func (r *GinRouter) AddMiddleware(middleware gin.HandlerFunc) {
	r.engine.Use(middleware)
}

// Group creates a new route group with the given path prefix and middlewares.
func (r *GinRouter) Group(relativePath string, handlers ...gin.HandlerFunc) *gin.RouterGroup {
	return r.engine.Group(relativePath, handlers...)
}

// GetEngine returns the underlying router engine.
func (r *GinRouter) GetEngine() *gin.Engine {
	return r.engine
}

// setupGlobalMiddleware configures global middleware for all routes.
func (r *GinRouter) setupGlobalMiddleware() {
	// Recovery middleware to recover from panics
	r.engine.Use(gin.Recovery())

	// Logger middleware for request logging
	r.engine.Use(middleware.Logger(r.logger))

	// Request ID middleware to assign a unique ID to each request
	r.engine.Use(middleware.RequestID())

	// Metrics middleware to collect metrics for requests
	r.engine.Use(middleware.Metrics(r.metricsCollector))

	// CORS middleware for cross-origin requests
	corsConfig := cors.Config{
		AllowOrigins:     r.config.CORS.AllowOrigins,
		AllowMethods:     r.config.CORS.AllowMethods,
		AllowHeaders:     r.config.CORS.AllowHeaders,
		ExposeHeaders:    r.config.CORS.ExposeHeaders,
		AllowCredentials: r.config.CORS.AllowCredentials,
		MaxAge:           time.Duration(r.config.CORS.MaxAge) * time.Second,
	}
	r.engine.Use(cors.New(corsConfig))

	// Enable compression middleware if configured
	if r.config.EnableCompression {
		r.engine.Use(middleware.Compression())
	}

	// Configure timeouts if specified
	if r.config.ReadTimeout > 0 {
		r.engine.Use(middleware.Timeout(time.Duration(r.config.ReadTimeout) * time.Second))
	}
}

// setupHealthRoutes configures health check routes.
func (r *GinRouter) setupHealthRoutes() {
	health := r.engine.Group("/health")
	{
		health.GET("/liveness", r.healthHandler.Liveness)
		health.GET("/readiness", r.healthHandler.Readiness)
		health.GET("/cluster", r.healthHandler.ClusterHealth)
		health.GET("/database/:database", r.healthHandler.DatabaseHealth)
		health.GET("/table/:database/:table", r.healthHandler.TableHealth)
		health.GET("/backend/:backendID", r.healthHandler.BackendHealth)
		health.GET("/frontend/:frontendID", r.healthHandler.FrontendHealth)
		health.GET("/component/:component", r.healthHandler.ComponentHealth)
		health.GET("/diagnostics/:scope/:target", r.healthHandler.Diagnostics)
		health.GET("/history/:scope/:target", r.healthHandler.HealthHistory)
		health.POST("/subscribe", r.healthHandler.SubscribeHealthUpdates)
		health.POST("/unsubscribe/:subscriptionID", r.healthHandler.UnsubscribeHealthUpdates)
		health.POST("/report", r.healthHandler.ReportHealthIssue)
	}
}

// setupQueryRoutes configures routes for query operations.
func (r *GinRouter) setupQueryRoutes() {
	// Create a query group with authentication middleware
	query := r.engine.Group("/api/v1/query", middleware.Authentication())

	// Apply rate limiting if configured
	if r.config.RateLimit.Enabled {
		query.Use(middleware.RateLimit(
			r.config.RateLimit.RequestsPerSecond,
			r.config.RateLimit.Burst,
			time.Duration(r.config.RateLimit.TimeoutSeconds)*time.Second,
		))
	}

	// Query routes
	{
		query.POST("/execute", r.queryHandler.ExecuteQuery)
		query.POST("/execute/arrow", r.queryHandler.ExecuteQueryArrow)
		query.POST("/explain", r.queryHandler.ExplainQuery)
		query.GET("/active", r.queryHandler.GetActiveQueries)
		query.GET("/history", r.queryHandler.GetQueryHistory)
		query.GET("/metrics/:queryID", r.queryHandler.GetQueryMetrics)
		query.POST("/cancel/:queryID", r.queryHandler.CancelQuery)
		query.POST("/analyze", r.queryHandler.AnalyzeQuery)
	}
}

// setupWriteRoutes configures routes for write operations.
func (r *GinRouter) setupWriteRoutes() {
	// Create a write group with authentication and authorization middleware
	write := r.engine.Group("/api/v1/write",
		middleware.Authentication(),
		middleware.Authorization("write"))

	// Apply rate limiting if configured
	if r.config.RateLimit.Enabled {
		write.Use(middleware.RateLimit(
			r.config.RateLimit.RequestsPerSecond,
			r.config.RateLimit.Burst,
			time.Duration(r.config.RateLimit.TimeoutSeconds)*time.Second,
		))
	}

	// Write routes
	{
		write.POST("/execute", r.writeHandler.ExecuteUpdate)
		write.POST("/batch", r.writeHandler.ExecuteBatch)
		write.POST("/load", r.writeHandler.LoadData)
		write.POST("/stream-load", r.writeHandler.StreamLoad)
		write.GET("/load-status/:loadID", r.writeHandler.GetLoadStatus)
		write.POST("/cancel-load/:loadID", r.writeHandler.CancelLoad)
	}
}

// setupMetadataRoutes configures routes for metadata operations.
func (r *GinRouter) setupMetadataRoutes() {
	// Create a metadata group with authentication middleware
	metadata := r.engine.Group("/api/v1/metadata", middleware.Authentication())

	// Metadata routes
	{
		metadata.GET("/databases", r.metadataHandler.GetDatabases)
		metadata.GET("/database/:database", r.metadataHandler.GetDatabaseInfo)
		metadata.GET("/tables/:database", r.metadataHandler.GetTables)
		metadata.GET("/table/:database/:table", r.metadataHandler.GetTableInfo)
		metadata.GET("/columns/:database/:table", r.metadataHandler.GetColumns)
		metadata.GET("/partitions/:database/:table", r.metadataHandler.GetPartitions)
		metadata.GET("/distribution/:database/:table", r.metadataHandler.GetDistribution)
		metadata.GET("/stats/:database/:table", r.metadataHandler.GetTableStats)
		metadata.GET("/stats/:database/:table/:column", r.metadataHandler.GetColumnStats)
		metadata.POST("/refresh-stats/:database/:table", r.metadataHandler.RefreshStats)
		metadata.GET("/schema/:database/:table", r.metadataHandler.GetSchema)
		metadata.GET("/backends", r.metadataHandler.GetBackends)
		metadata.GET("/frontends", r.metadataHandler.GetFrontends)
		metadata.GET("/jobs/:jobType", r.metadataHandler.GetJobs)
		metadata.GET("/search", r.metadataHandler.SearchObjects)
	}
}

// setupAdminRoutes configures routes for admin operations.
func (r *GinRouter) setupAdminRoutes() {
	// Create an admin group with authentication and admin authorization middleware
	admin := r.engine.Group("/api/v1/admin",
		middleware.Authentication(),
		middleware.Authorization("admin"))

	// Admin routes
	{
		admin.GET("/config", r.adminHandler.GetConfig)
		admin.POST("/config", r.adminHandler.UpdateConfig)
		admin.GET("/status", r.adminHandler.GetStatus)
		admin.POST("/flush", r.adminHandler.FlushCache)
		admin.POST("/restart", r.adminHandler.RestartService)
		admin.GET("/logs", r.adminHandler.GetLogs)
		admin.POST("/log-level", r.adminHandler.SetLogLevel)
		admin.GET("/tasks", r.adminHandler.GetTasks)
		admin.POST("/task/cancel/:taskID", r.adminHandler.CancelTask)
		admin.GET("/connections", r.adminHandler.GetConnections)
		admin.POST("/connection/close/:connectionID", r.adminHandler.CloseConnection)
	}
}

// setupUserRoutes configures routes for user management operations.
func (r *GinRouter) setupUserRoutes() {
	// Create a user group with authentication middleware
	user := r.engine.Group("/api/v1/user", middleware.Authentication())

	// User management routes (require admin authorization)
	userAdmin := user.Group("/", middleware.Authorization("admin"))
	{
		userAdmin.GET("/list", r.userHandler.ListUsers)
		userAdmin.POST("/create", r.userHandler.CreateUser)
		userAdmin.POST("/update", r.userHandler.UpdateUser)
		userAdmin.POST("/delete/:username", r.userHandler.DeleteUser)
		userAdmin.GET("/privileges/:username", r.userHandler.ListPrivileges)
		userAdmin.POST("/grant", r.userHandler.GrantPrivilege)
		userAdmin.POST("/revoke", r.userHandler.RevokePrivilege)
		userAdmin.GET("/roles", r.userHandler.ListRoles)
		userAdmin.GET("/role/:roleName", r.userHandler.GetRole)
		userAdmin.POST("/role/create", r.userHandler.CreateRole)
		userAdmin.POST("/role/delete/:roleName", r.userHandler.DeleteRole)
		userAdmin.POST("/role/grant", r.userHandler.GrantRoleToUser)
		userAdmin.POST("/role/revoke", r.userHandler.RevokeRoleFromUser)
	}

	// User profile routes (available to authenticated users)
	{
		user.GET("/profile", r.userHandler.GetUserProfile)
		user.POST("/change-password", r.userHandler.ChangePassword)
	}

	// Authentication routes (no authentication required)
	auth := r.engine.Group("/auth")
	{
		auth.POST("/login", r.userHandler.Login)
		auth.POST("/logout", r.userHandler.Logout)
		auth.POST("/refresh-token", r.userHandler.RefreshToken)
	}
}

// setupStreamRoutes configures routes for streaming operations.
func (r *GinRouter) setupStreamRoutes() {
	// Create a stream group with authentication middleware
	stream := r.engine.Group("/api/v1/stream", middleware.Authentication())

	// Stream routes
	{
		stream.POST("/load/create", r.streamHandler.CreateStreamLoad)
		stream.POST("/load/append/:sessionID", r.streamHandler.AppendStreamData)
		stream.POST("/load/commit/:sessionID", r.streamHandler.CommitStreamLoad)
		stream.POST("/load/abort/:sessionID", r.streamHandler.AbortStreamLoad)
		stream.GET("/load/status/:sessionID", r.streamHandler.GetStreamLoadStatus)
		stream.POST("/subscribe", r.streamHandler.SubscribeStream)
		stream.POST("/unsubscribe/:subscriptionID", r.streamHandler.UnsubscribeStream)
		stream.GET("/ws/:subscriptionID", r.streamHandler.WebSocketStream)
	}
}

// setupMetricsRoutes configures routes for metrics.
func (r *GinRouter) setupMetricsRoutes() {
	// Metrics routes - typically accessible without authentication
	metrics := r.engine.Group("/metrics")
	{
		metrics.GET("", gin.WrapH(promhttp.Handler()))
		metrics.GET("/proxy", r.healthHandler.ProxyMetrics)
		metrics.GET("/query", r.healthHandler.QueryMetrics)
		metrics.GET("/write", r.healthHandler.WriteMetrics)
		metrics.GET("/connections", r.healthHandler.ConnectionMetrics)
		metrics.GET("/cache", r.healthHandler.CacheMetrics)
	}
}

// setupDebugRoutes configures routes for debugging.
func (r *GinRouter) setupDebugRoutes() {
	// Debug routes - only enabled in debug mode
	if r.config.Debug {
		debug := r.engine.Group("/debug")
		{
			// Register pprof routes if in debug mode
			pprof.RouteRegister(debug)

			debug.GET("/vars", gin.WrapH(http.HandlerFunc(expvarHandler)))
			debug.POST("/gc", r.adminHandler.TriggerGC)
			debug.GET("/goroutines", r.adminHandler.GetGoroutines)
		}
	}
}

// expvarHandler handles expvar requests.
func expvarHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	http.DefaultServeMux.ServeHTTP(w, r)
}

// setupSwaggerRoutes configures routes for Swagger API documentation.
func (r *GinRouter) setupSwaggerRoutes() {
	// Swagger routes - typically accessible without authentication
	if r.config.EnableSwagger {
		r.engine.GET("/swagger/*any", gin.WrapH(http.StripPrefix("/swagger", http.FileServer(http.Dir("./swagger")))))
	}
}

// setupStaticRoutes configures routes for static files.
func (r *GinRouter) setupStaticRoutes() {
	// Static file routes - typically accessible without authentication
	if r.config.EnableStaticFiles {
		r.engine.Static("/static", "./static")
		r.engine.StaticFile("/favicon.ico", "./static/favicon.ico")
	}

	// Root route redirect to status page or documentation
	r.engine.GET("/", func(c *gin.Context) {
		c.Redirect(http.StatusFound, "/static/index.html")
	})
}

// ChiRouter is an alternative implementation of the Router interface using the Chi router.
// This is included as an example of how to support alternative router implementations.
type ChiRouter struct {
	// Implementation would go here
}

// FastHTTPRouter is another alternative implementation using the fasthttp router.
// This is included as an example of how to support alternative router implementations.
type FastHTTPRouter struct {
	// Implementation would go here
}

//Personal.AI order the ending
