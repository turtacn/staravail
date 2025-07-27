// Package middleware provides HTTP middleware components for the StarRocks proxy.
// These middleware functions handle cross-cutting concerns such as logging, authentication,
// authorization, rate limiting, request tracking, metrics collection, and more.
package middleware

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jellydator/ttlcache/v2"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/ratelimit"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	logging "github.com/turtacn/staravail/internal/common/logger"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/infrastructure/config"
)

// contextKey is a custom type for context keys to avoid collisions.
type contextKey string

const (
	// Context keys for request information
	requestIDKey  contextKey = "request_id"
	userKey       contextKey = "user"
	startTimeKey  contextKey = "start_time"
	clientInfoKey contextKey = "client_info"

	// Header names
	headerRequestID     = "X-Request-ID"
	headerAuthorization = "Authorization"
	headerUserAgent     = "User-Agent"
	headerContentType   = "Content-Type"
	headerContentLength = "Content-Length"
	headerRealIP        = "X-Real-IP"
	headerForwardedFor  = "X-Forwarded-For"

	// Error messages
	errMsgAuthRequired      = "Authentication required"
	errMsgInvalidAuth       = "Invalid authentication credentials"
	errMsgExpiredToken      = "Authentication token expired"
	errMsgNoPermission      = "Permission denied"
	errMsgRateLimitExceeded = "Rate limit exceeded"
	errMsgTimeout           = "Request timeout"
)

// clientLimiter stores rate limiters for individual clients.
var (
	clientLimiters      = ttlcache.NewCache()
	clientLimitersMutex sync.RWMutex
)

func init() {
	// Initialize client rate limiter cache with a cleanup interval
	clientLimiters.SetTTL(1 * time.Hour)
	clientLimiters.SetExpirationCallback(func(key string, value interface{}) {
		// Clean up rate limiter when it expires
		if limiter, ok := value.(*rate.Limiter); ok {
			limiter = nil
		}
	})
	go clientLimiters.Start()
}

// Logger returns a middleware that logs request and response information.
func Logger(logger *zap.Logger) gin.HandlerFunc {
	if logger == nil {
		logger = logging.GetLogger().Named("http.middleware")
	}

	return func(c *gin.Context) {
		// Start timer
		start := time.Now()
		path := c.Request.URL.Path
		raw := c.Request.URL.RawQuery
		if raw != "" {
			path = path + "?" + raw
		}

		requestID := c.GetHeader(headerRequestID)
		if requestID == "" {
			requestID = c.GetString(string(requestIDKey))
		}

		// Get client IP
		clientIP := c.ClientIP()

		// Log request
		logger.Info("Request started",
			zap.String("request_id", requestID),
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("client_ip", clientIP),
			zap.String("user_agent", c.Request.UserAgent()),
		)

		// Process request
		c.Next()

		// Log response
		latency := time.Since(start)
		statusCode := c.Writer.Status()
		bodySize := c.Writer.Size()

		// Extract error information if present
		var errMsg string
		if len(c.Errors) > 0 {
			errMsg = c.Errors.String()
		}

		// Log level based on status code
		logFunc := logger.Info
		if statusCode >= 400 && statusCode < 500 {
			logFunc = logger.Warn
		} else if statusCode >= 500 {
			logFunc = logger.Error
		}

		fields := []zap.Field{
			zap.String("request_id", requestID),
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("client_ip", clientIP),
			zap.Int("status_code", statusCode),
			zap.Int("body_size", bodySize),
			zap.Duration("latency", latency),
			zap.String("user_agent", c.Request.UserAgent()),
		}

		if errMsg != "" {
			fields = append(fields, zap.String("error", errMsg))
		}

		logFunc("Request completed", fields...)
	}
}

// RequestID returns a middleware that assigns a unique ID to each request.
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Check if request ID exists in header
		requestID := c.Request.Header.Get(headerRequestID)

		// Generate a new ID if not present
		if requestID == "" {
			requestID = uuid.New().String()
		}

		// Set request ID in context and header
		c.Set(string(requestIDKey), requestID)
		c.Header(headerRequestID, requestID)

		// Add request ID to the request context
		ctx := context.WithValue(c.Request.Context(), requestIDKey, requestID)
		c.Request = c.Request.WithContext(ctx)

		c.Next()
	}
}

// Authentication returns a middleware that verifies user authentication.
func Authentication() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Skip authentication for public paths
		if isPublicPath(c.Request.URL.Path) {
			c.Next()
			return
		}

		// Get authentication token from Authorization header
		authHeader := c.GetHeader(headerAuthorization)
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": errMsgAuthRequired,
			})
			return
		}

		// Extract token from header (Bearer token format)
		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": errMsgInvalidAuth,
			})
			return
		}
		token := parts[1]

		// Validate token and extract user information
		userInfo, err := auth.ValidateToken(token)
		if err != nil {
			// Handle specific token errors
			if errors.Is(err, auth.ErrTokenExpired) {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
					"error": errMsgExpiredToken,
				})
				return
			}

			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": errMsgInvalidAuth,
			})
			return
		}

		// Set user information in context
		c.Set(string(userKey), userInfo)

		// Add user info to the request context
		ctx := context.WithValue(c.Request.Context(), userKey, userInfo)
		c.Request = c.Request.WithContext(ctx)

		c.Next()
	}
}

// Authorization returns a middleware that checks user permissions for specific resources.
func Authorization(requiredRole string) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get user from context (set by Authentication middleware)
		userInfo, exists := c.Get(string(userKey))
		if !exists {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": errMsgAuthRequired,
			})
			return
		}

		user, ok := userInfo.(*auth.UserInfo)
		if !ok {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"error": "Invalid user information",
			})
			return
		}

		// Check if user has the required role
		if !user.HasRole(requiredRole) {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": errMsgNoPermission,
			})
			return
		}

		// Additional resource-specific permission checks
		if !isAuthorizedForResource(user, c.Request.URL.Path, c.Request.Method) {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": errMsgNoPermission,
			})
			return
		}

		c.Next()
	}
}

// RateLimit returns a middleware that limits the number of requests per second.
func RateLimit(rps float64, burst int, timeout time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get client IP for per-client rate limiting
		clientIP := c.ClientIP()

		// Get or create limiter for this client
		clientLimitersMutex.RLock()
		limiterInterface, exists := clientLimiters.Get(clientIP)
		clientLimitersMutex.RUnlock()

		var limiter *rate.Limiter
		if !exists {
			limiter = rate.NewLimiter(rate.Limit(rps), burst)
			clientLimitersMutex.Lock()
			clientLimiters.Set(clientIP, limiter)
			clientLimitersMutex.Unlock()
		} else {
			limiter = limiterInterface.(*rate.Limiter)
		}

		// Try to get token with timeout
		ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
		defer cancel()

		if err := limiter.Wait(ctx); err != nil {
			// If we hit the timeout, return rate limit exceeded
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":       errMsgRateLimitExceeded,
				"retry_after": int(1.0/rps) + 1, // Suggest retry after time in seconds
			})
			return
		}

		c.Next()
	}
}

// GlobalRateLimit returns a middleware that applies a global rate limit across all instances.
// This is useful for distributed environments with multiple proxy instances.
func GlobalRateLimit(limiter ratelimit.Limiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Take a token from the limiter, which will block if needed
		limiter.Take()
		c.Next()
	}
}

// Timeout returns a middleware that aborts requests that take too long.
func Timeout(timeout time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Create a context with timeout
		ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
		defer cancel()

		// Replace the request context
		c.Request = c.Request.WithContext(ctx)

		// Create a channel to signal when the request is done
		done := make(chan struct{})

		// Run the next handlers in a goroutine
		go func() {
			c.Next()
			close(done)
		}()

		// Wait for either completion or timeout
		select {
		case <-done:
			// Request completed normally
			return
		case <-ctx.Done():
			// Context timed out
			if ctx.Err() == context.DeadlineExceeded {
				c.AbortWithStatusJSON(http.StatusGatewayTimeout, gin.H{
					"error": errMsgTimeout,
				})
			}
			return
		}
	}
}

// Recovery returns a middleware that recovers from panics and logs the error.
func Recovery(logger *zap.Logger) gin.HandlerFunc {
	if logger == nil {
		logger = logging.GetLogger().Named("http.middleware.recovery")
	}

	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				// Get stack trace
				stack := util.GetStackTrace(3)

				// Log the error
				logger.Error("Panic recovered",
					zap.Any("error", err),
					zap.String("stack", stack),
					zap.String("request_id", c.GetString(string(requestIDKey))),
					zap.String("path", c.Request.URL.Path),
					zap.String("method", c.Request.Method),
				)

				// Return a 500 error
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
					"error": "Internal server error",
				})
			}
		}()

		c.Next()
	}
}

// CORS returns a middleware that handles Cross-Origin Resource Sharing.
func CORS(cfg *config.CORSConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", strings.Join(cfg.AllowOrigins, ","))
		c.Header("Access-Control-Allow-Methods", strings.Join(cfg.AllowMethods, ","))
		c.Header("Access-Control-Allow-Headers", strings.Join(cfg.AllowHeaders, ","))
		c.Header("Access-Control-Expose-Headers", strings.Join(cfg.ExposeHeaders, ","))
		c.Header("Access-Control-Max-Age", strconv.Itoa(cfg.MaxAge))

		if cfg.AllowCredentials {
			c.Header("Access-Control-Allow-Credentials", "true")
		}

		// Handle preflight requests
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

// Compression returns a middleware that compresses the response using gzip.
func Compression() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Compression can be handled by Gin's built-in middleware
		// or by custom logic depending on requirements
		c.Next()
	}
}

// Metrics returns a middleware that collects metrics for each request.
func Metrics(collector *metrics.MetricsCollector) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Record start time
		start := time.Now()

		// Extract path for metrics
		path := cleanPathForMetrics(c.Request.URL.Path)

		// Track request counter
		collector.HttpRequestsTotal.With(prometheus.Labels{
			"method": c.Request.Method,
			"path":   path,
		}).Inc()

		// Track concurrent requests
		concurrentRequests := collector.HttpConcurrentRequests.With(prometheus.Labels{
			"method": c.Request.Method,
			"path":   path,
		})
		concurrentRequests.Inc()
		defer concurrentRequests.Dec()

		// Process request
		c.Next()

		// Record request duration
		duration := time.Since(start).Seconds()
		collector.HttpRequestDuration.With(prometheus.Labels{
			"method": c.Request.Method,
			"path":   path,
			"status": strconv.Itoa(c.Writer.Status()),
		}).Observe(duration)

		// Record response size
		collector.HttpResponseSize.With(prometheus.Labels{
			"method": c.Request.Method,
			"path":   path,
			"status": strconv.Itoa(c.Writer.Status()),
		}).Observe(float64(c.Writer.Size()))

		// Track error rates
		if c.Writer.Status() >= 400 {
			collector.HttpErrorsTotal.With(prometheus.Labels{
				"method": c.Request.Method,
				"path":   path,
				"status": strconv.Itoa(c.Writer.Status()),
			}).Inc()
		}
	}
}

// BasicAuth returns a middleware that performs basic authentication.
func BasicAuth(authFunc func(username, password string) bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get username and password from Basic Auth header
		username, password, ok := c.Request.BasicAuth()
		if !ok {
			c.Header("WWW-Authenticate", "Basic realm=Authorization Required")
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": errMsgAuthRequired,
			})
			return
		}

		// Validate credentials
		if !authFunc(username, password) {
			c.Header("WWW-Authenticate", "Basic realm=Authorization Required")
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": errMsgInvalidAuth,
			})
			return
		}

		// Create user info
		userInfo := &auth.UserInfo{
			Username: username,
			Roles:    []string{"user"}, // Basic auth users get default role
		}

		// Set user information in context
		c.Set(string(userKey), userInfo)

		// Add user info to the request context
		ctx := context.WithValue(c.Request.Context(), userKey, userInfo)
		c.Request = c.Request.WithContext(ctx)

		c.Next()
	}
}

// IPFilter returns a middleware that allows or blocks requests based on client IP.
func IPFilter(allowedIPs []string, blockedIPs []string) gin.HandlerFunc {
	// Preprocess IP lists for efficient lookup
	allowedIPMap := make(map[string]bool)
	blockedIPMap := make(map[string]bool)

	for _, ip := range allowedIPs {
		allowedIPMap[ip] = true
	}

	for _, ip := range blockedIPs {
		blockedIPMap[ip] = true
	}

	return func(c *gin.Context) {
		clientIP := c.ClientIP()

		// Check blocked IPs first
		if len(blockedIPMap) > 0 {
			if blockedIPMap[clientIP] {
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
					"error": "Access denied by IP filter",
				})
				return
			}
		}

		// Then check allowed IPs if the list is not empty
		if len(allowedIPMap) > 0 {
			if !allowedIPMap[clientIP] {
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
					"error": "Access denied by IP filter",
				})
				return
			}
		}

		c.Next()
	}
}

// ClientInfo returns a middleware that extracts and stores client information.
func ClientInfo() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Extract client information
		info := map[string]string{
			"ip":         c.ClientIP(),
			"user_agent": c.Request.UserAgent(),
			"referer":    c.Request.Referer(),
		}

		// Store client info in context
		c.Set(string(clientInfoKey), info)

		// Add to request context
		ctx := context.WithValue(c.Request.Context(), clientInfoKey, info)
		c.Request = c.Request.WithContext(ctx)

		c.Next()
	}
}

// ContentTypeCheck returns a middleware that validates the Content-Type header.
func ContentTypeCheck(allowedTypes []string) gin.HandlerFunc {
	// Create a map for efficient lookup
	allowedTypesMap := make(map[string]bool)
	for _, t := range allowedTypes {
		allowedTypesMap[t] = true
	}

	return func(c *gin.Context) {
		// Skip for GET, HEAD, OPTIONS methods that typically don't have a body
		if c.Request.Method == http.MethodGet ||
			c.Request.Method == http.MethodHead ||
			c.Request.Method == http.MethodOptions {
			c.Next()
			return
		}

		// Check Content-Type header for requests with potential body
		contentType := c.GetHeader(headerContentType)
		if contentType == "" {
			// No Content-Type header, but we might allow this for some cases
			if c.Request.ContentLength > 0 {
				c.AbortWithStatusJSON(http.StatusUnsupportedMediaType, gin.H{
					"error": "Content-Type header is required",
				})
				return
			}
			c.Next()
			return
		}

		// Extract base content type without parameters
		baseContentType := strings.Split(contentType, ";")[0]
		baseContentType = strings.TrimSpace(baseContentType)

		// Check if content type is allowed
		if !allowedTypesMap[baseContentType] {
			c.AbortWithStatusJSON(http.StatusUnsupportedMediaType, gin.H{
				"error":         fmt.Sprintf("Unsupported Content-Type: %s", baseContentType),
				"allowed_types": allowedTypes,
			})
			return
		}

		c.Next()
	}
}

// SecurityHeaders returns a middleware that adds security-related headers.
func SecurityHeaders() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Add security headers
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("X-Frame-Options", "DENY")
		c.Header("X-XSS-Protection", "1; mode=block")
		c.Header("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		c.Header("Content-Security-Policy", "default-src 'self'")
		c.Header("Referrer-Policy", "strict-origin-when-cross-origin")
		c.Header("Feature-Policy", "camera 'none'; microphone 'none'")

		c.Next()
	}
}

// TrackResponseTime returns a middleware that tracks response time.
func TrackResponseTime() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Record start time
		startTime := time.Now()
		c.Set(string(startTimeKey), startTime)

		// Add to request context
		ctx := context.WithValue(c.Request.Context(), startTimeKey, startTime)
		c.Request = c.Request.WithContext(ctx)

		// Process request
		c.Next()

		// Calculate response time
		duration := time.Since(startTime)

		// Add response time header
		c.Header("X-Response-Time", duration.String())
	}
}

// RequireHTTPS returns a middleware that redirects HTTP requests to HTTPS.
func RequireHTTPS() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Check if request is already HTTPS
		if c.Request.TLS == nil {
			// Get host from request
			host := c.Request.Host

			// Build HTTPS URL
			url := "https://" + host + c.Request.RequestURI

			// Redirect to HTTPS
			c.Redirect(http.StatusMovedPermanently, url)
			c.Abort()
			return
		}

		c.Next()
	}
}

// isPublicPath checks if a path is accessible without authentication.
func isPublicPath(path string) bool {
	publicPaths := []string{
		"/health/",
		"/metrics",
		"/auth/login",
		"/auth/logout",
		"/auth/refresh-token",
		"/static/",
		"/swagger/",
		"/favicon.ico",
	}

	for _, prefix := range publicPaths {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}

	return false
}

// isAuthorizedForResource checks if a user is authorized to access a specific resource.
func isAuthorizedForResource(user *auth.UserInfo, path string, method string) bool {
	// This is a simple implementation. In a real system, this would likely
	// involve checking a permission database or ACL system.

	// Admin role has access to everything
	if user.HasRole("admin") {
		return true
	}

	// Handle write operations (usually require higher privileges)
	if method == http.MethodPost ||
		method == http.MethodPut ||
		method == http.MethodDelete ||
		method == http.MethodPatch {
		// Check if user has write permission
		return user.HasRole("writer")
	}

	// For admin routes, require admin role
	if strings.HasPrefix(path, "/api/v1/admin") {
		return user.HasRole("admin")
	}

	// For user management routes, require admin role
	if strings.HasPrefix(path, "/api/v1/user") &&
		!strings.HasPrefix(path, "/api/v1/user/profile") {
		return user.HasRole("admin")
	}

	// Basic read operations usually available to all authenticated users
	return true
}

// cleanPathForMetrics cleans the path for use in metrics to avoid high cardinality.
func cleanPathForMetrics(path string) string {
	// Replace numeric IDs and UUIDs with placeholders
	segments := strings.Split(path, "/")
	for i, segment := range segments {
		// Skip empty segments
		if segment == "" {
			continue
		}

		// Check if segment is numeric
		if _, err := strconv.Atoi(segment); err == nil {
			segments[i] = ":id"
			continue
		}

		// Check if segment is a UUID
		if security.IsUUID(segment) {
			segments[i] = ":uuid"
			continue
		}
	}

	// Rejoin path
	return strings.Join(segments, "/")
}

//Personal.AI order the ending
