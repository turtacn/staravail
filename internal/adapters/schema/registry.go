// Package schema provides functionality for working with schema registries and schema management.
package schema

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/turtacn/staravail/internal/common/config"
	"github.com/turtacn/staravail/internal/common/logging"
	"github.com/turtacn/staravail/internal/common/metrics"
)

// SchemaRegistry defines the interface for schema registry clients
type SchemaRegistry interface {
	// GetSchema retrieves a schema by ID
	GetSchema(id string) (string, error)

	// GetSchemaBySubject retrieves a schema by subject and version
	GetSchemaBySubject(subject string, version string) (string, error)

	// RegisterSchema registers a new schema
	RegisterSchema(subject string, schema string) (string, error)

	// CheckCompatibility checks if a schema is compatible with the latest version
	CheckCompatibility(subject string, schema string) (bool, error)

	// GetSubjects retrieves all subjects
	GetSubjects() ([]string, error)

	// GetVersions retrieves all versions for a subject
	GetVersions(subject string) ([]string, error)

	// DeleteSchema deletes a schema version
	DeleteSchema(subject string, version string) error

	// ExtractSchemaID extracts schema ID from Avro binary data
	ExtractSchemaID(avroData []byte) (string, error)

	// GetConfig gets the compatibility configuration for a subject
	GetConfig(subject string) (string, error)

	// SetConfig sets the compatibility configuration for a subject
	SetConfig(subject string, compatibility string) error
}

// CompatibilityLevel defines the compatibility level for schema evolution
type CompatibilityLevel string

const (
	// CompatibilityNone means any schema can be registered
	CompatibilityNone CompatibilityLevel = "NONE"

	// CompatibilityBackward means consumers using a new schema can read data produced with the latest registered schema
	CompatibilityBackward CompatibilityLevel = "BACKWARD"

	// CompatibilityBackwardTransitive means consumers using a new schema can read data produced with all registered schemas
	CompatibilityBackwardTransitive CompatibilityLevel = "BACKWARD_TRANSITIVE"

	// CompatibilityForward means data produced with a new schema can be read by consumers using the latest registered schema
	CompatibilityForward CompatibilityLevel = "FORWARD"

	// CompatibilityForwardTransitive means data produced with a new schema can be read by consumers using all registered schemas
	CompatibilityForwardTransitive CompatibilityLevel = "FORWARD_TRANSITIVE"

	// CompatibilityFull means a new schema can be used to read data produced with the latest registered schema and vice versa
	CompatibilityFull CompatibilityLevel = "FULL"

	// CompatibilityFullTransitive means a new schema can be used to read data produced with all registered schemas and vice versa
	CompatibilityFullTransitive CompatibilityLevel = "FULL_TRANSITIVE"
)

// RegistryType defines the type of schema registry
type RegistryType string

const (
	// RegistryConfluent represents Confluent Schema Registry
	RegistryConfluent RegistryType = "confluent"

	// RegistryApicurio represents Apicurio Registry
	RegistryApicurio RegistryType = "apicurio"

	// RegistryCustom represents a custom schema registry
	RegistryCustom RegistryType = "custom"
)

// SchemaRegistryConfig contains configuration for schema registry client
type SchemaRegistryConfig struct {
	// Type is the type of registry (confluent, apicurio, custom)
	Type RegistryType

	// URL is the base URL of the registry
	URL string

	// Username for authentication (if needed)
	Username string

	// Password for authentication (if needed)
	Password string

	// TimeoutMs is the timeout for registry requests in milliseconds
	TimeoutMs int

	// MaxCacheSize is the maximum number of schemas to cache
	MaxCacheSize int

	// CacheTTLMs is the TTL for cache entries in milliseconds
	CacheTTLMs int

	// DefaultCompatibility is the default compatibility level
	DefaultCompatibility CompatibilityLevel

	// RetryCount is the number of retries for registry requests
	RetryCount int

	// RetryIntervalMs is the interval between retries in milliseconds
	RetryIntervalMs int
}

// DefaultSchemaRegistryConfig returns the default configuration for schema registry client
func DefaultSchemaRegistryConfig() SchemaRegistryConfig {
	return SchemaRegistryConfig{
		Type:                 RegistryConfluent,
		TimeoutMs:            5000,                  // 5 seconds
		MaxCacheSize:         1000,                  // 1000 schemas
		CacheTTLMs:           300000,                // 5 minutes
		DefaultCompatibility: CompatibilityBackward, // Backward compatibility
		RetryCount:           3,                     // 3 retries
		RetryIntervalMs:      1000,                  // 1 second
	}
}

// ConfluentSchemaRegistry implements SchemaRegistry interface for Confluent Schema Registry
type ConfluentSchemaRegistry struct {
	// config is the registry configuration
	config SchemaRegistryConfig

	// client is the HTTP client
	client *http.Client

	// baseURL is the base URL of the registry
	baseURL string

	// logger is used for logging
	logger logging.Logger

	// metrics is used for recording metrics
	metrics metrics.MetricsRecorder

	// schemaCache caches schemas by ID
	schemaCache *sync.Map

	// schemaCacheTTL tracks TTL for cache entries
	schemaCacheTTL *sync.Map

	// versionCache caches schema versions by subject
	versionCache *sync.Map

	// versionCacheTTL tracks TTL for version cache entries
	versionCacheTTL *sync.Map

	// cacheMutex protects cache cleanup operations
	cacheMutex sync.Mutex

	// cleanup indicates if cleanup is running
	cleanup bool
}

// cacheEntry represents a cached item with expiration
type cacheEntry struct {
	// value is the cached value
	value interface{}

	// expiration is the expiration time
	expiration time.Time
}

// NewSchemaRegistry creates a new schema registry client
func NewSchemaRegistry(
	cfg config.SchemaRegistryConfig,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) (SchemaRegistry, error) {
	// Convert to internal config
	config := SchemaRegistryConfig{
		Type:                 RegistryType(cfg.Type),
		URL:                  cfg.URL,
		Username:             cfg.Username,
		Password:             cfg.Password,
		TimeoutMs:            cfg.TimeoutMs,
		MaxCacheSize:         cfg.MaxCacheSize,
		CacheTTLMs:           cfg.CacheTTLMs,
		DefaultCompatibility: CompatibilityLevel(cfg.DefaultCompatibility),
		RetryCount:           cfg.RetryCount,
		RetryIntervalMs:      cfg.RetryIntervalMs,
	}

	// Validate config
	if config.URL == "" {
		return nil, errors.New("registry URL is required")
	}

	// Use default values if not specified
	if config.TimeoutMs <= 0 {
		config.TimeoutMs = DefaultSchemaRegistryConfig().TimeoutMs
	}
	if config.MaxCacheSize <= 0 {
		config.MaxCacheSize = DefaultSchemaRegistryConfig().MaxCacheSize
	}
	if config.CacheTTLMs <= 0 {
		config.CacheTTLMs = DefaultSchemaRegistryConfig().CacheTTLMs
	}
	if config.RetryCount <= 0 {
		config.RetryCount = DefaultSchemaRegistryConfig().RetryCount
	}
	if config.RetryIntervalMs <= 0 {
		config.RetryIntervalMs = DefaultSchemaRegistryConfig().RetryIntervalMs
	}
	if config.DefaultCompatibility == "" {
		config.DefaultCompatibility = DefaultSchemaRegistryConfig().DefaultCompatibility
	}

	// Create HTTP client
	client := &http.Client{
		Timeout: time.Duration(config.TimeoutMs) * time.Millisecond,
	}

	// Create the registry client based on type
	switch config.Type {
	case RegistryConfluent:
		return NewConfluentSchemaRegistry(config, client, logger, metricsRecorder)
	case RegistryApicurio:
		return NewApicurioSchemaRegistry(config, client, logger, metricsRecorder)
	case RegistryCustom:
		return NewCustomSchemaRegistry(config, client, logger, metricsRecorder)
	default:
		return nil, errors.Errorf("unsupported registry type: %s", config.Type)
	}
}

// NewConfluentSchemaRegistry creates a new Confluent Schema Registry client
func NewConfluentSchemaRegistry(
	config SchemaRegistryConfig,
	client *http.Client,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) (*ConfluentSchemaRegistry, error) {
	// Normalize base URL
	baseURL := config.URL
	if !strings.HasSuffix(baseURL, "/") {
		baseURL = baseURL + "/"
	}

	// Create registry client
	registry := &ConfluentSchemaRegistry{
		config:          config,
		client:          client,
		baseURL:         baseURL,
		logger:          logger,
		metrics:         metricsRecorder,
		schemaCache:     &sync.Map{},
		schemaCacheTTL:  &sync.Map{},
		versionCache:    &sync.Map{},
		versionCacheTTL: &sync.Map{},
	}

	// Start cache cleanup goroutine
	go registry.startCacheCleanup()

	return registry, nil
}

// startCacheCleanup starts a background goroutine to clean up expired cache entries
func (r *ConfluentSchemaRegistry) startCacheCleanup() {
	r.cacheMutex.Lock()
	if r.cleanup {
		r.cacheMutex.Unlock()
		return
	}
	r.cleanup = true
	r.cacheMutex.Unlock()

	// Cleanup every minute
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		r.cleanupCache()
	}
}

// cleanupCache removes expired cache entries
func (r *ConfluentSchemaRegistry) cleanupCache() {
	now := time.Now()

	// Cleanup schema cache
	r.schemaCacheTTL.Range(func(key, value interface{}) bool {
		expiration, ok := value.(time.Time)
		if !ok {
			r.schemaCacheTTL.Delete(key)
			r.schemaCache.Delete(key)
			return true
		}

		if now.After(expiration) {
			r.schemaCacheTTL.Delete(key)
			r.schemaCache.Delete(key)
		}
		return true
	})

	// Cleanup version cache
	r.versionCacheTTL.Range(func(key, value interface{}) bool {
		expiration, ok := value.(time.Time)
		if !ok {
			r.versionCacheTTL.Delete(key)
			r.versionCache.Delete(key)
			return true
		}

		if now.After(expiration) {
			r.versionCacheTTL.Delete(key)
			r.versionCache.Delete(key)
		}
		return true
	})
}

// doRequest performs an HTTP request with retries
func (r *ConfluentSchemaRegistry) doRequest(req *http.Request) (*http.Response, error) {
	var resp *http.Response
	var err error

	// Add auth if provided
	if r.config.Username != "" && r.config.Password != "" {
		req.SetBasicAuth(r.config.Username, r.config.Password)
	}

	// Add headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// Start timer for metrics
	startTime := time.Now()

	// Perform request with retries
	for attempt := 0; attempt <= r.config.RetryCount; attempt++ {
		if attempt > 0 {
			// Log retry
			r.logger.Warn("Retrying schema registry request",
				"attempt", attempt,
				"url", req.URL.String())

			// Wait before retry
			time.Sleep(time.Duration(r.config.RetryIntervalMs) * time.Millisecond)
		}

		// Execute request
		resp, err = r.client.Do(req)
		if err == nil && (resp.StatusCode >= 200 && resp.StatusCode < 300) {
			break
		}

		// Log error
		if err != nil {
			r.logger.Error("Schema registry request failed",
				"error", err,
				"attempt", attempt,
				"url", req.URL.String())
			continue
		}

		// Log error response
		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		r.logger.Error("Schema registry returned error",
			"status", resp.StatusCode,
			"body", string(body),
			"attempt", attempt,
			"url", req.URL.String())

		// For some status codes, don't retry
		if resp.StatusCode == http.StatusNotFound ||
			resp.StatusCode == http.StatusUnauthorized ||
			resp.StatusCode == http.StatusForbidden {
			return resp, errors.Errorf("schema registry returned %d: %s", resp.StatusCode, string(body))
		}
	}

	// Record metrics
	duration := time.Since(startTime)
	r.metrics.TimerRecord("schema_registry_request", duration.Seconds()*1000, map[string]string{
		"url":    req.URL.String(),
		"method": req.Method,
	})

	return resp, err
}

// GetSchema retrieves a schema by ID
func (r *ConfluentSchemaRegistry) GetSchema(id string) (string, error) {
	// Check cache
	if schema, ok := r.getCachedSchema(id); ok {
		return schema, nil
	}

	// Build URL
	requestURL := fmt.Sprintf("%sschemas/ids/%s", r.baseURL, id)

	// Create request
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		return "", errors.Wrap(err, "failed to create request")
	}

	// Perform request
	resp, err := r.doRequest(req)
	if err != nil {
		return "", errors.Wrap(err, "failed to get schema")
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return "", errors.Errorf("failed to get schema: %d %s", resp.StatusCode, string(body))
	}

	// Read response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", errors.Wrap(err, "failed to read response body")
	}

	// Parse response
	var response struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return "", errors.Wrap(err, "failed to parse response")
	}

	// Cache schema
	r.cacheSchema(id, response.Schema)

	return response.Schema, nil
}

// GetSchemaBySubject retrieves a schema by subject and version
func (r *ConfluentSchemaRegistry) GetSchemaBySubject(subject string, version string) (string, error) {
	// Check cache
	cacheKey := fmt.Sprintf("%s:%s", subject, version)
	if schema, ok := r.getCachedSchema(cacheKey); ok {
		return schema, nil
	}

	// Use "latest" if version is empty
	if version == "" {
		version = "latest"
	}

	// Build URL
	requestURL := fmt.Sprintf("%ssubjects/%s/versions/%s", r.baseURL, url.PathEscape(subject), version)

	// Create request
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		return "", errors.Wrap(err, "failed to create request")
	}

	// Perform request
	resp, err := r.doRequest(req)
	if err != nil {
		return "", errors.Wrap(err, "failed to get schema")
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return "", errors.Errorf("failed to get schema: %d %s", resp.StatusCode, string(body))
	}

	// Read response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", errors.Wrap(err, "failed to read response body")
	}

	// Parse response
	var response struct {
		Schema string `json:"schema"`
		ID     int    `json:"id"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return "", errors.Wrap(err, "failed to parse response")
	}

	// Cache schema
	r.cacheSchema(cacheKey, response.Schema)
	r.cacheSchema(strconv.Itoa(response.ID), response.Schema)

	return response.Schema, nil
}

// RegisterSchema registers a new schema
func (r *ConfluentSchemaRegistry) RegisterSchema(subject string, schema string) (string, error) {
	// Build URL
	requestURL := fmt.Sprintf("%ssubjects/%s/versions", r.baseURL, url.PathEscape(subject))

	// Create request body
	requestBody := struct {
		Schema string `json:"schema"`
	}{
		Schema: schema,
	}
	requestBodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal request body")
	}

	// Create request
	req, err := http.NewRequest("POST", requestURL, bytes.NewReader(requestBodyBytes))
	if err != nil {
		return "", errors.Wrap(err, "failed to create request")
	}

	// Perform request
	resp, err := r.doRequest(req)
	if err != nil {
		return "", errors.Wrap(err, "failed to register schema")
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := ioutil.ReadAll(resp.Body)
		return "", errors.Errorf("failed to register schema: %d %s", resp.StatusCode, string(body))
	}

	// Read response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", errors.Wrap(err, "failed to read response body")
	}

	// Parse response
	var response struct {
		ID int `json:"id"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return "", errors.Wrap(err, "failed to parse response")
	}

	// Convert ID to string
	idStr := strconv.Itoa(response.ID)

	// Cache schema
	r.cacheSchema(idStr, schema)
	r.cacheSchema(fmt.Sprintf("%s:latest", subject), schema)

	// Invalidate version cache for subject
	r.invalidateVersionCache(subject)

	// Record metrics
	r.metrics.CounterInc("schema_registry_schema_registered", map[string]string{
		"subject": subject,
	})

	return idStr, nil
}

// CheckCompatibility checks if a schema is compatible with the latest version
func (r *ConfluentSchemaRegistry) CheckCompatibility(subject string, schema string) (bool, error) {
	// Build URL
	requestURL := fmt.Sprintf("%scompatibility/subjects/%s/versions/latest", r.baseURL, url.PathEscape(subject))

	// Create request body
	requestBody := struct {
		Schema string `json:"schema"`
	}{
		Schema: schema,
	}
	requestBodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return false, errors.Wrap(err, "failed to marshal request body")
	}

	// Create request
	req, err := http.NewRequest("POST", requestURL, bytes.NewReader(requestBodyBytes))
	if err != nil {
		return false, errors.Wrap(err, "failed to create request")
	}

	// Perform request
	resp, err := r.doRequest(req)
	if err != nil {
		return false, errors.Wrap(err, "failed to check compatibility")
	}
	defer resp.Body.Close()

	// Handle 404 (no schema registered yet)
	if resp.StatusCode == http.StatusNotFound {
		return true, nil
	}

	// Check status code
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return false, errors.Errorf("failed to check compatibility: %d %s", resp.StatusCode, string(body))
	}

	// Read response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, errors.Wrap(err, "failed to read response body")
	}

	// Parse response
	var response struct {
		IsCompatible bool `json:"is_compatible"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return false, errors.Wrap(err, "failed to parse response")
	}

	return response.IsCompatible, nil
}

// GetSubjects retrieves all subjects
func (r *ConfluentSchemaRegistry) GetSubjects() ([]string, error) {
	// Build URL
	requestURL := fmt.Sprintf("%ssubjects", r.baseURL)

	// Create request
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create request")
	}

	// Perform request
	resp, err := r.doRequest(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get subjects")
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return nil, errors.Errorf("failed to get subjects: %d %s", resp.StatusCode, string(body))
	}

	// Read response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response body")
	}

	// Parse response
	var subjects []string
	if err := json.Unmarshal(body, &subjects); err != nil {
		return nil, errors.Wrap(err, "failed to parse response")
	}

	return subjects, nil
}

// GetVersions retrieves all versions for a subject
func (r *ConfluentSchemaRegistry) GetVersions(subject string) ([]string, error) {
	// Check cache
	if versions, ok := r.getCachedVersions(subject); ok {
		return versions, nil
	}

	// Build URL
	requestURL := fmt.Sprintf("%ssubjects/%s/versions", r.baseURL, url.PathEscape(subject))

	// Create request
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create request")
	}

	// Perform request
	resp, err := r.doRequest(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get versions")
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return nil, errors.Errorf("failed to get versions: %d %s", resp.StatusCode, string(body))
	}

	// Read response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response body")
	}

	// Parse response
	var versionNumbers []int
	if err := json.Unmarshal(body, &versionNumbers); err != nil {
		return nil, errors.Wrap(err, "failed to parse response")
	}

	// Convert to strings
	versions := make([]string, len(versionNumbers))
	for i, v := range versionNumbers {
		versions[i] = strconv.Itoa(v)
	}

	// Cache versions
	r.cacheVersions(subject, versions)

	return versions, nil
}

// DeleteSchema deletes a schema version
func (r *ConfluentSchemaRegistry) DeleteSchema(subject string, version string) error {
	// Use "latest" if version is empty
	if version == "" {
		version = "latest"
	}

	// Build URL
	requestURL := fmt.Sprintf("%ssubjects/%s/versions/%s", r.baseURL, url.PathEscape(subject), version)

	// Create request
	req, err := http.NewRequest("DELETE", requestURL, nil)
	if err != nil {
		return errors.Wrap(err, "failed to create request")
	}

	// Perform request
	resp, err := r.doRequest(req)
	if err != nil {
		return errors.Wrap(err, "failed to delete schema")
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := ioutil.ReadAll(resp.Body)
		return errors.Errorf("failed to delete schema: %d %s", resp.StatusCode, string(body))
	}

	// Invalidate caches
	r.invalidateVersionCache(subject)
	r.invalidateSchemaCache(fmt.Sprintf("%s:%s", subject, version))
	r.invalidateSchemaCache(fmt.Sprintf("%s:latest", subject))

	// Record metrics
	r.metrics.CounterInc("schema_registry_schema_deleted", map[string]string{
		"subject": subject,
		"version": version,
	})

	return nil
}

// ExtractSchemaID extracts schema ID from Avro binary data
func (r *ConfluentSchemaRegistry) ExtractSchemaID(avroData []byte) (string, error) {
	// Check if data has the Confluent wire format
	// First byte is magic byte (0), next 4 bytes are schema ID
	if len(avroData) < 5 {
		return "", errors.New("data too short for Confluent wire format")
	}

	// Check magic byte
	if avroData[0] != 0 {
		return "", errors.New("invalid magic byte in Confluent wire format")
	}

	// Extract schema ID (4 bytes, big-endian)
	id := binary.BigEndian.Uint32(avroData[1:5])
	return strconv.FormatUint(uint64(id), 10), nil
}

// GetConfig gets the compatibility configuration for a subject
func (r *ConfluentSchemaRegistry) GetConfig(subject string) (string, error) {
	// Build URL (either global or subject-specific)
	var requestURL string
	if subject == "" {
		requestURL = fmt.Sprintf("%sconfig", r.baseURL)
	} else {
		requestURL = fmt.Sprintf("%sconfig/%s", r.baseURL, url.PathEscape(subject))
	}

	// Create request
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		return "", errors.Wrap(err, "failed to create request")
	}

	// Perform request
	resp, err := r.doRequest(req)
	if err != nil {
		return "", errors.Wrap(err, "failed to get config")
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		// If not found, return default
		if resp.StatusCode == http.StatusNotFound {
			return string(r.config.DefaultCompatibility), nil
		}

		body, _ := ioutil.ReadAll(resp.Body)
		return "", errors.Errorf("failed to get config: %d %s", resp.StatusCode, string(body))
	}

	// Read response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", errors.Wrap(err, "failed to read response body")
	}

	// Parse response
	var response struct {
		Compatibility string `json:"compatibilityLevel"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return "", errors.Wrap(err, "failed to parse response")
	}

	return response.Compatibility, nil
}

// SetConfig sets the compatibility configuration for a subject
func (r *ConfluentSchemaRegistry) SetConfig(subject string, compatibility string) error {
	// Build URL (either global or subject-specific)
	var requestURL string
	if subject == "" {
		requestURL = fmt.Sprintf("%sconfig", r.baseURL)
	} else {
		requestURL = fmt.Sprintf("%sconfig/%s", r.baseURL, url.PathEscape(subject))
	}

	// Create request body
	requestBody := struct {
		Compatibility string `json:"compatibility"`
	}{
		Compatibility: compatibility,
	}
	requestBodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return errors.Wrap(err, "failed to marshal request body")
	}

	// Create request
	req, err := http.NewRequest("PUT", requestURL, bytes.NewReader(requestBodyBytes))
	if err != nil {
		return errors.Wrap(err, "failed to create request")
	}

	// Perform request
	resp, err := r.doRequest(req)
	if err != nil {
		return errors.Wrap(err, "failed to set config")
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := ioutil.ReadAll(resp.Body)
		return errors.Errorf("failed to set config: %d %s", resp.StatusCode, string(body))
	}

	// Record metrics
	labels := map[string]string{
		"compatibility": compatibility,
	}
	if subject != "" {
		labels["subject"] = subject
	}
	r.metrics.CounterInc("schema_registry_config_updated", labels)

	return nil
}

// getCachedSchema gets a schema from cache if available
func (r *ConfluentSchemaRegistry) getCachedSchema(id string) (string, bool) {
	// Check if caching is disabled
	if r.config.MaxCacheSize <= 0 {
		return "", false
	}

	// Get from cache
	value, ok := r.schemaCache.Load(id)
	if !ok {
		return "", false
	}

	// Check expiration
	ttlValue, ok := r.schemaCacheTTL.Load(id)
	if !ok {
		// TTL not found, remove from cache
		r.schemaCache.Delete(id)
		return "", false
	}

	expiration, ok := ttlValue.(time.Time)
	if !ok || time.Now().After(expiration) {
		// Expired, remove from cache
		r.schemaCache.Delete(id)
		r.schemaCacheTTL.Delete(id)
		return "", false
	}

	// Convert to string
	schema, ok := value.(string)
	if !ok {
		// Invalid type, remove from cache
		r.schemaCache.Delete(id)
		r.schemaCacheTTL.Delete(id)
		return "", false
	}

	// Record metrics
	r.metrics.CounterInc("schema_registry_cache_hit", map[string]string{
		"type": "schema",
	})

	return schema, true
}

// cacheSchema caches a schema
func (r *ConfluentSchemaRegistry) cacheSchema(id string, schema string) {
	// Check if caching is disabled
	if r.config.MaxCacheSize <= 0 {
		return
	}

	// Calculate expiration
	expiration := time.Now().Add(time.Duration(r.config.CacheTTLMs) * time.Millisecond)

	// Store in cache
	r.schemaCache.Store(id, schema)
	r.schemaCacheTTL.Store(id, expiration)
}

// getCachedVersions gets versions from cache if available
func (r *ConfluentSchemaRegistry) getCachedVersions(subject string) ([]string, bool) {
	// Check if caching is disabled
	if r.config.MaxCacheSize <= 0 {
		return nil, false
	}

	// Get from cache
	value, ok := r.versionCache.Load(subject)
	if !ok {
		return nil, false
	}

	// Check expiration
	ttlValue, ok := r.versionCacheTTL.Load(subject)
	if !ok {
		// TTL not found, remove from cache
		r.versionCache.Delete(subject)
		return nil, false
	}

	expiration, ok := ttlValue.(time.Time)
	if !ok || time.Now().After(expiration) {
		// Expired, remove from cache
		r.versionCache.Delete(subject)
		r.versionCacheTTL.Delete(subject)
		return nil, false
	}

	// Convert to string slice
	versions, ok := value.([]string)
	if !ok {
		// Invalid type, remove from cache
		r.versionCache.Delete(subject)
		r.versionCacheTTL.Delete(subject)
		return nil, false
	}

	// Record metrics
	r.metrics.CounterInc("schema_registry_cache_hit", map[string]string{
		"type": "versions",
	})

	return versions, true
}

// cacheVersions caches versions
func (r *ConfluentSchemaRegistry) cacheVersions(subject string, versions []string) {
	// Check if caching is disabled
	if r.config.MaxCacheSize <= 0 {
		return
	}

	// Calculate expiration
	expiration := time.Now().Add(time.Duration(r.config.CacheTTLMs) * time.Millisecond)

	// Store in cache
	r.versionCache.Store(subject, versions)
	r.versionCacheTTL.Store(subject, expiration)
}

// invalidateSchemaCache invalidates a schema cache entry
func (r *ConfluentSchemaRegistry) invalidateSchemaCache(id string) {
	r.schemaCache.Delete(id)
	r.schemaCacheTTL.Delete(id)
}

// invalidateVersionCache invalidates a version cache entry
func (r *ConfluentSchemaRegistry) invalidateVersionCache(subject string) {
	r.versionCache.Delete(subject)
	r.versionCacheTTL.Delete(subject)
}

// ApicurioSchemaRegistry implements SchemaRegistry interface for Apicurio Registry
type ApicurioSchemaRegistry struct {
	// config is the registry configuration
	config SchemaRegistryConfig

	// client is the HTTP client
	client *http.Client

	// baseURL is the base URL of the registry
	baseURL string

	// logger is used for logging
	logger logging.Logger

	// metrics is used for recording metrics
	metrics metrics.MetricsRecorder

	// schemaCache caches schemas by ID
	schemaCache *sync.Map

	// schemaCacheTTL tracks TTL for cache entries
	schemaCacheTTL *sync.Map

	// versionCache caches schema versions by subject
	versionCache *sync.Map

	// versionCacheTTL tracks TTL for version cache entries
	versionCacheTTL *sync.Map

	// cacheMutex protects cache cleanup operations
	cacheMutex sync.Mutex

	// cleanup indicates if cleanup is running
	cleanup bool
}

// NewApicurioSchemaRegistry creates a new Apicurio Registry client
func NewApicurioSchemaRegistry(
	config SchemaRegistryConfig,
	client *http.Client,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) (*ApicurioSchemaRegistry, error) {
	// Normalize base URL
	baseURL := config.URL
	if !strings.HasSuffix(baseURL, "/") {
		baseURL = baseURL + "/"
	}

	// Create registry client
	registry := &ApicurioSchemaRegistry{
		config:          config,
		client:          client,
		baseURL:         baseURL,
		logger:          logger,
		metrics:         metricsRecorder,
		schemaCache:     &sync.Map{},
		schemaCacheTTL:  &sync.Map{},
		versionCache:    &sync.Map{},
		versionCacheTTL: &sync.Map{},
	}

	// Start cache cleanup goroutine
	go registry.startCacheCleanup()

	return registry, nil
}

// startCacheCleanup starts a background goroutine to clean up expired cache entries
func (r *ApicurioSchemaRegistry) startCacheCleanup() {
	r.cacheMutex.Lock()
	if r.cleanup {
		r.cacheMutex.Unlock()
		return
	}
	r.cleanup = true
	r.cacheMutex.Unlock()

	// Cleanup every minute
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		r.cleanupCache()
	}
}

// cleanupCache removes expired cache entries
func (r *ApicurioSchemaRegistry) cleanupCache() {
	now := time.Now()

	// Cleanup schema cache
	r.schemaCacheTTL.Range(func(key, value interface{}) bool {
		expiration, ok := value.(time.Time)
		if !ok {
			r.schemaCacheTTL.Delete(key)
			r.schemaCache.Delete(key)
			return true
		}

		if now.After(expiration) {
			r.schemaCacheTTL.Delete(key)
			r.schemaCache.Delete(key)
		}
		return true
	})

	// Cleanup version cache
	r.versionCacheTTL.Range(func(key, value interface{}) bool {
		expiration, ok := value.(time.Time)
		if !ok {
			r.versionCacheTTL.Delete(key)
			r.versionCache.Delete(key)
			return true
		}

		if now.After(expiration) {
			r.versionCacheTTL.Delete(key)
			r.versionCache.Delete(key)
		}
		return true
	})
}

// doRequest performs an HTTP request with retries
func (r *ApicurioSchemaRegistry) doRequest(req *http.Request) (*http.Response, error) {
	var resp *http.Response
	var err error

	// Add auth if provided
	if r.config.Username != "" && r.config.Password != "" {
		req.SetBasicAuth(r.config.Username, r.config.Password)
	}

	// Add headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// Start timer for metrics
	startTime := time.Now()

	// Perform request with retries
	for attempt := 0; attempt <= r.config.RetryCount; attempt++ {
		if attempt > 0 {
			// Log retry
			r.logger.Warn("Retrying schema registry request",
				"attempt", attempt,
				"url", req.URL.String())

			// Wait before retry
			time.Sleep(time.Duration(r.config.RetryIntervalMs) * time.Millisecond)
		}

		// Execute request
		resp, err = r.client.Do(req)
		if err == nil && (resp.StatusCode >= 200 && resp.StatusCode < 300) {
			break
		}

		// Log error
		if err != nil {
			r.logger.Error("Schema registry request failed",
				"error", err,
				"attempt", attempt,
				"url", req.URL.String())
			continue
		}

		// Log error response
		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		r.logger.Error("Schema registry returned error",
			"status", resp.StatusCode,
			"body", string(body),
			"attempt", attempt,
			"url", req.URL.String())

		// For some status codes, don't retry
		if resp.StatusCode == http.StatusNotFound ||
			resp.StatusCode == http.StatusUnauthorized ||
			resp.StatusCode == http.StatusForbidden {
			return resp, errors.Errorf("schema registry returned %d: %s", resp.StatusCode, string(body))
		}
	}

	// Record metrics
	duration := time.Since(startTime)
	r.metrics.TimerRecord("schema_registry_request", duration.Seconds()*1000, map[string]string{
		"url":    req.URL.String(),
		"method": req.Method,
	})

	return resp, err
}

// GetSchema retrieves a schema by ID
func (r *ApicurioSchemaRegistry) GetSchema(id string) (string, error) {
	// Check cache
	if schema, ok := r.getCachedSchema(id); ok {
		return schema, nil
	}

	// Build URL (Apicurio API differs from Confluent)
	requestURL := fmt.Sprintf("%sids/%s", r.baseURL, id)

	// Create request
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		return "", errors.Wrap(err, "failed to create request")
	}

	// Perform request
	resp, err := r.doRequest(req)
	if err != nil {
		return "", errors.Wrap(err, "failed to get schema")
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return "", errors.Errorf("failed to get schema: %d %s", resp.StatusCode, string(body))
	}

	// Read response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", errors.Wrap(err, "failed to read response body")
	}

	// Parse response (Apicurio response format differs from Confluent)
	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		return "", errors.Wrap(err, "failed to parse response")
	}

	// Extract schema
	schemaData, ok := response["schema"]
	if !ok {
		return "", errors.New("schema not found in response")
	}

	schemaStr, ok := schemaData.(string)
	if !ok {
		return "", errors.New("schema is not a string")
	}

	// Cache schema
	r.cacheSchema(id, schemaStr)

	return schemaStr, nil
}

// GetSchemaBySubject retrieves a schema by subject and version
func (r *ApicurioSchemaRegistry) GetSchemaBySubject(subject string, version string) (string, error) {
	// Check cache
	cacheKey := fmt.Sprintf("%s:%s", subject, version)
	if schema, ok := r.getCachedSchema(cacheKey); ok {
		return schema, nil
	}

	// Use "latest" if version is empty
	if version == "" {
		version = "latest"
	}

	// Build URL (Apicurio API differs from Confluent)
	var requestURL string
	if version == "latest" {
		requestURL = fmt.Sprintf("%sartifacts/%s", r.baseURL, url.PathEscape(subject))
	} else {
		requestURL = fmt.Sprintf("%sartifacts/%s/versions/%s", r.baseURL, url.PathEscape(subject), version)
	}

	// Create request
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		return "", errors.Wrap(err, "failed to create request")
	}

	// Perform request
	resp, err := r.doRequest(req)
	if err != nil {
		return "", errors.Wrap(err, "failed to get schema")
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return "", errors.Errorf("failed to get schema: %d %s", resp.StatusCode, string(body))
	}

	// Read response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", errors.Wrap(err, "failed to read response body")
	}

	// For Apicurio, the response body is the schema itself
	schema := string(body)

	// Extract ID from headers
	idStr := resp.Header.Get("X-Registry-ArtifactId")
	if idStr != "" {
		// Cache schema by ID as well
		r.cacheSchema(idStr, schema)
	}

	// Cache schema
	r.cacheSchema(cacheKey, schema)

	return schema, nil
}

// RegisterSchema registers a new schema
func (r *ApicurioSchemaRegistry) RegisterSchema(subject string, schema string) (string, error) {
	// Build URL (Apicurio API differs from Confluent)
	requestURL := fmt.Sprintf("%sartifacts", r.baseURL)

	// Create request
	req, err := http.NewRequest("POST", requestURL, strings.NewReader(schema))
	if err != nil {
		return "", errors.Wrap(err, "failed to create request")
	}

	// Add headers
	req.Header.Set("X-Registry-ArtifactId", subject)
	req.Header.Set("X-Registry-ArtifactType", "AVRO")

	// Perform request
	resp, err := r.doRequest(req)
	if err != nil {
		return "", errors.Wrap(err, "failed to register schema")
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := ioutil.ReadAll(resp.Body)
		return "", errors.Errorf("failed to register schema: %d %s", resp.StatusCode, string(body))
	}

	// Get ID from response headers
	idStr := resp.Header.Get("X-Registry-ContentId")
	if idStr == "" {
		// If ID not in header, try to extract from response body
		body, _ := ioutil.ReadAll(resp.Body)
		var response map[string]interface{}
		if err := json.Unmarshal(body, &response); err == nil {
			if id, ok := response["id"]; ok {
				if idFloat, ok := id.(float64); ok {
					idStr = strconv.FormatFloat(idFloat, 'f', 0, 64)
				}
			}
		}
	}

	// If still no ID, generate one from subject
	if idStr == "" {
		idStr = subject
	}

	// Cache schema
	r.cacheSchema(idStr, schema)
	r.cacheSchema(fmt.Sprintf("%s:latest", subject), schema)

	// Invalidate version cache for subject
	r.invalidateVersionCache(subject)

	// Record metrics
	r.metrics.CounterInc("schema_registry_schema_registered", map[string]string{
		"subject": subject,
	})

	return idStr, nil
}

// CheckCompatibility checks if a schema is compatible with the latest version
func (r *ApicurioSchemaRegistry) CheckCompatibility(subject string, schema string) (bool, error) {
	// Build URL (Apicurio API differs from Confluent)
	requestURL := fmt.Sprintf("%sartifacts/%s/rules/COMPATIBILITY", r.baseURL, url.PathEscape(subject))

	// Create request
	req, err := http.NewRequest("POST", requestURL, strings.NewReader(schema))
	if err != nil {
		return false, errors.Wrap(err, "failed to create request")
	}

	// Add headers
	req.Header.Set("Content-Type", "application/json")

	// Perform request
	resp, err := r.doRequest(req)
	if err != nil {
		return false, errors.Wrap(err, "failed to check compatibility")
	}
	defer resp.Body.Close()

	// Handle 404 (no schema registered yet)
	if resp.StatusCode == http.StatusNotFound {
		return true, nil
	}

	// Check status code
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return false, errors.Errorf("failed to check compatibility: %d %s", resp.StatusCode, string(body))
	}

	// Read response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, errors.Wrap(err, "failed to read response body")
	}

	// Parse response
	var response struct {
		Valid bool `json:"valid"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return false, errors.Wrap(err, "failed to parse response")
	}

	return response.Valid, nil
}

// GetSubjects retrieves all subjects
func (r *ApicurioSchemaRegistry) GetSubjects() ([]string, error) {
	// Build URL (Apicurio API differs from Confluent)
	requestURL := fmt.Sprintf("%sartifacts", r.baseURL)

	// Create request
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create request")
	}

	// Perform request
	resp, err := r.doRequest(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get subjects")
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return nil, errors.Errorf("failed to get subjects: %d %s", resp.StatusCode, string(body))
	}

	// Read response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response body")
	}

	// Parse response
	var subjects []string
	if err := json.Unmarshal(body, &subjects); err != nil {
		return nil, errors.Wrap(err, "failed to parse response")
	}

	return subjects, nil
}

// GetVersions retrieves all versions for a subject
func (r *ApicurioSchemaRegistry) GetVersions(subject string) ([]string, error) {
	// Check cache
	if versions, ok := r.getCachedVersions(subject); ok {
		return versions, nil
	}

	// Build URL (Apicurio API differs from Confluent)
	requestURL := fmt.Sprintf("%sartifacts/%s/versions", r.baseURL, url.PathEscape(subject))

	// Create request
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create request")
	}

	// Perform request
	resp, err := r.doRequest(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get versions")
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return nil, errors.Errorf("failed to get versions: %d %s", resp.StatusCode, string(body))
	}

	// Read response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response body")
	}

	// Parse response (Apicurio returns an array of objects)
	var versionObjects []struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(body, &versionObjects); err != nil {
		return nil, errors.Wrap(err, "failed to parse response")
	}

	// Extract version strings
	versions := make([]string, len(versionObjects))
	for i, v := range versionObjects {
		versions[i] = v.Version
	}

	// Cache versions
	r.cacheVersions(subject, versions)

	return versions, nil
}

// DeleteSchema deletes a schema version
func (r *ApicurioSchemaRegistry) DeleteSchema(subject string, version string) error {
	// Use "latest" if version is empty
	if version == "" {
		version = "latest"
	}

	// Build URL (Apicurio API differs from Confluent)
	var requestURL string
	if version == "latest" {
		requestURL = fmt.Sprintf("%sartifacts/%s", r.baseURL, url.PathEscape(subject))
	} else {
		requestURL = fmt.Sprintf("%sartifacts/%s/versions/%s", r.baseURL, url.PathEscape(subject), version)
	}

	// Create request
	req, err := http.NewRequest("DELETE", requestURL, nil)
	if err != nil {
		return errors.Wrap(err, "failed to create request")
	}

	// Perform request
	resp, err := r.doRequest(req)
	if err != nil {
		return errors.Wrap(err, "failed to delete schema")
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := ioutil.ReadAll(resp.Body)
		return errors.Errorf("failed to delete schema: %d %s", resp.StatusCode, string(body))
	}

	// Invalidate caches
	r.invalidateVersionCache(subject)
	r.invalidateSchemaCache(fmt.Sprintf("%s:%s", subject, version))
	r.invalidateSchemaCache(fmt.Sprintf("%s:latest", subject))

	// Record metrics
	r.metrics.CounterInc("schema_registry_schema_deleted", map[string]string{
		"subject": subject,
		"version": version,
	})

	return nil
}

// ExtractSchemaID extracts schema ID from Avro binary data
func (r *ApicurioSchemaRegistry) ExtractSchemaID(avroData []byte) (string, error) {
	// Check if data has the Confluent wire format
	// First byte is magic byte (0), next 4 bytes are schema ID
	if len(avroData) < 5 {
		return "", errors.New("data too short for wire format")
	}

	// Check magic byte
	if avroData[0] != 0 {
		return "", errors.New("invalid magic byte in wire format")
	}

	// Extract schema ID (4 bytes, big-endian)
	id := binary.BigEndian.Uint32(avroData[1:5])
	return strconv.FormatUint(uint64(id), 10), nil
}

// GetConfig gets the compatibility configuration for a subject
func (r *ApicurioSchemaRegistry) GetConfig(subject string) (string, error) {
	// Build URL (Apicurio API differs from Confluent)
	var requestURL string
	if subject == "" {
		requestURL = fmt.Sprintf("%sadmin/rules/COMPATIBILITY", r.baseURL)
	} else {
		requestURL = fmt.Sprintf("%sartifacts/%s/rules/COMPATIBILITY", r.baseURL, url.PathEscape(subject))
	}

	// Create request
	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		return "", errors.Wrap(err, "failed to create request")
	}

	// Perform request
	resp, err := r.doRequest(req)
	if err != nil {
		return "", errors.Wrap(err, "failed to get config")
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		// If not found, return default
		if resp.StatusCode == http.StatusNotFound {
			return string(r.config.DefaultCompatibility), nil
		}

		body, _ := ioutil.ReadAll(resp.Body)
		return "", errors.Errorf("failed to get config: %d %s", resp.StatusCode, string(body))
	}

	// Read response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", errors.Wrap(err, "failed to read response body")
	}

	// Parse response
	var response struct {
		Config string `json:"config"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return "", errors.Wrap(err, "failed to parse response")
	}

	// Map Apicurio compatibility to standard format
	switch response.Config {
	case "BACKWARD":
		return "BACKWARD", nil
	case "FORWARD":
		return "FORWARD", nil
	case "FULL":
		return "FULL", nil
	case "NONE":
		return "NONE", nil
	default:
		return response.Config, nil
	}
}

// SetConfig sets the compatibility configuration for a subject
func (r *ApicurioSchemaRegistry) SetConfig(subject string, compatibility string) error {
	// Build URL (Apicurio API differs from Confluent)
	var requestURL string
	if subject == "" {
		requestURL = fmt.Sprintf("%sadmin/rules/COMPATIBILITY", r.baseURL)
	} else {
		requestURL = fmt.Sprintf("%sartifacts/%s/rules/COMPATIBILITY", r.baseURL, url.PathEscape(subject))
	}

	// Create request body
	requestBody := struct {
		Config string `json:"config"`
	}{
		Config: compatibility,
	}
	requestBodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return errors.Wrap(err, "failed to marshal request body")
	}

	// Create request
	req, err := http.NewRequest("PUT", requestURL, bytes.NewReader(requestBodyBytes))
	if err != nil {
		return errors.Wrap(err, "failed to create request")
	}

	// Perform request
	resp, err := r.doRequest(req)
	if err != nil {
		return errors.Wrap(err, "failed to set config")
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := ioutil.ReadAll(resp.Body)
		return errors.Errorf("failed to set config: %d %s", resp.StatusCode, string(body))
	}

	// Record metrics
	labels := map[string]string{
		"compatibility": compatibility,
	}
	if subject != "" {
		labels["subject"] = subject
	}
	r.metrics.CounterInc("schema_registry_config_updated", labels)

	return nil
}

// getCachedSchema gets a schema from cache if available
func (r *ApicurioSchemaRegistry) getCachedSchema(id string) (string, bool) {
	// Check if caching is disabled
	if r.config.MaxCacheSize <= 0 {
		return "", false
	}

	// Get from cache
	value, ok := r.schemaCache.Load(id)
	if !ok {
		return "", false
	}

	// Check expiration
	ttlValue, ok := r.schemaCacheTTL.Load(id)
	if !ok {
		// TTL not found, remove from cache
		r.schemaCache.Delete(id)
		return "", false
	}

	expiration, ok := ttlValue.(time.Time)
	if !ok || time.Now().After(expiration) {
		// Expired, remove from cache
		r.schemaCache.Delete(id)
		r.schemaCacheTTL.Delete(id)
		return "", false
	}

	// Convert to string
	schema, ok := value.(string)
	if !ok {
		// Invalid type, remove from cache
		r.schemaCache.Delete(id)
		r.schemaCacheTTL.Delete(id)
		return "", false
	}

	// Record metrics
	r.metrics.CounterInc("schema_registry_cache_hit", map[string]string{
		"type": "schema",
	})

	return schema, true
}

// cacheSchema caches a schema
func (r *ApicurioSchemaRegistry) cacheSchema(id string, schema string) {
	// Check if caching is disabled
	if r.config.MaxCacheSize <= 0 {
		return
	}

	// Calculate expiration
	expiration := time.Now().Add(time.Duration(r.config.CacheTTLMs) * time.Millisecond)

	// Store in cache
	r.schemaCache.Store(id, schema)
	r.schemaCacheTTL.Store(id, expiration)
}

// getCachedVersions gets versions from cache if available
func (r *ApicurioSchemaRegistry) getCachedVersions(subject string) ([]string, bool) {
	// Check if caching is disabled
	if r.config.MaxCacheSize <= 0 {
		return nil, false
	}

	// Get from cache
	value, ok := r.versionCache.Load(subject)
	if !ok {
		return nil, false
	}

	// Check expiration
	ttlValue, ok := r.versionCacheTTL.Load(subject)
	if !ok {
		// TTL not found, remove from cache
		r.versionCache.Delete(subject)
		return nil, false
	}

	expiration, ok := ttlValue.(time.Time)
	if !ok || time.Now().After(expiration) {
		// Expired, remove from cache
		r.versionCache.Delete(subject)
		r.versionCacheTTL.Delete(subject)
		return nil, false
	}

	// Convert to string slice
	versions, ok := value.([]string)
	if !ok {
		// Invalid type, remove from cache
		r.versionCache.Delete(subject)
		r.versionCacheTTL.Delete(subject)
		return nil, false
	}

	// Record metrics
	r.metrics.CounterInc("schema_registry_cache_hit", map[string]string{
		"type": "versions",
	})

	return versions, true
}

// cacheVersions caches versions
func (r *ApicurioSchemaRegistry) cacheVersions(subject string, versions []string) {
	// Check if caching is disabled
	if r.config.MaxCacheSize <= 0 {
		return
	}

	// Calculate expiration
	expiration := time.Now().Add(time.Duration(r.config.CacheTTLMs) * time.Millisecond)

	// Store in cache
	r.versionCache.Store(subject, versions)
	r.versionCacheTTL.Store(subject, expiration)
}

// invalidateSchemaCache invalidates a schema cache entry
func (r *ApicurioSchemaRegistry) invalidateSchemaCache(id string) {
	r.schemaCache.Delete(id)
	r.schemaCacheTTL.Delete(id)
}

// invalidateVersionCache invalidates a version cache entry
func (r *ApicurioSchemaRegistry) invalidateVersionCache(subject string) {
	r.versionCache.Delete(subject)
	r.versionCacheTTL.Delete(subject)
}

// CustomSchemaRegistry implements SchemaRegistry interface for custom schema registry
type CustomSchemaRegistry struct {
	// Embed ConfluentSchemaRegistry for default implementations
	*ConfluentSchemaRegistry
}

// NewCustomSchemaRegistry creates a new custom schema registry client
func NewCustomSchemaRegistry(
	config SchemaRegistryConfig,
	client *http.Client,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) (*CustomSchemaRegistry, error) {
	// Create base registry client
	baseRegistry, err := NewConfluentSchemaRegistry(config, client, logger, metricsRecorder)
	if err != nil {
		return nil, err
	}

	// Create custom registry client
	registry := &CustomSchemaRegistry{
		ConfluentSchemaRegistry: baseRegistry,
	}

	return registry, nil
}

// RegisterMetrics registers metrics for the schema registry
func RegisterMetrics(registry metrics.MetricsRegistry) {
	// Register counters
	registry.RegisterCounter("schema_registry_schema_registered", "Number of schemas registered")
	registry.RegisterCounter("schema_registry_schema_deleted", "Number of schemas deleted")
	registry.RegisterCounter("schema_registry_config_updated", "Number of config updates")
	registry.RegisterCounter("schema_registry_cache_hit", "Number of cache hits")

	// Register timers
	registry.RegisterTimer("schema_registry_request", "Time taken for schema registry requests")
}

// SchemaRegistryFactory creates a schema registry client based on configuration
type SchemaRegistryFactory struct {
	// logger is used for logging
	logger logging.Logger
	// metrics is used for recording metrics
	metrics metrics.MetricsRecorder
}

// NewSchemaRegistryFactory creates a new schema registry factory
func NewSchemaRegistryFactory(logger logging.Logger, metricsRecorder metrics.MetricsRecorder) *SchemaRegistryFactory {
	return &SchemaRegistryFactory{
		logger:  logger,
		metrics: metricsRecorder,
	}
}

// CreateSchemaRegistry creates a schema registry client based on configuration
func (f *SchemaRegistryFactory) CreateSchemaRegistry(config config.SchemaRegistryConfig) (SchemaRegistry, error) {
	return NewSchemaRegistry(config, f.logger, f.metrics)
}

// SchemaRegistryManager manages multiple schema registry clients
type SchemaRegistryManager struct {
	// factory is used to create registry clients
	factory *SchemaRegistryFactory
	// registries stores registry clients by name
	registries map[string]SchemaRegistry
	// config stores configuration for each registry
	config map[string]config.SchemaRegistryConfig
	// mutex protects concurrent access
	mutex sync.RWMutex
}

// NewSchemaRegistryManager creates a new schema registry manager
func NewSchemaRegistryManager(factory *SchemaRegistryFactory) *SchemaRegistryManager {
	return &SchemaRegistryManager{
		factory:    factory,
		registries: make(map[string]SchemaRegistry),
		config:     make(map[string]config.SchemaRegistryConfig),
	}
}

// AddRegistry adds a registry configuration
func (m *SchemaRegistryManager) AddRegistry(name string, config config.SchemaRegistryConfig) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Store config
	m.config[name] = config

	return nil
}

// GetRegistry gets a registry client by name
func (m *SchemaRegistryManager) GetRegistry(name string) (SchemaRegistry, error) {
	m.mutex.RLock()
	registry, ok := m.registries[name]
	config, configOk := m.config[name]
	m.mutex.RUnlock()

	// If registry exists, return it
	if ok {
		return registry, nil
	}

	// If config doesn't exist, return error
	if !configOk {
		return nil, errors.Errorf("registry %s not configured", name)
	}

	// Create registry
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Check again in case another goroutine created it
	registry, ok = m.registries[name]
	if ok {
		return registry, nil
	}

	// Create registry
	registry, err := m.factory.CreateSchemaRegistry(config)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create registry %s", name)
	}

	// Store registry
	m.registries[name] = registry

	return registry, nil
}

// RemoveRegistry removes a registry client
func (m *SchemaRegistryManager) RemoveRegistry(name string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Remove registry and config
	delete(m.registries, name)
	delete(m.config, name)
}

// GetRegistryNames gets all registry names
func (m *SchemaRegistryManager) GetRegistryNames() []string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	// Get names
	names := make([]string, 0, len(m.config))
	for name := range m.config {
		names = append(names, name)
	}

	return names
}

// HighAvailabilitySchemaRegistry implements SchemaRegistry interface with high availability
type HighAvailabilitySchemaRegistry struct {
	// config is the registry configuration
	config SchemaRegistryConfig
	// registries is the list of registry clients
	registries []SchemaRegistry
	// logger is used for logging
	logger logging.Logger
	// metrics is used for recording metrics
	metrics metrics.MetricsRecorder
	// schemaCache caches schemas by ID
	schemaCache sync.Map
	// lastSuccessful tracks the last successful registry
	lastSuccessful int
	// mutex protects lastSuccessful
	mutex sync.Mutex
}

// NewHighAvailabilitySchemaRegistry creates a new high availability schema registry client
func NewHighAvailabilitySchemaRegistry(
	configs []config.SchemaRegistryConfig,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) (*HighAvailabilitySchemaRegistry, error) {
	// Need at least one registry
	if len(configs) == 0 {
		return nil, errors.New("at least one registry configuration is required")
	}

	// Create registry clients
	registries := make([]SchemaRegistry, len(configs))
	for i, cfg := range configs {
		registry, err := NewSchemaRegistry(cfg, logger, metricsRecorder)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create registry %d", i)
		}
		registries[i] = registry
	}

	// Create high availability registry
	registry := &HighAvailabilitySchemaRegistry{
		config:     SchemaRegistryConfig{}, // Use default config
		registries: registries,
		logger:     logger,
		metrics:    metricsRecorder,
	}

	return registry, nil
}

// executeWithFailover executes a function with failover to other registries
func (r *HighAvailabilitySchemaRegistry) executeWithFailover(
	fn func(registry SchemaRegistry) (interface{}, error),
) (interface{}, error) {
	// Get starting index
	r.mutex.Lock()
	startIndex := r.lastSuccessful
	r.mutex.Unlock()

	// Try each registry in order, starting with the last successful one
	var lastErr error
	for i := 0; i < len(r.registries); i++ {
		// Calculate registry index
		index := (startIndex + i) % len(r.registries)
		registry := r.registries[index]

		// Execute function
		result, err := fn(registry)
		if err == nil {
			// Update last successful
			r.mutex.Lock()
			r.lastSuccessful = index
			r.mutex.Unlock()

			return result, nil
		}

		// Log error
		r.logger.Warn("Schema registry request failed, trying next registry",
			"error", err,
			"registry", index)

		lastErr = err
	}

	// All registries failed
	return nil, errors.Wrap(lastErr, "all schema registries failed")
}

// GetSchema retrieves a schema by ID
func (r *HighAvailabilitySchemaRegistry) GetSchema(id string) (string, error) {
	// Check cache
	if cachedSchema, ok := r.schemaCache.Load(id); ok {
		return cachedSchema.(string), nil
	}

	// Execute with failover
	result, err := r.executeWithFailover(func(registry SchemaRegistry) (interface{}, error) {
		return registry.GetSchema(id)
	})
	if err != nil {
		return "", err
	}

	// Cache result
	schema := result.(string)
	r.schemaCache.Store(id, schema)

	return schema, nil
}

// GetSchemaBySubject retrieves a schema by subject and version
func (r *HighAvailabilitySchemaRegistry) GetSchemaBySubject(subject string, version string) (string, error) {
	// Check cache
	cacheKey := fmt.Sprintf("%s:%s", subject, version)
	if cachedSchema, ok := r.schemaCache.Load(cacheKey); ok {
		return cachedSchema.(string), nil
	}

	// Execute with failover
	result, err := r.executeWithFailover(func(registry SchemaRegistry) (interface{}, error) {
		return registry.GetSchemaBySubject(subject, version)
	})
	if err != nil {
		return "", err
	}

	// Cache result
	schema := result.(string)
	r.schemaCache.Store(cacheKey, schema)

	return schema, nil
}

// RegisterSchema registers a new schema
func (r *HighAvailabilitySchemaRegistry) RegisterSchema(subject string, schema string) (string, error) {
	// Execute with failover
	result, err := r.executeWithFailover(func(registry SchemaRegistry) (interface{}, error) {
		return registry.RegisterSchema(subject, schema)
	})
	if err != nil {
		return "", err
	}

	// Cache result
	id := result.(string)
	r.schemaCache.Store(id, schema)
	r.schemaCache.Store(fmt.Sprintf("%s:latest", subject), schema)

	return id, nil
}

// CheckCompatibility checks if a schema is compatible with the latest version
func (r *HighAvailabilitySchemaRegistry) CheckCompatibility(subject string, schema string) (bool, error) {
	// Execute with failover
	result, err := r.executeWithFailover(func(registry SchemaRegistry) (interface{}, error) {
		return registry.CheckCompatibility(subject, schema)
	})
	if err != nil {
		return false, err
	}

	return result.(bool), nil
}

// GetSubjects retrieves all subjects
func (r *HighAvailabilitySchemaRegistry) GetSubjects() ([]string, error) {
	// Execute with failover
	result, err := r.executeWithFailover(func(registry SchemaRegistry) (interface{}, error) {
		return registry.GetSubjects()
	})
	if err != nil {
		return nil, err
	}

	return result.([]string), nil
}

// GetVersions retrieves all versions for a subject
func (r *HighAvailabilitySchemaRegistry) GetVersions(subject string) ([]string, error) {
	// Execute with failover
	result, err := r.executeWithFailover(func(registry SchemaRegistry) (interface{}, error) {
		return registry.GetVersions(subject)
	})
	if err != nil {
		return nil, err
	}

	return result.([]string), nil
}

// DeleteSchema deletes a schema version
func (r *HighAvailabilitySchemaRegistry) DeleteSchema(subject string, version string) error {
	// Execute with failover
	_, err := r.executeWithFailover(func(registry SchemaRegistry) (interface{}, error) {
		return nil, registry.DeleteSchema(subject, version)
	})

	// Invalidate cache
	r.schemaCache.Delete(fmt.Sprintf("%s:%s", subject, version))
	r.schemaCache.Delete(fmt.Sprintf("%s:latest", subject))

	return err
}

// ExtractSchemaID extracts schema ID from Avro binary data
func (r *HighAvailabilitySchemaRegistry) ExtractSchemaID(avroData []byte) (string, error) {
	// Use first registry for extraction (no need for failover)
	return r.registries[0].ExtractSchemaID(avroData)
}

// GetConfig gets the compatibility configuration for a subject
func (r *HighAvailabilitySchemaRegistry) GetConfig(subject string) (string, error) {
	// Execute with failover
	result, err := r.executeWithFailover(func(registry SchemaRegistry) (interface{}, error) {
		return registry.GetConfig(subject)
	})
	if err != nil {
		return "", err
	}

	return result.(string), nil
}

// SetConfig sets the compatibility configuration for a subject
func (r *HighAvailabilitySchemaRegistry) SetConfig(subject string, compatibility string) error {
	// Execute with failover for each registry
	for i, registry := range r.registries {
		if err := registry.SetConfig(subject, compatibility); err != nil {
			r.logger.Warn("Failed to set config on registry",
				"error", err,
				"registry", i)
		}
	}

	return nil
}

//Personal.AI order the ending
