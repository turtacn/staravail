// Package schema provides functionality for working with schema registries and schema resolution.
package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/pkg/errors"
	"github.com/turtacn/staravail/internal/common/config"
	"github.com/turtacn/staravail/internal/common/logging"
	"github.com/turtacn/staravail/internal/common/metrics"
)

// SchemaResolver defines the interface for schema resolvers
type SchemaResolver interface {
	// ResolveSchema resolves a schema by ID
	ResolveSchema(id string) (*Schema, error)

	// ResolveSchemaBySubject resolves a schema by subject and version
	ResolveSchemaBySubject(subject string, version string) (*Schema, error)

	// ValidateData validates data against a schema
	ValidateData(schema *Schema, data []byte) error

	// ConvertData converts data from one schema to another
	ConvertData(sourceSchema *Schema, targetSchema *Schema, data []byte) ([]byte, error)

	// EncodeData encodes data using a schema
	EncodeData(schema *Schema, data map[string]interface{}) ([]byte, error)

	// DecodeData decodes data using a schema
	DecodeData(schema *Schema, data []byte) (map[string]interface{}, error)

	// GetFields gets the fields defined in a schema
	GetFields(schema *Schema) ([]Field, error)

	// GetField gets a specific field from a schema
	GetField(schema *Schema, fieldName string) (*Field, error)

	// GetSchemaInfo gets schema information
	GetSchemaInfo(schema *Schema) (*SchemaInfo, error)

	// RegisterSchema registers a new schema with the registry
	RegisterSchema(subject string, schemaStr string) (string, error)

	// IsCompatible checks if a schema is compatible with the latest version
	IsCompatible(subject string, schemaStr string) (bool, error)

	// Close closes the resolver and releases resources
	Close() error
}

// Schema represents a parsed schema
type Schema struct {
	// ID is the schema ID
	ID string

	// Subject is the schema subject
	Subject string

	// Version is the schema version
	Version string

	// Definition is the schema definition string
	Definition string

	// Type is the schema type (avro, json, protobuf)
	Type SchemaType

	// Codec is the avro codec for encoding/decoding
	Codec *goavro.Codec

	// Fields is the list of fields in the schema
	Fields []Field

	// RawSchema is the raw schema object (type depends on schema type)
	RawSchema interface{}

	// CreatedAt is when the schema was created
	CreatedAt time.Time
}

// SchemaType defines the type of schema
type SchemaType string

const (
	// SchemaTypeAvro represents an Avro schema
	SchemaTypeAvro SchemaType = "avro"

	// SchemaTypeJSON represents a JSON schema
	SchemaTypeJSON SchemaType = "json"

	// SchemaTypeProtobuf represents a Protobuf schema
	SchemaTypeProtobuf SchemaType = "protobuf"
)

// Field represents a field in a schema
type Field struct {
	// Name is the field name
	Name string

	// Type is the field type
	Type string

	// Doc is the field documentation
	Doc string

	// IsNullable indicates if the field is nullable
	IsNullable bool

	// IsRequired indicates if the field is required
	IsRequired bool

	// DefaultValue is the default value for the field
	DefaultValue interface{}

	// LogicalType is the logical type for the field
	LogicalType string

	// NestedFields is the list of nested fields (if type is record)
	NestedFields []Field

	// IsArray indicates if the field is an array
	IsArray bool

	// IsMap indicates if the field is a map
	IsMap bool

	// ElementType is the element type for arrays or maps
	ElementType string
}

// SchemaInfo provides information about a schema
type SchemaInfo struct {
	// ID is the schema ID
	ID string

	// Subject is the schema subject
	Subject string

	// Version is the schema version
	Version string

	// Type is the schema type
	Type SchemaType

	// FieldCount is the number of top-level fields
	FieldCount int

	// CompatibilityMode is the compatibility mode
	CompatibilityMode string

	// CreatedAt is when the schema was created
	CreatedAt time.Time

	// Dependencies is the list of dependencies
	Dependencies []string
}

// DefaultSchemaResolverConfig returns the default schema resolver configuration
func DefaultSchemaResolverConfig() config.SchemaResolverConfig {
	return config.SchemaResolverConfig{
		CacheSize:             1000,
		CacheTTLMs:            300000, // 5 minutes
		EnableValidation:      true,
		StrictMode:            false,
		AutoRegister:          false,
		DefaultType:           string(SchemaTypeAvro),
		EnableMetrics:         true,
		EnableSchemaEvolution: true,
	}
}

// AvroSchemaResolver implements SchemaResolver for Avro schemas
type AvroSchemaResolver struct {
	// Registry is the schema registry client
	Registry SchemaRegistry

	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder

	// Config is the resolver configuration
	Config config.SchemaResolverConfig

	// SchemaCache caches schemas by ID
	SchemaCache *sync.Map

	// SchemaCacheTTL tracks TTL for cache entries
	SchemaCacheTTL *sync.Map

	// SubjectSchemaCache caches schemas by subject+version
	SubjectSchemaCache *sync.Map

	// SubjectSchemaCacheTTL tracks TTL for subject cache entries
	SubjectSchemaCacheTTL *sync.Map

	// CodecCache caches avro codecs by schema definition
	CodecCache *sync.Map

	// CacheMutex protects cache cleanup operations
	CacheMutex sync.Mutex

	// Cleanup indicates if cleanup is running
	Cleanup bool

	// Context for cancellation
	Ctx context.Context

	// CancelFunc for stopping background tasks
	Cancel context.CancelFunc
}

// NewSchemaResolver creates a new schema resolver
func NewSchemaResolver(
	registry SchemaRegistry,
	cfg config.SchemaResolverConfig,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) (SchemaResolver, error) {
	// Use default config values if not specified
	if cfg.CacheSize <= 0 {
		cfg.CacheSize = DefaultSchemaResolverConfig().CacheSize
	}
	if cfg.CacheTTLMs <= 0 {
		cfg.CacheTTLMs = DefaultSchemaResolverConfig().CacheTTLMs
	}
	if cfg.DefaultType == "" {
		cfg.DefaultType = DefaultSchemaResolverConfig().DefaultType
	}

	// Create context for background tasks
	ctx, cancel := context.WithCancel(context.Background())

	// Create resolver
	resolver := &AvroSchemaResolver{
		Registry:              registry,
		Logger:                logger,
		Metrics:               metricsRecorder,
		Config:                cfg,
		SchemaCache:           &sync.Map{},
		SchemaCacheTTL:        &sync.Map{},
		SubjectSchemaCache:    &sync.Map{},
		SubjectSchemaCacheTTL: &sync.Map{},
		CodecCache:            &sync.Map{},
		Ctx:                   ctx,
		Cancel:                cancel,
	}

	// Start cache cleanup goroutine
	go resolver.startCacheCleanup()

	// Register metrics
	if cfg.EnableMetrics {
		RegisterResolverMetrics(metricsRecorder)
	}

	return resolver, nil
}

// startCacheCleanup starts a background goroutine to clean up expired cache entries
func (r *AvroSchemaResolver) startCacheCleanup() {
	r.CacheMutex.Lock()
	if r.Cleanup {
		r.CacheMutex.Unlock()
		return
	}
	r.Cleanup = true
	r.CacheMutex.Unlock()

	// Cleanup every minute
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.cleanupCache()
		case <-r.Ctx.Done():
			return
		}
	}
}

// cleanupCache removes expired cache entries
func (r *AvroSchemaResolver) cleanupCache() {
	now := time.Now()

	// Cleanup schema cache
	r.SchemaCacheTTL.Range(func(key, value interface{}) bool {
		expiration, ok := value.(time.Time)
		if !ok {
			r.SchemaCacheTTL.Delete(key)
			r.SchemaCache.Delete(key)
			return true
		}

		if now.After(expiration) {
			r.SchemaCacheTTL.Delete(key)
			r.SchemaCache.Delete(key)
		}
		return true
	})

	// Cleanup subject schema cache
	r.SubjectSchemaCacheTTL.Range(func(key, value interface{}) bool {
		expiration, ok := value.(time.Time)
		if !ok {
			r.SubjectSchemaCacheTTL.Delete(key)
			r.SubjectSchemaCache.Delete(key)
			return true
		}

		if now.After(expiration) {
			r.SubjectSchemaCacheTTL.Delete(key)
			r.SubjectSchemaCache.Delete(key)
		}
		return true
	})
}

// ResolveSchema resolves a schema by ID
func (r *AvroSchemaResolver) ResolveSchema(id string) (*Schema, error) {
	// Start timer for metrics
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		r.Metrics.TimerRecord("schema_resolver_resolve_schema", duration.Seconds()*1000, map[string]string{
			"method": "by_id",
		})
	}()

	// Check cache
	if schema, ok := r.getCachedSchema(id); ok {
		r.Metrics.CounterInc("schema_resolver_cache_hit", map[string]string{
			"type": "schema_by_id",
		})
		return schema, nil
	}

	// Get schema from registry
	schemaStr, err := r.Registry.GetSchema(id)
	if err != nil {
		r.Metrics.CounterInc("schema_resolver_error", map[string]string{
			"operation": "resolve_schema",
			"error":     "registry_error",
		})
		return nil, errors.Wrap(err, "failed to get schema from registry")
	}

	// Parse schema
	schema, err := r.parseSchema(id, "", "", schemaStr)
	if err != nil {
		r.Metrics.CounterInc("schema_resolver_error", map[string]string{
			"operation": "resolve_schema",
			"error":     "parse_error",
		})
		return nil, errors.Wrap(err, "failed to parse schema")
	}

	// Cache schema
	r.cacheSchema(id, schema)

	return schema, nil
}

// ResolveSchemaBySubject resolves a schema by subject and version
func (r *AvroSchemaResolver) ResolveSchemaBySubject(subject string, version string) (*Schema, error) {
	// Start timer for metrics
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		r.Metrics.TimerRecord("schema_resolver_resolve_schema", duration.Seconds()*1000, map[string]string{
			"method": "by_subject",
		})
	}()

	// Use "latest" if version is empty
	if version == "" {
		version = "latest"
	}

	// Cache key combines subject and version
	cacheKey := fmt.Sprintf("%s:%s", subject, version)

	// Check cache
	if schema, ok := r.getCachedSubjectSchema(cacheKey); ok {
		r.Metrics.CounterInc("schema_resolver_cache_hit", map[string]string{
			"type": "schema_by_subject",
		})
		return schema, nil
	}

	// Get schema from registry
	schemaStr, err := r.Registry.GetSchemaBySubject(subject, version)
	if err != nil {
		r.Metrics.CounterInc("schema_resolver_error", map[string]string{
			"operation": "resolve_schema_by_subject",
			"error":     "registry_error",
		})
		return nil, errors.Wrap(err, "failed to get schema from registry")
	}

	// Parse schema
	schema, err := r.parseSchema("", subject, version, schemaStr)
	if err != nil {
		r.Metrics.CounterInc("schema_resolver_error", map[string]string{
			"operation": "resolve_schema_by_subject",
			"error":     "parse_error",
		})
		return nil, errors.Wrap(err, "failed to parse schema")
	}

	// Cache schema
	r.cacheSubjectSchema(cacheKey, schema)
	if schema.ID != "" {
		r.cacheSchema(schema.ID, schema)
	}

	return schema, nil
}

// ValidateData validates data against a schema
func (r *AvroSchemaResolver) ValidateData(schema *Schema, data []byte) error {
	// Skip validation if disabled
	if !r.Config.EnableValidation {
		return nil
	}

	// Start timer for metrics
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		r.Metrics.TimerRecord("schema_resolver_validate_data", duration.Seconds()*1000, nil)
	}()

	// For Avro, we decode the data to validate it
	_, err := r.DecodeData(schema, data)
	if err != nil {
		r.Metrics.CounterInc("schema_resolver_validation_error", nil)
		if r.Config.StrictMode {
			return errors.Wrap(err, "data validation failed")
		}
		// Log but don't return error in non-strict mode
		r.Logger.Warn("Data validation failed but continuing in non-strict mode",
			"error", err,
			"schema_id", schema.ID,
			"subject", schema.Subject)
	}

	return nil
}

// ConvertData converts data from one schema to another
func (r *AvroSchemaResolver) ConvertData(sourceSchema *Schema, targetSchema *Schema, data []byte) ([]byte, error) {
	// Start timer for metrics
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		r.Metrics.TimerRecord("schema_resolver_convert_data", duration.Seconds()*1000, nil)
	}()

	// Decode data using source schema
	native, err := r.DecodeData(sourceSchema, data)
	if err != nil {
		r.Metrics.CounterInc("schema_resolver_error", map[string]string{
			"operation": "convert_data",
			"error":     "decode_error",
		})
		return nil, errors.Wrap(err, "failed to decode data with source schema")
	}

	// Encode data using target schema
	converted, err := r.EncodeData(targetSchema, native)
	if err != nil {
		r.Metrics.CounterInc("schema_resolver_error", map[string]string{
			"operation": "convert_data",
			"error":     "encode_error",
		})
		return nil, errors.Wrap(err, "failed to encode data with target schema")
	}

	return converted, nil
}

// EncodeData encodes data using a schema
func (r *AvroSchemaResolver) EncodeData(schema *Schema, data map[string]interface{}) ([]byte, error) {
	// Start timer for metrics
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		r.Metrics.TimerRecord("schema_resolver_encode_data", duration.Seconds()*1000, nil)
	}()

	// Check that we have a valid codec
	if schema.Codec == nil {
		r.Metrics.CounterInc("schema_resolver_error", map[string]string{
			"operation": "encode_data",
			"error":     "no_codec",
		})
		return nil, errors.New("schema has no codec")
	}

	// Encode the data
	binary, err := schema.Codec.BinaryFromNative(nil, data)
	if err != nil {
		r.Metrics.CounterInc("schema_resolver_error", map[string]string{
			"operation": "encode_data",
			"error":     "codec_error",
		})
		return nil, errors.Wrap(err, "failed to encode data")
	}

	return binary, nil
}

// DecodeData decodes data using a schema
func (r *AvroSchemaResolver) DecodeData(schema *Schema, data []byte) (map[string]interface{}, error) {
	// Start timer for metrics
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		r.Metrics.TimerRecord("schema_resolver_decode_data", duration.Seconds()*1000, nil)
	}()

	// Check that we have a valid codec
	if schema.Codec == nil {
		r.Metrics.CounterInc("schema_resolver_error", map[string]string{
			"operation": "decode_data",
			"error":     "no_codec",
		})
		return nil, errors.New("schema has no codec")
	}

	// If data starts with the Confluent wire format (magic byte + schema ID), skip those bytes
	if len(data) > 5 && data[0] == 0 {
		// Extract schema ID from the data
		extractedID, err := r.Registry.ExtractSchemaID(data)
		if err == nil {
			// If the extracted ID doesn't match the schema ID, log a warning
			if schema.ID != "" && extractedID != schema.ID {
				r.Logger.Warn("Schema ID in data doesn't match provided schema",
					"data_schema_id", extractedID,
					"provided_schema_id", schema.ID)

				// In schema evolution mode, we should resolve the schema from the data
				if r.Config.EnableSchemaEvolution {
					// Resolve the schema from the data
					dataSchema, err := r.ResolveSchema(extractedID)
					if err == nil {
						// Use the schema from the data instead
						schema = dataSchema
						r.Logger.Info("Using schema from data due to schema evolution",
							"schema_id", extractedID)
					}
				}
			}
		}

		// Skip the magic byte and schema ID (5 bytes total)
		data = data[5:]
	}

	// Decode the data
	native, remainingBytes, err := schema.Codec.NativeFromBinary(data)
	if err != nil {
		r.Metrics.CounterInc("schema_resolver_error", map[string]string{
			"operation": "decode_data",
			"error":     "codec_error",
		})
		return nil, errors.Wrap(err, "failed to decode data")
	}

	// Check if there are remaining bytes (indicates possibly corrupted data)
	if len(remainingBytes) > 0 {
		r.Logger.Warn("Decoded data has remaining bytes",
			"remaining_bytes", len(remainingBytes),
			"schema_id", schema.ID)

		if r.Config.StrictMode {
			return nil, errors.Errorf("decoded data has %d remaining bytes", len(remainingBytes))
		}
	}

	// Convert to map
	nativeMap, ok := native.(map[string]interface{})
	if !ok {
		r.Metrics.CounterInc("schema_resolver_error", map[string]string{
			"operation": "decode_data",
			"error":     "type_error",
		})
		return nil, errors.Errorf("decoded data is not a map: %T", native)
	}

	return nativeMap, nil
}

// GetFields gets the fields defined in a schema
func (r *AvroSchemaResolver) GetFields(schema *Schema) ([]Field, error) {
	// Return cached fields if available
	if schema.Fields != nil && len(schema.Fields) > 0 {
		return schema.Fields, nil
	}

	// Parse fields from schema
	fields, err := r.parseFields(schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse fields")
	}

	// Cache fields in schema
	schema.Fields = fields

	return fields, nil
}

// GetField gets a specific field from a schema
func (r *AvroSchemaResolver) GetField(schema *Schema, fieldName string) (*Field, error) {
	// Get all fields
	fields, err := r.GetFields(schema)
	if err != nil {
		return nil, err
	}

	// Find the requested field
	for _, field := range fields {
		if field.Name == fieldName {
			return &field, nil
		}
	}

	return nil, errors.Errorf("field %s not found in schema", fieldName)
}

// GetSchemaInfo gets schema information
func (r *AvroSchemaResolver) GetSchemaInfo(schema *Schema) (*SchemaInfo, error) {
	// Get fields to count them
	fields, err := r.GetFields(schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get fields")
	}

	// Get compatibility mode from registry
	compatibilityMode := ""
	if schema.Subject != "" {
		compatibilityMode, _ = r.Registry.GetConfig(schema.Subject)
	}

	// Create schema info
	info := &SchemaInfo{
		ID:                schema.ID,
		Subject:           schema.Subject,
		Version:           schema.Version,
		Type:              schema.Type,
		FieldCount:        len(fields),
		CompatibilityMode: compatibilityMode,
		CreatedAt:         schema.CreatedAt,
		Dependencies:      []string{}, // We don't track dependencies yet
	}

	return info, nil
}

// RegisterSchema registers a new schema with the registry
func (r *AvroSchemaResolver) RegisterSchema(subject string, schemaStr string) (string, error) {
	// Start timer for metrics
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		r.Metrics.TimerRecord("schema_resolver_register_schema", duration.Seconds()*1000, nil)
	}()

	// Validate the schema before registering
	_, err := goavro.NewCodec(schemaStr)
	if err != nil {
		r.Metrics.CounterInc("schema_resolver_error", map[string]string{
			"operation": "register_schema",
			"error":     "invalid_schema",
		})
		return "", errors.Wrap(err, "invalid schema")
	}

	// Register the schema
	id, err := r.Registry.RegisterSchema(subject, schemaStr)
	if err != nil {
		r.Metrics.CounterInc("schema_resolver_error", map[string]string{
			"operation": "register_schema",
			"error":     "registry_error",
		})
		return "", errors.Wrap(err, "failed to register schema")
	}

	// Parse and cache the newly registered schema
	schema, err := r.parseSchema(id, subject, "latest", schemaStr)
	if err != nil {
		r.Logger.Warn("Failed to parse newly registered schema",
			"error", err,
			"subject", subject,
			"id", id)
	} else {
		// Cache the parsed schema
		r.cacheSchema(id, schema)
		r.cacheSubjectSchema(fmt.Sprintf("%s:latest", subject), schema)
	}

	return id, nil
}

// IsCompatible checks if a schema is compatible with the latest version
func (r *AvroSchemaResolver) IsCompatible(subject string, schemaStr string) (bool, error) {
	// Start timer for metrics
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		r.Metrics.TimerRecord("schema_resolver_check_compatibility", duration.Seconds()*1000, nil)
	}()

	// Validate the schema first
	_, err := goavro.NewCodec(schemaStr)
	if err != nil {
		r.Metrics.CounterInc("schema_resolver_error", map[string]string{
			"operation": "check_compatibility",
			"error":     "invalid_schema",
		})
		return false, errors.Wrap(err, "invalid schema")
	}

	// Check compatibility with registry
	compatible, err := r.Registry.CheckCompatibility(subject, schemaStr)
	if err != nil {
		r.Metrics.CounterInc("schema_resolver_error", map[string]string{
			"operation": "check_compatibility",
			"error":     "registry_error",
		})
		return false, errors.Wrap(err, "failed to check compatibility")
	}

	return compatible, nil
}

// Close closes the resolver and releases resources
func (r *AvroSchemaResolver) Close() error {
	// Cancel background tasks
	r.Cancel()
	return nil
}

// parseSchema parses a schema definition string into a Schema object
func (r *AvroSchemaResolver) parseSchema(id string, subject string, version string, schemaStr string) (*Schema, error) {
	// Check if we have a cached codec for this schema
	var codec *goavro.Codec
	if cachedCodec, ok := r.CodecCache.Load(schemaStr); ok {
		codec = cachedCodec.(*goavro.Codec)
	} else {
		// Create a new codec
		var err error
		codec, err = goavro.NewCodec(schemaStr)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create codec")
		}
		// Cache the codec
		r.CodecCache.Store(schemaStr, codec)
	}

	// Parse the schema as JSON to get raw schema
	var rawSchema interface{}
	if err := json.Unmarshal([]byte(schemaStr), &rawSchema); err != nil {
		return nil, errors.Wrap(err, "failed to parse schema JSON")
	}

	// Create schema object
	schema := &Schema{
		ID:         id,
		Subject:    subject,
		Version:    version,
		Definition: schemaStr,
		Type:       SchemaTypeAvro,
		Codec:      codec,
		RawSchema:  rawSchema,
		CreatedAt:  time.Now(),
	}

	return schema, nil
}

// parseFields parses fields from a schema
func (r *AvroSchemaResolver) parseFields(schema *Schema) ([]Field, error) {
	// Get raw schema
	rawSchema, ok := schema.RawSchema.(map[string]interface{})
	if !ok {
		return nil, errors.New("schema is not a valid JSON object")
	}

	// Get fields array
	fieldsArray, ok := rawSchema["fields"].([]interface{})
	if !ok {
		return nil, errors.New("schema has no fields array")
	}

	// Parse each field
	fields := make([]Field, 0, len(fieldsArray))
	for _, fieldObj := range fieldsArray {
		fieldMap, ok := fieldObj.(map[string]interface{})
		if !ok {
			continue
		}

		// Extract basic field properties
		name, _ := fieldMap["name"].(string)
		doc, _ := fieldMap["doc"].(string)
		defaultValue := fieldMap["default"]

		// Extract field type (can be string or complex object)
		fieldType := ""
		isNullable := false
		logicalType := ""
		isArray := false
		isMap := false
		elementType := ""
		nestedFields := []Field{}

		// Process type information
		typeInfo := fieldMap["type"]
		switch t := typeInfo.(type) {
		case string:
			// Simple type
			fieldType = t
		case []interface{}:
			// Union type, check for nullable
			if len(t) == 2 {
				if t[0] == "null" {
					isNullable = true
					// Second type is the actual type
					if strType, ok := t[1].(string); ok {
						fieldType = strType
					} else if mapType, ok := t[1].(map[string]interface{}); ok {
						// Complex type in union
						if typeStr, ok := mapType["type"].(string); ok {
							fieldType = typeStr
							// Handle array and map types
							if fieldType == "array" {
								isArray = true
								if items, ok := mapType["items"]; ok {
									if itemsStr, ok := items.(string); ok {
										elementType = itemsStr
									}
								}
							} else if fieldType == "map" {
								isMap = true
								if values, ok := mapType["values"]; ok {
									if valuesStr, ok := values.(string); ok {
										elementType = valuesStr
									}
								}
							}
						}
					}
				} else if t[1] == "null" {
					isNullable = true
					// First type is the actual type
					if strType, ok := t[0].(string); ok {
						fieldType = strType
					} else if mapType, ok := t[0].(map[string]interface{}); ok {
						// Complex type in union
						if typeStr, ok := mapType["type"].(string); ok {
							fieldType = typeStr
							// Handle array and map types
							if fieldType == "array" {
								isArray = true
								if items, ok := mapType["items"]; ok {
									if itemsStr, ok := items.(string); ok {
										elementType = itemsStr
									}
								}
							} else if fieldType == "map" {
								isMap = true
								if values, ok := mapType["values"]; ok {
									if valuesStr, ok := values.(string); ok {
										elementType = valuesStr
									}
								}
							}
						}
					}
				}
			}
		case map[string]interface{}:
			// Complex type
			if typeStr, ok := t["type"].(string); ok {
				fieldType = typeStr

				// Check for logical type
				if lt, ok := t["logicalType"].(string); ok {
					logicalType = lt
				}

				// Handle record type with nested fields
				if fieldType == "record" {
					if nestedFieldsArray, ok := t["fields"].([]interface{}); ok {
						// Create temporary schema for parsing nested fields
						tempSchema := &Schema{
							RawSchema: t,
						}
						parsedNestedFields, err := r.parseFields(tempSchema)
						if err == nil {
							nestedFields = parsedNestedFields
						}
					}
				}

				// Handle array type
				if fieldType == "array" {
					isArray = true
					if items, ok := t["items"]; ok {
						if itemsStr, ok := items.(string); ok {
							elementType = itemsStr
						} else if itemsMap, ok := items.(map[string]interface{}); ok {
							if itemType, ok := itemsMap["type"].(string); ok {
								elementType = itemType
							}
						}
					}
				}

				// Handle map type
				if fieldType == "map" {
					isMap = true
					if values, ok := t["values"]; ok {
						if valuesStr, ok := values.(string); ok {
							elementType = valuesStr
						} else if valuesMap, ok := values.(map[string]interface{}); ok {
							if valueType, ok := valuesMap["type"].(string); ok {
								elementType = valueType
							}
						}
					}
				}
			}
		}

		// Create field
		field := Field{
			Name:         name,
			Type:         fieldType,
			Doc:          doc,
			IsNullable:   isNullable,
			IsRequired:   !isNullable && defaultValue == nil,
			DefaultValue: defaultValue,
			LogicalType:  logicalType,
			NestedFields: nestedFields,
			IsArray:      isArray,
			IsMap:        isMap,
			ElementType:  elementType,
		}

		fields = append(fields, field)
	}

	return fields, nil
}

// getCachedSchema gets a schema from cache if available
func (r *AvroSchemaResolver) getCachedSchema(id string) (*Schema, error) {
	// Check if caching is disabled
	if r.Config.CacheSize <= 0 {
		return nil, errors.New("cache not found")
	}

	// Get from cache
	value, ok := r.SchemaCache.Load(id)
	if !ok {
		return nil, errors.New("cache not found")
	}

	// Check expiration
	ttlValue, ok := r.SchemaCacheTTL.Load(id)
	if !ok {
		// TTL not found, remove from cache
		r.SchemaCache.Delete(id)
		return nil, errors.New("cache expired")
	}

	expiration, ok := ttlValue.(time.Time)
	if !ok || time.Now().After(expiration) {
		// Expired, remove from cache
		r.SchemaCache.Delete(id)
		r.SchemaCacheTTL.Delete(id)
		return nil, errors.New("cache expired")
	}

	// Convert to Schema
	schema, ok := value.(*Schema)
	if !ok {
		// Invalid type, remove from cache
		r.SchemaCache.Delete(id)
		r.SchemaCacheTTL.Delete(id)
		return nil, errors.New("invalid cache type")
	}

	return schema, nil
}

// cacheSchema caches a schema
func (r *AvroSchemaResolver) cacheSchema(id string, schema *Schema) {
	// Check if caching is disabled
	if r.Config.CacheSize <= 0 {
		return
	}

	// Calculate expiration
	expiration := time.Now().Add(time.Duration(r.Config.CacheTTLMs) * time.Millisecond)

	// Store in cache
	r.SchemaCache.Store(id, schema)
	r.SchemaCacheTTL.Store(id, expiration)
}

// getCachedSubjectSchema gets a schema by subject:version from cache if available
func (r *AvroSchemaResolver) getCachedSubjectSchema(key string) (*Schema, error) {
	// Check if caching is disabled
	if r.Config.CacheSize <= 0 {
		return nil, errors.New("cache not found")
	}

	// Get from cache
	value, ok := r.SubjectSchemaCache.Load(key)
	if !ok {
		return nil, errors.New("cache not found")
	}

	// Check expiration
	ttlValue, ok := r.SubjectSchemaCacheTTL.Load(key)
	if !ok {
		// TTL not found, remove from cache
		r.SubjectSchemaCache.Delete(key)
		return nil, errors.New("cache expired")
	}

	expiration, ok := ttlValue.(time.Time)
	if !ok || time.Now().After(expiration) {
		// Expired, remove from cache
		r.SubjectSchemaCache.Delete(key)
		r.SubjectSchemaCacheTTL.Delete(key)
		return nil, errors.New("cache expired")
	}

	// Convert to Schema
	schema, ok := value.(*Schema)
	if !ok {
		// Invalid type, remove from cache
		r.SubjectSchemaCache.Delete(key)
		r.SubjectSchemaCacheTTL.Delete(key)
		return nil, errors.New("invalid cache type")
	}

	return schema, nil
}

// cacheSubjectSchema caches a schema by subject:version
func (r *AvroSchemaResolver) cacheSubjectSchema(key string, schema *Schema) {
	// Check if caching is disabled
	if r.Config.CacheSize <= 0 {
		return
	}

	// Calculate expiration
	expiration := time.Now().Add(time.Duration(r.Config.CacheTTLMs) * time.Millisecond)

	// Store in cache
	r.SubjectSchemaCache.Store(key, schema)
	r.SubjectSchemaCacheTTL.Store(key, expiration)
}

// RegisterResolverMetrics registers metrics for the schema resolver
func RegisterResolverMetrics(registry metrics.MetricsRecorder) {
	// Register counters
	registry.RegisterCounter("schema_resolver_cache_hit", "Number of schema resolver cache hits")
	registry.RegisterCounter("schema_resolver_error", "Number of schema resolver errors")
	registry.RegisterCounter("schema_resolver_validation_error", "Number of schema validation errors")

	// Register timers
	registry.RegisterTimer("schema_resolver_resolve_schema", "Time taken to resolve schemas")
	registry.RegisterTimer("schema_resolver_validate_data", "Time taken to validate data")
	registry.RegisterTimer("schema_resolver_convert_data", "Time taken to convert data")
	registry.RegisterTimer("schema_resolver_encode_data", "Time taken to encode data")
	registry.RegisterTimer("schema_resolver_decode_data", "Time taken to decode data")
	registry.RegisterTimer("schema_resolver_register_schema", "Time taken to register schemas")
	registry.RegisterTimer("schema_resolver_check_compatibility", "Time taken to check compatibility")
}

// SchemaResolverFactory creates schema resolvers
type SchemaResolverFactory struct {
	// RegistryManager manages schema registries
	RegistryManager *SchemaRegistryManager
	// Logger for logging
	Logger logging.Logger
	// Metrics for recording metrics
	Metrics metrics.MetricsRecorder
	// Config is the default resolver configuration
	Config config.SchemaResolverConfig
}

// NewSchemaResolverFactory creates a new schema resolver factory
func NewSchemaResolverFactory(
	registryManager *SchemaRegistryManager,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
	cfg config.SchemaResolverConfig,
) *SchemaResolverFactory {
	// Use default config values if needed
	if cfg.CacheSize <= 0 {
		cfg.CacheSize = DefaultSchemaResolverConfig().CacheSize
	}
	if cfg.CacheTTLMs <= 0 {
		cfg.CacheTTLMs = DefaultSchemaResolverConfig().CacheTTLMs
	}
	if cfg.DefaultType == "" {
		cfg.DefaultType = DefaultSchemaResolverConfig().DefaultType
	}

	return &SchemaResolverFactory{
		RegistryManager: registryManager,
		Logger:          logger,
		Metrics:         metricsRecorder,
		Config:          cfg,
	}
}

// CreateResolver creates a schema resolver for a registry
func (f *SchemaResolverFactory) CreateResolver(registryName string) (SchemaResolver, error) {
	// Get registry
	registry, err := f.RegistryManager.GetRegistry(registryName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get registry %s", registryName)
	}

	// Create resolver
	resolver, err := NewSchemaResolver(registry, f.Config, f.Logger, f.Metrics)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create resolver")
	}

	return resolver, nil
}

// JSONSchemaResolver implements SchemaResolver for JSON schemas
type JSONSchemaResolver struct {
	// Registry is the schema registry client
	Registry SchemaRegistry

	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder

	// Config is the resolver configuration
	Config config.SchemaResolverConfig

	// SchemaCache caches schemas by ID
	SchemaCache *sync.Map

	// SchemaCacheTTL tracks TTL for cache entries
	SchemaCacheTTL *sync.Map

	// SubjectSchemaCache caches schemas by subject+version
	SubjectSchemaCache *sync.Map

	// SubjectSchemaCacheTTL tracks TTL for subject cache entries
	SubjectSchemaCacheTTL *sync.Map

	// CacheMutex protects cache cleanup operations
	CacheMutex sync.Mutex

	// Cleanup indicates if cleanup is running
	Cleanup bool

	// Context for cancellation
	Ctx context.Context

	// CancelFunc for stopping background tasks
	Cancel context.CancelFunc
}

// NewJSONSchemaResolver creates a new JSON schema resolver
func NewJSONSchemaResolver(
	registry SchemaRegistry,
	cfg config.SchemaResolverConfig,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) (*JSONSchemaResolver, error) {
	// Use default config values if not specified
	if cfg.CacheSize <= 0 {
		cfg.CacheSize = DefaultSchemaResolverConfig().CacheSize
	}
	if cfg.CacheTTLMs <= 0 {
		cfg.CacheTTLMs = DefaultSchemaResolverConfig().CacheTTLMs
	}

	// Create context for background tasks
	ctx, cancel := context.WithCancel(context.Background())

	// Create resolver
	resolver := &JSONSchemaResolver{
		Registry:              registry,
		Logger:                logger,
		Metrics:               metricsRecorder,
		Config:                cfg,
		SchemaCache:           &sync.Map{},
		SchemaCacheTTL:        &sync.Map{},
		SubjectSchemaCache:    &sync.Map{},
		SubjectSchemaCacheTTL: &sync.Map{},
		Ctx:                   ctx,
		Cancel:                cancel,
	}

	// Start cache cleanup goroutine
	go resolver.startCacheCleanup()

	return resolver, nil
}

// ResolveSchema resolves a schema by ID
func (r *JSONSchemaResolver) ResolveSchema(id string) (*Schema, error) {
	// Implementation for JSON Schema
	// This is a placeholder - would need to be implemented with a JSON Schema validator
	return nil, errors.New("JSON Schema resolver not fully implemented")
}

// ResolveSchemaBySubject resolves a schema by subject and version
func (r *JSONSchemaResolver) ResolveSchemaBySubject(subject string, version string) (*Schema, error) {
	// Implementation for JSON Schema
	// This is a placeholder - would need to be implemented with a JSON Schema validator
	return nil, errors.New("JSON Schema resolver not fully implemented")
}

// ValidateData validates data against a schema
func (r *JSONSchemaResolver) ValidateData(schema *Schema, data []byte) error {
	// Implementation for JSON Schema
	// This is a placeholder - would need to be implemented with a JSON Schema validator
	return errors.New("JSON Schema resolver not fully implemented")
}

// ConvertData converts data from one schema to another
func (r *JSONSchemaResolver) ConvertData(sourceSchema *Schema, targetSchema *Schema, data []byte) ([]byte, error) {
	// Implementation for JSON Schema
	// This is a placeholder - would need to be implemented with a JSON Schema validator
	return nil, errors.New("JSON Schema resolver not fully implemented")
}

// EncodeData encodes data using a schema
func (r *JSONSchemaResolver) EncodeData(schema *Schema, data map[string]interface{}) ([]byte, error) {
	// Implementation for JSON Schema
	// This is a placeholder - would need to be implemented with a JSON Schema validator
	return nil, errors.New("JSON Schema resolver not fully implemented")
}

// DecodeData decodes data using a schema
func (r *JSONSchemaResolver) DecodeData(schema *Schema, data []byte) (map[string]interface{}, error) {
	// Implementation for JSON Schema
	// This is a placeholder - would need to be implemented with a JSON Schema validator
	return nil, errors.New("JSON Schema resolver not fully implemented")
}

// GetFields gets the fields defined in a schema
func (r *JSONSchemaResolver) GetFields(schema *Schema) ([]Field, error) {
	// Implementation for JSON Schema
	// This is a placeholder - would need to be implemented with a JSON Schema validator
	return nil, errors.New("JSON Schema resolver not fully implemented")
}

// GetField gets a specific field from a schema
func (r *JSONSchemaResolver) GetField(schema *Schema, fieldName string) (*Field, error) {
	// Implementation for JSON Schema
	// This is a placeholder - would need to be implemented with a JSON Schema validator
	return nil, errors.New("JSON Schema resolver not fully implemented")
}

// GetSchemaInfo gets schema information
func (r *JSONSchemaResolver) GetSchemaInfo(schema *Schema) (*SchemaInfo, error) {
	// Implementation for JSON Schema
	// This is a placeholder - would need to be implemented with a JSON Schema validator
	return nil, errors.New("JSON Schema resolver not fully implemented")
}

// RegisterSchema registers a new schema with the registry
func (r *JSONSchemaResolver) RegisterSchema(subject string, schemaStr string) (string, error) {
	// Implementation for JSON Schema
	// This is a placeholder - would need to be implemented with a JSON Schema validator
	return "", errors.New("JSON Schema resolver not fully implemented")
}

// IsCompatible checks if a schema is compatible with the latest version
func (r *JSONSchemaResolver) IsCompatible(subject string, schemaStr string) (bool, error) {
	// Implementation for JSON Schema
	// This is a placeholder - would need to be implemented with a JSON Schema validator
	return false, errors.New("JSON Schema resolver not fully implemented")
}

// Close closes the resolver and releases resources
func (r *JSONSchemaResolver) Close() error {
	// Cancel background tasks
	r.Cancel()
	return nil
}

// startCacheCleanup starts a background goroutine to clean up expired cache entries
func (r *JSONSchemaResolver) startCacheCleanup() {
	r.CacheMutex.Lock()
	if r.Cleanup {
		r.CacheMutex.Unlock()
		return
	}
	r.Cleanup = true
	r.CacheMutex.Unlock()

	// Cleanup every minute
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.cleanupCache()
		case <-r.Ctx.Done():
			return
		}
	}
}

// cleanupCache removes expired cache entries
func (r *JSONSchemaResolver) cleanupCache() {
	now := time.Now()

	// Cleanup schema cache
	r.SchemaCacheTTL.Range(func(key, value interface{}) bool {
		expiration, ok := value.(time.Time)
		if !ok {
			r.SchemaCacheTTL.Delete(key)
			r.SchemaCache.Delete(key)
			return true
		}

		if now.After(expiration) {
			r.SchemaCacheTTL.Delete(key)
			r.SchemaCache.Delete(key)
		}
		return true
	})

	// Cleanup subject schema cache
	r.SubjectSchemaCacheTTL.Range(func(key, value interface{}) bool {
		expiration, ok := value.(time.Time)
		if !ok {
			r.SubjectSchemaCacheTTL.Delete(key)
			r.SubjectSchemaCache.Delete(key)
			return true
		}

		if now.After(expiration) {
			r.SubjectSchemaCacheTTL.Delete(key)
			r.SubjectSchemaCache.Delete(key)
		}
		return true
	})
}

// SchemaResolverManager manages multiple schema resolvers
type SchemaResolverManager struct {
	// Factory is used to create resolvers
	Factory *SchemaResolverFactory
	// Resolvers stores resolver clients by name
	Resolvers map[string]SchemaResolver
	// Mutex protects concurrent access
	Mutex sync.RWMutex
}

// NewSchemaResolverManager creates a new schema resolver manager
func NewSchemaResolverManager(factory *SchemaResolverFactory) *SchemaResolverManager {
	return &SchemaResolverManager{
		Factory:   factory,
		Resolvers: make(map[string]SchemaResolver),
	}
}

// GetResolver gets a resolver by registry name
func (m *SchemaResolverManager) GetResolver(registryName string) (SchemaResolver, error) {
	m.Mutex.RLock()
	resolver, ok := m.Resolvers[registryName]
	m.Mutex.RUnlock()

	// If resolver exists, return it
	if ok {
		return resolver, nil
	}

	// Create resolver
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	// Check again in case another goroutine created it
	resolver, ok = m.Resolvers[registryName]
	if ok {
		return resolver, nil
	}

	// Create resolver
	resolver, err := m.Factory.CreateResolver(registryName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create resolver for registry %s", registryName)
	}

	// Store resolver
	m.Resolvers[registryName] = resolver

	return resolver, nil
}

// RemoveResolver removes a resolver
func (m *SchemaResolverManager) RemoveResolver(registryName string) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	// Get resolver
	resolver, ok := m.Resolvers[registryName]
	if !ok {
		return
	}

	// Close resolver
	_ = resolver.Close()

	// Remove resolver
	delete(m.Resolvers, registryName)
}

// Close closes all resolvers
func (m *SchemaResolverManager) Close() error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	// Close all resolvers
	for name, resolver := range m.Resolvers {
		if err := resolver.Close(); err != nil {
			m.Factory.Logger.Error("Failed to close resolver",
				"error", err,
				"registry", name)
		}
	}

	// Clear resolvers
	m.Resolvers = make(map[string]SchemaResolver)

	return nil
}

//Personal.AI order the ending
