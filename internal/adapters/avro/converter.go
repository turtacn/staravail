// Package avro provides functionality for working with Apache Avro data formats.
package avro

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"

	"github.com/linkedin/goavro/v2"
	"github.com/pkg/errors"
	"github.com/turtacn/staravail/internal/common/config"
	"github.com/turtacn/staravail/internal/common/logging"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/common/schema"
)

// FormatType represents the type of Avro format
type FormatType string

const (
	// FormatOCF represents Object Container File format
	FormatOCF FormatType = "OCF"
	// FormatBinary represents Avro Binary format
	FormatBinary FormatType = "Binary"
	// FormatJSON represents JSON format with Avro schema
	FormatJSON FormatType = "JSON"
	// FormatUnknown represents an unknown format
	FormatUnknown FormatType = "Unknown"
)

// AvroConverter defines the interface for converting between Avro formats
type AvroConverter interface {
	// ConvertOCFToBinary converts OCF format data to Binary format records
	ConvertOCFToBinary(ocfData []byte) ([][]byte, error)

	// ConvertOCFToJSON converts OCF format data to JSON format
	ConvertOCFToJSON(ocfData []byte) ([][]byte, error)

	// ConvertBinaryToJSON converts Binary format data to JSON format
	ConvertBinaryToJSON(binaryData []byte, schemaID string) ([]byte, error)

	// ConvertJSONToBinary converts JSON format data to Binary format
	ConvertJSONToBinary(jsonData []byte, schemaID string) ([]byte, error)

	// ConvertStreamOCFToBinary converts OCF format from a reader to Binary format
	ConvertStreamOCFToBinary(reader io.Reader, writer io.Writer) error

	// ConvertStreamBinaryToJSON converts Binary format from a reader to JSON format
	ConvertStreamBinaryToJSON(reader io.Reader, writer io.Writer, schemaID string) error

	// ConvertStreamJSONToBinary converts JSON format from a reader to Binary format
	ConvertStreamJSONToBinary(reader io.Reader, writer io.Writer, schemaID string) error

	// DetectFormat detects the format of the given data
	DetectFormat(data []byte) FormatType

	// ExtractSchemaFromData extracts schema from the data if possible
	ExtractSchemaFromData(data []byte, format FormatType) (string, error)
}

// AvroConverterConfig contains configuration for Avro converter
type AvroConverterConfig struct {
	// BatchSize is the number of records to process in batch
	BatchSize int
	// EnableSchemaCache enables caching of schemas
	EnableSchemaCache bool
	// MaxSchemaCacheSize is the maximum number of schemas to cache
	MaxSchemaCacheSize int
	// BufferSize is the size of the buffer for stream operations
	BufferSize int
}

// DefaultAvroConverterConfig returns the default configuration for Avro converter
func DefaultAvroConverterConfig() AvroConverterConfig {
	return AvroConverterConfig{
		BatchSize:          1000,
		EnableSchemaCache:  true,
		MaxSchemaCacheSize: 100,
		BufferSize:         64 * 1024, // 64KB
	}
}

// AvroConverterImpl implements the AvroConverter interface
type AvroConverterImpl struct {
	// config is the converter configuration
	config AvroConverterConfig
	// schemaResolver is used to resolve schemas
	schemaResolver schema.SchemaResolver
	// logger is used for logging
	logger logging.Logger
	// metrics is used for recording metrics
	metrics metrics.MetricsRecorder
	// schemaCache caches schemas by ID
	schemaCache map[string]string
	// schemaCacheMutex protects the schema cache
	schemaCacheMutex sync.RWMutex
	// codecCache caches goavro codecs by schema
	codecCache map[string]*goavro.Codec
	// codecCacheMutex protects the codec cache
	codecCacheMutex sync.RWMutex
}

// NewAvroConverter creates a new Avro converter
func NewAvroConverter(
	cfg config.AvroConverterConfig,
	schemaResolver schema.SchemaResolver,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) (AvroConverter, error) {
	// Initialize configuration
	config := AvroConverterConfig{
		BatchSize:          cfg.BatchSize,
		EnableSchemaCache:  cfg.EnableSchemaCache,
		MaxSchemaCacheSize: cfg.MaxSchemaCacheSize,
		BufferSize:         cfg.BufferSize,
	}

	// Use default values if not specified
	if config.BatchSize <= 0 {
		config.BatchSize = DefaultAvroConverterConfig().BatchSize
	}
	if config.MaxSchemaCacheSize <= 0 {
		config.MaxSchemaCacheSize = DefaultAvroConverterConfig().MaxSchemaCacheSize
	}
	if config.BufferSize <= 0 {
		config.BufferSize = DefaultAvroConverterConfig().BufferSize
	}

	// Create converter
	converter := &AvroConverterImpl{
		config:         config,
		schemaResolver: schemaResolver,
		logger:         logger,
		metrics:        metricsRecorder,
		schemaCache:    make(map[string]string),
		codecCache:     make(map[string]*goavro.Codec),
	}

	return converter, nil
}

// ConvertOCFToBinary converts OCF format data to Binary format records
func (c *AvroConverterImpl) ConvertOCFToBinary(ocfData []byte) ([][]byte, error) {
	// Create reader for OCF data
	ocfReader, err := c.createOCFReader(bytes.NewReader(ocfData))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create OCF reader")
	}
	defer ocfReader.Close()

	// Read header
	header, err := ocfReader.ReadHeader()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read OCF header")
	}

	// Create codec
	codec, err := c.getCodec(header.Schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	// Buffer for results
	result := make([][]byte, 0)

	// Read blocks
	for {
		block, err := ocfReader.ReadBlockObjects()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, errors.Wrap(err, "failed to read OCF block")
		}

		// Convert each object to binary
		for _, obj := range block.Objects {
			binary, err := codec.BinaryFromNative(nil, obj)
			if err != nil {
				return nil, errors.Wrap(err, "failed to convert object to binary")
			}
			result = append(result, binary)
		}
	}

	// Record metrics
	c.metrics.CounterAdd("avro_ocf_to_binary_records", float64(len(result)), nil)

	return result, nil
}

// ConvertOCFToJSON converts OCF format data to JSON format
func (c *AvroConverterImpl) ConvertOCFToJSON(ocfData []byte) ([][]byte, error) {
	// Create reader for OCF data
	ocfReader, err := c.createOCFReader(bytes.NewReader(ocfData))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create OCF reader")
	}
	defer ocfReader.Close()

	// Read header
	header, err := ocfReader.ReadHeader()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read OCF header")
	}

	// Create codec
	codec, err := c.getCodec(header.Schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	// Buffer for results
	result := make([][]byte, 0)

	// Read blocks
	for {
		block, err := ocfReader.ReadBlockObjects()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, errors.Wrap(err, "failed to read OCF block")
		}

		// Convert each object to JSON
		for _, obj := range block.Objects {
			json, err := codec.TextualFromNative(nil, obj)
			if err != nil {
				return nil, errors.Wrap(err, "failed to convert object to JSON")
			}
			result = append(result, json)
		}
	}

	// Record metrics
	c.metrics.CounterAdd("avro_ocf_to_json_records", float64(len(result)), nil)

	return result, nil
}

// ConvertBinaryToJSON converts Binary format data to JSON format
func (c *AvroConverterImpl) ConvertBinaryToJSON(binaryData []byte, schemaID string) ([]byte, error) {
	// Get schema
	schema, err := c.getSchema(schemaID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get schema")
	}

	// Create codec
	codec, err := c.getCodec(schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	// Decode binary
	native, _, err := codec.NativeFromBinary(bytes.NewReader(binaryData))
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode binary")
	}

	// Convert to JSON
	json, err := codec.TextualFromNative(nil, native)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert to JSON")
	}

	// Record metrics
	c.metrics.CounterInc("avro_binary_to_json_records", nil)

	return json, nil
}

// ConvertJSONToBinary converts JSON format data to Binary format
func (c *AvroConverterImpl) ConvertJSONToBinary(jsonData []byte, schemaID string) ([]byte, error) {
	// Get schema
	schema, err := c.getSchema(schemaID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get schema")
	}

	// Create codec
	codec, err := c.getCodec(schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	// Decode JSON
	native, _, err := codec.NativeFromTextual(jsonData)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode JSON")
	}

	// Convert to binary
	binary, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert to binary")
	}

	// Record metrics
	c.metrics.CounterInc("avro_json_to_binary_records", nil)

	return binary, nil
}

// ConvertStreamOCFToBinary converts OCF format from a reader to Binary format
func (c *AvroConverterImpl) ConvertStreamOCFToBinary(reader io.Reader, writer io.Writer) error {
	// Create reader for OCF data
	ocfReader, err := c.createOCFReader(reader)
	if err != nil {
		return errors.Wrap(err, "failed to create OCF reader")
	}
	defer ocfReader.Close()

	// Read header
	header, err := ocfReader.ReadHeader()
	if err != nil {
		return errors.Wrap(err, "failed to read OCF header")
	}

	// Create codec
	codec, err := c.getCodec(header.Schema)
	if err != nil {
		return errors.Wrap(err, "failed to create codec")
	}

	// Counter for metrics
	recordCount := 0

	// Read blocks
	for {
		block, err := ocfReader.ReadBlockObjects()
		if err != nil {
			if err == io.EOF {
				break
			}
			return errors.Wrap(err, "failed to read OCF block")
		}

		// Convert each object to binary
		for _, obj := range block.Objects {
			binary, err := codec.BinaryFromNative(nil, obj)
			if err != nil {
				return errors.Wrap(err, "failed to convert object to binary")
			}

			// Write binary to output
			if _, err := writer.Write(binary); err != nil {
				return errors.Wrap(err, "failed to write binary")
			}

			recordCount++
		}
	}

	// Record metrics
	c.metrics.CounterAdd("avro_ocf_to_binary_records", float64(recordCount), nil)

	return nil
}

// ConvertStreamBinaryToJSON converts Binary format from a reader to JSON format
func (c *AvroConverterImpl) ConvertStreamBinaryToJSON(reader io.Reader, writer io.Writer, schemaID string) error {
	// Get schema
	schema, err := c.getSchema(schemaID)
	if err != nil {
		return errors.Wrap(err, "failed to get schema")
	}

	// Create binary reader
	binaryReader, err := c.createBinaryReader(reader, schema)
	if err != nil {
		return errors.Wrap(err, "failed to create binary reader")
	}
	defer binaryReader.Close()

	// Create codec
	codec, err := c.getCodec(schema)
	if err != nil {
		return errors.Wrap(err, "failed to create codec")
	}

	// Counter for metrics
	recordCount := 0

	// Read records in batches
	for {
		batch, err := binaryReader.ReadBatch(c.config.BatchSize)
		if err != nil {
			return errors.Wrap(err, "failed to read binary batch")
		}

		// If batch is empty, we're done
		if len(batch) == 0 {
			break
		}

		// Convert each object to JSON
		for _, obj := range batch {
			json, err := codec.TextualFromNative(nil, obj)
			if err != nil {
				return errors.Wrap(err, "failed to convert to JSON")
			}

			// Write JSON to output
			if _, err := writer.Write(json); err != nil {
				return errors.Wrap(err, "failed to write JSON")
			}

			// Write newline
			if _, err := writer.Write([]byte("\n")); err != nil {
				return errors.Wrap(err, "failed to write newline")
			}

			recordCount++
		}
	}

	// Record metrics
	c.metrics.CounterAdd("avro_binary_to_json_records", float64(recordCount), nil)

	return nil
}

// ConvertStreamJSONToBinary converts JSON format from a reader to Binary format
func (c *AvroConverterImpl) ConvertStreamJSONToBinary(reader io.Reader, writer io.Writer, schemaID string) error {
	// Get schema
	schema, err := c.getSchema(schemaID)
	if err != nil {
		return errors.Wrap(err, "failed to get schema")
	}

	// Create codec
	codec, err := c.getCodec(schema)
	if err != nil {
		return errors.Wrap(err, "failed to create codec")
	}

	// Create scanner to read JSON lines
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, c.config.BufferSize), c.config.BufferSize)

	// Counter for metrics
	recordCount := 0

	// Read JSON lines
	for scanner.Scan() {
		line := scanner.Bytes()

		// Skip empty lines
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		// Decode JSON
		native, _, err := codec.NativeFromTextual(line)
		if err != nil {
			return errors.Wrap(err, "failed to decode JSON")
		}

		// Convert to binary
		binary, err := codec.BinaryFromNative(nil, native)
		if err != nil {
			return errors.Wrap(err, "failed to convert to binary")
		}

		// Write binary to output
		if _, err := writer.Write(binary); err != nil {
			return errors.Wrap(err, "failed to write binary")
		}

		recordCount++
	}

	// Check for scanner errors
	if err := scanner.Err(); err != nil {
		return errors.Wrap(err, "scanner error")
	}

	// Record metrics
	c.metrics.CounterAdd("avro_json_to_binary_records", float64(recordCount), nil)

	return nil
}

// DetectFormat detects the format of the given data
func (c *AvroConverterImpl) DetectFormat(data []byte) FormatType {
	// Check if it's OCF
	if len(data) >= 4 && string(data[:4]) == "Obj\x01" {
		return FormatOCF
	}

	// Check if it's JSON
	if len(data) > 0 && (data[0] == '{' || data[0] == '[') {
		var js interface{}
		if err := json.Unmarshal(data, &js); err == nil {
			return FormatJSON
		}
	}

	// Try to decode as Avro binary with a simple schema
	// This is a heuristic since binary doesn't have a clear signature
	simpleSchema := `{"type":"record","name":"Test","fields":[{"name":"f1","type":"string"}]}`
	codec, err := goavro.NewCodec(simpleSchema)
	if err == nil {
		_, _, err := codec.NativeFromBinary(bytes.NewReader(data))
		if err == nil || strings.Contains(err.Error(), "cannot decode binary") {
			// If we can decode or get a specific Avro error, it's likely binary
			return FormatBinary
		}
	}

	// Unknown format
	return FormatUnknown
}

// ExtractSchemaFromData extracts schema from the data if possible
func (c *AvroConverterImpl) ExtractSchemaFromData(data []byte, format FormatType) (string, error) {
	switch format {
	case FormatOCF:
		// Extract schema from OCF
		schema, err := ExtractSchemaFromOCF(data)
		if err != nil {
			return "", errors.Wrap(err, "failed to extract schema from OCF")
		}
		return schema, nil

	case FormatJSON:
		// For JSON, we can't determine the schema without additional information
		return "", errors.New("cannot extract schema from JSON without schema reference")

	case FormatBinary:
		// For binary, we can't determine the schema without additional information
		return "", errors.New("cannot extract schema from binary without schema reference")

	default:
		return "", errors.New("unknown format")
	}
}

// getSchema gets schema by ID, with caching if enabled
func (c *AvroConverterImpl) getSchema(schemaID string) (string, error) {
	// Check cache first if enabled
	if c.config.EnableSchemaCache {
		c.schemaCacheMutex.RLock()
		schema, ok := c.schemaCache[schemaID]
		c.schemaCacheMutex.RUnlock()

		if ok {
			return schema, nil
		}
	}

	// Get schema from resolver
	schema, err := c.schemaResolver.GetSchema(schemaID)
	if err != nil {
		return "", errors.Wrap(err, "failed to get schema from resolver")
	}

	// Cache schema if enabled
	if c.config.EnableSchemaCache {
		c.schemaCacheMutex.Lock()

		// Check if cache is full
		if len(c.schemaCache) >= c.config.MaxSchemaCacheSize {
			// Simple eviction: clear the entire cache
			c.schemaCache = make(map[string]string)
		}

		c.schemaCache[schemaID] = schema
		c.schemaCacheMutex.Unlock()
	}

	return schema, nil
}

// getCodec gets or creates a codec for the given schema, with caching
func (c *AvroConverterImpl) getCodec(schema string) (*goavro.Codec, error) {
	// Check cache
	c.codecCacheMutex.RLock()
	codec, ok := c.codecCache[schema]
	c.codecCacheMutex.RUnlock()

	if ok {
		return codec, nil
	}

	// Create codec
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	// Cache codec
	c.codecCacheMutex.Lock()
	c.codecCache[schema] = codec
	c.codecCacheMutex.Unlock()

	return codec, nil
}

// createOCFReader creates an OCF reader with the current configuration
func (c *AvroConverterImpl) createOCFReader(reader io.Reader) (OCFReader, error) {
	// Create OCF reader config
	ocfConfig := config.OCFReaderConfig{
		BufferSize:   c.config.BufferSize,
		MaxBlockSize: 1024 * 1024 * 1024, // 1GB max block size
	}

	// Create reader
	return NewOCFReader(reader, ocfConfig, c.logger)
}

// createBinaryReader creates a Binary reader with the current configuration
func (c *AvroConverterImpl) createBinaryReader(reader io.Reader, schema string) (BinaryReader, error) {
	// Create Binary reader config
	binaryConfig := config.BinaryReaderConfig{
		BufferSize:             c.config.BufferSize,
		EnableSchemaResolution: true,
		EnableReuse:            true,
	}

	// Create reader
	binaryReader, err := NewBinaryReader(reader, binaryConfig, c.logger, c.metrics)
	if err != nil {
		return nil, err
	}

	// Set schema
	if err := binaryReader.SetSchema(schema); err != nil {
		return nil, errors.Wrap(err, "failed to set schema")
	}

	return binaryReader, nil
}

// BatchConvertJSONRecordsToBinary converts multiple JSON records to Binary format
func (c *AvroConverterImpl) BatchConvertJSONRecordsToBinary(jsonRecords [][]byte, schemaID string) ([][]byte, error) {
	// Get schema
	schema, err := c.getSchema(schemaID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get schema")
	}

	// Create codec
	codec, err := c.getCodec(schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	// Convert each record
	result := make([][]byte, 0, len(jsonRecords))
	for _, jsonRecord := range jsonRecords {
		// Skip empty records
		if len(bytes.TrimSpace(jsonRecord)) == 0 {
			continue
		}

		// Decode JSON
		native, _, err := codec.NativeFromTextual(jsonRecord)
		if err != nil {
			return nil, errors.Wrap(err, "failed to decode JSON")
		}

		// Convert to binary
		binary, err := codec.BinaryFromNative(nil, native)
		if err != nil {
			return nil, errors.Wrap(err, "failed to convert to binary")
		}

		result = append(result, binary)
	}

	// Record metrics
	c.metrics.CounterAdd("avro_json_to_binary_records", float64(len(result)), nil)

	return result, nil
}

// BatchConvertBinaryRecordsToJSON converts multiple Binary records to JSON format
func (c *AvroConverterImpl) BatchConvertBinaryRecordsToJSON(binaryRecords [][]byte, schemaID string) ([][]byte, error) {
	// Get schema
	schema, err := c.getSchema(schemaID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get schema")
	}

	// Create codec
	codec, err := c.getCodec(schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	// Convert each record
	result := make([][]byte, 0, len(binaryRecords))
	for _, binaryRecord := range binaryRecords {
		// Skip empty records
		if len(binaryRecord) == 0 {
			continue
		}

		// Decode binary
		native, _, err := codec.NativeFromBinary(bytes.NewReader(binaryRecord))
		if err != nil {
			return nil, errors.Wrap(err, "failed to decode binary")
		}

		// Convert to JSON
		json, err := codec.TextualFromNative(nil, native)
		if err != nil {
			return nil, errors.Wrap(err, "failed to convert to JSON")
		}

		result = append(result, json)
	}

	// Record metrics
	c.metrics.CounterAdd("avro_binary_to_json_records", float64(len(result)), nil)

	return result, nil
}

// ConvertBinaryToCSV converts Binary format data to CSV format
func (c *AvroConverterImpl) ConvertBinaryToCSV(binaryData []byte, schemaID string) ([]byte, error) {
	// Get schema
	schema, err := c.getSchema(schemaID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get schema")
	}

	// Create codec
	codec, err := c.getCodec(schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	// Decode binary
	native, _, err := codec.NativeFromBinary(bytes.NewReader(binaryData))
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode binary")
	}

	// Convert to map
	record, ok := native.(map[string]interface{})
	if !ok {
		return nil, errors.New("data is not a record")
	}

	// Get fields from schema
	schemaObj, err := parseSchema(schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse schema")
	}
	fields, err := getRecordFields(schemaObj)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get fields")
	}

	// Build CSV
	var buf bytes.Buffer
	for i, field := range fields {
		name := field["name"].(string)
		value := record[name]

		// Convert value to string
		var strValue string
		if value == nil {
			strValue = ""
		} else {
			strValue = fmt.Sprintf("%v", value)
		}

		// Escape if needed
		if strings.Contains(strValue, ",") || strings.Contains(strValue, "\"") || strings.Contains(strValue, "\n") {
			strValue = "\"" + strings.Replace(strValue, "\"", "\"\"", -1) + "\""
		}

		// Write to buffer
		buf.WriteString(strValue)
		if i < len(fields)-1 {
			buf.WriteByte(',')
		}
	}
	buf.WriteByte('\n')

	// Record metrics
	c.metrics.CounterInc("avro_binary_to_csv_records", nil)

	return buf.Bytes(), nil
}

// ConvertCSVToBinary converts CSV format data to Binary format
func (c *AvroConverterImpl) ConvertCSVToBinary(csvData []byte, schemaID string) ([]byte, error) {
	// Get schema
	schema, err := c.getSchema(schemaID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get schema")
	}

	// Create codec
	codec, err := c.getCodec(schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	// Get fields from schema
	schemaObj, err := parseSchema(schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse schema")
	}
	fields, err := getRecordFields(schemaObj)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get fields")
	}

	// Parse CSV
	csvReader := csv.NewReader(bytes.NewReader(csvData))
	csvRecord, err := csvReader.Read()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read CSV")
	}

	// Check fields count
	if len(csvRecord) != len(fields) {
		return nil, errors.Errorf("CSV record has %d fields, but schema has %d fields", len(csvRecord), len(fields))
	}

	// Create record
	record := make(map[string]interface{})
	for i, field := range fields {
		name := field["name"].(string)
		fieldType, err := getFieldType(field)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get type for field %s", name)
		}

		// Convert value based on type
		value, err := convertStringToType(csvRecord[i], fieldType)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to convert value for field %s", name)
		}

		record[name] = value
	}

	// Convert to binary
	binary, err := codec.BinaryFromNative(nil, record)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert to binary")
	}

	// Record metrics
	c.metrics.CounterInc("avro_csv_to_binary_records", nil)

	return binary, nil
}

// getFieldType gets the Avro type for a field
func getFieldType(field map[string]interface{}) (string, error) {
	fieldType, ok := field["type"]
	if !ok {
		return "", errors.New("field has no type")
	}

	// Handle primitive types
	if strType, ok := fieldType.(string); ok {
		return strType, nil
	}

	// Handle complex types
	typeJSON, err := json.Marshal(fieldType)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal field type")
	}

	return string(typeJSON), nil
}

// convertStringToType converts a string value to the specified Avro type
func convertStringToType(value string, avroType string) (interface{}, error) {
	// Handle primitive types
	switch avroType {
	case "null":
		return nil, nil
	case "boolean":
		return value == "true" || value == "1" || value == "yes", nil
	case "int":
		i, err := strconv.Atoi(value)
		if err != nil {
			return nil, errors.Wrap(err, "invalid int")
		}
		return i, nil
	case "long":
		i, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "invalid long")
		}
		return i, nil
	case "float":
		f, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return nil, errors.Wrap(err, "invalid float")
		}
		return float32(f), nil
	case "double":
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, errors.Wrap(err, "invalid double")
		}
		return f, nil
	case "bytes":
		return []byte(value), nil
	case "string":
		return value, nil
	default:
		// For complex types, use JSON unmarshaling
		var result interface{}
		if err := json.Unmarshal([]byte(value), &result); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal complex type")
		}
		return result, nil
	}
}

// CreateAvroConverter creates an Avro converter with the given configuration
func CreateAvroConverter(
	config config.ConverterConfig,
	schemaResolver schema.SchemaResolver,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) (AvroConverter, error) {
	// Create converter configuration
	converterConfig := config.AvroConverter

	// Create converter
	return NewAvroConverter(converterConfig, schemaResolver, logger, metricsRecorder)
}

// RegisterMetrics registers metrics for the Avro converter
func RegisterMetrics(registry metrics.MetricsRegistry) {
	// Register counters
	registry.RegisterCounter("avro_ocf_to_binary_records", "Number of records converted from OCF to Binary format")
	registry.RegisterCounter("avro_ocf_to_json_records", "Number of records converted from OCF to JSON format")
	registry.RegisterCounter("avro_binary_to_json_records", "Number of records converted from Binary to JSON format")
	registry.RegisterCounter("avro_json_to_binary_records", "Number of records converted from JSON to Binary format")
	registry.RegisterCounter("avro_binary_to_csv_records", "Number of records converted from Binary to CSV format")
	registry.RegisterCounter("avro_csv_to_binary_records", "Number of records converted from CSV to Binary format")

	// Register timers
	registry.RegisterTimer("avro_conversion_time", "Time taken for Avro format conversion")
}

// AvroFormatDetector implements a detector for Avro formats
type AvroFormatDetector struct {
	// logger is used for logging
	logger logging.Logger
}

// NewAvroFormatDetector creates a new Avro format detector
func NewAvroFormatDetector(logger logging.Logger) *AvroFormatDetector {
	return &AvroFormatDetector{
		logger: logger,
	}
}

// DetectFormat detects the format of the given data
func (d *AvroFormatDetector) DetectFormat(data []byte) (FormatType, string) {
	// Check if it's OCF
	if len(data) >= 4 && string(data[:4]) == "Obj\x01" {
		// Try to extract schema
		schema, err := ExtractSchemaFromOCF(data)
		if err != nil {
			d.logger.Warn("Failed to extract schema from OCF", "error", err)
			return FormatOCF, ""
		}
		return FormatOCF, schema
	}

	// Check if it's JSON
	if len(data) > 0 && (data[0] == '{' || data[0] == '[') {
		var js interface{}
		if err := json.Unmarshal(data, &js); err == nil {
			return FormatJSON, ""
		}
	}

	// Try to decode as Avro binary with a simple schema
	// This is a heuristic since binary doesn't have a clear signature
	simpleSchema := `{"type":"record","name":"Test","fields":[{"name":"f1","type":"string"}]}`
	codec, err := goavro.NewCodec(simpleSchema)
	if err == nil {
		_, _, err := codec.NativeFromBinary(bytes.NewReader(data))
		if err == nil || strings.Contains(err.Error(), "cannot decode binary") {
			// If we can decode or get a specific Avro error, it's likely binary
			return FormatBinary, ""
		}
	}

	// Unknown format
	return FormatUnknown, ""
}

// IsAvroFormat checks if the given data is in any Avro format
func IsAvroFormat(data []byte) bool {
	detector := NewAvroFormatDetector(logging.NewNoOpLogger())
	format, _ := detector.DetectFormat(data)
	return format != FormatUnknown
}

//Personal.AI order the ending
