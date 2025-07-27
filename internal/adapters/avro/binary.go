// Package avro provides functionality for working with Apache Avro data formats.
package avro

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"

	"github.com/linkedin/goavro/v2"
	"github.com/pkg/errors"

	logging "github.com/turtacn/staravail/internal/common/logger"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/infrastructure/config"
)

// BinaryReader defines the interface for reading Avro Binary format
type BinaryReader interface {
	// ReadRecord reads a single record from the input
	ReadRecord() (interface{}, error)

	// SetSchema sets the reader schema
	SetSchema(schema string) error

	// SetWriterSchema sets the writer schema (for schema evolution)
	SetWriterSchema(writerSchema string) error

	// ReadBatch reads a batch of records from the input
	ReadBatch(maxRecords int) ([]interface{}, error)

	// Reset resets the reader to the beginning
	Reset() error

	// Close closes the reader
	Close() error
}

// BinaryWriter defines the interface for writing Avro Binary format
type BinaryWriter interface {
	// WriteRecord writes a single record to the output
	WriteRecord(record interface{}) error

	// SetSchema sets the writer schema
	SetSchema(schema string) error

	// WriteRecords writes multiple records to the output
	WriteRecords(records []interface{}) error

	// Flush flushes any buffered data
	Flush() error

	// Close closes the writer
	Close() error
}

// BinaryReaderConfig contains configuration for Binary reader
type BinaryReaderConfig struct {
	// BufferSize is the size of the read buffer
	BufferSize int
	// EnableSchemaResolution enables schema resolution between reader and writer schemas
	EnableSchemaResolution bool
	// EnableReuse enables reuse of objects to reduce allocations
	EnableReuse bool
}

// DefaultBinaryReaderConfig returns the default configuration for Binary reader
func DefaultBinaryReaderConfig() BinaryReaderConfig {
	return BinaryReaderConfig{
		BufferSize:             64 * 1024, // 64KB
		EnableSchemaResolution: true,
		EnableReuse:            true,
	}
}

// BinaryWriterConfig contains configuration for Binary writer
type BinaryWriterConfig struct {
	// BufferSize is the size of the write buffer
	BufferSize int
	// EnableBuffering enables buffering of records before writing
	EnableBuffering bool
	// BufferRecords is the number of records to buffer before writing
	BufferRecords int
}

// DefaultBinaryWriterConfig returns the default configuration for Binary writer
func DefaultBinaryWriterConfig() BinaryWriterConfig {
	return BinaryWriterConfig{
		BufferSize:      64 * 1024, // 64KB
		EnableBuffering: true,
		BufferRecords:   1000,
	}
}

// BinaryReaderImpl implements the BinaryReader interface
type BinaryReaderImpl struct {
	// reader is the underlying reader
	reader io.Reader
	// readerBuf is a buffered reader wrapper
	readerBuf *bufferedReader
	// closer is the underlying closer
	closer io.Closer
	// codec is the goavro codec
	codec *goavro.Codec
	// readerSchema is the schema used for reading
	readerSchema string
	// writerSchema is the schema used by the writer
	writerSchema string
	// config is the reader configuration
	config BinaryReaderConfig
	// logger is used for logging
	logger logging.Logger
	// metrics is used for recording metrics
	metrics metrics.MetricsRecorder
	// hasSchemaResolution indicates if schema resolution is needed
	hasSchemaResolution bool
	// mutex protects concurrent access
	mutex sync.Mutex
	// objectPool is a pool of objects for reuse
	objectPool sync.Pool
}

// bufferedReader implements a buffered reader with Reset capability
type bufferedReader struct {
	reader io.Reader
	buf    []byte
	start  int
	end    int
}

// newBufferedReader creates a new buffered reader
func newBufferedReader(r io.Reader, size int) *bufferedReader {
	return &bufferedReader{
		reader: r,
		buf:    make([]byte, size),
	}
}

// Read reads data from the buffer
func (b *bufferedReader) Read(p []byte) (n int, err error) {
	if b.start >= b.end {
		// Buffer is empty, fill it
		b.start = 0
		b.end, err = b.reader.Read(b.buf)
		if err != nil {
			return 0, err
		}
		if b.end == 0 {
			return 0, io.EOF
		}
	}

	// Copy data from buffer to p
	n = copy(p, b.buf[b.start:b.end])
	b.start += n
	return n, nil
}

// Reset resets the reader with a new reader
func (b *bufferedReader) Reset(r io.Reader) {
	b.reader = r
	b.start = 0
	b.end = 0
}

// NewBinaryReader creates a new Binary reader
func NewBinaryReader(r io.Reader, cfg config.BinaryReaderConfig, logger logging.Logger, metricsRecorder metrics.MetricsRecorder) (BinaryReader, error) {
	// Initialize configuration
	config := BinaryReaderConfig{
		BufferSize:             cfg.BufferSize,
		EnableSchemaResolution: cfg.EnableSchemaResolution,
		EnableReuse:            cfg.EnableReuse,
	}

	// Use default values if not specified
	if config.BufferSize <= 0 {
		config.BufferSize = DefaultBinaryReaderConfig().BufferSize
	}

	// Create buffered reader
	br := newBufferedReader(r, config.BufferSize)

	// Create the reader
	reader := &BinaryReaderImpl{
		reader:    r,
		readerBuf: br,
		config:    config,
		logger:    logger,
		metrics:   metricsRecorder,
	}

	// If the reader implements io.Closer, store it
	if closer, ok := r.(io.Closer); ok {
		reader.closer = closer
	}

	// Initialize object pool if reuse is enabled
	if config.EnableReuse {
		reader.objectPool = sync.Pool{
			New: func() interface{} {
				return make(map[string]interface{})
			},
		}
	}

	return reader, nil
}

// SetSchema sets the reader schema
func (r *BinaryReaderImpl) SetSchema(schema string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Store reader schema
	r.readerSchema = schema

	// Create codec
	if r.writerSchema != "" && r.writerSchema != schema && r.config.EnableSchemaResolution {
		// Create codec with schema resolution
		return r.createCodecWithResolution()
	} else {
		// Create codec without schema resolution
		codec, err := goavro.NewCodec(schema)
		if err != nil {
			return errors.Wrap(err, "failed to create codec")
		}
		r.codec = codec
		r.hasSchemaResolution = false
	}

	return nil
}

// SetWriterSchema sets the writer schema (for schema evolution)
func (r *BinaryReaderImpl) SetWriterSchema(writerSchema string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Store writer schema
	r.writerSchema = writerSchema

	// If reader schema is already set, create codec with resolution
	if r.readerSchema != "" && r.readerSchema != writerSchema && r.config.EnableSchemaResolution {
		return r.createCodecWithResolution()
	} else if r.readerSchema == "" {
		// If reader schema is not set, use writer schema as reader schema
		codec, err := goavro.NewCodec(writerSchema)
		if err != nil {
			return errors.Wrap(err, "failed to create codec")
		}
		r.codec = codec
		r.readerSchema = writerSchema
		r.hasSchemaResolution = false
	}

	return nil
}

// createCodecWithResolution creates a codec with schema resolution
func (r *BinaryReaderImpl) createCodecWithResolution() error {
	// Parse reader schema
	readerSchema, err := parseSchema(r.readerSchema)
	if err != nil {
		return errors.Wrap(err, "failed to parse reader schema")
	}

	// Parse writer schema
	writerSchema, err := parseSchema(r.writerSchema)
	if err != nil {
		return errors.Wrap(err, "failed to parse writer schema")
	}

	// Check compatibility
	if err := checkSchemaCompatibility(writerSchema, readerSchema); err != nil {
		return errors.Wrap(err, "schemas are not compatible")
	}

	// Create codec with writer schema
	codec, err := goavro.NewCodec(r.writerSchema)
	if err != nil {
		return errors.Wrap(err, "failed to create codec")
	}
	r.codec = codec
	r.hasSchemaResolution = true

	return nil
}

// ReadRecord reads a single record from the input
func (r *BinaryReaderImpl) ReadRecord() (interface{}, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Check if codec is initialized
	if r.codec == nil {
		return nil, errors.New("codec not initialized, schema must be set")
	}

	// Read record
	native, _, err := r.codec.NativeFromBinary(r.readerBuf)
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, errors.Wrap(err, "failed to decode record")
	}

	// Apply schema resolution if needed
	if r.hasSchemaResolution {
		native, err = r.resolveSchema(native)
		if err != nil {
			return nil, errors.Wrap(err, "failed to resolve schema")
		}
	}

	// Record metrics
	r.metrics.CounterInc("avro_binary_records_read", nil)

	return native, nil
}

// ReadBatch reads a batch of records from the input
func (r *BinaryReaderImpl) ReadBatch(maxRecords int) ([]interface{}, error) {
	// Validate maxRecords
	if maxRecords <= 0 {
		maxRecords = 1000 // Default to 1000 records
	}

	// Allocate result slice
	result := make([]interface{}, 0, maxRecords)

	// Read records until maxRecords or EOF
	for i := 0; i < maxRecords; i++ {
		record, err := r.ReadRecord()
		if err != nil {
			if err == io.EOF {
				// Return what we have so far
				return result, nil
			}
			return nil, err
		}
		result = append(result, record)
	}

	return result, nil
}

// resolveSchema applies schema resolution to convert data from writer schema to reader schema
func (r *BinaryReaderImpl) resolveSchema(data interface{}) (interface{}, error) {
	// If data is not a map, return as is
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		return data, nil
	}

	// Get a map from the pool if reuse is enabled
	var resultMap map[string]interface{}
	if r.config.EnableReuse {
		resultMapObj := r.objectPool.Get()
		resultMap = resultMapObj.(map[string]interface{})
		// Clear the map
		for k := range resultMap {
			delete(resultMap, k)
		}
		// Return the map to the pool when done
		defer r.objectPool.Put(resultMapObj)
	} else {
		resultMap = make(map[string]interface{})
	}

	// Parse reader schema
	readerSchema, err := parseSchema(r.readerSchema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse reader schema")
	}

	// Get reader fields
	readerFields, err := getRecordFields(readerSchema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get reader fields")
	}

	// Apply schema resolution
	for _, field := range readerFields {
		fieldName := field["name"].(string)
		// Check if field exists in writer data
		if value, ok := dataMap[fieldName]; ok {
			// Copy value to result
			resultMap[fieldName] = value
		} else if defaultValue, ok := field["default"]; ok {
			// Use default value if provided
			resultMap[fieldName] = defaultValue
		} else {
			// Field is required but not present
			return nil, errors.Errorf("required field %s not found in data", fieldName)
		}
	}

	return resultMap, nil
}

// Reset resets the reader to the beginning
func (r *BinaryReaderImpl) Reset() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Check if reader implements io.Seeker
	seeker, ok := r.reader.(io.Seeker)
	if !ok {
		return errors.New("reader does not support seeking")
	}

	// Seek to the beginning
	_, err := seeker.Seek(0, io.SeekStart)
	if err != nil {
		return errors.Wrap(err, "failed to seek to the beginning")
	}

	// Reset buffered reader
	r.readerBuf.Reset(r.reader)

	return nil
}

// Close closes the reader
func (r *BinaryReaderImpl) Close() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Close underlying reader if it implements io.Closer
	if r.closer != nil {
		return r.closer.Close()
	}

	return nil
}

// BinaryWriterImpl implements the BinaryWriter interface
type BinaryWriterImpl struct {
	// writer is the underlying writer
	writer io.Writer
	// writerBuf is a buffered writer wrapper
	writerBuf *bufio.Writer
	// closer is the underlying closer
	closer io.Closer
	// codec is the goavro codec
	codec *goavro.Codec
	// schema is the schema used for writing
	schema string
	// config is the writer configuration
	config BinaryWriterConfig
	// logger is used for logging
	logger logging.Logger
	// metrics is used for recording metrics
	metrics metrics.MetricsRecorder
	// buffer is a buffer for records when EnableBuffering is true
	buffer []interface{}
	// mutex protects concurrent access
	mutex sync.Mutex
}

// NewBinaryWriter creates a new Binary writer
func NewBinaryWriter(w io.Writer, cfg config.BinaryWriterConfig, logger logging.Logger, metricsRecorder metrics.MetricsRecorder) (BinaryWriter, error) {
	// Initialize configuration
	config := BinaryWriterConfig{
		BufferSize:      cfg.BufferSize,
		EnableBuffering: cfg.EnableBuffering,
		BufferRecords:   cfg.BufferRecords,
	}

	// Use default values if not specified
	if config.BufferSize <= 0 {
		config.BufferSize = DefaultBinaryWriterConfig().BufferSize
	}
	if config.BufferRecords <= 0 {
		config.BufferRecords = DefaultBinaryWriterConfig().BufferRecords
	}

	// Create buffered writer
	bw := bufio.NewWriterSize(w, config.BufferSize)

	// Create the writer
	writer := &BinaryWriterImpl{
		writer:    w,
		writerBuf: bw,
		config:    config,
		logger:    logger,
		metrics:   metricsRecorder,
	}

	// If the writer implements io.Closer, store it
	if closer, ok := w.(io.Closer); ok {
		writer.closer = closer
	}

	// Initialize buffer if buffering is enabled
	if config.EnableBuffering {
		writer.buffer = make([]interface{}, 0, config.BufferRecords)
	}

	return writer, nil
}

// SetSchema sets the writer schema
func (w *BinaryWriterImpl) SetSchema(schema string) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Store schema
	w.schema = schema

	// Create codec
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return errors.Wrap(err, "failed to create codec")
	}
	w.codec = codec

	return nil
}

// WriteRecord writes a single record to the output
func (w *BinaryWriterImpl) WriteRecord(record interface{}) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Check if codec is initialized
	if w.codec == nil {
		return errors.New("codec not initialized, schema must be set")
	}

	// If buffering is enabled, add to buffer
	if w.config.EnableBuffering {
		w.buffer = append(w.buffer, record)

		// If buffer is full, flush it
		if len(w.buffer) >= w.config.BufferRecords {
			return w.flushBufferLocked()
		}

		return nil
	}

	// Encode record
	binary, err := w.codec.BinaryFromNative(nil, record)
	if err != nil {
		return errors.Wrap(err, "failed to encode record")
	}

	// Write to output
	if _, err := w.writerBuf.Write(binary); err != nil {
		return errors.Wrap(err, "failed to write record")
	}

	// Record metrics
	w.metrics.CounterInc("avro_binary_records_written", nil)

	return nil
}

// WriteRecords writes multiple records to the output
func (w *BinaryWriterImpl) WriteRecords(records []interface{}) error {
	// If buffering is enabled, use it
	if w.config.EnableBuffering {
		w.mutex.Lock()
		defer w.mutex.Unlock()

		// Add records to buffer
		w.buffer = append(w.buffer, records...)

		// If buffer is full, flush it
		if len(w.buffer) >= w.config.BufferRecords {
			return w.flushBufferLocked()
		}

		return nil
	}

	// Write records individually
	for _, record := range records {
		if err := w.WriteRecord(record); err != nil {
			return err
		}
	}

	return nil
}

// flushBufferLocked flushes the buffer (must be called with mutex held)
func (w *BinaryWriterImpl) flushBufferLocked() error {
	// Skip if buffer is empty
	if len(w.buffer) == 0 {
		return nil
	}

	// Encode and write each record
	for _, record := range w.buffer {
		binary, err := w.codec.BinaryFromNative(nil, record)
		if err != nil {
			return errors.Wrap(err, "failed to encode record")
		}

		if _, err := w.writerBuf.Write(binary); err != nil {
			return errors.Wrap(err, "failed to write record")
		}
	}

	// Record metrics
	w.metrics.CounterAdd("avro_binary_records_written", float64(len(w.buffer)), nil)

	// Clear buffer
	w.buffer = w.buffer[:0]

	return nil
}

// Flush flushes any buffered data
func (w *BinaryWriterImpl) Flush() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Flush buffer if buffering is enabled
	if w.config.EnableBuffering {
		if err := w.flushBufferLocked(); err != nil {
			return err
		}
	}

	// Flush the buffered writer
	return w.writerBuf.Flush()
}

// Close closes the writer
func (w *BinaryWriterImpl) Close() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Flush buffer if buffering is enabled
	if w.config.EnableBuffering {
		if err := w.flushBufferLocked(); err != nil {
			return err
		}
	}

	// Flush the buffered writer
	if err := w.writerBuf.Flush(); err != nil {
		return errors.Wrap(err, "failed to flush writer")
	}

	// Close underlying writer if it implements io.Closer
	if w.closer != nil {
		return w.closer.Close()
	}

	return nil
}

// parseSchema parses a schema string into an object
func parseSchema(schema string) (interface{}, error) {
	var schemaObj interface{}
	if err := json.Unmarshal([]byte(schema), &schemaObj); err != nil {
		return nil, errors.Wrap(err, "invalid schema JSON")
	}
	return schemaObj, nil
}

// getRecordFields gets fields from a record schema
func getRecordFields(schema interface{}) ([]map[string]interface{}, error) {
	// Handle schema as string
	if schemaStr, ok := schema.(string); ok {
		var schemaObj interface{}
		if err := json.Unmarshal([]byte(schemaStr), &schemaObj); err != nil {
			return nil, errors.Wrap(err, "invalid schema JSON")
		}
		schema = schemaObj
	}

	// Handle schema as map
	schemaMap, ok := schema.(map[string]interface{})
	if !ok {
		return nil, errors.New("schema is not a map")
	}

	// Check type
	schemaType, ok := schemaMap["type"].(string)
	if !ok {
		return nil, errors.New("schema type is not a string")
	}

	// Must be a record
	if schemaType != "record" {
		return nil, errors.Errorf("schema is not a record: %s", schemaType)
	}

	// Get fields
	fieldsObj, ok := schemaMap["fields"]
	if !ok {
		return nil, errors.New("record schema has no fields")
	}

	// Convert fields to slice of maps
	fields, ok := fieldsObj.([]interface{})
	if !ok {
		return nil, errors.New("fields is not a slice")
	}

	// Convert fields to slice of maps
	result := make([]map[string]interface{}, 0, len(fields))
	for _, field := range fields {
		fieldMap, ok := field.(map[string]interface{})
		if !ok {
			return nil, errors.New("field is not a map")
		}
		result = append(result, fieldMap)
	}

	return result, nil
}

// checkSchemaCompatibility checks if writer schema is compatible with reader schema
func checkSchemaCompatibility(writerSchema, readerSchema interface{}) error {
	// Handle schema as string
	if writerSchemaStr, ok := writerSchema.(string); ok {
		var writerSchemaObj interface{}
		if err := json.Unmarshal([]byte(writerSchemaStr), &writerSchemaObj); err != nil {
			return errors.Wrap(err, "invalid writer schema JSON")
		}
		writerSchema = writerSchemaObj
	}
	if readerSchemaStr, ok := readerSchema.(string); ok {
		var readerSchemaObj interface{}
		if err := json.Unmarshal([]byte(readerSchemaStr), &readerSchemaObj); err != nil {
			return errors.Wrap(err, "invalid reader schema JSON")
		}
		readerSchema = readerSchemaObj
	}

	// Get writer schema type
	writerSchemaMap, ok := writerSchema.(map[string]interface{})
	if !ok {
		return errors.New("writer schema is not a map")
	}
	writerType, ok := writerSchemaMap["type"].(string)
	if !ok {
		return errors.New("writer schema type is not a string")
	}

	// Get reader schema type
	readerSchemaMap, ok := readerSchema.(map[string]interface{})
	if !ok {
		return errors.New("reader schema is not a map")
	}
	readerType, ok := readerSchemaMap["type"].(string)
	if !ok {
		return errors.New("reader schema type is not a string")
	}

	// Check if types are compatible
	if writerType != readerType {
		// Handle special cases like union and promotion
		if isPromotable(writerType, readerType) {
			return nil
		}
		return errors.Errorf("incompatible types: writer=%s, reader=%s", writerType, readerType)
	}

	// If both are records, check field compatibility
	if writerType == "record" && readerType == "record" {
		// Get writer fields
		writerFields, err := getRecordFields(writerSchema)
		if err != nil {
			return errors.Wrap(err, "failed to get writer fields")
		}

		// Get reader fields
		readerFields, err := getRecordFields(readerSchema)
		if err != nil {
			return errors.Wrap(err, "failed to get reader fields")
		}

		// Create reader field map for lookup
		readerFieldMap := make(map[string]map[string]interface{})
		for _, field := range readerFields {
			name := field["name"].(string)
			readerFieldMap[name] = field
		}

		// Check each writer field
		for _, writerField := range writerFields {
			name := writerField["name"].(string)
			// Check if field exists in reader
			if readerField, ok := readerFieldMap[name]; ok {
				// Check if field types are compatible
				writerFieldType := writerField["type"]
				readerFieldType := readerField["type"]
				if err := checkSchemaCompatibility(writerFieldType, readerFieldType); err != nil {
					return errors.Wrapf(err, "incompatible field: %s", name)
				}
			}
		}
	}

	return nil
}

// isPromotable checks if writer type can be promoted to reader type
func isPromotable(writerType, readerType string) bool {
	// Numeric type promotion
	if writerType == "int" && (readerType == "long" || readerType == "float" || readerType == "double") {
		return true
	}
	if writerType == "long" && (readerType == "float" || readerType == "double") {
		return true
	}
	if writerType == "float" && readerType == "double" {
		return true
	}

	// String promotion
	if writerType == "string" && readerType == "bytes" {
		return true
	}
	if writerType == "bytes" && readerType == "string" {
		return true
	}

	return false
}

// ConvertToJSON converts an Avro binary data to JSON
func ConvertToJSON(data []byte, schema string) ([]byte, error) {
	// Create codec
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	// Decode binary
	native, _, err := codec.NativeFromBinary(bytes.NewReader(data))
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode binary")
	}

	// Convert to JSON
	json, err := codec.TextualFromNative(nil, native)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert to JSON")
	}

	return json, nil
}

// ConvertFromJSON converts JSON data to Avro binary
func ConvertFromJSON(data []byte, schema string) ([]byte, error) {
	// Create codec
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	// Decode JSON
	native, _, err := codec.NativeFromTextual(data)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode JSON")
	}

	// Convert to binary
	binary, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert to binary")
	}

	return binary, nil
}

// ConvertToCSV converts Avro data to CSV
func ConvertToCSV(data []byte, schema string) ([]byte, error) {
	// Create codec
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	// Decode binary
	native, _, err := codec.NativeFromBinary(bytes.NewReader(data))
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

	return buf.Bytes(), nil
}

// ConvertStructToAvro converts a Go struct to Avro binary
func ConvertStructToAvro(obj interface{}, schema string) ([]byte, error) {
	// Convert struct to map
	data, err := structToMap(obj)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert struct to map")
	}

	// Create codec
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create codec")
	}

	// Convert to binary
	binary, err := codec.BinaryFromNative(nil, data)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert to binary")
	}

	return binary, nil
}

// ConvertAvroToStruct converts Avro binary to a Go struct
func ConvertAvroToStruct(data []byte, schema string, obj interface{}) error {
	// Create codec
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return errors.Wrap(err, "failed to create codec")
	}

	// Decode binary
	native, _, err := codec.NativeFromBinary(bytes.NewReader(data))
	if err != nil {
		return errors.Wrap(err, "failed to decode binary")
	}

	// Convert to struct
	return mapToStruct(native, obj)
}

// structToMap converts a Go struct to a map
func structToMap(obj interface{}) (map[string]interface{}, error) {
	// Marshal to JSON
	jsonData, err := json.Marshal(obj)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal struct to JSON")
	}

	// Unmarshal to map
	var result map[string]interface{}
	if err := json.Unmarshal(jsonData, &result); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal JSON to map")
	}

	return result, nil
}

// mapToStruct converts a map to a Go struct
func mapToStruct(data interface{}, obj interface{}) error {
	// Marshal to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return errors.Wrap(err, "failed to marshal map to JSON")
	}

	// Unmarshal to struct
	if err := json.Unmarshal(jsonData, obj); err != nil {
		return errors.Wrap(err, "failed to unmarshal JSON to struct")
	}

	return nil
}

// CreateSchemaFromStruct creates an Avro schema from a Go struct
func CreateSchemaFromStruct(obj interface{}) (string, error) {
	// Get type information
	t := reflect.TypeOf(obj)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return "", errors.New("object is not a struct")
	}

	// Generate schema
	schemaMap := make(map[string]interface{})
	schemaMap["type"] = "record"
	schemaMap["name"] = t.Name()
	schemaMap["namespace"] = t.PkgPath()

	// Generate fields
	fields := make([]map[string]interface{}, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldName := field.Tag.Get("json")
		if fieldName == "" {
			fieldName = field.Name
		} else {
			// Handle json:"name,omitempty"
			parts := strings.Split(fieldName, ",")
			if len(parts) > 0 {
				fieldName = parts[0]
			}
		}

		// Skip if field name is "-"
		if fieldName == "-" {
			continue
		}

		fieldMap := make(map[string]interface{})
		fieldMap["name"] = fieldName
		fieldMap["type"] = getAvroType(field.Type)

		fields = append(fields, fieldMap)
	}
	schemaMap["fields"] = fields

	// Convert to JSON
	schemaJSON, err := json.Marshal(schemaMap)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal schema to JSON")
	}

	return string(schemaJSON), nil
}

// getAvroType gets the Avro type for a Go type
func getAvroType(t reflect.Type) interface{} {
	switch t.Kind() {
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return "int"
	case reflect.Int64:
		return "long"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return "int"
	case reflect.Uint64:
		return "long"
	case reflect.Float32:
		return "float"
	case reflect.Float64:
		return "double"
	case reflect.String:
		return "string"
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "bytes"
		}
		return map[string]interface{}{
			"type":  "array",
			"items": getAvroType(t.Elem()),
		}
	case reflect.Map:
		if t.Key().Kind() == reflect.String {
			return map[string]interface{}{
				"type":   "map",
				"values": getAvroType(t.Elem()),
			}
		}
	case reflect.Struct:
		// Handle time.Time as string
		if t.String() == "time.Time" {
			return "string"
		}
		// Create nested record
		fields := make([]map[string]interface{}, 0, t.NumField())
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			fieldName := field.Tag.Get("json")
			if fieldName == "" {
				fieldName = field.Name
			} else {
				// Handle json:"name,omitempty"
				parts := strings.Split(fieldName, ",")
				if len(parts) > 0 {
					fieldName = parts[0]
				}
			}

			// Skip if field name is "-"
			if fieldName == "-" {
				continue
			}

			fieldMap := make(map[string]interface{})
			fieldMap["name"] = fieldName
			fieldMap["type"] = getAvroType(field.Type)

			fields = append(fields, fieldMap)
		}
		return map[string]interface{}{
			"type":      "record",
			"name":      t.Name(),
			"namespace": t.PkgPath(),
			"fields":    fields,
		}
	case reflect.Ptr:
		// For pointers, create a union with null
		return []interface{}{
			"null",
			getAvroType(t.Elem()),
		}
	}

	// Default to string for unsupported types
	return "string"
}

//Personal.AI order the ending
