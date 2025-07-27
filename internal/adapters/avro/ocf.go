// Package avro provides functionality for working with Apache Avro data formats.
package avro

import (
	"bufio"
	"bytes"
	"compress/flate"
	"compress/zlib"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"sync"

	"github.com/golang/snappy"
	"github.com/linkedin/goavro/v2"
	"github.com/pkg/errors"
	"github.com/turtacn/staravail/internal/common/config"
	"github.com/turtacn/staravail/internal/common/logging"
)

const (
	// OCFMagicBytes is the magic header of Avro OCF files
	OCFMagicBytes = "Obj\x01"
	// OCFMetadataSchema is the key for the schema metadata
	OCFMetadataSchema = "avro.schema"
	// OCFMetadataCodec is the key for the codec metadata
	OCFMetadataCodec = "avro.codec"
	// OCFSyncMarkerSize is the size of the sync marker in bytes
	OCFSyncMarkerSize = 16
)

// Supported compression codecs
const (
	CodecNull    = "null"
	CodecDeflate = "deflate"
	CodecSnappy  = "snappy"
	CodecZlib    = "zlib"
)

// OCFHeaderInfo contains information from the OCF header
type OCFHeaderInfo struct {
	// Schema is the parsed Avro schema
	Schema string
	// Codec is the compression codec used
	Codec string
	// Metadata is additional metadata from the header
	Metadata map[string][]byte
	// SyncMarker is the sync marker for this file
	SyncMarker []byte
}

// OCFBlock represents a data block within an OCF file
type OCFBlock struct {
	// Count is the number of records in the block
	Count int64
	// RawData is the raw (possibly compressed) block data
	RawData []byte
	// Objects is the decoded objects (only populated if ReadObjects is used)
	Objects []interface{}
}

// OCFReader defines the interface for reading Avro OCF (Object Container File) format
type OCFReader interface {
	// ReadHeader reads and validates the OCF header
	ReadHeader() (*OCFHeaderInfo, error)

	// ReadBlock reads the next block of data
	ReadBlock() (*OCFBlock, error)

	// ReadBlockObjects reads the next block of data and decodes the objects
	ReadBlockObjects() (*OCFBlock, error)

	// GetSchema returns the schema from the OCF file
	GetSchema() string

	// GetCodec returns the codec used in the OCF file
	GetCodec() string

	// GetHeaderInfo returns the header information
	GetHeaderInfo() *OCFHeaderInfo

	// Reset resets the reader to the beginning
	Reset() error

	// Close closes the reader
	Close() error
}

// OCFReaderConfig contains configuration for OCF reader
type OCFReaderConfig struct {
	// BufferSize is the size of the read buffer
	BufferSize int
	// MaxBlockSize is the maximum allowed block size
	MaxBlockSize int64
}

// DefaultOCFReaderConfig returns the default configuration for OCF reader
func DefaultOCFReaderConfig() OCFReaderConfig {
	return OCFReaderConfig{
		BufferSize:   64 * 1024,          // 64KB
		MaxBlockSize: 1024 * 1024 * 1024, // 1GB
	}
}

// OCFReaderImpl implements the OCFReader interface
type OCFReaderImpl struct {
	// reader is the underlying reader
	reader io.Reader
	// bufferedReader is a buffered reader wrapper
	bufferedReader *bufio.Reader
	// closer is the underlying closer
	closer io.Closer
	// header contains the parsed header information
	header *OCFHeaderInfo
	// codec is the goavro codec
	codec *goavro.Codec
	// config is the reader configuration
	config OCFReaderConfig
	// logger is used for logging
	logger logging.Logger
	// headerRead indicates if the header has been read
	headerRead bool
	// mutex protects concurrent access
	mutex sync.Mutex
}

// NewOCFReader creates a new OCF reader
func NewOCFReader(r io.Reader, cfg config.OCFReaderConfig, logger logging.Logger) (OCFReader, error) {
	// Initialize configuration
	config := OCFReaderConfig{
		BufferSize:   cfg.BufferSize,
		MaxBlockSize: cfg.MaxBlockSize,
	}

	// Use default values if not specified
	if config.BufferSize <= 0 {
		config.BufferSize = DefaultOCFReaderConfig().BufferSize
	}
	if config.MaxBlockSize <= 0 {
		config.MaxBlockSize = DefaultOCFReaderConfig().MaxBlockSize
	}

	// Create buffered reader
	br := bufio.NewReaderSize(r, config.BufferSize)

	// Create the reader
	reader := &OCFReaderImpl{
		reader:         r,
		bufferedReader: br,
		config:         config,
		logger:         logger,
	}

	// If the reader implements io.Closer, store it
	if closer, ok := r.(io.Closer); ok {
		reader.closer = closer
	}

	return reader, nil
}

// ReadHeader reads and validates the OCF header
func (r *OCFReaderImpl) ReadHeader() (*OCFHeaderInfo, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// If we've already read the header, return it
	if r.headerRead {
		return r.header, nil
	}

	// Read magic bytes
	magic := make([]byte, len(OCFMagicBytes))
	n, err := io.ReadFull(r.bufferedReader, magic)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read magic bytes")
	}
	if n != len(OCFMagicBytes) || string(magic) != OCFMagicBytes {
		return nil, errors.New("invalid OCF magic bytes")
	}

	// Read metadata
	metadata, err := r.readMetadata()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read metadata")
	}

	// Extract schema
	schemaBytes, ok := metadata[OCFMetadataSchema]
	if !ok {
		return nil, errors.New("schema not found in OCF metadata")
	}

	// Extract codec
	codecBytes, ok := metadata[OCFMetadataCodec]
	if !ok {
		// Default to null codec if not specified
		codecBytes = []byte("null")
	}

	// Validate codec
	codec := string(codecBytes)
	switch codec {
	case CodecNull, CodecDeflate, CodecSnappy, CodecZlib:
		// Supported codec
	default:
		return nil, errors.Errorf("unsupported codec: %s", codec)
	}

	// Read sync marker
	syncMarker := make([]byte, OCFSyncMarkerSize)
	n, err = io.ReadFull(r.bufferedReader, syncMarker)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read sync marker")
	}
	if n != OCFSyncMarkerSize {
		return nil, errors.New("invalid sync marker size")
	}

	// Create goavro codec
	schema := string(schemaBytes)
	goavroCodec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create goavro codec")
	}
	r.codec = goavroCodec

	// Store header info
	r.header = &OCFHeaderInfo{
		Schema:     schema,
		Codec:      codec,
		Metadata:   metadata,
		SyncMarker: syncMarker,
	}
	r.headerRead = true

	r.logger.Debug("Read OCF header",
		"codec", codec,
		"sync_marker", fmt.Sprintf("%x", syncMarker))

	return r.header, nil
}

// readMetadata reads the metadata section from OCF
func (r *OCFReaderImpl) readMetadata() (map[string][]byte, error) {
	// Read metadata count
	var count int64
	if err := binary.Read(r.bufferedReader, binary.BigEndian, &count); err != nil {
		return nil, errors.Wrap(err, "failed to read metadata count")
	}

	// Validate count to prevent DoS attacks
	if count < 0 || count > 1000 {
		return nil, errors.Errorf("invalid metadata count: %d", count)
	}

	// Read metadata pairs
	metadata := make(map[string][]byte)
	for i := int64(0); i < count; i++ {
		// Read key
		var keyLen int64
		if err := binary.Read(r.bufferedReader, binary.BigEndian, &keyLen); err != nil {
			return nil, errors.Wrap(err, "failed to read metadata key length")
		}
		if keyLen < 0 || keyLen > 1024 {
			return nil, errors.Errorf("invalid metadata key length: %d", keyLen)
		}

		key := make([]byte, keyLen)
		n, err := io.ReadFull(r.bufferedReader, key)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read metadata key")
		}
		if int64(n) != keyLen {
			return nil, errors.New("incomplete metadata key")
		}

		// Read value
		var valueLen int64
		if err := binary.Read(r.bufferedReader, binary.BigEndian, &valueLen); err != nil {
			return nil, errors.Wrap(err, "failed to read metadata value length")
		}
		if valueLen < 0 || valueLen > 1024*1024 {
			return nil, errors.Errorf("invalid metadata value length: %d", valueLen)
		}

		value := make([]byte, valueLen)
		n, err = io.ReadFull(r.bufferedReader, value)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read metadata value")
		}
		if int64(n) != valueLen {
			return nil, errors.New("incomplete metadata value")
		}

		metadata[string(key)] = value
	}

	return metadata, nil
}

// ReadBlock reads the next block of data
func (r *OCFReaderImpl) ReadBlock() (*OCFBlock, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Ensure header has been read
	if !r.headerRead {
		if _, err := r.ReadHeader(); err != nil {
			return nil, errors.Wrap(err, "failed to read header")
		}
	}

	// Read block count
	var count int64
	err := binary.Read(r.bufferedReader, binary.BigEndian, &count)
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, errors.Wrap(err, "failed to read block count")
	}

	// A count of zero indicates the end of the file
	if count == 0 {
		// Read the final sync marker
		syncMarker := make([]byte, OCFSyncMarkerSize)
		n, err := io.ReadFull(r.bufferedReader, syncMarker)
		if err != nil && err != io.EOF {
			return nil, errors.Wrap(err, "failed to read final sync marker")
		}
		if n != OCFSyncMarkerSize {
			return nil, errors.New("invalid final sync marker size")
		}
		if !bytes.Equal(syncMarker, r.header.SyncMarker) {
			return nil, errors.New("invalid final sync marker")
		}
		return nil, io.EOF
	}

	// Validate count to prevent DoS attacks
	if count < 0 {
		return nil, errors.Errorf("invalid block count: %d", count)
	}

	// Read block size
	var size int64
	if err := binary.Read(r.bufferedReader, binary.BigEndian, &size); err != nil {
		return nil, errors.Wrap(err, "failed to read block size")
	}

	// Validate size to prevent DoS attacks
	if size <= 0 || size > r.config.MaxBlockSize {
		return nil, errors.Errorf("invalid block size: %d", size)
	}

	// Read block data
	data := make([]byte, size)
	n, err := io.ReadFull(r.bufferedReader, data)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read block data")
	}
	if int64(n) != size {
		return nil, errors.New("incomplete block data")
	}

	// Read sync marker
	syncMarker := make([]byte, OCFSyncMarkerSize)
	n, err = io.ReadFull(r.bufferedReader, syncMarker)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read sync marker")
	}
	if n != OCFSyncMarkerSize {
		return nil, errors.New("invalid sync marker size")
	}
	if !bytes.Equal(syncMarker, r.header.SyncMarker) {
		return nil, errors.New("invalid sync marker")
	}

	// Create block
	block := &OCFBlock{
		Count:   count,
		RawData: data,
	}

	return block, nil
}

// ReadBlockObjects reads the next block of data and decodes the objects
func (r *OCFReaderImpl) ReadBlockObjects() (*OCFBlock, error) {
	// Read block
	block, err := r.ReadBlock()
	if err != nil {
		return nil, err
	}

	// Decompress data if necessary
	var decompressedData []byte
	switch r.header.Codec {
	case CodecNull:
		decompressedData = block.RawData
	case CodecDeflate:
		decompressed, err := decompress(block.RawData)
		if err != nil {
			return nil, errors.Wrap(err, "failed to decompress deflate data")
		}
		decompressedData = decompressed
	case CodecSnappy:
		decompressed, err := snappy.Decode(nil, block.RawData)
		if err != nil {
			return nil, errors.Wrap(err, "failed to decompress snappy data")
		}
		decompressedData = decompressed
	case CodecZlib:
		decompressed, err := decompress(block.RawData)
		if err != nil {
			return nil, errors.Wrap(err, "failed to decompress zlib data")
		}
		decompressedData = decompressed
	default:
		return nil, errors.Errorf("unsupported codec: %s", r.header.Codec)
	}

	// Decode objects
	objects := make([]interface{}, 0, block.Count)
	reader := bytes.NewReader(decompressedData)

	for i := int64(0); i < block.Count; i++ {
		// Read the value as native Go objects
		native, _, err := r.codec.NativeFromBinary(reader)
		if err != nil {
			return nil, errors.Wrap(err, "failed to decode object")
		}
		objects = append(objects, native)
	}

	// Check if we've read all data
	if reader.Len() > 0 {
		r.logger.Warn("Block has more data than expected",
			"remaining_bytes", reader.Len(),
			"expected_count", block.Count,
			"actual_count", len(objects))
	}

	// Add objects to block
	block.Objects = objects

	return block, nil
}

// GetSchema returns the schema from the OCF file
func (r *OCFReaderImpl) GetSchema() string {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Ensure header has been read
	if !r.headerRead {
		if _, err := r.ReadHeader(); err != nil {
			r.logger.Error("Failed to read header", "error", err)
			return ""
		}
	}

	return r.header.Schema
}

// GetCodec returns the codec used in the OCF file
func (r *OCFReaderImpl) GetCodec() string {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Ensure header has been read
	if !r.headerRead {
		if _, err := r.ReadHeader(); err != nil {
			r.logger.Error("Failed to read header", "error", err)
			return ""
		}
	}

	return r.header.Codec
}

// GetHeaderInfo returns the header information
func (r *OCFReaderImpl) GetHeaderInfo() *OCFHeaderInfo {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Ensure header has been read
	if !r.headerRead {
		if _, err := r.ReadHeader(); err != nil {
			r.logger.Error("Failed to read header", "error", err)
			return nil
		}
	}

	return r.header
}

// Reset resets the reader to the beginning
func (r *OCFReaderImpl) Reset() error {
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
	r.bufferedReader.Reset(r.reader)

	// Reset header read flag
	r.headerRead = false
	r.header = nil

	return nil
}

// Close closes the reader
func (r *OCFReaderImpl) Close() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Close underlying reader if it implements io.Closer
	if r.closer != nil {
		return r.closer.Close()
	}

	return nil
}

// decompress decompresses data using the deflate/zlib algorithm
func decompress(data []byte) ([]byte, error) {
	// Since this is OCF, we use raw deflate (RFC 1951) rather than zlib (RFC 1950)
	// In practice, both codecs are similar but have different headers
	reader := bytes.NewReader(data)

	// Try deflate first
	var decompressed bytes.Buffer
	flateReader := flate.NewReader(reader)
	defer flateReader.Close()

	_, err := io.Copy(&decompressed, flateReader)
	if err != nil {
		// If deflate fails, try zlib
		reader.Reset(data)
		zlibReader, err := zlib.NewReader(reader)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create zlib reader")
		}
		defer zlibReader.Close()

		decompressed.Reset()
		_, err = io.Copy(&decompressed, zlibReader)
		if err != nil {
			return nil, errors.Wrap(err, "failed to decompress data")
		}
	}

	return decompressed.Bytes(), nil
}

// OCFWriter defines the interface for writing Avro OCF format
type OCFWriter interface {
	// WriteHeader writes the OCF header
	WriteHeader(schema string, codec string, metadata map[string][]byte) error

	// WriteBlock writes a block of data
	WriteBlock(objects []interface{}) error

	// Flush flushes any buffered data
	Flush() error

	// Close closes the writer
	Close() error
}

// OCFWriterConfig contains configuration for OCF writer
type OCFWriterConfig struct {
	// BufferSize is the size of the write buffer
	BufferSize int
	// BlockSize is the number of records per block
	BlockSize int
}

// DefaultOCFWriterConfig returns the default configuration for OCF writer
func DefaultOCFWriterConfig() OCFWriterConfig {
	return OCFWriterConfig{
		BufferSize: 64 * 1024, // 64KB
		BlockSize:  1000,      // 1000 records per block
	}
}

// OCFWriterImpl implements the OCFWriter interface
type OCFWriterImpl struct {
	// writer is the underlying writer
	writer io.Writer
	// bufferedWriter is a buffered writer wrapper
	bufferedWriter *bufio.Writer
	// closer is the underlying closer
	closer io.Closer
	// codec is the goavro codec
	codec *goavro.Codec
	// codecName is the codec name
	codecName string
	// syncMarker is the sync marker for this file
	syncMarker []byte
	// config is the writer configuration
	config OCFWriterConfig
	// logger is used for logging
	logger logging.Logger
	// headerWritten indicates if the header has been written
	headerWritten bool
	// bufferObjects is a buffer for objects before writing a block
	bufferObjects []interface{}
	// mutex protects concurrent access
	mutex sync.Mutex
}

// NewOCFWriter creates a new OCF writer
func NewOCFWriter(w io.Writer, cfg config.OCFWriterConfig, logger logging.Logger) (OCFWriter, error) {
	// Initialize configuration
	config := OCFWriterConfig{
		BufferSize: cfg.BufferSize,
		BlockSize:  cfg.BlockSize,
	}

	// Use default values if not specified
	if config.BufferSize <= 0 {
		config.BufferSize = DefaultOCFWriterConfig().BufferSize
	}
	if config.BlockSize <= 0 {
		config.BlockSize = DefaultOCFWriterConfig().BlockSize
	}

	// Create buffered writer
	bw := bufio.NewWriterSize(w, config.BufferSize)

	// Create the writer
	writer := &OCFWriterImpl{
		writer:         w,
		bufferedWriter: bw,
		config:         config,
		logger:         logger,
		bufferObjects:  make([]interface{}, 0, config.BlockSize),
	}

	// If the writer implements io.Closer, store it
	if closer, ok := w.(io.Closer); ok {
		writer.closer = closer
	}

	// Generate random sync marker
	writer.syncMarker = make([]byte, OCFSyncMarkerSize)
	if _, err := io.ReadFull(rand.Reader, writer.syncMarker); err != nil {
		return nil, errors.Wrap(err, "failed to generate sync marker")
	}

	return writer, nil
}

// WriteHeader writes the OCF header
func (w *OCFWriterImpl) WriteHeader(schema string, codec string, metadata map[string][]byte) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Check if header has already been written
	if w.headerWritten {
		return errors.New("header already written")
	}

	// Validate codec
	switch codec {
	case CodecNull, CodecDeflate, CodecSnappy, CodecZlib:
		// Supported codec
	default:
		return errors.Errorf("unsupported codec: %s", codec)
	}

	// Create goavro codec
	goavroCodec, err := goavro.NewCodec(schema)
	if err != nil {
		return errors.Wrap(err, "failed to create goavro codec")
	}
	w.codec = goavroCodec
	w.codecName = codec

	// Validate schema
	var schemaObj interface{}
	if err := json.Unmarshal([]byte(schema), &schemaObj); err != nil {
		return errors.Wrap(err, "invalid schema JSON")
	}

	// Write magic bytes
	if _, err := w.bufferedWriter.WriteString(OCFMagicBytes); err != nil {
		return errors.Wrap(err, "failed to write magic bytes")
	}

	// Prepare metadata
	if metadata == nil {
		metadata = make(map[string][]byte)
	}
	metadata[OCFMetadataSchema] = []byte(schema)
	metadata[OCFMetadataCodec] = []byte(codec)

	// Write metadata count
	if err := binary.Write(w.bufferedWriter, binary.BigEndian, int64(len(metadata))); err != nil {
		return errors.Wrap(err, "failed to write metadata count")
	}

	// Write metadata pairs
	for key, value := range metadata {
		// Write key
		if err := binary.Write(w.bufferedWriter, binary.BigEndian, int64(len(key))); err != nil {
			return errors.Wrap(err, "failed to write metadata key length")
		}
		if _, err := w.bufferedWriter.WriteString(key); err != nil {
			return errors.Wrap(err, "failed to write metadata key")
		}

		// Write value
		if err := binary.Write(w.bufferedWriter, binary.BigEndian, int64(len(value))); err != nil {
			return errors.Wrap(err, "failed to write metadata value length")
		}
		if _, err := w.bufferedWriter.Write(value); err != nil {
			return errors.Wrap(err, "failed to write metadata value")
		}
	}

	// Write sync marker
	if _, err := w.bufferedWriter.Write(w.syncMarker); err != nil {
		return errors.Wrap(err, "failed to write sync marker")
	}

	// Mark header as written
	w.headerWritten = true

	w.logger.Debug("Wrote OCF header",
		"codec", codec,
		"sync_marker", fmt.Sprintf("%x", w.syncMarker))

	return nil
}

// WriteBlock writes a block of data
func (w *OCFWriterImpl) WriteBlock(objects []interface{}) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Ensure header has been written
	if !w.headerWritten {
		return errors.New("header not written")
	}

	// Add objects to buffer
	w.bufferObjects = append(w.bufferObjects, objects...)

	// If buffer is full, write a block
	if len(w.bufferObjects) >= w.config.BlockSize {
		if err := w.flushBufferLocked(); err != nil {
			return err
		}
	}

	return nil
}

// flushBufferLocked flushes the buffer to a new block (must be called with mutex held)
func (w *OCFWriterImpl) flushBufferLocked() error {
	// Skip if buffer is empty
	if len(w.bufferObjects) == 0 {
		return nil
	}

	// Encode objects
	var encodedData bytes.Buffer
	for _, obj := range w.bufferObjects {
		binary, err := w.codec.BinaryFromNative(nil, obj)
		if err != nil {
			return errors.Wrap(err, "failed to encode object")
		}
		if _, err := encodedData.Write(binary); err != nil {
			return errors.Wrap(err, "failed to write encoded object")
		}
	}

	// Compress data if necessary
	var compressedData []byte
	switch w.codecName {
	case CodecNull:
		compressedData = encodedData.Bytes()
	case CodecDeflate:
		var compressed bytes.Buffer
		flateWriter, err := flate.NewWriter(&compressed, flate.DefaultCompression)
		if err != nil {
			return errors.Wrap(err, "failed to create deflate writer")
		}
		if _, err := flateWriter.Write(encodedData.Bytes()); err != nil {
			return errors.Wrap(err, "failed to compress data")
		}
		if err := flateWriter.Close(); err != nil {
			return errors.Wrap(err, "failed to close deflate writer")
		}
		compressedData = compressed.Bytes()
	case CodecSnappy:
		compressed := snappy.Encode(nil, encodedData.Bytes())
		compressedData = compressed
	case CodecZlib:
		var compressed bytes.Buffer
		zlibWriter := zlib.NewWriter(&compressed)
		if _, err := zlibWriter.Write(encodedData.Bytes()); err != nil {
			return errors.Wrap(err, "failed to compress data")
		}
		if err := zlibWriter.Close(); err != nil {
			return errors.Wrap(err, "failed to close zlib writer")
		}
		compressedData = compressed.Bytes()
	default:
		return errors.Errorf("unsupported codec: %s", w.codecName)
	}

	// Write block count
	if err := binary.Write(w.bufferedWriter, binary.BigEndian, int64(len(w.bufferObjects))); err != nil {
		return errors.Wrap(err, "failed to write block count")
	}

	// Write block size
	if err := binary.Write(w.bufferedWriter, binary.BigEndian, int64(len(compressedData))); err != nil {
		return errors.Wrap(err, "failed to write block size")
	}

	// Write block data
	if _, err := w.bufferedWriter.Write(compressedData); err != nil {
		return errors.Wrap(err, "failed to write block data")
	}

	// Write sync marker
	if _, err := w.bufferedWriter.Write(w.syncMarker); err != nil {
		return errors.Wrap(err, "failed to write sync marker")
	}

	// Clear buffer
	w.bufferObjects = w.bufferObjects[:0]

	return nil
}

// Flush flushes any buffered data
func (w *OCFWriterImpl) Flush() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Flush any remaining objects
	if err := w.flushBufferLocked(); err != nil {
		return err
	}

	// Flush the buffered writer
	return w.bufferedWriter.Flush()
}

// Close closes the writer
func (w *OCFWriterImpl) Close() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Flush any remaining objects
	if err := w.flushBufferLocked(); err != nil {
		return err
	}

	// Write empty block to indicate end of file
	if err := binary.Write(w.bufferedWriter, binary.BigEndian, int64(0)); err != nil {
		return errors.Wrap(err, "failed to write empty block count")
	}

	// Write sync marker
	if _, err := w.bufferedWriter.Write(w.syncMarker); err != nil {
		return errors.Wrap(err, "failed to write final sync marker")
	}

	// Flush the buffered writer
	if err := w.bufferedWriter.Flush(); err != nil {
		return errors.Wrap(err, "failed to flush writer")
	}

	// Close underlying writer if it implements io.Closer
	if w.closer != nil {
		return w.closer.Close()
	}

	return nil
}

// ExtractSchemaFromOCF extracts the schema from an OCF file
func ExtractSchemaFromOCF(data []byte) (string, error) {
	// Create a reader
	reader := bytes.NewReader(data)

	// Read magic bytes
	magic := make([]byte, len(OCFMagicBytes))
	n, err := io.ReadFull(reader, magic)
	if err != nil {
		return "", errors.Wrap(err, "failed to read magic bytes")
	}
	if n != len(OCFMagicBytes) || string(magic) != OCFMagicBytes {
		return "", errors.New("invalid OCF magic bytes")
	}

	// Read metadata count
	var count int64
	if err := binary.Read(reader, binary.BigEndian, &count); err != nil {
		return "", errors.Wrap(err, "failed to read metadata count")
	}

	// Validate count to prevent DoS attacks
	if count < 0 || count > 1000 {
		return "", errors.Errorf("invalid metadata count: %d", count)
	}

	// Read metadata pairs
	for i := int64(0); i < count; i++ {
		// Read key length
		var keyLen int64
		if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
			return "", errors.Wrap(err, "failed to read metadata key length")
		}
		if keyLen < 0 || keyLen > 1024 {
			return "", errors.Errorf("invalid metadata key length: %d", keyLen)
		}

		// Read key
		key := make([]byte, keyLen)
		n, err := io.ReadFull(reader, key)
		if err != nil {
			return "", errors.Wrap(err, "failed to read metadata key")
		}
		if int64(n) != keyLen {
			return "", errors.New("incomplete metadata key")
		}

		// Read value length
		var valueLen int64
		if err := binary.Read(reader, binary.BigEndian, &valueLen); err != nil {
			return "", errors.Wrap(err, "failed to read metadata value length")
		}
		if valueLen < 0 || valueLen > 1024*1024 {
			return "", errors.Errorf("invalid metadata value length: %d", valueLen)
		}

		// Read value
		value := make([]byte, valueLen)
		n, err = io.ReadFull(reader, value)
		if err != nil {
			return "", errors.Wrap(err, "failed to read metadata value")
		}
		if int64(n) != valueLen {
			return "", errors.New("incomplete metadata value")
		}

		// Check if this is the schema
		if string(key) == OCFMetadataSchema {
			return string(value), nil
		}
	}

	return "", errors.New("schema not found in OCF metadata")
}

// IsOCFFile checks if the given data is an OCF file
func IsOCFFile(data []byte) bool {
	if len(data) < len(OCFMagicBytes) {
		return false
	}
	return string(data[:len(OCFMagicBytes)]) == OCFMagicBytes
}

//Personal.AI order the ending
