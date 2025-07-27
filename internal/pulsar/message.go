// Package pulsar provides functionality for interacting with Apache Pulsar.
package pulsar

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/turtacn/staravail/internal/avro"
	"github.com/turtacn/staravail/internal/common/logging"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/message"
)

// MessageFormat represents the format of a message
type MessageFormat string

const (
	// MessageFormatUnknown represents an unknown message format
	MessageFormatUnknown MessageFormat = "unknown"

	// MessageFormatAvro represents an Avro message format
	MessageFormatAvro MessageFormat = "avro"

	// MessageFormatJSON represents a JSON message format
	MessageFormatJSON MessageFormat = "json"

	// MessageFormatRaw represents a raw binary message format
	MessageFormatRaw MessageFormat = "raw"

	// MessageFormatProtobuf represents a Protobuf message format
	MessageFormatProtobuf MessageFormat = "protobuf"

	// MessageFormatString represents a plain text string message format
	MessageFormatString MessageFormat = "string"
)

// MessageFormatDetectionResult contains the result of format detection
type MessageFormatDetectionResult struct {
	// Format is the detected format
	Format MessageFormat

	// Confidence is the confidence level (0-1)
	Confidence float64

	// SchemaID is the detected schema ID (for Avro)
	SchemaID int

	// SchemaVersion is the detected schema version
	SchemaVersion string

	// ContentType is the content type if available
	ContentType string

	// DetectionMethod is the method used for detection
	DetectionMethod string
}

// HandlerType represents the type of a message handler
type HandlerType string

const (
	// HandlerTypeAvro represents an Avro message handler
	HandlerTypeAvro HandlerType = "avro"

	// HandlerTypeJSON represents a JSON message handler
	HandlerTypeJSON HandlerType = "json"

	// HandlerTypeRaw represents a raw binary message handler
	HandlerTypeRaw HandlerType = "raw"

	// HandlerTypeProtobuf represents a Protobuf message handler
	HandlerTypeProtobuf HandlerType = "protobuf"

	// HandlerTypeGeneric represents a generic message handler
	HandlerTypeGeneric HandlerType = "generic"
)

// MessageHandlerContext contains context for message handling
type MessageHandlerContext struct {
	// Context is the base context
	Context context.Context

	// Message is the message being handled
	Message Message

	// DetectionResult is the result of format detection
	DetectionResult MessageFormatDetectionResult

	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder

	// HandlerOptions contains options for the handler
	HandlerOptions MessageHandlerOptions

	// StartTime is when handling started
	StartTime time.Time

	// Custom fields can be added during processing
	Custom map[string]interface{}
}

// NewMessageHandlerContext creates a new message handler context
func NewMessageHandlerContext(
	ctx context.Context,
	msg Message,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
	options MessageHandlerOptions,
) *MessageHandlerContext {
	return &MessageHandlerContext{
		Context:        ctx,
		Message:        msg,
		Logger:         logger,
		Metrics:        metricsRecorder,
		HandlerOptions: options,
		StartTime:      time.Now(),
		Custom:         make(map[string]interface{}),
	}
}

// MessageHandler is the interface for a message handler
type MessageHandler interface {
	// HandleMessage handles a message
	HandleMessage(ctx *MessageHandlerContext) (*message.Data, error)

	// GetHandlerType returns the type of the handler
	GetHandlerType() HandlerType

	// CanHandle returns whether this handler can handle the message
	CanHandle(ctx *MessageHandlerContext) bool

	// Name returns the name of the handler
	Name() string
}

// MessageValidator is the interface for a message validator
type MessageValidator interface {
	// Validate validates a message
	Validate(ctx *MessageHandlerContext) error

	// Name returns the name of the validator
	Name() string
}

// MessageFilter is the interface for a message filter
type MessageFilter interface {
	// Filter returns true if the message should be processed
	Filter(ctx *MessageHandlerContext) bool

	// Name returns the name of the filter
	Name() string
}

// MessageConverter is the interface for a message converter
type MessageConverter interface {
	// Convert converts a message to the desired format
	Convert(ctx *MessageHandlerContext, targetFormat MessageFormat) ([]byte, error)

	// Name returns the name of the converter
	Name() string
}

// MessageHandlerOptions contains options for message handlers
type MessageHandlerOptions struct {
	// EnableSchemaResolution enables schema resolution for Avro messages
	EnableSchemaResolution bool

	// EnableFormatDetection enables automatic format detection
	EnableFormatDetection bool

	// DefaultFormat is the default format to use if detection fails
	DefaultFormat MessageFormat

	// SchemaRegistryURL is the URL of the schema registry
	SchemaRegistryURL string

	// SchemaNameStrategy is the strategy to derive schema name
	SchemaNameStrategy string

	// EnableValidation enables message validation
	EnableValidation bool

	// Validators is a list of validators to use
	Validators []MessageValidator

	// Filters is a list of filters to use
	Filters []MessageFilter

	// Converters is a map of converters to use
	Converters map[MessageFormat]MessageConverter

	// StrictTypeMapping enforces strict type mapping
	StrictTypeMapping bool

	// CustomDecoders is a map of custom decoders for specific formats
	CustomDecoders map[string]interface{}

	// MaxMessageSize is the maximum allowed message size
	MaxMessageSize int64

	// DefaultTimestampField is the default field for timestamps
	DefaultTimestampField string

	// RecordProcessTimeout is the timeout for processing a record
	RecordProcessTimeout time.Duration
}

// DefaultMessageHandlerOptions returns default message handler options
func DefaultMessageHandlerOptions() MessageHandlerOptions {
	return MessageHandlerOptions{
		EnableSchemaResolution: true,
		EnableFormatDetection:  true,
		DefaultFormat:          MessageFormatJSON,
		EnableValidation:       true,
		Validators:             []MessageValidator{},
		Filters:                []MessageFilter{},
		Converters:             make(map[MessageFormat]MessageConverter),
		StrictTypeMapping:      false,
		CustomDecoders:         make(map[string]interface{}),
		MaxMessageSize:         10 * 1024 * 1024, // 10MB
		DefaultTimestampField:  "timestamp",
		RecordProcessTimeout:   30 * time.Second,
	}
}

// BaseMessageHandler provides common functionality for message handlers
type BaseMessageHandler struct {
	// HandlerType is the type of the handler
	HandlerType HandlerType

	// HandlerName is the name of the handler
	HandlerName string

	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder

	// SchemaResolver is for resolving schemas
	SchemaResolver *avro.SchemaResolver

	// Options contains options for the handler
	Options MessageHandlerOptions
}

// NewBaseMessageHandler creates a new base message handler
func NewBaseMessageHandler(
	handlerType HandlerType,
	handlerName string,
	logger logging.Logger,
	metrics metrics.MetricsRecorder,
	options MessageHandlerOptions,
) *BaseMessageHandler {
	var schemaResolver *avro.SchemaResolver
	if options.EnableSchemaResolution && options.SchemaRegistryURL != "" {
		schemaResolver = avro.NewSchemaResolver(options.SchemaRegistryURL, logger)
	}

	return &BaseMessageHandler{
		HandlerType:    handlerType,
		HandlerName:    handlerName,
		Logger:         logger,
		Metrics:        metrics,
		SchemaResolver: schemaResolver,
		Options:        options,
	}
}

// GetHandlerType returns the type of the handler
func (h *BaseMessageHandler) GetHandlerType() HandlerType {
	return h.HandlerType
}

// Name returns the name of the handler
func (h *BaseMessageHandler) Name() string {
	return h.HandlerName
}

// ValidateMessage runs all validators on a message
func (h *BaseMessageHandler) ValidateMessage(ctx *MessageHandlerContext) error {
	if !h.Options.EnableValidation {
		return nil
	}

	for _, validator := range h.Options.Validators {
		if err := validator.Validate(ctx); err != nil {
			h.Logger.Warn("Message validation failed",
				"validator", validator.Name(),
				"error", err)

			// Record metrics
			if h.Metrics != nil {
				h.Metrics.CounterInc("message_validation_failures", map[string]string{
					"validator": validator.Name(),
					"handler":   h.HandlerName,
				})
			}

			return errors.Wrapf(err, "validation failed: %s", validator.Name())
		}
	}

	return nil
}

// FilterMessage runs all filters on a message
func (h *BaseMessageHandler) FilterMessage(ctx *MessageHandlerContext) bool {
	for _, filter := range h.Options.Filters {
		if !filter.Filter(ctx) {
			h.Logger.Debug("Message filtered out",
				"filter", filter.Name())

			// Record metrics
			if h.Metrics != nil {
				h.Metrics.CounterInc("message_filtered", map[string]string{
					"filter":  filter.Name(),
					"handler": h.HandlerName,
				})
			}

			return false
		}
	}

	return true
}

// DetectMessageFormat detects the format of a message
func (h *BaseMessageHandler) DetectMessageFormat(ctx *MessageHandlerContext) MessageFormatDetectionResult {
	msg := ctx.Message
	payload := msg.Payload()

	result := MessageFormatDetectionResult{
		Format:          MessageFormatUnknown,
		Confidence:      0.0,
		DetectionMethod: "none",
	}

	// Check if too small for detection
	if len(payload) < 5 {
		result.Format = MessageFormatRaw
		result.Confidence = 0.6
		result.DetectionMethod = "size"
		return result
	}

	// First, check content-type property if available
	if contentType, ok := msg.Properties()["content-type"]; ok {
		result.ContentType = contentType

		// Check for common content types
		contentType = strings.ToLower(contentType)
		if strings.Contains(contentType, "avro") {
			result.Format = MessageFormatAvro
			result.Confidence = 0.9
			result.DetectionMethod = "content-type"
			return result
		} else if strings.Contains(contentType, "json") {
			result.Format = MessageFormatJSON
			result.Confidence = 0.9
			result.DetectionMethod = "content-type"
			return result
		} else if strings.Contains(contentType, "proto") {
			result.Format = MessageFormatProtobuf
			result.Confidence = 0.9
			result.DetectionMethod = "content-type"
			return result
		} else if strings.Contains(contentType, "text") {
			// Try to determine if it's JSON or plain text
			if isJSON(payload) {
				result.Format = MessageFormatJSON
				result.Confidence = 0.8
				result.DetectionMethod = "content-type+pattern"
			} else {
				result.Format = MessageFormatString
				result.Confidence = 0.8
				result.DetectionMethod = "content-type"
			}
			return result
		}
	}

	// Check for Avro magic byte (first byte is 0)
	if payload[0] == 0 {
		// Check if long enough to contain schema ID
		if len(payload) >= 5 {
			// Extract schema ID
			schemaID := int(binary.BigEndian.Uint32(payload[1:5]))
			result.Format = MessageFormatAvro
			result.Confidence = 0.95
			result.SchemaID = schemaID
			result.DetectionMethod = "magic-byte"
			return result
		}
	}

	// Check if it's valid JSON
	if isJSON(payload) {
		result.Format = MessageFormatJSON
		result.Confidence = 0.9
		result.DetectionMethod = "pattern"
		return result
	}

	// Check if it's likely a string
	if isLikelyString(payload) {
		result.Format = MessageFormatString
		result.Confidence = 0.7
		result.DetectionMethod = "pattern"
		return result
	}

	// Check for specific schema version property
	if schemaVersion, ok := msg.Properties()["schema-version"]; ok {
		result.SchemaVersion = schemaVersion
		// If we have a schema version, it's likely Avro or Protobuf
		// but we don't know which one for sure
		result.Format = MessageFormatAvro // assumption
		result.Confidence = 0.6
		result.DetectionMethod = "schema-version"
		return result
	}

	// Default to raw format
	result.Format = h.Options.DefaultFormat
	result.Confidence = 0.5
	result.DetectionMethod = "default"
	return result
}

// isJSON checks if data is valid JSON
func isJSON(data []byte) bool {
	var js json.RawMessage
	return json.Unmarshal(data, &js) == nil
}

// isLikelyString checks if data is likely a string
func isLikelyString(data []byte) bool {
	// Check if data is printable ASCII
	printable := true
	for _, b := range data {
		if b < 32 || b > 126 {
			printable = false
			break
		}
	}

	return printable
}

// AvroMessageHandler implements MessageHandler for Avro messages
type AvroMessageHandler struct {
	*BaseMessageHandler

	// AvroConverter is for converting Avro data
	AvroConverter *avro.Converter

	// SchemaCache is a cache for Avro schemas
	SchemaCache sync.Map
}

// NewAvroMessageHandler creates a new Avro message handler
func NewAvroMessageHandler(
	logger logging.Logger,
	metrics metrics.MetricsRecorder,
	options MessageHandlerOptions,
) *AvroMessageHandler {
	base := NewBaseMessageHandler(
		HandlerTypeAvro,
		"AvroMessageHandler",
		logger,
		metrics,
		options,
	)

	avroConverter := avro.NewConverter(options.SchemaRegistryURL, logger)

	return &AvroMessageHandler{
		BaseMessageHandler: base,
		AvroConverter:      avroConverter,
		SchemaCache:        sync.Map{},
	}
}

// CanHandle returns whether this handler can handle the message
func (h *AvroMessageHandler) CanHandle(ctx *MessageHandlerContext) bool {
	// If detection result is available, use it
	if ctx.DetectionResult.Format != MessageFormatUnknown {
		return ctx.DetectionResult.Format == MessageFormatAvro
	}

	// Otherwise, detect format
	if h.Options.EnableFormatDetection {
		detectionResult := h.DetectMessageFormat(ctx)
		ctx.DetectionResult = detectionResult
		return detectionResult.Format == MessageFormatAvro
	}

	// Default based on handler type
	return true
}

// HandleMessage handles an Avro message
func (h *AvroMessageHandler) HandleMessage(ctx *MessageHandlerContext) (*message.Data, error) {
	// Validate message
	if err := h.ValidateMessage(ctx); err != nil {
		return nil, errors.Wrap(err, "validation failed")
	}

	// Filter message
	if !h.FilterMessage(ctx) {
		return nil, errors.New("message filtered out")
	}

	// Record metrics for message size
	if h.Metrics != nil {
		h.Metrics.HistogramObserve("message_size_bytes", float64(len(ctx.Message.Payload())), map[string]string{
			"handler": h.HandlerName,
			"format":  string(MessageFormatAvro),
		})
	}

	// Get message payload
	payload := ctx.Message.Payload()

	// Extract schema ID
	if len(payload) < 5 {
		return nil, errors.New("invalid Avro message: too short")
	}

	// Verify magic byte
	if payload[0] != 0 {
		return nil, errors.New("invalid Avro message: missing magic byte")
	}

	// Extract schema ID
	schemaID := int(binary.BigEndian.Uint32(payload[1:5]))
	ctx.DetectionResult.SchemaID = schemaID

	// Decode Avro data
	var decoded interface{}
	var err error

	// Use cached codec if available
	codecKey := fmt.Sprintf("schema-%d", schemaID)
	codecVal, found := h.SchemaCache.Load(codecKey)

	if found {
		// Use cached codec
		codec := codecVal.(*goavro.Codec)
		native, _, err := codec.NativeFromBinary(payload[5:])
		if err != nil {
			h.Logger.Error("Failed to decode Avro data with cached codec",
				"schemaID", schemaID,
				"error", err)

			// Record metrics
			if h.Metrics != nil {
				h.Metrics.CounterInc("avro_decode_failures", map[string]string{
					"schemaID": fmt.Sprintf("%d", schemaID),
					"reason":   "codec_error",
				})
			}

			return nil, errors.Wrap(err, "failed to decode Avro data")
		}

		decoded = native
	} else if h.AvroConverter != nil {
		// Use converter to decode
		decoded, err = h.AvroConverter.AvroToMap(payload)
		if err != nil {
			h.Logger.Error("Failed to convert Avro to map",
				"schemaID", schemaID,
				"error", err)

			// Record metrics
			if h.Metrics != nil {
				h.Metrics.CounterInc("avro_decode_failures", map[string]string{
					"schemaID": fmt.Sprintf("%d", schemaID),
					"reason":   "converter_error",
				})
			}

			return nil, errors.Wrap(err, "failed to convert Avro to map")
		}

		// Cache the codec for future use
		if h.AvroConverter.GetCodec(schemaID) != nil {
			h.SchemaCache.Store(codecKey, h.AvroConverter.GetCodec(schemaID))
		}
	} else {
		// Fallback to schema resolver if available
		if h.SchemaResolver == nil {
			return nil, errors.New("no Avro converter or schema resolver available")
		}

		schema, err := h.SchemaResolver.GetSchema(schemaID)
		if err != nil {
			h.Logger.Error("Failed to resolve schema",
				"schemaID", schemaID,
				"error", err)

			// Record metrics
			if h.Metrics != nil {
				h.Metrics.CounterInc("schema_resolution_failures", map[string]string{
					"schemaID": fmt.Sprintf("%d", schemaID),
				})
			}

			return nil, errors.Wrap(err, "failed to resolve schema")
		}

		// Create codec
		codec, err := goavro.NewCodec(schema)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create Avro codec")
		}

		// Decode data
		native, _, err := codec.NativeFromBinary(payload[5:])
		if err != nil {
			return nil, errors.Wrap(err, "failed to decode Avro data")
		}

		decoded = native

		// Cache the codec for future use
		h.SchemaCache.Store(codecKey, codec)
	}

	// Convert decoded data to map if it's not already
	var dataMap map[string]interface{}

	switch v := decoded.(type) {
	case map[string]interface{}:
		dataMap = v
	case map[string]string:
		dataMap = make(map[string]interface{})
		for k, v := range v {
			dataMap[k] = v
		}
	case map[string]float64:
		dataMap = make(map[string]interface{})
		for k, v := range v {
			dataMap[k] = v
		}
	case map[string]int:
		dataMap = make(map[string]interface{})
		for k, v := range v {
			dataMap[k] = v
		}
	case map[string]bool:
		dataMap = make(map[string]interface{})
		for k, v := range v {
			dataMap[k] = v
		}
	default:
		// Try to convert to JSON and back to get a map
		jsonBytes, err := json.Marshal(decoded)
		if err != nil {
			return nil, errors.Wrap(err, "failed to convert decoded data to map: JSON marshaling failed")
		}

		if err := json.Unmarshal(jsonBytes, &dataMap); err != nil {
			return nil, errors.Wrap(err, "failed to convert decoded data to map: JSON unmarshaling failed")
		}
	}

	// Create message data
	data := &message.Data{
		Payload:     ctx.Message.Payload(),
		Properties:  ctx.Message.Properties(),
		PublishTime: ctx.Message.PublishTime(),
		EventTime:   ctx.Message.EventTime(),
		Key:         ctx.Message.Key(),
		OrderingKey: ctx.Message.OrderingKey(),
		MessageID:   string(ctx.Message.ID()),
		Topic:       ctx.Message.Topic(),
		SchemaID:    schemaID,
		Format:      string(MessageFormatAvro),
		Data:        dataMap,
	}

	// Add schema version if available
	if ctx.DetectionResult.SchemaVersion != "" {
		data.SchemaVersion = ctx.DetectionResult.SchemaVersion
	}

	// Record metrics
	if h.Metrics != nil {
		h.Metrics.CounterInc("messages_processed", map[string]string{
			"handler": h.HandlerName,
			"format":  string(MessageFormatAvro),
		})
		h.Metrics.HistogramObserve("message_processing_time_ms",
			float64(time.Since(ctx.StartTime).Milliseconds()),
			map[string]string{
				"handler": h.HandlerName,
				"format":  string(MessageFormatAvro),
			})
	}

	h.Logger.Debug("Successfully processed Avro message",
		"schemaID", schemaID,
		"topic", ctx.Message.Topic(),
		"messageID", ctx.Message.ID())

	return data, nil
}

// JSONMessageHandler implements MessageHandler for JSON messages
type JSONMessageHandler struct {
	*BaseMessageHandler
}

// NewJSONMessageHandler creates a new JSON message handler
func NewJSONMessageHandler(
	logger logging.Logger,
	metrics metrics.MetricsRecorder,
	options MessageHandlerOptions,
) *JSONMessageHandler {
	base := NewBaseMessageHandler(
		HandlerTypeJSON,
		"JSONMessageHandler",
		logger,
		metrics,
		options,
	)

	return &JSONMessageHandler{
		BaseMessageHandler: base,
	}
}

// CanHandle returns whether this handler can handle the message
func (h *JSONMessageHandler) CanHandle(ctx *MessageHandlerContext) bool {
	// If detection result is available, use it
	if ctx.DetectionResult.Format != MessageFormatUnknown {
		return ctx.DetectionResult.Format == MessageFormatJSON
	}

	// Otherwise, detect format
	if h.Options.EnableFormatDetection {
		detectionResult := h.DetectMessageFormat(ctx)
		ctx.DetectionResult = detectionResult
		return detectionResult.Format == MessageFormatJSON
	}

	// Default based on handler type
	return true
}

// HandleMessage handles a JSON message
func (h *JSONMessageHandler) HandleMessage(ctx *MessageHandlerContext) (*message.Data, error) {
	// Validate message
	if err := h.ValidateMessage(ctx); err != nil {
		return nil, errors.Wrap(err, "validation failed")
	}

	// Filter message
	if !h.FilterMessage(ctx) {
		return nil, errors.New("message filtered out")
	}

	// Record metrics for message size
	if h.Metrics != nil {
		h.Metrics.HistogramObserve("message_size_bytes", float64(len(ctx.Message.Payload())), map[string]string{
			"handler": h.HandlerName,
			"format":  string(MessageFormatJSON),
		})
	}

	// Get message payload
	payload := ctx.Message.Payload()

	// Decode JSON data
	var dataMap map[string]interface{}
	if err := json.Unmarshal(payload, &dataMap); err != nil {
		// Try unmarshaling as JSON array and convert to map
		var dataArray []interface{}
		if jsonErr := json.Unmarshal(payload, &dataArray); jsonErr == nil {
			// Create a map with an "items" field containing the array
			dataMap = map[string]interface{}{
				"items": dataArray,
			}
		} else {
			h.Logger.Error("Failed to decode JSON data",
				"error", err,
				"payload", string(payload[:min(100, len(payload))]))

			// Record metrics
			if h.Metrics != nil {
				h.Metrics.CounterInc("json_decode_failures", map[string]string{
					"reason": "unmarshall_error",
				})
			}

			return nil, errors.Wrap(err, "failed to decode JSON data")
		}
	}

	// Create message data
	data := &message.Data{
		Payload:     ctx.Message.Payload(),
		Properties:  ctx.Message.Properties(),
		PublishTime: ctx.Message.PublishTime(),
		EventTime:   ctx.Message.EventTime(),
		Key:         ctx.Message.Key(),
		OrderingKey: ctx.Message.OrderingKey(),
		MessageID:   string(ctx.Message.ID()),
		Topic:       ctx.Message.Topic(),
		Format:      string(MessageFormatJSON),
		Data:        dataMap,
	}

	// Record metrics
	if h.Metrics != nil {
		h.Metrics.CounterInc("messages_processed", map[string]string{
			"handler": h.HandlerName,
			"format":  string(MessageFormatJSON),
		})
		h.Metrics.HistogramObserve("message_processing_time_ms",
			float64(time.Since(ctx.StartTime).Milliseconds()),
			map[string]string{
				"handler": h.HandlerName,
				"format":  string(MessageFormatJSON),
			})
	}

	h.Logger.Debug("Successfully processed JSON message",
		"topic", ctx.Message.Topic(),
		"messageID", ctx.Message.ID())

	return data, nil
}

// RawMessageHandler implements MessageHandler for raw binary messages
type RawMessageHandler struct {
	*BaseMessageHandler
}

// NewRawMessageHandler creates a new raw message handler
func NewRawMessageHandler(
	logger logging.Logger,
	metrics metrics.MetricsRecorder,
	options MessageHandlerOptions,
) *RawMessageHandler {
	base := NewBaseMessageHandler(
		HandlerTypeRaw,
		"RawMessageHandler",
		logger,
		metrics,
		options,
	)

	return &RawMessageHandler{
		BaseMessageHandler: base,
	}
}

// CanHandle returns whether this handler can handle the message
func (h *RawMessageHandler) CanHandle(ctx *MessageHandlerContext) bool {
	// Raw handler is a fallback handler that can handle any message
	return true
}

// HandleMessage handles a raw message
func (h *RawMessageHandler) HandleMessage(ctx *MessageHandlerContext) (*message.Data, error) {
	// Validate message
	if err := h.ValidateMessage(ctx); err != nil {
		return nil, errors.Wrap(err, "validation failed")
	}

	// Filter message
	if !h.FilterMessage(ctx) {
		return nil, errors.New("message filtered out")
	}

	// Record metrics for message size
	if h.Metrics != nil {
		h.Metrics.HistogramObserve("message_size_bytes", float64(len(ctx.Message.Payload())), map[string]string{
			"handler": h.HandlerName,
			"format":  string(MessageFormatRaw),
		})
	}

	// Create message data with raw payload
	data := &message.Data{
		Payload:     ctx.Message.Payload(),
		Properties:  ctx.Message.Properties(),
		PublishTime: ctx.Message.PublishTime(),
		EventTime:   ctx.Message.EventTime(),
		Key:         ctx.Message.Key(),
		OrderingKey: ctx.Message.OrderingKey(),
		MessageID:   string(ctx.Message.ID()),
		Topic:       ctx.Message.Topic(),
		Format:      string(MessageFormatRaw),
		// No structured data for raw messages
	}

	// Record metrics
	if h.Metrics != nil {
		h.Metrics.CounterInc("messages_processed", map[string]string{
			"handler": h.HandlerName,
			"format":  string(MessageFormatRaw),
		})
		h.Metrics.HistogramObserve("message_processing_time_ms",
			float64(time.Since(ctx.StartTime).Milliseconds()),
			map[string]string{
				"handler": h.HandlerName,
				"format":  string(MessageFormatRaw),
			})
	}

	h.Logger.Debug("Successfully processed raw message",
		"topic", ctx.Message.Topic(),
		"messageID", ctx.Message.ID(),
		"size", len(ctx.Message.Payload()))

	return data, nil
}

// MessageSizeValidator validates message size
type MessageSizeValidator struct {
	// MaxSize is the maximum allowed size
	MaxSize int64
}

// NewMessageSizeValidator creates a new message size validator
func NewMessageSizeValidator(maxSize int64) *MessageSizeValidator {
	return &MessageSizeValidator{
		MaxSize: maxSize,
	}
}

// Validate validates message size
func (v *MessageSizeValidator) Validate(ctx *MessageHandlerContext) error {
	if int64(len(ctx.Message.Payload())) > v.MaxSize {
		return fmt.Errorf("message size exceeds maximum allowed size of %d bytes", v.MaxSize)
	}
	return nil
}

// Name returns the name of the validator
func (v *MessageSizeValidator) Name() string {
	return "MessageSizeValidator"
}

// EmptyMessageFilter filters out empty messages
type EmptyMessageFilter struct{}

// NewEmptyMessageFilter creates a new empty message filter
func NewEmptyMessageFilter() *EmptyMessageFilter {
	return &EmptyMessageFilter{}
}

// Filter returns true if the message should be processed
func (f *EmptyMessageFilter) Filter(ctx *MessageHandlerContext) bool {
	return len(ctx.Message.Payload()) > 0
}

// Name returns the name of the filter
func (f *EmptyMessageFilter) Name() string {
	return "EmptyMessageFilter"
}

// TopicFilter filters messages based on topic
type TopicFilter struct {
	// AllowedTopics is a list of allowed topics
	AllowedTopics []string

	// BlockedTopics is a list of blocked topics
	BlockedTopics []string
}

// NewTopicFilter creates a new topic filter
func NewTopicFilter(allowedTopics, blockedTopics []string) *TopicFilter {
	return &TopicFilter{
		AllowedTopics: allowedTopics,
		BlockedTopics: blockedTopics,
	}
}

// Filter returns true if the message should be processed
func (f *TopicFilter) Filter(ctx *MessageHandlerContext) bool {
	topic := ctx.Message.Topic()

	// Check if topic is blocked
	for _, blockedTopic := range f.BlockedTopics {
		if blockedTopic == topic || (strings.HasSuffix(blockedTopic, "*") &&
			strings.HasPrefix(topic, blockedTopic[:len(blockedTopic)-1])) {
			return false
		}
	}

	// If no allowed topics specified, allow all
	if len(f.AllowedTopics) == 0 {
		return true
	}

	// Check if topic is allowed
	for _, allowedTopic := range f.AllowedTopics {
		if allowedTopic == topic || (strings.HasSuffix(allowedTopic, "*") &&
			strings.HasPrefix(topic, allowedTopic[:len(allowedTopic)-1])) {
			return true
		}
	}

	return false
}

// Name returns the name of the filter
func (f *TopicFilter) Name() string {
	return "TopicFilter"
}

// JSONToAvroConverter converts JSON to Avro
type JSONToAvroConverter struct {
	// AvroConverter is for converting to Avro
	AvroConverter *avro.Converter

	// Logger is for logging
	Logger logging.Logger
}

// NewJSONToAvroConverter creates a new JSON to Avro converter
func NewJSONToAvroConverter(schemaRegistryURL string, logger logging.Logger) *JSONToAvroConverter {
	return &JSONToAvroConverter{
		AvroConverter: avro.NewConverter(schemaRegistryURL, logger),
		Logger:        logger,
	}
}

// Convert converts a message to Avro format
func (c *JSONToAvroConverter) Convert(ctx *MessageHandlerContext, targetFormat MessageFormat) ([]byte, error) {
	if targetFormat != MessageFormatAvro {
		return nil, fmt.Errorf("unsupported target format: %s", targetFormat)
	}

	// Get message payload
	payload := ctx.Message.Payload()

	// Parse JSON
	var data map[string]interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, errors.Wrap(err, "failed to parse JSON")
	}

	// Get schema ID or name from context or properties
	var schemaID int
	var schemaName string

	if ctx.DetectionResult.SchemaID > 0 {
		schemaID = ctx.DetectionResult.SchemaID
	} else if idStr, ok := ctx.Message.Properties()["schema-id"]; ok {
		if id, err := parseSchemaID(idStr); err == nil {
			schemaID = id
		}
	}

	if name, ok := ctx.Message.Properties()["schema-name"]; ok {
		schemaName = name
	} else if topic := ctx.Message.Topic(); topic != "" {
		// Derive schema name from topic
		parts := strings.Split(topic, "/")
		if len(parts) > 0 {
			schemaName = parts[len(parts)-1] + "-value"
		}
	}

	// Convert to Avro
	var avroData []byte
	var err error

	if schemaID > 0 {
		// Convert using schema ID
		avroData, err = c.AvroConverter.MapToAvro(data, schemaID)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to convert to Avro with schema ID %d", schemaID)
		}
	} else if schemaName != "" {
		// Convert using schema name
		avroData, err = c.AvroConverter.MapToAvroWithSchemaName(data, schemaName)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to convert to Avro with schema name %s", schemaName)
		}
	} else {
		return nil, errors.New("no schema ID or name available for Avro conversion")
	}

	return avroData, nil
}

// Name returns the name of the converter
func (c *JSONToAvroConverter) Name() string {
	return "JSONToAvroConverter"
}

// parseSchemaID parses a schema ID from a string
func parseSchemaID(s string) (int, error) {
	var id int
	_, err := fmt.Sscanf(s, "%d", &id)
	return id, err
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// MessageHandlerRegistry manages message handlers
type MessageHandlerRegistry struct {
	// Handlers is a map of handler type to handler
	Handlers map[HandlerType]MessageHandler

	// DefaultHandler is the default handler to use
	DefaultHandler MessageHandler

	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder
}

// NewMessageHandlerRegistry creates a new message handler registry
func NewMessageHandlerRegistry(
	logger logging.Logger,
	metrics metrics.MetricsRecorder,
) *MessageHandlerRegistry {
	return &MessageHandlerRegistry{
		Handlers: make(map[HandlerType]MessageHandler),
		Logger:   logger,
		Metrics:  metrics,
	}
}

// RegisterHandler registers a handler
func (r *MessageHandlerRegistry) RegisterHandler(handler MessageHandler) {
	r.Handlers[handler.GetHandlerType()] = handler
	r.Logger.Info("Registered message handler",
		"type", handler.GetHandlerType(),
		"name", handler.Name())
}

// SetDefaultHandler sets the default handler
func (r *MessageHandlerRegistry) SetDefaultHandler(handler MessageHandler) {
	r.DefaultHandler = handler
	r.Logger.Info("Set default message handler",
		"type", handler.GetHandlerType(),
		"name", handler.Name())
}

// GetHandler gets a handler for a message
func (r *MessageHandlerRegistry) GetHandler(ctx *MessageHandlerContext) MessageHandler {
	// First, try to get handler based on detection result
	if ctx.DetectionResult.Format != MessageFormatUnknown {
		switch ctx.DetectionResult.Format {
		case MessageFormatAvro:
			if handler, ok := r.Handlers[HandlerTypeAvro]; ok {
				return handler
			}
		case MessageFormatJSON:
			if handler, ok := r.Handlers[HandlerTypeJSON]; ok {
				return handler
			}
		case MessageFormatProtobuf:
			if handler, ok := r.Handlers[HandlerTypeProtobuf]; ok {
				return handler
			}
		}
	}

	// Try all handlers to see if any can handle the message
	for _, handler := range r.Handlers {
		if handler.CanHandle(ctx) {
			return handler
		}
	}

	// Fall back to default handler
	if r.DefaultHandler != nil {
		return r.DefaultHandler
	}

	// Use raw handler as last resort
	if handler, ok := r.Handlers[HandlerTypeRaw]; ok {
		return handler
	}

	// No suitable handler found
	return nil
}

// MessageHandlerFactory creates message handlers
type MessageHandlerFactory struct {
	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder

	// DefaultOptions contains default options for handlers
	DefaultOptions MessageHandlerOptions

	// Registry is the handler registry
	Registry *MessageHandlerRegistry

	// SchemaRegistryURL is the URL of the schema registry
	SchemaRegistryURL string
}

// NewMessageHandlerFactory creates a new message handler factory
func NewMessageHandlerFactory(
	logger logging.Logger,
	metrics metrics.MetricsRecorder,
	schemaRegistryURL string,
) *MessageHandlerFactory {
	factory := &MessageHandlerFactory{
		Logger:            logger,
		Metrics:           metrics,
		DefaultOptions:    DefaultMessageHandlerOptions(),
		Registry:          NewMessageHandlerRegistry(logger, metrics),
		SchemaRegistryURL: schemaRegistryURL,
	}

	// Initialize default options
	factory.DefaultOptions.SchemaRegistryURL = schemaRegistryURL

	// Create default validators and filters
	factory.DefaultOptions.Validators = []MessageValidator{
		NewMessageSizeValidator(factory.DefaultOptions.MaxMessageSize),
	}

	factory.DefaultOptions.Filters = []MessageFilter{
		NewEmptyMessageFilter(),
	}

	// Create and register default handlers
	factory.registerDefaultHandlers()

	return factory
}

// registerDefaultHandlers registers the default handlers
func (f *MessageHandlerFactory) registerDefaultHandlers() {
	// Create handlers with default options
	avroHandler := NewAvroMessageHandler(f.Logger, f.Metrics, f.DefaultOptions)
	jsonHandler := NewJSONMessageHandler(f.Logger, f.Metrics, f.DefaultOptions)
	rawHandler := NewRawMessageHandler(f.Logger, f.Metrics, f.DefaultOptions)

	// Register handlers
	f.Registry.RegisterHandler(avroHandler)
	f.Registry.RegisterHandler(jsonHandler)
	f.Registry.RegisterHandler(rawHandler)

	// Set default handler
	f.Registry.SetDefaultHandler(rawHandler)
}

// CreateHandlerWithOptions creates a handler with custom options
func (f *MessageHandlerFactory) CreateHandlerWithOptions(
	handlerType HandlerType,
	options MessageHandlerOptions,
) (MessageHandler, error) {
	switch handlerType {
	case HandlerTypeAvro:
		return NewAvroMessageHandler(f.Logger, f.Metrics, options), nil
	case HandlerTypeJSON:
		return NewJSONMessageHandler(f.Logger, f.Metrics, options), nil
	case HandlerTypeRaw:
		return NewRawMessageHandler(f.Logger, f.Metrics, options), nil
	default:
		return nil, fmt.Errorf("unsupported handler type: %s", handlerType)
	}
}

// CreateHandlerWithDefaults creates a handler with default options
func (f *MessageHandlerFactory) CreateHandlerWithDefaults(
	handlerType HandlerType,
) (MessageHandler, error) {
	return f.CreateHandlerWithOptions(handlerType, f.DefaultOptions)
}

// GetRegistry returns the handler registry
func (f *MessageHandlerFactory) GetRegistry() *MessageHandlerRegistry {
	return f.Registry
}

// ProcessMessage processes a message with the appropriate handler
func (f *MessageHandlerFactory) ProcessMessage(
	ctx context.Context,
	msg Message,
) (*message.Data, error) {
	// Create handler context
	handlerCtx := NewMessageHandlerContext(
		ctx,
		msg,
		f.Logger,
		f.Metrics,
		f.DefaultOptions,
	)

	// Detect message format if enabled
	if f.DefaultOptions.EnableFormatDetection {
		// Use the first handler to detect format (they all have the same detection logic)
		for _, handler := range f.Registry.Handlers {
			detectionResult := handler.(*BaseMessageHandler).DetectMessageFormat(handlerCtx)
			handlerCtx.DetectionResult = detectionResult
			break
		}
	}

	// Get appropriate handler
	handler := f.Registry.GetHandler(handlerCtx)
	if handler == nil {
		return nil, errors.New("no suitable handler found for message")
	}

	// Process message with handler
	f.Logger.Debug("Processing message with handler",
		"handler", handler.Name(),
		"topic", msg.Topic(),
		"messageID", string(msg.ID()))

	// Record metrics
	startTime := time.Now()
	if f.Metrics != nil {
		f.Metrics.CounterInc("message_processing_attempts", map[string]string{
			"handler": handler.Name(),
		})
	}

	// Process message
	data, err := handler.HandleMessage(handlerCtx)

	// Record metrics
	if f.Metrics != nil {
		status := "success"
		if err != nil {
			status = "error"
			f.Metrics.CounterInc("message_processing_errors", map[string]string{
				"handler": handler.Name(),
				"error":   fmt.Sprintf("%T", err),
			})
		}

		f.Metrics.HistogramObserve("message_processing_time_ms",
			float64(time.Since(startTime).Milliseconds()),
			map[string]string{
				"handler": handler.Name(),
				"status":  status,
			})
	}

	return data, err
}

// RegisterConverters registers message converters
func (f *MessageHandlerFactory) RegisterConverters() {
	// Create JSON to Avro converter
	jsonToAvro := NewJSONToAvroConverter(f.SchemaRegistryURL, f.Logger)

	// Register converter
	f.DefaultOptions.Converters[MessageFormatAvro] = jsonToAvro

	f.Logger.Info("Registered message converters")
}

// RegisterCustomValidator registers a custom validator
func (f *MessageHandlerFactory) RegisterCustomValidator(validator MessageValidator) {
	f.DefaultOptions.Validators = append(f.DefaultOptions.Validators, validator)
	f.Logger.Info("Registered custom validator", "name", validator.Name())
}

// RegisterCustomFilter registers a custom filter
func (f *MessageHandlerFactory) RegisterCustomFilter(filter MessageFilter) {
	f.DefaultOptions.Filters = append(f.DefaultOptions.Filters, filter)
	f.Logger.Info("Registered custom filter", "name", filter.Name())
}

// RegisterMetrics registers metrics
func (f *MessageHandlerFactory) RegisterMetrics() {
	if f.Metrics == nil {
		return
	}

	// Register counters
	f.Metrics.RegisterCounter("messages_processed", "Number of messages processed successfully")
	f.Metrics.RegisterCounter("message_processing_attempts", "Number of message processing attempts")
	f.Metrics.RegisterCounter("message_processing_errors", "Number of message processing errors")
	f.Metrics.RegisterCounter("message_validation_failures", "Number of message validation failures")
	f.Metrics.RegisterCounter("message_filtered", "Number of messages filtered out")
	f.Metrics.RegisterCounter("avro_decode_failures", "Number of Avro decode failures")
	f.Metrics.RegisterCounter("json_decode_failures", "Number of JSON decode failures")
	f.Metrics.RegisterCounter("schema_resolution_failures", "Number of schema resolution failures")

	// Register histograms
	f.Metrics.RegisterHistogram("message_size_bytes", "Size of messages in bytes",
		[]float64{10, 100, 1000, 10000, 100000, 1000000})
	f.Metrics.RegisterHistogram("message_processing_time_ms", "Time to process a message in milliseconds",
		[]float64{1, 5, 10, 50, 100, 500, 1000, 5000})

	f.Logger.Info("Registered message metrics")
}

// GenericMessageHandler is a generic handler that can handle any message format
type GenericMessageHandler struct {
	*BaseMessageHandler

	// Registry is the handler registry
	Registry *MessageHandlerRegistry
}

// NewGenericMessageHandler creates a new generic message handler
func NewGenericMessageHandler(
	logger logging.Logger,
	metrics metrics.MetricsRecorder,
	options MessageHandlerOptions,
	registry *MessageHandlerRegistry,
) *GenericMessageHandler {
	base := NewBaseMessageHandler(
		HandlerTypeGeneric,
		"GenericMessageHandler",
		logger,
		metrics,
		options,
	)

	return &GenericMessageHandler{
		BaseMessageHandler: base,
		Registry:           registry,
	}
}

// CanHandle returns whether this handler can handle the message
func (h *GenericMessageHandler) CanHandle(ctx *MessageHandlerContext) bool {
	// Generic handler can handle any message
	return true
}

// HandleMessage handles a message by delegating to the appropriate handler
func (h *GenericMessageHandler) HandleMessage(ctx *MessageHandlerContext) (*message.Data, error) {
	// Detect format if not already detected
	if ctx.DetectionResult.Format == MessageFormatUnknown && h.Options.EnableFormatDetection {
		ctx.DetectionResult = h.DetectMessageFormat(ctx)
	}

	// Get appropriate handler
	handler := h.Registry.GetHandler(ctx)
	if handler == nil || handler == h {
		// No suitable handler found or we got ourselves, use raw handler
		if rawHandler, ok := h.Registry.Handlers[HandlerTypeRaw]; ok {
			handler = rawHandler
		} else {
			return nil, errors.New("no suitable handler found for message")
		}
	}

	// Process message with handler
	return handler.HandleMessage(ctx)
}

// MessageTypeDetectionResult contains the result of message type detection
type MessageTypeDetectionResult struct {
	// Type is the detected message type
	Type string

	// Confidence is the confidence level (0-1)
	Confidence float64

	// SchemaName is the detected schema name
	SchemaName string

	// Namespace is the detected namespace
	Namespace string

	// Version is the detected version
	Version string

	// Fields is a map of field names to types
	Fields map[string]string
}

// MessageTypeDetector detects message types
type MessageTypeDetector struct {
	// SchemaResolver is for resolving schemas
	SchemaResolver *avro.SchemaResolver

	// Logger is for logging
	Logger logging.Logger

	// SchemaCache is a cache for schemas
	SchemaCache sync.Map

	// TypeMappings is a map of schemas to types
	TypeMappings map[string]string
}

// NewMessageTypeDetector creates a new message type detector
func NewMessageTypeDetector(
	schemaRegistryURL string,
	logger logging.Logger,
) *MessageTypeDetector {
	return &MessageTypeDetector{
		SchemaResolver: avro.NewSchemaResolver(schemaRegistryURL, logger),
		Logger:         logger,
		TypeMappings:   make(map[string]string),
	}
}

// DetectType detects the type of a message
func (d *MessageTypeDetector) DetectType(
	ctx *MessageHandlerContext,
) (MessageTypeDetectionResult, error) {
	result := MessageTypeDetectionResult{
		Confidence: 0,
		Fields:     make(map[string]string),
	}

	// Check if the format is detected
	if ctx.DetectionResult.Format == MessageFormatUnknown {
		return result, errors.New("message format not detected")
	}

	// Handle different formats
	switch ctx.DetectionResult.Format {
	case MessageFormatAvro:
		return d.detectAvroType(ctx)

	case MessageFormatJSON:
		return d.detectJSONType(ctx)

	default:
		return result, fmt.Errorf("type detection not supported for format: %s", ctx.DetectionResult.Format)
	}
}

// detectAvroType detects the type of an Avro message
func (d *MessageTypeDetector) detectAvroType(
	ctx *MessageHandlerContext,
) (MessageTypeDetectionResult, error) {
	result := MessageTypeDetectionResult{
		Confidence: 0,
		Fields:     make(map[string]string),
	}

	// Check if we have a schema ID
	if ctx.DetectionResult.SchemaID == 0 {
		return result, errors.New("no schema ID available for Avro message")
	}

	schemaID := ctx.DetectionResult.SchemaID

	// Try to get schema from cache
	schemaKey := fmt.Sprintf("schema-%d", schemaID)
	if cachedSchema, ok := d.SchemaCache.Load(schemaKey); ok {
		schemaStr := cachedSchema.(string)
		return d.parseAvroSchema(schemaStr, result)
	}

	// Resolve schema
	schemaStr, err := d.SchemaResolver.GetSchema(schemaID)
	if err != nil {
		return result, errors.Wrap(err, "failed to resolve schema")
	}

	// Cache schema
	d.SchemaCache.Store(schemaKey, schemaStr)

	// Parse schema
	return d.parseAvroSchema(schemaStr, result)
}

// parseAvroSchema parses an Avro schema to extract type information
func (d *MessageTypeDetector) parseAvroSchema(
	schemaStr string,
	result MessageTypeDetectionResult,
) (MessageTypeDetectionResult, error) {
	// Parse schema
	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(schemaStr), &schema); err != nil {
		return result, errors.Wrap(err, "failed to parse Avro schema")
	}

	// Extract type information
	if typeName, ok := schema["name"].(string); ok {
		result.Type = typeName
		result.Confidence = 1.0
	}

	if namespace, ok := schema["namespace"].(string); ok {
		result.Namespace = namespace
	}

	// Extract fields
	if fields, ok := schema["fields"].([]interface{}); ok {
		for _, field := range fields {
			if fieldMap, ok := field.(map[string]interface{}); ok {
				if fieldName, ok := fieldMap["name"].(string); ok {
					if fieldType, ok := fieldMap["type"].(string); ok {
						result.Fields[fieldName] = fieldType
					} else if fieldTypeObj, ok := fieldMap["type"].(map[string]interface{}); ok {
						if typeName, ok := fieldTypeObj["type"].(string); ok {
							result.Fields[fieldName] = typeName
						}
					}
				}
			}
		}
	}

	return result, nil
}

// detectJSONType detects the type of a JSON message
func (d *MessageTypeDetector) detectJSONType(
	ctx *MessageHandlerContext,
) (MessageTypeDetectionResult, error) {
	result := MessageTypeDetectionResult{
		Confidence: 0.7, // JSON type detection is less certain
		Fields:     make(map[string]string),
	}

	// Parse JSON
	var data map[string]interface{}
	if err := json.Unmarshal(ctx.Message.Payload(), &data); err != nil {
		return result, errors.Wrap(err, "failed to parse JSON")
	}

	// Look for type indicators
	if typeName, ok := data["type"].(string); ok {
		result.Type = typeName
		result.Confidence = 0.9
	} else if typeName, ok := data["_type"].(string); ok {
		result.Type = typeName
		result.Confidence = 0.9
	} else if typeName, ok := data["@type"].(string); ok {
		result.Type = typeName
		result.Confidence = 0.9
	} else {
		// Try to infer type from topic
		topic := ctx.Message.Topic()
		parts := strings.Split(topic, "/")
		if len(parts) > 0 {
			result.Type = parts[len(parts)-1]
			result.Confidence = 0.6
		}
	}

	// Extract version if available
	if version, ok := data["version"].(string); ok {
		result.Version = version
	} else if version, ok := data["_version"].(string); ok {
		result.Version = version
	}

	// Extract fields and their types
	for k, v := range data {
		if k == "type" || k == "_type" || k == "@type" || k == "version" || k == "_version" {
			continue
		}

		result.Fields[k] = reflect.TypeOf(v).String()
	}

	return result, nil
}

// AddTypeMapping adds a mapping from schema to type
func (d *MessageTypeDetector) AddTypeMapping(schemaName, typeName string) {
	d.TypeMappings[schemaName] = typeName
}

// GetTypeMapping gets the type for a schema
func (d *MessageTypeDetector) GetTypeMapping(schemaName string) (string, bool) {
	typeName, ok := d.TypeMappings[schemaName]
	return typeName, ok
}

//Personal.AI order the ending
