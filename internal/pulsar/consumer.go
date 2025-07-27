// Package pulsar provides functionality for interacting with Apache Pulsar.
package pulsar

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"

	"github.com/turtacn/staravail/internal/common/logger"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/common/types/model"
	"github.com/turtacn/staravail/internal/infrastructure/config"
)

// ConsumerState represents the current state of a consumer
type ConsumerState string

const (
	// ConsumerStateInitializing means the consumer is being initialized
	ConsumerStateInitializing ConsumerState = "initializing"

	// ConsumerStateConnecting means the consumer is connecting to Pulsar
	ConsumerStateConnecting ConsumerState = "connecting"

	// ConsumerStateReady means the consumer is ready to receive messages
	ConsumerStateReady ConsumerState = "ready"

	// ConsumerStatePaused means the consumer is paused
	ConsumerStatePaused ConsumerState = "paused"

	// ConsumerStateReconnecting means the consumer is reconnecting
	ConsumerStateReconnecting ConsumerState = "reconnecting"

	// ConsumerStateClosing means the consumer is being closed
	ConsumerStateClosing ConsumerState = "closing"

	// ConsumerStateClosed means the consumer is closed
	ConsumerStateClosed ConsumerState = "closed"

	// ConsumerStateError means the consumer is in an error state
	ConsumerStateError ConsumerState = "error"
)

// SubscriptionType is an enum for the subscription types
type SubscriptionType int

const (
	// SubscriptionTypeExclusive means only one consumer can use the subscription
	SubscriptionTypeExclusive SubscriptionType = iota

	// SubscriptionTypeShared means multiple consumers can use the same subscription
	SubscriptionTypeShared

	// SubscriptionTypeFailover means multiple consumers can connect but only one is active
	SubscriptionTypeFailover

	// SubscriptionTypeKeyShared means multiple consumers share a subscription with key-based distribution
	SubscriptionTypeKeyShared
)

// String returns the string representation of the subscription type
func (t SubscriptionType) String() string {
	switch t {
	case SubscriptionTypeExclusive:
		return "exclusive"
	case SubscriptionTypeShared:
		return "shared"
	case SubscriptionTypeFailover:
		return "failover"
	case SubscriptionTypeKeyShared:
		return "key_shared"
	default:
		return "unknown"
	}
}

// ToPulsarSubscriptionType converts to pulsar.SubscriptionType
func (t SubscriptionType) ToPulsarSubscriptionType() pulsar.SubscriptionType {
	switch t {
	case SubscriptionTypeExclusive:
		return pulsar.Exclusive
	case SubscriptionTypeShared:
		return pulsar.Shared
	case SubscriptionTypeFailover:
		return pulsar.Failover
	case SubscriptionTypeKeyShared:
		return pulsar.KeyShared
	default:
		return pulsar.Exclusive
	}
}

// SubscriptionInitialPosition is an enum for the initial position
type SubscriptionInitialPosition int

const (
	// SubscriptionPositionLatest means start consuming from the latest message
	SubscriptionPositionLatest SubscriptionInitialPosition = iota

	// SubscriptionPositionEarliest means start consuming from the earliest message
	SubscriptionPositionEarliest
)

// String returns the string representation of the subscription initial position
func (p SubscriptionInitialPosition) String() string {
	switch p {
	case SubscriptionPositionLatest:
		return "latest"
	case SubscriptionPositionEarliest:
		return "earliest"
	default:
		return "unknown"
	}
}

// ToPulsarSubscriptionInitialPosition converts to pulsar.SubscriptionInitialPosition
func (p SubscriptionInitialPosition) ToPulsarSubscriptionInitialPosition() pulsar.SubscriptionInitialPosition {
	switch p {
	case SubscriptionPositionLatest:
		return pulsar.SubscriptionPositionLatest
	case SubscriptionPositionEarliest:
		return pulsar.SubscriptionPositionEarliest
	default:
		return pulsar.SubscriptionPositionLatest
	}
}

// ConsumerOptions contains options for creating a consumer
type ConsumerOptions struct {
	// ClientOptions contains options for the Pulsar client
	ClientOptions pulsar.ClientOptions

	// Topics is the list of topics to subscribe to
	Topics []string

	// TopicsPattern is a regex pattern for topics to subscribe to
	TopicsPattern string

	// SubscriptionName is the name of the subscription
	SubscriptionName string

	// SubscriptionType is the type of subscription
	SubscriptionType SubscriptionType

	// SubscriptionInitialPosition is the initial position for the subscription
	SubscriptionInitialPosition SubscriptionInitialPosition

	// ReceiverQueueSize is the size of the receiver queue
	ReceiverQueueSize int

	// ConsumerName is the name of the consumer
	ConsumerName string

	// MaxReconnectAttempts is the maximum number of reconnect attempts
	MaxReconnectAttempts int

	// ReconnectDelay is the delay between reconnect attempts
	ReconnectDelay time.Duration

	// AckTimeout is the timeout for unacked messages
	AckTimeout time.Duration

	// NackRedeliveryDelay is the delay before redelivering a nacked message
	NackRedeliveryDelay time.Duration

	// EnableRetry controls whether to enable retry on failures
	EnableRetry bool

	// EnableBatchIndexAck controls whether to enable batch index acknowledgment
	EnableBatchIndexAck bool

	// MaxPendingMessages is the maximum number of pending messages
	MaxPendingMessages int

	// HealthCheckInterval is the interval for health checks
	HealthCheckInterval time.Duration

	// EnableAutoAckOnClose controls whether to auto-ack messages on close
	EnableAutoAckOnClose bool

	// EnableAutoScaleReceiverQueueSize controls whether to auto-scale receiver queue size
	EnableAutoScaleReceiverQueueSize bool

	// DeadLetterPolicy is the policy for dead letter
	DeadLetterPolicy *DeadLetterPolicy

	// MetricsEnabled controls whether metrics are enabled
	MetricsEnabled bool

	// MetricsUpdateInterval is the interval for updating metrics
	MetricsUpdateInterval time.Duration
}

// DeadLetterPolicy represents a policy for handling dead letter
type DeadLetterPolicy struct {
	// MaxRedeliverCount is the maximum number of redeliveries
	MaxRedeliverCount int

	// DeadLetterTopic is the topic for dead letter
	DeadLetterTopic string

	// RetryLetterTopic is the topic for retry
	RetryLetterTopic string
}

// ToPulsarDeadLetterPolicy converts to pulsar.DeadLetterPolicy
func (p *DeadLetterPolicy) ToPulsarDeadLetterPolicy() *pulsar.DLQPolicy {
	if p == nil {
		return nil
	}

	return &pulsar.DLQPolicy{
		MaxDeliveries:    uint32(p.MaxRedeliverCount),
		DeadLetterTopic:  p.DeadLetterTopic,
		RetryLetterTopic: p.RetryLetterTopic,
	}
}

// DefaultConsumerOptions returns the default consumer options
func DefaultConsumerOptions() ConsumerOptions {
	return ConsumerOptions{
		ClientOptions: pulsar.ClientOptions{
			ConnectionTimeout: 30 * time.Second,
			OperationTimeout:  30 * time.Second,
		},
		SubscriptionType:                 SubscriptionTypeShared,
		SubscriptionInitialPosition:      SubscriptionPositionLatest,
		ReceiverQueueSize:                1000,
		MaxReconnectAttempts:             10,
		ReconnectDelay:                   5 * time.Second,
		AckTimeout:                       10 * time.Second,
		NackRedeliveryDelay:              1 * time.Minute,
		EnableRetry:                      true,
		EnableBatchIndexAck:              true,
		MaxPendingMessages:               10000,
		HealthCheckInterval:              1 * time.Minute,
		EnableAutoAckOnClose:             false,
		EnableAutoScaleReceiverQueueSize: false,
		MetricsEnabled:                   true,
		MetricsUpdateInterval:            1 * time.Minute,
	}
}

// ConsumerMetrics contains metrics for a consumer
type ConsumerMetrics struct {
	// MessagesReceived is the total number of messages received
	MessagesReceived int64

	// BytesReceived is the total number of bytes received
	BytesReceived int64

	// MessagesAcked is the total number of messages acknowledged
	MessagesAcked int64

	// MessagesNacked is the total number of messages not acknowledged
	MessagesNacked int64

	// ReceiveFailures is the total number of receive failures
	ReceiveFailures int64

	// AckFailures is the total number of acknowledgment failures
	AckFailures int64

	// NackFailures is the total number of negative acknowledgment failures
	NackFailures int64

	// MessagesRedelivered is the total number of messages redelivered
	MessagesRedelivered int64

	// PendingMessages is the number of pending messages
	PendingMessages int

	// LastReceiveTime is the last time a message was received
	LastReceiveTime time.Time

	// LastErrorTime is the last time an error occurred
	LastErrorTime time.Time

	// LastError is the last error that occurred
	LastError error

	// ConnectionAttempts is the number of connection attempts
	ConnectionAttempts int

	// ReconnectionAttempts is the number of reconnection attempts
	ReconnectionAttempts int

	// ReceiveRate is the rate of message reception
	ReceiveRate float64

	// AckRate is the rate of message acknowledgments
	AckRate float64

	// AverageMessageSize is the average message size
	AverageMessageSize float64

	// AverageReceiveLatency is the average latency for receiving messages
	AverageReceiveLatency time.Duration

	// PulsarLag is the lag between the producer and consumer
	PulsarLag time.Duration
}

// ConsumerHealthCheck contains health check information
type ConsumerHealthCheck struct {
	// IsHealthy indicates if the consumer is healthy
	IsHealthy bool

	// LastCheckTime is when the last health check was performed
	LastCheckTime time.Time

	// State is the current state of the consumer
	State ConsumerState

	// LastError is the last error that occurred
	LastError error

	// HealthIssues contains a list of health issues
	HealthIssues []string
}

// Message is an interface for a Pulsar message
type Message interface {
	// ID returns the message ID
	ID() []byte

	// Topic returns the topic of the message
	Topic() string

	// Payload returns the payload of the message
	Payload() []byte

	// Properties returns the properties of the message
	Properties() map[string]string

	// PublishTime returns the publish time of the message
	PublishTime() time.Time

	// EventTime returns the event time of the message
	EventTime() time.Time

	// Key returns the key of the message
	Key() string

	// OrderingKey returns the ordering key of the message
	OrderingKey() string

	// RedeliveryCount returns the redelivery count of the message
	RedeliveryCount() int32

	// SchemaVersion returns the schema version of the message
	SchemaVersion() []byte
}

// PulsarMessage implements the Message interface
type PulsarMessage struct {
	// msg is the underlying Pulsar message
	msg pulsar.Message
}

// NewPulsarMessage creates a new PulsarMessage
func NewPulsarMessage(msg pulsar.Message) *PulsarMessage {
	return &PulsarMessage{
		msg: msg,
	}
}

// ID returns the message ID
func (m *PulsarMessage) ID() []byte {
	msgID := m.msg.ID()
	if msgID == nil {
		return nil
	}
	return msgID.Serialize()
}

// Topic returns the topic of the message
func (m *PulsarMessage) Topic() string {
	return m.msg.Topic()
}

// Payload returns the payload of the message
func (m *PulsarMessage) Payload() []byte {
	return m.msg.Payload()
}

// Properties returns the properties of the message
func (m *PulsarMessage) Properties() map[string]string {
	return m.msg.Properties()
}

// PublishTime returns the publish time of the message
func (m *PulsarMessage) PublishTime() time.Time {
	return m.msg.PublishTime()
}

// EventTime returns the event time of the message
func (m *PulsarMessage) EventTime() time.Time {
	return m.msg.EventTime()
}

// Key returns the key of the message
func (m *PulsarMessage) Key() string {
	return m.msg.Key()
}

// OrderingKey returns the ordering key of the message
func (m *PulsarMessage) OrderingKey() string {
	return m.msg.OrderingKey()
}

// RedeliveryCount returns the redelivery count of the message
func (m *PulsarMessage) RedeliveryCount() int32 {
	return m.msg.RedeliveryCount()
}

// SchemaVersion returns the schema version of the message
func (m *PulsarMessage) SchemaVersion() []byte {
	return m.msg.SchemaVersion()
}

// RawMessage returns the underlying Pulsar message
func (m *PulsarMessage) RawMessage() pulsar.Message {
	return m.msg
}

// MessageHandler is a function that handles a message
type MessageHandler func(msg Message) error

// MessageHandlerOptions contains options for message handlers
type MessageHandlerOptions struct {
	// EnableProfiling enables profiling of message processing
	EnableProfiling bool

	// HandleParallel enables parallel message handling
	HandleParallel bool

	// MaxParallelHandlers is the maximum number of parallel handlers
	MaxParallelHandlers int

	// EnableAutoAck enables automatic acknowledgment
	EnableAutoAck bool

	// HandleTimeout is the timeout for handling a message
	HandleTimeout time.Duration
}

// DefaultMessageHandlerOptions returns default message handler options
func DefaultMessageHandlerOptions() MessageHandlerOptions {
	return MessageHandlerOptions{
		EnableProfiling:     false,
		HandleParallel:      true,
		MaxParallelHandlers: 10,
		EnableAutoAck:       false,
		HandleTimeout:       30 * time.Second,
	}
}

// PulsarConsumer is the interface for a Pulsar consumer
type PulsarConsumer interface {
	// Subscribe subscribes to the topics
	Subscribe(ctx context.Context) error

	// Receive receives a message synchronously
	Receive(ctx context.Context) (Message, error)

	// ReceiveAsync receives messages asynchronously with a handler
	ReceiveAsync(ctx context.Context, handler MessageHandler, options MessageHandlerOptions) error

	// Ack acknowledges a message
	Ack(msg Message) error

	// AckID acknowledges a message by ID
	AckID(msgID []byte) error

	// Nack negatively acknowledges a message
	Nack(msg Message) error

	// Close closes the consumer
	Close() error

	// Pause pauses the consumer
	Pause() error

	// Resume resumes a paused consumer
	Resume() error

	// Seek seeks to a position in the topic
	Seek(msgID []byte) error

	// SeekByTime seeks to a position by time
	SeekByTime(time time.Time) error

	// GetLastMessageID gets the last message ID
	GetLastMessageID() ([]byte, error)

	// IsConnected returns whether the consumer is connected
	IsConnected() bool

	// GetMetrics returns the consumer metrics
	GetMetrics() ConsumerMetrics

	// GetHealthCheck performs a health check
	GetHealthCheck() ConsumerHealthCheck

	// GetSubscriptionName returns the subscription name
	GetSubscriptionName() string

	// GetTopics returns the topics being consumed
	GetTopics() []string

	// GetTopicsPattern returns the topics pattern
	GetTopicsPattern() string

	// GetState returns the current state of the consumer
	GetState() ConsumerState

	// GetOptions returns the consumer options
	GetOptions() ConsumerOptions

	// GetName returns the consumer name
	GetName() string
}

// DefaultPulsarConsumer implements PulsarConsumer
type DefaultPulsarConsumer struct {
	// Options contains the consumer options
	Options ConsumerOptions

	// Client is the Pulsar client
	Client pulsar.Client

	// Consumer is the underlying Pulsar consumer
	Consumer pulsar.Consumer

	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder

	// State is the current state of the consumer
	State ConsumerState

	// StateMutex protects the state
	StateMutex sync.RWMutex

	// ConsumerMetrics contains metrics for the consumer
	ConsumerMetrics ConsumerMetrics

	// MetricsMutex protects the metrics
	MetricsMutex sync.RWMutex

	// HealthCheck contains the health check status
	HealthCheck ConsumerHealthCheck

	// HealthCheckMutex protects the health check
	HealthCheckMutex sync.RWMutex

	// PendingMessages is a map of pending message IDs to messages
	PendingMessages sync.Map

	// MetricsCtx is the context for metrics collection
	MetricsCtx context.Context

	// MetricsCancel is the cancel function for metrics collection
	MetricsCancel context.CancelFunc

	// LastError is the last error that occurred
	LastError error

	// LastErrorMutex protects the last error
	LastErrorMutex sync.RWMutex

	// ReceiveChannel is a channel for received messages
	ReceiveChannel chan pulsar.Message

	// StopChannel is a channel for stopping the consumer
	StopChannel chan struct{}

	// IsRunning indicates if the consumer is running
	IsRunning atomic.Bool

	// IsPaused indicates if the consumer is paused
	IsPaused atomic.Bool

	// WorkerWg is the wait group for workers
	WorkerWg sync.WaitGroup
}

// NewPulsarConsumer creates a new Pulsar consumer
func NewPulsarConsumer(
	options ConsumerOptions,
	logger logging.Logger,
	metricsRecorder metrics.MetricsRecorder,
) *DefaultPulsarConsumer {
	return &DefaultPulsarConsumer{
		Options:        options,
		Logger:         logger,
		Metrics:        metricsRecorder,
		State:          ConsumerStateInitializing,
		HealthCheck:    ConsumerHealthCheck{IsHealthy: true, State: ConsumerStateInitializing},
		ReceiveChannel: make(chan pulsar.Message, 1000),
		StopChannel:    make(chan struct{}),
	}
}

// Subscribe subscribes to the topics
func (c *DefaultPulsarConsumer) Subscribe(ctx context.Context) error {
	c.setStateInternal(ConsumerStateConnecting)
	c.Logger.Info("Subscribing to Pulsar", "topics", c.Options.Topics, "pattern", c.Options.TopicsPattern)

	// Create client if not exists
	if c.Client == nil {
		client, err := pulsar.NewClient(c.Options.ClientOptions)
		if err != nil {
			c.setLastError(errors.Wrap(err, "failed to create Pulsar client"))
			c.setStateInternal(ConsumerStateError)
			return c.LastError
		}
		c.Client = client
	}

	// Create consumer options
	consumerOptions := pulsar.ConsumerOptions{
		SubscriptionName:            c.Options.SubscriptionName,
		Type:                        c.Options.SubscriptionType.ToPulsarSubscriptionType(),
		SubscriptionInitialPosition: c.Options.SubscriptionInitialPosition.ToPulsarSubscriptionInitialPosition(),
		ReceiverQueueSize:           c.Options.ReceiverQueueSize,
		Name:                        c.Options.ConsumerName,
		RetryEnable:                 c.Options.EnableRetry,
		AckTimeout:                  c.Options.AckTimeout,
		NackRedeliveryDelay:         c.Options.NackRedeliveryDelay,
		DLQ:                         c.Options.DeadLetterPolicy.ToPulsarDeadLetterPolicy(),
		MessageChannel:              c.ReceiveChannel,
	}

	// Set topics or pattern
	if len(c.Options.Topics) > 0 {
		consumerOptions.Topics = c.Options.Topics
	} else if c.Options.TopicsPattern != "" {
		consumerOptions.TopicsPattern = c.Options.TopicsPattern
	} else {
		c.setLastError(errors.New("neither topics nor topics pattern provided"))
		c.setStateInternal(ConsumerStateError)
		return c.LastError
	}

	// Create consumer
	consumer, err := c.Client.Subscribe(consumerOptions)
	if err != nil {
		c.setLastError(errors.Wrap(err, "failed to subscribe to Pulsar"))
		c.setStateInternal(ConsumerStateError)
		return c.LastError
	}

	// Store consumer
	c.Consumer = consumer

	// Reset metrics
	c.resetMetrics()

	// Start metrics collection if enabled
	if c.Options.MetricsEnabled {
		c.startMetricsCollection()
	}

	// Start health check
	c.startHealthCheck()

	// Mark as ready
	c.setStateInternal(ConsumerStateReady)
	c.IsRunning.Store(true)

	c.Logger.Info("Successfully subscribed to Pulsar",
		"topics", c.Options.Topics,
		"pattern", c.Options.TopicsPattern,
		"subscription", c.Options.SubscriptionName,
		"type", c.Options.SubscriptionType.String())

	return nil
}

// Receive receives a message synchronously
func (c *DefaultPulsarConsumer) Receive(ctx context.Context) (Message, error) {
	// Check if consumer is ready
	if !c.isInReadyState() {
		return nil, errors.New("consumer is not ready")
	}

	// Check if consumer is paused
	if c.IsPaused.Load() {
		return nil, errors.New("consumer is paused")
	}

	// Try to receive message
	var msg pulsar.Message
	var err error

	// Use channel or direct receive based on whether a channel was provided
	if c.ReceiveChannel != nil {
		select {
		case msg = <-c.ReceiveChannel:
			// Message received from channel
		case <-ctx.Done():
			// Context done
			return nil, ctx.Err()
		case <-c.StopChannel:
			// Consumer stopping
			return nil, errors.New("consumer is stopping")
		}
	} else {
		// Direct receive from consumer
		msg, err = c.Consumer.Receive(ctx)
		if err != nil {
			c.MetricsMutex.Lock()
			c.ConsumerMetrics.ReceiveFailures++
			c.ConsumerMetrics.LastErrorTime = time.Now()
			c.ConsumerMetrics.LastError = err
			c.MetricsMutex.Unlock()

			// Log error
			c.Logger.Error("Failed to receive message", "error", err)

			// Record metrics
			if c.Metrics != nil {
				c.Metrics.CounterInc("pulsar_consumer_receive_failures", map[string]string{
					"subscription": c.Options.SubscriptionName,
					"topics":       fmt.Sprintf("%v", c.Options.Topics),
				})
			}

			return nil, errors.Wrap(err, "failed to receive message")
		}
	}

	// Check if message is nil
	if msg == nil {
		return nil, errors.New("received nil message")
	}

	// Update metrics
	c.MetricsMutex.Lock()
	c.ConsumerMetrics.MessagesReceived++
	c.ConsumerMetrics.BytesReceived += int64(len(msg.Payload()))
	c.ConsumerMetrics.LastReceiveTime = time.Now()
	c.ConsumerMetrics.PendingMessages++
	c.MetricsMutex.Unlock()

	// Store in pending messages
	msgID := msg.ID().Serialize()
	c.PendingMessages.Store(string(msgID), msg)

	// Record metrics
	if c.Metrics != nil {
		c.Metrics.CounterInc("pulsar_consumer_messages_received", map[string]string{
			"subscription": c.Options.SubscriptionName,
			"topic":        msg.Topic(),
		})
		c.Metrics.CounterAdd("pulsar_consumer_bytes_received", float64(len(msg.Payload())), map[string]string{
			"subscription": c.Options.SubscriptionName,
			"topic":        msg.Topic(),
		})
		c.Metrics.GaugeSet("pulsar_consumer_pending_messages", float64(c.getPendingMessageCount()), map[string]string{
			"subscription": c.Options.SubscriptionName,
		})
	}

	// Create message wrapper
	pulsarMsg := NewPulsarMessage(msg)

	// Debug log
	c.Logger.Debug("Received message",
		"id", pulsarMsg.ID(),
		"topic", pulsarMsg.Topic(),
		"size", len(pulsarMsg.Payload()),
		"redeliveryCount", pulsarMsg.RedeliveryCount())

	return pulsarMsg, nil
}

// ReceiveAsync receives messages asynchronously with a handler
func (c *DefaultPulsarConsumer) ReceiveAsync(
	ctx context.Context,
	handler MessageHandler,
	options MessageHandlerOptions,
) error {
	// Check if consumer is ready
	if !c.isInReadyState() {
		return errors.New("consumer is not ready")
	}

	// Create context for async receive
	receiveCtx, cancel := context.WithCancel(ctx)

	// Start worker pool
	workerCount := options.MaxParallelHandlers
	if !options.HandleParallel {
		workerCount = 1
	}

	// Start workers
	c.WorkerWg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go c.asyncWorker(receiveCtx, i, handler, options)
	}

	// Start the stop handler
	go func() {
		select {
		case <-ctx.Done():
			// Context done, cancel receive context
			cancel()
		case <-c.StopChannel:
			// Consumer stopping, cancel receive context
			cancel()
		}

		// Wait for workers to finish
		c.WorkerWg.Wait()
	}()

	c.Logger.Info("Started async message receiving",
		"workerCount", workerCount,
		"enableAutoAck", options.EnableAutoAck,
		"handleParallel", options.HandleParallel)

	return nil
}

// asyncWorker is a worker for async message handling
func (c *DefaultPulsarConsumer) asyncWorker(
	ctx context.Context,
	workerID int,
	handler MessageHandler,
	options MessageHandlerOptions,
) {
	defer c.WorkerWg.Done()

	c.Logger.Debug("Starting async worker", "workerID", workerID)

	for {
		// Check if we should stop
		select {
		case <-ctx.Done():
			c.Logger.Debug("Async worker stopping due to context done", "workerID", workerID)
			return
		case <-c.StopChannel:
			c.Logger.Debug("Async worker stopping due to consumer stop", "workerID", workerID)
			return
		default:
			// Continue
		}

		// Check if consumer is paused
		if c.IsPaused.Load() {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Receive message with timeout
		receiveCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		msg, err := c.Receive(receiveCtx)
		cancel()

		if err != nil {
			// Check if error is due to context timeout
			if errors.Is(err, context.DeadlineExceeded) {
				// This is normal, just retry
				continue
			}

			// Log other errors
			c.Logger.Error("Error receiving message in async worker",
				"workerID", workerID,
				"error", err)

			// Back off for a short time
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Process message
		startTime := time.Now()

		// Create context for handler with timeout
		handleCtx, handleCancel := context.WithTimeout(ctx, options.HandleTimeout)

		// Handle message
		handleErr := func() error {
			if options.EnableProfiling {
				// With profiling
				handlerName := fmt.Sprintf("MessageHandler-%s", c.Options.SubscriptionName)
				c.Logger.Debug("Starting message handler with profiling",
					"workerID", workerID,
					"handler", handlerName,
					"msgID", msg.ID())

				// TODO: Add profiling logic here if needed

				err := handler(msg)

				// TODO: Add post-profiling logic here if needed

				return err
			} else {
				// Without profiling
				return handler(msg)
			}
		}()

		// Calculate processing time
		processingTime := time.Since(startTime)

		// Clean up handle context
		handleCancel()

		// Handle result
		if handleErr != nil {
			// Handler failed
			c.Logger.Error("Error handling message",
				"workerID", workerID,
				"msgID", msg.ID(),
				"error", handleErr,
				"processingTime", processingTime)

			// Record metrics
			if c.Metrics != nil {
				c.Metrics.CounterInc("pulsar_consumer_handler_errors", map[string]string{
					"subscription": c.Options.SubscriptionName,
					"topic":        msg.Topic(),
				})
				c.Metrics.HistogramObserve("pulsar_consumer_handler_time_ms", float64(processingTime.Milliseconds()), map[string]string{
					"subscription": c.Options.SubscriptionName,
					"topic":        msg.Topic(),
					"success":      "false",
				})
			}

			// Nack message
			if err := c.Nack(msg); err != nil {
				c.Logger.Error("Error nacking message after handler error",
					"workerID", workerID,
					"msgID", msg.ID(),
					"error", err)
			}
		} else {
			// Handler succeeded
			c.Logger.Debug("Successfully handled message",
				"workerID", workerID,
				"msgID", msg.ID(),
				"processingTime", processingTime)

			// Record metrics
			if c.Metrics != nil {
				c.Metrics.CounterInc("pulsar_consumer_handler_successes", map[string]string{
					"subscription": c.Options.SubscriptionName,
					"topic":        msg.Topic(),
				})
				c.Metrics.HistogramObserve("pulsar_consumer_handler_time_ms", float64(processingTime.Milliseconds()), map[string]string{
					"subscription": c.Options.SubscriptionName,
					"topic":        msg.Topic(),
					"success":      "true",
				})
			}

			// Auto-ack if enabled
			if options.EnableAutoAck {
				if err := c.Ack(msg); err != nil {
					c.Logger.Error("Error auto-acking message",
						"workerID", workerID,
						"msgID", msg.ID(),
						"error", err)
				}
			}
		}
	}
}

// Ack acknowledges a message
func (c *DefaultPulsarConsumer) Ack(msg Message) error {
	// Check if consumer is ready
	if !c.isInReadyState() {
		return errors.New("consumer is not ready")
	}

	// Get raw message
	pulsarMsg, ok := msg.(*PulsarMessage)
	if !ok {
		return errors.New("message is not a PulsarMessage")
	}

	rawMsg := pulsarMsg.RawMessage()

	// Acknowledge message
	c.Consumer.Ack(rawMsg)

	// Update metrics
	c.MetricsMutex.Lock()
	c.ConsumerMetrics.MessagesAcked++
	c.ConsumerMetrics.PendingMessages--
	c.MetricsMutex.Unlock()

	// Remove from pending messages
	msgID := rawMsg.ID().Serialize()
	c.PendingMessages.Delete(string(msgID))

	// Record metrics
	if c.Metrics != nil {
		c.Metrics.CounterInc("pulsar_consumer_messages_acked", map[string]string{
			"subscription": c.Options.SubscriptionName,
			"topic":        rawMsg.Topic(),
		})
		c.Metrics.GaugeSet("pulsar_consumer_pending_messages", float64(c.getPendingMessageCount()), map[string]string{
			"subscription": c.Options.SubscriptionName,
		})
	}

	c.Logger.Debug("Acknowledged message",
		"id", msg.ID(),
		"topic", msg.Topic())

	return nil
}

// AckID acknowledges a message by ID
func (c *DefaultPulsarConsumer) AckID(msgID []byte) error {
	// Check if consumer is ready
	if !c.isInReadyState() {
		return errors.New("consumer is not ready")
	}

	// Try to find the message in pending messages
	val, ok := c.PendingMessages.Load(string(msgID))
	if !ok {
		return errors.New("message ID not found in pending messages")
	}

	// Get raw message
	rawMsg, ok := val.(pulsar.Message)
	if !ok {
		return errors.New("stored value is not a pulsar.Message")
	}

	// Acknowledge message
	c.Consumer.Ack(rawMsg)

	// Update metrics
	c.MetricsMutex.Lock()
	c.ConsumerMetrics.MessagesAcked++
	c.ConsumerMetrics.PendingMessages--
	c.MetricsMutex.Unlock()

	// Remove from pending messages
	c.PendingMessages.Delete(string(msgID))

	// Record metrics
	if c.Metrics != nil {
		c.Metrics.CounterInc("pulsar_consumer_messages_acked", map[string]string{
			"subscription": c.Options.SubscriptionName,
			"topic":        rawMsg.Topic(),
		})
		c.Metrics.GaugeSet("pulsar_consumer_pending_messages", float64(c.getPendingMessageCount()), map[string]string{
			"subscription": c.Options.SubscriptionName,
		})
	}

	c.Logger.Debug("Acknowledged message by ID", "id", msgID)

	return nil
}

// Nack negatively acknowledges a message
func (c *DefaultPulsarConsumer) Nack(msg Message) error {
	// Check if consumer is ready
	if !c.isInReadyState() {
		return errors.New("consumer is not ready")
	}

	// Get raw message
	pulsarMsg, ok := msg.(*PulsarMessage)
	if !ok {
		return errors.New("message is not a PulsarMessage")
	}

	rawMsg := pulsarMsg.RawMessage()

	// Nack message
	c.Consumer.Nack(rawMsg)

	// Update metrics
	c.MetricsMutex.Lock()
	c.ConsumerMetrics.MessagesNacked++
	c.ConsumerMetrics.PendingMessages--
	c.MetricsMutex.Unlock()

	// Remove from pending messages
	msgID := rawMsg.ID().Serialize()
	c.PendingMessages.Delete(string(msgID))

	// Record metrics
	if c.Metrics != nil {
		c.Metrics.CounterInc("pulsar_consumer_messages_nacked", map[string]string{
			"subscription": c.Options.SubscriptionName,
			"topic":        rawMsg.Topic(),
		})
		c.Metrics.GaugeSet("pulsar_consumer_pending_messages", float64(c.getPendingMessageCount()), map[string]string{
			"subscription": c.Options.SubscriptionName,
		})
	}

	c.Logger.Debug("Nacked message",
		"id", msg.ID(),
		"topic", msg.Topic(),
		"redeliveryCount", msg.RedeliveryCount())

	return nil
}

// Close closes the consumer
func (c *DefaultPulsarConsumer) Close() error {
	c.setStateInternal(ConsumerStateClosing)
	c.Logger.Info("Closing Pulsar consumer", "subscription", c.Options.SubscriptionName)

	// Signal to stop
	close(c.StopChannel)

	// Stop metrics collection
	if c.MetricsCancel != nil {
		c.MetricsCancel()
	}

	// Mark as not running
	c.IsRunning.Store(false)

	// Wait for workers to finish
	c.WorkerWg.Wait()

	// Close consumer
	if c.Consumer != nil {
		if c.Options.EnableAutoAckOnClose {
			// Auto-ack all pending messages
			var pendingCount int
			c.PendingMessages.Range(func(key, value interface{}) bool {
				msg, ok := value.(pulsar.Message)
				if ok {
					c.Consumer.Ack(msg)
					pendingCount++
				}
				return true
			})

			if pendingCount > 0 {
				c.Logger.Info("Auto-acked pending messages on close", "count", pendingCount)
			}
		}

		c.Consumer.Close()
		c.Consumer = nil
	}

	// Close client
	if c.Client != nil {
		c.Client.Close()
		c.Client = nil
	}

	// Clear pending messages
	c.PendingMessages = sync.Map{}

	// Update state
	c.setStateInternal(ConsumerStateClosed)

	c.Logger.Info("Pulsar consumer closed", "subscription", c.Options.SubscriptionName)

	return nil
}

// Pause pauses the consumer
func (c *DefaultPulsarConsumer) Pause() error {
	// Check if already paused
	if c.IsPaused.Load() {
		return nil
	}

	c.Logger.Info("Pausing Pulsar consumer", "subscription", c.Options.SubscriptionName)

	// Mark as paused
	c.IsPaused.Store(true)

	// Update state
	c.setStateInternal(ConsumerStatePaused)

	return nil
}

// Resume resumes a paused consumer
func (c *DefaultPulsarConsumer) Resume() error {
	// Check if paused
	if !c.IsPaused.Load() {
		return nil
	}

	c.Logger.Info("Resuming Pulsar consumer", "subscription", c.Options.SubscriptionName)

	// Mark as not paused
	c.IsPaused.Store(false)

	// Update state
	c.setStateInternal(ConsumerStateReady)

	return nil
}

// Seek seeks to a position in the topic
func (c *DefaultPulsarConsumer) Seek(msgID []byte) error {
	// Check if consumer is ready
	if !c.isInReadyState() {
		return errors.New("consumer is not ready")
	}

	// Create message ID
	pulsarMsgID, err := pulsar.DeserializeMessageID(msgID)
	if err != nil {
		return errors.Wrap(err, "failed to deserialize message ID")
	}

	// Seek to position
	if err := c.Consumer.Seek(pulsarMsgID); err != nil {
		c.setLastError(errors.Wrap(err, "failed to seek"))
		return c.LastError
	}

	c.Logger.Info("Seek successful", "msgID", msgID)

	return nil
}

// SeekByTime seeks to a position by time
func (c *DefaultPulsarConsumer) SeekByTime(t time.Time) error {
	// Check if consumer is ready
	if !c.isInReadyState() {
		return errors.New("consumer is not ready")
	}

	// Seek to time
	if err := c.Consumer.SeekByTime(t); err != nil {
		c.setLastError(errors.Wrap(err, "failed to seek by time"))
		return c.LastError
	}

	c.Logger.Info("Seek by time successful", "time", t)

	return nil
}

// GetLastMessageID gets the last message ID
func (c *DefaultPulsarConsumer) GetLastMessageID() ([]byte, error) {
	// Check if consumer is ready
	if !c.isInReadyState() {
		return nil, errors.New("consumer is not ready")
	}

	// Get last message ID
	msgID, err := c.Consumer.LastMessageID()
	if err != nil {
		c.setLastError(errors.Wrap(err, "failed to get last message ID"))
		return nil, c.LastError
	}

	return msgID.Serialize(), nil
}

// IsConnected returns whether the consumer is connected
func (c *DefaultPulsarConsumer) IsConnected() bool {
	// Check state
	state := c.GetState()
	return state == ConsumerStateReady || state == ConsumerStatePaused
}

// GetMetrics returns the consumer metrics
func (c *DefaultPulsarConsumer) GetMetrics() ConsumerMetrics {
	c.MetricsMutex.RLock()
	defer c.MetricsMutex.RUnlock()

	// Create a copy of metrics
	metrics := c.ConsumerMetrics

	// Calculate derived metrics
	if metrics.MessagesReceived > 0 {
		metrics.AverageMessageSize = float64(metrics.BytesReceived) / float64(metrics.MessagesReceived)
	}

	return metrics
}

// GetHealthCheck performs a health check
func (c *DefaultPulsarConsumer) GetHealthCheck() ConsumerHealthCheck {
	c.HealthCheckMutex.RLock()
	defer c.HealthCheckMutex.RUnlock()

	// Create a copy of health check
	healthCheck := c.HealthCheck

	return healthCheck
}

// GetSubscriptionName returns the subscription name
func (c *DefaultPulsarConsumer) GetSubscriptionName() string {
	return c.Options.SubscriptionName
}

// GetTopics returns the topics being consumed
func (c *DefaultPulsarConsumer) GetTopics() []string {
	return c.Options.Topics
}

// GetTopicsPattern returns the topics pattern
func (c *DefaultPulsarConsumer) GetTopicsPattern() string {
	return c.Options.TopicsPattern
}

// GetState returns the current state of the consumer
func (c *DefaultPulsarConsumer) GetState() ConsumerState {
	c.StateMutex.RLock()
	defer c.StateMutex.RUnlock()
	return c.State
}

// GetOptions returns the consumer options
func (c *DefaultPulsarConsumer) GetOptions() ConsumerOptions {
	return c.Options
}

// GetName returns the consumer name
func (c *DefaultPulsarConsumer) GetName() string {
	return c.Options.ConsumerName
}

// setStateInternal sets the state without locking
func (c *DefaultPulsarConsumer) setStateInternal(state ConsumerState) {
	c.StateMutex.Lock()
	defer c.StateMutex.Unlock()

	// Update state
	if c.State != state {
		c.Logger.Info("Consumer state changed", "from", c.State, "to", state)
		c.State = state

		// Update health check
		c.HealthCheckMutex.Lock()
		c.HealthCheck.State = state
		c.HealthCheck.LastCheckTime = time.Now()

		// Update health status based on state
		switch state {
		case ConsumerStateReady, ConsumerStatePaused:
			c.HealthCheck.IsHealthy = true
			c.HealthCheck.HealthIssues = nil
		case ConsumerStateError:
			c.HealthCheck.IsHealthy = false
			c.HealthCheck.HealthIssues = []string{"Consumer in error state"}
		case ConsumerStateClosed:
			c.HealthCheck.IsHealthy = false
			c.HealthCheck.HealthIssues = []string{"Consumer is closed"}
		default:
			// Other states are transitional, so maintain current health status
		}

		c.HealthCheckMutex.Unlock()

		// Record metrics
		if c.Metrics != nil {
			c.Metrics.GaugeSet("pulsar_consumer_state", float64(1), map[string]string{
				"subscription": c.Options.SubscriptionName,
				"state":        string(state),
			})
		}
	}
}

// isInReadyState checks if the consumer is in a ready state
func (c *DefaultPulsarConsumer) isInReadyState() bool {
	state := c.GetState()
	return state == ConsumerStateReady || state == ConsumerStatePaused
}

// setLastError sets the last error
func (c *DefaultPulsarConsumer) setLastError(err error) {
	c.LastErrorMutex.Lock()
	defer c.LastErrorMutex.Unlock()

	// Update last error
	c.LastError = err

	// Update health check
	c.HealthCheckMutex.Lock()
	c.HealthCheck.LastError = err
	c.HealthCheck.LastCheckTime = time.Now()

	// Add to health issues if error
	if err != nil {
		c.HealthCheck.HealthIssues = append(c.HealthCheck.HealthIssues, err.Error())
		// Keep only the last 10 issues
		if len(c.HealthCheck.HealthIssues) > 10 {
			c.HealthCheck.HealthIssues = c.HealthCheck.HealthIssues[len(c.HealthCheck.HealthIssues)-10:]
		}
	}
	c.HealthCheckMutex.Unlock()

	// Update metrics
	c.MetricsMutex.Lock()
	c.ConsumerMetrics.LastErrorTime = time.Now()
	c.ConsumerMetrics.LastError = err
	c.MetricsMutex.Unlock()
}

// resetMetrics resets the consumer metrics
func (c *DefaultPulsarConsumer) resetMetrics() {
	c.MetricsMutex.Lock()
	defer c.MetricsMutex.Unlock()

	c.ConsumerMetrics = ConsumerMetrics{}
}

// startMetricsCollection starts collecting metrics
func (c *DefaultPulsarConsumer) startMetricsCollection() {
	// Stop existing metrics collection if any
	if c.MetricsCancel != nil {
		c.MetricsCancel()
	}

	// Create new context
	c.MetricsCtx, c.MetricsCancel = context.WithCancel(context.Background())

	// Start collection
	go c.collectMetrics()
}

// collectMetrics collects metrics periodically
func (c *DefaultPulsarConsumer) collectMetrics() {
	ticker := time.NewTicker(c.Options.MetricsUpdateInterval)
	defer ticker.Stop()

	var lastMessagesReceived, lastMessagesAcked, lastBytesReceived int64
	var lastTime time.Time = time.Now()

	for {
		select {
		case <-ticker.C:
			// Get current values
			c.MetricsMutex.Lock()

			messagesReceived := c.ConsumerMetrics.MessagesReceived
			messagesAcked := c.ConsumerMetrics.MessagesAcked
			bytesReceived := c.ConsumerMetrics.BytesReceived
			pendingMessages := c.ConsumerMetrics.PendingMessages

			// Calculate rates
			now := time.Now()
			timeDiff := now.Sub(lastTime).Seconds()

			if timeDiff > 0 {
				messageReceiveRate := float64(messagesReceived-lastMessagesReceived) / timeDiff
				messageAckRate := float64(messagesAcked-lastMessagesAcked) / timeDiff

				c.ConsumerMetrics.ReceiveRate = messageReceiveRate
				c.ConsumerMetrics.AckRate = messageAckRate
			}

			// Update last values
			lastMessagesReceived = messagesReceived
			lastMessagesAcked = messagesAcked
			lastBytesReceived = bytesReceived
			lastTime = now

			c.MetricsMutex.Unlock()

			// Record metrics
			if c.Metrics != nil {
				c.Metrics.GaugeSet("pulsar_consumer_receive_rate", c.ConsumerMetrics.ReceiveRate, map[string]string{
					"subscription": c.Options.SubscriptionName,
				})
				c.Metrics.GaugeSet("pulsar_consumer_ack_rate", c.ConsumerMetrics.AckRate, map[string]string{
					"subscription": c.Options.SubscriptionName,
				})
				c.Metrics.GaugeSet("pulsar_consumer_pending_messages", float64(pendingMessages), map[string]string{
					"subscription": c.Options.SubscriptionName,
				})
			}

		case <-c.MetricsCtx.Done():
			return
		}
	}
}

// startHealthCheck starts the health check
func (c *DefaultPulsarConsumer) startHealthCheck() {
	go func() {
		ticker := time.NewTicker(c.Options.HealthCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.checkHealth()
			case <-c.StopChannel:
				return
			}
		}
	}()
}

// checkHealth performs a health check
func (c *DefaultPulsarConsumer) checkHealth() {
	c.HealthCheckMutex.Lock()
	defer c.HealthCheckMutex.Unlock()

	// Update check time
	c.HealthCheck.LastCheckTime = time.Now()

	// Get current state
	state := c.GetState()
	c.HealthCheck.State = state

	// Clear issues
	c.HealthCheck.HealthIssues = make([]string, 0)

	// Check health based on state
	isHealthy := true

	switch state {
	case ConsumerStateReady:
		// Check if messages are being received
		c.MetricsMutex.RLock()
		lastReceiveTime := c.ConsumerMetrics.LastReceiveTime
		lastErrorTime := c.ConsumerMetrics.LastErrorTime
		lastError := c.ConsumerMetrics.LastError
		c.MetricsMutex.RUnlock()

		// Check for recent errors
		if lastError != nil && time.Since(lastErrorTime) < c.Options.HealthCheckInterval {
			isHealthy = false
			c.HealthCheck.HealthIssues = append(c.HealthCheck.HealthIssues,
				fmt.Sprintf("Recent error: %v", lastError))
		}

		// Check if we haven't received messages for a while (only if we've received some before)
		if !lastReceiveTime.IsZero() && time.Since(lastReceiveTime) > c.Options.HealthCheckInterval*2 {
			isHealthy = false
			c.HealthCheck.HealthIssues = append(c.HealthCheck.HealthIssues,
				fmt.Sprintf("No messages received since %v", lastReceiveTime))
		}

	case ConsumerStatePaused:
		// Paused is considered healthy
		isHealthy = true

	case ConsumerStateError:
		// Error state is unhealthy
		isHealthy = false
		c.HealthCheck.HealthIssues = append(c.HealthCheck.HealthIssues, "Consumer in error state")

		// Add last error if available
		c.LastErrorMutex.RLock()
		if c.LastError != nil {
			c.HealthCheck.HealthIssues = append(c.HealthCheck.HealthIssues, c.LastError.Error())
		}
		c.LastErrorMutex.RUnlock()

	case ConsumerStateClosed:
		// Closed is unhealthy for a consumer that should be running
		if c.IsRunning.Load() {
			isHealthy = false
			c.HealthCheck.HealthIssues = append(c.HealthCheck.HealthIssues, "Consumer is closed but should be running")
		} else {
			isHealthy = true
		}

	case ConsumerStateInitializing, ConsumerStateConnecting, ConsumerStateReconnecting:
		// Transitional states are healthy if they don't last too long
		c.StateMutex.RLock()
		stateLastChanged := c.HealthCheck.LastCheckTime
		c.StateMutex.RUnlock()

		if time.Since(stateLastChanged) > time.Minute {
			isHealthy = false
			c.HealthCheck.HealthIssues = append(c.HealthCheck.HealthIssues,
				fmt.Sprintf("Consumer stuck in %s state for too long", state))
		} else {
			isHealthy = true
		}

	default:
		isHealthy = false
		c.HealthCheck.HealthIssues = append(c.HealthCheck.HealthIssues,
			fmt.Sprintf("Unknown consumer state: %s", state))
	}

	// Update health status
	c.HealthCheck.IsHealthy = isHealthy

	// Log if unhealthy
	if !isHealthy {
		c.Logger.Warn("Consumer health check failed",
			"subscription", c.Options.SubscriptionName,
			"state", state,
			"issues", c.HealthCheck.HealthIssues)
	}

	// Record metrics
	if c.Metrics != nil {
		c.Metrics.GaugeSet("pulsar_consumer_health", map[bool]float64{true: 1.0, false: 0.0}[isHealthy], map[string]string{
			"subscription": c.Options.SubscriptionName,
		})
	}
}

// getPendingMessageCount gets the number of pending messages
func (c *DefaultPulsarConsumer) getPendingMessageCount() int {
	var count int
	c.PendingMessages.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

// RegisterConsumerMetrics registers the consumer metrics
func RegisterConsumerMetrics(registry metrics.MetricsRecorder) {
	// Register counters
	registry.RegisterCounter("pulsar_consumer_messages_received", "Number of messages received from Pulsar")
	registry.RegisterCounter("pulsar_consumer_bytes_received", "Number of bytes received from Pulsar")
	registry.RegisterCounter("pulsar_consumer_messages_acked", "Number of messages acknowledged")
	registry.RegisterCounter("pulsar_consumer_messages_nacked", "Number of messages negatively acknowledged")
	registry.RegisterCounter("pulsar_consumer_receive_failures", "Number of message receive failures")
	registry.RegisterCounter("pulsar_consumer_handler_errors", "Number of message handler errors")
	registry.RegisterCounter("pulsar_consumer_handler_successes", "Number of message handler successes")

	// Register gauges
	registry.RegisterGauge("pulsar_consumer_state", "Current state of the consumer")
	registry.RegisterGauge("pulsar_consumer_health", "Health of the consumer (1=healthy, 0=unhealthy)")
	registry.RegisterGauge("pulsar_consumer_receive_rate", "Rate of message reception")
	registry.RegisterGauge("pulsar_consumer_ack_rate", "Rate of message acknowledgment")
	registry.RegisterGauge("pulsar_consumer_pending_messages", "Number of pending messages")

	// Register histograms
	registry.RegisterHistogram("pulsar_consumer_handler_time_ms", "Time to handle a message in milliseconds", []float64{1, 5, 10, 50, 100, 500, 1000, 5000})
}

// PulsarConsumerFactory creates and manages Pulsar consumers
type PulsarConsumerFactory struct {
	// Logger is for logging
	Logger logging.Logger

	// Metrics is for recording metrics
	Metrics metrics.MetricsRecorder

	// DefaultOptions contains default options for consumers
	DefaultOptions ConsumerOptions

	// Consumers is a map of created consumers
	Consumers map[string]PulsarConsumer

	// ConsumersMutex protects the consumers map
	ConsumersMutex sync.RWMutex
}

// NewPulsarConsumerFactory creates a new consumer factory
func NewPulsarConsumerFactory(
	logger logging.Logger,
	metrics metrics.MetricsRecorder,
	defaultOptions ConsumerOptions,
) *PulsarConsumerFactory {
	return &PulsarConsumerFactory{
		Logger:         logger,
		Metrics:        metrics,
		DefaultOptions: defaultOptions,
		Consumers:      make(map[string]PulsarConsumer),
	}
}

// CreateConsumer creates a new consumer with the given options
func (f *PulsarConsumerFactory) CreateConsumer(options ConsumerOptions) (PulsarConsumer, error) {
	// Create consumer
	consumer := NewPulsarConsumer(options, f.Logger, f.Metrics)

	// Generate a key for the consumer
	var key string
	if options.ConsumerName != "" {
		key = options.ConsumerName
	} else {
		key = fmt.Sprintf("%s-%s", options.SubscriptionName, time.Now().Format("20060102-150405"))
	}

	// Store consumer
	f.ConsumersMutex.Lock()
	f.Consumers[key] = consumer
	f.ConsumersMutex.Unlock()

	return consumer, nil
}

// CreateConsumerWithDefaults creates a new consumer with default options
func (f *PulsarConsumerFactory) CreateConsumerWithDefaults(
	topics []string,
	subscriptionName string,
	consumerName string,
) (PulsarConsumer, error) {
	// Create options from defaults
	options := f.DefaultOptions
	options.Topics = topics
	options.SubscriptionName = subscriptionName
	options.ConsumerName = consumerName

	return f.CreateConsumer(options)
}

// GetConsumer gets a consumer by name
func (f *PulsarConsumerFactory) GetConsumer(name string) (PulsarConsumer, bool) {
	f.ConsumersMutex.RLock()
	defer f.ConsumersMutex.RUnlock()

	consumer, ok := f.Consumers[name]
	return consumer, ok
}

// CloseConsumer closes a consumer
func (f *PulsarConsumerFactory) CloseConsumer(name string) error {
	f.ConsumersMutex.Lock()
	consumer, ok := f.Consumers[name]
	if !ok {
		f.ConsumersMutex.Unlock()
		return errors.Errorf("consumer not found: %s", name)
	}

	// Remove from map
	delete(f.Consumers, name)
	f.ConsumersMutex.Unlock()

	// Close consumer
	return consumer.Close()
}

// CloseAllConsumers closes all consumers
func (f *PulsarConsumerFactory) CloseAllConsumers() error {
	f.ConsumersMutex.Lock()
	consumers := make([]PulsarConsumer, 0, len(f.Consumers))

	// Get all consumers
	for _, consumer := range f.Consumers {
		consumers = append(consumers, consumer)
	}

	// Clear map
	f.Consumers = make(map[string]PulsarConsumer)
	f.ConsumersMutex.Unlock()

	// Close all consumers
	var errors []error
	for _, consumer := range consumers {
		if err := consumer.Close(); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("errors closing consumers: %v", errors)
	}

	return nil
}

// GetConsumerCount returns the number of consumers
func (f *PulsarConsumerFactory) GetConsumerCount() int {
	f.ConsumersMutex.RLock()
	defer f.ConsumersMutex.RUnlock()

	return len(f.Consumers)
}

// CreatePatternConsumer creates a consumer that subscribes to a topic pattern
func (f *PulsarConsumerFactory) CreatePatternConsumer(
	topicsPattern string,
	subscriptionName string,
	consumerName string,
) (PulsarConsumer, error) {
	// Create options from defaults
	options := f.DefaultOptions
	options.TopicsPattern = topicsPattern
	options.SubscriptionName = subscriptionName
	options.ConsumerName = consumerName

	return f.CreateConsumer(options)
}

// MessageToData converts a Pulsar message to a message.Data
func MessageToData(msg Message) (*message.Data, error) {
	// Create data
	data := &message.Data{
		Payload:     msg.Payload(),
		Properties:  msg.Properties(),
		PublishTime: msg.PublishTime(),
		EventTime:   msg.EventTime(),
		Key:         msg.Key(),
		OrderingKey: msg.OrderingKey(),
		MessageID:   string(msg.ID()),
		Topic:       msg.Topic(),
	}

	return data, nil
}

//Personal.AI order the ending
