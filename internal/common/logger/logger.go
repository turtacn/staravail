// Package logger provides a unified logging component for the project.
package logger

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Field represents a log field.
type Field struct {
	Key   string
	Value interface{}
}

// Level represents the severity level of a log message.
type Level int

const (
	// DebugLevel is used for development-mode debugging.
	DebugLevel Level = iota
	// InfoLevel is used for production-mode information.
	InfoLevel
	// WarnLevel is used for warning conditions.
	WarnLevel
	// ErrorLevel is used for error conditions.
	ErrorLevel
	// FatalLevel is used for critical conditions that require immediate attention.
	FatalLevel
)

// String returns a string representation of the log level.
func (l Level) String() string {
	switch l {
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	case FatalLevel:
		return "FATAL"
	default:
		return fmt.Sprintf("Level(%d)", l)
	}
}

// ToZapLevel converts our level to zap's level.
func (l Level) ToZapLevel() zapcore.Level {
	switch l {
	case DebugLevel:
		return zapcore.DebugLevel
	case InfoLevel:
		return zapcore.InfoLevel
	case WarnLevel:
		return zapcore.WarnLevel
	case ErrorLevel:
		return zapcore.ErrorLevel
	case FatalLevel:
		return zapcore.FatalLevel
	default:
		return zapcore.InfoLevel
	}
}

// FromZapLevel converts zap's level to our level.
func FromZapLevel(level zapcore.Level) Level {
	switch level {
	case zapcore.DebugLevel:
		return DebugLevel
	case zapcore.InfoLevel:
		return InfoLevel
	case zapcore.WarnLevel:
		return WarnLevel
	case zapcore.ErrorLevel:
		return ErrorLevel
	case zapcore.FatalLevel, zapcore.DPanicLevel, zapcore.PanicLevel:
		return FatalLevel
	default:
		return InfoLevel
	}
}

// Logger defines the interface for logging operations.
type Logger interface {
	// Debug logs a message at debug level.
	Debug(msg string, fields ...Field)
	// Info logs a message at info level.
	Info(msg string, fields ...Field)
	// Warn logs a message at warn level.
	Warn(msg string, fields ...Field)
	// Error logs a message at error level.
	Error(msg string, fields ...Field)
	// Fatal logs a message at fatal level and then calls os.Exit(1).
	Fatal(msg string, fields ...Field)

	// Debugf logs a formatted message at debug level.
	Debugf(format string, args ...interface{})
	// Infof logs a formatted message at info level.
	Infof(format string, args ...interface{})
	// Warnf logs a formatted message at warn level.
	Warnf(format string, args ...interface{})
	// Errorf logs a formatted message at error level.
	Errorf(format string, args ...interface{})
	// Fatalf logs a formatted message at fatal level and then calls os.Exit(1).
	Fatalf(format string, args ...interface{})

	// WithFields returns a new Logger with the given fields added to each log entry.
	WithFields(fields ...Field) Logger
	// WithField returns a new Logger with the given field added to each log entry.
	WithField(key string, value interface{}) Logger
	// WithContext returns a new Logger with context values added to each log entry.
	WithContext(ctx context.Context) Logger

	// SetLevel sets the minimum log level that will be logged.
	SetLevel(level Level)
	// GetLevel returns the current log level.
	GetLevel() Level

	// Sync flushes any buffered log entries.
	Sync() error
}

// Config represents the configuration for a logger.
type Config struct {
	// Level is the minimum log level that will be logged.
	Level Level `json:"level"`
	// Format is the log format to use. Valid values are "json", "console".
	Format string `json:"format"`
	// EnableConsole specifies whether to log to the console.
	EnableConsole bool `json:"enable_console"`
	// ConsoleLevel is the minimum log level that will be logged to the console.
	ConsoleLevel Level `json:"console_level"`
	// EnableFile specifies whether to log to a file.
	EnableFile bool `json:"enable_file"`
	// FileLevel is the minimum log level that will be logged to the file.
	FileLevel Level `json:"file_level"`
	// FilePath is the path to the log file.
	FilePath string `json:"file_path"`
	// FileMaxSize is the maximum size in megabytes of the log file before it gets rotated.
	FileMaxSize int `json:"file_max_size"`
	// FileMaxBackups is the maximum number of old log files to retain.
	FileMaxBackups int `json:"file_max_backups"`
	// FileMaxAge is the maximum number of days to retain old log files.
	FileMaxAge int `json:"file_max_age"`
	// FileCompress specifies whether the rotated log files should be compressed.
	FileCompress bool `json:"file_compress"`
	// EnableSampling specifies whether to enable log sampling.
	EnableSampling bool `json:"enable_sampling"`
	// SamplingInitial is the initial number of entries to log for each level and message.
	SamplingInitial int `json:"sampling_initial"`
	// SamplingThereafter is the number of entries to log after the initial for each level and message.
	SamplingThereafter int `json:"sampling_thereafter"`
	// ContextKeys is a list of context keys to include in the log.
	ContextKeys []string `json:"context_keys"`
	// DevelopmentMode specifies whether to run in development mode.
	DevelopmentMode bool `json:"development_mode"`
	// DisableCaller disables including the caller in the log.
	DisableCaller bool `json:"disable_caller"`
	// DisableStacktrace disables including the stacktrace in the log.
	DisableStacktrace bool `json:"disable_stacktrace"`
}

// DefaultConfig returns a default configuration for the logger.
func DefaultConfig() Config {
	return Config{
		Level:              InfoLevel,
		Format:             "json",
		EnableConsole:      true,
		ConsoleLevel:       InfoLevel,
		EnableFile:         false,
		FileLevel:          InfoLevel,
		FilePath:           "logs/app.log",
		FileMaxSize:        100,
		FileMaxBackups:     10,
		FileMaxAge:         30,
		FileCompress:       true,
		EnableSampling:     false,
		SamplingInitial:    100,
		SamplingThereafter: 100,
		ContextKeys:        []string{"request_id", "user_id", "session_id"},
		DevelopmentMode:    false,
		DisableCaller:      false,
		DisableStacktrace:  false,
	}
}

// zapLogger is an implementation of Logger that uses zap.
type zapLogger struct {
	logger *zap.Logger
	level  Level
	config Config
	fields []Field
}

// NewZapLogger creates a new zapLogger with the given configuration.
func NewZapLogger(config Config) (Logger, error) {
	// Create the encoder config
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "timestamp",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "message",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.MillisDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	// Set development mode encoder config if enabled
	if config.DevelopmentMode {
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		encoderConfig.EncodeCaller = zapcore.FullCallerEncoder
	}

	// Configure outputs
	var cores []zapcore.Core

	// Console output
	if config.EnableConsole {
		var encoder zapcore.Encoder
		if config.Format == "json" {
			encoder = zapcore.NewJSONEncoder(encoderConfig)
		} else {
			encoder = zapcore.NewConsoleEncoder(encoderConfig)
		}

		consoleCore := zapcore.NewCore(
			encoder,
			zapcore.Lock(os.Stdout),
			zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
				return lvl >= config.ConsoleLevel.ToZapLevel()
			}),
		)
		cores = append(cores, consoleCore)
	}

	// File output
	if config.EnableFile {
		// Ensure the log directory exists
		logDir := filepath.Dir(config.FilePath)
		if err := os.MkdirAll(logDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create log directory: %w", err)
		}

		// Create the log writer with rotation
		writer := &lumberjack.Logger{
			Filename:   config.FilePath,
			MaxSize:    config.FileMaxSize,
			MaxBackups: config.FileMaxBackups,
			MaxAge:     config.FileMaxAge,
			Compress:   config.FileCompress,
		}

		var encoder zapcore.Encoder
		if config.Format == "json" {
			encoder = zapcore.NewJSONEncoder(encoderConfig)
		} else {
			encoder = zapcore.NewConsoleEncoder(encoderConfig)
		}

		fileCore := zapcore.NewCore(
			encoder,
			zapcore.AddSync(writer),
			zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
				return lvl >= config.FileLevel.ToZapLevel()
			}),
		)
		cores = append(cores, fileCore)
	}

	// Create the core
	core := zapcore.NewTee(cores...)

	// Configure sampling if enabled
	if config.EnableSampling {
		core = zapcore.NewSamplerWithOptions(
			core,
			time.Second,
			config.SamplingInitial,
			config.SamplingThereafter,
		)
	}

	// Create the logger
	zapOptions := []zap.Option{
		zap.ErrorOutput(zapcore.Lock(os.Stderr)),
	}

	if !config.DisableCaller {
		zapOptions = append(zapOptions, zap.AddCaller())
	}

	if !config.DisableStacktrace {
		zapOptions = append(zapOptions, zap.AddStacktrace(zapcore.ErrorLevel))
	}

	logger := zap.New(core, zapOptions...)

	return &zapLogger{
		logger: logger,
		level:  config.Level,
		config: config,
		fields: []Field{},
	}, nil
}

// toZapFields converts our fields to zap fields.
func toZapFields(fields []Field) []zap.Field {
	zapFields := make([]zap.Field, len(fields))
	for i, field := range fields {
		zapFields[i] = zap.Any(field.Key, field.Value)
	}
	return zapFields
}

// Debug logs a message at debug level.
func (l *zapLogger) Debug(msg string, fields ...Field) {
	if l.level > DebugLevel {
		return
	}
	allFields := append(l.fields, fields...)
	l.logger.Debug(msg, toZapFields(allFields)...)
}

// Info logs a message at info level.
func (l *zapLogger) Info(msg string, fields ...Field) {
	if l.level > InfoLevel {
		return
	}
	allFields := append(l.fields, fields...)
	l.logger.Info(msg, toZapFields(allFields)...)
}

// Warn logs a message at warn level.
func (l *zapLogger) Warn(msg string, fields ...Field) {
	if l.level > WarnLevel {
		return
	}
	allFields := append(l.fields, fields...)
	l.logger.Warn(msg, toZapFields(allFields)...)
}

// Error logs a message at error level.
func (l *zapLogger) Error(msg string, fields ...Field) {
	if l.level > ErrorLevel {
		return
	}
	allFields := append(l.fields, fields...)
	l.logger.Error(msg, toZapFields(allFields)...)
}

// Fatal logs a message at fatal level and then calls os.Exit(1).
func (l *zapLogger) Fatal(msg string, fields ...Field) {
	if l.level > FatalLevel {
		return
	}
	allFields := append(l.fields, fields...)
	l.logger.Fatal(msg, toZapFields(allFields)...)
}

// Debugf logs a formatted message at debug level.
func (l *zapLogger) Debugf(format string, args ...interface{}) {
	if l.level > DebugLevel {
		return
	}
	l.logger.Debug(fmt.Sprintf(format, args...), toZapFields(l.fields)...)
}

// Infof logs a formatted message at info level.
func (l *zapLogger) Infof(format string, args ...interface{}) {
	if l.level > InfoLevel {
		return
	}
	l.logger.Info(fmt.Sprintf(format, args...), toZapFields(l.fields)...)
}

// Warnf logs a formatted message at warn level.
func (l *zapLogger) Warnf(format string, args ...interface{}) {
	if l.level > WarnLevel {
		return
	}
	l.logger.Warn(fmt.Sprintf(format, args...), toZapFields(l.fields)...)
}

// Errorf logs a formatted message at error level.
func (l *zapLogger) Errorf(format string, args ...interface{}) {
	if l.level > ErrorLevel {
		return
	}
	l.logger.Error(fmt.Sprintf(format, args...), toZapFields(l.fields)...)
}

// Fatalf logs a formatted message at fatal level and then calls os.Exit(1).
func (l *zapLogger) Fatalf(format string, args ...interface{}) {
	if l.level > FatalLevel {
		return
	}
	l.logger.Fatal(fmt.Sprintf(format, args...), toZapFields(l.fields)...)
}

// WithFields returns a new Logger with the given fields added to each log entry.
func (l *zapLogger) WithFields(fields ...Field) Logger {
	newFields := make([]Field, len(l.fields)+len(fields))
	copy(newFields, l.fields)
	copy(newFields[len(l.fields):], fields)

	return &zapLogger{
		logger: l.logger,
		level:  l.level,
		config: l.config,
		fields: newFields,
	}
}

// WithField returns a new Logger with the given field added to each log entry.
func (l *zapLogger) WithField(key string, value interface{}) Logger {
	return l.WithFields(Field{Key: key, Value: value})
}

// WithContext returns a new Logger with context values added to each log entry.
func (l *zapLogger) WithContext(ctx context.Context) Logger {
	if ctx == nil {
		return l
	}

	logger := l
	for _, key := range l.config.ContextKeys {
		if value := ctx.Value(key); value != nil {
			logger = logger.WithField(key, value).(*zapLogger)
		}
	}
	return logger
}

// SetLevel sets the minimum log level that will be logged.
func (l *zapLogger) SetLevel(level Level) {
	l.level = level
}

// GetLevel returns the current log level.
func (l *zapLogger) GetLevel() Level {
	return l.level
}

// Sync flushes any buffered log entries.
func (l *zapLogger) Sync() error {
	return l.logger.Sync()
}

// Factory is a factory for creating loggers.
type Factory struct {
	config  Config
	mu      sync.RWMutex
	loggers map[string]Logger
}

// NewFactory creates a new logger factory with the given configuration.
func NewFactory(config Config) *Factory {
	return &Factory{
		config:  config,
		loggers: make(map[string]Logger),
	}
}

// GetLogger returns a logger for the given module.
func (f *Factory) GetLogger(module string) Logger {
	f.mu.RLock()
	logger, exists := f.loggers[module]
	f.mu.RUnlock()

	if exists {
		return logger
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Check again in case another goroutine created it
	logger, exists = f.loggers[module]
	if exists {
		return logger
	}

	// Create a new logger
	zapLogger, err := NewZapLogger(f.config)
	if err != nil {
		// Fall back to a default logger if we can't create one
		zapLogger, _ = NewZapLogger(DefaultConfig())
	}

	// Add the module name as a field
	logger = zapLogger.WithField("module", module)
	f.loggers[module] = logger

	return logger
}

// UpdateConfig updates the configuration for all loggers.
func (f *Factory) UpdateConfig(config Config) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.config = config

	// Create a new default logger with the updated config
	defaultLogger, err := NewZapLogger(config)
	if err != nil {
		return err
	}

	// Update all existing loggers
	for module, oldLogger := range f.loggers {
		// Get the existing fields except for the module
		fields := make([]Field, 0)
		if zapLogger, ok := oldLogger.(*zapLogger); ok {
			for _, field := range zapLogger.fields {
				if field.Key != "module" {
					fields = append(fields, field)
				}
			}
		}

		// Create a new logger with the updated config
		newLogger := defaultLogger.WithField("module", module)
		if len(fields) > 0 {
			newLogger = newLogger.WithFields(fields...)
		}

		f.loggers[module] = newLogger
	}

	return nil
}

// Global logger instance and factory for convenience
var (
	defaultFactory *Factory
	defaultLogger  Logger
	once           sync.Once
)

// init initializes the global logger with default configuration.
func init() {
	once.Do(func() {
		config := DefaultConfig()
		factory := NewFactory(config)
		defaultFactory = factory
		defaultLogger = factory.GetLogger("global")
	})
}

// Debug logs a message at debug level using the global logger.
func Debug(msg string, fields ...Field) {
	defaultLogger.Debug(msg, fields...)
}

// Info logs a message at info level using the global logger.
func Info(msg string, fields ...Field) {
	defaultLogger.Info(msg, fields...)
}

// Warn logs a message at warn level using the global logger.
func Warn(msg string, fields ...Field) {
	defaultLogger.Warn(msg, fields...)
}

// Error logs a message at error level using the global logger.
func Error(msg string, fields ...Field) {
	defaultLogger.Error(msg, fields...)
}

// Fatal logs a message at fatal level and then calls os.Exit(1) using the global logger.
func Fatal(msg string, fields ...Field) {
	defaultLogger.Fatal(msg, fields...)
}

// Debugf logs a formatted message at debug level using the global logger.
func Debugf(format string, args ...interface{}) {
	defaultLogger.Debugf(format, args...)
}

// Infof logs a formatted message at info level using the global logger.
func Infof(format string, args ...interface{}) {
	defaultLogger.Infof(format, args...)
}

// Warnf logs a formatted message at warn level using the global logger.
func Warnf(format string, args ...interface{}) {
	defaultLogger.Warnf(format, args...)
}

// Errorf logs a formatted message at error level using the global logger.
func Errorf(format string, args ...interface{}) {
	defaultLogger.Errorf(format, args...)
}

// Fatalf logs a formatted message at fatal level and then calls os.Exit(1) using the global logger.
func Fatalf(format string, args ...interface{}) {
	defaultLogger.Fatalf(format, args...)
}

// WithFields returns a new Logger with the given fields added to each log entry.
func WithFields(fields ...Field) Logger {
	return defaultLogger.WithFields(fields...)
}

// WithField returns a new Logger with the given field added to each log entry.
func WithField(key string, value interface{}) Logger {
	return defaultLogger.WithField(key, value)
}

// WithContext returns a new Logger with context values added to each log entry.
func WithContext(ctx context.Context) Logger {
	return defaultLogger.WithContext(ctx)
}

// SetLevel sets the minimum log level that will be logged for the global logger.
func SetLevel(level Level) {
	defaultLogger.SetLevel(level)
}

// GetLevel returns the current log level for the global logger.
func GetLevel() Level {
	return defaultLogger.GetLevel()
}

// Sync flushes any buffered log entries for the global logger.
func Sync() error {
	return defaultLogger.Sync()
}

// GetLogger returns a logger for the given module from the default factory.
func GetLogger(module string) Logger {
	return defaultFactory.GetLogger(module)
}

// UpdateConfig updates the configuration for all loggers in the default factory.
func UpdateConfig(config Config) error {
	return defaultFactory.UpdateConfig(config)
}

// F is a shorthand for creating a Field.
func F(key string, value interface{}) Field {
	return Field{Key: key, Value: value}
}

// GetWriter returns an io.Writer that writes to the logger at the specified level.
// This can be used to redirect output from other libraries to the logger.
func GetWriter(logger Logger, level Level) io.Writer {
	return &logWriter{logger: logger, level: level}
}

// logWriter is an io.Writer that writes to a logger.
type logWriter struct {
	logger Logger
	level  Level
}

// Write implements io.Writer.
func (w *logWriter) Write(p []byte) (n int, err error) {
	msg := string(p)
	switch w.level {
	case DebugLevel:
		w.logger.Debug(msg)
	case InfoLevel:
		w.logger.Info(msg)
	case WarnLevel:
		w.logger.Warn(msg)
	case ErrorLevel:
		w.logger.Error(msg)
	case FatalLevel:
		w.logger.Fatal(msg)
	default:
		w.logger.Info(msg)
	}
	return len(p), nil
}

//Personal.AI order the ending
