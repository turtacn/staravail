// Package config provides configuration loading and validation utilities.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/pflag"

	"github.com/turtacn/staravail/internal/common/logger"
)

var (
	// log is the logger for the config package
	log = logger.GetLogger("config.loader")

	// ErrInvalidConfig is returned when the configuration is invalid
	ErrInvalidConfig = errors.New("invalid configuration")

	// ErrFileNotFound is returned when the configuration file is not found
	ErrFileNotFound = errors.New("configuration file not found")

	// ErrUnsupportedFormat is returned when the configuration file format is not supported
	ErrUnsupportedFormat = errors.New("unsupported configuration file format")

	// ErrInvalidEnvironment is returned when the environment is not valid
	ErrInvalidEnvironment = errors.New("invalid environment")
)

// Environment represents a deployment environment
type Environment string

const (
	// DevEnvironment represents the development environment
	DevEnvironment Environment = "dev"
	// TestEnvironment represents the test environment
	TestEnvironment Environment = "test"
	// StagingEnvironment represents the staging environment
	StagingEnvironment Environment = "staging"
	// ProdEnvironment represents the production environment
	ProdEnvironment Environment = "prod"
)

// Valid checks if the environment is valid
func (e Environment) Valid() bool {
	switch e {
	case DevEnvironment, TestEnvironment, StagingEnvironment, ProdEnvironment:
		return true
	default:
		return false
	}
}

// String returns the string representation of the environment
func (e Environment) String() string {
	return string(e)
}

// ConfigFactory is a factory for creating configuration instances
type ConfigFactory struct {
	// baseConfig is the base configuration
	baseConfig *Config
	// environment is the current environment
	environment Environment
	// configPath is the path to the configuration files
	configPath string
	// watchers is a map of file paths to file watchers
	watchers map[string]*fsnotify.Watcher
	// callbacks is a map of configuration types to callback functions
	callbacks map[string][]ConfigChangeCallback
	// mu protects the factory
	mu sync.RWMutex
	// configInstances is a map of module names to configuration instances
	configInstances map[string]interface{}
}

// ConfigChangeCallback is a callback function that is called when a configuration changes
type ConfigChangeCallback func(oldConfig, newConfig interface{}) error

// NewConfigFactory creates a new configuration factory
func NewConfigFactory(configPath string, environment Environment) (*ConfigFactory, error) {
	if !environment.Valid() {
		return nil, fmt.Errorf("%w: %s", ErrInvalidEnvironment, environment)
	}

	factory := &ConfigFactory{
		environment:     environment,
		configPath:      configPath,
		watchers:        make(map[string]*fsnotify.Watcher),
		callbacks:       make(map[string][]ConfigChangeCallback),
		configInstances: make(map[string]interface{}),
	}

	// Load the base configuration
	baseConfig, err := factory.LoadConfig("")
	if err != nil {
		return nil, fmt.Errorf("failed to load base configuration: %w", err)
	}
	factory.baseConfig = baseConfig

	return factory, nil
}

// LoadConfig loads a configuration for the specified module
func (f *ConfigFactory) LoadConfig(module string) (*Config, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Check if we already have a cached instance
	if module != "" {
		if instance, ok := f.configInstances[module]; ok {
			if cfg, ok := instance.(*Config); ok {
				return cfg, nil
			}
		}
	}

	// Start with default configuration
	config := DefaultConfig()

	// Load base configuration file
	baseConfigPath := filepath.Join(f.configPath, "config.yaml")
	if err := f.loadFromFile(baseConfigPath, config); err != nil && !errors.Is(err, ErrFileNotFound) {
		return nil, fmt.Errorf("failed to load base configuration: %w", err)
	}

	// Load environment-specific configuration
	envConfigPath := filepath.Join(f.configPath, fmt.Sprintf("config.%s.yaml", f.environment))
	if err := f.loadFromFile(envConfigPath, config); err != nil && !errors.Is(err, ErrFileNotFound) {
		return nil, fmt.Errorf("failed to load environment configuration: %w", err)
	}

	// Load module-specific configuration if provided
	if module != "" {
		moduleConfigPath := filepath.Join(f.configPath, fmt.Sprintf("%s.yaml", module))
		if err := f.loadFromFile(moduleConfigPath, config); err != nil && !errors.Is(err, ErrFileNotFound) {
			return nil, fmt.Errorf("failed to load module configuration: %w", err)
		}

		// Load module-environment-specific configuration
		moduleEnvConfigPath := filepath.Join(f.configPath, fmt.Sprintf("%s.%s.yaml", module, f.environment))
		if err := f.loadFromFile(moduleEnvConfigPath, config); err != nil && !errors.Is(err, ErrFileNotFound) {
			return nil, fmt.Errorf("failed to load module-environment configuration: %w", err)
		}
	}

	// Load configuration from environment variables
	if err := f.loadFromEnv(config); err != nil {
		return nil, fmt.Errorf("failed to load configuration from environment variables: %w", err)
	}

	// Load configuration from command line arguments
	if err := f.loadFromArgs(config); err != nil {
		return nil, fmt.Errorf("failed to load configuration from command line arguments: %w", err)
	}

	// Validate the configuration
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	// If we're loading for a module, cache the instance
	if module != "" {
		f.mu.Lock()
		f.configInstances[module] = config
		f.mu.Unlock()

		// Set up hot reload for module-specific configurations
		if err := f.setupHotReload(module, config); err != nil {
			log.Warnf("Failed to set up hot reload for module %s: %v", module, err)
		}
	}

	return config, nil
}

// loadFromFile loads configuration from a YAML file into the config struct
func (f *ConfigFactory) loadFromFile(filePath string, config *Config) error {
	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return fmt.Errorf("%w: %s", ErrFileNotFound, filePath)
	}

	// Read the file
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("error reading file %s: %w", filePath, err)
	}

	// Get the file extension
	ext := strings.ToLower(filepath.Ext(filePath))

	// Parse the file based on its extension
	var configMap map[string]interface{}
	switch ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &configMap); err != nil {
			return fmt.Errorf("error parsing YAML file %s: %w", filePath, err)
		}
	case ".json":
		if err := json.Unmarshal(data, &configMap); err != nil {
			return fmt.Errorf("error parsing JSON file %s: %w", filePath, err)
		}
	default:
		return fmt.Errorf("%w: %s", ErrUnsupportedFormat, ext)
	}

	// Use mapstructure to decode into our struct
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		WeaklyTypedInput: true,
		Result:           config,
		TagName:          "mapstructure",
	})
	if err != nil {
		return fmt.Errorf("error creating decoder: %w", err)
	}

	if err := decoder.Decode(configMap); err != nil {
		return fmt.Errorf("error decoding configuration: %w", err)
	}

	log.Infof("Loaded configuration from file: %s", filePath)
	return nil
}

// loadFromEnv loads configuration from environment variables
func (f *ConfigFactory) loadFromEnv(config *Config) error {
	// Define the environment variable prefix
	prefix := "STARROCKS_PROXY_"

	// Process all fields in the config struct
	configType := reflect.TypeOf(*config)
	configValue := reflect.ValueOf(config).Elem()

	// Recursively process all struct fields
	if err := processEnvVars(configValue, configType, prefix, ""); err != nil {
		return err
	}

	return nil
}

// processEnvVars recursively processes struct fields to apply environment variable overrides
func processEnvVars(value reflect.Value, typ reflect.Type, prefix, path string) error {
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		fieldValue := value.Field(i)

		// Skip unexported fields
		if field.PkgPath != "" {
			continue
		}

		// Get the field name from mapstructure tag or use the field name
		name := field.Tag.Get("mapstructure")
		if name == "" || name == "-" {
			name = strings.ToLower(field.Name)
		}

		// Build the full path and environment variable name
		fieldPath := name
		if path != "" {
			fieldPath = path + "." + name
		}
		envName := prefix + strings.ToUpper(strings.ReplaceAll(fieldPath, ".", "_"))

		// Handle the field based on its kind
		switch fieldValue.Kind() {
		case reflect.Struct:
			// Recursively process struct fields
			if err := processEnvVars(fieldValue, field.Type, prefix, fieldPath); err != nil {
				return err
			}
		default:
			// Check if an environment variable exists for this field
			envValue, exists := os.LookupEnv(envName)
			if !exists {
				continue
			}

			// Apply the environment variable value to the field
			if err := setFieldFromString(fieldValue, envValue); err != nil {
				return fmt.Errorf("error setting field %s from environment variable %s: %w", fieldPath, envName, err)
			}

			log.Debugf("Applied environment variable %s to field %s", envName, fieldPath)
		}
	}

	return nil
}

// setFieldFromString sets a reflect.Value from a string value based on the field's type
func setFieldFromString(value reflect.Value, strValue string) error {
	if !value.CanSet() {
		return fmt.Errorf("field is not settable")
	}

	switch value.Kind() {
	case reflect.String:
		value.SetString(strValue)
	case reflect.Bool:
		boolValue, err := strconv.ParseBool(strValue)
		if err != nil {
			return fmt.Errorf("invalid boolean value: %w", err)
		}
		value.SetBool(boolValue)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		intValue, err := strconv.ParseInt(strValue, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid integer value: %w", err)
		}
		value.SetInt(intValue)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		uintValue, err := strconv.ParseUint(strValue, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid unsigned integer value: %w", err)
		}
		value.SetUint(uintValue)
	case reflect.Float32, reflect.Float64:
		floatValue, err := strconv.ParseFloat(strValue, 64)
		if err != nil {
			return fmt.Errorf("invalid float value: %w", err)
		}
		value.SetFloat(floatValue)
	case reflect.Slice:
		// Handle slice of strings
		if value.Type().Elem().Kind() == reflect.String {
			parts := strings.Split(strValue, ",")
			slice := reflect.MakeSlice(value.Type(), len(parts), len(parts))
			for i, part := range parts {
				slice.Index(i).SetString(strings.TrimSpace(part))
			}
			value.Set(slice)
		} else {
			return fmt.Errorf("unsupported slice type: %s", value.Type().Elem().Kind())
		}
	case reflect.Map:
		// Handle map of string to string
		if value.Type().Key().Kind() == reflect.String && value.Type().Elem().Kind() == reflect.String {
			parts := strings.Split(strValue, ",")
			mapValue := reflect.MakeMap(value.Type())
			for _, part := range parts {
				keyVal := strings.SplitN(strings.TrimSpace(part), "=", 2)
				if len(keyVal) == 2 {
					mapValue.SetMapIndex(reflect.ValueOf(keyVal[0]), reflect.ValueOf(keyVal[1]))
				}
			}
			value.Set(mapValue)
		} else {
			return fmt.Errorf("unsupported map type: %s->%s", value.Type().Key().Kind(), value.Type().Elem().Kind())
		}
	default:
		return fmt.Errorf("unsupported field type: %s", value.Kind())
	}

	return nil
}

// loadFromArgs loads configuration from command line arguments
func (f *ConfigFactory) loadFromArgs(config *Config) error {
	// Use pflag to define and parse command line flags
	// We only define flags for commonly used configuration options

	// Define flags for server configuration
	pflag.String("host", config.Server.Host, "Server host")
	pflag.Int("http-port", config.Server.HTTPPort, "HTTP port")
	pflag.Int("mysql-port", config.Server.MySQLPort, "MySQL port")
	pflag.Int("admin-port", config.Server.AdminPort, "Admin port")
	pflag.Int("max-connections", config.Server.MaxConnections, "Maximum number of connections")

	// Define flags for StarRocks configuration
	pflag.StringSlice("fe-addresses", config.Starrocks.FEAddresses, "StarRocks Frontend addresses")
	pflag.String("username", config.Starrocks.Username, "StarRocks username")
	pflag.String("password", config.Starrocks.Password, "StarRocks password")
	pflag.String("database", config.Starrocks.Database, "StarRocks database")
	pflag.Int("query-timeout", config.Starrocks.QueryTimeout, "Query timeout in seconds")

	// Define flags for logging configuration
	pflag.String("log-level", config.Log.Level, "Log level")
	pflag.String("log-format", config.Log.Format, "Log format")
	pflag.String("log-output", config.Log.OutputPath, "Log output path")
	pflag.Bool("log-console", config.Log.EnableConsole, "Enable console logging")
	pflag.Bool("log-file", config.Log.EnableFile, "Enable file logging")

	// Define flags for monitoring configuration
	pflag.Bool("enable-monitor", config.Monitor.Enable, "Enable monitoring")
	pflag.Int("metrics-port", config.Monitor.MetricsPort, "Metrics port")
	pflag.Bool("enable-tracing", config.Monitor.EnableTracing, "Enable distributed tracing")

	// Define flags for write buffer configuration
	pflag.Bool("enable-buffer", config.WriteBuffer.Enable, "Enable write buffering")
	pflag.Int64("buffer-size", config.WriteBuffer.BufferSize, "Buffer size in bytes")
	pflag.Int("flush-interval", config.WriteBuffer.FlushInterval, "Flush interval in milliseconds")

	// Define flags for batch configuration
	pflag.Bool("enable-batch", config.Batch.Enable, "Enable batch processing")
	pflag.Int("batch-size", config.Batch.BatchSize, "Batch size")
	pflag.Int("batch-interval", config.Batch.BatchInterval, "Batch interval in milliseconds")

	// Define flags for authentication configuration
	pflag.Bool("enable-auth", config.Auth.Enable, "Enable authentication")
	pflag.String("auth-type", config.Auth.Type, "Authentication type")

	// Parse the flags
	pflag.Parse()

	// Apply flag values to the configuration
	if pflag.Changed("host") {
		config.Server.Host, _ = pflag.CommandLine.GetString("host")
	}
	if pflag.Changed("http-port") {
		config.Server.HTTPPort, _ = pflag.CommandLine.GetInt("http-port")
	}
	if pflag.Changed("mysql-port") {
		config.Server.MySQLPort, _ = pflag.CommandLine.GetInt("mysql-port")
	}
	if pflag.Changed("admin-port") {
		config.Server.AdminPort, _ = pflag.CommandLine.GetInt("admin-port")
	}
	if pflag.Changed("max-connections") {
		config.Server.MaxConnections, _ = pflag.CommandLine.GetInt("max-connections")
	}

	if pflag.Changed("fe-addresses") {
		config.Starrocks.FEAddresses, _ = pflag.CommandLine.GetStringSlice("fe-addresses")
	}
	if pflag.Changed("username") {
		config.Starrocks.Username, _ = pflag.CommandLine.GetString("username")
	}
	if pflag.Changed("password") {
		config.Starrocks.Password, _ = pflag.CommandLine.GetString("password")
	}
	if pflag.Changed("database") {
		config.Starrocks.Database, _ = pflag.CommandLine.GetString("database")
	}
	if pflag.Changed("query-timeout") {
		config.Starrocks.QueryTimeout, _ = pflag.CommandLine.GetInt("query-timeout")
	}

	if pflag.Changed("log-level") {
		config.Log.Level, _ = pflag.CommandLine.GetString("log-level")
	}
	if pflag.Changed("log-format") {
		config.Log.Format, _ = pflag.CommandLine.GetString("log-format")
	}
	if pflag.Changed("log-output") {
		config.Log.OutputPath, _ = pflag.CommandLine.GetString("log-output")
	}
	if pflag.Changed("log-console") {
		config.Log.EnableConsole, _ = pflag.CommandLine.GetBool("log-console")
	}
	if pflag.Changed("log-file") {
		config.Log.EnableFile, _ = pflag.CommandLine.GetBool("log-file")
	}

	if pflag.Changed("enable-monitor") {
		config.Monitor.Enable, _ = pflag.CommandLine.GetBool("enable-monitor")
	}
	if pflag.Changed("metrics-port") {
		config.Monitor.MetricsPort, _ = pflag.CommandLine.GetInt("metrics-port")
	}
	if pflag.Changed("enable-tracing") {
		config.Monitor.EnableTracing, _ = pflag.CommandLine.GetBool("enable-tracing")
	}

	if pflag.Changed("enable-buffer") {
		config.WriteBuffer.Enable, _ = pflag.CommandLine.GetBool("enable-buffer")
	}
	if pflag.Changed("buffer-size") {
		config.WriteBuffer.BufferSize, _ = pflag.CommandLine.GetInt64("buffer-size")
	}
	if pflag.Changed("flush-interval") {
		config.WriteBuffer.FlushInterval, _ = pflag.CommandLine.GetInt("flush-interval")
	}

	if pflag.Changed("enable-batch") {
		config.Batch.Enable, _ = pflag.CommandLine.GetBool("enable-batch")
	}
	if pflag.Changed("batch-size") {
		config.Batch.BatchSize, _ = pflag.CommandLine.GetInt("batch-size")
	}
	if pflag.Changed("batch-interval") {
		config.Batch.BatchInterval, _ = pflag.CommandLine.GetInt("batch-interval")
	}

	if pflag.Changed("enable-auth") {
		config.Auth.Enable, _ = pflag.CommandLine.GetBool("enable-auth")
	}
	if pflag.Changed("auth-type") {
		config.Auth.Type, _ = pflag.CommandLine.GetString("auth-type")
	}

	return nil
}

// setupHotReload sets up hot reload for a configuration
func (f *ConfigFactory) setupHotReload(module string, config *Config) error {
	// Create a list of files to watch
	filesToWatch := []string{
		filepath.Join(f.configPath, "config.yaml"),
		filepath.Join(f.configPath, fmt.Sprintf("config.%s.yaml", f.environment)),
	}

	if module != "" {
		filesToWatch = append(filesToWatch,
			filepath.Join(f.configPath, fmt.Sprintf("%s.yaml", module)),
			filepath.Join(f.configPath, fmt.Sprintf("%s.%s.yaml", module, f.environment)),
		)
	}

	// Create a new file watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create file watcher: %w", err)
	}

	// Add the directories containing the files to the watcher
	dirsWatched := make(map[string]bool)
	for _, file := range filesToWatch {
		dir := filepath.Dir(file)
		if _, exists := dirsWatched[dir]; !exists {
			if err := watcher.Add(dir); err != nil {
				watcher.Close()
				return fmt.Errorf("failed to add directory to watcher: %w", err)
			}
			dirsWatched[dir] = true
		}
	}

	// Store the watcher
	f.mu.Lock()
	watcherKey := module
	if watcherKey == "" {
		watcherKey = "base"
	}
	if oldWatcher, exists := f.watchers[watcherKey]; exists {
		oldWatcher.Close()
	}
	f.watchers[watcherKey] = watcher
	f.mu.Unlock()

	// Start watching for changes
	go f.watchConfigFiles(watcherKey, filesToWatch, config)

	return nil
}

// watchConfigFiles watches for changes to configuration files
func (f *ConfigFactory) watchConfigFiles(watcherKey string, files []string, config *Config) {
	f.mu.RLock()
	watcher, exists := f.watchers[watcherKey]
	f.mu.RUnlock()

	if !exists {
		log.Errorf("Watcher not found for key: %s", watcherKey)
		return
	}

	// Create a map of file names to watch
	fileNames := make(map[string]bool)
	for _, file := range files {
		if _, err := os.Stat(file); err == nil {
			fileNames[filepath.Base(file)] = true
		}
	}

	// Create a debouncer to prevent multiple reloads
	var debounceTimer *time.Timer
	var debounceTimerMu sync.Mutex

	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}

			// Check if the event is for one of our config files
			if !fileNames[filepath.Base(event.Name)] {
				continue
			}

			// Check if the event is a write or create event
			if event.Op&(fsnotify.Write|fsnotify.Create) == 0 {
				continue
			}

			// Debounce the reload to prevent multiple reloads for a single change
			debounceTimerMu.Lock()
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			debounceTimer = time.AfterFunc(500*time.Millisecond, func() {
				log.Infof("Config file changed, reloading: %s", event.Name)
				if err := f.reloadConfig(watcherKey, config); err != nil {
					log.Errorf("Failed to reload config: %v", err)
				}
			})
			debounceTimerMu.Unlock()

		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Errorf("Config file watcher error: %v", err)
		}
	}
}

// reloadConfig reloads the configuration for a module
func (f *ConfigFactory) reloadConfig(watcherKey string, oldConfig *Config) error {
	// Determine the module name from the watcher key
	module := watcherKey
	if module == "base" {
		module = ""
	}

	// Create a new configuration instance
	newConfig := DefaultConfig()

	// Load configuration from files, environment variables, and command line arguments
	if err := f.loadConfigForModule(module, newConfig); err != nil {
		return fmt.Errorf("failed to load configuration for module %s: %w", module, err)
	}

	// Validate the new configuration
	if err := validateConfig(newConfig); err != nil {
		return fmt.Errorf("configuration validation failed: %w", err)
	}

	// Update the configuration instance
	f.mu.Lock()
	if module == "" {
		f.baseConfig = newConfig
	} else {
		f.configInstances[module] = newConfig
	}
	f.mu.Unlock()

	// Call the registered callbacks
	f.mu.RLock()
	callbacks, exists := f.callbacks[watcherKey]
	f.mu.RUnlock()

	if exists {
		for _, callback := range callbacks {
			if err := callback(oldConfig, newConfig); err != nil {
				log.Warnf("Config change callback failed: %v", err)
			}
		}
	}

	// Copy values from new config to old config
	*oldConfig = *newConfig

	return nil
}

// loadConfigForModule loads configuration for a module
func (f *ConfigFactory) loadConfigForModule(module string, config *Config) error {
	// Load base configuration file
	baseConfigPath := filepath.Join(f.configPath, "config.yaml")
	if err := f.loadFromFile(baseConfigPath, config); err != nil && !errors.Is(err, ErrFileNotFound) {
		return fmt.Errorf("failed to load base configuration: %w", err)
	}

	// Load environment-specific configuration
	envConfigPath := filepath.Join(f.configPath, fmt.Sprintf("config.%s.yaml", f.environment))
	if err := f.loadFromFile(envConfigPath, config); err != nil && !errors.Is(err, ErrFileNotFound) {
		return fmt.Errorf("failed to load environment configuration: %w", err)
	}

	// Load module-specific configuration if provided
	if module != "" {
		moduleConfigPath := filepath.Join(f.configPath, fmt.Sprintf("%s.yaml", module))
		if err := f.loadFromFile(moduleConfigPath, config); err != nil && !errors.Is(err, ErrFileNotFound) {
			return fmt.Errorf("failed to load module configuration: %w", err)
		}

		// Load module-environment-specific configuration
		moduleEnvConfigPath := filepath.Join(f.configPath, fmt.Sprintf("%s.%s.yaml", module, f.environment))
		if err := f.loadFromFile(moduleEnvConfigPath, config); err != nil && !errors.Is(err, ErrFileNotFound) {
			return fmt.Errorf("failed to load module-environment configuration: %w", err)
		}
	}

	// Load configuration from environment variables
	if err := f.loadFromEnv(config); err != nil {
		return fmt.Errorf("failed to load configuration from environment variables: %w", err)
	}

	// Load configuration from command line arguments
	if err := f.loadFromArgs(config); err != nil {
		return fmt.Errorf("failed to load configuration from command line arguments: %w", err)
	}

	return nil
}

// RegisterConfigChangeCallback registers a callback to be called when a configuration changes
func (f *ConfigFactory) RegisterConfigChangeCallback(module string, callback ConfigChangeCallback) {
	f.mu.Lock()
	defer f.mu.Unlock()

	watcherKey := module
	if watcherKey == "" {
		watcherKey = "base"
	}

	if _, exists := f.callbacks[watcherKey]; !exists {
		f.callbacks[watcherKey] = make([]ConfigChangeCallback, 0)
	}
	f.callbacks[watcherKey] = append(f.callbacks[watcherKey], callback)
}

// GetModuleConfig gets the configuration for a module
func (f *ConfigFactory) GetModuleConfig(module string) (*Config, error) {
	f.mu.RLock()
	instance, exists := f.configInstances[module]
	f.mu.RUnlock()

	if exists {
		if cfg, ok := instance.(*Config); ok {
			return cfg, nil
		}
		return nil, fmt.Errorf("invalid configuration instance for module %s", module)
	}

	// Load the configuration if it doesn't exist
	return f.LoadConfig(module)
}

// GetBaseConfig gets the base configuration
func (f *ConfigFactory) GetBaseConfig() *Config {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.baseConfig
}

// GetEnvironment gets the current environment
func (f *ConfigFactory) GetEnvironment() Environment {
	return f.environment
}

// Close closes the configuration factory and its resources
func (f *ConfigFactory) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	var errs []error
	for key, watcher := range f.watchers {
		if err := watcher.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close watcher for %s: %w", key, err))
		}
	}
	f.watchers = make(map[string]*fsnotify.Watcher)

	if len(errs) > 0 {
		return fmt.Errorf("errors closing config factory: %v", errs)
	}
	return nil
}

// Global configuration factory and instance
var (
	// globalFactory is the global configuration factory
	globalFactory *ConfigFactory
	// globalConfig is the global configuration instance
	globalConfig *Config
	// globalFactoryMu protects the global factory and config
	globalFactoryMu sync.RWMutex
	// globalFactoryOnce ensures the global factory is created only once
	globalFactoryOnce sync.Once
)

// InitGlobalConfig initializes the global configuration
func InitGlobalConfig(configPath string, environment Environment) error {
	var err error
	globalFactoryOnce.Do(func() {
		// Create the configuration factory
		factory, factoryErr := NewConfigFactory(configPath, environment)
		if factoryErr != nil {
			err = factoryErr
			return
		}

		// Load the base configuration
		config, configErr := factory.LoadConfig("")
		if configErr != nil {
			err = configErr
			return
		}

		// Set the global factory and config
		globalFactoryMu.Lock()
		globalFactory = factory
		globalConfig = config
		globalFactoryMu.Unlock()

		// Apply the log configuration
		if logErr := config.ApplyLogConfig(); logErr != nil {
			log.Warnf("Failed to apply log configuration: %v", logErr)
		}
	})

	return err
}

// GetGlobalConfigFactory gets the global configuration factory
func GetGlobalConfigFactory() *ConfigFactory {
	globalFactoryMu.RLock()
	defer globalFactoryMu.RUnlock()
	return globalFactory
}

// GetGlobalConfig gets the global configuration
func GetGlobalConfig() *Config {
	globalFactoryMu.RLock()
	defer globalFactoryMu.RUnlock()
	return globalConfig
}

// GetModuleConfig gets the configuration for a module
func GetModuleConfig(module string) (*Config, error) {
	globalFactoryMu.RLock()
	factory := globalFactory
	globalFactoryMu.RUnlock()

	if factory == nil {
		return nil, fmt.Errorf("global configuration factory not initialized")
	}

	return factory.GetModuleConfig(module)
}

// RegisterGlobalConfigChangeCallback registers a callback to be called when the global configuration changes
func RegisterGlobalConfigChangeCallback(module string, callback ConfigChangeCallback) error {
	globalFactoryMu.RLock()
	factory := globalFactory
	globalFactoryMu.RUnlock()

	if factory == nil {
		return fmt.Errorf("global configuration factory not initialized")
	}

	factory.RegisterConfigChangeCallback(module, callback)
	return nil
}

// MergeConfigs merges two configurations, with the second taking precedence
func MergeConfigs(base, override *Config) *Config {
	result := *base

	// Use reflection to merge the configurations
	baseValue := reflect.ValueOf(base).Elem()
	overrideValue := reflect.ValueOf(override).Elem()
	resultValue := reflect.ValueOf(&result).Elem()

	mergeStructs(baseValue, overrideValue, resultValue)

	return &result
}

// mergeStructs merges two structs using reflection
func mergeStructs(base, override, result reflect.Value) {
	for i := 0; i < base.NumField(); i++ {
		baseField := base.Field(i)
		overrideField := override.Field(i)
		resultField := result.Field(i)

		// Skip unexported fields
		if !baseField.CanInterface() {
			continue
		}

		// Handle struct fields recursively
		if baseField.Kind() == reflect.Struct {
			mergeStructs(baseField, overrideField, resultField)
			continue
		}

		// For other fields, check if the override has a non-zero value
		if !isZeroValue(overrideField) {
			resultField.Set(overrideField)
		}
	}
}

// isZeroValue checks if a value is the zero value for its type
func isZeroValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Complex64, reflect.Complex128:
		return v.Complex() == complex(0, 0)
	case reflect.Array, reflect.Chan, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}

// ValidateStruct validates a struct using its validate tags
func ValidateStruct(s interface{}) error {
	val := reflect.ValueOf(s)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return fmt.Errorf("validate: expected struct, got %s", val.Kind())
	}

	typ := val.Type()
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		if !field.CanInterface() {
			continue
		}

		tag := typ.Field(i).Tag.Get("validate")
		if tag == "" {
			continue
		}

		fieldName := typ.Field(i).Name
		fieldValue := field.Interface()

		// Handle nested structs
		if field.Kind() == reflect.Struct {
			if err := ValidateStruct(fieldValue); err != nil {
				return fmt.Errorf("%s: %w", fieldName, err)
			}
			continue
		}

		// Handle validations for this field
		if err := validateField(fieldName, fieldValue, tag); err != nil {
			return err
		}
	}

	return nil
}

// validateField validates a field based on its validation tag
func validateField(name string, value interface{}, tag string) error {
	validations := strings.Split(tag, ",")
	for _, validation := range validations {
		parts := strings.SplitN(validation, "=", 2)
		validationType := parts[0]

		switch validationType {
		case "required":
			if isEmptyValue(reflect.ValueOf(value)) {
				return fmt.Errorf("field %s is required", name)
			}
		case "min", "max":
			if len(parts) != 2 {
				return fmt.Errorf("invalid validation for field %s: %s", name, validation)
			}
			param, err := strconv.ParseFloat(parts[1], 64)
			if err != nil {
				return fmt.Errorf("invalid validation parameter for field %s: %s", name, validation)
			}

			var actualValue float64
			switch v := value.(type) {
			case int:
				actualValue = float64(v)
			case int8:
				actualValue = float64(v)
			case int16:
				actualValue = float64(v)
			case int32:
				actualValue = float64(v)
			case int64:
				actualValue = float64(v)
			case uint:
				actualValue = float64(v)
			case uint8:
				actualValue = float64(v)
			case uint16:
				actualValue = float64(v)
			case uint32:
				actualValue = float64(v)
			case uint64:
				actualValue = float64(v)
			case float32:
				actualValue = float64(v)
			case float64:
				actualValue = v
			default:
				return fmt.Errorf("field %s has unsupported type for min/max validation", name)
			}

			if validationType == "min" && actualValue < param {
				return fmt.Errorf("field %s must be at least %g (got %g)", name, param, actualValue)
			} else if validationType == "max" && actualValue > param {
				return fmt.Errorf("field %s must be at most %g (got %g)", name, param, actualValue)
			}
		case "oneof":
			if len(parts) != 2 {
				return fmt.Errorf("invalid validation for field %s: %s", name, validation)
			}
			options := strings.Split(parts[1], " ")
			strValue := fmt.Sprintf("%v", value)
			valid := false
			for _, option := range options {
				if option == strValue {
					valid = true
					break
				}
			}
			if !valid {
				return fmt.Errorf("field %s must be one of [%s] (got %s)", name, strings.Join(options, ", "), strValue)
			}
		}
	}

	return nil
}

// isEmptyValue checks if a value is empty
func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	case reflect.Array, reflect.Map, reflect.Slice:
		return v.Len() == 0
	case reflect.Struct:
		return false
	default:
		return false
	}
}

//Personal.AI order the ending
