package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/turtacn/staravail/internal/common/logger"
	"github.com/turtacn/staravail/internal/common/metrics"
	"github.com/turtacn/staravail/internal/core/engine"
	"github.com/turtacn/staravail/internal/core/proxy"
	"github.com/turtacn/staravail/internal/infrastructure/config"
	"github.com/turtacn/staravail/internal/interfaces/http/router"
	"github.com/turtacn/staravail/internal/interfaces/mysql/server"
)

// Command line flags
var (
	// configPath is the path to the configuration file
	configPath string

	// logLevel is the log level (debug, info, warn, error)
	logLevel string

	// version flag indicates whether to print version information
	showVersion bool

	// help flag indicates whether to print help information
	showHelp bool

	// validateConfig flag indicates whether to only validate the config and exit
	validateConfig bool

	// generateConfig flag indicates whether to generate a default config file
	generateConfig bool

	// generateConfigPath is the path where the default config file should be generated
	generateConfigPath string

	// monitoringPort is the port for the monitoring HTTP server
	monitoringPort int

	// debugMode flag enables additional debug features
	debugMode bool
)

func init() {
	// Define and parse command line flags
	flag.StringVar(&configPath, "config", "config.yaml", "path to the configuration file")
	flag.StringVar(&logLevel, "log-level", "", "log level (debug, info, warn, error)")
	flag.BoolVar(&showVersion, "version", false, "print version information and exit")
	flag.BoolVar(&showHelp, "help", false, "print help information and exit")
	flag.BoolVar(&validateConfig, "validate-config", false, "validate configuration and exit")
	flag.BoolVar(&generateConfig, "generate-config", false, "generate default configuration file")
	flag.StringVar(&generateConfigPath, "generate-config-path", "config.yaml", "path for generated configuration file")
	flag.IntVar(&monitoringPort, "monitoring-port", 0, "override monitoring HTTP port")
	flag.BoolVar(&debugMode, "debug", false, "enable debug mode")
}

// main is the entry point of the application
func main() {
	// Parse command line flags
	flag.Parse()

	// Handle help flag
	if showHelp {
		flag.Usage()
		os.Exit(0)
	}

	// Handle version flag
	if showVersion {
		printVersionInfo()
		os.Exit(0)
	}

	// Handle generate config flag
	if generateConfig {
		if err := config.GenerateDefaultConfig(generateConfigPath); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to generate default configuration: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Default configuration generated at %s\n", generateConfigPath)
		os.Exit(0)
	}

	// Initialize a basic logger for startup
	startupLogger, err := logging.NewBasicLogger(logLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize startup logger: %v\n", err)
		os.Exit(1)
	}

	// Load configuration
	startupLogger.Info("Loading configuration", zap.String("path", configPath))
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		startupLogger.Error("Failed to load configuration", zap.Error(err))
		os.Exit(1)
	}

	// Override config with command line arguments if provided
	if logLevel != "" {
		cfg.Logging.Level = logLevel
	}
	if monitoringPort > 0 {
		cfg.Monitoring.Port = monitoringPort
	}
	if debugMode {
		cfg.Debug.Enabled = true
	}

	// Validate configuration
	startupLogger.Info("Validating configuration")
	if err := cfg.Validate(); err != nil {
		startupLogger.Error("Configuration validation failed", zap.Error(err))
		os.Exit(1)
	}

	// If only validating config, exit now
	if validateConfig {
		startupLogger.Info("Configuration validated successfully")
		os.Exit(0)
	}

	// Initialize the real logger with the loaded configuration
	logger, err := logging.NewLogger(cfg.Logging)
	if err != nil {
		startupLogger.Error("Failed to initialize logger", zap.Error(err))
		os.Exit(1)
	}
	defer logger.Sync()

	// Replace the global logger
	zap.ReplaceGlobals(logger)

	// Log startup information
	logger.Info("Starting StarAvail",
		zap.String("version", version.Version),
		zap.String("build_date", version.BuildDate),
		zap.String("git_commit", version.GitCommit),
		zap.Int("cpu_cores", runtime.NumCPU()),
		zap.String("go_version", runtime.Version()),
		zap.String("os", runtime.GOOS),
		zap.String("arch", runtime.GOARCH),
	)

	// Create application context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize metrics system
	logger.Info("Initializing metrics system")
	metricsRegistry, err := metrics.NewRegistry(cfg.Monitoring)
	if err != nil {
		logger.Error("Failed to initialize metrics system", zap.Error(err))
		os.Exit(1)
	}

	// Register basic metrics
	metricsRegistry.RegisterBuildInfo(version.Version, version.GitCommit, version.BuildDate)

	// Initialize security manager
	logger.Info("Initializing security manager")
	securityManager, err := security.NewManager(cfg.Security, logger.Named("security"))
	if err != nil {
		logger.Error("Failed to initialize security manager", zap.Error(err))
		os.Exit(1)
	}

	// Initialize StarRocks client
	logger.Info("Initializing StarRocks client")
	starrocksClient, err := client.NewStarRocksClient(cfg.StarRocks, metricsRegistry, logger.Named("client"))
	if err != nil {
		logger.Error("Failed to initialize StarRocks client", zap.Error(err))
		os.Exit(1)
	}

	// Test StarRocks connection
	logger.Info("Testing StarRocks connection")
	if err := starrocksClient.TestConnection(); err != nil {
		logger.Error("Failed to connect to StarRocks", zap.Error(err))
		os.Exit(1)
	}

	// Initialize health monitor
	logger.Info("Initializing health monitor")
	healthMonitor := health.NewMonitor(
		cfg.Health,
		starrocksClient,
		metricsRegistry,
		logger.Named("health"),
	)

	// Initialize executor factory
	logger.Info("Initializing executor factory")
	executorFactory := engine.NewExecutorFactory(
		starrocksClient,
		metricsRegistry,
	)

	// Initialize executor manager
	logger.Info("Initializing executor manager")
	executorManager := engine.NewExecutorManager(
		executorFactory,
		metricsRegistry,
	)

	// Initialize services
	logger.Info("Initializing core services")

	// Query service
	queryService, err := service.NewQueryService(
		cfg.Services.Query,
		starrocksClient,
		executorManager,
		metricsRegistry,
		logger.Named("query-service"),
	)
	if err != nil {
		logger.Error("Failed to initialize query service", zap.Error(err))
		os.Exit(1)
	}

	// Write service
	writeService, err := service.NewWriteService(
		cfg.Services.Write,
		starrocksClient,
		executorManager,
		metricsRegistry,
		logger.Named("write-service"),
	)
	if err != nil {
		logger.Error("Failed to initialize write service", zap.Error(err))
		os.Exit(1)
	}

	// Schema service
	schemaService, err := service.NewSchemaService(
		cfg.Services.Schema,
		starrocksClient,
		metricsRegistry,
		logger.Named("schema-service"),
	)
	if err != nil {
		logger.Error("Failed to initialize schema service", zap.Error(err))
		os.Exit(1)
	}

	// Health service
	healthService, err := service.NewHealthService(
		cfg.Services.Health,
		healthMonitor,
		metricsRegistry,
		logger.Named("health-service"),
	)
	if err != nil {
		logger.Error("Failed to initialize health service", zap.Error(err))
		os.Exit(1)
	}

	// Create the proxy core
	logger.Info("Creating proxy core")
	proxyCore, err := proxy.NewCore(
		cfg.Proxy,
		queryService,
		writeService,
		schemaService,
		healthService,
		securityManager,
		metricsRegistry,
		logger.Named("proxy-core"),
	)
	if err != nil {
		logger.Error("Failed to create proxy core", zap.Error(err))
		os.Exit(1)
	}

	// Initialize HTTP server
	logger.Info("Initializing HTTP server")
	httpServer, err := http.NewServer(
		cfg.Server.HTTP,
		proxyCore,
		securityManager,
		healthMonitor,
		metricsRegistry,
		logger.Named("http-server"),
	)
	if err != nil {
		logger.Error("Failed to initialize HTTP server", zap.Error(err))
		os.Exit(1)
	}

	// Initialize MySQL server
	logger.Info("Initializing MySQL server")
	mysqlServer, err := mysql.NewServer(
		cfg.Server.MySQL,
		proxyCore,
		securityManager,
		metricsRegistry,
		logger.Named("mysql-server"),
	)
	if err != nil {
		logger.Error("Failed to initialize MySQL server", zap.Error(err))
		os.Exit(1)
	}

	// Create a slice of startable components
	components := []struct {
		name    string
		starter func(context.Context) error
		stopper func(context.Context) error
	}{
		{
			name:    "health monitor",
			starter: healthMonitor.Start,
			stopper: healthMonitor.Stop,
		},
		{
			name:    "proxy core",
			starter: proxyCore.Start,
			stopper: proxyCore.Stop,
		},
		{
			name:    "HTTP server",
			starter: httpServer.Start,
			stopper: httpServer.Stop,
		},
		{
			name:    "MySQL server",
			starter: mysqlServer.Start,
			stopper: mysqlServer.Stop,
		},
	}

	// Create a channel to signal when all components have started
	allStarted := make(chan struct{})

	// Start all components in separate goroutines
	logger.Info("Starting all components")
	for _, component := range components {
		componentName := component.name
		componentStarter := component.starter

		go func() {
			logger.Info("Starting component", zap.String("component", componentName))
			if err := componentStarter(ctx); err != nil {
				logger.Error("Failed to start component",
					zap.String("component", componentName),
					zap.Error(err))
				cancel() // Cancel the context to stop all other components
				return
			}
			logger.Info("Component started successfully", zap.String("component", componentName))
		}()
	}

	// Wait a bit to ensure components are starting up
	time.Sleep(500 * time.Millisecond)

	// Check if context was cancelled during startup
	select {
	case <-ctx.Done():
		logger.Error("Startup failed, exiting")
		os.Exit(1)
	default:
		close(allStarted)
	}

	// Create a channel to receive OS signals
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// Log startup complete
	pid := os.Getpid()
	hostname, _ := os.Hostname()
	executablePath, _ := os.Executable()
	workingDir, _ := os.Getwd()

	logger.Info("StarAvail started successfully",
		zap.Int("pid", pid),
		zap.String("hostname", hostname),
		zap.String("executable", executablePath),
		zap.String("working_dir", workingDir),
		zap.String("config_file", absPath(configPath)),
		zap.String("log_level", cfg.Logging.Level),
	)

	// Print startup message to stdout
	fmt.Printf("\nStarAvail %s started successfully!\n", version.Version)
	fmt.Printf("PID: %d, Hostname: %s\n", pid, hostname)
	fmt.Printf("HTTP API: http://%s:%d\n", cfg.Server.HTTP.Host, cfg.Server.HTTP.Port)
	fmt.Printf("MySQL Protocol: %s:%d\n", cfg.Server.MySQL.Host, cfg.Server.MySQL.Port)
	fmt.Printf("Monitoring: http://%s:%d/metrics\n", cfg.Monitoring.Host, cfg.Monitoring.Port)
	fmt.Printf("\nPress Ctrl+C to stop the server\n\n")

	// Wait for termination signal
	sig := <-signalChan
	logger.Info("Received signal, shutting down", zap.String("signal", sig.String()))

	// Create a context with timeout for graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Proxy.ShutdownTimeout)
	defer shutdownCancel()

	// Stop all components in reverse order
	logger.Info("Stopping all components")
	for i := len(components) - 1; i >= 0; i-- {
		componentName := components[i].name
		componentStopper := components[i].stopper

		logger.Info("Stopping component", zap.String("component", componentName))
		if err := componentStopper(shutdownCtx); err != nil {
			logger.Error("Error stopping component",
				zap.String("component", componentName),
				zap.Error(err))
		} else {
			logger.Info("Component stopped successfully", zap.String("component", componentName))
		}
	}

	// Close StarRocks client
	logger.Info("Closing StarRocks client connection")
	if err := starrocksClient.Close(); err != nil {
		logger.Error("Error closing StarRocks client", zap.Error(err))
	}

	// Final cleanup
	logger.Info("Shutdown complete")
}

// printVersionInfo prints the version information
func printVersionInfo() {
	fmt.Printf("StarAvail %s\n", version.Version)
	fmt.Printf("Git Commit: %s\n", version.GitCommit)
	fmt.Printf("Build Date: %s\n", version.BuildDate)
	fmt.Printf("Go Version: %s\n", runtime.Version())
	fmt.Printf("OS/Arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
}

// absPath returns the absolute path of a file
func absPath(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return abs
}

//Personal.AI order the ending
