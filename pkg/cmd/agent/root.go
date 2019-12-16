// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg"
	"github.com/scylladb/mermaid/pkg/cpuset"
	"github.com/scylladb/mermaid/pkg/rclone"
	"github.com/scylladb/mermaid/pkg/rclone/rcserver"
	"github.com/scylladb/mermaid/pkg/util/httppprof"
	"github.com/scylladb/mermaid/pkg/util/netwait"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var rootArgs = struct {
	configFile string
	version    bool
}{}

var rootCmd = &cobra.Command{
	Use:           "scylla-manager",
	Short:         "Scylla Manager server",
	Args:          cobra.NoArgs,
	SilenceUsage:  true,
	SilenceErrors: true,

	RunE: func(cmd *cobra.Command, args []string) (runError error) {
		// Print version and return
		if rootArgs.version {
			fmt.Fprintf(cmd.OutOrStdout(), "%s\n", pkg.Version())
			return nil
		}

		c, err := parseAndValidateConfigFile(rootArgs.configFile)
		if err != nil {
			return err
		}

		// Get a base context with tracing id
		ctx := log.WithNewTraceID(context.Background())

		logger, err := logger(c)
		if err != nil {
			return errors.Wrapf(err, "logger")
		}
		defer func() {
			if runError != nil {
				logger.Error(ctx, "Bye", "error", runError)
			} else {
				logger.Info(ctx, "Bye")
			}
			logger.Sync() // nolint
		}()

		// Redirect standard logger to the logger
		zap.RedirectStdLog(log.BaseOf(logger))
		// Set logger to netwait
		netwait.DefaultWaiter.Logger = logger.Named("wait")

		logger.Info(ctx, "Scylla Manager Agent", "version", pkg.Version())

		// Wait for Scylla API to be available
		addr := net.JoinHostPort(c.Scylla.APIAddress, c.Scylla.APIPort)
		if _, err := netwait.AnyAddr(ctx, addr); err != nil {
			return errors.Wrapf(
				err,
				"no connection to Scylla API, make sure that Scylla server is running and api_address and api_port are set correctly in config file %s",
				rootArgs.configFile,
			)
		}

		// Update configuration from the REST API
		if err := c.enrichConfigFromAPI(ctx, addr); err != nil {
			return err
		}

		logger.Info(ctx, "Using config", "config", obfuscateSecrets(c), "config_file", rootArgs.configFile)

		// Instruct users to set auth token
		if c.AuthToken == "" {
			ip, _, _ := net.SplitHostPort(c.HTTPS)
			logger.Info(ctx, "WARNING! Scylla data may be exposed on IP "+ip+", "+
				"protect it by specifying auth_token in config file", "config_file", rootArgs.configFile,
			)
		}

		// Try to get a CPU to pin to
		var cpu = c.CPU
		if cpu == noCPU {
			if c, err := findFreeCPU(); err != nil {
				if cause := errors.Cause(err); os.IsNotExist(cause) || cause == cpuset.ErrNoCPUSetConfig {
					// Ignore if there is no cpuset file
					logger.Debug(ctx, "Failed to find CPU to pin to", "error", err)
				} else {
					logger.Error(ctx, "Failed to find CPU to pin to", "error", err)
				}
			} else {
				cpu = c
			}
		}
		// Pin to CPU if possible
		if cpu == noCPU {
			logger.Info(ctx, "Running on all CPUs")
		} else {
			if err := pinToCPU(cpu); err != nil {
				logger.Error(ctx, "Failed to pin to CPU", "cpu", cpu, "error", err)
			} else {
				logger.Info(ctx, "Running on CPU", "cpu", cpu)
			}
		}

		// Redirect rclone logger to the logger
		rclone.RedirectLogPrint(logger.Named("rclone"))
		// Init rclone config options
		rclone.InitFsConfig()
		// Register rclone providers
		if err := rclone.RegisterLocalDirProvider("data", "Jailed Scylla data", c.Scylla.DataDirectory); err != nil {
			return err
		}
		if err := rclone.RegisterS3Provider(c.S3); err != nil {
			return err
		}

		// Start servers
		errCh := make(chan error)

		logger.Info(ctx, "Starting HTTPS server", "address", c.HTTPS)
		go func() {
			server := http.Server{
				Addr:    c.HTTPS,
				Handler: newRouter(c, rcserver.New(), logger.Named("http")),
			}
			errCh <- errors.Wrap(server.ListenAndServeTLS(c.TLSCertFile, c.TLSKeyFile), "HTTPS server start")
		}()

		if c.Prometheus != "" {
			logger.Info(ctx, "Starting Prometheus server", "address", c.Prometheus)
			go func() {
				prometheusServer := &http.Server{
					Addr:    c.Prometheus,
					Handler: promhttp.Handler(),
				}
				errCh <- errors.Wrap(prometheusServer.ListenAndServe(), "prometheus server")
			}()
		}

		if c.Debug != "" {
			go func() {
				logger.Info(ctx, "Starting debug server", "address", c.Debug)
				server := http.Server{
					Addr:    c.Debug,
					Handler: httppprof.Handler(),
				}
				errCh <- errors.Wrap(server.ListenAndServe(), "debug server start")
			}()
		}

		logger.Info(ctx, "Service started")

		return <-errCh
	},
}

func logger(c config) (log.Logger, error) {
	if c.Logger.Development {
		return log.NewDevelopmentWithLevel(c.Logger.Level), nil
	}

	return log.NewProduction(log.Config{
		Mode:  c.Logger.Mode,
		Level: c.Logger.Level,
	})
}

func obfuscateSecrets(c config) config {
	cfg := c
	cfg.AuthToken = strings.Repeat("*", len(cfg.AuthToken))
	return cfg
}

func init() {
	f := rootCmd.Flags()
	f.StringVarP(&rootArgs.configFile, "config-file", "c", "/etc/scylla-manager-agent/scylla-manager-agent.yaml", "configuration file `path`")
	f.BoolVar(&rootArgs.version, "version", false, "print product version and exit")
}
