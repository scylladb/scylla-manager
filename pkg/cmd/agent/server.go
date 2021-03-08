// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg"
	"github.com/scylladb/scylla-manager/pkg/config"
	"github.com/scylladb/scylla-manager/pkg/config/enrich"
	"github.com/scylladb/scylla-manager/pkg/rclone"
	"github.com/scylladb/scylla-manager/pkg/rclone/rcserver"
	"github.com/scylladb/scylla-manager/pkg/util/cpuset"
	"github.com/scylladb/scylla-manager/pkg/util/httppprof"
	"github.com/scylladb/scylla-manager/pkg/util/netwait"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type server struct {
	config config.AgentConfig
	logger log.Logger

	httpsServer      *http.Server
	prometheusServer *http.Server
	debugServer      *http.Server

	errCh chan error
}

func newServer(c config.AgentConfig, logger log.Logger) *server {
	return &server{
		config: c,
		logger: logger,
		errCh:  make(chan error, 4),
	}
}

func (s *server) init(ctx context.Context) error {
	// Redirect standard logger to the logger
	zap.RedirectStdLog(log.BaseOf(s.logger))
	// Set s.logger to netwait
	netwait.DefaultWaiter.Logger = s.logger.Named("wait")

	s.logger.Info(ctx, "Scylla Manager Agent", "version", pkg.Version(), "pid", os.Getpid())

	// Wait for Scylla API to be available
	addr := net.JoinHostPort(s.config.Scylla.APIAddress, s.config.Scylla.APIPort)
	if _, err := netwait.AnyAddr(ctx, addr); err != nil {
		return errors.Wrapf(
			err,
			"no connection to Scylla API, make sure that Scylla server is running and api_address and api_port are set correctly in config file %s",
			rootArgs.configFiles,
		)
	}

	// Update configuration from the REST API
	if err := enrich.AgentConfigFromAPI(ctx, addr, &s.config); err != nil {
		return err
	}

	s.logger.Info(ctx, "Using config", "config", config.ObfuscatedAgentConfig(s.config), "config_files", rootArgs.configFiles)

	// Instruct users to set auth token
	if s.config.AuthToken == "" {
		ip, _, _ := net.SplitHostPort(s.config.HTTPS)
		s.logger.Info(ctx, "WARNING! Scylla data may be exposed on IP "+ip+", "+
			"protect it by specifying auth_token in config file", "config_files", rootArgs.configFiles,
		)
	}

	// Try to get a CPU to pin to
	cpu := s.config.CPU
	if cpu == config.NoCPU {
		if c, err := findFreeCPU(); err != nil {
			if os.IsNotExist(errors.Cause(err)) || errors.Is(err, cpuset.ErrNoCPUSetConfig) {
				// Ignore if there is no cpuset file
				s.logger.Debug(ctx, "Failed to find CPU to pin to", "error", err)
			} else {
				s.logger.Error(ctx, "Failed to find CPU to pin to", "error", err)
			}
		} else {
			cpu = c
		}
	}
	// Pin to CPU if possible
	if cpu == config.NoCPU {
		s.logger.Info(ctx, "Running on all CPUs")
	} else {
		if err := pinToCPU(cpu); err != nil {
			s.logger.Error(ctx, "Failed to pin to CPU", "cpu", cpu, "error", err)
		} else {
			s.logger.Info(ctx, "Running on CPU", "cpu", cpu)
		}
	}

	// Redirect rclone logger to the ogger
	rclone.RedirectLogPrint(s.logger.Named("rclone"))
	// Init rclone config options
	rclone.InitFsConfigWithOptions(s.config.Rclone)
	// Register rclone providers
	if err := rclone.RegisterLocalDirProvider("data", "Jailed Scylla data", s.config.Scylla.DataDirectory); err != nil {
		return err
	}
	if err := rclone.RegisterS3Provider(s.config.S3); err != nil {
		return err
	}
	if err := rclone.RegisterGCSProvider(s.config.GCS); err != nil {
		return err
	}

	if err := rclone.RegisterAzureProvider(s.config.Azure); err != nil {
		return err
	}

	return nil
}

func (s *server) makeHTTPServers() {
	s.httpsServer = &http.Server{
		Addr:    s.config.HTTPS,
		Handler: newRouter(s.config, rcserver.New(), s.logger.Named("http")),
	}
	if s.config.Prometheus != "" {
		s.prometheusServer = &http.Server{
			Addr:    s.config.Prometheus,
			Handler: promhttp.Handler(),
		}
	}
	if s.config.Debug != "" {
		s.debugServer = &http.Server{
			Addr:    s.config.Debug,
			Handler: httppprof.Handler(),
		}
	}
}

func (s *server) startServers(ctx context.Context) {
	if s.httpsServer != nil {
		s.logger.Info(ctx, "Starting HTTPS server", "address", s.httpsServer.Addr)
		go func() {
			s.errCh <- errors.Wrap(s.httpsServer.ListenAndServeTLS(s.config.TLSCertFile, s.config.TLSKeyFile), "HTTPS server start")
		}()
	}

	if s.prometheusServer != nil {
		s.logger.Info(ctx, "Starting Prometheus server", "address", s.prometheusServer.Addr)
		go func() {
			s.errCh <- errors.Wrap(s.prometheusServer.ListenAndServe(), "prometheus server start")
		}()
	}

	if s.debugServer != nil {
		s.logger.Info(ctx, "Starting debug server", "address", s.debugServer.Addr)
		go func() {
			s.errCh <- errors.Wrap(s.debugServer.ListenAndServe(), "debug server start")
		}()
	}

	s.logger.Info(ctx, "Service started")
}

func (s *server) shutdownServers(ctx context.Context, timeout time.Duration) {
	s.logger.Info(ctx, "Closing servers", "timeout", timeout)

	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var eg errgroup.Group
	eg.Go(s.shutdownHTTPServer(tctx, s.httpsServer))
	eg.Go(s.shutdownHTTPServer(tctx, s.prometheusServer))
	eg.Go(s.shutdownHTTPServer(tctx, s.debugServer))
	eg.Wait() // nolint: errcheck
}

func (s *server) shutdownHTTPServer(ctx context.Context, server *http.Server) func() error {
	return func() error {
		if server == nil {
			return nil
		}
		if err := server.Shutdown(ctx); err != nil {
			s.logger.Info(ctx, "Closing server failed", "address", server.Addr, "error", err)
		} else {
			s.logger.Info(ctx, "Closing server done", "address", server.Addr)
		}

		// Force close
		return server.Close()
	}
}
