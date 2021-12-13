// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rclone/rclone/fs"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/pkg"
	"github.com/scylladb/scylla-manager/pkg/config/agent"
	"github.com/scylladb/scylla-manager/pkg/rclone"
	"github.com/scylladb/scylla-manager/pkg/rclone/rcserver"
	"github.com/scylladb/scylla-manager/pkg/util/certutil"
	"github.com/scylladb/scylla-manager/pkg/util/cpuset"
	"github.com/scylladb/scylla-manager/pkg/util/httppprof"
	"github.com/scylladb/scylla-manager/pkg/util/netwait"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type server struct {
	config agent.Config
	logger log.Logger

	httpsServer      *http.Server
	prometheusServer *http.Server
	debugServer      *http.Server

	errCh chan error
}

func newServer(c agent.Config, logger log.Logger) *server {
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
	if err := agent.EnrichConfigFromAPI(ctx, addr, &s.config); err != nil {
		return err
	}

	s.logger.Info(ctx, "Using config", "config", agent.Obfuscate(s.config), "config_files", rootArgs.configFiles)

	// Instruct users to set auth token
	if s.config.AuthToken == "" {
		ip, _, _ := net.SplitHostPort(s.config.HTTPS)
		s.logger.Info(ctx, "WARNING! Scylla data may be exposed on IP "+ip+", "+
			"protect it by specifying auth_token in config file", "config_files", rootArgs.configFiles,
		)
	}

	// Try to get CPUs to pin to
	var cpus []int
	if s.config.CPU != agent.NoCPU {
		cpus = []int{s.config.CPU}
	} else if free, err := findFreeCPUs(); err != nil {
		if os.IsNotExist(errors.Cause(err)) || errors.Is(err, cpuset.ErrNoCPUSetConfig) {
			// Ignore if there is no cpuset file
			s.logger.Debug(ctx, "Failed to find CPUs to pin to", "error", err)
		} else {
			s.logger.Error(ctx, "Failed to find CPUs to pin to", "error", err)
		}
	} else {
		cpus = free
	}
	// Pin to CPUs if possible
	if len(cpus) == 0 {
		s.logger.Info(ctx, "Running on all CPUs")
		runtime.GOMAXPROCS(1)
	} else {
		if err := pinToCPUs(cpus); err != nil {
			s.logger.Error(ctx, "Failed to pin to CPUs", "cpus", cpus, "error", err)
		} else {
			s.logger.Info(ctx, "Running on CPUs", "cpus", cpus)
		}
		runtime.GOMAXPROCS(len(cpus))
	}

	// Log memory limit
	if l, err := cgroupMemoryLimit(); err != nil {
		s.logger.Debug(ctx, "Failed to get cgroup memory limit", "error", err)
	} else {
		s.logger.Info(ctx, "Cgroup memory limit", "limit", fs.SizeSuffix(l))
	}

	// Redirect rclone logger to the ogger
	rclone.RedirectLogPrint(s.logger.Named("rclone"))
	// Init rclone config options
	rclone.InitFsConfigWithOptions(s.config.Rclone)
	// Add prometheus metrics
	rclone.MustRegisterPrometheusMetrics("scylla_manager_agent_rclone")

	// Register rclone providers
	return multierr.Combine(
		rclone.RegisterLocalDirProvider("data", "Jailed Scylla data", s.config.Scylla.DataDirectory),
		rclone.RegisterS3Provider(s.config.S3),
		rclone.RegisterGCSProvider(s.config.GCS),
		rclone.RegisterAzureProvider(s.config.Azure),
	)
}

func (s *server) makeServers(ctx context.Context) error {
	tlsConfig, err := s.tlsConfig(ctx)
	if err != nil {
		return errors.Wrapf(err, "tls")
	}
	s.httpsServer = &http.Server{
		Addr:      s.config.HTTPS,
		TLSConfig: tlsConfig,
		Handler:   newRouter(s.config, rcserver.New(), s.logger.Named("http")),
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

	return nil
}

func (s *server) tlsConfig(ctx context.Context) (*tls.Config, error) {
	var (
		cert tls.Certificate
		err  error
	)
	if s.config.HasTLSCert() {
		s.logger.Info(ctx, "Loading TLS certificate from disk",
			"cert_file", s.config.TLSCertFile,
			"key_file", s.config.TLSKeyFile,
		)
		cert, err = tls.LoadX509KeyPair(s.config.TLSCertFile, s.config.TLSKeyFile)
	} else {
		hosts := strset.New()
		for _, h := range []string{s.config.Scylla.BroadcastRPCAddress, s.config.Scylla.RPCAddress, s.config.Scylla.ListenAddress} {
			if h != "" && h != net.IPv4zero.String() && h != net.IPv4zero.String() {
				hosts.Add(h)
			}
		}
		s.logger.Info(ctx, "Generating TLS certificate", "hosts", hosts.List())
		cert, err = certutil.GenerateSelfSignedCertificate(hosts.List())
	}
	if err != nil {
		return nil, errors.Wrap(err, "certificate")
	}
	tlsConfig := s.config.TLSVersion.TLSConfig()
	tlsConfig.Certificates = []tls.Certificate{cert}

	return tlsConfig, nil
}

func (s *server) startServers(ctx context.Context) {
	if s.httpsServer != nil {
		s.logger.Info(ctx, "Starting HTTPS server", "address", s.httpsServer.Addr)
		go func() {
			s.errCh <- errors.Wrap(s.httpsServer.ListenAndServeTLS("", ""), "HTTPS server start")
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
