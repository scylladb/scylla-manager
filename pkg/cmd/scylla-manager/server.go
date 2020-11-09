// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/pkg/cmd/scylla-manager/config"
	"github.com/scylladb/scylla-manager/pkg/restapi"
	"github.com/scylladb/scylla-manager/pkg/service/backup"
	"github.com/scylladb/scylla-manager/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/pkg/service/healthcheck"
	"github.com/scylladb/scylla-manager/pkg/service/repair"
	"github.com/scylladb/scylla-manager/pkg/service/scheduler"
	"github.com/scylladb/scylla-manager/pkg/service/secrets/dbsecrets"
	"github.com/scylladb/scylla-manager/pkg/util/httppprof"
	"github.com/scylladb/scylla-manager/pkg/util/prom"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
)

type server struct {
	config  *config.ServerConfig
	session gocqlx.Session
	logger  log.Logger

	clusterSvc *cluster.Service
	healthSvc  *healthcheck.Service
	backupSvc  *backup.Service
	repairSvc  *repair.Service
	schedSvc   *scheduler.Service

	httpServer       *http.Server
	httpsServer      *http.Server
	prometheusServer *http.Server
	debugServer      *http.Server

	errCh chan error
}

func newServer(config *config.ServerConfig, logger log.Logger) (*server, error) {
	session, err := gocqlx.WrapSession(gocqlClusterConfig(config).CreateSession())
	if err != nil {
		return nil, errors.Wrapf(err, "database")
	}

	s := &server{
		config:  config,
		session: session,
		logger:  logger,

		errCh: make(chan error, 4),
	}

	var mw prom.MetricsWatcher
	if err := s.makeServices(&mw); err != nil {
		return nil, err
	}
	if err := s.makeServers(&mw); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *server) makeServices(mw *prom.MetricsWatcher) error {
	secretsStore, err := dbsecrets.New(s.session)
	if err != nil {
		return errors.Wrap(err, "db secrets service")
	}

	s.clusterSvc, err = cluster.NewService(s.session, secretsStore, s.logger.Named("cluster"))
	if err != nil {
		return errors.Wrapf(err, "cluster service")
	}
	s.clusterSvc.SetOnChangeListener(s.onClusterChange)

	s.healthSvc, err = healthcheck.NewService(
		s.config.Healthcheck,
		s.clusterSvc.GetClusterName,
		s.clusterSvc.Client,
		secretsStore,
		s.logger.Named("healthcheck"),
	)
	if err != nil {
		return errors.Wrapf(err, "healthcheck service")
	}

	s.backupSvc, err = backup.NewService(
		s.session,
		s.config.Backup,
		s.clusterSvc.GetClusterName,
		s.clusterSvc.Client,
		s.clusterSvc.GetSession,
		s.logger.Named("backup"),
		mw,
	)
	if err != nil {
		return errors.Wrapf(err, "backup service")
	}

	s.repairSvc, err = repair.NewService(
		s.session,
		s.config.Repair,
		s.clusterSvc.GetClusterName,
		s.clusterSvc.Client,
		s.logger.Named("repair"),
		mw,
	)
	if err != nil {
		return errors.Wrapf(err, "repair service")
	}

	s.schedSvc, err = scheduler.NewService(
		s.session,
		s.clusterSvc.GetClusterName,
		s.logger.Named("scheduler"),
	)
	if err != nil {
		return errors.Wrapf(err, "scheduler service")
	}

	// Register the runners
	s.schedSvc.SetRunner(scheduler.HealthCheckAlternatorTask, s.healthSvc.AlternatorRunner())
	s.schedSvc.SetRunner(scheduler.HealthCheckCQLTask, s.healthSvc.CQLRunner())
	s.schedSvc.SetRunner(scheduler.HealthCheckRESTTask, s.healthSvc.RESTRunner())
	s.schedSvc.SetRunner(scheduler.BackupTask, scheduler.PolicyRunner{scheduler.NewLockClusterPolicy(), s.backupSvc.Runner()})
	s.schedSvc.SetRunner(scheduler.RepairTask, scheduler.PolicyRunner{scheduler.NewLockClusterPolicy(), s.repairSvc.Runner()})

	return nil
}

func (s *server) onClusterChange(ctx context.Context, c cluster.Change) error {
	switch c.Type {
	case cluster.Create:
		if err := s.schedSvc.PutTaskOnce(ctx, makeAutoHealthCheckAlternatorTask(c.ID)); err != nil {
			return errors.Wrapf(err, "add automatically scheduled health check for cluster %s", c.ID)
		}
		if err := s.schedSvc.PutTaskOnce(ctx, makeAutoHealthCheckTask(c.ID)); err != nil {
			return errors.Wrapf(err, "add automatically scheduled health check for cluster %s", c.ID)
		}
		if err := s.schedSvc.PutTaskOnce(ctx, makeAutoHealthCheckRESTTask(c.ID)); err != nil {
			return errors.Wrapf(err, "add automatically scheduled REST health check for cluster %s", c.ID)
		}
		if !c.WithoutRepair {
			if err := s.schedSvc.PutTask(ctx, makeAutoRepairTask(c.ID)); err != nil {
				return errors.Wrapf(err, "add automatically scheduled weekly repair for cluster %s", c.ID)
			}
		}
	case cluster.Delete:
		tasks, err := s.schedSvc.ListTasks(ctx, c.ID, "")
		if err != nil {
			return errors.Wrapf(err, "find this cluster %s tasks", c.ID)
		}
		var errs error
		for _, t := range tasks {
			errs = multierr.Append(errs, s.schedSvc.DeleteTask(ctx, t))
		}
		if errs != nil {
			return errors.Wrapf(errs, "remove cluster %s tasks", c.ID)
		}
	}

	s.healthSvc.InvalidateCache(c.ID)

	return nil
}

func (s *server) makeServers(mw *prom.MetricsWatcher) error {
	h := restapi.New(restapi.Services{
		Cluster:     s.clusterSvc,
		HealthCheck: s.healthSvc,
		Repair:      s.repairSvc,
		Backup:      s.backupSvc,
		Scheduler:   s.schedSvc,
	}, s.logger.Named("http"))

	if s.config.HTTP != "" {
		s.httpServer = &http.Server{
			Addr:    s.config.HTTP,
			Handler: h,
		}
	}
	if s.config.HTTPS != "" {
		s.httpsServer = &http.Server{
			Addr:    s.config.HTTPS,
			Handler: h,
		}

		if s.config.TLSCAFile != "" {
			pool := x509.NewCertPool()
			b, err := ioutil.ReadFile(s.config.TLSCAFile)
			if err != nil {
				return errors.Wrapf(err, "https read certificate file %s", s.config.TLSCAFile)
			}
			if !pool.AppendCertsFromPEM(b) {
				return errors.Errorf("https no certificates found in %s", s.config.TLSCAFile)
			}
			s.httpsServer.TLSConfig = &tls.Config{
				ClientCAs:  pool,
				ClientAuth: tls.RequireAndVerifyClientCert,
			}
		}
	}
	if s.config.Prometheus != "" {
		s.prometheusServer = &http.Server{
			Addr:    s.config.Prometheus,
			Handler: restapi.NewPrometheus(s.clusterSvc, mw),
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

func (s *server) startServices(ctx context.Context) error {
	if err := s.schedSvc.LoadTasks(ctx); err != nil {
		return errors.Wrapf(err, "schedule service")
	}
	return nil
}

func (s *server) startServers(ctx context.Context) {
	if s.httpServer != nil {
		s.logger.Info(ctx, "Starting HTTP server", "address", s.httpServer.Addr)
		go func() {
			s.errCh <- s.httpServer.ListenAndServe()
		}()
	}

	if s.httpsServer != nil {
		s.logger.Info(ctx, "Starting HTTPS server", "address", s.httpsServer.Addr, "client_ca", s.config.TLSCAFile)
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
	eg.Go(s.shutdownHTTPServer(tctx, s.httpServer))
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

func (s *server) close() {
	// The cluster service needs to be closed last because it handles closing of
	// connections to agent running on the nodes.
	s.schedSvc.Close()
	s.clusterSvc.Close()

	s.session.Close()
}
