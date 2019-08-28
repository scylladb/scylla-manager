// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/internal/fsutil"
	"github.com/scylladb/mermaid/internal/httputil/pprof"
	"github.com/scylladb/mermaid/internal/kv"
	"github.com/scylladb/mermaid/restapi"
	"github.com/scylladb/mermaid/service/backup"
	"github.com/scylladb/mermaid/service/cluster"
	"github.com/scylladb/mermaid/service/healthcheck"
	"github.com/scylladb/mermaid/service/repair"
	"github.com/scylladb/mermaid/service/scheduler"
	"go.uber.org/multierr"
)

type server struct {
	config  *serverConfig
	session *gocql.Session
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

func newServer(config *serverConfig, logger log.Logger) (*server, error) {
	session, err := gocqlClusterConfig(config).CreateSession()
	if err != nil {
		return nil, errors.Wrapf(err, "database")
	}

	s := &server{
		config:  config,
		session: session,
		logger:  logger,

		errCh: make(chan error, 4),
	}

	if err := s.makeServices(); err != nil {
		return nil, err
	}
	if err := s.makeHTTPServers(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *server) makeServices() error {
	dir := filepath.Join(fsutil.HomeDir(), ".certs")

	sslCertStore, err := kv.NewFsStore(dir, "cert")
	if err != nil {
		return errors.Wrapf(err, "failed to init SSL cert store at %s", dir)
	}
	sslKeyStore, err := kv.NewFsStore(dir, "key")
	if err != nil {
		return errors.Wrapf(err, "failed to init SSL key store at %s", dir)
	}

	s.clusterSvc, err = cluster.NewService(s.session, sslCertStore, sslKeyStore, s.logger.Named("cluster"))
	if err != nil {
		return errors.Wrapf(err, "cluster service")
	}
	s.clusterSvc.SetOnChangeListener(s.onClusterChange)

	s.healthSvc, err = healthcheck.NewService(
		s.config.Healthcheck,
		s.clusterSvc.GetClusterName,
		s.clusterSvc.Client,
		sslCertStore,
		sslKeyStore,
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
		s.logger.Named("backup"),
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
	s.schedSvc.SetRunner(scheduler.HealthCheckTask, s.healthSvc.CQLRunner())
	s.schedSvc.SetRunner(scheduler.HealthCheckRESTTask, s.healthSvc.RESTRunner())
	policy := scheduler.NewLockClusterPolicy()
	s.schedSvc.SetRunner(scheduler.BackupTask, scheduler.PolicyRunner{policy, s.backupSvc.Runner()})
	s.schedSvc.SetRunner(scheduler.RepairTask, scheduler.PolicyRunner{policy, s.repairSvc.Runner()})

	return nil
}

func (s *server) onClusterChange(ctx context.Context, c cluster.Change) error {
	switch c.Type {
	case cluster.Create:
		if err := s.schedSvc.PutTaskOnce(ctx, makeAutoHealthCheckTask(c.ID)); err != nil {
			return errors.Wrapf(err, "failed to add automatically scheduled health check for cluster %s", c.ID)
		}
		if err := s.schedSvc.PutTaskOnce(ctx, makeAutoHealthCheckRESTTask(c.ID)); err != nil {
			return errors.Wrapf(err, "failed to add automatically scheduled REST health check for cluster %s", c.ID)
		}
		if err := s.schedSvc.PutTask(ctx, makeAutoRepairTask(c.ID)); err != nil {
			return errors.Wrapf(err, "failed to add automatically scheduled weekly repair for cluster %s", c.ID)
		}
	case cluster.Delete:
		tasks, err := s.schedSvc.ListTasks(ctx, c.ID, "")
		if err != nil {
			return errors.Wrapf(err, "failed to find this cluster %s tasks", c.ID)
		}
		var errs error
		for _, t := range tasks {
			errs = multierr.Append(errs, s.schedSvc.DeleteTask(ctx, t))
		}
		if errs != nil {
			return errors.Wrapf(errs, "failed to remove cluster %s tasks", c.ID)
		}
	}

	s.healthSvc.InvalidateTLSConfigCache(c.ID)

	return nil
}

func (s *server) makeHTTPServers() error {
	h := restapi.New(restapi.Services{
		Cluster:     s.clusterSvc,
		HealthCheck: s.healthSvc,
		Repair:      s.repairSvc,
		Backup:      s.backupSvc,
		Scheduler:   s.schedSvc,
	}, s.logger.Named("restapi"))

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
				return errors.Wrapf(err, "https failed to read certificate file %s", s.config.TLSCAFile)
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
			Handler: restapi.NewPrometheus(s.clusterSvc),
		}
	}
	if s.config.Debug != "" {
		s.debugServer = &http.Server{
			Addr:    s.config.Debug,
			Handler: pprof.Handler(),
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

func (s *server) startHTTPServers(ctx context.Context) {
	if s.httpServer != nil {
		s.logger.Info(ctx, "Starting HTTP server", "address", s.httpServer.Addr)
		go func() {
			s.errCh <- s.httpServer.ListenAndServe()
		}()
	}

	if s.httpsServer != nil {
		s.logger.Info(ctx, "Starting HTTPS server", "address", s.httpsServer.Addr, "client_ca", s.config.TLSCAFile)
		go func() {
			s.errCh <- errors.Wrap(s.httpsServer.ListenAndServeTLS(s.config.TLSCertFile, s.config.TLSKeyFile), "HTTPS server failed to start")
		}()
	}

	if s.prometheusServer != nil {
		s.logger.Info(ctx, "Starting Prometheus server", "address", s.prometheusServer.Addr)
		go func() {
			s.errCh <- errors.Wrap(s.prometheusServer.ListenAndServe(), "Prometheus server failed to start")
		}()
	}

	if s.debugServer != nil {
		s.logger.Info(ctx, "Starting debug server", "address", s.debugServer.Addr)
		go func() {
			s.errCh <- errors.Wrap(s.debugServer.ListenAndServe(), "debug server failed to start")
		}()
	}
}

func (s *server) shutdownServers(ctx context.Context, timeout time.Duration) {
	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(4)
	shutdownHTTPServer(tctx, s.httpServer, &wg, s.logger)
	shutdownHTTPServer(tctx, s.httpsServer, &wg, s.logger)
	shutdownHTTPServer(tctx, s.prometheusServer, &wg, s.logger)
	shutdownHTTPServer(tctx, s.debugServer, &wg, s.logger)
	wg.Wait()
}

func (s *server) close() {
	if s.httpServer != nil {
		s.httpServer.Close()
	}
	if s.httpsServer != nil {
		s.httpsServer.Close()
	}
	if s.prometheusServer != nil {
		s.prometheusServer.Close()
	}

	// The cluster service needs to be closed last because it handles closing of
	// connections to agent running on the nodes.
	s.schedSvc.Close()
	s.clusterSvc.Close()
	s.session.Close()
}

func shutdownHTTPServer(ctx context.Context, s *http.Server, wg *sync.WaitGroup, l log.Logger) {
	if s == nil {
		wg.Done()
		return
	}

	go func() {
		defer wg.Done()
		if err := s.Shutdown(ctx); err != nil {
			l.Info(ctx, "Closing server failed", "addr", s.Addr, "error", err)
		} else {
			l.Info(ctx, "Closing server done", "addr", s.Addr)
		}
	}()
}
