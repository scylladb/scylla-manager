// Copyright (C) 2017 ScyllaDB

package command

import (
	"context"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/gops/agent"
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/log"
	"github.com/scylladb/mermaid/repair"
	"github.com/scylladb/mermaid/restapi"
	"github.com/scylladb/mermaid/scylla"
	"github.com/scylladb/mermaid/uuid"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

// clusterConfig is a temporary solution and will be soon replaced by a
// a cluster configuration service.
type clusterConfig struct {
	UUID  uuid.UUID `yaml:"uuid"`
	Hosts []string  `yaml:"hosts"`
}

type dbConfig struct {
	Hosts    []string `yaml:"hosts"`
	Keyspace string   `yaml:"keyspace"`
	User     string   `yaml:"user"`
	Password string   `yaml:"password"`
}

type serverConfig struct {
	HTTP        string   `yaml:"http"`
	HTTPS       string   `yaml:"https"`
	TLSCertFile string   `yaml:"tls_cert_file"`
	TLSKeyFile  string   `yaml:"tls_key_file"`
	Database    dbConfig `yaml:"database"`

	Clusters []*clusterConfig `yaml:"clusters"`
}

func (c *serverConfig) validate() error {
	if c.HTTP == "" && c.HTTPS == "" {
		return errors.New("missing http or https")
	}
	if c.HTTPS != "" {
		if c.TLSCertFile == "" {
			return errors.New("missing tls_cert_file")
		}
		if c.TLSKeyFile == "" {
			return errors.New("missing tls_key_file")
		}
	}
	if len(c.Database.Hosts) == 0 {
		return errors.New("missing database.hosts")
	}

	if len(c.Clusters) == 0 {
		return errors.New("no clusters configured")
	}
	for _, cluster := range c.Clusters {
		if len(cluster.Hosts) == 0 {
			errors.Errorf("no hosts for clusters %s", cluster.UUID)
		}
	}

	return nil
}

// ServerCommand runs the management server.
type ServerCommand struct {
	BaseCommand

	configFile string
	debug      bool
}

// InitFlags sets the command flags.
func (cmd *ServerCommand) InitFlags() {
	f := cmd.BaseCommand.NewFlagSet(cmd)
	f.StringVar(&cmd.configFile, "config-file", "/etc/scylla-mgmt/scylla-mgmt.yaml", "Path to a YAML file to read configuration from.")
	f.BoolVar(&cmd.debug, "debug", false, "")

	cmd.HideFlags("debug")
}

// Run implements cli.Command.
func (cmd *ServerCommand) Run(args []string) int {
	// parse command line arguments
	if err := cmd.Parse(args); err != nil {
		cmd.UI.Error(errors.Wrap(err, "flags").Error())
		return 1
	}

	// read configuration
	config, err := cmd.readConfig(cmd.configFile)
	if err != nil {
		cmd.UI.Error(errors.Wrap(err, "failed to read configuration").Error())
		return 1
	}
	if err := config.validate(); err != nil {
		cmd.UI.Error(errors.Wrap(err, "invalid configuration").Error())
		return 1
	}

	// create logger
	logger, err := cmd.logger()
	if err != nil {
		cmd.UI.Error(errors.Wrap(err, "failed to create logger").Error())
		return 1
	}

	// create database session
	session, err := cmd.clusterConfig(config).CreateSession()
	if err != nil {
		cmd.UI.Error(errors.Wrap(err, "failed to create database session").Error())
		return 1
	}
	defer session.Close()

	// create configuration based scylla provider
	m := make(map[uuid.UUID]*scylla.Client, len(config.Clusters))
	for _, c := range config.Clusters {
		client, err := scylla.NewClient(c.Hosts, logger.Named("scylla"))
		if err != nil {
			cmd.UI.Error(errors.Wrap(err, "failed to create scylla api client").Error())
			return 1
		}
		m[c.UUID] = client
	}
	provider := func(clusterID uuid.UUID) (*scylla.Client, error) {
		c, ok := m[clusterID]
		if !ok {
			return nil, errors.Errorf("unknown cluster %s", clusterID)
		}

		return c, nil
	}

	// create repair service
	repairSvc, err := repair.NewService(session, provider, logger.Named("repair"))
	if err != nil {
		cmd.UI.Error(errors.Wrap(err, "service init error").Error())
		return 1
	}

	// create REST handler
	handler := restapi.New(repairSvc, logger.Named("restapi"))

	// in debug mode launch gops agent
	if cmd.debug {
		if err := agent.Listen(nil); err != nil {
			cmd.UI.Error(errors.Wrap(err, "failed to run debug agent").Error())
			return 1
		}
	}

	// listen and serve
	var (
		httpServer  *http.Server
		httpsServer *http.Server

		ctx   = context.Background()
		errCh = make(chan error, 2)
	)

	if config.HTTP != "" {
		httpServer = &http.Server{
			Addr:    config.HTTP,
			Handler: handler,
		}

		go func() {
			logger.Info(ctx, "Starting HTTP", "Address", httpServer.Addr)
			if err := httpServer.ListenAndServe(); err != nil {
				errCh <- err
			}
		}()
	}

	if config.HTTPS != "" {
		httpsServer = &http.Server{
			Addr:    config.HTTPS,
			Handler: handler,
		}

		go func() {
			logger.Info(ctx, "Starting HTTPS", "Address", httpsServer.Addr)
			if err := httpsServer.ListenAndServeTLS(config.TLSCertFile, config.TLSKeyFile); err != nil {
				errCh <- err
			}
		}()
	}

	// wait
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		if err != nil {
			logger.Error(ctx, "Server error", "Error", err)
		}
	case sig := <-signalCh:
		{
			logger.Info(ctx, "Received signal", "Signal", sig)
		}
	}

	// graceful shutdown
	var (
		timeoutCtx, cancelFunc = context.WithTimeout(ctx, 30*time.Second)
		wg                     sync.WaitGroup
	)
	defer cancelFunc()

	if httpServer != nil {
		logger.Info(ctx, "Closing HTTP...")

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := httpServer.Shutdown(timeoutCtx); err != nil {
				logger.Info(ctx, "Closing HTTP error", "Error", err)
			}
		}()
	}
	if httpsServer != nil {
		logger.Info(ctx, "Closing HTTPS...")

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := httpsServer.Shutdown(timeoutCtx); err != nil {
				logger.Info(ctx, "Closing HTTPS error", "Error", err)
			}
		}()
	}
	wg.Wait()

	// close agent
	if cmd.debug {
		agent.Close()
	}

	// bye
	logger.Info(ctx, "Server stopped")
	logger.Sync()

	return 0
}

// Synopsis implements cli.Command.
func (cmd *ServerCommand) Synopsis() string {
	return "Starts the Scylla management server"
}

func (cmd *ServerCommand) readConfig(file string) (*serverConfig, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, errors.Wrap(err, "io error")
	}
	defer f.Close()

	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, errors.Wrap(err, "io error")
	}

	config := cmd.defaultConfig()
	return config, yaml.Unmarshal(b, config)
}

func (cmd *ServerCommand) defaultConfig() *serverConfig {
	return &serverConfig{
		HTTP: "localhost:80",
		Database: dbConfig{
			Keyspace: "scylla_management",
		},
	}
}

func (cmd *ServerCommand) clusterConfig(config *serverConfig) *gocql.ClusterConfig {
	c := gocql.NewCluster(config.Database.Hosts...)

	// overwrite the default settings
	c.Consistency = gocql.LocalQuorum
	c.Keyspace = config.Database.Keyspace

	// authentication
	if config.Database.User != "" {
		c.Authenticator = gocql.PasswordAuthenticator{
			Username: config.Database.User,
			Password: config.Database.Password,
		}
	}

	return c
}

func (cmd *ServerCommand) logger() (log.Logger, error) {
	var (
		z   *zap.Logger
		err error
	)
	if cmd.debug {
		z, err = zap.NewDevelopment()
	} else {
		z, err = zap.NewProduction()
	}

	if err != nil {
		return log.NopLogger, err
	}

	return log.NewLogger(z), nil
}
