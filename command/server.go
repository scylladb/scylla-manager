// Copyright (C) 2017 ScyllaDB

package command

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"text/template"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/gops/agent"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/migrate"
	"github.com/scylladb/mermaid/log"
	"github.com/scylladb/mermaid/log/gocqllog"
	"github.com/scylladb/mermaid/repair"
	"github.com/scylladb/mermaid/restapi"
	"github.com/scylladb/mermaid/scylla"
	"github.com/scylladb/mermaid/uuid"
	"gopkg.in/yaml.v2"
)

// clusterConfig is a temporary solution and will be soon replaced by a
// a cluster configuration service.
type clusterConfig struct {
	UUID                            uuid.UUID `yaml:"uuid"`
	Hosts                           []string  `yaml:"hosts"`
	ShardCount                      float64   `yaml:"shard_count"`
	Murmur3PartitionerIgnoreMsbBits float64   `yaml:"murmur3_partitioner_ignore_msb_bits"`
}

type dbConfig struct {
	Hosts                         []string      `yaml:"hosts"`
	User                          string        `yaml:"user"`
	Password                      string        `yaml:"password"`
	Keyspace                      string        `yaml:"keyspace"`
	KeyspaceTplFile               string        `yaml:"keyspace_tpl_file"`
	MigrateDir                    string        `yaml:"migrate_dir"`
	MigrateTimeout                time.Duration `yaml:"migrate_timeout"`
	MigrateMaxWaitSchemaAgreement time.Duration `yaml:"migrate_max_wait_schema_agreement"`
	Consistency                   string        `yaml:"consistency"`
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
	if _, err := gocql.ParseConsistencyWrapper(c.Database.Consistency); err != nil {
		return errors.New("invalid database.consistency")
	}

	for _, cluster := range c.Clusters {
		if len(cluster.Hosts) == 0 {
			errors.Errorf("cluster %s: missing %q", cluster.UUID, "hosts")
		}
		if cluster.ShardCount == 0 {
			errors.Errorf("cluster %s: missing %q", cluster.UUID, "shard_count")
		}
		if cluster.Murmur3PartitionerIgnoreMsbBits == 0 {
			errors.Errorf("cluster %s: missing %q", cluster.UUID, "murmur3_partitioner_ignore_msb_bits")
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
		cmd.UI.Error(fmt.Sprintf("Command line error: %s", err))
		return 1
	}

	// try to make absolute path
	if absp, err := filepath.Abs(cmd.configFile); err == nil {
		cmd.configFile = absp
	}

	// read configuration
	config, err := cmd.readConfig(cmd.configFile)
	if err != nil {
		cmd.UI.Error(fmt.Sprintf("Configuration error %s: %s", cmd.configFile, err))
		return 1
	}
	if err := config.validate(); err != nil {
		cmd.UI.Error(fmt.Sprintf("Configuration error %s: %s", cmd.configFile, err))
		return 1
	}

	// get a base context
	ctx := context.Background()

	// create logger
	logger, err := cmd.logger()
	if err != nil {
		cmd.UI.Error(fmt.Sprintf("Logger error: %s", err))
		return 1
	}

	// set gocql logger
	gocql.Logger = gocqllog.New(ctx, logger.Named("gocql"))

	// create management keyspace
	logger.Info(ctx, "Using keyspace",
		"keyspace", config.Database.Keyspace,
		"template", config.Database.KeyspaceTplFile,
	)
	if err := cmd.createKeyspace(config); err != nil {
		cmd.UI.Error(fmt.Sprintf("Database error: %s", err))
		return 1
	}

	// migrate schema
	logger.Info(ctx, "Migrating schema", "dir", config.Database.MigrateDir)
	if err := cmd.migrateSchema(config); err != nil {
		cmd.UI.Error(fmt.Sprintf("Database migration error: %s", err))
		return 1
	}

	// create database session
	session, err := cmd.clusterConfig(config).CreateSession()
	if err != nil {
		cmd.UI.Error(fmt.Sprintf("Database error: %s", err))
		return 1
	}
	defer session.Close()

	// create configuration based scylla provider
	provider, err := cmd.scyllaProviderFunc(config, logger)
	if err != nil {
		cmd.UI.Error(fmt.Sprintf("Scylla provider error: %s", err))
		return 1
	}

	// create repair service
	repairSvc, err := repair.NewService(session, provider, logger.Named("repair"))
	if err != nil {
		cmd.UI.Error(fmt.Sprintf("Repair service error: %s", err))
		return 1
	}
	if err := repairSvc.FixRunStatus(ctx); err != nil {
		cmd.UI.Error(fmt.Sprintf("Repair service error: %s", err))
		return 1
	}

	// create REST handler
	handler := restapi.New(repairSvc, logger.Named("restapi"))

	// in debug mode launch gops agent
	if cmd.debug {
		if err := agent.Listen(nil); err != nil {
			cmd.UI.Error(fmt.Sprintf("Debug agent startup error: %s", err))
		}
	}

	// listen and serve
	var (
		httpServer  *http.Server
		httpsServer *http.Server
		errCh       = make(chan error, 2)
	)

	if len(config.Clusters) == 0 {
		logger.Info(ctx, "No clusters configured")
	}

	if config.HTTP != "" {
		httpServer = &http.Server{
			Addr:    config.HTTP,
			Handler: handler,
		}
		go func() {
			logger.Info(ctx, "Starting HTTP", "address", httpServer.Addr)
			errCh <- httpServer.ListenAndServe()
		}()
	}

	if config.HTTPS != "" {
		httpsServer = &http.Server{
			Addr:    config.HTTPS,
			Handler: handler,
		}
		go func() {
			logger.Info(ctx, "Starting HTTPS", "address", httpsServer.Addr)
			errCh <- httpsServer.ListenAndServeTLS(config.TLSCertFile, config.TLSKeyFile)
		}()
	}

	// wait
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		if err != nil {
			logger.Error(ctx, "Server error", "error", err)
		}
	case sig := <-signalCh:
		{
			logger.Info(ctx, "Received signal", "signal", sig)
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
				logger.Info(ctx, "Closing HTTP error", "error", err)
			}
			httpServer.Close()
		}()
	}
	if httpsServer != nil {
		logger.Info(ctx, "Closing HTTPS...")

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := httpsServer.Shutdown(timeoutCtx); err != nil {
				logger.Info(ctx, "Closing HTTPS error", "error", err)
			}
			httpsServer.Close()
		}()
	}
	wg.Wait()

	// close repair
	repairSvc.Close(ctx)

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
		Database: dbConfig{
			Keyspace:                      "scylla_management",
			KeyspaceTplFile:               "/etc/scylla-mgmt/create_keyspace.cql.tpl",
			MigrateDir:                    "/etc/scylla-mgmt/cql",
			MigrateTimeout:                30 * time.Second,
			MigrateMaxWaitSchemaAgreement: 5 * time.Minute,
			Consistency:                   gocql.Quorum.String(),
		},
	}
}

func (cmd *ServerCommand) logger() (log.Logger, error) {
	if cmd.debug {
		return log.NewDevelopment(), nil
	}
	return log.NewProduction("scylla-mgmt")
}

func (cmd *ServerCommand) createKeyspace(config *serverConfig) error {
	stmt, err := cmd.readKeyspaceTplFile(config)
	if err != nil {
		return err
	}

	c := cmd.clusterConfig(config)
	c.Keyspace = "system"
	c.Timeout = config.Database.MigrateTimeout
	c.MaxWaitSchemaAgreement = config.Database.MigrateMaxWaitSchemaAgreement

	session, err := c.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	return gocqlx.Query(session.Query(stmt), nil).ExecRelease()
}

func (cmd *ServerCommand) readKeyspaceTplFile(config *serverConfig) (stmt string, err error) {
	b, err := ioutil.ReadFile(config.Database.KeyspaceTplFile)
	if err != nil {
		return "", errors.Wrapf(err, "could not read file %s", config.Database.KeyspaceTplFile)
	}

	t := template.New("")
	if _, err := t.Parse(string(b)); err != nil {
		return "", errors.Wrapf(err, "template error file %s", config.Database.KeyspaceTplFile)
	}

	buf := new(bytes.Buffer)
	if err := t.Execute(buf, config.Database); err != nil {
		return "", errors.Wrapf(err, "template error file %s", config.Database.KeyspaceTplFile)
	}

	return buf.String(), err
}

func (cmd *ServerCommand) migrateSchema(config *serverConfig) error {
	c := cmd.clusterConfig(config)
	c.Timeout = config.Database.MigrateTimeout
	c.MaxWaitSchemaAgreement = config.Database.MigrateMaxWaitSchemaAgreement

	session, err := c.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	return migrate.Migrate(context.Background(), session, config.Database.MigrateDir)
}

func (cmd *ServerCommand) clusterConfig(config *serverConfig) *gocql.ClusterConfig {
	c := gocql.NewCluster(config.Database.Hosts...)

	// overwrite the default settings
	c.Consistency = gocql.ParseConsistency(config.Database.Consistency)
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

func (cmd *ServerCommand) scyllaProviderFunc(config *serverConfig, logger log.Logger) (scylla.ProviderFunc, error) {
	m := make(map[uuid.UUID]*scylla.Client, len(config.Clusters))
	for _, c := range config.Clusters {
		client, err := scylla.NewClient(c.Hosts, logger.Named("scylla"))
		if err != nil {
			return nil, err
		}
		m[c.UUID] = scylla.WithConfig(client, scylla.Config{
			"murmur3_partitioner_ignore_msb_bits": c.Murmur3PartitionerIgnoreMsbBits,
			"shard_count":                         c.ShardCount,
		})
	}

	return func(clusterID uuid.UUID) (*scylla.Client, error) {
		c, ok := m[clusterID]
		if !ok {
			return nil, errors.Errorf("unknown cluster %s", clusterID)
		}

		return c, nil
	}, nil
}
