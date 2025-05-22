// Copyright (C) 2025 ScyllaDB

package hackathon

import (
	"context"
	"crypto/tls"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/command/repair/hackathon/repair"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/util/logutil"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"github.com/scylladb/scylla-manager/v3/sqlc/queries"
	"github.com/spf13/cobra"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
	_ "modernc.org/sqlite"
)

//go:embed res.yaml
var res []byte

type command struct {
	cobra.Command
	dataPath               string
	host                   string
	port                   int64
	authToken              string
	username               string
	password               string
	sslUserCertFile        string
	sslUserKeyFile         string
	forceTLSDisabled       bool
	forceNonSSLSessionPort bool
}

func NewCommand() *cobra.Command {
	var (
		cmd = &command{}
		r   []byte
	)
	r = res
	if err := yaml.Unmarshal(r, &cmd.Command); err != nil {
		panic(err)
	}
	cmd.init()
	cmd.RunE = func(_ *cobra.Command, _ []string) error {
		return cmd.run()
	}
	return &cmd.Command
}

func (cmd *command) init() {
	w := cmd.Flags()
	w.StringVar(&cmd.dataPath, "data-path", "", "path to store data")
	w.StringVar(&cmd.host, "host", "", "")
	w.Int64Var(&cmd.port, "port", 10001, "")
	w.StringVar(&cmd.authToken, "auth-token", "", "")
	w.StringVarP(&cmd.username, "username", "u", "", "")
	w.StringVarP(&cmd.password, "password", "p", "", "")
	w.StringVar(&cmd.sslUserCertFile, "ssl-user-cert-file", "", "")
	w.StringVar(&cmd.sslUserKeyFile, "ssl-user-key-file", "", "")
	w.BoolVar(&cmd.forceTLSDisabled, "force-tls-disabled", false, "")
	w.BoolVar(&cmd.forceNonSSLSessionPort, "force-non-ssl-session-port", false, "")
}

func (cmd *command) run() error {
	logger := log.NewDevelopmentWithLevel(zap.DebugLevel)
	ctx := context.Background()

	client, err := cmd.getClient(ctx, logger)
	if err != nil {
		return errors.Wrap(err, "create client")
	}

	session, err := cmd.getSession(ctx)
	if err != nil {
		return errors.Wrap(err, "create session")
	}

	s, err := repair.NewService(session, repair.DefaultConfig(), metrics.NewRepairMetrics(),
		func(ctx context.Context, _ uuid.UUID) (*scyllaclient.Client, error) {
			return client, nil
		},
		func(ctx context.Context, _ uuid.UUID, _ ...cluster.SessionConfigOption) (gocqlx.Session, error) {
			return cmd.getClusterSession(ctx, client)
		}, nil, logger.Named("repair"))
	if err != nil {
		return errors.Wrap(err, "create repair service")
	}

	props, err := json.Marshal(defaultTaskProperties())
	if err != nil {
		return errors.Wrap(err, "marshal task properties")
	}

	return s.Runner().Run(ctx, uuid.Nil, uuid.Nil, uuid.Nil, props)
}

func (cmd *command) getClient(ctx context.Context, logger log.Logger) (*scyllaclient.Client, error) {
	config := scyllaclient.DefaultConfigWithTimeout(scyllaclient.DefaultTimeoutConfig())
	if cmd.port != 0 {
		config.Port = fmt.Sprint(cmd.port)
	}
	config.AuthToken = cmd.authToken
	knownHosts, err := cmd.knownHosts(ctx, logger)
	if err != nil {
		return nil, errors.Wrap(err, "get known hosts")
	}
	config.Hosts = knownHosts

	return scyllaclient.NewClient(config, logger.Named("client"))
}

func (cmd *command) knownHosts(ctx context.Context, logger log.Logger) ([]string, error) {
	config := scyllaclient.DefaultConfigWithTimeout(scyllaclient.DefaultTimeoutConfig())
	if cmd.port != 0 {
		config.Port = fmt.Sprint(cmd.port)
	}
	config.Timeout = 5 * time.Second
	config.AuthToken = cmd.authToken
	config.Hosts = []string{cmd.host}

	client, err := scyllaclient.NewClient(config, logger.Named("client"))
	if err != nil {
		return nil, err
	}
	defer logutil.LogOnError(ctx, logger, client.Close, "Couldn't close scylla client")

	return client.GossiperEndpointLiveGet(ctx)
}

func (cmd *command) getClusterSession(ctx context.Context, client *scyllaclient.Client) (session gocqlx.Session, err error) {
	cfg := gocql.NewCluster()

	sessionHosts, err := cmd.getRPCAddresses(ctx, client)
	if err != nil {
		if errors.Is(err, errNoRPCAddressesFound) {
			return session, err
		}
	}
	cfg.Hosts = sessionHosts

	ni, err := client.AnyNodeInfo(ctx)
	if err != nil {
		return session, errors.Wrap(err, "fetch node info")
	}
	if err := cmd.extendClusterConfigWithAuthentication(ni, cfg); err != nil {
		return session, errors.Wrap(err, "extend cluster config with authentication")
	}
	if err := cmd.extendClusterConfigWithTLS(ni, cfg); err != nil {
		return session, errors.Wrap(err, "extend cluster config with TLS")
	}

	return gocqlx.WrapSession(cfg.CreateSession())
}

func (cmd *command) getRPCAddresses(ctx context.Context, client *scyllaclient.Client) ([]string, error) {
	var sessionHosts []string
	var combinedError error
	for _, h := range client.Config().Hosts {
		ni, err := client.NodeInfo(ctx, h)
		if err != nil {
			combinedError = multierr.Append(combinedError, err)
			continue
		}
		addr := ni.CQLAddr(h, cmd.forceTLSDisabled || cmd.forceNonSSLSessionPort)
		sessionHosts = append(sessionHosts, addr)
	}

	if len(sessionHosts) == 0 {
		combinedError = multierr.Append(errNoRPCAddressesFound, combinedError)
	}

	return sessionHosts, combinedError
}

var errNoRPCAddressesFound = errors.New("no RPC addresses found")

func (cmd *command) extendClusterConfigWithAuthentication(ni *scyllaclient.NodeInfo, cfg *gocql.ClusterConfig) error {
	if ni.CqlPasswordProtected {
		cfg.Authenticator = gocql.PasswordAuthenticator{
			Username: cmd.username,
			Password: cmd.password,
		}
	}
	return nil
}

func (cmd *command) extendClusterConfigWithTLS(ni *scyllaclient.NodeInfo, cfg *gocql.ClusterConfig) error {
	if ni.ClientEncryptionEnabled && !cmd.forceTLSDisabled {
		cfg.SslOpts = &gocql.SslOptions{
			Config: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
		if ni.ClientEncryptionRequireAuth {
			userKey, err := os.ReadFile(cmd.sslUserKeyFile)
			if err != nil {
				return errors.Wrap(err, "read user key file")
			}
			userCrt, err := os.ReadFile(cmd.sslUserCertFile)
			if err != nil {
				return errors.Wrap(err, "read user cert file")
			}
			keyPair, err := tls.X509KeyPair(userCrt, userKey)
			if err != nil {
				return errors.Wrap(err, "invalid TLS/SSL user key pair")
			}
			cfg.SslOpts.Config.Certificates = []tls.Certificate{keyPair}
		}
	}

	return nil
}

//go:embed schema.sql
var schema string

func (cmd *command) getSession(ctx context.Context) (*queries.Queries, error) {
	if cmd.dataPath == "" {
		return nil, errors.New("no data path specified")
	}
	fi, err := os.Stat(cmd.dataPath)
	if err != nil {
		return nil, errors.Wrap(err, "check data path")
	}
	if fi.Mode().IsDir() {
		cmd.dataPath = path.Join(cmd.dataPath, "db.db")
	}
	db, err := sql.Open("sqlite", cmd.dataPath)
	if err != nil {
		return nil, errors.Wrap(err, "opens sqllite db")
	}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		return nil, errors.Wrap(err, "create schema")
	}
	return queries.New(db), nil
}

// taskProperties is the main data structure of the runner.Properties blob.
type taskProperties struct {
	Keyspace            []string `json:"keyspace"`
	DC                  []string `json:"dc"`
	Host                string   `json:"host"`
	IgnoreDownHosts     bool     `json:"ignore_down_hosts"`
	FailFast            bool     `json:"fail_fast"`
	Continue            bool     `json:"continue"`
	Intensity           float64  `json:"intensity"`
	Parallel            int      `json:"parallel"`
	SmallTableThreshold int64    `json:"small_table_threshold"`
}

func defaultTaskProperties() *taskProperties {
	return &taskProperties{
		// Don't repair system_traces unless it has been deliberately specified.
		Keyspace: []string{"*", "!system_traces"},

		Continue:  true,
		Intensity: 0,

		// Consider 1GB table as small by default.
		SmallTableThreshold: 1 * 1024 * 1024 * 1024,
	}
}
