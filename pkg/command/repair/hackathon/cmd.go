// Copyright (C) 2025 ScyllaDB

package hackathon

import (
	"context"
	"crypto/tls"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
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
	"github.com/spf13/cobra"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
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

	s, err := repair.NewService(cmd.dataPath, repair.DefaultConfig(), metrics.NewRepairMetrics(),
		func(ctx context.Context, _ uuid.UUID) (*scyllaclient.Client, error) {
			return client, nil
		},
		func(ctx context.Context, _ uuid.UUID, _ ...cluster.SessionConfigOption) (gocqlx.Session, error) {
			return cmd.getSession(ctx, client)
		}, nil, logger.Named("repair"))
	if err != nil {
		return errors.Wrap(err, "create repair service")
	}

	return s.Runner().Run(ctx, uuid.Nil, uuid.Nil, uuid.Nil, json.RawMessage{})
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

func (cmd *command) getSession(ctx context.Context, client *scyllaclient.Client) (session gocqlx.Session, err error) {
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
