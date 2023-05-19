package main

import (
	"context"
	"crypto/tls"
	"os"
	"strconv"

	"github.com/gocql/gocql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	config "github.com/scylladb/scylla-manager/v3/pkg/config/server"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/util/netwait"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"xorm.io/xorm"
)

var (
	keyspace []string
	location []string
)

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Does the database backup to the specified location",

	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := config.ParseConfigFiles([]string{cfgFile})
		if err != nil {
			return errors.Wrapf(err, "failed to parse %s config file", cfgFile)
		}

		logger, err := c.MakeLogger()
		if err != nil {
			return errors.Wrapf(err, "logger")
		}
		defer logger.Sync()

		zap.RedirectStdLog(log.BaseOf(logger))
		netwait.DefaultWaiter.Logger = logger.Named("wait")

		engine, err := xorm.NewEngine("sqlite3", dbFile)
		if err != nil {
			return err
		}
		defer engine.Close()

		kcs := &keepCompatibilityService{logger: logger.Named("keep-backward-compatibility-service")}
		sqliteCache, err := backup.NewSQLiteCache(engine, logger.Named("sqlite cache"))
		if err != nil {
			return err
		}

		backupSvc, err := backup.NewService(
			sqliteCache,
			c.Backup,
			metrics.NewBackupMetrics().MustRegister(),
			kcs.clusterName,
			kcs.createScyllaClient,
			kcs.clusterCQLSession,
			logger.Named("backup"),
		)

		props := []byte(`
{
	"keyspace": ["system_distributed"],
	"location": ["s3:backuptest-smoke"]
}
`)
		ctx := context.Background()

		target, err := backupSvc.GetTarget(ctx, uuid.Nil, props)
		if err != nil {
			return err
		}
		if err := backupSvc.Backup(ctx, uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom(), target); err != nil {
			return err
		}

		return nil
	},
}

func init() {
	backupCmd.Flags().StringSliceVar(&keyspace, "keyspace", []string{}, "list of keyspaces to backup")
	backupCmd.Flags().StringSliceVar(&location, "location", []string{}, "list of backup locations")
	rootCmd.AddCommand(backupCmd)
}

type keepCompatibilityService struct {
	logger log.Logger
}

func (s *keepCompatibilityService) clusterName(ctx context.Context, clusterID uuid.UUID) (string, error) {
	return "cluster_name", nil
}

func (s *keepCompatibilityService) createScyllaClient(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
	sc := scyllaclient.DefaultConfig()
	sc.AuthToken = authToken
	sc.Transport = scyllaclient.DefaultTransport()
	sc.Hosts = []string{host}
	return scyllaclient.NewClient(sc, s.logger.Named("scylla"))
}

func (s *keepCompatibilityService) clusterCQLSession(ctx context.Context, clusterID uuid.UUID) (session gocqlx.Session, err error) {
	s.logger.Debug(ctx, "ClusterCQLSession", "cluster_id", clusterID)

	client, err := s.createScyllaClient(ctx, clusterID)
	if err != nil {
		return session, errors.Wrap(err, "get client")
	}

	ni, err := client.AnyNodeInfo(ctx)
	if err != nil {
		return session, errors.Wrap(err, "fetch node info")
	}

	scyllaCluster := gocql.NewCluster(client.Config().Hosts...)

	// Set port if needed
	if cqlPort := ni.CQLPort(client.Config().Hosts[0]); cqlPort != "9042" {
		p, err := strconv.Atoi(cqlPort)
		if err != nil {
			return session, errors.Wrap(err, "parse cql port")
		}
		scyllaCluster.Port = p
	}

	if ni.CqlPasswordProtected {
		scyllaCluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cqlUsername,
			Password: cqlPassword,
		}
	}

	if ni.ClientEncryptionEnabled {
		scyllaCluster.SslOpts = &gocql.SslOptions{
			Config: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
		if ni.ClientEncryptionRequireAuth {
			tlsCert, err := os.ReadFile(sslCertFile)
			if err != nil {
				return gocqlx.Session{}, err
			}
			tlsKey, err := os.ReadFile(sslKeyFile)
			if err != nil {
				return gocqlx.Session{}, err
			}
			keyPair, err := tls.X509KeyPair(tlsCert, tlsKey)
			if err != nil {
				return gocqlx.Session{}, errors.Wrap(err, "invalid TLS/SSL user key pair")
			}

			if err != nil {
				return session, err
			}
			scyllaCluster.SslOpts.Config.Certificates = []tls.Certificate{keyPair}
		}
	}

	return gocqlx.WrapSession(scyllaCluster.CreateSession())
}
