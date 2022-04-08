// Copyright (C) 2017 ScyllaDB

package clusterupdate

import (
	_ "embed"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/command/flag"
	"github.com/scylladb/scylla-manager/v3/pkg/managerclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/fsutil"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

//go:embed res.yaml
var res []byte

type command struct {
	cobra.Command
	client *managerclient.Client

	cluster              string
	name                 string
	host                 string
	port                 int64
	authToken            string
	username             string
	password             string
	sslUserCertFile      string
	sslUserKeyFile       string
	deleteCQLCredentials bool
	deleteSSLUserCert    bool
}

func NewCommand(client *managerclient.Client) *cobra.Command {
	cmd := &command{
		client: client,
	}
	if err := yaml.Unmarshal(res, &cmd.Command); err != nil {
		panic(err)
	}
	cmd.init()
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		return cmd.run()
	}
	return &cmd.Command
}

func (cmd *command) init() {
	defer flag.MustSetUsages(&cmd.Command, res, "cluster")

	w := flag.Wrap(cmd.Flags())
	w.Cluster(&cmd.cluster)
	w.Unwrap().StringVarP(&cmd.name, "name", "n", "", "")
	w.Unwrap().StringVar(&cmd.host, "host", "", "")
	w.Unwrap().Int64Var(&cmd.port, "port", 10001, "")
	w.Unwrap().StringVar(&cmd.authToken, "auth-token", "", "")
	w.Unwrap().StringVarP(&cmd.username, "username", "u", "", "")
	w.Unwrap().StringVarP(&cmd.password, "password", "p", "", "")
	w.Unwrap().StringVar(&cmd.sslUserCertFile, "ssl-user-cert-file", "", "")
	w.Unwrap().StringVar(&cmd.sslUserKeyFile, "ssl-user-key-file", "", "")
	w.Unwrap().BoolVar(&cmd.deleteCQLCredentials, "delete-cql-credentials", false, "")
	w.Unwrap().BoolVar(&cmd.deleteSSLUserCert, "delete-ssl-user-cert", false, "")
}

func (cmd *command) run() error {
	cluster, err := cmd.client.GetCluster(cmd.Context(), cmd.cluster)
	if err != nil {
		return err
	}

	ok := false
	if cmd.Flags().Changed("name") {
		cluster.Name = cmd.name
		ok = true
	}
	if cmd.Flags().Changed("host") {
		cluster.Host = cmd.host
		ok = true
	}
	if cmd.Flags().Changed("port") {
		cluster.Port = cmd.port
		ok = true
	}
	if cmd.Flags().Changed("username") {
		cluster.Username = cmd.username
		ok = true
	}
	if cmd.Flags().Changed("password") {
		cluster.Password = cmd.password
		ok = true
	}
	if cmd.Flags().Changed("auth-token") {
		cluster.AuthToken = cmd.authToken
		ok = true
	}

	if cmd.username != "" && cmd.password == "" {
		return errors.New("missing flag \"password\"")
	}
	if cmd.password != "" && cmd.username == "" {
		return errors.New("missing flag \"username\"")
	}

	if cmd.Flags().Changed("ssl-user-cert-file") {
		if cmd.sslUserKeyFile == "" {
			return errors.New("missing flag \"ssl-user-key-file\"")
		}
		b, err := fsutil.ReadFile(cmd.sslUserCertFile)
		if err != nil {
			return err
		}
		cluster.SslUserCertFile = b
		ok = true
	}
	if cmd.Flags().Changed("ssl-user-key-file") {
		if cmd.sslUserCertFile == "" {
			return errors.New("missing flag \"ssl-user-cert-file\"")
		}
		b, err := fsutil.ReadFile(cmd.sslUserKeyFile)
		if err != nil {
			return err
		}
		cluster.SslUserKeyFile = b
		ok = true
	}

	if !ok && !cmd.deleteCQLCredentials && !cmd.deleteSSLUserCert {
		return errors.New("nothing to do")
	}
	if cmd.deleteCQLCredentials || cmd.deleteSSLUserCert {
		if err := cmd.client.DeleteClusterSecrets(cmd.Context(), cmd.cluster, cmd.deleteCQLCredentials, cmd.deleteSSLUserCert); err != nil {
			return err
		}
	}
	return cmd.client.UpdateCluster(cmd.Context(), cluster)
}
