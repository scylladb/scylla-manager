// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/managerclient"
	"github.com/scylladb/scylla-manager/pkg/util/clipper"
	"github.com/spf13/cobra"
)

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Add or delete clusters",
}

func init() {
	cmd := clusterCmd
	register(cmd, rootCmd)
}

var (
	cfgClusterID              string
	cfgClusterName            string
	cfgClusterHost            string
	cfgClusterAuthToken       string
	cfgClusterUsername        string
	cfgClusterPassword        string
	cfgClusterSSLUserCertFile string
	cfgClusterSSLUserKeyFile  string
)

func clusterInitCommonFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&cfgClusterName, "name", "n", "", "`alias` you can give to your cluster")
	cmd.Flags().StringVar(&cfgClusterHost, "host", "", "hostname or IP of one of the cluster nodes")
	cmd.Flags().StringVar(&cfgClusterAuthToken, "auth-token", "", "authentication token set on the cluster nodes in agent config file")
	cmd.Flags().StringVarP(&cfgClusterUsername, "username", "u", "", "CQL `username` used in advanced CQL health check")
	cmd.Flags().StringVarP(&cfgClusterPassword, "password", "p", "", "CQL `password` associated with user")
	cmd.Flags().StringVar(&cfgClusterSSLUserCertFile, "ssl-user-cert-file", "", "`path` to client certificate when using client/server encryption with require_client_auth enabled")
	cmd.Flags().StringVar(&cfgClusterSSLUserKeyFile, "ssl-user-key-file", "", "`path` to key associated with ssl-user-cert-file")
}

func clusterAddedMessage(w io.Writer, id, name string) error {
	nameOrID := func() string {
		if name != "" {
			return name
		}
		return id
	}

	nameOrPlaceholder := func() string {
		if name != "" {
			return name
		}
		return "<name>"
	}

	messageLines := []string{
		"Cluster added! You can set it as default, by exporting its name or ID as env variable:",
		"$ export SCYLLA_MANAGER_CLUSTER=" + id,
		"$ export SCYLLA_MANAGER_CLUSTER=" + nameOrPlaceholder(),
		"",
		"Now run:",
		"$ sctool status -c " + nameOrID(),
		"$ sctool task list -c " + nameOrID(),
	}

	return clipper.Say(w, messageLines...)
}

const clusterAddNoAuthTokenWarning = `
WARNING! Scylla data may be exposed
Protect it by specifying auth_token in /etc/scylla-manager-agent/scylla-manager-agent.yaml on Scylla nodes
`

var clusterAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Adds a cluster to manager",

	RunE: func(cmd *cobra.Command, args []string) error {
		if cfgClusterID != "" {
			clusters, err := client.ListClusters(ctx)
			if err != nil {
				return err
			}
			for _, c := range clusters {
				if c.ID == cfgClusterID {
					return errors.Errorf("Cluster ID  %q already taken", cfgClusterID)
				}
			}
		}

		c := &managerclient.Cluster{
			ID:        cfgClusterID,
			Name:      cfgClusterName,
			Host:      cfgClusterHost,
			AuthToken: cfgClusterAuthToken,
			Username:  cfgClusterUsername,
			Password:  cfgClusterPassword,
		}

		if cfgClusterUsername != "" && cfgClusterPassword == "" {
			return errors.New("missing flag \"password\"")
		}
		if cfgClusterPassword != "" && cfgClusterUsername == "" {
			return errors.New("missing flag \"username\"")
		}

		if cfgClusterSSLUserCertFile != "" && cfgClusterSSLUserKeyFile == "" {
			return errors.New("missing flag \"ssl-user-key-file\"")
		}
		if cfgClusterSSLUserKeyFile != "" && cfgClusterSSLUserCertFile == "" {
			return errors.New("missing flag \"ssl-user-cert-file\"")
		}
		if cfgClusterSSLUserCertFile != "" {
			b0, err := readFile(cfgClusterSSLUserCertFile)
			if err != nil {
				return err
			}
			c.SslUserCertFile = b0

			b1, err := readFile(cfgClusterSSLUserKeyFile)
			if err != nil {
				return err
			}
			c.SslUserKeyFile = b1
		}

		withoutRepair, err := cmd.Flags().GetBool("without-repair")
		if err != nil {
			return err
		}
		c.WithoutRepair = withoutRepair

		id, err := client.CreateCluster(ctx, c)
		if err != nil {
			return err
		}

		w := cmd.OutOrStdout()
		fmt.Fprintln(w, id)

		w = cmd.OutOrStderr()
		if err := clusterAddedMessage(w, id, cfgClusterName); err != nil {
			return err
		}

		if cfgClusterAuthToken == "" {
			fmt.Fprintln(w, clusterAddNoAuthTokenWarning)
		}

		return nil
	},
}

func init() {
	cmd := clusterAddCmd
	clusterInitCommonFlags(cmd)
	cmd.Flags().StringVarP(&cfgClusterID, "id", "i", "", "explicitly specify cluster ID, when not provided random UUID will be generated")
	cmd.Flags().Bool("without-repair", false, "skip automatic repair scheduling")
	requireFlags(cmd, "host")
	register(cmd, clusterCmd)
}

var clusterUpdateCmd = &cobra.Command{
	Use:   "update",
	Short: "Modifies a cluster",

	RunE: func(cmd *cobra.Command, args []string) error {
		cluster, err := client.GetCluster(ctx, cfgCluster)
		if err != nil {
			return err
		}

		ok := false
		if cmd.Flags().Changed("name") {
			cluster.Name = cfgClusterName
			ok = true
		}
		if cmd.Flags().Changed("host") {
			cluster.Host = cfgClusterHost
			ok = true
		}
		if cmd.Flags().Changed("username") {
			cluster.Username = cfgClusterUsername
			ok = true
		}
		if cmd.Flags().Changed("password") {
			cluster.Password = cfgClusterPassword
			ok = true
		}
		if cmd.Flags().Changed("auth-token") {
			cluster.AuthToken = cfgClusterAuthToken
			ok = true
		}

		if cfgClusterUsername != "" && cfgClusterPassword == "" {
			return errors.New("missing flag \"password\"")
		}
		if cfgClusterPassword != "" && cfgClusterUsername == "" {
			return errors.New("missing flag \"username\"")
		}

		if cmd.Flags().Changed("ssl-user-cert-file") {
			if cfgClusterSSLUserKeyFile == "" {
				return errors.New("missing flag \"ssl-user-key-file\"")
			}
			b, err := readFile(cfgClusterSSLUserCertFile)
			if err != nil {
				return err
			}
			cluster.SslUserCertFile = b
			ok = true
		}
		if cmd.Flags().Changed("ssl-user-key-file") {
			if cfgClusterSSLUserCertFile == "" {
				return errors.New("missing flag \"ssl-user-cert-file\"")
			}
			b, err := readFile(cfgClusterSSLUserKeyFile)
			if err != nil {
				return err
			}
			cluster.SslUserKeyFile = b
			ok = true
		}

		var (
			deleteCQLCredentials bool
			deleteSSLUserCert    bool
		)
		if cmd.Flags().Changed("delete-cql-credentials") {
			deleteCQLCredentials = true
		}
		if cmd.Flags().Changed("delete-ssl-user-cert") {
			deleteSSLUserCert = true
		}

		if !ok && !deleteCQLCredentials && !deleteSSLUserCert {
			return errors.New("nothing to do")
		}

		if err := client.DeleteClusterSecrets(ctx, cfgCluster, deleteCQLCredentials, deleteSSLUserCert); err != nil {
			return err
		}
		if err := client.UpdateCluster(ctx, cluster); err != nil {
			return err
		}

		return nil
	},
}

func init() {
	cmd := clusterUpdateCmd
	clusterInitCommonFlags(cmd)
	cmd.Flags().Bool("delete-cql-credentials", false, "delete CQL username and password if added, features that require CQL may not work")
	cmd.Flags().Bool("delete-ssl-user-cert", false, "delete SSL user certificate if added")
	register(cmd, clusterCmd)
}

var clusterDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Deletes a cluster from manager",

	RunE: func(cmd *cobra.Command, args []string) error {
		if err := client.DeleteCluster(ctx, cfgCluster); err != nil {
			return err
		}

		return nil
	},
}

func init() {
	cmd := clusterDeleteCmd
	register(cmd, clusterCmd)
}

var clusterListCmd = &cobra.Command{
	Use:   "list",
	Short: "Shows managed clusters",

	RunE: func(cmd *cobra.Command, args []string) error {
		clusters, err := client.ListClusters(ctx)
		if err != nil {
			return err
		}
		return render(cmd.OutOrStdout(), clusters)
	},
}

func init() {
	cmd := clusterListCmd
	register(cmd, clusterCmd)
}
