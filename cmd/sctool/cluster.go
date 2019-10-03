// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/internal/clipper"
	"github.com/scylladb/mermaid/mermaidclient"
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
	cfgClusterName            string
	cfgClusterHost            string
	cfgClusterAuthToken       string
	cfgClusterSSLUserCertFile string
	cfgClusterSSLUserKeyFile  string
)

func clusterInitCommonFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&cfgClusterName, "name", "n", "", "`alias` you can give to your cluster")
	cmd.Flags().StringVar(&cfgClusterHost, "host", "", "hostname or IP of one of the cluster nodes")
	cmd.Flags().StringVar(&cfgClusterAuthToken, "auth-token", "", "authentication token set on the cluster nodes in agent config file")
	cmd.Flags().StringVar(&cfgClusterSSLUserCertFile, "ssl-user-cert-file", "", "`path` to client certificate when using client/server encryption with require_client_auth enabled")
	cmd.Flags().StringVar(&cfgClusterSSLUserKeyFile, "ssl-user-key-file", "", "`path` to key associated with ssl-user-cert-file")
}

func clusterAddedMessage(w io.Writer, id, name, startDay, interval string) error {
	if name == "" {
		name = "<name> (use --name flag to set cluster name)"
	}
	messageLines := []string{
		"Cluster added! You can set it as default, by exporting env variable.",
		"",
		"$ export SCYLLA_MANAGER_CLUSTER=" + id,
		"$ export SCYLLA_MANAGER_CLUSTER=" + name,
		"",
		"To see the currently scheduled tasks run:",
		"$ sctool task list -c " + id,
	}
	return clipper.Say(w, messageLines...)
}

var clusterAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Add a cluster to manager",

	RunE: func(cmd *cobra.Command, args []string) error {
		c := &mermaidclient.Cluster{
			Name:      cfgClusterName,
			Host:      cfgClusterHost,
			AuthToken: cfgClusterAuthToken,
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

		id, err := client.CreateCluster(ctx, c)
		if err != nil {
			return err
		}

		w := cmd.OutOrStdout()
		fmt.Fprintln(w, id)

		tasks, err := client.ListTasks(ctx, id, "repair", false, "")
		if err != nil {
			return err
		}
		if len(tasks.ExtendedTaskSlice) > 0 {
			s := tasks.ExtendedTaskSlice[0].Schedule
			w := cmd.OutOrStderr()
			if err := clusterAddedMessage(w, id, cfgClusterName, mermaidclient.FormatTime(s.StartDate), s.Interval); err != nil {
				return err
			}
		}

		if cfgClusterAuthToken == "" {
			fmt.Fprintln(w, "WARNING! Scylla data is exposed, "+
				"protect it by specifying auth_token in Scylla Manager Agent config file on Scylla nodes\n")
		}

		return nil
	},
}

func init() {
	cmd := clusterAddCmd
	withScyllaDocs(cmd, "/add-a-cluster/", "/sctool/#cluster-add")
	register(cmd, clusterCmd)

	clusterInitCommonFlags(cmd)
	requireFlags(cmd, "host")
}

var clusterUpdateCmd = &cobra.Command{
	Use:   "update",
	Short: "Modify a cluster",

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
		if cmd.Flags().Changed("auth-token") {
			cluster.AuthToken = cfgClusterAuthToken
			ok = true
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
		if !ok {
			return errors.New("nothing to do")
		}

		if err := client.UpdateCluster(ctx, cluster); err != nil {
			return err
		}

		return nil
	},
}

func init() {
	cmd := clusterUpdateCmd
	withScyllaDocs(cmd, "/sctool/#cluster-update")
	register(cmd, clusterCmd)

	clusterInitCommonFlags(cmd)
}

var clusterDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete a cluster from manager",

	RunE: func(cmd *cobra.Command, args []string) error {
		if err := client.DeleteCluster(ctx, cfgCluster); err != nil {
			return err
		}

		return nil
	},
}

func init() {
	cmd := clusterDeleteCmd
	withScyllaDocs(cmd, "/sctool/#cluster-delete")
	register(cmd, clusterCmd)
}

var clusterListCmd = &cobra.Command{
	Use:   "list",
	Short: "Show managed clusters",

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
	withScyllaDocs(cmd, "/sctool/#cluster-list")
	register(cmd, clusterCmd)
}
