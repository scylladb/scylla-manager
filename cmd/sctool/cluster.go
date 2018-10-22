// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"io/ioutil"

	"github.com/pkg/errors"
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
	cfgClusterSSHUser         string
	cfgClusterSSHIdentityFile string
)

func clusterInitCommonFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&cfgClusterName, "name", "n", "", "`alias` you can give to your cluster")
	cmd.Flags().StringVar(&cfgClusterHost, "host", "", "hostname or IP of one of the cluster nodes")
	cmd.Flags().StringVar(&cfgClusterSSHUser, "ssh-user", "", "SSH user `name` used to connect to the cluster nodes")
	cmd.Flags().StringVar(&cfgClusterSSHIdentityFile, "ssh-identity-file", "", "`path` to identity file containing SSH private key")
}

var clusterAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Add a cluster to manager",

	RunE: func(cmd *cobra.Command, args []string) error {
		c := &mermaidclient.Cluster{
			Name: cfgClusterName,
			Host: cfgClusterHost,
		}

		if cfgClusterSSHUser != "" && cfgClusterSSHIdentityFile == "" {
			return printableError{errors.New("missing flag \"ssh-identity-file\"")}
		}
		if cfgClusterSSHIdentityFile != "" && cfgClusterSSHUser == "" {
			return printableError{errors.New("missing flag \"ssh-user\"")}
		}
		if cfgClusterSSHUser != "" && cfgClusterSSHIdentityFile != "" {
			b, err := ioutil.ReadFile(cfgClusterSSHIdentityFile)
			if err != nil {
				return printableError{inner: err}
			}
			c.SSHIdentityFile = b
			c.SSHUser = cfgClusterSSHUser
		}

		id, err := client.CreateCluster(ctx, c)
		if err != nil {
			return printableError{err}
		}

		w := cmd.OutOrStdout()
		fmt.Fprintln(w, id)

		tasks, err := client.ListTasks(ctx, id, "repair", false, "")
		if err != nil {
			return printableError{err}
		}
		if len(tasks.ExtendedTaskSlice) > 0 {
			s := tasks.ExtendedTaskSlice[0].Schedule
			w := cmd.OutOrStderr()
			fmt.Fprintf(w, clipper, id, mermaidclient.FormatTime(s.StartDate), s.Interval, id)
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
			return printableError{err}
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
		if cmd.Flags().Changed("ssh-user") {
			cluster.SSHUser = cfgClusterSSHUser
			ok = true
		}
		if cmd.Flags().Changed("ssh-identity-file") {
			b, err := ioutil.ReadFile(cfgClusterSSHIdentityFile)
			if err != nil {
				return printableError{inner: err}
			}
			cluster.SSHIdentityFile = b
			ok = true
		}
		if !ok {
			return errors.New("nothing to do")
		}

		if err := client.UpdateCluster(ctx, cluster); err != nil {
			return printableError{err}
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
			return printableError{err}
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
			return printableError{err}
		}
		return render(cmd.OutOrStdout(), clusters)
	},
}

func init() {
	cmd := clusterListCmd
	withScyllaDocs(cmd, "/sctool/#cluster-list")
	register(cmd, clusterCmd)
}
