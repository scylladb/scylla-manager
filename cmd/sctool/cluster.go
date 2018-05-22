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
	register(clusterCmd, rootCmd)
}

var (
	cfgClusterName       string
	cfgClusterHosts      []string
	cfgClusterShardCount int64
	cfgSSHIdentityFile   string
	cfgSSHUser           string
)

func clusterInitCommonFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&cfgClusterName, "name", "n", "", "alias `name`")
	cmd.Flags().StringSliceVar(&cfgClusterHosts, "hosts", nil, "comma-separated `list` of hosts")
	cmd.Flags().Int64Var(&cfgClusterShardCount, "shard-count", 0, "number of shards per node, each node must have equal number of shards")
	cmd.Flags().StringVar(&cfgSSHIdentityFile, "ssh-identity-file", "", "SSH private key in PEM format")
	cmd.Flags().StringVar(&cfgSSHUser, "ssh-user", "", "SSH user used to connect to scylla nodes with")
}

var clusterAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Adds a cluster to manager",

	RunE: func(cmd *cobra.Command, args []string) error {
		c := &mermaidclient.Cluster{
			Name:       cfgClusterName,
			Hosts:      cfgClusterHosts,
			ShardCount: cfgClusterShardCount,
		}

		if cfgSSHIdentityFile != "" {
			b, err := ioutil.ReadFile(cfgSSHIdentityFile)
			if err != nil {
				return printableError{inner: err}
			}
			c.SSHIdentityFile = b
			if cfgSSHUser == "" {
				return printableError{errors.New("an ssh user is needed")}
			}
			c.SSHUser = cfgSSHUser
		}

		id, err := client.CreateCluster(ctx, c)
		if err != nil {
			return printableError{err}
		}

		w := cmd.OutOrStdout()
		fmt.Fprintln(w, id)

		tasks, err := client.ListSchedTasks(ctx, id, "repair_auto_schedule", false, "")
		if err != nil {
			return printableError{err}
		}

		if len(tasks) > 0 {
			s := tasks[0].Schedule
			werr := cmd.OutOrStderr()
			fmt.Fprintf(werr, clipper, id, formatTime(s.StartDate), s.IntervalDays, id)
		}

		return nil
	},
}

func init() {
	cmd := clusterAddCmd
	register(cmd, clusterCmd)

	clusterInitCommonFlags(cmd)
	requireFlags(cmd, "hosts")
	requireFlags(cmd, "shard-count")
}

var clusterUpdateCmd = &cobra.Command{
	Use:   "update",
	Short: "Modifies a cluster",

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
		if cmd.Flags().Changed("hosts") {
			cluster.Hosts = cfgClusterHosts
			ok = true
		}
		if cmd.Flags().Changed("shard-count") {
			cluster.ShardCount = cfgClusterShardCount
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
	register(cmd, clusterCmd)

	clusterInitCommonFlags(cmd)
}

var clusterDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Deletes a cluster from manager",

	RunE: func(cmd *cobra.Command, args []string) error {
		if err := client.DeleteCluster(ctx, cfgCluster); err != nil {
			return printableError{err}
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
		units, err := client.ListClusters(ctx)
		if err != nil {
			return printableError{err}
		}

		t := newTable("cluster id", "name", "hosts", "shard count")
		for _, u := range units {
			t.AddRow(u.ID, u.Name, u.Hosts, u.ShardCount)
		}
		fmt.Fprint(cmd.OutOrStdout(), t)

		return nil
	},
}

func init() {
	register(clusterListCmd, clusterCmd)
}
