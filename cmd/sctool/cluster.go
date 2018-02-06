// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/mermaidclient"
	"github.com/spf13/cobra"
)

var clusterCmd = withoutArgs(&cobra.Command{
	Use:   "cluster",
	Short: "Add or delete clusters",
})

func init() {
	subcommand(clusterCmd, rootCmd)
}

var (
	cfgClusterName       string
	cfgClusterHosts      []string
	cfgClusterShardCount int64
)

func clusterInitCommonFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&cfgClusterName, "name", "n", "", "alias `name`")
	cmd.Flags().StringSliceVar(&cfgClusterHosts, "hosts", nil, "comma-separated `list` of hosts")
	cmd.Flags().Int64Var(&cfgClusterShardCount, "shard-count", 0, "number of shards per node, each node must have equal number of shards")
}

var clusterAddCmd = withoutArgs(&cobra.Command{
	Use:   "add",
	Short: "Adds a cluster to manager",

	RunE: func(cmd *cobra.Command, args []string) error {
		id, err := client.CreateCluster(ctx, &mermaidclient.Cluster{
			Name:       cfgClusterName,
			Hosts:      cfgClusterHosts,
			ShardCount: cfgClusterShardCount,
		})
		if err != nil {
			return printableError{err}
		}

		fmt.Fprintln(cmd.OutOrStdout(), id)

		return nil
	},
})

func init() {
	cmd := clusterAddCmd
	subcommand(cmd, clusterCmd)

	clusterInitCommonFlags(cmd)
	requireFlags(cmd, "hosts")
	requireFlags(cmd, "shard-count")
}

var clusterUpdateCmd = withoutArgs(&cobra.Command{
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
})

func init() {
	cmd := clusterUpdateCmd
	subcommand(cmd, clusterCmd)

	clusterInitCommonFlags(cmd)
}

var clusterDeleteCmd = withoutArgs(&cobra.Command{
	Use:   "delete",
	Short: "Deletes a cluster from manager",

	RunE: func(cmd *cobra.Command, args []string) error {
		if err := client.DeleteCluster(ctx, cfgCluster); err != nil {
			return printableError{err}
		}

		return nil
	},
})

func init() {
	cmd := clusterDeleteCmd
	subcommand(cmd, clusterCmd)
}

var clusterListCmd = withoutArgs(&cobra.Command{
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
})

func init() {
	subcommand(clusterListCmd, clusterCmd)
}
