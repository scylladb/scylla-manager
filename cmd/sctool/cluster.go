// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/mermaidclient"
	"github.com/spf13/cobra"
)

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Add or delete clusters",
}

func init() {
	rootCmd.AddCommand(clusterCmd)
}

var (
	cfgClusterName       string
	cfgClusterHosts      []string
	cfgClusterShardCount int64
)

func clusterInitCommonFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&cfgClusterName, "name", "n", "", "alias `name`")
	cmd.Flags().StringSliceVar(&cfgClusterHosts, "hosts", nil, "comma-separated `list` of hosts")
	cmd.Flags().Int64Var(&cfgClusterShardCount, "shard-count", 16, "number of shards in nodes")
}

var clusterAddCmd = &cobra.Command{
	Use:   "add",
	Short: "Adds a cluster to management",
	Args:  cobra.NoArgs,

	RunE: func(cmd *cobra.Command, args []string) error {
		id, err := client.CreateCluster(context.Background(), &mermaidclient.Cluster{
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
}

func init() {
	clusterCmd.AddCommand(clusterAddCmd)
	clusterInitCommonFlags(clusterAddCmd)
}

var clusterUpdateCmd = &cobra.Command{
	Use:   "update",
	Short: "Modifies a cluster",

	RunE: func(cmd *cobra.Command, args []string) error {
		cluster, err := client.GetCluster(context.Background(), cfgCluster)
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

		if err := client.UpdateCluster(context.Background(), cluster); err != nil {
			return printableError{err}
		}

		return nil
	},
}

func init() {
	clusterCmd.AddCommand(clusterUpdateCmd)
	initClusterFlag(clusterUpdateCmd, clusterUpdateCmd.Flags())
	clusterInitCommonFlags(clusterUpdateCmd)
}

var clusterDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Deletes a cluster from management",

	RunE: func(cmd *cobra.Command, args []string) error {
		if err := client.DeleteCluster(context.Background(), cfgCluster); err != nil {
			return printableError{err}
		}

		return nil
	},
}

func init() {
	clusterCmd.AddCommand(clusterDeleteCmd)
	initClusterFlag(clusterDeleteCmd, clusterDeleteCmd.Flags())
}

var clusterListCmd = &cobra.Command{
	Use:   "list",
	Short: "Shows managed clusters",

	RunE: func(cmd *cobra.Command, args []string) error {
		units, err := client.ListClusters(context.Background())
		if err != nil {
			return printableError{err}
		}
		if len(units) == 0 {
			return nil
		}

		t := newTable("unit id", "name", "hosts", "shard count")
		for _, u := range units {
			t.AddRow(u.ID, u.Name, u.Hosts, u.ShardCount)
		}
		fmt.Fprint(cmd.OutOrStdout(), t.Render())

		return nil
	},
}

func init() {
	clusterCmd.AddCommand(clusterListCmd)
}
