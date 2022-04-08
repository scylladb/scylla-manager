// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"runtime"
	"strings"

	api "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/downloader"
	"github.com/scylladb/scylla-manager/v3/pkg/rclone"
	backup "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	scyllaOperations "github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/client/operations"
	"github.com/spf13/cobra"
	"go.uber.org/zap/zapcore"
)

var downloadFilesArgs = struct {
	configFiles []string

	location    backup.LocationValue
	dataDir     string
	keyspace    []string
	mode        downloader.TableDirModeValue
	clearTables bool
	dryRun      bool

	nodeID      uuid.Value
	snapshotTag backup.SnapshotTagValue

	rateLimit int
	parallel  int
	debug     bool

	dumpManifest  bool
	dumpTokens    bool
	listSnapshots bool
	listNodes     bool
}{}

var downloadFilesCmd = &cobra.Command{
	Use:   "download-files",
	Short: "Downloads files from backup location",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		a := downloadFilesArgs

		// Validate action flags
		actionFlags := 0
		for _, b := range []bool{a.dryRun, a.dumpManifest, a.dumpTokens, a.listSnapshots, a.listNodes} {
			if b {
				actionFlags++
			}
		}
		if actionFlags > 1 {
			return errors.Errorf("you can specify only one of the flags: %q, %q, %q, %q, %q",
				"dry-run", "dump-manifest", "dump-tokens", "list-snapshots", "list-nodes")
		}

		// Setup command
		level := zapcore.ErrorLevel
		if a.debug {
			level = zapcore.DebugLevel
		}
		c, logger, err := setupCommand(a.configFiles, level)
		if err != nil {
			return err
		}

		// Set parallel
		rclone.GetConfig().Checkers = 2 * a.parallel
		rclone.GetConfig().Transfers = a.parallel
		// Prefer local listing of directory to checking one by one
		rclone.GetConfig().NoTraverse = false
		// Set rate limit
		rclone.SetRateLimit(a.rateLimit)
		// Start accounting after setting all options
		rclone.StartAccountingOperations()

		// Create downloader
		opts := []downloader.Option{
			downloader.WithTableDirMode(a.mode.Value()),
		}
		if len(a.keyspace) != 0 {
			opts = append(opts, downloader.WithKeyspace(a.keyspace))
		}
		if a.clearTables {
			opts = append(opts, downloader.WithClearTables())
		}
		d, err := downloader.New(a.location.Value(), a.dataDir, logger, opts...)
		if err != nil {
			return err
		}

		var (
			ctx = context.Background()
			w   = cmd.OutOrStdout()
		)

		// Handle list nodes
		if a.listNodes {
			nodes, err := d.ListNodes(ctx)
			if err != nil {
				return err
			}
			if _, err := nodes.WriteTo(w); err != nil {
				return err
			}
			return nil
		}

		// Get node ID
		nodeID := a.nodeID.Value()
		if nodeID == uuid.Nil {
			addr := net.JoinHostPort(c.Scylla.APIAddress, c.Scylla.APIPort)
			nodeID, err = localNodeID(ctx, addr)
			if err != nil {
				logger.Info(ctx, "Failed to get node ID from Scylla", "error", err)
				return errors.Errorf("could not get node ID from Scylla, set flag %q", "node")
			}
		}

		// Handle list snapshots
		if a.listSnapshots {
			snapshots, err := d.ListNodeSnapshots(ctx, nodeID)
			if err != nil {
				return err
			}
			for _, s := range snapshots {
				fmt.Fprintln(w, s)
			}
			return nil
		}

		// Validate if snapshot tag is set
		snapshotTag := a.snapshotTag.Value()
		if snapshotTag == "" {
			return errors.Errorf("required flag(s) %q not set", "snapshot-tag")
		}

		// Get manifest
		m, err := d.LookupManifest(ctx, downloader.ManifestLookupCriteria{NodeID: nodeID, SnapshotTag: snapshotTag})
		if err != nil {
			return errors.Wrap(err, "lookup manifest")
		}

		// Handle action flags that work with the manifest
		switch {
		case a.dumpManifest:
			enc := json.NewEncoder(w)
			enc.SetIndent("", "  ")
			return enc.Encode(m.ManifestContent)
		case a.dumpTokens:
			for i := range m.Tokens {
				if i > 0 {
					fmt.Fprint(w, ",")
				}
				fmt.Fprintf(w, "%d", m.Tokens[i])
			}
			fmt.Fprintln(w)
			return nil
		case a.dryRun:
			plan, err := d.DryRun(ctx, m)
			if err != nil {
				return err
			}
			if a.dataDir != "" {
				plan.BaseDir = a.dataDir
			}
			if _, err := plan.WriteTo(w); err != nil {
				return err
			}
			return nil
		}

		// Download files
		if !a.debug {
			stop := rclone.StartProgress()
			defer stop()
		}
		return d.Download(ctx, m)
	},
}

func localNodeID(ctx context.Context, addr string) (uuid.UUID, error) {
	c := scyllaOperations.New(api.New(addr, "/", []string{"http"}), strfmt.Default)
	resp, err := c.StorageServiceHostidLocalGet(scyllaOperations.NewStorageServiceHostidLocalGetParamsWithContext(ctx))
	if err != nil {
		return uuid.Nil, err
	}
	return uuid.Parse(resp.Payload)
}

func init() {
	cmd := downloadFilesCmd
	f := cmd.Flags()
	a := &downloadFilesArgs

	f.StringSliceVarP(&a.configFiles, "config-file", "c", []string{"/etc/scylla-manager-agent/scylla-manager-agent.yaml"}, "configuration file `path`")
	f.VarP(&a.location, "location", "L", "backup location in the format <provider>:<name> e.g. s3:my-bucket, the supported providers are: "+strings.Join(backup.Providers(), ", "))                   //nolint: lll
	f.StringVarP(&a.dataDir, "data-dir", "d", "", "`path` to Scylla data directory (typically /var/lib/scylla/data) or other directory to use for downloading the files (default current directory)") //nolint: lll
	f.StringSliceVarP(&a.keyspace, "keyspace", "K", nil, "a comma-separated `list` of keyspace/tables glob patterns, e.g. 'keyspace,!keyspace.table_prefix_*'")
	f.Var(&a.mode, "mode", "mode changes resulting directory structure, supported values are: `upload, sstableloader`, set 'upload' to use table upload directories, set 'sstableloader' for <keyspace>/<table> directories layout") // nolint: lll
	f.BoolVar(&a.clearTables, "clear-tables", false, "remove sstables before downloading")
	f.BoolVar(&a.dryRun, "dry-run", false, "validate and print a plan without downloading (or clearing) any files")
	f.VarP(&a.nodeID, "node", "n", "'Host `ID`' value from nodetool status command output of a node you want to restore (default local node)")
	f.VarP(&a.snapshotTag, "snapshot-tag", "T", "Scylla Manager snapshot `tag` as read from backup listing e.g. sm_20060102150405UTC, use --list-snapshots to get a list of snapshots of the node") // nolint: lll
	f.IntVar(&a.rateLimit, "rate-limit", 0, "rate limit in megabytes (MiB) per second (default no limit)")
	f.IntVarP(&a.parallel, "parallel", "p", 2*runtime.NumCPU(), "how many files to download in parallel")
	f.BoolVar(&a.debug, "debug", false, "enable debug logs")
	f.BoolVar(&a.dumpManifest, "dump-manifest", false, "print Scylla Manager backup manifest as JSON")
	f.BoolVar(&a.dumpTokens, "dump-tokens", false, "print list of tokens from the manifest")
	f.BoolVar(&a.listSnapshots, "list-snapshots", false, "print list of snapshots of the specified node, this also takes into account keyspace filter and returns only snapshots containing any of requested keyspaces or tables, newest snapshots are printed first") // nolint: lll
	f.BoolVar(&a.listNodes, "list-nodes", false, "print list of nodes including cluster name and node IP, this command would help you find nodes you can restore data from")                                                                                           // nolint: lll

	requireFlags(cmd, "location")
	rootCmd.AddCommand(cmd)
}
