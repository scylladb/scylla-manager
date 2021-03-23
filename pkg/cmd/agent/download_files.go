// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"

	"github.com/scylladb/scylla-manager/pkg/downloader"
	"github.com/scylladb/scylla-manager/pkg/rclone"
	backup "github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
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

	dumpManifest bool
	dumpTokens   bool
}{}

var downloadFilesCmd = &cobra.Command{
	Use:   "download-files",
	Short: "Downloads files from backup location",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		a := downloadFilesArgs

		level := zapcore.ErrorLevel
		if a.dryRun {
			level = zapcore.InfoLevel
		}
		if a.debug {
			level = zapcore.DebugLevel
		}
		logger, err := setupCommand(a.configFiles, level)
		if err != nil {
			return err
		}

		// Set parallel
		rclone.GetConfig().Checkers = 2 * a.parallel
		rclone.GetConfig().Transfers = a.parallel
		// Set rate limit
		rclone.SetRateLimit(a.rateLimit)
		// Start accounting after setting all options
		rclone.StartAccountingOperations()

		d, err := downloader.New(a.location.Value(), a.dataDir, logger)
		if err != nil {
			return err
		}
		if len(a.keyspace) != 0 {
			if _, err := d.WithKeyspace(a.keyspace); err != nil {
				return err
			}
		}
		d.WithTableDirMode(a.mode.Value())

		if a.clearTables {
			d.WithClearTables()
		}
		if a.dryRun {
			d.WithDryRun()
		}

		ctx := context.Background()
		c := downloader.ManifestLookupCriteria{
			NodeID:      a.nodeID.Value(),
			SnapshotTag: a.snapshotTag.Value(),
		}
		m, err := d.LookupManifest(ctx, c)
		if err != nil {
			return err
		}

		w := cmd.OutOrStdout()
		if a.dumpManifest {
			enc := json.NewEncoder(w)
			enc.SetIndent("", "  ")
			return enc.Encode(m.Content)
		}
		if a.dumpTokens {
			for i := range m.Content.Tokens {
				if i > 0 {
					fmt.Fprint(w, ",")
				}
				fmt.Fprintf(w, "%d", m.Content.Tokens[i])
			}
			fmt.Fprintln(w)
			return nil
		}

		if !a.dryRun && !a.debug {
			stop := rclone.StartProgress()
			defer stop()
		}
		return d.Download(ctx, m)
	},
}

func init() {
	cmd := downloadFilesCmd
	f := cmd.Flags()
	a := &downloadFilesArgs

	f.StringSliceVarP(&a.configFiles, "config-file", "c", []string{"/etc/scylla-manager-agent/scylla-manager-agent.yaml"}, "configuration file `path`")
	f.VarP(&a.location, "location", "L", "backup location in the format <provider>:<name> e.g. s3:my-bucket, the supported providers are: "+strings.Join(backup.Providers(), ", "))                   //nolint: lll
	f.StringVarP(&a.dataDir, "data-dir", "d", "", "`path` to Scylla data directory (typically /var/lib/scylla/data) or other directory to use for downloading the files (default current directory)") //nolint: lll
	f.StringSliceVarP(&a.keyspace, "keyspace", "K", nil, "a comma-separated `list` of keyspace/tables glob patterns, e.g. 'keyspace,!keyspace.table_prefix_*'")
	f.Var(&a.mode, "mode", "`upload|sstableloader`, use an alternate table directory structure, set 'upload' to use table upload directories, set 'sstableloader' for <keyspace>/<table> directories layout") //nolint: lll
	f.BoolVar(&a.clearTables, "clear-tables", false, "remove sstables before downloading")
	f.BoolVar(&a.dryRun, "dry-run", false, "validates and prints backup information without downloading (or clearing) any files")
	f.VarP(&a.nodeID, "node", "n", "nodetool status Host `ID` of node you want to restore")
	f.VarP(&a.snapshotTag, "snapshot-tag", "T", "Scylla Manager snapshot `tag` as read from backup listing e.g. sm_20060102150405UTC")
	f.IntVar(&a.rateLimit, "rate-limit", 0, "rate limit in megabytes (MiB) per second (default no limit)")
	f.IntVarP(&a.parallel, "parallel", "p", 2*runtime.NumCPU(), "how many files to download in parallel")
	f.BoolVar(&a.debug, "debug", false, "enable debug logs")
	f.BoolVar(&a.dumpManifest, "dump-manifest", false, "print Scylla Manager backup manifest as JSON")
	f.BoolVar(&a.dumpTokens, "dump-tokens", false, "print list of tokens owned by the snapshoted node")

	requireFlags(cmd, "location", "node", "snapshot-tag")
	rootCmd.AddCommand(cmd)
}
