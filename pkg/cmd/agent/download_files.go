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
	f.VarP(&a.location, "location", "L", "backup location in the format [<dc>:]<provider>:<name> ex. s3:my-bucket. The <dc>: part is optional and is only needed when different datacenters are being used to upload data to different locations. The supported providers are: "+strings.Join(backup.Providers(), ", ")) //nolint: lll
	f.StringVarP(&a.dataDir, "data-dir", "d", "", "`path` to Scylla data directory (/var/lib/scylla/data) or other directory to use for downloading the files")
	f.StringSliceVarP(&a.keyspace, "keyspace", "K", nil, "a comma-separated `list` of keyspace/tables glob patterns, e.g. 'keyspace,!keyspace.table_prefix_*'")
	f.Var(&a.mode, "mode", "`upload|sstableloader`, use an alternate table directory structure, upload for table upload directories, sstableloader for <keyspace>/<table>")
	f.BoolVar(&a.clearTables, "clear-tables", false, "remove sstables before downloading")
	f.BoolVar(&a.dryRun, "dry-run", false, "validates and prints backup information without downloading (or clearing) any files")
	f.VarP(&a.nodeID, "node", "n", "`ID` of node whose snapshot you want to download, you can obtain ID from nodetool status")
	f.VarP(&a.snapshotTag, "snapshot-tag", "T", "snapshot `tag` as read from backup listing")
	f.IntVar(&a.rateLimit, "rate-limit", 0, "rate limit in megabytes (MiB) per second, set to 0 for no limit")
	f.IntVarP(&a.parallel, "parallel", "p", 2*runtime.NumCPU(), "how many files to download in parallel")
	f.BoolVar(&a.debug, "debug", false, "enable debug logs")
	f.BoolVar(&a.dumpManifest, "dump-manifest", false, "print Scylla Manager backup manifest as json")
	f.BoolVar(&a.dumpTokens, "dump-tokens", false, "print list of tokens owned by the snapshoted node")

	requireFlags(cmd, "location", "data-dir", "node", "snapshot-tag")
	rootCmd.AddCommand(cmd)
}
