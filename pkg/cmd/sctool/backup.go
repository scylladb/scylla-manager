// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/mermaid/pkg/mermaidclient"
	"github.com/scylladb/mermaid/pkg/util/duration"
	"github.com/spf13/cobra"
	"go.uber.org/atomic"
)

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Schedules backups",
	RunE: func(cmd *cobra.Command, args []string) error {
		props := make(map[string]interface{})

		t := &mermaidclient.Task{
			Type:       "backup",
			Enabled:    true,
			Schedule:   new(mermaidclient.Schedule),
			Properties: props,
		}

		f := cmd.Flag("start-date")
		startDate, err := mermaidclient.ParseStartDate(f.Value.String())
		if err != nil {
			return err
		}
		t.Schedule.StartDate = startDate

		i, err := cmd.Flags().GetString("interval")
		if err != nil {
			return err
		}
		if _, err := duration.ParseDuration(i); err != nil {
			return err
		}
		t.Schedule.Interval = i

		t.Schedule.NumRetries, err = cmd.Flags().GetInt64("num-retries")
		if err != nil {
			return err
		}

		if f = cmd.Flag("keyspace"); f.Changed {
			keyspace, err := cmd.Flags().GetStringSlice("keyspace")
			if err != nil {
				return err
			}
			props["keyspace"] = unescapeFilters(keyspace)
		}

		if f = cmd.Flag("dc"); f.Changed {
			dc, err := cmd.Flags().GetStringSlice("dc")
			if err != nil {
				return err
			}
			props["dc"] = unescapeFilters(dc)
		}

		locations, err := cmd.Flags().GetStringSlice("location")
		if err != nil {
			return err
		}
		props["location"] = locations

		if f = cmd.Flag("retention"); f.Changed {
			retention, err := cmd.Flags().GetInt("retention")
			if err != nil {
				return err
			}
			props["retention"] = retention
		}

		for _, name := range []string{"rate-limit", "snapshot-parallel", "upload-parallel"} {
			if f = cmd.Flag(name); f.Changed {
				v, err := cmd.Flags().GetStringSlice(name)
				if err != nil {
					return err
				}
				props[strings.Replace(name, "-", "_", 1)] = v
			}
		}

		force, err := cmd.Flags().GetBool("force")
		if err != nil {
			return err
		}

		dryRun, err := cmd.Flags().GetBool("dry-run")
		if err != nil {
			return err
		}
		if dryRun {
			showTables, err := cmd.Flags().GetBool("show-tables")
			if err != nil {
				return err
			}

			stillWaiting := atomic.NewBool(true)
			time.AfterFunc(5*time.Second, func() {
				if stillWaiting.Load() {
					fmt.Fprintf(cmd.OutOrStderr(), "NOTICE: this may take a while, we are performing disk size calculations on the nodes\n")
				}
			})

			res, err := client.GetBackupTarget(ctx, cfgCluster, t)
			stillWaiting.Store(false)
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStderr(), "NOTICE: dry run mode, backup is not scheduled\n\n")
			if showTables {
				res.ShowTables = -1
			}
			return res.Render(cmd.OutOrStdout())
		}

		id, err := client.CreateTask(ctx, cfgCluster, t, force)
		if err != nil {
			return err
		}

		fmt.Fprintln(cmd.OutOrStdout(), mermaidclient.TaskJoin("backup", id))

		return nil
	},
}

func init() {
	cmd := backupCmd
	withScyllaDocs(cmd, "/sctool/#backup")
	register(cmd, rootCmd)

	fs := cmd.Flags()
	fs.StringSliceP("keyspace", "K", nil,
		"a comma-separated `list` of keyspace/tables glob patterns, e.g. 'keyspace,!keyspace.table_prefix_*' used to include or exclude keyspaces from backup")
	fs.StringSlice("dc", nil,
		"a comma-separated `list` of datacenter glob patterns, e.g. 'dc1,!otherdc*' used to specify the DCs to include or exclude from backup")
	fs.StringSliceP("location", "L", nil,
		"a comma-separated `list` of backup locations in the format [<dc>:]<provider>:<name> ex. s3:my-bucket. The <dc>: part is optional and is only needed when different datacenters are being used to upload data to different locations. <name> must be an alphanumeric string and may contain a dash and or a dot, but other characters are forbidden. The only supported storage <provider> at the moment is s3") //nolint: lll
	fs.Int("retention", 3,
		"The number of backups which are to be stored")
	fs.StringSlice("rate-limit", nil,
		"a comma-separated `list` of megabytes (MiB) per second rate limits expressed in the format [<dc>:]<limit>. The <dc>: part is optional and only needed when different datacenters need different upload limits. Set to 0 for no limit (default 100)") //nolint: lll
	fs.StringSlice("snapshot-parallel", nil,
		"a comma-separated `list` of snapshot parallelism limits in the format [<dc>:]<limit>. The <dc>: part is optional and allows for specifying different limits in selected datacenters. If The <dc>: part is not set, the limit is global (e.g. 'dc1:2,5') the runs are parallel in n nodes (2 in dc1) and n nodes in all the other datacenters") //nolint: lll
	fs.StringSlice("upload-parallel", nil,
		"a comma-separated `list` of upload parallelism limits in the format [<dc>:]<limit>. The <dc>: part is optional and allows for specifying different limits in selected datacenters. If The <dc>: part is not set the limit is global (e.g. 'dc1:2,5') the runs are parallel in n nodes (2 in dc1) and n nodes in all the other datacenters") //nolint: lll
	fs.Bool("force", false,
		"forces backup to skip database validation and schedules a backup even if there are no matching keyspaces/tables")
	fs.Bool("dry-run", false,
		"validates and prints backup information without scheduling a backup")
	fs.Bool("show-tables", false, "print all table names for a keyspace")

	taskInitCommonFlags(fs)
	requireFlags(cmd, "location")
}

var backupListCmd = &cobra.Command{
	Use:   "list",
	Short: "Lists available backups",
	RunE: func(cmd *cobra.Command, args []string) error {
		var (
			location    []string
			allClusters bool
			keyspace    []string
			minDate     strfmt.DateTime
			maxDate     strfmt.DateTime

			err error
		)

		location, err = cmd.Flags().GetStringSlice("location")
		if err != nil {
			return err
		}
		allClusters, err = cmd.Flags().GetBool("all-clusters")
		if err != nil {
			return err
		}
		keyspace, err = cmd.Flags().GetStringSlice("keyspace")
		if err != nil {
			return err
		}
		if f := cmd.Flag("min-date"); f.Changed {
			minDate, err = mermaidclient.ParseDate(f.Value.String())
			if err != nil {
				return err
			}
		}
		if f := cmd.Flag("max-date"); f.Changed {
			maxDate, err = mermaidclient.ParseDate(f.Value.String())
			if err != nil {
				return err
			}
		}
		showTables, err := cmd.Flags().GetBool("show-tables")
		if err != nil {
			return err
		}

		stillWaiting := atomic.NewBool(true)
		time.AfterFunc(5*time.Second, func() {
			if stillWaiting.Load() {
				fmt.Fprintf(cmd.OutOrStderr(), "NOTICE: this may take a while, we are reading metadata from backup location(s)\n")
			}
		})

		list, err := client.ListBackups(ctx, cfgCluster, location, allClusters, keyspace, minDate, maxDate)
		stillWaiting.Store(false)
		if err != nil {
			return err
		}
		list.AllClusters = allClusters
		if showTables {
			list.ShowTables = -1
		}

		return list.Render(cmd.OutOrStdout())
	},
}

func init() {
	cmd := backupListCmd
	withScyllaDocs(cmd, "/sctool/#backup-list")
	register(cmd, backupCmd)

	fs := cmd.Flags()
	fs.StringSliceP("location", "L", nil,
		"a comma-separated `list` of backup locations in the format [<dc>:]<provider>:<name> ex. s3:my-bucket. The <dc>: part is optional and is only needed when different datacenters are being used to upload data to different locations. The supported providers are: s3") //nolint: lll
	fs.Bool("all-clusters", false,
		"show backups of all clusters stored in location")
	fs.StringSliceP("keyspace", "K", nil,
		"a comma-separated `list` of keyspace/tables glob patterns, e.g. 'keyspace,!keyspace.table_prefix_*' used to include or exclude keyspaces from backup")
	fs.String("min-date", "",
		"specifies minimal snapshot date expressed in RFC3339 form or now[+duration], e.g. now+3d2h10m, valid units are d, h, m, s")
	fs.String("max-date", "",
		"specifies maximal snapshot date expressed in RFC3339 form or now[+duration], e.g. now+3d2h10m, valid units are d, h, m, s")
	fs.Bool("show-tables", false, "print all table names for a keyspace")
}

var backupFilesCmd = &cobra.Command{
	Use:   "files",
	Short: "Lists files in backup",
	RunE: func(cmd *cobra.Command, args []string) error {
		var (
			location    []string
			allClusters bool
			keyspace    []string
			snapshotTag string
			withVersion bool

			err error
		)

		location, err = cmd.Flags().GetStringSlice("location")
		if err != nil {
			return err
		}
		allClusters, err = cmd.Flags().GetBool("all-clusters")
		if err != nil {
			return err
		}
		keyspace, err = cmd.Flags().GetStringSlice("keyspace")
		if err != nil {
			return err
		}
		snapshotTag, err = cmd.Flags().GetString("snapshot-tag")
		if err != nil {
			return err
		}
		withVersion, err = cmd.Flags().GetBool("with-version")
		if err != nil {
			return err
		}

		stillWaiting := atomic.NewBool(true)
		time.AfterFunc(5*time.Second, func() {
			if stillWaiting.Load() {
				fmt.Fprintf(cmd.OutOrStderr(), "NOTICE: this may take a while, we are reading metadata from backup location(s)\n")
			}
		})

		filesInfo, err := client.ListBackupFiles(ctx, cfgCluster, location, allClusters, keyspace, snapshotTag)
		stillWaiting.Store(false)
		if err != nil {
			return err
		}

		w := cmd.OutOrStdout()
		d := cmd.Flag("delimiter").Value.String()

		// Nodes may share path to schema, we will print only unique ones.
		schemaPaths := strset.New()
		for _, fi := range filesInfo {
			if fi.Schema != "" {
				schemaPaths.Add(path.Join(fi.Location, fi.Schema))
			}
		}
		// Schema files first
		for _, schemaPath := range schemaPaths.List() {
			var filePath = strings.Replace(schemaPath, ":", "://", 1)
			_, err = fmt.Fprintln(w, filePath, d, "./")
			if err != nil {
				return err
			}
		}
		for _, fi := range filesInfo {
			for _, t := range fi.Files {
				dir := path.Join(t.Keyspace, t.Table)
				if withVersion {
					dir += "-" + t.Version
				}
				for _, f := range t.Files {
					var filePath = strings.Replace(path.Join(fi.Location, t.Path, f), ":", "://", 1)

					_, err = fmt.Fprintln(w, filePath, d, dir)
					if err != nil {
						return err
					}
				}
			}
		}

		return nil
	},
}

func init() {
	cmd := backupFilesCmd
	withScyllaDocs(cmd, "/sctool/#backup-files")
	register(cmd, backupCmd)

	fs := cmd.Flags()
	fs.StringSliceP("location", "L", nil,
		"a comma-separated `list` of backup locations in the format [<dc>:]<provider>:<name> ex. s3:my-bucket. The <dc>: part is optional and is only needed when different datacenters are being used to upload data to different locations. The supported providers are: s3") //nolint: lll
	fs.Bool("all-clusters", false,
		"show backups of all clusters stored in location")
	fs.StringSliceP("keyspace", "K", nil,
		"a comma-separated `list` of keyspace/tables glob patterns, e.g. 'keyspace,!keyspace.table_prefix_*' used to include or exclude keyspaces from backup")
	fs.StringP("snapshot-tag", "T", "", "snapshot `tag` as read from backup listing")

	fs.StringP("delimiter", "d", "\t", "use `delimiter` instead of TAB for field delimiter")
	fs.Bool("with-version", false, "render table names with version UUID")

	requireFlags(cmd, "snapshot-tag")
}

var backupDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Deletes backup snapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		var (
			location    []string
			snapshotTag string
			err         error
		)

		location, err = cmd.Flags().GetStringSlice("location")
		if err != nil {
			return err
		}
		snapshotTag, err = cmd.Flags().GetString("snapshot-tag")
		if err != nil {
			return err
		}

		err = client.DeleteSnapshot(ctx, cfgCluster, location, snapshotTag)
		if err != nil {
			return err
		}

		return nil
	},
}

func init() {
	cmd := backupDeleteCmd
	withScyllaDocs(cmd, "/sctool/#backup-delete-snapshot")
	register(cmd, backupCmd)

	fs := cmd.Flags()
	fs.StringSliceP("location", "L", nil,
		"a comma-separated `list` of backup locations in the format [<dc>:]<provider>:<name> ex. s3:my-bucket. The <dc>: part is optional and is only needed when different datacenters are being used to upload data to different locations. The supported providers are: s3") //nolint: lll
	fs.StringP("snapshot-tag", "T", "", "snapshot `tag` as read from backup listing")

	requireFlags(cmd, "snapshot-tag")
}
