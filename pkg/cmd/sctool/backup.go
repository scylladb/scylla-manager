// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/pkg/managerclient"
	"github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/pkg/service/scheduler"
	"github.com/scylladb/scylla-manager/pkg/util/duration"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/atomic"
)

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Schedules backups",
	RunE: func(cmd *cobra.Command, args []string) error {
		t := &managerclient.Task{
			Type:       "backup",
			Enabled:    true,
			Schedule:   new(managerclient.Schedule),
			Properties: make(map[string]interface{}),
		}

		return backupTaskUpdate(t, cmd)
	},
}

func backupTaskUpdate(t *managerclient.Task, cmd *cobra.Command) error {
	if err := commonFlagsUpdate(t, cmd); err != nil {
		return err
	}

	props := t.Properties.(map[string]interface{})

	if f := cmd.Flag("location"); f.Changed {
		locations, err := cmd.Flags().GetStringSlice("location")
		if err != nil {
			return err
		}
		props["location"] = locations
	}

	if f := cmd.Flag("retention"); f.Changed {
		retention, err := cmd.Flags().GetInt("retention")
		if err != nil {
			return err
		}
		props["retention"] = retention
	}

	for _, name := range []string{"rate-limit", "snapshot-parallel", "upload-parallel"} {
		if f := cmd.Flag(name); f.Changed {
			v, err := cmd.Flags().GetStringSlice(name)
			if err != nil {
				return err
			}
			props[strings.Replace(name, "-", "_", 1)] = v
		}
	}

	if f := cmd.Flag("purge-only"); f.Changed {
		ok, err := cmd.Flags().GetBool("purge-only")
		if err != nil {
			return err
		}
		props["purge_only"] = ok
	}

	t.Properties = props

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

	if t.ID == "" {
		id, err := client.CreateTask(ctx, cfgCluster, t)
		if err != nil {
			return err
		}
		t.ID = id.String()
	} else if err := client.UpdateTask(ctx, cfgCluster, t); err != nil {
		return err
	}

	fmt.Fprintln(cmd.OutOrStdout(), managerclient.TaskJoin(t.Type, t.ID))

	return nil
}

func commonFlagsUpdate(t *managerclient.Task, cmd *cobra.Command) error {
	props := t.Properties.(map[string]interface{})

	if f := cmd.Flag("enabled"); f != nil && f.Changed {
		enabled, err := strconv.ParseBool(f.Value.String())
		if err != nil {
			return err
		}
		t.Enabled = enabled
	}

	if f := cmd.Flag("start-date"); f.Changed || time.Time(t.Schedule.StartDate).IsZero() {
		f := cmd.Flag("start-date")
		startDate, err := managerclient.ParseStartDate(f.Value.String())
		if err != nil {
			return err
		}
		t.Schedule.StartDate = startDate
	}

	if f := cmd.Flag("interval"); f.Changed || t.Schedule.Interval == "" {
		i, err := cmd.Flags().GetString("interval")
		if err != nil {
			return err
		}
		if _, err := duration.ParseDuration(i); err != nil {
			return err
		}
		t.Schedule.Interval = i
	}

	if f := cmd.Flag("num-retries"); f.Changed || t.Schedule.NumRetries == 0 {
		nr, err := cmd.Flags().GetInt64("num-retries")
		if err != nil {
			return err
		}
		t.Schedule.NumRetries = nr
	}

	if f := cmd.Flag("keyspace"); f != nil && f.Changed {
		keyspace, err := cmd.Flags().GetStringSlice("keyspace")
		if err != nil {
			return err
		}
		props["keyspace"] = unescapeFilters(keyspace)
	}

	if f := cmd.Flag("dc"); f != nil && f.Changed {
		dc, err := cmd.Flags().GetStringSlice("dc")
		if err != nil {
			return err
		}
		props["dc"] = unescapeFilters(dc)
	}

	return nil
}

func init() {
	cmd := backupCmd
	taskInitCommonFlags(backupFlags(cmd))
	requireFlags(cmd, "location")
	register(cmd, rootCmd)
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
			minDate, err = managerclient.ParseDate(f.Value.String())
			if err != nil {
				return err
			}
		}
		if f := cmd.Flag("max-date"); f.Changed {
			maxDate, err = managerclient.ParseDate(f.Value.String())
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
	fs := cmd.Flags()
	fs.StringSliceP("location", "L", nil,
		"comma-separated `list` of backup locations in the format [<dc>:]<provider>:<name> e.g. s3:my-bucket, the supported providers are: "+strings.Join(backupspec.Providers(), ", ")+". The <dc>: part is optional and is only needed when different datacenters are being used to upload data to different locations") // nolint: lll
	fs.Bool("all-clusters", false,
		"show backups of all clusters stored in location")
	fs.StringSliceP("keyspace", "K", nil,
		"comma-separated `list` of keyspace/tables glob patterns, e.g. 'keyspace,!keyspace.table_prefix_*' used to include or exclude keyspaces from backup")
	fs.String("min-date", "",
		"specifies minimal snapshot date expressed in RFC3339 form or now[+duration], e.g. now+3d2h10m, valid units are d, h, m, s")
	fs.String("max-date", "",
		"specifies maximal snapshot date expressed in RFC3339 form or now[+duration], e.g. now+3d2h10m, valid units are d, h, m, s")
	fs.Bool("show-tables", false, "print all table names for a keyspace")
	register(cmd, backupCmd)
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
			filePath := strings.Replace(schemaPath, ":", "://", 1)
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
					filePath := strings.Replace(path.Join(fi.Location, t.Path, f), ":", "://", 1)

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
	fs := cmd.Flags()
	fs.StringSliceP("location", "L", nil,
		"comma-separated `list` of backup locations in the format [<dc>:]<provider>:<name> e.g. s3:my-bucket, the supported providers are: "+strings.Join(backupspec.Providers(), ", ")+". The <dc>: part is optional and is only needed when different datacenters are being used to upload data to different locations") // nolint: lll
	fs.Bool("all-clusters", false,
		"show backups of all clusters stored in location")
	fs.StringSliceP("keyspace", "K", nil,
		"comma-separated `list` of keyspace/tables glob patterns, e.g. 'keyspace,!keyspace.table_prefix_*' used to include or exclude keyspaces from backup")
	fs.StringP("snapshot-tag", "T", "", "snapshot `tag` as read from backup listing")

	fs.StringP("delimiter", "d", "\t", "use `delimiter` instead of TAB for field delimiter")
	fs.Bool("with-version", false, "render table names with version UUID")
	requireFlags(cmd, "snapshot-tag")
	register(cmd, backupCmd)
}

var backupDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Deletes backup snapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		var (
			location     []string
			snapshotTags []string
			err          error
		)

		location, err = cmd.Flags().GetStringSlice("location")
		if err != nil {
			return err
		}
		snapshotTags, err = cmd.Flags().GetStringSlice("snapshot-tag")
		if err != nil {
			return err
		}

		err = client.DeleteSnapshot(ctx, cfgCluster, location, snapshotTags)
		if err != nil {
			return err
		}

		return nil
	},
}

func init() {
	cmd := backupDeleteCmd
	fs := cmd.Flags()
	fs.StringSliceP("location", "L", nil,
		"comma-separated `list` of backup locations in the format [<dc>:]<provider>:<name> e.g. s3:my-bucket, the supported providers are: "+strings.Join(backupspec.Providers(), ", ")+". The <dc>: part is optional and is only needed when different datacenters are being used to upload data to different locations") // nolint: lll
	fs.StringSliceP("snapshot-tag", "T", nil, "comma-separated `list` of snapshot tags as read from backup listing")
	requireFlags(cmd, "snapshot-tag")
	register(cmd, backupCmd)
}

var backupUpdateCmd = &cobra.Command{
	Use:   "update <type/task-id>",
	Short: "Modifies a backup task",
	Args:  cobra.ExactArgs(1),

	RunE: func(cmd *cobra.Command, args []string) error {
		taskType, taskID, err := managerclient.TaskSplit(args[0])
		if err != nil {
			return err
		}

		if scheduler.TaskType(taskType) != scheduler.BackupTask {
			return fmt.Errorf("backup update can't handle %s task", taskType)
		}

		t, err := client.GetTask(ctx, cfgCluster, taskType, taskID)
		if err != nil {
			return err
		}

		return backupTaskUpdate(t, cmd)
	},
}

func init() {
	cmd := backupUpdateCmd
	fs := backupFlags(cmd)
	fs.StringP("enabled", "e", "true", "enabled")
	taskInitCommonFlags(fs)
	register(cmd, backupCmd)
}

func backupFlags(cmd *cobra.Command) *pflag.FlagSet {
	fs := cmd.Flags()
	fs.StringSliceP("keyspace", "K", nil,
		"comma-separated `list` of keyspace/tables glob patterns, e.g. 'keyspace,!keyspace.table_prefix_*' used to include or exclude keyspaces from backup")
	fs.StringSlice("dc", nil,
		"comma-separated `list` of datacenter glob patterns, e.g. 'dc1,!otherdc*' used to specify the DCs to include or exclude from backup")
	fs.StringSliceP("location", "L", nil,
		"comma-separated `list` of backup locations in the format [<dc>:]<provider>:<name> e.g. s3:my-bucket, the supported providers are: "+strings.Join(backupspec.Providers(), ", ")+". The <dc>: part is optional and is only needed when different datacenters are being used to upload data to different locations") // nolint: lll
	fs.Int("retention", 3,
		"number of backups which are to be stored")
	fs.StringSlice("rate-limit", nil,
		"comma-separated `list` of megabytes (MiB) per second rate limits expressed in the format [<dc>:]<limit>. The <dc>: part is optional and only needed when different datacenters need different upload limits. Set to 0 for no limit (default 100)") // nolint: lll
	fs.StringSlice("snapshot-parallel", nil,
		"comma-separated `list` of snapshot parallelism limits in the format [<dc>:]<limit>. The <dc>: part is optional and allows for specifying different limits in selected datacenters. If The <dc>: part is not set, the limit is global (e.g. 'dc1:2,5') the runs are parallel in n nodes (2 in dc1) and n nodes in all the other datacenters") // nolint: lll
	fs.StringSlice("upload-parallel", nil,
		"comma-separated `list` of upload parallelism limits in the format [<dc>:]<limit>. The <dc>: part is optional and allows for specifying different limits in selected datacenters. If The <dc>: part is not set the limit is global (e.g. 'dc1:2,5') the runs are parallel in n nodes (2 in dc1) and n nodes in all the other datacenters") // nolint: lll
	fs.Bool("dry-run", false,
		"validate and print backup information without scheduling a backup")
	fs.Bool("show-tables", false, "print all table names for a keyspace. Used only in conjunction with --dry-run")

	// Add hidden option to run purge process only.
	fs.Bool("purge-only", false, "")
	fs.MarkHidden("purge-only") // nolint: errcheck

	return fs
}

var backupValidateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validates backup files in remote locations",
	RunE: func(cmd *cobra.Command, args []string) error {
		t := &managerclient.Task{
			Type:       "validate_backup",
			Enabled:    true,
			Schedule:   new(managerclient.Schedule),
			Properties: make(map[string]interface{}),
		}

		if err := commonFlagsUpdate(t, cmd); err != nil {
			return err
		}

		props := t.Properties.(map[string]interface{})
		if f := cmd.Flag("location"); f.Changed {
			v, err := cmd.Flags().GetStringSlice("location")
			if err != nil {
				return err
			}
			props["location"] = v
		}

		if f := cmd.Flag("delete-orphaned-files"); f.Changed {
			v, err := cmd.Flags().GetBool("delete-orphaned-files")
			if err != nil {
				return err
			}
			props["delete_orphaned_files"] = v
		}

		if f := cmd.Flag("parallel"); f.Changed {
			v, err := cmd.Flags().GetInt("parallel")
			if err != nil {
				return err
			}
			props["parallel"] = v
		}

		id, err := client.CreateTask(ctx, cfgCluster, t)
		if err != nil {
			return err
		}

		fmt.Fprintln(cmd.OutOrStdout(), managerclient.TaskJoin(t.Type, id))

		return nil
	},
}

func init() {
	cmd := backupValidateCmd
	fs := cmd.Flags()
	fs.StringSliceP("location", "L", nil,
		"comma-separated `list` of backup locations in the format [<dc>:]<provider>:<name> e.g. s3:my-bucket. The <dc>: part is optional and is only needed when different datacenters are being used to upload data to different locations. The supported providers are: s3, gcs, azure") // nolint: lll
	fs.Bool("delete-orphaned-files", false, "delete data files not belonging to any snapshot if they are found")
	fs.Int("parallel", 0, "number of hosts to analyze in parallel")
	taskInitCommonFlagsWithParams(fs, 0)
	register(cmd, backupCmd)
}
