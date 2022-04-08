// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/scylladb/scylla-manager/v3/pkg/rclone"
	"github.com/scylladb/scylla-manager/v3/pkg/rclone/bench"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/spf13/cobra"
	"go.uber.org/zap/zapcore"
)

var benchmarkArgs = struct {
	dirGlob  []string
	location backupspec.LocationValue

	configFiles   []string
	debug         bool
	memProfileDir string
	prometheus    string
	rateLimit     int
}{}

var benchmarkCmd = &cobra.Command{
	Use:   "benchmark",
	Short: "Executes benchmark scenarios, copies all files in each scenario directory to the location",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		ctx := context.Background()
		defer func() {
			if err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "FAILED: %v\n", err)
				os.Exit(1)
			}
		}()

		level := zapcore.ErrorLevel
		if benchmarkArgs.debug {
			level = zapcore.DebugLevel
		}
		_, logger, err := setupCommand(benchmarkArgs.configFiles, level)
		if err != nil {
			return err
		}

		// Run Prometheus if needed
		if benchmarkArgs.prometheus != "" {
			go func() {
				prometheusServer := &http.Server{
					Addr:    benchmarkArgs.prometheus,
					Handler: promhttp.Handler(),
				}
				logger.Info(ctx, "Starting Prometheus server", "addr", benchmarkArgs.prometheus)
				if err := prometheusServer.ListenAndServe(); err != nil {
					logger.Error(ctx, "Failed to start prometheus server", "error", err)
				}
			}()
		}

		// Set rate limit
		rclone.StartAccountingOperations()
		// Start accounting after setting all options
		rclone.SetRateLimit(benchmarkArgs.rateLimit)

		// Run the scenarios
		w := cmd.OutOrStderr()
		b, err := bench.NewBenchmark(ctx, benchmarkArgs.location.Value().RemotePath(""))
		if err != nil {
			return err
		}
		for _, g := range benchmarkArgs.dirGlob {
			matches, err := filepath.Glob(g)
			if err != nil {
				return errors.Wrap(err, "listing scenarios")
			}
			for _, match := range matches {
				dir, err := filepath.Abs(match)
				if err != nil {
					return errors.Wrapf(err, "absolute path %s", match)
				}
				s, err := b.StartScenario(ctx, dir)
				if err != nil {
					return errors.Wrapf(err, "benchmark %s", dir)
				}
				if _, err := s.WriteTo(w); err != nil {
					return errors.Wrapf(err, "summary %s", dir)
				}

				if benchmarkArgs.memProfileDir != "" {
					err := os.MkdirAll(benchmarkArgs.memProfileDir, 0o755)
					if err != nil {
						return errors.Wrap(err, "create memory profile dir")
					}
					name := benchmarkArgs.location.String() + "_" + path.Base(dir)
					if err := writeProfile(benchmarkArgs.memProfileDir, name); err != nil {
						return errors.Wrap(err, "write memory profile")
					}
				}
			}
		}

		return nil
	},
}

func writeProfile(dirPath, name string) error {
	// e.g. s3_backups20200504111515.mem.pprof
	filePath := strings.ReplaceAll(name, ":", "_") +
		timeutc.Now().Format("20060102150405") +
		".mem.pprof"
	f, err := os.Create(path.Join(dirPath, filePath))
	if err != nil {
		return errors.Wrap(err, "create memory profile")
	}
	defer f.Close()
	// get up-to-date statistics
	runtime.GC()
	if err := pprof.WriteHeapProfile(f); err != nil {
		return errors.Wrap(err, "write memory profile")
	}

	return nil
}

func init() {
	cmd := benchmarkCmd

	f := cmd.Flags()
	f.StringSliceVarP(&benchmarkArgs.dirGlob, "dir", "d", []string{},
		"comma-separated `list of glob patterns` pointing to schema directories generated with create-scenario subcommand")
	f.VarP(&benchmarkArgs.location, "location", "L", "backup location in the format <provider>:<name> e.g. s3:my-bucket, the supported providers are: "+strings.Join(backupspec.Providers(), ", ")) // nolint: lll
	f.BoolVar(&benchmarkArgs.debug, "debug", false, "enable debug logs")
	f.StringSliceVarP(&benchmarkArgs.configFiles, "config-file", "c", []string{"/etc/scylla-manager-agent/scylla-manager-agent.yaml"}, "configuration file `path`")
	f.StringVarP(&benchmarkArgs.memProfileDir, "mem-profile-dir", "m", "", "`path` to a directory where memory profiles will be saved, if not set profiles will not be captured")
	f.StringVar(&benchmarkArgs.prometheus, "prometheus", "", "address to bind prometheus metrics endpoint e.g. 0.0.0.0:5091")
	f.IntVar(&benchmarkArgs.rateLimit, "rate-limit", 100, "rate limit in megabytes (MiB) per second, set to 0 for no limit")

	requireFlags(cmd, "dir", "location")
	rootCmd.AddCommand(cmd)
}

var createFilesArgs = struct {
	defaultScenario bool

	dir    string
	sizeMb int
	count  int
}{}

var defaultScenario = []struct {
	size  int
	count int
}{
	{1, 1000},
	{50, 20},
	{300, 20},
	{2000, 1},
}

var createScenarioCmd = &cobra.Command{
	Use:   "create-scenario",
	Short: "Adds files of specified size to a scenario directory",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		defer func() {
			if err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "FAILED: %v\n", err)
				os.Exit(1)
			}
		}()

		if createFilesArgs.defaultScenario {
			for _, s := range defaultScenario {
				if err := bench.CreateFiles(createFilesArgs.dir, s.size, s.count); err != nil {
					return errors.Wrap(err, "create default scenario")
				}
			}
			return nil
		}

		if createFilesArgs.sizeMb == 0 || createFilesArgs.count == 0 {
			return errors.New("provide size and count parameters")
		}

		return bench.CreateFiles(createFilesArgs.dir, createFilesArgs.sizeMb, createFilesArgs.count)
	},
}

func init() {
	cmd := createScenarioCmd

	f := cmd.Flags()
	f.StringVarP(&createFilesArgs.dir, "dir", "d", "", "`path` to the scenario directory, files will be put in that directory")
	f.BoolVar(&createFilesArgs.defaultScenario, "default", false, "create a default scenario consisting of 1000x1MiB, 20x50MiB, 20x300MiB and 1x2000MiB files")
	f.IntVarP(&createFilesArgs.count, "count", "c", 0, "number of files to create")
	f.IntVarP(&createFilesArgs.sizeMb, "size", "s", 0, "file size in MiB")

	requireFlags(cmd, "dir")
	benchmarkCmd.AddCommand(cmd)
}
