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
	"runtime/debug"
	"runtime/pprof"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/scylladb/scylla-manager/pkg/rclone/bench"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/spf13/cobra"
)

var benchmarkArgs = struct {
	configFile    []string
	scenarioGlob  []string
	location      string
	memProfileDir string
	debug         bool
}{}

var benchmarkCmd = &cobra.Command{
	Use:   "benchmark",
	Short: "Runs directory copy on provided scenario paths against provided locations",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		ctx := context.Background()
		defer func() {
			if err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "FAILED: %v\n", err)
				os.Exit(1)
			}
		}()

		c, logger, err := setupAgentCommand(benchmarkArgs.configFile, benchmarkArgs.debug)
		if err != nil {
			return err
		}

		if c.Prometheus != "" {
			go func() {
				prometheusServer := &http.Server{
					Addr:    c.Prometheus,
					Handler: promhttp.Handler(),
				}
				logger.Info(ctx, "Starting Prometheus server", "address", prometheusServer.Addr)
				if err := errors.Wrap(prometheusServer.ListenAndServe(), "prometheus server start"); err != nil {
					logger.Error(ctx, "Error in Prometheus server", "error", err)
				}
			}()
		}

		w := cmd.OutOrStderr()
		b, err := bench.NewBenchmark(ctx, benchmarkArgs.location)
		if err != nil {
			return err
		}
		fmt.Fprintln(w, "Benchmarking location: "+benchmarkArgs.location)
		for _, scenarioGlob := range benchmarkArgs.scenarioGlob {
			matches, err := filepath.Glob(scenarioGlob)
			if err != nil {
				return errors.Wrap(err, "listing scenarios")
			}
			for _, match := range matches {
				scenarioPath, err := filepath.Abs(match)
				if err != nil {
					return errors.Wrap(err, "loading path "+match)
				}
				s, err := b.CopyDir(ctx, scenarioPath)
				if err != nil {
					return errors.Wrap(err, "benchmark "+benchmarkArgs.location)
				}
				fmt.Fprint(w, s.String())
				if benchmarkArgs.memProfileDir != "" {
					err := os.MkdirAll(benchmarkArgs.memProfileDir, 0755)
					if err != nil {
						return errors.Wrap(err, "create memory profile dir")
					}
					name := benchmarkArgs.location + "_" + path.Base(scenarioPath)
					if err := writeProfile(benchmarkArgs.memProfileDir, name); err != nil {
						logger.Error(ctx, "Writing memory profile", "error", err)
					}
				}
			}
		}
		// Release memory after running benchmark to check how much of the
		// memory is reclaimed.
		stats := bench.StartScenario("FreeOSMemory")
		debug.FreeOSMemory()
		stats.EndScenario()
		fmt.Fprintln(w, stats.String())

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
	f.StringSliceVarP(&benchmarkArgs.configFile, "config-file", "c", []string{"/etc/scylla-manager-agent/scylla-manager-agent.yaml"}, "configuration file `path`")
	f.StringSliceVarP(&benchmarkArgs.scenarioGlob, "scenario", "d", []string{},
		"local glob path to scenario directories that will be copied to the provided location and benchmarked as single scenario")
	f.StringVarP(&benchmarkArgs.location, "location", "L", "",
		"backup location in the format [<dc>:]<provider>:<bucket> ex. s3:my-bucket. The <dc>: part is optional and is only needed when different datacenters are being used to upload data to different location. The supported providers are: s3, gcs, azure") //nolint: lll
	f.StringVarP(&benchmarkArgs.memProfileDir, "mem-profile-dir", "m", "", "specify directory path for saving memory profiles. If not provided profiles will not be generated")
	f.BoolVar(&benchmarkArgs.debug, "debug", false, "enable debug logs")

	if err := cmd.MarkFlagRequired("location"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("scenario"); err != nil {
		panic(err)
	}

	rootCmd.AddCommand(cmd)
}

var createFilesArgs = struct {
	dir           string
	sizeMb        int
	count         int
	createDefault bool
}{}

var defaultScenarios = []struct {
	size  int
	count int
}{
	{50, 1},
	{300, 1},
	{2000, 1},
	{50, 20},
	{300, 20},
	{1, 1000},
}

var createScenarioCmd = &cobra.Command{
	Use:   "create-scenario",
	Short: "Create scenario files of specified size and count",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		defer func() {
			if err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "FAILED: %v\n", err)
				os.Exit(1)
			}
		}()

		if createFilesArgs.createDefault {
			for _, s := range defaultScenarios {
				if err := bench.CreateFiles(createFilesArgs.dir, s.size, s.count); err != nil {
					return errors.Wrap(err, "create default scenario files")
				}
			}
			return nil
		}

		if createFilesArgs.sizeMb == 0 || createFilesArgs.count == 0 {
			return errors.New("notting to create")
		}

		return bench.CreateFiles(createFilesArgs.dir, createFilesArgs.sizeMb, createFilesArgs.count)
	},
}

func init() {
	cmd := createScenarioCmd

	f := cmd.Flags()
	f.StringVarP(&createFilesArgs.dir, "dir", "d", "", "path to the directory that will be used for generating temporary files")
	f.IntVarP(&createFilesArgs.sizeMb, "size", "s", 0, "size of each file in MiB")
	f.IntVarP(&createFilesArgs.count, "count", "c", 0, "number of files to create")
	f.BoolVar(&createFilesArgs.createDefault, "default", false, "create default scenarios")

	if err := cmd.MarkFlagRequired("dir"); err != nil {
		panic(err)
	}

	benchmarkCmd.AddCommand(cmd)
}
