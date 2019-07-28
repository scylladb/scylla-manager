// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/rclone"
	"github.com/scylladb/mermaid/rclone/rcserver"
	"github.com/spf13/cobra"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v2"
)

var rootArgs = struct {
	configFile string
	version    bool
}{}

var rootCmd = &cobra.Command{
	Use:           "scylla-manager",
	Short:         "Scylla Manager server",
	Args:          cobra.NoArgs,
	SilenceUsage:  true,
	SilenceErrors: true,

	RunE: func(cmd *cobra.Command, args []string) error {
		// Print version and return
		if rootArgs.version {
			fmt.Fprintf(cmd.OutOrStdout(), "%s\n", mermaid.Version())
			return nil
		}

		// Parse config
		b, err := ioutil.ReadFile(rootArgs.configFile)
		if err != nil {
			return errors.Wrapf(err, "failed to read config file %s", rootArgs.configFile)
		}
		var c config
		if err := yaml.Unmarshal(b, &c); err != nil {
			return errors.Wrapf(err, "failed to parse config file %s", rootArgs.configFile)
		}
		rclone.SetDefaultConfig(toRcloneLogLevel(c.Logger.Level))

		cpus, err := pinToCPU(c.CPU)
		if err != nil {
			return errors.Wrap(err, "failed to pin to CPU")
		}
		fs.Infof(nil, "Pinned to CPUs %+v", cpus)

		fs.Infof(nil, "Starting HTTPS address %s", c.HTTPS)

		// Start server
		server := http.Server{
			Addr:    c.HTTPS,
			Handler: newRouter(c, rcserver.New(), http.DefaultClient),
		}
		return server.ListenAndServeTLS(c.TLSCertFile, c.TLSKeyFile)
	},
}

func toRcloneLogLevel(level zapcore.Level) fs.LogLevel {
	switch level {
	case zapcore.DebugLevel:
		return fs.LogLevelDebug
	case zapcore.ErrorLevel:
		return fs.LogLevelError
	}
	return fs.LogLevelInfo
}

func init() {
	f := rootCmd.Flags()
	f.StringVarP(&rootArgs.configFile, "config-file", "c", "/etc/scylla-manager-agent/scylla-manager-agent.yaml", "configuration file `path`")
	f.BoolVar(&rootArgs.version, "version", false, "print product version and exit")
}
