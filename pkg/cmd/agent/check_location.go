// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/scylladb/scylla-manager/pkg/rclone/operations"
	"github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	"github.com/spf13/cobra"
)

var checkLocationArgs = struct {
	configFile []string
	location   string
	debug      bool
}{}

var checkLocationCmd = &cobra.Command{
	Use:   "check-location",
	Short: "Checks if backup location is accessible",
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		defer func() {
			if err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "FAILED: %v\n", err)
				os.Exit(1)
			}
		}()

		_, _, err = setupCommand(checkLocationArgs.configFile, checkLocationArgs.debug)
		if err != nil {
			return err
		}

		location, err := backupspec.StripDC(checkLocationArgs.location)
		if err != nil {
			return err
		}

		f, err := fs.NewFs(context.Background(), location)
		if err != nil {
			if errors.Is(err, fs.ErrorNotFoundInConfigFile) {
				return fmt.Errorf("unknown provider %s", location)
			}
			return errors.Wrap(err, "init fs")
		}

		return operations.CheckPermissions(context.Background(), f)
	},
}

func init() {
	cmd := checkLocationCmd

	f := cmd.Flags()
	f.StringSliceVarP(&checkLocationArgs.configFile, "config-file", "c", []string{"/etc/scylla-manager-agent/scylla-manager-agent.yaml"}, "configuration file `path`")
	f.StringVarP(&checkLocationArgs.location, "location", "L", "",
		"backup location in the format [<dc>:]<provider>:<name> ex. s3:my-bucket. The <dc>: part is optional and is only needed when different datacenters are being used to upload data to different locations. The supported providers are: "+strings.Join(backupspec.Providers(), ", ")) // nolint: lll
	f.BoolVar(&checkLocationArgs.debug, "debug", false, "enable debug logs")

	if err := cmd.MarkFlagRequired("location"); err != nil {
		panic(err)
	}

	rootCmd.AddCommand(cmd)
}
