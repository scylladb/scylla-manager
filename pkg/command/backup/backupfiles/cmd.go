// Copyright (C) 2017 ScyllaDB

package backupfiles

import (
	_ "embed"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/pkg/command/flag"
	"github.com/scylladb/scylla-manager/pkg/managerclient"
	"github.com/spf13/cobra"
	"go.uber.org/atomic"
	"gopkg.in/yaml.v2"
)

//go:embed res.yaml
var res []byte

type command struct {
	cobra.Command
	client *managerclient.Client

	cluster     string
	location    []string
	allClusters bool
	keyspace    []string
	snapshotTag string
	delimiter   string
	withVersion bool
}

func NewCommand(client *managerclient.Client) *cobra.Command {
	cmd := &command{
		client: client,
	}
	if err := yaml.Unmarshal(res, &cmd.Command); err != nil {
		panic(err)
	}
	cmd.init()
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		return cmd.run()
	}
	return &cmd.Command
}

func (cmd *command) init() {
	defer flag.MustSetUsages(&cmd.Command, res, "cluster", "snapshot-tag")

	w := flag.Wrap(cmd.Flags())
	w.Cluster(&cmd.cluster)
	w.Location(&cmd.location)
	w.Keyspace(&cmd.keyspace)
	w.Unwrap().BoolVar(&cmd.allClusters, "all-clusters", false, "")
	w.Unwrap().StringVarP(&cmd.snapshotTag, "snapshot-tag", "T", "", "")
	w.Unwrap().StringVarP(&cmd.delimiter, "delimiter", "d", "\t", "")
	w.Unwrap().BoolVar(&cmd.withVersion, "with-version", false, "")
}

func (cmd *command) run() error {
	stillWaiting := atomic.NewBool(true)
	time.AfterFunc(5*time.Second, func() {
		if stillWaiting.Load() {
			fmt.Fprintf(cmd.OutOrStderr(), "NOTICE: this may take a while, we are reading metadata from backup location(s)\n")
		}
	})

	filesInfo, err := cmd.client.ListBackupFiles(cmd.Context(), cmd.cluster, cmd.location, cmd.allClusters, cmd.keyspace, cmd.snapshotTag)
	stillWaiting.Store(false)
	if err != nil {
		return err
	}

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
		_, err = fmt.Fprintln(cmd.OutOrStdout(), filePath, cmd.delimiter, "./")
		if err != nil {
			return err
		}
	}
	for _, fi := range filesInfo {
		for _, t := range fi.Files {
			dir := path.Join(t.Keyspace, t.Table)
			if cmd.withVersion {
				dir += "-" + t.Version
			}
			for _, f := range t.Files {
				filePath := strings.Replace(path.Join(fi.Location, t.Path, f), ":", "://", 1)

				_, err = fmt.Fprintln(cmd.OutOrStdout(), filePath, cmd.delimiter, dir)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}
