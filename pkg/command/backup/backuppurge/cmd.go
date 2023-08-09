// Copyright (C) 2017 ScyllaDB

package backuppurge

import (
	_ "embed"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/atomic"
	"gopkg.in/yaml.v2"

	"github.com/scylladb/scylla-manager/v3/pkg/command/flag"
	"github.com/scylladb/scylla-manager/v3/pkg/managerclient"
)

//go:embed res.yaml
var res []byte

type command struct {
	cobra.Command
	client   *managerclient.Client
	dryRun   bool
	cluster  string
	location []string
}

func NewCommand(client *managerclient.Client) *cobra.Command {
	cmd := &command{
		client: client,
	}
	if err := yaml.Unmarshal(res, &cmd.Command); err != nil {
		panic(err)
	}

	defer flag.MustSetUsages(&cmd.Command, res, "cluster")
	cmd.init()
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		return cmd.run()
	}
	return &cmd.Command
}

func (cmd *command) init() {
	w := flag.Wrap(cmd.Flags())
	w.Cluster(&cmd.cluster)
	w.Location(&cmd.location)
	w.DryRun(&cmd.dryRun)
}

func (cmd *command) run() error {
	stillWaiting := atomic.NewBool(true)
	time.AfterFunc(5*time.Second, func() {
		if stillWaiting.Load() {
			fmt.Fprintf(cmd.OutOrStderr(), "NOTICE: this may take a while, we are reading metadata from backup location(s)\n")
		}
	})
	var warnings Warnings
	resp, warnings, err := cmd.client.PurgeBackups(cmd.Context(), cmd.cluster, cmd.location, cmd.dryRun)
	if err != nil {
		return err
	}
	err = resp.Render(cmd.OutOrStdout())
	if err != nil {
		return err
	}
	return warnings.Render(cmd.OutOrStdout())
}

// Warnings represent warnings.
type Warnings []string

// Render render Warnings to io.Writer.
func (w Warnings) Render(writer io.Writer) error {
	if len(w) == 0 {
		_, err := fmt.Fprintln(writer, "no warnings")
		return err
	}
	for idx := range w {
		_, err := fmt.Fprintf(writer, "warning#%d:%s\n", idx, w[idx])
		if err != nil {
			return err
		}
	}
	return nil
}
