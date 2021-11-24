// Copyright (C) 2017 ScyllaDB

package repaircontrol

import (
	_ "embed"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/command/flag"
	"github.com/scylladb/scylla-manager/pkg/managerclient"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

//go:embed res.yaml
var res []byte

type command struct {
	cobra.Command
	client *managerclient.Client

	cluster   string
	intensity *flag.Intensity
	parallel  int
}

func NewCommand(client *managerclient.Client) *cobra.Command {
	cmd := &command{
		client:    client,
		intensity: flag.NewIntensity(1),
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
	defer flag.MustSetUsages(&cmd.Command, res, "cluster")

	w := flag.Wrap(cmd.Flags())
	w.Cluster(&cmd.cluster)
	w.Unwrap().Var(cmd.intensity, "intensity", "")
	w.Unwrap().IntVar(&cmd.parallel, "parallel", 0, "")
}

func (cmd *command) run() error {
	if !cmd.Flag("intensity").Changed && !cmd.Flag("parallel").Changed {
		return errors.New("at least one of intensity or parallel flags needs to be specified")
	}

	if cmd.Flag("intensity").Changed {
		if err := cmd.client.SetRepairIntensity(cmd.Context(), cmd.cluster, cmd.intensity.Value()); err != nil {
			return err
		}
	}
	if cmd.Flag("parallel").Changed {
		if err := cmd.client.SetRepairParallel(cmd.Context(), cmd.cluster, int64(cmd.parallel)); err != nil {
			return err
		}
	}

	return nil
}
