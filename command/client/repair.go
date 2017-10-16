// Copyright (C) 2017 ScyllaDB

package client

import (
	"fmt"

	"github.com/pkg/errors"
)

// RepairStart starts a repair of a unit.
type RepairStart struct {
	BaseClientCommand
	unit string
}

// Synopsis implements cli.Command.
func (cmd *RepairStart) Synopsis() string {
	return "Starts a repair of a unit"
}

// InitFlags sets the command flags.
func (cmd *RepairStart) InitFlags() {
	f := cmd.NewFlagSet(cmd.Help)
	f.StringVar(&cmd.cluster, "cluster", cmd.cluster, "ID or name of a cluster")
	f.StringVar(&cmd.unit, "unit", "", "ID or name of a repair unit")
}

// Run implements cli.Command.
func (cmd *RepairStart) Run(args []string) int {
	// parse command line arguments
	if err := cmd.Parse(args); err != nil {
		cmd.UI.Error(errorStr(err))
		return 1
	}

	// validate command line arguments
	if err := cmd.validate(); err != nil {
		cmd.UI.Error(errorStr(err))
		return 1
	}

	u, err := cmd.client().StartRepair(cmd.context, cmd.unit)
	if err != nil {
		cmd.UI.Error(errorStr(err))
		return 1
	}

	cmd.UI.Info(u.String())

	return 0
}

func (cmd *RepairStart) validate() error {
	if err := cmd.BaseClientCommand.validate(); err != nil {
		return err
	}
	if cmd.unit == "" {
		return errors.New("missing unit")
	}

	return nil
}

// RepairStop stops a repair of a unit.
type RepairStop struct {
	BaseClientCommand
	unit string
}

// Synopsis implements cli.Command.
func (cmd *RepairStop) Synopsis() string {
	return "Stops a repair of a unit"
}

// InitFlags sets the command flags.
func (cmd *RepairStop) InitFlags() {
	f := cmd.NewFlagSet(cmd.Help)
	f.StringVar(&cmd.cluster, "cluster", cmd.cluster, "ID or name of a cluster")
	f.StringVar(&cmd.unit, "unit", "", "ID or name of a repair unit")
}

// Run implements cli.Command.
func (cmd *RepairStop) Run(args []string) int {
	// parse command line arguments
	if err := cmd.Parse(args); err != nil {
		cmd.UI.Error(errorStr(err))
		return 1
	}

	// validate command line arguments
	if err := cmd.validate(); err != nil {
		cmd.UI.Error(errorStr(err))
		return 1
	}

	u, err := cmd.client().StopRepair(cmd.context, cmd.unit)
	if err != nil {
		cmd.UI.Error(errorStr(err))
		return 1
	}

	cmd.UI.Info(u.String())

	return 0
}

func (cmd *RepairStop) validate() error {
	if err := cmd.BaseClientCommand.validate(); err != nil {
		return err
	}
	if cmd.unit == "" {
		return errors.New("missing unit")
	}

	return nil
}

// RepairProgress stops a repair of a unit.
type RepairProgress struct {
	BaseClientCommand
	unit string
	task string
}

// Synopsis implements cli.Command.
func (cmd *RepairProgress) Synopsis() string {
	return "Shows progress of a repair"
}

// InitFlags sets the command flags.
func (cmd *RepairProgress) InitFlags() {
	f := cmd.NewFlagSet(cmd.Help)
	f.StringVar(&cmd.cluster, "cluster", cmd.cluster, "ID or name of a cluster")
	f.StringVar(&cmd.unit, "unit", "", "ID or name of a repair unit")
	f.StringVar(&cmd.task, "task", "", "repair task ID")
}

// Run implements cli.Command.
func (cmd *RepairProgress) Run(args []string) int {
	// parse command line arguments
	if err := cmd.Parse(args); err != nil {
		cmd.UI.Error(errorStr(err))
		return 1
	}

	// validate command line arguments
	if err := cmd.validate(); err != nil {
		cmd.UI.Error(errorStr(err))
		return 1
	}

	status, progress, rows, err := cmd.client().RepairProgress(cmd.context, cmd.unit, cmd.task)
	if err != nil {
		cmd.UI.Error(errorStr(err))
		return 1
	}

	cmd.UI.Info(fmt.Sprintf("Status: %s, progress: %d%%\n", status, progress))

	t := newTable("host", "shard", "progress", "errors")
	for _, r := range rows {
		if r.Shard == -1 {
			t.append(r.Host, "-", "-", "-")
		} else {
			t.append(r.Host, r.Shard, r.Progress, r.Error)
		}
	}
	cmd.UI.Info(t.String())

	return 0
}

func (cmd *RepairProgress) validate() error {
	if err := cmd.BaseClientCommand.validate(); err != nil {
		return err
	}
	if cmd.unit == "" {
		return errors.New("missing unit")
	}
	if cmd.task == "" {
		return errors.New("missing task")
	}

	return nil
}

// RepairUnitList shows repair units within a cluster.
type RepairUnitList struct {
	BaseClientCommand
}

// Synopsis implements cli.Command.
func (cmd *RepairUnitList) Synopsis() string {
	return "Shows repair units within a cluster"
}

// Run implements cli.Command.
func (cmd *RepairUnitList) Run(args []string) int {
	// parse command line arguments
	if err := cmd.Parse(args); err != nil {
		cmd.UI.Error(errorStr(err))
		return 1
	}

	// validate command line arguments
	if err := cmd.validate(); err != nil {
		cmd.UI.Error(errorStr(err))
		return 1
	}

	units, err := cmd.client().ListRepairUnits(cmd.context)
	if err != nil {
		cmd.UI.Error(errorStr(err))
		return 1
	}

	t := newTable("unit id", "keyspace", "tables")
	for _, u := range units {
		t.append(u.ID, u.Keyspace, u.Tables)
	}
	cmd.UI.Info(t.String())

	return 0
}
