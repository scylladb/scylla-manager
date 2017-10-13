// Copyright (C) 2017 ScyllaDB

package client

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/command"
	"github.com/scylladb/mermaid/command/client/mermaid"
)

// RepairStart starts unit repair.
type RepairStart struct {
	BaseClientCommand
	unit string
}

// Synopsis implements cli.Command.
func (cmd *RepairStart) Synopsis() string {
	return "Starts unit repair"
}

// InitFlags sets the command flags.
func (cmd *RepairStart) InitFlags() {
	f := cmd.NewFlagSet(cmd.Help)
	f.StringVar(&cmd.cluster, "cluster", cmd.cluster, "`UUID` or name of a cluster")
	f.StringVar(&cmd.unit, "unit", "", "`UUID` or name of a repair unit")
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

	id, err := cmd.client().StartRepair(cmd.context, cmd.unit)
	if err != nil {
		cmd.UI.Error(errorStr(err))
		return 1
	}

	cmd.UI.Info(id)

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

// RepairStop stops unit repair.
type RepairStop struct {
	BaseClientCommand
	unit string
}

// Synopsis implements cli.Command.
func (cmd *RepairStop) Synopsis() string {
	return "Stops unit repair"
}

// InitFlags sets the command flags.
func (cmd *RepairStop) InitFlags() {
	f := cmd.NewFlagSet(cmd.Help)
	f.StringVar(&cmd.cluster, "cluster", cmd.cluster, "`UUID` or name of a cluster")
	f.StringVar(&cmd.unit, "unit", "", "`UUID` or name of a repair unit")
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

	id, err := cmd.client().StopRepair(cmd.context, cmd.unit)
	if err != nil {
		cmd.UI.Error(errorStr(err))
		return 1
	}

	cmd.UI.Info(id)

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

// RepairProgress shows repair progress.
type RepairProgress struct {
	BaseClientCommand
	unit string
	task string
}

// Synopsis implements cli.Command.
func (cmd *RepairProgress) Synopsis() string {
	return "Shows repair progress"
}

// InitFlags sets the command flags.
func (cmd *RepairProgress) InitFlags() {
	f := cmd.NewFlagSet(cmd.Help)
	f.StringVar(&cmd.cluster, "cluster", cmd.cluster, "`UUID` or name of a cluster")
	f.StringVar(&cmd.unit, "unit", "", "`UUID` or name of a repair unit")
	f.StringVar(&cmd.task, "task", "", "repair task `UUID`")
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

// RepairUnitCreate creates a new repair unit.
type RepairUnitCreate struct {
	BaseClientCommand
	keyspace string
	tables   string
}

// Synopsis implements cli.Command.
func (cmd *RepairUnitCreate) Synopsis() string {
	return "Creates a new repair unit"
}

// InitFlags sets the command flags.
func (cmd *RepairUnitCreate) InitFlags() {
	f := cmd.NewFlagSet(cmd.Help)
	f.StringVar(&cmd.cluster, "cluster", cmd.cluster, "`UUID` or name of a cluster")
	f.StringVar(&cmd.keyspace, "keyspace", "", "Keyspace `name`")
	f.StringVar(&cmd.tables, "tables", "", "Comma-separated `list` of tables in the keyspace, optional if empty repair the whole keyspace")
}

// Run implements cli.Command.
func (cmd *RepairUnitCreate) Run(args []string) int {
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

	id, err := cmd.client().CreateRepairUnit(cmd.context, &mermaid.RepairUnit{
		Keyspace: cmd.keyspace,
		Tables:   strings.Split(cmd.tables, ","),
	})
	if err != nil {
		cmd.UI.Error(errorStr(err))
		return 1
	}

	cmd.UI.Info(id)

	return 0
}

func (cmd *RepairUnitCreate) validate() error {
	if err := cmd.BaseClientCommand.validate(); err != nil {
		return err
	}
	if cmd.keyspace == "" {
		return errors.New("missing keyspace")
	}

	return nil
}

// RepairUnitUpdate modifies existing repair unit.
type RepairUnitUpdate struct {
	BaseClientCommand
	unit string

	keyspace command.OptionalString
	tables   command.OptionalString
}

// Synopsis implements cli.Command.
func (cmd *RepairUnitUpdate) Synopsis() string {
	return "Modifies existing repair unit"
}

// InitFlags sets the command flags.
func (cmd *RepairUnitUpdate) InitFlags() {
	f := cmd.NewFlagSet(cmd.Help)
	f.StringVar(&cmd.cluster, "cluster", cmd.cluster, "`UUID` or name of a cluster")
	f.StringVar(&cmd.unit, "unit", "", "`UUID` or name of a repair unit")

	f.Var(&cmd.keyspace, "keyspace", "Optional keyspace `name`")
	f.Var(&cmd.tables, "tables", "Optional comma-separated `list` of tables in the keyspace")
}

// Run implements cli.Command.
func (cmd *RepairUnitUpdate) Run(args []string) int {
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

	client := cmd.client()

	u, err := client.GetRepairUnit(cmd.context, cmd.unit)
	if err != nil {
		cmd.UI.Error(errorStr(err))
		return 1
	}

	if cmd.keyspace.Changed {
		u.Keyspace = cmd.keyspace.Value
	}
	if cmd.tables.Changed {
		u.Tables = strings.Split(cmd.tables.Value, ",")
	}

	err = client.UpdateRepairUnit(cmd.context, cmd.unit, u)
	if err != nil {
		cmd.UI.Error(errorStr(err))
		return 1
	}

	return 0
}

func (cmd *RepairUnitUpdate) validate() error {
	if err := cmd.BaseClientCommand.validate(); err != nil {
		return err
	}
	if cmd.unit == "" {
		return errors.New("missing unit")
	}

	ok := cmd.keyspace.Changed || cmd.tables.Changed
	if !ok {
		return errors.New("missing values to update")
	}

	return nil
}

// RepairUnitList shows repair units.
type RepairUnitList struct {
	BaseClientCommand
}

// Synopsis implements cli.Command.
func (cmd *RepairUnitList) Synopsis() string {
	return "Shows repair units"
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
