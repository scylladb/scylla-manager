// Copyright (C) 2017 ScyllaDB

package client

import (
	"fmt"
	"net/url"
	"path"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/restapiclient/client/operations"
)

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
		cmd.UI.Error(fmt.Sprintf("Error: %s", err))
		return 1
	}

	// validate command line arguments
	if err := cmd.validate(); err != nil {
		cmd.UI.Error(fmt.Sprintf("Error: %s", err))
		return 1
	}

	resp, err := cmd.client().GetClusterClusterIDRepairUnits(&operations.GetClusterClusterIDRepairUnitsParams{
		Context:   cmd.context,
		ClusterID: cmd.cluster,
	})
	if err != nil {
		cmd.UI.Error(fmt.Sprintf("Host %s: %s", cmd.apiHost, err))
		return 1
	}

	t := newTable("unit id", "keyspace", "tables")
	for _, p := range resp.Payload {
		t.append(p.ID, p.Keyspace, p.Tables)
	}
	cmd.UI.Info(t.String())

	return 0
}

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
	f.StringVar(&cmd.cluster, "cluster", cmd.cluster, "ID or name of a cluster.")
	f.StringVar(&cmd.unit, "unit", "", "ID or name of a repair unit.")
}

// Run implements cli.Command.
func (cmd *RepairStart) Run(args []string) int {
	// parse command line arguments
	if err := cmd.Parse(args); err != nil {
		cmd.UI.Error(fmt.Sprintf("Error: %s", err))
		return 1
	}

	// validate command line arguments
	if err := cmd.validate(); err != nil {
		cmd.UI.Error(fmt.Sprintf("Error: %s", err))
		return 1
	}

	resp, err := cmd.client().PutClusterClusterIDRepairUnitUnitIDRepair(&operations.PutClusterClusterIDRepairUnitUnitIDRepairParams{
		Context:   cmd.context,
		ClusterID: cmd.cluster,
		UnitID:    cmd.unit,
	})
	if err != nil {
		cmd.UI.Error(fmt.Sprintf("Host %s: %s", cmd.apiHost, err))
		return 1
	}

	// extract ID from the location header
	location, err := url.Parse(resp.Location)
	if err != nil {
		cmd.UI.Error(fmt.Sprintf("Cannot parse response: %s", err))
		return 1
	}
	_, id := path.Split(location.Path)
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
	f.StringVar(&cmd.cluster, "cluster", cmd.cluster, "ID or name of a cluster.")
	f.StringVar(&cmd.unit, "unit", "", "ID or name of a repair unit.")
}

// Run implements cli.Command.
func (cmd *RepairStop) Run(args []string) int {
	// parse command line arguments
	if err := cmd.Parse(args); err != nil {
		cmd.UI.Error(fmt.Sprintf("Error: %s", err))
		return 1
	}

	// validate command line arguments
	if err := cmd.validate(); err != nil {
		cmd.UI.Error(fmt.Sprintf("Error: %s", err))
		return 1
	}

	_, err := cmd.client().PutClusterClusterIDRepairUnitUnitIDStopRepair(&operations.PutClusterClusterIDRepairUnitUnitIDStopRepairParams{
		Context:   cmd.context,
		ClusterID: cmd.cluster,
		UnitID:    cmd.unit,
	})
	if err != nil {
		cmd.UI.Error(fmt.Sprintf("Host %s: %s", cmd.apiHost, err))
		return 1
	}

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
