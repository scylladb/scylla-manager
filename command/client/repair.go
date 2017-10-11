// Copyright (C) 2017 ScyllaDB

package client

import (
	"bytes"
	"fmt"
	"net"
	"sort"
	"strconv"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/restapiclient/client/operations"
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

	id, err := extractIDFromLocation(resp.Location)
	if err != nil {
		cmd.UI.Error(fmt.Sprintf("Cannot parse response: %s", err))
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
		cmd.UI.Error(fmt.Sprintf("Error: %s", err))
		return 1
	}

	// validate command line arguments
	if err := cmd.validate(); err != nil {
		cmd.UI.Error(fmt.Sprintf("Error: %s", err))
		return 1
	}

	resp, err := cmd.client().PutClusterClusterIDRepairUnitUnitIDStopRepair(&operations.PutClusterClusterIDRepairUnitUnitIDStopRepairParams{
		Context:   cmd.context,
		ClusterID: cmd.cluster,
		UnitID:    cmd.unit,
	})
	if err != nil {
		cmd.UI.Error(fmt.Sprintf("Host %s: %s", cmd.apiHost, err))
		return 1
	}

	id, err := extractIDFromLocation(resp.Location)
	if err != nil {
		cmd.UI.Error(fmt.Sprintf("Cannot parse response: %s", err))
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
		cmd.UI.Error(fmt.Sprintf("Error: %s", err))
		return 1
	}

	// validate command line arguments
	if err := cmd.validate(); err != nil {
		cmd.UI.Error(fmt.Sprintf("Error: %s", err))
		return 1
	}

	resp, err := cmd.client().GetClusterClusterIDRepairTaskTaskID(&operations.GetClusterClusterIDRepairTaskTaskIDParams{
		Context:   cmd.context,
		ClusterID: cmd.cluster,
		UnitID:    cmd.unit,
		TaskID:    cmd.task,
	})
	if err != nil {
		cmd.UI.Error(fmt.Sprintf("Host %s: %s", cmd.apiHost, err))
		return 1
	}

	type row struct {
		host     net.IP
		shard    int64
		progress int32
		error    int32
	}
	var rows []row

	for hostName, h := range resp.Payload.Hosts {
		ip := net.ParseIP(hostName)
		if ip == nil {
			cmd.UI.Error(fmt.Sprintf("Host %s: invalid host %s: %s", cmd.apiHost, hostName, err))
			return 1
		}

		if h.Total == 0 {
			rows = append(rows, row{
				host:  ip,
				shard: -1,
			})
			continue
		}

		for shardName, s := range h.Shards {
			shard, err := strconv.ParseInt(shardName, 10, 64)
			if err != nil {
				cmd.UI.Error(fmt.Sprintf("Host %s: invalid shard number %s: %s", cmd.apiHost, shardName, err))
				return 1
			}

			rows = append(rows, row{
				host:     ip,
				shard:    shard,
				progress: s.PercentComplete,
				error:    s.Error,
			})
		}
	}

	sort.Slice(rows, func(i, j int) bool {
		switch bytes.Compare(rows[i].host, rows[j].host) {
		case -1:
			return true
		case 0:
			return rows[i].shard < rows[j].shard
		default:
			return false
		}
	})

	cmd.UI.Info(fmt.Sprintf("Status: %s, progress: %d%%\n", resp.Payload.Status, resp.Payload.PercentComplete))

	t := newTable("host", "shard", "progress", "errors")
	for _, r := range rows {
		if r.shard == -1 {
			t.append(r.host, "-", "-", "-")
		} else {
			t.append(r.host, r.shard, r.progress, r.error)
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
