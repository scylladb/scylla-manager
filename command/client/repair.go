// Copyright (C) 2017 ScyllaDB

package client

import (
	"fmt"

	"github.com/scylladb/mermaid/restapiclient/client/operations"
)

// RepairUnitList lists repair units.
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
		cmd.UI.Error(fmt.Sprintf("Command line error: %s", err))
		return 1
	}

	// validate command line arguments
	if err := cmd.validate(); err != nil {
		cmd.UI.Error(fmt.Sprintf("Command line error: %s", err))
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
