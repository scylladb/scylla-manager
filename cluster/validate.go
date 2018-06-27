// Copyright (C) 2017 ScyllaDB

package cluster

import (
	"context"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid"
	"go.uber.org/multierr"
	"gopkg.in/fatih/set.v0"
)

func validateHosts(ctx context.Context, c *Cluster, hostInfo func(ctx context.Context, c *Cluster, host string) (cluster, dc string, err error)) error {
	clusters := set.NewNonTS()
	dcs := set.NewNonTS()

	var err error
	for _, h := range c.Hosts {
		c, dc, e := hostInfo(ctx, c, h)
		if e != nil {
			err = multierr.Append(err, errors.Wrapf(e, "failed to get node %s info", h))
		} else {
			clusters.Add(c)
			dcs.Add(dc)
		}
	}

	if err != nil {
		return mermaid.ErrValidate(err, "invalid hosts")
	}

	if clusters.Size() != 1 {
		err = multierr.Append(err, errors.New("mixed clusters"))
	} else if dcs.Size() != 1 {
		err = multierr.Append(err, errors.New("mixed datacenters"))
	}

	return mermaid.ErrValidate(err, "invalid hosts")
}
