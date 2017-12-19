// Copyright (C) 2017 ScyllaDB

package cluster

import (
	"context"

	"github.com/fatih/set"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

func validateHosts(ctx context.Context, hosts []string, hostInfo func(ctx context.Context, host string) (cluster, dc string, err error)) (err error) {
	clusters := set.NewNonTS()
	dcs := set.NewNonTS()

	for _, h := range hosts {
		c, dc, e := hostInfo(ctx, h)
		if e != nil {
			err = multierr.Append(err, errors.Wrap(e, h))
		} else {
			clusters.Add(c)
			dcs.Add(dc)
		}
	}

	if err != nil {
		return
	}

	if clusters.Size() != 1 {
		err = errors.New("mixed clusters")
	} else if dcs.Size() != 1 {
		err = errors.New("mixed datacenters")
	}

	return
}
