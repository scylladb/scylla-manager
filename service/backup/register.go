// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/scyllaclient"
)

// registerProvider is using client to register provider with the provided hosts.
// Config should be provider specific interface.
func registerProvider(ctx context.Context, client *scyllaclient.Client, p Provider, host string, config Config) error {
	switch p {
	case S3:
		params := scyllaclient.S3Params{
			EnvAuth:         true,
			DisableChecksum: true,
			Endpoint:        config.TestS3Endpoint,
		}
		if err := client.RcloneRegisterS3Remote(ctx, host, p.String(), params); err != nil {
			return err
		}
	default:
		return errors.Errorf("unrecognised provider %q", p)
	}
	return nil
}
