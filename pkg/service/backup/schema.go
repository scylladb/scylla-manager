// Copyright (C) 2017 ScyllaDB

package backup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"go.uber.org/multierr"
)

func createSchemaArchive(ctx context.Context, units []Unit, clusterSession gocqlx.Session) (b *bytes.Buffer, err error) {
	b = &bytes.Buffer{}
	gw := gzip.NewWriter(b)
	tw := tar.NewWriter(gw)

	now := timeutc.Now()

	for _, u := range units {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		km, err := clusterSession.KeyspaceMetadata(u.Keyspace)
		if err != nil {
			return nil, errors.Wrapf(err, "describe keyspace %s schema", u.Keyspace)
		}

		cqlSchema, err := km.ToCQL()
		if err != nil {
			return nil, errors.Wrapf(err, "cql keyspace %s metadata", u.Keyspace)
		}

		if err := tw.WriteHeader(&tar.Header{
			Name:    u.Keyspace + ".cql",
			Size:    int64(len(cqlSchema)),
			Mode:    0o600,
			ModTime: now,
		}); err != nil {
			return nil, errors.Wrapf(err, "tar keyspace %s schema", u.Keyspace)
		}

		if _, err := io.Copy(tw, strings.NewReader(cqlSchema)); err != nil {
			return nil, errors.Wrapf(err, "copy %s schema", u.Keyspace)
		}
	}

	if err := multierr.Combine(tw.Close(), gw.Close()); err != nil {
		return nil, errors.Wrap(err, "writer close")
	}

	return b, nil
}
