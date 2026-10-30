// Copyright (C) 2026 ScyllaDB

package scyllaclient

import (
	"context"
	"encoding/json"
	stdErrors "errors"
	"io"
	"net/http"

	"github.com/pkg/errors"
)

// TableSSTables is a single table entry of Scylla sstable_info response.
type TableSSTables struct {
	Keyspace string        `json:"keyspace"`
	Table    string        `json:"table"`
	SSTables []SSTableInfo `json:"sstables"`
}

// SSTableInfo describes a single SSTable as reported by Scylla sstable_info.
// Only the fields used by SM are decoded.
type SSTableInfo struct {
	Generation         string              `json:"generation"`
	ExtendedProperties []SSTableNamedAttrs `json:"extended_properties"`
}

// SSTableNamedAttrs is a named group of SSTable attributes, e.g. "encryption_info".
type SSTableNamedAttrs struct {
	Group      string             `json:"group"`
	Attributes []SSTableAttribute `json:"attributes"`
}

// SSTableAttribute is a single SSTable attribute.
type SSTableAttribute struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// SSTableInfo returns information about the SSTables of keyspace.table on host,
// including their extended attributes (e.g. encryption options).
// The endpoint isn't part of the generated Scylla client, so it's called directly.
func (c *Client) SSTableInfo(ctx context.Context, host, keyspace, table string) (out []TableSSTables, err error) {
	u := c.newURL(host, "/storage_service/sstable_info")
	q := u.Query()
	q.Set("keyspace", keyspace)
	q.Set("cf", table)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(forceHost(ctx, host), http.MethodGet, u.String(), http.NoBody)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do("StorageServiceSSTableInfo", req)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = stdErrors.Join(err, resp.Body.Close())
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512)) // nolint: errcheck
		return nil, errors.Errorf("sstable_info: unexpected status %d: %s", resp.StatusCode, body)
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, errors.Wrap(err, "decode sstable_info response")
	}
	return out, nil
}
