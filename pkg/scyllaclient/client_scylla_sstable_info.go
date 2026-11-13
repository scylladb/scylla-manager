// Copyright (C) 2026 ScyllaDB

package scyllaclient

import (
	"context"
	"encoding/json"
	stdErrors "errors"
	"net/http"

	"github.com/pkg/errors"
)

// SSTableEncryptionInfoGroup is the sstable_info extended properties group
// with an SSTable's scylla_encryption_options.
const SSTableEncryptionInfoGroup = "encryption_info"

// TableSSTables is the sstable_info description of one table's SSTables.
type TableSSTables struct {
	Keyspace string        `json:"keyspace"`
	Table    string        `json:"table"`
	SSTables []SSTableInfo `json:"sstables"`
}

// SSTableInfo describes one SSTable. Only the fields Scylla Manager uses are decoded.
type SSTableInfo struct {
	Generation         SSTableGeneration        `json:"generation"`
	ExtendedProperties []SSTableNamedProperties `json:"extended_properties"`
}

// SSTableGeneration is the SSTable ID as sstable_info reports it, formatted
// the same way as in SSTable file names (see sstable.ExtractID): an integer
// for older SSTables or the UUID based, base36 encoded ID for newer ones.
// Scylla versions report it as a JSON string or, for integer generations
// of older versions, as a JSON number. Both are decoded to a string.
type SSTableGeneration string

// UnmarshalJSON implements json.Unmarshaler.
func (g *SSTableGeneration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		*g = SSTableGeneration(s)
		return nil
	}
	var n json.Number
	if err := json.Unmarshal(b, &n); err != nil {
		return errors.Wrapf(err, "decode sstable generation %s", b)
	}
	*g = SSTableGeneration(n.String())
	return nil
}

// SSTableNamedProperties is a named group of SSTable properties.
type SSTableNamedProperties struct {
	Group      string            `json:"group"`
	Attributes []SSTableProperty `json:"attributes"`
}

// SSTableProperty is a single key value SSTable property.
type SSTableProperty struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// PropertyGroup returns the attributes of the given extended properties
// group as a map, or nil when the SSTable has no such group.
func (s SSTableInfo) PropertyGroup(group string) map[string]string {
	for _, g := range s.ExtendedProperties {
		if g.Group != group {
			continue
		}
		out := make(map[string]string, len(g.Attributes))
		for _, a := range g.Attributes {
			out[a.Key] = a.Value
		}
		return out
	}
	return nil
}

// SSTableInfo returns the SSTables of the keyspace on the host, as reported by
// Scylla's GET /storage_service/sstable_info. If table is empty, all tables of
// the keyspace are returned.
func (c *Client) SSTableInfo(ctx context.Context, host, keyspace, table string) (_ []TableSSTables, err error) {
	// Scylla versions without the endpoint answer 404, which won't change on retry.
	ctx = withShouldRetryHandler(ctx, sstableInfoShouldRetryHandler)

	u := c.newURL(host, "/storage_service/sstable_info")
	q := u.Query()
	q.Add("keyspace", keyspace)
	if table != "" {
		q.Add("cf", table)
	}
	u.RawQuery = q.Encode()

	r, err := http.NewRequestWithContext(forceHost(ctx, host), http.MethodGet, u.String(), http.NoBody)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do("StorageServiceSstableInfoGet", r)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = stdErrors.Join(err, resp.Body.Close())
	}()

	var out []TableSSTables
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, errors.Wrap(err, "decode sstable_info")
	}
	return out, nil
}

// sstableInfoShouldRetryHandler doesn't retry Scylla's answer for an unknown
// endpoint. The agent passes Scylla's error body ({"message", "code"}) through,
// which the raw client decodes as an agent error without status, so the
// "Not found" message is matched as well.
func sstableInfoShouldRetryHandler(err error) *bool {
	code, msg := StatusCodeAndMessageOf(err)
	if code == http.StatusNotFound || (code == 0 && msg == "Not found") {
		return new(false)
	}
	return nil
}
