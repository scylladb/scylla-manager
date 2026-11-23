// Copyright (C) 2026 ScyllaDB

package scyllaclient

import (
	"net/url"
)

// Scylla REST API rejects requests with request line and headers exceeding 32KiB.
// Headroom is left for the path, other query params and headers.
const (
	scyllaMaxRequestSize  = 32 * 1024
	requestHeadroomSize   = 4 * 1024
	maxListQueryParamSize = scyllaMaxRequestSize - requestHeadroomSize
)

func listQueryParamItemSize(item string) int {
	return len(url.QueryEscape(item)) + len(url.QueryEscape(","))
}

// batchListQueryParam splits items into batches fitting maxListQueryParamSize.
func batchListQueryParam(items []string) [][]string {
	var (
		out   [][]string
		batch []string
		size  int
	)
	for _, item := range items {
		itemSize := listQueryParamItemSize(item)
		if len(batch) > 0 && size+itemSize > maxListQueryParamSize {
			out = append(out, batch)
			batch, size = nil, 0
		}
		batch = append(batch, item)
		size += itemSize
	}
	if len(batch) > 0 {
		out = append(out, batch)
	}
	return out
}
