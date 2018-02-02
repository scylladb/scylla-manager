// Copyright (C) 2017 ScyllaDB

package mermaidclient

import (
	"net/url"
	"path"

	"github.com/scylladb/mermaid/uuid"
)

func uuidFromLocation(location string) (uuid.UUID, error) {
	l, err := url.Parse(location)
	if err != nil {
		return uuid.Nil, err
	}
	_, id := path.Split(l.Path)

	return uuid.Parse(id)
}
