// Copyright (C) 2017 ScyllaDB

package mermaidclient

import (
	"github.com/scylladb/mermaid/uuid"
	"net/url"
	"path"
)

func extractIDFromLocation(location string) (uuid.UUID, error) {
	l, err := url.Parse(location)
	if err != nil {
		return uuid.Nil, err
	}
	_, id := path.Split(l.Path)

	return uuid.Parse(id)
}

func extractTaskIDFromLocation(location string) (uuid.UUID, error) {
	l, err := url.Parse(location)
	if err != nil {
		return uuid.Nil, err
	}
	id := l.Query().Get("task_id")

	return uuid.Parse(id)
}
