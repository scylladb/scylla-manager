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

	var u uuid.UUID
	if err := u.UnmarshalText([]byte(id)); err != nil {
		return uuid.Nil, err
	}

	return u, nil
}

func extractTaskIDFromLocation(location string) (uuid.UUID, error) {
	l, err := url.Parse(location)
	if err != nil {
		return uuid.Nil, err
	}
	id := l.Query().Get("task_id")

	var u uuid.UUID
	if err := u.UnmarshalText([]byte(id)); err != nil {
		return uuid.Nil, err
	}

	return u, nil
}
