package client

import (
	"net/url"
	"path"
)

func extractIDFromLocation(location string) (string, error) {
	l, err := url.Parse(location)
	if err != nil {
		return "", err
	}
	_, id := path.Split(l.Path)
	return id, nil
}
