// Copyright (C) 2017 ScyllaDB

package downloader

import (
	"fmt"
	"os"
	"os/user"
	"syscall"

	"github.com/pkg/errors"
)

func dirOwner(dir string) (*user.User, error) {
	s, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	sys, ok := s.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, errors.Errorf("unexpected OS stat typ %T", s.Sys())
	}

	return user.LookupId(fmt.Sprint(sys.Uid))
}
