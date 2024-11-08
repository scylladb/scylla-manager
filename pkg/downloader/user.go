// Copyright (C) 2017 ScyllaDB

package downloader

import (
	"os"
	"os/user"
	"strconv"
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

	return user.LookupId(strconv.FormatUint(uint64(sys.Uid), 10))
}
