package main

import (
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/util/inexlist/ksfilter"
)

func tabletKeyspacesAreNotSupported(keyspaceFilter, tabletKeyspaces []string) error {
	filter, err := ksfilter.NewFilter(keyspaceFilter)
	if err != nil {
		return errors.Wrap(err, "new keyspace filter")
	}
	for _, ks := range tabletKeyspaces {
		if filter.Check(ks, "") {
			return errors.Errorf("1-1-restore doesn't support tablet based replication. Keyspace: %s", ks)
		}
	}
	return nil
}

func main() {

}
