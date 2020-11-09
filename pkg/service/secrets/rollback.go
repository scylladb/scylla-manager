// Copyright (C) 2017 ScyllaDB

package secrets

import (
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/service"
)

type keyValueHolder struct {
	KeyValue
	data []byte
}

func (v *keyValueHolder) MarshalBinary() (data []byte, err error) {
	return v.data, nil
}

func (v *keyValueHolder) UnmarshalBinary(data []byte) error {
	v.data = data
	return nil
}

// PutWithRollback gets former value of secret and returns a function to restore
// it if put was successful.
func PutWithRollback(store Store, secret KeyValue) (func(), error) {
	old := &keyValueHolder{
		KeyValue: secret,
	}

	if err := store.Get(old); err != nil && err != service.ErrNotFound {
		return nil, errors.Wrap(err, "read")
	}
	if err := store.Put(secret); err != nil {
		return nil, errors.Wrap(err, "put")
	}
	return func() {
		if old.data == nil {
			store.Delete(old) // nolint:errcheck
		} else {
			store.Put(old) // nolint:errcheck
		}
	}, nil
}
