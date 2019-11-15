// Copyright (C) 2017 ScyllaDB

package dbsecrets

import (
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"github.com/scylladb/mermaid/schema"
	"github.com/scylladb/mermaid/service/secrets"
	"github.com/scylladb/mermaid/uuid"
)

// Store manages secrets in a database.
type Store struct {
	session *gocql.Session
}

// Check that Store implements secrets.Store
var _ secrets.Store = &Store{}

// New returns instance of Store.
func New(session *gocql.Session) (*Store, error) {
	if session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}

	return &Store{
		session: session,
	}, nil
}

// Put saves provided secret into database.
func (s *Store) Put(secret secrets.KeyValue) error {
	clusterID, key := secret.Key()
	if clusterID == uuid.Nil || key == "" {
		return secrets.ErrEmptyKeyValue
	}
	value, err := secret.MarshalBinary()
	if err != nil {
		return errors.Wrap(err, "secret marshal")
	}

	stmt, names := schema.Secrets.Insert()
	q := gocqlx.Query(s.session.Query(stmt), names).BindMap(qb.M{
		"cluster_id": clusterID,
		"key":        key,
		"value":      value,
	})

	return q.ExecRelease()
}

// Get retrieves existing secret information about secret.
// ClusterID and Key must be provided.
// Value retrieved from database is marshalled back into `secret`.
func (s *Store) Get(secret secrets.KeyValue) error {
	clusterID, key := secret.Key()
	if clusterID == uuid.Nil || key == "" {
		return secrets.ErrEmptyKeyValue
	}
	stmt, names := schema.Secrets.Get("value")
	q := gocqlx.Query(s.session.Query(stmt), names).BindMap(qb.M{
		"cluster_id": clusterID,
		"key":        key,
	})
	if q.Err() != nil {
		return q.Err()
	}

	var v []byte
	if err := q.GetRelease(&v); err != nil {
		return err
	}

	return secret.UnmarshalBinary(v)
}

// Delete removes existing secret stored under given ClusterID and Key.
func (s *Store) Delete(secret secrets.KeyValue) error {
	clusterID, key := secret.Key()
	if clusterID == uuid.Nil || key == "" {
		return secrets.ErrEmptyKeyValue
	}
	stmt, names := schema.Secrets.Delete()
	q := gocqlx.Query(s.session.Query(stmt), names).BindMap(qb.M{
		"cluster_id": clusterID,
		"key":        key,
	})

	return q.ExecRelease()
}

// DeleteAll removes existing secrets associated with given `clusterID`.
func (s *Store) DeleteAll(clusterID uuid.UUID) error {
	stmt, names := qb.Delete(schema.Secrets.Name()).Where(qb.Eq("cluster_id")).ToCql()
	q := gocqlx.Query(s.session.Query(stmt), names).BindMap(qb.M{
		"cluster_id": clusterID,
	})

	return q.ExecRelease()
}
