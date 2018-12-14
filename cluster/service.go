// Copyright (C) 2017 ScyllaDB

package cluster

import (
	"bytes"
	"context"
	"net/http"
	"sort"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/internal/kv"
	"github.com/scylladb/mermaid/internal/ssh"
	"github.com/scylladb/mermaid/schema"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
)

// ChangeType specifies type on Change.
type ChangeType int8

// ChangeType enumeration
const (
	Create ChangeType = iota
	Update
	Delete
)

// Change specifies cluster modification.
type Change struct {
	ID   uuid.UUID
	Type ChangeType
}

// Service manages cluster configurations.
type Service struct {
	session          *gocql.Session
	sshConfig        ssh.Config
	keyStore         kv.Store
	clientCache      *scyllaclient.CachedProvider
	logger           log.Logger
	onChangeListener func(ctx context.Context, c Change) error
}

// NewService creates a new service instance.
func NewService(session *gocql.Session, sshConfig ssh.Config, keyStore kv.Store, l log.Logger) (*Service, error) {
	if session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}
	if keyStore == nil {
		return nil, errors.New("invalid keyStore")
	}

	s := &Service{
		session:   session,
		sshConfig: sshConfig,
		keyStore:  keyStore,
		logger:    l,
	}
	s.clientCache = scyllaclient.NewCachedProvider(s.client)

	return s, nil
}

// SetOnChangeListener sets a function that would be invoked when a cluster
// changes.
func (s *Service) SetOnChangeListener(f func(ctx context.Context, c Change) error) {
	s.onChangeListener = f
}

// Client is cluster client provider.
func (s *Service) Client(ctx context.Context, clusterID uuid.UUID) (*scyllaclient.Client, error) {
	s.logger.Debug(ctx, "Client", "clusterID", clusterID)
	return s.clientCache.Client(ctx, clusterID)
}

func (s *Service) client(ctx context.Context, clusterID uuid.UUID) (*scyllaclient.Client, error) {
	c, err := s.GetClusterByID(ctx, clusterID)
	if err != nil {
		return nil, err
	}

	hosts, err := s.getHosts(ctx, c)
	if err != nil {
		return nil, errors.Wrap(err, "failed to determine closest datacenter")
	}

	if err := s.setKnownHosts(ctx, c, hosts); err != nil {
		return nil, errors.Wrap(err, "failed to update cluster")
	}

	transport, err := s.createTransport(c)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create transport")
	}

	s.logger.Info(ctx, "New cluster client", "clusterID", clusterID)

	return scyllaclient.NewClient(hosts, transport, s.logger.Named("client"))
}

// getHosts returns a list of all hosts sorted by DC speed. This is
// an optimisation for Epsilon-Greedy host pool used internally by
// scyllaclient.Client that makes it use supposedly faster hosts first.
func (s *Service) getHosts(ctx context.Context, c *Cluster) (hosts []string, err error) {
	transport, err := s.createTransport(c)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create transport")
	}
	client, err := scyllaclient.NewClient(append(c.KnownHosts, c.Host), transport, s.logger.Named("client"))
	if err != nil {
		return nil, err
	}
	dcs, err := client.Datacenters(ctx)
	if err != nil {
		return nil, err
	}
	closest, err := client.ClosestDC(ctx, dcs)
	if err != nil {
		return nil, err
	}
	for _, dc := range closest {
		hosts = append(hosts, dcs[dc]...)
	}
	return hosts, nil
}

func (s *Service) createTransport(c *Cluster) (http.RoundTripper, error) {
	if c.SSHUser == "" {
		return http.DefaultTransport, nil
	}

	b, err := s.keyStore.Get(c.ID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read SSH identity file")
	}

	config, err := s.sshConfig.WithIdentityFileAuth(c.SSHUser, b)
	if err != nil {
		return nil, errors.Wrap(err, "invalid SSH configuration")
	}

	return ssh.NewTransport(config), nil
}

func (s *Service) setKnownHosts(ctx context.Context, c *Cluster, hosts []string) error {
	c.KnownHosts = hosts

	stmt, names := schema.Cluster.Update("known_hosts")
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(c)
	return q.ExecRelease()
}

// ListClusters returns all the clusters for a given filtering criteria.
func (s *Service) ListClusters(ctx context.Context, f *Filter) ([]*Cluster, error) {
	s.logger.Debug(ctx, "ListClusters", "filter", f)

	// validate the filter
	if err := f.Validate(); err != nil {
		return nil, err
	}

	stmt, _ := qb.Select(schema.Cluster.Name()).ToCql()

	q := s.session.Query(stmt).WithContext(ctx)
	defer q.Release()

	var clusters []*Cluster
	if err := gocqlx.Select(&clusters, q); err != nil {
		return nil, err
	}

	sort.Slice(clusters, func(i, j int) bool {
		return bytes.Compare(clusters[i].ID.Bytes(), clusters[j].ID.Bytes()) < 0
	})

	// nothing to filter
	if f.Name == "" {
		return clusters, nil
	}

	filtered := clusters[:0]
	for _, u := range clusters {
		if u.Name == f.Name {
			filtered = append(filtered, u)
		}
	}

	return filtered, nil
}

// GetCluster returns cluster based on ID or name. If nothing was found
// mermaid.ErrNotFound is returned.
func (s *Service) GetCluster(ctx context.Context, idOrName string) (*Cluster, error) {
	if id, err := uuid.Parse(idOrName); err == nil {
		return s.GetClusterByID(ctx, id)
	}

	return s.GetClusterByName(ctx, idOrName)
}

// GetClusterByID returns repair cluster based on ID. If nothing was found
// mermaid.ErrNotFound is returned.
func (s *Service) GetClusterByID(ctx context.Context, id uuid.UUID) (*Cluster, error) {
	s.logger.Debug(ctx, "GetClusterByID", "id", id)

	stmt, names := schema.Cluster.Get()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"id": id,
	})
	defer q.Release()

	if q.Err() != nil {
		return nil, q.Err()
	}

	var c Cluster
	if err := gocqlx.Get(&c, q.Query); err != nil {
		return nil, err
	}

	return &c, nil
}

// GetClusterByName returns repair cluster based on name. If nothing was found
// mermaid.ErrNotFound is returned.
func (s *Service) GetClusterByName(ctx context.Context, name string) (*Cluster, error) {
	s.logger.Debug(ctx, "GetClusterByName", "name", name)

	clusters, err := s.ListClusters(ctx, &Filter{Name: name})
	if err != nil {
		return nil, err
	}

	switch len(clusters) {
	case 0:
		return nil, mermaid.ErrNotFound
	case 1:
		return clusters[0], nil
	default:
		return nil, errors.Errorf("multiple clusters share the same name %q", name)
	}
}

// PutCluster upserts a cluster, cluster instance must pass Validate() checks.
// If u.ID == uuid.Nil a new one is generated.
func (s *Service) PutCluster(ctx context.Context, c *Cluster) (ferr error) {
	s.logger.Debug(ctx, "PutCluster", "cluster", c)
	if c == nil {
		return mermaid.ErrNilPtr
	}

	t := Update
	if c.ID == uuid.Nil {
		s.logger.Info(ctx, "Adding new cluster", "cluster_id", c.ID)
		t = Create

		var err error
		if c.ID, err = uuid.NewRandom(); err != nil {
			return errors.Wrap(err, "couldn't generate random UUID for Cluster")
		}
	}

	// validate cluster
	if err := c.Validate(); err != nil {
		return err
	}

	// check for conflicting names
	if c.Name != "" {
		conflict, err := s.GetClusterByName(ctx, c.Name)
		if err != mermaid.ErrNotFound {
			if err != nil {
				return err
			}
			if conflict.ID != c.ID {
				return mermaid.ErrValidate(errors.Errorf("name %q is already taken", c.Name), "invalid name")
			}
		}
	}

	var oldIdentityFile []byte
	if b, err := s.keyStore.Get(c.ID); err == nil {
		oldIdentityFile = b
	}
	// save identity file
	if shouldSaveIdentityFile(c.SSHUser, c.SSHIdentityFile) {
		if err := s.keyStore.Put(c.ID, c.SSHIdentityFile); err != nil {
			return errors.Wrap(err, "failed to save identity file")
		}
		s.clientCache.Invalidate(c.ID)
	}
	defer func() {
		if ferr != nil {
			// Rollback and restore the old identity file
			if shouldSaveIdentityFile(c.SSHUser, c.SSHIdentityFile) {
				if err := s.keyStore.Put(c.ID, oldIdentityFile); err != nil {
					s.logger.Debug(ctx, "post error delete failed", "error", err)
				}
				s.clientCache.Invalidate(c.ID)
			}
		}
	}()

	// validate hosts connectivity
	if hosts, err := s.getHosts(ctx, c); err != nil {
		return mermaid.ErrValidate(err, "host connectivity check failed")
	} else { // nolint: golint
		c.KnownHosts = hosts
	}

	stmt, names := schema.Cluster.Insert()
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(c)

	if err := q.ExecRelease(); err != nil {
		return err
	}

	if t == Update {
		s.clientCache.Invalidate(c.ID)
	}

	if s.onChangeListener == nil {
		return nil
	}
	return s.onChangeListener(ctx, Change{ID: c.ID, Type: t})
}

// DeleteCluster removes cluster based on ID.
func (s *Service) DeleteCluster(ctx context.Context, id uuid.UUID) error {
	s.logger.Debug(ctx, "DeleteCluster", "id", id)

	stmt, names := schema.Cluster.Delete()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"id": id,
	})

	if err := q.ExecRelease(); err != nil {
		return err
	}

	if err := s.keyStore.Put(id, nil); err != nil {
		return err
	}

	s.clientCache.Invalidate(id)

	if s.onChangeListener == nil {
		return nil
	}
	return s.onChangeListener(ctx, Change{ID: id, Type: Delete})
}

func shouldSaveIdentityFile(sshUser string, sshIdentityFile []byte) bool {
	if len(sshIdentityFile) > 0 {
		return true
	}
	return sshUser == ""
}

// Close closes all SSH connections to cluster.
func (s *Service) Close() {
	s.clientCache.Close()
}
