// Copyright (C) 2017 ScyllaDB

package cluster

import (
	"bytes"
	"context"
	"sort"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/internal/kv"
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
	sslCertStore     kv.Store
	sslKeyStore      kv.Store
	clientCache      *scyllaclient.CachedProvider
	logger           log.Logger
	onChangeListener func(ctx context.Context, c Change) error
}

func NewService(session *gocql.Session, sslCertStore, sslKeyStore kv.Store, l log.Logger) (*Service, error) {
	if session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}
	if sslCertStore == nil {
		return nil, errors.New("missing SSL cert store")
	}
	if sslKeyStore == nil {
		return nil, errors.New("missing SSL key store")
	}

	s := &Service{
		session:      session,
		sslCertStore: sslCertStore,
		sslKeyStore:  sslKeyStore,
		logger:       l,
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
	s.logger.Debug(ctx, "Client", "cluster_id", clusterID)
	return s.clientCache.Client(ctx, clusterID)
}

func (s *Service) client(ctx context.Context, clusterID uuid.UUID) (*scyllaclient.Client, error) {
	c, err := s.GetClusterByID(ctx, clusterID)
	if err != nil {
		return nil, err
	}

	client, err := s.createClient(c)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	hosts, err := s.discoverHosts(ctx, client)
	if err != nil {
		return nil, err
	}
	if err := s.setKnownHosts(c, hosts); err != nil {
		return nil, errors.Wrap(err, "failed to update cluster")
	}

	s.logger.Info(ctx, "New Scylla REST client", "cluster_id", clusterID)

	return s.createClient(c)
}

func (s *Service) createClient(c *Cluster) (*scyllaclient.Client, error) {
	config := scyllaclient.DefaultConfig()
	config.Hosts = c.KnownHosts
	return scyllaclient.NewClient(config, s.logger.Named("client"))
}

// discoverHosts returns a list of all hosts sorted by DC speed. This is
// an optimisation for Epsilon-Greedy host pool used internally by
// scyllaclient.Client that makes it use supposedly faster hosts first.
func (s *Service) discoverHosts(ctx context.Context, client *scyllaclient.Client) (hosts []string, err error) {
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

func (s *Service) loadKnownHosts(c *Cluster) error {
	stmt, names := schema.Cluster.Get("known_hosts")
	q := gocqlx.Query(s.session.Query(stmt), names).BindStruct(c)
	return q.GetRelease(c)
}

func (s *Service) setKnownHosts(c *Cluster, hosts []string) error {
	c.KnownHosts = hosts

	stmt, names := schema.Cluster.Update("known_hosts")
	q := gocqlx.Query(s.session.Query(stmt), names).BindStruct(c)
	return q.ExecRelease()
}

// ListClusters returns all the clusters for a given filtering criteria.
func (s *Service) ListClusters(ctx context.Context, f *Filter) ([]*Cluster, error) {
	s.logger.Debug(ctx, "ListClusters", "filter", f)

	// Validate the filter
	if err := f.Validate(); err != nil {
		return nil, err
	}

	stmt, _ := qb.Select(schema.Cluster.Name()).ToCql()

	q := s.session.Query(stmt)
	defer q.Release()

	var clusters []*Cluster
	if err := gocqlx.Select(&clusters, q); err != nil {
		return nil, err
	}

	sort.Slice(clusters, func(i, j int) bool {
		return bytes.Compare(clusters[i].ID.Bytes(), clusters[j].ID.Bytes()) < 0
	})

	// Nothing to filter
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

// GetClusterByID returns cluster based on ID. If nothing was found
// mermaid.ErrNotFound is returned.
func (s *Service) GetClusterByID(ctx context.Context, id uuid.UUID) (*Cluster, error) {
	s.logger.Debug(ctx, "GetClusterByID", "id", id)

	stmt, names := schema.Cluster.Get()

	q := gocqlx.Query(s.session.Query(stmt), names).BindMap(qb.M{
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

// GetClusterByName returns cluster based on name. If nothing was found
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

// GetClusterName returns cluster name for a given ID. If nothing was found
// mermaid.ErrNotFound is returned.
func (s *Service) GetClusterName(ctx context.Context, id uuid.UUID) (string, error) {
	s.logger.Debug(ctx, "GetClusterName", "id", id)

	c, err := s.GetClusterByID(ctx, id)
	if err != nil {
		return "", err
	}

	return c.String(), nil
}

// PutCluster upserts a cluster, cluster instance must pass Validate() checks.
// If u.ID == uuid.Nil a new one is generated.
func (s *Service) PutCluster(ctx context.Context, c *Cluster) (err error) {
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

	// Validate cluster model.
	if err := c.Validate(); err != nil {
		return err
	}

	// Check for conflicting cluster names.
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

	// Check hosts connectivity.
	if err := s.validateHostsConnectivity(ctx, c); err != nil {
		return err
	}

	// Rollback on error.
	var rollback []func()
	defer func() {
		if err != nil {
			for _, r := range rollback {
				r()
			}
		}
	}()

	if len(c.SSLUserCertFile) != 0 {
		r, err := putWithRollback(s.sslCertStore, c.ID, c.SSLUserCertFile)
		rollback = append(rollback, r)
		if err != nil {
			return errors.Wrap(err, "failed to save SSL cert file")
		}
	}
	if len(c.SSLUserKeyFile) != 0 {
		r, err := putWithRollback(s.sslKeyStore, c.ID, c.SSLUserKeyFile)
		rollback = append(rollback, r)
		if err != nil {
			return errors.Wrap(err, "failed to save SSL key file")
		}
	}

	stmt, names := schema.Cluster.Insert()
	q := gocqlx.Query(s.session.Query(stmt), names).BindStruct(c)

	if err := q.ExecRelease(); err != nil {
		return err
	}

	if t == Update {
		s.clientCache.Invalidate(c.ID)
	}

	return s.notifyChangeListener(ctx, Change{ID: c.ID, Type: t})
}

func (s *Service) validateHostsConnectivity(ctx context.Context, c *Cluster) error {
	// If host changes ignore old known hosts.
	if c.Host != "" {
		c.KnownHosts = []string{c.Host}
	} else if err := s.loadKnownHosts(c); err != nil {
		return errors.Wrap(err, "failed to load known hosts")
	}

	client, err := s.createClient(c)
	if err != nil {
		return err
	}
	defer client.Close()

	// To validate connectivity try refreshing host pool, this is the same
	// sanity check we do in client function before returning client to
	// the system.
	hosts, err := s.discoverHosts(ctx, client)
	if err != nil {
		s.logger.Info(ctx, "Host connectivity check failed",
			"cluster_id", c.ID,
			"error", err,
		)
		return mermaid.ErrValidate(errors.New("host connectivity check failed"), "")
	}

	// Update known hosts.
	c.KnownHosts = hosts

	return nil
}

// DeleteCluster removes cluster based on ID.
func (s *Service) DeleteCluster(ctx context.Context, clusterID uuid.UUID) error {
	s.logger.Debug(ctx, "DeleteCluster", "cluster_id", clusterID)

	stmt, names := schema.Cluster.Delete()
	q := gocqlx.Query(s.session.Query(stmt), names).BindMap(qb.M{
		"id": clusterID,
	})

	if err := q.ExecRelease(); err != nil {
		return err
	}

	for _, kv := range []kv.Store{s.sslKeyStore, s.sslCertStore} {
		if err := kv.Put(clusterID, nil); err != nil {
			s.logger.Error(ctx, "Failed to delete file",
				"cluster_id", clusterID,
				"error", err,
			)
		}
	}

	s.clientCache.Invalidate(clusterID)

	return s.notifyChangeListener(ctx, Change{ID: clusterID, Type: Delete})
}

// ListNodes returns information about all the nodes in the cluster.
// Address will be set as node name if it's not resolvable.
func (s *Service) ListNodes(ctx context.Context, clusterID uuid.UUID) ([]Node, error) {
	s.logger.Debug(ctx, "ListNodes", "cluster_id", clusterID)

	var nodes []Node

	client, err := s.client(ctx, clusterID)
	if err != nil {
		return nil, err
	}

	dcs, err := client.Datacenters(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get dcs for cluster with id %s", clusterID)
	}

	for dc, hosts := range dcs {
		for _, h := range hosts {
			sh, err := client.ShardCount(ctx, h)
			if err != nil {
				s.logger.Error(ctx, "Can't get number of shards", "error", err)
			}
			nodes = append(nodes, Node{
				Datacenter: dc,
				Address:    h,
				ShardNum:   sh,
			})
		}
	}

	return nodes, nil
}

func (s *Service) notifyChangeListener(ctx context.Context, c Change) error {
	if s.onChangeListener == nil {
		return nil
	}
	return s.onChangeListener(ctx, c)
}

// Close closes all connections to cluster.
func (s *Service) Close() {
	s.clientCache.Close()
}
