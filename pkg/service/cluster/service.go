// Copyright (C) 2017 ScyllaDB

package cluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"sort"
	"strconv"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/pkg/schema/table"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/pkg/secrets"
	"github.com/scylladb/scylla-manager/pkg/service"
	"github.com/scylladb/scylla-manager/pkg/store"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
	"go.uber.org/multierr"
)

// ChangeType specifies type on Change.
type ChangeType int8

// ChangeType enumeration.
const (
	Create ChangeType = iota
	Update
	Delete
)

// Change specifies cluster modification.
type Change struct {
	ID            uuid.UUID
	Type          ChangeType
	WithoutRepair bool
}

// Service manages cluster configurations.
type Service struct {
	session          gocqlx.Session
	secretsStore     store.Store
	clientCache      *scyllaclient.CachedProvider
	logger           log.Logger
	onChangeListener func(ctx context.Context, c Change) error
}

func NewService(session gocqlx.Session, secretsStore store.Store, l log.Logger) (*Service, error) {
	if session.Session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}

	s := &Service{
		session:      session,
		secretsStore: secretsStore,
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
	s.logger.Info(ctx, "Creating new Scylla REST client", "cluster_id", clusterID)

	c, err := s.GetClusterByID(ctx, clusterID)
	if err != nil {
		return nil, err
	}

	client, err := s.createClient(c)
	if err != nil {
		return nil, errors.Wrap(err, "create client")
	}
	defer client.Close()

	hosts, err := s.discoverHosts(ctx, client)
	if err != nil {
		return nil, errors.Wrap(err, "discover cluster topology")
	}
	if err := s.setKnownHosts(c, hosts); err != nil {
		return nil, errors.Wrap(err, "update cluster")
	}

	return s.createClient(c)
}

func (s *Service) createClient(c *Cluster) (*scyllaclient.Client, error) {
	config := scyllaclient.DefaultConfig()
	config.Hosts = c.KnownHosts
	config.AuthToken = c.AuthToken

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
	q := table.Cluster.GetQuery(s.session, "known_hosts").BindStruct(c)
	return q.GetRelease(c)
}

func (s *Service) setKnownHosts(c *Cluster, hosts []string) error {
	c.KnownHosts = hosts

	q := table.Cluster.UpdateQuery(s.session, "known_hosts").BindStruct(c)
	return q.ExecRelease()
}

// ListClusters returns all the clusters for a given filtering criteria.
func (s *Service) ListClusters(ctx context.Context, f *Filter) ([]*Cluster, error) {
	s.logger.Debug(ctx, "ListClusters", "filter", f)

	// Validate the filter
	if err := f.Validate(); err != nil {
		return nil, err
	}

	q := qb.Select(table.Cluster.Name()).Query(s.session)
	defer q.Release()

	var clusters []*Cluster
	if err := q.Select(&clusters); err != nil {
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
// scylla-manager.ErrNotFound is returned.
func (s *Service) GetCluster(ctx context.Context, idOrName string) (*Cluster, error) {
	if id, err := uuid.Parse(idOrName); err == nil {
		return s.GetClusterByID(ctx, id)
	}

	return s.GetClusterByName(ctx, idOrName)
}

// GetClusterByID returns cluster based on ID. If nothing was found
// scylla-manager.ErrNotFound is returned.
func (s *Service) GetClusterByID(ctx context.Context, id uuid.UUID) (*Cluster, error) {
	s.logger.Debug(ctx, "GetClusterByID", "id", id)

	q := table.Cluster.GetQuery(s.session).BindMap(qb.M{
		"id": id,
	})
	defer q.Release()

	if q.Err() != nil {
		return nil, q.Err()
	}

	var c Cluster
	if err := q.Get(&c); err != nil {
		return nil, err
	}

	return &c, nil
}

// GetClusterByName returns cluster based on name. If nothing was found
// scylla-manager.ErrNotFound is returned.
func (s *Service) GetClusterByName(ctx context.Context, name string) (*Cluster, error) {
	s.logger.Debug(ctx, "GetClusterByName", "name", name)

	clusters, err := s.ListClusters(ctx, &Filter{Name: name})
	if err != nil {
		return nil, err
	}

	switch len(clusters) {
	case 0:
		return nil, service.ErrNotFound
	case 1:
		return clusters[0], nil
	default:
		return nil, errors.Errorf("multiple clusters share the same name %q", name)
	}
}

// GetClusterName returns cluster name for a given ID. If nothing was found
// scylla-manager.ErrNotFound is returned.
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
		return service.ErrNilPtr
	}

	t := Update
	if c.ID == uuid.Nil {
		t = Create

		var err error
		if c.ID, err = uuid.NewRandom(); err != nil {
			return errors.Wrap(err, "couldn't generate random UUID for Cluster")
		}
	} else {
		// User may set ID on his own
		_, err := s.GetClusterByID(ctx, c.ID)
		if err != nil {
			if !errors.Is(err, service.ErrNotFound) {
				return err
			}
			t = Create
		}
	}

	if t == Create {
		s.logger.Info(ctx, "Adding new cluster", "cluster_id", c.ID)
	} else {
		s.logger.Info(ctx, "Updating cluster", "cluster_id", c.ID)
	}

	// Validate cluster model.
	if err := c.Validate(); err != nil {
		return err
	}

	// Check for conflicting cluster names.
	if c.Name != "" {
		conflict, err := s.GetClusterByName(ctx, c.Name)
		if !errors.Is(err, service.ErrNotFound) {
			if err != nil {
				return err
			}
			if conflict.ID != c.ID {
				return service.ErrValidate(errors.Errorf("name %q is already taken", c.Name))
			}
		}
	}

	// Check hosts connectivity.
	if err := s.validateHostsConnectivity(ctx, c); err != nil {
		var tip string
		switch scyllaclient.StatusCodeOf(err) {
		case 0:
			tip = "make sure the IP is correct and access to port 10001 is unblocked"
		case 401:
			tip = "make sure auth_token config option on nodes is set correctly"
		}
		if tip != "" {
			err = errors.Errorf("%s - %s", err, tip)
		}

		return err
	}

	// Rollback on error.
	var rollback []func()
	defer func() {
		if err != nil {
			for _, r := range rollback {
				if r != nil {
					r()
				}
			}
		}
	}()

	if len(c.SSLUserCertFile) != 0 && len(c.SSLUserKeyFile) != 0 {
		r, err := store.PutWithRollback(s.secretsStore, &secrets.TLSIdentity{
			ClusterID:  c.ID,
			Cert:       c.SSLUserCertFile,
			PrivateKey: c.SSLUserKeyFile,
		})
		if err != nil {
			return errors.Wrap(err, "save SSL cert file")
		}
		rollback = append(rollback, r)
	}

	if c.Username != "" {
		r, err := store.PutWithRollback(s.secretsStore, &secrets.CQLCreds{
			ClusterID: c.ID,
			Username:  c.Username,
			Password:  c.Password,
		})
		if err != nil {
			return errors.Wrap(err, "save SSL cert file")
		}
		rollback = append(rollback, r)
	}

	q := table.Cluster.InsertQuery(s.session).BindStruct(c)

	if err := q.ExecRelease(); err != nil {
		return err
	}

	if c.AuthToken == "" {
		s.logger.Info(ctx, "WARNING! Scylla data is exposed on hosts, "+
			"protect it by specifying auth_token in Scylla Manager Agent config file on Scylla nodes",
			"cluster_id", c.ID,
			"hosts", c.KnownHosts,
		)
	}

	switch t {
	case Create:
		s.logger.Info(ctx, "Cluster added", "cluster_id", c.ID)
	case Update:
		s.logger.Info(ctx, "Cluster updated", "cluster_id", c.ID)
		s.clientCache.Invalidate(c.ID)
	}

	changeEvent := Change{
		ID:            c.ID,
		Type:          t,
		WithoutRepair: c.WithoutRepair,
	}
	return s.notifyChangeListener(ctx, changeEvent)
}

func (s *Service) validateHostsConnectivity(ctx context.Context, c *Cluster) error {
	// If host changes ignore old known hosts.
	if c.Host != "" {
		c.KnownHosts = []string{c.Host}
	} else if err := s.loadKnownHosts(c); err != nil {
		return errors.Wrap(err, "load known hosts")
	}

	client, err := s.createClient(c)
	if err != nil {
		return errors.Wrap(err, "create client")
	}
	defer client.Close()

	status, err := client.Status(ctx)
	if err != nil {
		return errors.Wrap(err, "status")
	}

	// Get live hosts
	live := status.LiveHosts()
	if len(live) == 0 {
		return service.ErrValidate(errors.New("no live nodes"))
	}

	// For every live host check there are no errors
	var errs error
	for i, err := range client.CheckHostsConnectivity(ctx, live) {
		errs = multierr.Append(errs, errors.Wrap(err, live[i]))
	}
	if errs != nil {
		return service.ErrValidate(errors.Wrap(errs, "connectivity check"))
	}

	// Update known hosts.
	c.KnownHosts = live

	return nil
}

// DeleteCluster removes cluster and it's secrets.
func (s *Service) DeleteCluster(ctx context.Context, clusterID uuid.UUID) error {
	s.logger.Debug(ctx, "DeleteCluster", "cluster_id", clusterID)

	q := table.Cluster.DeleteQuery(s.session).BindMap(qb.M{
		"id": clusterID,
	})

	if err := q.ExecRelease(); err != nil {
		return err
	}

	if err := s.secretsStore.DeleteAll(clusterID); err != nil {
		s.logger.Error(ctx, "Failed to delete cluster secrets",
			"cluster_id", clusterID,
			"error", err,
		)
		return errors.Wrap(err, "delete cluster secrets")
	}

	s.clientCache.Invalidate(clusterID)

	return s.notifyChangeListener(ctx, Change{ID: clusterID, Type: Delete})
}

// DeleteCQLCredentials removes the associated CQLCreds from secrets store.
func (s *Service) DeleteCQLCredentials(_ context.Context, clusterID uuid.UUID) error {
	return s.secretsStore.Delete(&secrets.CQLCreds{
		ClusterID: clusterID,
	})
}

// DeleteSSLUserCert removes the associated TLSIdentity from secrets store.
func (s *Service) DeleteSSLUserCert(_ context.Context, clusterID uuid.UUID) error {
	return s.secretsStore.Delete(&secrets.TLSIdentity{
		ClusterID: clusterID,
	})
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
		return nil, errors.Wrapf(err, "get hosts for cluster with id %s", clusterID)
	}

	for dc, hosts := range dcs {
		for _, h := range hosts {
			sh, err := client.ShardCount(ctx, h)
			if err != nil {
				s.logger.Error(ctx, "Failed to get number of shards", "error", err)
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

// GetSession returns CQL session to provided cluster.
func (s *Service) GetSession(ctx context.Context, clusterID uuid.UUID) (session gocqlx.Session, err error) {
	s.logger.Debug(ctx, "GetSession", "cluster_id", clusterID)

	client, err := s.client(ctx, clusterID)
	if err != nil {
		return session, errors.Wrap(err, "get client")
	}

	ni, err := client.AnyNodeInfo(ctx)
	if err != nil {
		return session, errors.Wrap(err, "fetch node info")
	}

	scyllaCluster := gocql.NewCluster(client.Config().Hosts...)

	// Set port if needed
	if cqlPort := ni.CQLPort(client.Config().Hosts[0]); cqlPort != "9042" {
		p, err := strconv.Atoi(cqlPort)
		if err != nil {
			return session, errors.Wrap(err, "parse cql port")
		}
		scyllaCluster.Port = p
	}

	if ni.CqlPasswordProtected {
		credentials := secrets.CQLCreds{
			ClusterID: clusterID,
		}
		err := s.secretsStore.Get(&credentials)
		if errors.Is(err, service.ErrNotFound) {
			return session, errors.New("cluster requires CQL authentication but username/password was not set")
		}
		if err != nil {
			return session, errors.Wrap(err, "get credentials")
		}

		scyllaCluster.Authenticator = gocql.PasswordAuthenticator{
			Username: credentials.Username,
			Password: credentials.Password,
		}
	}

	if ni.ClientEncryptionEnabled {
		scyllaCluster.SslOpts = &gocql.SslOptions{
			Config: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
		if ni.ClientEncryptionRequireAuth {
			keyPair, err := s.loadTLSIdentity(clusterID)
			if err != nil {
				return session, err
			}
			scyllaCluster.SslOpts.Config.Certificates = []tls.Certificate{keyPair}
		}
	}

	return gocqlx.WrapSession(scyllaCluster.CreateSession())
}

func (s *Service) loadTLSIdentity(clusterID uuid.UUID) (tls.Certificate, error) {
	tlsIdentity := secrets.TLSIdentity{
		ClusterID: clusterID,
	}
	err := s.secretsStore.Get(&tlsIdentity)
	if errors.Is(err, service.ErrNotFound) {
		return tls.Certificate{}, errors.Wrap(err, "TLS/SSL key/cert is not registered")
	}
	if err != nil {
		return tls.Certificate{}, errors.Wrap(err, "get TLS/SSL identity")
	}

	keyPair, err := tls.X509KeyPair(tlsIdentity.Cert, tlsIdentity.PrivateKey)
	if err != nil {
		return tls.Certificate{}, errors.Wrap(err, "invalid TLS/SSL user key pair")
	}
	return keyPair, nil
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
