// Copyright (C) 2017 ScyllaDB

package cluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/secrets"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
	"github.com/scylladb/scylla-manager/v3/pkg/util/logutil"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/multierr"
)

// ProviderFunc defines the function that will be used by other services to get current cluster data.
type ProviderFunc func(ctx context.Context, id uuid.UUID) (*Cluster, error)

// ChangeType specifies type on Change.
type ChangeType int8

// ErrNoValidKnownHost is thrown when it was not possible to connect to any of the currently known hosts of the cluster.
var ErrNoValidKnownHost = errors.New("unable to connect to any of cluster's known hosts")

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

// Servicer interface defines the responsibilities of the cluster service.
// It's a duplicate of the restapi.ClusterService, but I want to avoid doing bigger refactor
// and removing the interface from restapi package (although nothing prevents us from doing so).
type Servicer interface {
	ListClusters(ctx context.Context, f *Filter) ([]*Cluster, error)
	GetCluster(ctx context.Context, idOrName string) (*Cluster, error)
	PutCluster(ctx context.Context, c *Cluster) error
	DeleteCluster(ctx context.Context, id uuid.UUID) error
	CheckCQLCredentials(id uuid.UUID) (bool, error)
	DeleteCQLCredentials(ctx context.Context, id uuid.UUID) error
	DeleteSSLUserCert(ctx context.Context, id uuid.UUID) error
	ListNodes(ctx context.Context, id uuid.UUID) ([]Node, error)
}

// Service manages cluster configurations.
type Service struct {
	session          gocqlx.Session
	metrics          metrics.ClusterMetrics
	secretsStore     store.Store
	clientCache      *scyllaclient.CachedProvider
	timeoutConfig    scyllaclient.TimeoutConfig
	logger           log.Logger
	onChangeListener func(ctx context.Context, c Change) error
}

func NewService(session gocqlx.Session, metrics metrics.ClusterMetrics, secretsStore store.Store, timeoutConfig scyllaclient.TimeoutConfig,
	cacheInvalidationTimeout time.Duration, l log.Logger,
) (*Service, error) {
	if session.Session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}

	s := &Service{
		session:       session,
		metrics:       metrics,
		secretsStore:  secretsStore,
		logger:        l,
		timeoutConfig: timeoutConfig,
	}
	s.clientCache = scyllaclient.NewCachedProvider(s.CreateClientNoCache, cacheInvalidationTimeout, l)

	return s, nil
}

// Init initializes metrics from database.
func (s *Service) Init(ctx context.Context) error {
	s.logger.Debug(ctx, "Init")

	var clusters []*Cluster
	if err := s.session.Query(table.Cluster.SelectAll()).SelectRelease(&clusters); err != nil {
		return err
	}

	for _, c := range clusters {
		s.metrics.SetName(c.ID, c.Name)
	}

	return nil
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

// CreateClientNoCache creates Scylla API that load balances calls to every node from given cluster.
// There may be a situation that cluster keeps outdated information about list of available hosts.
// To work it around:
//   - function iterates over all currently known hosts
//   - calls consecutive client to get list of available hosts known by Scylla server
//   - updates list of known hosts to Scylla Manager DB
//   - returns client created on top of list of hosts returned by the Scylla server
func (s *Service) CreateClientNoCache(ctx context.Context, clusterID uuid.UUID) (*scyllaclient.Client, error) {
	s.logger.Info(ctx, "Creating new Scylla HTTP client", "cluster_id", clusterID)

	c, err := s.GetClusterByID(ctx, clusterID)
	if err != nil {
		return nil, err
	}

	if err := s.discoverAndSetClusterHosts(ctx, c); err != nil {
		return nil, errors.Wrap(err, "discover and set cluster hosts")
	}

	config := s.clientConfig(c)
	return scyllaclient.NewClient(config, s.logger.Named("client"))
}

func (s *Service) clientConfig(c *Cluster) scyllaclient.Config {
	config := scyllaclient.DefaultConfigWithTimeout(s.timeoutConfig)
	if c.Port != 0 {
		config.Port = fmt.Sprint(c.Port)
	}
	config.AuthToken = c.AuthToken
	config.Hosts = c.KnownHosts
	return config
}

func (s *Service) discoverAndSetClusterHosts(ctx context.Context, c *Cluster) error {
	knownHosts, err := s.discoverClusterHosts(ctx, c)
	if err != nil {
		if errors.Is(err, ErrNoValidKnownHost) {
			s.logger.Error(ctx, "There is no single valid known host for the cluster. "+
				"Please update it with 'sctool cluster update -h <host>'",
				"cluster", c.ID,
				"contact point", c.Host,
				"discovered hosts", c.KnownHosts,
			)
		}
		return err
	}
	return errors.Wrap(s.setKnownHosts(c, knownHosts), "update known_hosts in SM DB")
}

func (s *Service) discoverClusterHosts(ctx context.Context, c *Cluster) ([]string, error) {
	var contactPoints []string
	if c.Host != "" {
		contactPoints = append(contactPoints, c.Host) // Go with the designated contact point first
	} else {
		s.logger.Error(ctx, "Missing --host flag. Using only previously discovered hosts instead", "cluster ID", c.ID)
	}
	contactPoints = append(contactPoints, c.KnownHosts...) // In case it failed, try to contact previously discovered hosts

	for _, cp := range contactPoints {
		if cp == "" {
			s.logger.Error(ctx, "Empty contact point", "cluster ID", c.ID, "contact points", contactPoints)
			continue
		}

		config := scyllaclient.DefaultConfigWithTimeout(s.timeoutConfig)
		if c.Port != 0 {
			config.Port = fmt.Sprint(c.Port)
		}
		config.AuthToken = c.AuthToken
		config.Hosts = []string{cp}

		client, err := scyllaclient.NewClient(config, s.logger.Named("client"))
		if err != nil {
			s.logger.Error(ctx, "Couldn't connect to contact point", "contact point", cp, "error", err)
			continue
		}

		knownHosts, err := s.discoverHosts(ctx, client)
		logutil.LogOnError(ctx, s.logger, client.Close, "Couldn't close scylla client")
		if err != nil {
			s.logger.Error(ctx, "Couldn't discover hosts", "host", cp, "error", err)
			continue
		}
		return knownHosts, nil
	}

	return nil, ErrNoValidKnownHost
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
		return nil, util.ErrNotFound
	case 1:
		return clusters[0], nil
	default:
		return nil, errors.Errorf("multiple clusters share the same name %q", name)
	}
}

// NameFunc returns name for a given ID.
type NameFunc func(ctx context.Context, clusterID uuid.UUID) (string, error)

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
		return util.ErrNilPtr
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
			if !errors.Is(err, util.ErrNotFound) {
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
		if !errors.Is(err, util.ErrNotFound) {
			if err != nil {
				return err
			}
			if conflict.ID != c.ID {
				return util.ErrValidate(errors.Errorf("name %q is already taken", c.Name))
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
			err = fmt.Errorf("%w - %s", err, tip)
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

	// Create the session and log error
	_, err = s.GetSession(ctx, c.ID)
	if err != nil {
		s.logger.Info(ctx, "WARNING! Cannot create CQL session to the cluster. It will affect backup/restore/healthcheck services.",
			"cluster_id", c.ID)
	}

	switch t {
	case Create:
		s.logger.Info(ctx, "Cluster added", "cluster_id", c.ID)
	case Update:
		s.logger.Info(ctx, "Cluster updated", "cluster_id", c.ID)
		s.clientCache.Invalidate(c.ID)
	}

	s.metrics.SetName(c.ID, c.Name)

	changeEvent := Change{
		ID:            c.ID,
		Type:          t,
		WithoutRepair: c.WithoutRepair,
	}
	return s.notifyChangeListener(ctx, changeEvent)
}

func (s *Service) validateHostsConnectivity(ctx context.Context, c *Cluster) error {
	if err := s.loadKnownHosts(c); err != nil && !errors.Is(err, gocql.ErrNotFound) {
		return errors.Wrap(err, "load known hosts")
	}

	knownHosts, err := s.discoverClusterHosts(ctx, c)
	if err != nil {
		return errors.Wrap(err, "discover cluster hosts")
	}
	c.KnownHosts = knownHosts

	config := s.clientConfig(c)
	client, err := scyllaclient.NewClient(config, s.logger.Named("client"))
	if err != nil {
		return err
	}
	defer logutil.LogOnError(ctx, s.logger, client.Close, "Couldn't close scylla client")

	status, err := client.Status(ctx)
	if err != nil {
		return errors.Wrap(err, "cluster status")
	}
	live := status.Live().Hosts()
	if len(live) == 0 {
		return util.ErrValidate(errors.New("no live nodes"))
	}

	var errs error
	for i, err := range client.CheckHostsConnectivity(ctx, live) {
		errs = multierr.Append(errs, errors.Wrap(err, live[i]))
	}
	if errs != nil {
		return util.ErrValidate(errors.Wrap(errs, "connectivity check"))
	}
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

// CheckCQLCredentials checks if associated CQLCreds exist in secrets store.
func (s *Service) CheckCQLCredentials(id uuid.UUID) (bool, error) {
	credentials := secrets.CQLCreds{
		ClusterID: id,
	}
	return s.secretsStore.Check(&credentials)
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

	client, err := s.CreateClientNoCache(ctx, clusterID)
	if err != nil {
		return nil, err
	}
	defer logutil.LogOnError(ctx, s.logger, client.Close, "Couldn't close scylla client")

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

// SessionConfigOption defines function modifying cluster config that can be used when creating session.
type SessionConfigOption func(ctx context.Context, clusterID uuid.UUID, client *scyllaclient.Client, cfg *gocql.ClusterConfig) error

// SingleHostSessionConfigOption ensures that session will be connected only to the single, provided host.
func SingleHostSessionConfigOption(host string) SessionConfigOption {
	return func(ctx context.Context, clusterID uuid.UUID, client *scyllaclient.Client, cfg *gocql.ClusterConfig) error {
		ni, err := client.NodeInfo(ctx, host)
		if err != nil {
			return errors.Wrapf(err, "fetch node (%s) info", host)
		}
		cqlAddr := ni.CQLAddr(host)
		cfg.Hosts = []string{cqlAddr}
		cfg.HostFilter = gocql.WhiteListHostFilter(cqlAddr)
		cfg.DisableInitialHostLookup = true
		return nil
	}
}

// SessionFunc returns CQL session for given cluster ID.
type SessionFunc func(ctx context.Context, clusterID uuid.UUID, opts ...SessionConfigOption) (gocqlx.Session, error)

// GetSession returns CQL session to provided cluster.
func (s *Service) GetSession(ctx context.Context, clusterID uuid.UUID, opts ...SessionConfigOption) (session gocqlx.Session, err error) {
	s.logger.Info(ctx, "Get session", "cluster_id", clusterID)

	client, err := s.CreateClientNoCache(ctx, clusterID)
	if err != nil {
		return session, errors.Wrap(err, "get client")
	}
	defer logutil.LogOnError(ctx, s.logger, client.Close, "Couldn't close scylla client")

	cfg := gocql.NewCluster()
	for _, opt := range opts {
		if err := opt(ctx, clusterID, client, cfg); err != nil {
			return session, err
		}
	}
	// Fill hosts if they weren't specified by the options
	if len(cfg.Hosts) == 0 {
		sessionHosts, err := GetRPCAddresses(ctx, client, client.Config().Hosts)
		if err != nil {
			s.logger.Info(ctx, "Gets session", "err", err)
			if errors.Is(err, ErrNoRPCAddressesFound) {
				return session, err
			}
		}
		cfg.Hosts = sessionHosts
	}

	ni, err := client.AnyNodeInfo(ctx)
	if err != nil {
		return session, errors.Wrap(err, "fetch node info")
	}
	if err := s.extendClusterConfigWithAuthentication(clusterID, ni, cfg); err != nil {
		return session, err
	}
	if err := s.extendClusterConfigWithTLS(ctx, clusterID, ni, cfg); err != nil {
		return session, err
	}

	return gocqlx.WrapSession(cfg.CreateSession())
}

// ErrNoCQLCredentials is returned when cluster CQL credentials are required to create session,
// but they weren't added to the SM.
var ErrNoCQLCredentials = errors.New("cluster requires CQL authentication but username/password was not set. " +
	"Use 'sctool cluster update --username --password' for adding them")

func (s *Service) extendClusterConfigWithAuthentication(clusterID uuid.UUID, ni *scyllaclient.NodeInfo, cfg *gocql.ClusterConfig) error {
	if ni.CqlPasswordProtected {
		credentials := secrets.CQLCreds{
			ClusterID: clusterID,
		}
		err := s.secretsStore.Get(&credentials)
		if errors.Is(err, util.ErrNotFound) {
			return ErrNoCQLCredentials
		}
		if err != nil {
			return errors.Wrap(err, "get credentials")
		}

		cfg.Authenticator = gocql.PasswordAuthenticator{
			Username: credentials.Username,
			Password: credentials.Password,
		}
	}
	return nil
}

func (s *Service) extendClusterConfigWithTLS(ctx context.Context, clusterID uuid.UUID, ni *scyllaclient.NodeInfo, cfg *gocql.ClusterConfig) error {
	cluster, err := s.GetClusterByID(ctx, clusterID)
	if err != nil {
		return errors.Wrap(err, "get cluster by id")
	}

	cqlPort := ni.CQLPort()
	if ni.ClientEncryptionEnabled && !cluster.ForceTLSDisabled {
		if !cluster.ForceNonSSLSessionPort {
			cqlPort = ni.CQLSSLPort()
		}
		cfg.SslOpts = &gocql.SslOptions{
			Config: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
		if ni.ClientEncryptionRequireAuth {
			keyPair, err := s.loadTLSIdentity(clusterID)
			if err != nil {
				return err
			}
			cfg.SslOpts.Config.Certificates = []tls.Certificate{keyPair}
		}
	}

	p, err := strconv.Atoi(cqlPort)
	if err != nil {
		return errors.Wrap(err, "parse cql port")
	}
	cfg.Port = p
	return nil
}

// ErrNoTLSIdentity is returned when cluster TSL/SSL key/cert is required to create session,
// but they weren't added to the SM.
var ErrNoTLSIdentity = errors.New("cluster requires encryption authentication but TSL/SSL key/cert were not set. " +
	"Use 'sctool cluster update --ssl-user-key-file --ssl-user-cert-file' for adding them")

func (s *Service) loadTLSIdentity(clusterID uuid.UUID) (tls.Certificate, error) {
	tlsIdentity := secrets.TLSIdentity{
		ClusterID: clusterID,
	}
	err := s.secretsStore.Get(&tlsIdentity)
	if errors.Is(err, util.ErrNotFound) {
		return tls.Certificate{}, ErrNoTLSIdentity
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

// ErrNoRPCAddressesFound is the error representation of "no RPC addresses found".
var ErrNoRPCAddressesFound = errors.New("no RPC addresses found")

// GetRPCAddresses accepts client and hosts parameters that are used later on to query client.NodeInfo endpoint
// returning RPC addresses for given hosts.
// RPC addresses are the ones that scylla uses to accept CQL connections.
func GetRPCAddresses(ctx context.Context, client *scyllaclient.Client, hosts []string) ([]string, error) {
	var sessionHosts []string
	var combinedError error
	for _, h := range hosts {
		ni, err := client.NodeInfo(ctx, h)
		if err != nil {
			combinedError = multierr.Append(combinedError, err)
			continue
		}
		sessionHosts = append(sessionHosts, ni.CQLAddr(h))
	}

	if len(sessionHosts) == 0 {
		combinedError = multierr.Append(ErrNoRPCAddressesFound, combinedError)
	}

	return sessionHosts, combinedError
}
