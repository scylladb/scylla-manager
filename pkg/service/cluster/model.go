// Copyright (C) 2017 ScyllaDB

package cluster

import (
	"crypto/tls"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/v3/pkg/service"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/multierr"
)

// Cluster specifies a cluster properties.
type Cluster struct {
	ID         uuid.UUID         `json:"id"`
	Name       string            `json:"name"`
	Labels     map[string]string `json:"labels"`
	Host       string            `json:"host"`
	KnownHosts []string          `json:"-"`
	Port       int               `json:"port,omitempty"`
	AuthToken  string            `json:"auth_token"`

	ForceTLSDisabled       bool `json:"force_tls_disabled"`
	ForceNonSSLSessionPort bool `json:"force_non_ssl_session_port"`

	Username        string `json:"username,omitempty" db:"-"`
	Password        string `json:"password,omitempty" db:"-"`
	SSLUserCertFile []byte `json:"ssl_user_cert_file,omitempty" db:"-"`
	SSLUserKeyFile  []byte `json:"ssl_user_key_file,omitempty" db:"-"`
	WithoutRepair   bool   `json:"without_repair,omitempty" db:"-"`
}

// String returns cluster Name or ID if Name is empty.
func (c *Cluster) String() string {
	if c == nil {
		return ""
	}
	if c.Name != "" {
		return c.Name
	}
	return c.ID.String()
}

func (c *Cluster) Validate() error {
	if c == nil {
		return errors.Wrap(service.ErrNilPtr, "invalid filter")
	}

	var errs error
	if _, err := uuid.Parse(c.Name); err == nil {
		errs = multierr.Append(errs, errors.New("name cannot be an UUID"))
	}
	if c.Username == "" && c.Password != "" {
		errs = multierr.Append(errs, errors.New("missing user"))
	}
	if c.Username != "" && c.Password == "" {
		errs = multierr.Append(errs, errors.New("missing password"))
	}
	if len(c.SSLUserCertFile) != 0 && len(c.SSLUserKeyFile) == 0 {
		errs = multierr.Append(errs, errors.New("missing SSL user key"))
	}
	if len(c.SSLUserKeyFile) != 0 && len(c.SSLUserCertFile) == 0 {
		errs = multierr.Append(errs, errors.New("missing SSL user cert"))
	}
	if len(c.SSLUserCertFile) != 0 {
		_, err := tls.X509KeyPair(c.SSLUserCertFile, c.SSLUserKeyFile)
		errs = multierr.Append(errs, errors.Wrap(err, "invalid SSL user key pair"))
	}

	return service.ErrValidate(errors.Wrap(errs, "invalid cluster"))
}

// AllHosts returns KnownHosts with Host at the front and without duplicates.
func (c *Cluster) AllHosts() []string {
	hostSet := strset.New(c.KnownHosts...)
	if c.Host == "" {
		return hostSet.List()
	}
	hostSet.Remove(c.Host)
	return append([]string{c.Host}, hostSet.List()...)
}

// Filter filters Clusters.
type Filter struct {
	Name string
}

func (f *Filter) Validate() error {
	if f == nil {
		return service.ErrNilPtr
	}

	var err error
	if _, e := uuid.Parse(f.Name); e == nil {
		err = multierr.Append(err, errors.New("name cannot be an UUID"))
	}

	return err
}

// Node represents single node in a cluster.
type Node struct {
	Datacenter string
	Address    string
	ShardNum   uint
}
