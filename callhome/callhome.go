// Copyright (C) 2017 ScyllaDB

package callhome

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/hashicorp/go-version"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/internal/osutil"
	"github.com/scylladb/mermaid/uuid"
)

const (
	statusInstall       = "mi"
	statusDaily         = "md"
	statusInstallDocker = "mdi"
	statusDailyDocker   = "mdd"
)

const (
	defaultHostURL = "https://repositories.scylladb.com/scylla/check_version"
)

// Checker is a container for all dependencies needed for making calls to
// check manager version.
// Checker will make HTTP GET request to the HostURL with parameters extracted
// from Env and Version.
// If version returned in the response is higher than the current running
// version Checker will add info level entry to the Logger.
type Checker struct {
	HostURL string
	Version string
	Client  *http.Client
	Env     OSEnv
	Logger  log.Logger
}

// OSEnv contains all methods required by the Checker.
type OSEnv interface {
	MacUUID() uuid.UUID
	RegUUID() uuid.UUID
	LinuxDistro() string
	Docker() bool
}

type osenv struct{}

func (e osenv) MacUUID() uuid.UUID {
	return osutil.MacUUID()
}

func (e osenv) RegUUID() uuid.UUID {
	return osutil.RegUUID()
}

func (e osenv) LinuxDistro() string {
	return string(osutil.LinuxDistro())
}

func (e osenv) Docker() bool {
	return osutil.Docker()
}

// DefaultEnv represents default running environment.
var DefaultEnv osenv

// NewChecker creates new service.
func NewChecker(hostURL, version string, env OSEnv) *Checker {
	if hostURL == "" {
		hostURL = defaultHostURL
	}
	if version == "" {
		version = mermaid.Version()
	}

	return &Checker{
		HostURL: hostURL,
		Version: version,
		Client:  http.DefaultClient,
		Env:     env,
	}
}

type checkResponse struct {
	LatestPatchVersion string `json:"latest_patch_version"`
	Version            string `json:"version"`
}

// Result contains information about the version check.
type Result struct {
	UpdateAvailable bool   // true if new version is available.
	Installed       string // Installed version.
	Available       string // Available version.
}

// CheckForUpdates sends request for comparing current version with installed.
// If install is true it sends install status.
func (s *Checker) CheckForUpdates(ctx context.Context, install bool) (Result, error) {
	res := Result{}
	u, err := url.Parse(s.HostURL)
	if err != nil {
		return res, err
	}
	q := u.Query()
	q.Add("system", "scylla-manager")
	q.Add("version", s.Version)
	q.Add("uu", s.Env.MacUUID().String())
	q.Add("rid", s.Env.RegUUID().String())
	q.Add("rtype", s.Env.LinuxDistro())
	sts := statusDaily
	docker := s.Env.Docker()
	if docker {
		sts = statusDailyDocker
	}
	if install {
		if docker {
			sts = statusInstallDocker
		} else {
			sts = statusInstall
		}
	}
	q.Add("sts", sts)
	u.RawQuery = q.Encode()
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return res, err
	}
	req = req.WithContext(ctx)
	resp, err := s.Client.Do(req)
	if err != nil {
		return res, err
	}

	d, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return res, err
	}
	resp.Body.Close()

	check := checkResponse{}
	if err := json.Unmarshal(d, &check); err != nil {
		return res, err
	}

	available, err := version.NewVersion(check.Version)
	if err != nil {
		return res, err
	}
	installed, err := version.NewVersion(s.Version)
	if err != nil {
		return res, err
	}
	res.Available = available.Original()
	res.Installed = installed.Original()

	if installed.LessThan(available) {
		res.UpdateAvailable = true
	}
	return res, nil
}
