// Copyright (C) 2017 ScyllaDB

/*
Package callhome implements service for checking Scylla Manager versions by
sending requests in intervals to the version checking endpoint.

Call home template:

	https://repositories.scylladb.com/scylla/check_version/test?
	sts={status}
	uu={per-machine-uuid}&
	rid={the-registration-uuid}&
	version={scylla-manager-version}&
	rtype={os:centos/ubuntu}&
	system=scylla-manager

status - allowed values are
  - "mi" for manager install
  - "md" for manager daily
  - "mdi" for manager install in docker
  - "mdd" for manager check in docker

per-machine-uuid - uuid of the machine that manager is running on
the-registration-uuid - uuid of the registration unique to registered user
scylla-manager-version - version of the Scylla Manger
rtype - linux distribution id (ubuntu, debian, rehl, centos, fedora, unknown)
*/
package callhome
