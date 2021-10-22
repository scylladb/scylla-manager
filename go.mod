module github.com/scylladb/scylla-manager

go 1.16

require (
	github.com/aws/aws-sdk-go v1.35.17
	github.com/cenkalti/backoff/v4 v4.0.0
	github.com/cespare/xxhash/v2 v2.1.1
	github.com/derekparker/trie v0.0.0-20200317170641-1fdf38b7b0e9
	github.com/go-chi/chi/v5 v5.0.0
	github.com/go-chi/render v1.0.0
	github.com/go-openapi/errors v0.19.6
	github.com/go-openapi/jsonreference v0.19.4 // indirect
	github.com/go-openapi/runtime v0.19.20
	github.com/go-openapi/strfmt v0.19.5
	github.com/go-openapi/swag v0.19.9
	github.com/go-openapi/validate v0.19.10
	github.com/gobwas/glob v0.2.3
	github.com/gocql/gocql v0.0.0-20200131111108-92af2e088537
	github.com/golang/mock v1.4.4
	github.com/google/go-cmp v0.5.4
	github.com/goware/prefixer v0.0.0-20160118172347-395022866408
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed
	github.com/hashicorp/go-version v1.2.0
	github.com/kr/pretty v0.2.0 // indirect
	github.com/mitchellh/mapstructure v1.3.2
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.8.0
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.14.0
	github.com/prometheus/procfs v0.2.0
	github.com/rclone/rclone v1.51.0
	github.com/robfig/cron/v3 v3.0.1
	github.com/scylladb/go-log v0.0.6
	github.com/scylladb/go-reflectx v1.0.1
	github.com/scylladb/go-set v1.0.2
	github.com/scylladb/gocqlx/v2 v2.4.0
	github.com/scylladb/termtables v0.0.0-20191203121021-c4c0b6d42ff4
	github.com/spf13/cobra v1.1.1
	github.com/spf13/pflag v1.0.5
	go.uber.org/atomic v1.7.0
	go.uber.org/config v1.4.0
	go.uber.org/goleak v1.0.0
	go.uber.org/multierr v1.6.0
	go.uber.org/zap v1.16.0
	golang.org/x/crypto v0.0.0-20201112155050-0c6587e931a9
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9
	golang.org/x/sys v0.0.0-20201029080932-201ba4db2418
	golang.org/x/tools v0.0.0-20201017001424-6003fad69a88
	gopkg.in/yaml.v2 v2.3.0
)

replace (
	github.com/gocql/gocql => github.com/scylladb/gocql v1.5.1-0.20210906110332-fb22d64efc33
	github.com/rclone/rclone => github.com/scylladb/rclone v1.54.1-0.20210923101756-ca38103352d9
	google.golang.org/api v0.34.0 => github.com/scylladb/google-api-go-client v0.34.0-patched
)
