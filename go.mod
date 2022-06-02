module github.com/scylladb/scylla-manager/v3

go 1.16

require (
	github.com/aws/aws-sdk-go v1.35.17
	github.com/cenkalti/backoff/v4 v4.0.0
	github.com/cespare/xxhash/v2 v2.1.1
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
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed
	github.com/hashicorp/go-version v1.2.0
	github.com/hbollon/go-edlib v1.5.0
	github.com/json-iterator/go v1.1.10
	github.com/kr/pretty v0.2.0 // indirect
	github.com/lnquy/cron v1.1.1
	github.com/mitchellh/mapstructure v1.3.2
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.8.0
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.14.0
	github.com/prometheus/procfs v0.2.0
	github.com/rclone/rclone v1.51.0
	github.com/robfig/cron/v3 v3.0.1
	github.com/scylladb/go-log v0.0.7
	github.com/scylladb/go-reflectx v1.0.1
	github.com/scylladb/go-set v1.0.2
	github.com/scylladb/gocqlx/v2 v2.6.1-0.20211220144210-2b885ac61b11
	github.com/scylladb/termtables v0.0.0-20191203121021-c4c0b6d42ff4
	github.com/spf13/cobra v1.1.1
	github.com/spf13/pflag v1.0.5
	go.uber.org/atomic v1.7.0
	go.uber.org/config v1.4.0
	go.uber.org/goleak v1.1.11
	go.uber.org/multierr v1.6.0
	go.uber.org/zap v1.21.0
	golang.org/x/crypto v0.0.0-20201112155050-0c6587e931a9
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20210510120138-977fb7262007
	golang.org/x/tools v0.1.5
	gopkg.in/yaml.v2 v2.3.0
)

replace (
	github.com/gocql/gocql => github.com/scylladb/gocql v1.5.1-0.20210906110332-fb22d64efc33
	github.com/rclone/rclone => github.com/scylladb/rclone v1.54.1-0.20220302094033-0bf74ef6c54b
	google.golang.org/api v0.34.0 => github.com/scylladb/google-api-go-client v0.34.0-patched
)
