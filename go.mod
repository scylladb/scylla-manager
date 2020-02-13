module github.com/scylladb/mermaid

go 1.13

require (
	github.com/OneOfOne/xxhash v1.2.5 // indirect
	github.com/aws/aws-sdk-go v1.23.8
	github.com/cenkalti/backoff/v4 v4.0.0
	github.com/cespare/xxhash v1.0.0
	github.com/go-chi/chi v4.0.2+incompatible
	github.com/go-chi/render v1.0.0
	github.com/go-openapi/analysis v0.19.2
	github.com/go-openapi/errors v0.19.2
	github.com/go-openapi/jsonpointer v0.19.2
	github.com/go-openapi/jsonreference v0.19.2
	github.com/go-openapi/loads v0.19.2
	github.com/go-openapi/runtime v0.19.2
	github.com/go-openapi/spec v0.19.2
	github.com/go-openapi/strfmt v0.19.0
	github.com/go-openapi/swag v0.19.2
	github.com/go-openapi/validate v0.19.2
	github.com/gobwas/glob v0.2.3
	github.com/gocql/gocql v0.0.0-20190423091413-b99afaf3b163
	github.com/golang/mock v1.3.1
	github.com/google/go-cmp v0.3.1
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed
	github.com/hashicorp/go-version v1.1.0
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v1.0.0
	github.com/prometheus/client_model v0.0.0-20190129233127-fd36f4220a90
	github.com/prometheus/common v0.6.0 // indirect
	github.com/prometheus/procfs v0.0.3 // indirect
	github.com/rclone/rclone v1.50.2
	github.com/scylladb/go-log v0.0.0-20190808115121-2ceb34174b18
	github.com/scylladb/go-reflectx v1.0.1
	github.com/scylladb/go-set v1.0.1
	github.com/scylladb/gocqlx v1.3.2
	github.com/scylladb/termtables v0.0.0-20191203121021-c4c0b6d42ff4
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.3
	go.uber.org/atomic v1.3.2
	go.uber.org/multierr v1.1.0
	go.uber.org/zap v1.9.1
	golang.org/x/crypto v0.0.0-20190820162420-60c769a6c586
	golang.org/x/sys v0.0.0-20190826163724-acd9dae8e8cc
	golang.org/x/tools v0.0.0-20191010201905-e5ffc44a6fee
	gopkg.in/yaml.v2 v2.2.4
)

replace (
	github.com/gocql/gocql => github.com/scylladb/gocql v1.3.2
	github.com/rclone/rclone => github.com/scylladb/rclone v1.50.2-patched-19
)
