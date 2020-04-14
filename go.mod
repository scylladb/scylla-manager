module github.com/scylladb/mermaid

go 1.13

require (
	github.com/OneOfOne/xxhash v1.2.5 // indirect
	github.com/aws/aws-sdk-go v1.25.31
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
	github.com/gocql/gocql v0.0.0-20200131111108-92af2e088537
	github.com/golang/mock v1.3.1
	github.com/google/go-cmp v0.4.0
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed
	github.com/hashicorp/go-version v1.2.0
	github.com/mitchellh/mapstructure v1.2.2
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.4.0
	github.com/prometheus/client_model v0.2.0
	github.com/rclone/rclone v1.51.0
	github.com/scylladb/go-log v0.0.4
	github.com/scylladb/go-reflectx v1.0.1
	github.com/scylladb/go-set v1.0.2
	github.com/scylladb/gocqlx v1.5.0
	github.com/scylladb/termtables v0.0.0-20191203121021-c4c0b6d42ff4
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.5
	go.uber.org/atomic v1.5.0
	go.uber.org/config v1.4.0
	go.uber.org/goleak v1.0.0
	go.uber.org/multierr v1.4.0
	go.uber.org/zap v1.14.0
	golang.org/x/crypto v0.0.0-20200109152110-61a87790db17
	golang.org/x/lint v0.0.0-20200302205851-738671d3881b // indirect
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e
	golang.org/x/sys v0.0.0-20200122134326-e047566fdf82
	golang.org/x/tools v0.0.0-20200318150045-ba25ddc85566
	gopkg.in/yaml.v2 v2.2.5
)

replace (
	github.com/gocql/gocql => github.com/scylladb/gocql v1.3.3
	github.com/rclone/rclone => github.com/scylladb/rclone v1.51.0-patched-4
)
