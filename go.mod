module github.com/scylladb/scylla-manager

go 1.15

require (
	github.com/OneOfOne/xxhash v1.2.5 // indirect
	github.com/aws/aws-sdk-go v1.32.11
	github.com/cenkalti/backoff/v4 v4.0.0
	github.com/cespare/xxhash v1.1.0
	github.com/cespare/xxhash/v2 v2.1.1
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/go-chi/chi v4.0.2+incompatible
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
	github.com/google/go-cmp v0.5.2
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed
	github.com/hashicorp/go-version v1.2.0
	github.com/kr/pretty v0.2.0 // indirect
	github.com/mitchellh/mapstructure v1.3.2
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.7.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.10.0
	github.com/rclone/rclone v1.51.0
	github.com/scylladb/go-log v0.0.4
	github.com/scylladb/go-reflectx v1.0.1
	github.com/scylladb/go-set v1.0.2
	github.com/scylladb/gocqlx/v2 v2.2.0
	github.com/scylladb/termtables v0.0.0-20191203121021-c4c0b6d42ff4
	github.com/smartystreets/goconvey v1.6.4 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	go.uber.org/atomic v1.6.0
	go.uber.org/config v1.4.0
	go.uber.org/goleak v1.0.0
	go.uber.org/multierr v1.5.0
	go.uber.org/zap v1.15.0
	golang.org/x/crypto v0.0.0-20200709230013-948cd5f35899
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	golang.org/x/sys v0.0.0-20200930185726-fdedc70b468f
	golang.org/x/tools v0.0.0-20201017001424-6003fad69a88
	gopkg.in/yaml.v2 v2.3.0
)

replace (
	github.com/Azure/azure-storage-blob-go v0.11.0 => github.com/ajankovic/azure-storage-blob-go v0.11.0-patched
	github.com/gocql/gocql => github.com/scylladb/gocql v1.4.3
	github.com/rclone/rclone => github.com/scylladb/rclone v1.53.3-ajankovic-5
	google.golang.org/api v0.30.0 => github.com/scylladb/google-api-go-client v0.30.1-0.20200901094148-b1d4e0b17dda
)
