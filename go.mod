module github.com/scylladb/mermaid

go 1.12

require (
	github.com/OneOfOne/xxhash v1.2.5 // indirect
	github.com/apcera/termtables v0.0.0-20170405184538-bcbc5dc54055
	github.com/cespare/xxhash v1.0.0
	github.com/go-chi/chi v3.3.2+incompatible
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
	github.com/golang/mock v1.2.0
	github.com/google/go-cmp v0.2.0
	github.com/google/gops v0.3.3
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed
	github.com/hashicorp/go-version v1.1.0
	github.com/ncw/rclone v1.47.0
	github.com/onsi/ginkgo v1.8.0 // indirect
	github.com/onsi/gomega v1.5.0 // indirect
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v0.8.0
	github.com/prometheus/client_model v0.0.0-20180712105110-5c3871d89910
	github.com/scylladb/go-log v0.0.0-20190321090841-00b3a3e11fea
	github.com/scylladb/go-set v1.0.1
	github.com/scylladb/gocqlx v1.3.1
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/cobra v0.0.4-0.20190109003409-7547e83b2d85
	github.com/spf13/pflag v1.0.3
	go.uber.org/atomic v1.3.2
	go.uber.org/multierr v1.1.0
	go.uber.org/zap v1.9.1
	golang.org/x/crypto v0.0.0-20190617133340-57b3e21c3d56
	golang.org/x/sys v0.0.0-20190616124812-15dcb6c0061f
	gopkg.in/yaml.v2 v2.2.2
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.2.0
