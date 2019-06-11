all: LICENSE.3RD_PARTY.scylla-manager-server LICENSE.3RD_PARTY.scylla-manager-client LICENSE.3RD_PARTY.scylla-manager-agent

.PHONY: *

LICENSE.3RD_PARTY.scylla-manager-server:
	./licensegen ../cmd/scylla-manager > $@

LICENSE.3RD_PARTY.scylla-manager-client:
	./licensegen ../cmd/sctool > $@

LICENSE.3RD_PARTY.scylla-manager-agent:
	./licensegen ../cmd/agent > $@
