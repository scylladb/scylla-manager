all: LICENSE.3RD_PARTY.scylla-manager-server LICENSE.3RD_PARTY.scylla-manager-client LICENSE.3RD_PARTY.scylla-manager-agent

.PHONY: *

LICENSE.3RD_PARTY.scylla-manager-server:
	./licensegen.sh ../cmd/scylla-manager > $@

LICENSE.3RD_PARTY.scylla-manager-client:
	./licensegen.sh ../cmd/sctool > $@

LICENSE.3RD_PARTY.scylla-manager-agent:
	./licensegen.sh ../cmd/agent > $@
