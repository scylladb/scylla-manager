all: LICENSE.3RD_PARTY.scylla-manager-server LICENSE.3RD_PARTY.scylla-manager-client LICENSE.3RD_PARTY.scylla-manager-agent

.PHONY: *

LICENSE.3RD_PARTY.scylla-manager-server:
	./licensegen.sh ../../pkg/cmd/scylla-manager > $@

LICENSE.3RD_PARTY.scylla-manager-client:
	./licensegen.sh ../../pkg/cmd/sctool > $@

LICENSE.3RD_PARTY.scylla-manager-agent:
	./licensegen.sh ../../pkg/cmd/agent > $@
