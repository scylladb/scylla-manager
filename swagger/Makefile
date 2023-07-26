all: gen/agent gen/scylla-manager gen/scylla/v1 gen/scylla/v2

CLIENT := ../bin/swagger generate client

gen/agent: agent.json
	@rm -Rf $@ && mkdir -p $@
	$(CLIENT) -A agent -T templates/agent -f $< -t $@ > $@/swagger.log
	@go run imports.go -file gen/agent/client/agent_client.go -import "github.com/scylladb/scylla-manager/v3/swagger/gen/agent/client/operations"

gen/scylla-manager: scylla-manager.json
	@rm -Rf $@ && mkdir -p $@
	$(CLIENT) -A scylla-manager -f $< -t $@ > $@/swagger.log
	@go run imports.go -file gen/scylla-manager/client/scylla_manager_client.go -import "github.com/scylladb/scylla-manager/v3/swagger/gen/scylla-manager/client/operations"

gen/scylla/v1: scylla_v1.json
	@rm -Rf $@ && mkdir -p $@
	$(CLIENT) -A scylla_v1 -T templates/scylla -f $< -t $@ >$@/swagger.log
	@go run imports.go -file gen/scylla/v1/client/scylla_v1_client.go -import "github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/client/operations"

gen/scylla/v2: scylla_v2.json
	@rm -Rf $@ && mkdir -p $@
	$(CLIENT) -A scylla_v2 -T templates/scylla -f $< -t $@ > $@/swagger.log
	@go run imports.go -file gen/scylla/v2/client/scylla_v2_client.go -import "github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v2/client/config"

clean:
	rm -Rf gen