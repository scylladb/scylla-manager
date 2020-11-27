all: gen/agent gen/scylla-manager gen/scylla/v1 gen/scylla/v2

CLIENT := swagger generate client

gen/agent: agent.json
	@rm -Rf $@ && mkdir -p $@
	$(CLIENT) -A agent -f $< -t $@ &> $@/swagger.log -T templates/agent
	@go run imports.go -file gen/agent/client/agent_client.go -import "github.com/scylladb/scylla-manager/swagger/gen/agent/client/operations"

gen/scylla-manager: scylla-manager.json
	@rm -Rf $@ && mkdir -p $@
	$(CLIENT) -A scylla-manager -f $< -t $@ &> $@/swagger.log
	@go run imports.go -file gen/scylla-manager/client/scylla_manager_client.go -import "github.com/scylladb/scylla-manager/swagger/gen/scylla-manager/client/operations"

gen/scylla/v1: scylla_v1.json
	@rm -Rf $@ && mkdir -p $@
	$(CLIENT) -A scylla_v1 -f $< -t $@ &> $@/swagger.log -T templates/scylla
	@go run imports.go -file gen/scylla/v1/client/scylla_v1_client.go -import "github.com/scylladb/scylla-manager/swagger/gen/scylla/v1/client/operations"

gen/scylla/v2: scylla_v2.json
	@rm -Rf $@ && mkdir -p $@
	$(CLIENT) -A scylla_v2 -f $< -t $@ &> $@/swagger.log -T templates/scylla

clean:
	rm -Rf gen