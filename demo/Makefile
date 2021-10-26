CURL := curl -H "Content-Type: application/octet-stream" -X POST http://18.194.211.185:5080/api/v1/cluster/prod/exec

1-echo:
	@$(MAKE) .exec FILE=echo.in

2-echo-limit:
	@$(MAKE) .exec FILE=echo.in FILTER='?limit=1'

3-cqlsh:
	@$(MAKE) .exec FILE=cqlsh.in

4-logs:
	@$(MAKE) .exec FILE=logs.in

.exec:
	@echo "------------------------------------------------------------------------------------------------------------------------------------"
	@cat $(FILE)
	@echo
	$(CURL)$(FILTER) --data-binary @$(FILE)

