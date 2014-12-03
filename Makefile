test:
	@echo '### This assumes you have a redis server listening on 6739 already'
	@echo '### Bringing up test cluster'
	(cd extra/cluster/testconfs && make up) 2>/dev/null 1>&2
	sleep 2
	go test ./...
	
	@echo '### Bringing test cluster down'
	(cd extra/cluster/testconfs && make down) 2>/dev/null 1>&2
