REDIS_SERVER=$(shell which redis-server)

up:
	$(REDIS_SERVER) node1.conf 2>&1 >node1.log &
	$(REDIS_SERVER) node2.conf 2>&1 >node2.log &
	redis-cli -p 7000 cluster meet 127.0.0.1 7001
	redis-cli -p 7000 cluster addslots $$(seq 0 8191)
	redis-cli -p 7001 cluster addslots $$(seq 8192 16383)

down:
	ps -a -o pid -o cmd | \
		grep redis-server | \
		grep :700[0-9] | \
		awk '{print $$1}' | \
		xargs -n1 kill
	rm -f *-cluster.conf
	rm -f *.log
	rm -f *.rdb
	rm -f *.aof
