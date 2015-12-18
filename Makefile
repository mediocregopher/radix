TESTTMP = ./test-tmp

# VANILLA REDIS CONF
define VANILLA_CONF
daemonize yes
port 6379
pidfile $(TESTTMP)/redis_vanilla.pid
save ""
appendonly no
endef

# CLUSTER REDIS NODES
define NODE1_CONF
daemonize yes
port 7000
pidfile $(TESTTMP)/redis_cluster_node1.pid
cluster-node-timeout 5000
save ""
appendonly no
cluster-enabled yes
cluster-config-file $(TESTTMP)/redis_cluster_node1.conf
endef

define NODE2_CONF
daemonize yes
port 7001
pidfile $(TESTTMP)/redis_cluster_node2.pid
cluster-node-timeout 5000
save ""
appendonly no
cluster-enabled yes
cluster-config-file $(TESTTMP)/redis_cluster_node2.conf
endef

export VANILLA_CONF
export NODE1_CONF
export NODE2_CONF

start: cleanup
	mkdir -p $(TESTTMP)
	echo "$$VANILLA_CONF" | redis-server -
	echo "$$NODE1_CONF" | redis-server -
	echo "$$NODE2_CONF" | redis-server -
	sleep 1
	redis-cli -p 7000 cluster meet 127.0.0.1 7001
	redis-cli -p 7000 cluster addslots $$(seq 0 8191)
	redis-cli -p 7001 cluster addslots $$(seq 8192 16383)

cleanup:
	rm -rf $(TESTTMP)

stop:
	kill `cat $(TESTTMP)/redis_vanilla.pid` || true
	kill `cat $(TESTTMP)/redis_cluster_node1.pid` || true
	kill `cat $(TESTTMP)/redis_cluster_node2.pid` || true
	make cleanup
