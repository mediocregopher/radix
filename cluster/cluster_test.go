package cluster

import (
	"strings"
	. "testing"

	"github.com/stretchr/testify/assert"

	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
)

// These tests assume there is a cluster running on ports 7000 and 7001, with
// the first half of the slots assigned to 7000 and the second half assigned to
// 7001. Calling `make up` inside of extra/cluster/testconfs will set this up
// for you. This is automatically done if you are just calling `make test` in
// the project root.
//
// It is also assumed that there is an unrelated redis instance on port 6379,
// which will be connected to but not modified in any way

func getCluster(t *T) *Cluster {
	cluster, err := New("127.0.0.1:7000")
	if err != nil {
		t.Fatal(err)
	}
	// Pretend there is no throttle initially, so tests can get at least one
	// more reset call
	cluster.resetThrottle.Stop()
	cluster.resetThrottle = nil
	return cluster
}

func TestReset(t *T) {
	// Simply initializing a cluster proves Reset works to some degree, since
	// NewCluster calls Reset
	cluster := getCluster(t)
	old7000Pool := cluster.pools["127.0.0.1:7000"]
	old7001Pool := cluster.pools["127.0.0.1:7001"]

	// We make a bogus client and add it to the cluster to prove that it gets
	// removed, since it's not needed
	p, err := pool.New("tcp", "127.0.0.1:6379", 10)
	assert.Nil(t, err)
	cluster.pools["127.0.0.1:6379"] = p

	// We use resetInnerUsingPool so that we can specifically specify the pool
	// being used, so we don't accidentally use the 6379 one (which doesn't have
	// CLUSTER commands)
	respCh := make(chan bool)
	cluster.callCh <- func(c *Cluster) {
		err := cluster.resetInnerUsingPool(old7000Pool)
		assert.Nil(t, err)
		respCh <- true
	}
	<-respCh

	// Prove that the bogus client is closed
	_, ok := cluster.pools["127.0.0.1:6379"]
	assert.Equal(t, false, ok)

	// Prove that the remaining two addresses are still in clients, were not
	// reconnected, and still work
	assert.Equal(t, 2, len(cluster.pools))
	assert.Equal(t, old7000Pool, cluster.pools["127.0.0.1:7000"])
	assert.Equal(t, old7001Pool, cluster.pools["127.0.0.1:7001"])
	assert.Nil(t, cluster.Cmd("GET", "foo").Err)
	assert.Nil(t, cluster.Cmd("GET", "bar").Err)
}

func TestCmd(t *T) {
	cluster := getCluster(t)
	assert.Nil(t, cluster.Cmd("SET", "foo", "bar").Err)
	assert.Nil(t, cluster.Cmd("SET", "bar", "foo").Err)

	s, err := cluster.Cmd("GET", "foo").Str()
	assert.Nil(t, err)
	assert.Equal(t, "bar", s)

	s, err = cluster.Cmd("GET", "bar").Str()
	assert.Nil(t, err)
	assert.Equal(t, "foo", s)
}

func TestCmdMiss(t *T) {
	cluster := getCluster(t)
	// foo and bar are on different nodes in our configuration. We set foo to
	// something, then try to retrieve it with a client pointed at a different
	// node. It should be redirected and returned correctly

	assert.Nil(t, cluster.Cmd("SET", "foo", "baz").Err)

	barClient, err := cluster.GetForKey("bar")
	assert.Nil(t, err)

	args := []interface{}{"foo"}
	r := cluster.clientCmd(barClient, "GET", args, false, nil, false)
	s, err := r.Str()
	assert.Nil(t, err)
	assert.Equal(t, "baz", s)
}

// This one is kind of a toughy. We have to set a certain slot (which isn't
// being used anywhere else) to be migrating, and test that it does the right
// thing. We'll use a key which isn't set so that we don't have to actually
// migrate the key to get an ASK response
func TestCmdAsk(t *T) {
	cluster := getCluster(t)
	key := "wat"
	slot := CRC16([]byte(key)) % numSlots

	assert.Nil(t, cluster.Cmd("DEL", key).Err)

	// the key "wat" originally belongs on 7000
	src, err := cluster.getConn("", "127.0.0.1:7000")
	assert.Nil(t, err)
	dst, err := cluster.getConn("", "127.0.0.1:7001")
	assert.Nil(t, err)

	// We need the node ids. Unfortunately, this is the best way to get them
	nodes, err := src.Cmd("CLUSTER", "NODES").Str()
	assert.Nil(t, err)
	lines := strings.Split(nodes, "\n")
	var srcID, dstID string
	for _, line := range lines {
		id := strings.Split(line, " ")[0]
		if id == "" {
			continue
		}
		if strings.Index(line, "myself,") > -1 {
			srcID = id
		} else {
			dstID = id
		}
	}

	// Start the "migration"
	assert.Nil(t, dst.Cmd("CLUSTER", "SETSLOT", slot, "IMPORTING", srcID).Err)
	assert.Nil(t, src.Cmd("CLUSTER", "SETSLOT", slot, "MIGRATING", dstID).Err)

	// Make sure we can still "get" the value
	assert.Equal(t, true, cluster.Cmd("GET", key).IsType(redis.Nil))

	// Bail on the migration TODO this doesn't totally bail for some reason
	assert.Nil(t, dst.Cmd("CLUSTER", "SETSLOT", slot, "NODE", srcID).Err)
	assert.Nil(t, src.Cmd("CLUSTER", "SETSLOT", slot, "NODE", srcID).Err)
}
