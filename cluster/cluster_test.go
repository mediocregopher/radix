package cluster

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	. "testing"

	"github.com/stretchr/testify/assert"

	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
)

// These tests assume there is a cluster running on ports 7000 and 7001, with
// the first half of the slots assigned to 7000 and the second half assigned to
// 7001.
//
// It is also assumed that there is an unrelated redis instance on port 6379,
// which will be connected to but not modified in any way
//
// You can use `make start` to automatically set these up.

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

func randStr() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func keyForNode(c *Cluster, addr string) string {
	for {
		k := randStr()
		if addr == keyToAddr(k, c.mapping) {
			return k
		}
	}
}

const (
	addr1 = "127.0.0.1:7000"
	addr2 = "127.0.0.1:7001"
)

func TestReset(t *T) {
	// Simply initializing a cluster proves Reset works to some degree, since
	// NewCluster calls Reset
	cluster := getCluster(t)
	old7000Pool := cluster.pools[addr1]
	old7001Pool := cluster.pools[addr2]

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
	assert.Equal(t, old7000Pool, cluster.pools[addr1])
	assert.Equal(t, old7001Pool, cluster.pools[addr2])
	assert.Nil(t, cluster.Cmd("GET", keyForNode(cluster, addr1)).Err)
	assert.Nil(t, cluster.Cmd("GET", keyForNode(cluster, addr2)).Err)
}

func TestCmd(t *T) {
	cluster := getCluster(t)
	k1 := keyForNode(cluster, addr1)
	k2 := keyForNode(cluster, addr2)
	assert.Nil(t, cluster.Cmd("SET", k1, "bar").Err)
	assert.Nil(t, cluster.Cmd("SET", k2, "foo").Err)

	s, err := cluster.Cmd("GET", k1).Str()
	assert.Nil(t, err)
	assert.Equal(t, "bar", s)

	s, err = cluster.Cmd("GET", k2).Str()
	assert.Nil(t, err)
	assert.Equal(t, "foo", s)
}

func TestCmdMiss(t *T) {
	cluster := getCluster(t)
	key := keyForNode(cluster, addr1)

	// We set key to something, then try to retrieve it with a client pointed at
	// a different node. It should be redirected and returned correctly

	assert.Nil(t, cluster.Cmd("SET", key, "baz").Err)

	client, err := cluster.getConn("", addr2)
	assert.Nil(t, err)

	args := []interface{}{key}
	r := cluster.clientCmd(client, "GET", args, false, nil, false)
	s, err := r.Str()
	assert.Nil(t, err)
	assert.Equal(t, "baz", s)
}

// This one is kind of a toughy. We have to set a certain slot to be migrating,
// and test that it does the right thing. We'll use a key which isn't set so
// that we don't have to actually migrate the key to get an ASK response
func TestCmdAsk(t *T) {
	cluster := getCluster(t)
	key := keyForNode(cluster, addr1)
	slot := CRC16([]byte(key)) % numSlots

	// just in case
	assert.Nil(t, cluster.Cmd("DEL", key).Err)

	src, err := cluster.getConn("", addr1)
	assert.Nil(t, err)
	dst, err := cluster.getConn("", addr2)
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

	// Bail on the migration
	assert.Nil(t, dst.Cmd("CLUSTER", "SETSLOT", slot, "NODE", srcID).Err)
	assert.Nil(t, src.Cmd("CLUSTER", "SETSLOT", slot, "NODE", srcID).Err)
}
