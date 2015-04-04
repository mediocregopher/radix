package util

import (
	"strconv"
	. "testing"

	"github.com/mediocregopher/radix.v2/cluster"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScan(t *T) {
	client, err := redis.Dial("tcp", "127.0.0.1:6379")
	require.Nil(t, err)

	prefix := "scanTestPrefix"

	fullMap := map[string]bool{}
	for i := 0; i < 100; i++ {
		key := prefix + ":" + strconv.Itoa(i)
		fullMap[key] = true
		require.Nil(t, client.Cmd("SET", key, "1").Err)
	}

	// make sure we get all results when scanning with an existing prefix
	ch := make(chan string)
	go func() {
		err = Scan(client, ch, "SCAN", "", prefix+":*")
	}()
	testMap := map[string]bool{}
	for key := range ch {
		testMap[key] = true
	}
	require.Nil(t, err)
	assert.Equal(t, fullMap, testMap)

	// make sure we don't get any results when scanning with a non-existing
	// prefix
	ch = make(chan string)
	go func() {
		err = Scan(client, ch, "SCAN", "", prefix+"DNE:*")
	}()
	testMap = map[string]bool{}
	for key := range ch {
		testMap[key] = true
	}
	require.Nil(t, err)
	assert.Equal(t, map[string]bool{}, testMap)
}

// Similar to TestScan, but scans over a set instead of the whole key space
func TestSScan(t *T) {
	client, err := redis.Dial("tcp", "127.0.0.1:6379")
	require.Nil(t, err)

	key := "scanTestSet"

	fullMap := map[string]bool{}
	for i := 0; i < 100; i++ {
		elem := strconv.Itoa(i)
		fullMap[elem] = true
		require.Nil(t, client.Cmd("SADD", key, elem).Err)
	}

	// make sure we get all results when scanning with an existing prefix
	ch := make(chan string)
	go func() {
		err = Scan(client, ch, "SSCAN", key, "*")
	}()
	testMap := map[string]bool{}
	for elem := range ch {
		testMap[elem] = true
	}
	require.Nil(t, err)
	assert.Equal(t, fullMap, testMap)

	// make sure we don't get any results when scanning with a non-existing
	// prefix
	ch = make(chan string)
	go func() {
		err = Scan(client, ch, "SSCAN", key+"DNE", "*")
	}()
	testMap = map[string]bool{}
	for elem := range ch {
		testMap[elem] = true
	}
	require.Nil(t, err)
	assert.Equal(t, map[string]bool{}, testMap)
}

// Similar to TestScan, but scans over a whole cluster
func TestClusterScan(t *T) {
	cluster, err := cluster.New("127.0.0.1:7000")
	require.Nil(t, err)

	prefix := "scanTestPrefix"

	fullMap := map[string]bool{}
	for i := 0; i < 100; i++ {
		key := prefix + ":" + strconv.Itoa(i)
		fullMap[key] = true
		require.Nil(t, cluster.Cmd("SET", key, "1").Err)
	}

	// make sure we get all results when scanning with an existing prefix
	ch := make(chan string)
	go func() {
		err = Scan(cluster, ch, "SCAN", "", prefix+":*")
	}()
	testMap := map[string]bool{}
	for key := range ch {
		testMap[key] = true
	}
	require.Nil(t, err)
	assert.Equal(t, fullMap, testMap)

	// make sure we don't get any results when scanning with a non-existing
	// prefix
	ch = make(chan string)
	go func() {
		err = Scan(cluster, ch, "SCAN", "", prefix+"DNE:*")
	}()
	testMap = map[string]bool{}
	for key := range ch {
		testMap[key] = true
	}
	require.Nil(t, err)
	assert.Equal(t, map[string]bool{}, testMap)
}
