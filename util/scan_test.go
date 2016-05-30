package util

import (
	"strconv"
	. "testing"

	"github.com/levenlabs/golib/testutil"
	"github.com/mediocregopher/radix.v2/cluster"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func randPrefix(t *T, c Cmder, count int) (string, map[string]bool) {
	prefix := testutil.RandStr()
	fullMap := map[string]bool{}
	for i := 0; i < count; i++ {
		key := prefix + ":" + strconv.Itoa(i)
		fullMap[key] = true
		require.Nil(t, c.Cmd("SET", key, "1").Err)
	}
	return prefix, fullMap
}

func randSet(t *T, c Cmder, count int) (string, map[string]bool) {
	key := testutil.RandStr()
	fullMap := map[string]bool{}
	for i := 0; i < count; i++ {
		elem := strconv.Itoa(i)
		fullMap[elem] = true
		require.Nil(t, c.Cmd("SADD", key, elem).Err)
	}
	return key, fullMap
}

/*
// We're disabling these tests. They will not pass the race-condition checker

func TestScan(t *T) {
	client, err := redis.Dial("tcp", "127.0.0.1:6379")
	require.Nil(t, err)
	prefix, fullMap := randPrefix(t, client, 100)

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
	key, fullMap := randSet(t, client, 100)

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
	prefix, fullMap := randPrefix(t, cluster, 100)

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
*/

////////////////////////////////////////////////////////////////////////////////

func TestScanner(t *T) {
	client, err := redis.Dial("tcp", "127.0.0.1:6379")
	require.Nil(t, err)
	prefix, fullMap := randPrefix(t, client, 100)

	// make sure we get all results when scanning with an existing prefix
	testMap := map[string]bool{}
	sc := NewScanner(client, ScanOpts{Command: "SCAN", Pattern: prefix + ":*"})
	for sc.HasNext() {
		testMap[sc.Next()] = true
	}
	require.Nil(t, sc.Err())
	assert.Equal(t, fullMap, testMap)

	// make sure we don't get any results when scanning with a non-existing
	// prefix
	testMap = map[string]bool{}
	sc = NewScanner(client, ScanOpts{Command: "SCAN", Pattern: prefix + "DNE:*"})
	for sc.HasNext() {
		testMap[sc.Next()] = true
	}
	require.Nil(t, sc.Err())
	assert.Empty(t, testMap)
}

// Similar to TestScanner, but scans over a set instead of the whole key space
func TestScannerSet(t *T) {
	client, err := redis.Dial("tcp", "127.0.0.1:6379")
	require.Nil(t, err)
	key, fullMap := randSet(t, client, 100)

	// make sure we get all results when scanning an existing set
	testMap := map[string]bool{}
	sc := NewScanner(client, ScanOpts{Command: "SSCAN", Key: key})
	for sc.HasNext() {
		testMap[sc.Next()] = true
	}
	require.Nil(t, sc.Err())
	assert.Equal(t, fullMap, testMap)

	// make sure we don't get any results when scanning a non-existent set
	testMap = map[string]bool{}
	sc = NewScanner(client, ScanOpts{Command: "SSCAN", Key: key + "DNE"})
	for sc.HasNext() {
		testMap[sc.Next()] = true
	}
	require.Nil(t, sc.Err())
	assert.Empty(t, testMap)
}

// Similar to TestScanner, but scans over a whole cluster
func TestScannerCluster(t *T) {
	cluster, err := cluster.New("127.0.0.1:7000")
	require.Nil(t, err)
	prefix, fullMap := randPrefix(t, cluster, 100)

	// make sure we get all results when scanning with an existing prefix
	testMap := map[string]bool{}
	sc := NewScanner(cluster, ScanOpts{Command: "SCAN", Pattern: prefix + ":*"})
	for sc.HasNext() {
		testMap[sc.Next()] = true
	}
	require.Nil(t, sc.Err())
	assert.Equal(t, fullMap, testMap)

	// make sure we don't get any results when scanning with a non-existing
	// prefix
	testMap = map[string]bool{}
	sc = NewScanner(cluster, ScanOpts{Command: "SCAN", Pattern: prefix + "DNE:*"})
	for sc.HasNext() {
		testMap[sc.Next()] = true
	}
	require.Nil(t, sc.Err())
	assert.Empty(t, testMap)
}
