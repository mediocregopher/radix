package radix

import (
	"context"
	"log"
	"regexp"
	"strconv"
	. "testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var redisVersionPat = regexp.MustCompile(`(?m)^redis_version:(\d+)\.(\d+)\.(\d+).*$`)

func requireRedisVersion(tb TB, c Client, major, minor, patch int) {
	ctx := testCtx(tb)
	tb.Helper()

	var info string
	require.NoError(tb, c.Do(ctx, Cmd(&info, "INFO", "server")))

	m := redisVersionPat.FindStringSubmatch(info)
	if m == nil {
		tb.Fatal("failed to get redis server version")
	}

	gotMajor, _ := strconv.Atoi(m[1])
	gotMinor, _ := strconv.Atoi(m[2])
	gotPatch, _ := strconv.Atoi(m[3])

	if gotMajor < major ||
		(gotMajor == major && gotMinor < minor) ||
		(gotMajor == major && gotMinor == minor && gotPatch < patch) {
		tb.Skipf("not supported with current redis version %d.%d.%d, need at least %d.%d.%d",
			gotMajor,
			gotMinor,
			gotPatch,
			major,
			minor,
			patch)
	}
}

func TestScanner(t *T) {
	ctx := testCtx(t)
	c := dial()

	// Make a random dataset
	prefix := randStr()
	fullMap := map[string]bool{}
	for i := 0; i < 100; i++ {
		key := prefix + ":" + strconv.Itoa(i)
		fullMap[key] = true
		require.Nil(t, c.Do(ctx, Cmd(nil, "SET", key, "1")))
	}

	// make sure we get all results when scanning with an existing prefix
	sc := (ScannerConfig{Command: "SCAN", Pattern: prefix + ":*"}).New(c)
	var key string
	for sc.Next(ctx, &key) {
		delete(fullMap, key)
	}
	require.Nil(t, sc.Close())
	assert.Empty(t, fullMap)

	// make sure we don't get any results when scanning with a non-existing
	// prefix
	sc = (ScannerConfig{Command: "SCAN", Pattern: prefix + "DNE:*"}).New(c)
	assert.False(t, sc.Next(ctx, nil))
	require.Nil(t, sc.Close())
}

// Similar to TestScanner, but scans over a set instead of the whole key space
func TestScannerSet(t *T) {
	ctx := testCtx(t)
	c := dial()

	key := randStr()
	fullMap := map[string]bool{}
	for i := 0; i < 100; i++ {
		elem := strconv.Itoa(i)
		fullMap[elem] = true
		require.Nil(t, c.Do(ctx, Cmd(nil, "SADD", key, elem)))
	}

	// make sure we get all results when scanning an existing set
	sc := (ScannerConfig{Command: "SSCAN", Key: key}).New(c)
	var val string
	for sc.Next(ctx, &val) {
		delete(fullMap, val)
	}
	require.Nil(t, sc.Close())
	assert.Empty(t, fullMap)

	// make sure we don't get any results when scanning a non-existent set
	sc = (ScannerConfig{Command: "SSCAN", Key: key + "DNE"}).New(c)
	assert.False(t, sc.Next(ctx, nil))
	require.Nil(t, sc.Close())
}

func TestScannerType(t *T) {
	ctx := testCtx(t)
	c := dial()
	requireRedisVersion(t, c, 6, 0, 0)

	for i := 0; i < 100; i++ {
		require.NoError(t, c.Do(ctx, Cmd(nil, "SET", randStr(), "string")))
		require.NoError(t, c.Do(ctx, Cmd(nil, "LPUSH", randStr(), "list")))
		require.NoError(t, c.Do(ctx, Cmd(nil, "HMSET", randStr(), "hash", "hash")))
		require.NoError(t, c.Do(ctx, Cmd(nil, "SADD", randStr(), "set")))
		require.NoError(t, c.Do(ctx, Cmd(nil, "ZADD", randStr(), "1000", "zset")))
	}

	scanType := func(type_ string) {
		sc := (ScannerConfig{Command: "SCAN", Type: type_}).New(c)

		var key string
		for sc.Next(ctx, &key) {
			var got string
			require.NoError(t, c.Do(ctx, Cmd(&got, "TYPE", key)))
			assert.Equalf(t, type_, got, "key %s has wrong type %q, expected %q", got, type_)
		}
		require.NoError(t, sc.Close())
	}

	scanType("string")
	scanType("list")
	scanType("hash")
	scanType("set")
	scanType("zset")
}

func BenchmarkScanner(b *B) {
	ctx := testCtx(b)
	c := dial()

	const total = 10 * 1000

	// Make a random dataset
	prefix := randStr()
	for i := 0; i < total; i++ {
		key := prefix + ":" + strconv.Itoa(i)
		require.Nil(b, c.Do(ctx, Cmd(nil, "SET", key, "1")))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// make sure we get all results when scanning with an existing prefix
		sc := (ScannerConfig{Command: "SCAN", Pattern: prefix + ":*"}).New(c)
		var key string
		var got int
		for sc.Next(ctx, &key) {
			got++
		}
		if got != total {
			require.Failf(b, "mismatched between inserted and scanned keys", "expected %d keys, got %d", total, got)
		}
	}
}

func ExampleScanner_scan() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := (PoolConfig{}).New(ctx, "tcp", "127.0.0.1:6379")
	if err != nil {
		log.Fatal(err)
	}

	s := (ScannerConfig{}).New(client)
	var key string
	for s.Next(ctx, &key) {
		log.Printf("key: %q", key)
	}
	if err := s.Close(); err != nil {
		log.Fatal(err)
	}
}

func ExampleScanner_hscan() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := (PoolConfig{}).New(ctx, "tcp", "127.0.0.1:6739")
	if err != nil {
		log.Fatal(err)
	}

	s := (ScannerConfig{Command: "HSCAN", Key: "somekey"}).New(client)
	var key string
	for s.Next(ctx, &key) {
		log.Printf("key: %q", key)
	}
	if err := s.Close(); err != nil {
		log.Fatal(err)
	}
}

func ExampleScanner_cluster() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Initialize the cluster in any way you see fit
	cluster, err := (ClusterConfig{}).New(ctx, []string{"127.0.0.1:6379"})
	if err != nil {
		panic(err)
	}

	s := (ScannerConfig{Command: "HSCAN", Key: "somekey"}).NewMulti(cluster)
	var key string
	for s.Next(ctx, &key) {
		log.Printf("key: %q", key)
	}
	if err := s.Close(); err != nil {
		log.Fatal(err)
	}
}
