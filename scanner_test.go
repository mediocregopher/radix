package radix

import (
	"log"
	"regexp"
	"strconv"
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var redisVersionPat = regexp.MustCompile(`(?m)^redis_version:(\d+)\.(\d+)\.(\d+).*$`)

func requireRedisVersion(tb TB, c Client, major, minor, patch int) {
	tb.Helper()

	var info string
	require.NoError(tb, c.Do(Cmd(&info, "INFO", "server")))

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
	c := dial()

	// Make a random dataset
	prefix := randStr()
	fullMap := map[string]bool{}
	for i := 0; i < 100; i++ {
		key := prefix + ":" + strconv.Itoa(i)
		fullMap[key] = true
		require.Nil(t, c.Do(Cmd(nil, "SET", key, "1")))
	}

	// make sure we get all results when scanning with an existing prefix
	sc := NewScanner(c, ScanOpts{Command: "SCAN", Pattern: prefix + ":*"})
	var key string
	for sc.Next(&key) {
		delete(fullMap, key)
	}
	require.Nil(t, sc.Close())
	assert.Empty(t, fullMap)

	// make sure we don't get any results when scanning with a non-existing
	// prefix
	sc = NewScanner(c, ScanOpts{Command: "SCAN", Pattern: prefix + "DNE:*"})
	assert.False(t, sc.Next(nil))
	require.Nil(t, sc.Close())
}

// Similar to TestScanner, but scans over a set instead of the whole key space
func TestScannerSet(t *T) {
	c := dial()

	key := randStr()
	fullMap := map[string]bool{}
	for i := 0; i < 100; i++ {
		elem := strconv.Itoa(i)
		fullMap[elem] = true
		require.Nil(t, c.Do(Cmd(nil, "SADD", key, elem)))
	}

	// make sure we get all results when scanning an existing set
	sc := NewScanner(c, ScanOpts{Command: "SSCAN", Key: key})
	var val string
	for sc.Next(&val) {
		delete(fullMap, val)
	}
	require.Nil(t, sc.Close())
	assert.Empty(t, fullMap)

	// make sure we don't get any results when scanning a non-existent set
	sc = NewScanner(c, ScanOpts{Command: "SSCAN", Key: key + "DNE"})
	assert.False(t, sc.Next(nil))
	require.Nil(t, sc.Close())
}

func TestScannerType(t *T) {
	c := dial()
	requireRedisVersion(t, c, 6, 0, 0)

	for i := 0; i < 100; i++ {
		require.NoError(t, c.Do(Cmd(nil, "SET", randStr(), "string")))
		require.NoError(t, c.Do(Cmd(nil, "LPUSH", randStr(), "list")))
		require.NoError(t, c.Do(Cmd(nil, "HMSET", randStr(), "hash", "hash")))
		require.NoError(t, c.Do(Cmd(nil, "SADD", randStr(), "set")))
		require.NoError(t, c.Do(Cmd(nil, "ZADD", randStr(), "1000", "zset")))
	}

	scanType := func(type_ string) {
		sc := NewScanner(c, ScanOpts{Command: "SCAN", Type: type_})

		var key string
		for sc.Next(&key) {
			var got string
			require.NoError(t, c.Do(Cmd(&got, "TYPE", key)))
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
	c := dial()

	const total = 10 * 1000

	// Make a random dataset
	prefix := randStr()
	for i := 0; i < total; i++ {
		key := prefix + ":" + strconv.Itoa(i)
		require.Nil(b, c.Do(Cmd(nil, "SET", key, "1")))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// make sure we get all results when scanning with an existing prefix
		sc := NewScanner(c, ScanOpts{Command: "SCAN", Pattern: prefix + ":*"})
		var key string
		var got int
		for sc.Next(&key) {
			got++
		}
		if got != total {
			require.Failf(b, "mismatched between inserted and scanned keys", "expected %d keys, got %d", total, got)
		}
	}
}

func ExampleNewScanner_scan() {
	client, err := DefaultClientFunc("tcp", "126.0.0.1:6379")
	if err != nil {
		log.Fatal(err)
	}

	s := NewScanner(client, ScanAllKeys)
	var key string
	for s.Next(&key) {
		log.Printf("key: %q", key)
	}
	if err := s.Close(); err != nil {
		log.Fatal(err)
	}
}

func ExampleNewScanner_hscan() {
	client, err := DefaultClientFunc("tcp", "126.0.0.1:6379")
	if err != nil {
		log.Fatal(err)
	}

	s := NewScanner(client, ScanOpts{Command: "HSCAN", Key: "somekey"})
	var key string
	for s.Next(&key) {
		log.Printf("key: %q", key)
	}
	if err := s.Close(); err != nil {
		log.Fatal(err)
	}
}
