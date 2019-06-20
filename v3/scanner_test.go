package radix

import (
	"log"
	"strconv"
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
