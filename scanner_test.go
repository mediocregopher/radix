package radix

import (
	"strconv"
	. "testing"

	"github.com/levenlabs/golib/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanner(t *T) {
	c := dial()

	// Make a random dataset
	prefix := testutil.RandStr()
	fullMap := map[string]bool{}
	for i := 0; i < 100; i++ {
		key := prefix + ":" + strconv.Itoa(i)
		fullMap[key] = true
		require.Nil(t, Cmd("SET", key, "1").Run(c))
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
		require.Nil(t, Cmd("SADD", key, elem).Run(c))
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
