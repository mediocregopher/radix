package radix

import (
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterScanner(t *T) {
	c, _ := newTestCluster()
	defer c.Close()
	exp := map[string]bool{}
	for _, k := range clusterSlotKeys {
		exp[k] = true
		require.Nil(t, c.Do(Cmd(nil, "SET", k, "1")))
	}

	scanner := c.NewScanner(ScanAllKeys)
	var k string
	got := map[string]bool{}
	for scanner.Next(&k) {
		got[k] = true
	}

	assert.Equal(t, exp, got)
}
