package radix

import (
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterScanner(t *T) {
	ctx := testCtx(t)
	c, _ := newTestCluster(ctx, ClusterConfig{})
	defer c.Close()
	exp := map[string]bool{}
	for _, k := range clusterSlotKeys {
		exp[k] = true
		require.Nil(t, c.Do(ctx, Cmd(nil, "SET", k, "1")))
	}

	scanner := (ScannerConfig{}).NewMulti(c)
	var k string
	got := map[string]bool{}
	for scanner.Next(ctx, &k) {
		got[k] = true
	}

	assert.Equal(t, exp, got)
}
