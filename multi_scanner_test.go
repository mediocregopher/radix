package radix

import (
	"context"
	"testing"
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testClusterScanner(t *T, ctx context.Context, c *Cluster) {
	t.Helper()

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

func TestClusterScanner(t *T) {
	t.Run("full topo", func(t *testing.T) {
		ctx := testCtx(t)
		c, _ := newTestCluster(ctx, ClusterConfig{})
		defer c.Close()
		testClusterScanner(t, ctx, c)
	})

	t.Run("only primaries topo", func(t *testing.T) {
		ctx := testCtx(t)
		c, _ := newTestClusterPrimariesOnly(ctx, ClusterConfig{})
		defer c.Close()
		testClusterScanner(t, ctx, c)
	})
}
