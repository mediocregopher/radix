package cluster

import (
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestCluster() (*cluster, *stubCluster) {
	scl := newStubCluster(testTopo)
	c, err := newCluster(scl.poolFunc(), scl.addrs()...)
	if err != nil {
		panic(err)
	}
	return c, scl
}

func TestClusterInitSync(t *T) {
	c, scl := newTestCluster()
	assertClusterState := func() {
		assert.Len(t, c.pools, len(testTopo))
		for addr := range c.pools {
			assert.Contains(t, scl.stubs, addr)
		}
	}
	assertClusterState()

	// move a node to a new address
	oldAddr := scl.randStub().addr
	newAddr := "10.128.0.40:6379"
	t.Logf("moving %s to %s", oldAddr, newAddr)
	scl.move(newAddr, oldAddr)
	require.Nil(t, c.sync())
	assertClusterState()
}
