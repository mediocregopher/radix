package cluster

import (
	. "testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestCluster() (*Cluster, *stubCluster) {
	scl := newStubCluster(testTopo)
	c, err := NewCluster(scl.poolFunc(), scl.addrs()...)
	if err != nil {
		panic(err)
	}
	return c, scl
}

func TestClusterSync(t *T) {
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
	require.Nil(t, c.Sync())
	assertClusterState()

	// move it back to make sure SyncEvery works
	errCh := make(chan error, 10)
	c.SyncEvery(500*time.Millisecond, errCh)
	t.Logf("moving %s to %s", newAddr, oldAddr)
	scl.move(oldAddr, newAddr)

	time.Sleep(1 * time.Second)
	assertClusterState()
	c.Close()
	require.Nil(t, <-errCh)

}
