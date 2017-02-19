package cluster

import (
	"crypto/rand"
	"encoding/hex"
	. "testing"

	radix "github.com/mediocregopher/radix.v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// slotKeys contains a random key for every slot. Unfortunately I haven't come
// up with a better way to do this than brute force. It takes like 5 seconds on
// my laptop, which isn't terrible.
var slotKeys = func() [numSlots]string {
	var a [numSlots]string
	for {
		k := []byte(randStr())
		a[Slot(k)] = string(k)

		var notFull bool
		for _, k := range a {
			if k == "" {
				notFull = true
				break
			}
		}

		if !notFull {
			return a
		}
	}
}()

func randStr() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

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
		c.RLock()
		defer c.RUnlock()
		assert.Len(t, c.pools, len(testTopo))
		for addr := range c.pools {
			assert.Contains(t, scl.stubs, addr)
		}
	}
	assertClusterState()

	// cluster is unstable af
	for i := 0; i < 10; i++ {
		// move a node to a new address
		oldAddr := scl.randStub().addr
		newAddr := "10.128.1.1:6379"
		t.Logf("moving %s to %s", oldAddr, newAddr)
		scl.move(newAddr, oldAddr)
		require.Nil(t, c.Sync())
		assertClusterState()

		// move it back
		t.Logf("moving %s to %s", newAddr, oldAddr)
		scl.move(oldAddr, newAddr)
		require.Nil(t, c.Sync())
		assertClusterState()
	}
}

func TestGet(t *T) {
	c, _ := newTestCluster()
	for s := uint16(0); s < numSlots; s++ {
		var connSlots []uint16
		err := c.Do(radix.Cmd(&connSlots, "CONNSLOTS", slotKeys[s]))
		require.Nil(t, err)
		assert.True(t, s >= connSlots[0] && s < connSlots[1])
	}
}
