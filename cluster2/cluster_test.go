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
// up with a better way to do this than brute force. It takes less than a second
// on my laptop, so whatevs.
var slotKeys = func() [numSlots]string {
	var a [numSlots]string
	for {
		// we get a set of random characters and try increasingly larger subsets
		// of that set until one is in a slot which hasn't been set yet. This is
		// optimal because it minimizes the number of reads from random needed
		// to fill a slot, and the keys being filled are of minimal size.
		k := []byte(randStr())
		for i := 1; i <= len(k); i++ {
			ksmall := k[:i]
			if a[Slot(ksmall)] == "" {
				a[Slot(ksmall)] = string(ksmall)
				break
			}
		}

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
	return scl.newCluster(), scl
}

func TestClusterSync(t *T) {
	c, scl := newTestCluster()
	assertClusterState := func() {
		require.Nil(t, c.Sync())
		c.RLock()
		defer c.RUnlock()
		assert.Equal(t, c.tt, scl.topo())
		assert.Len(t, c.pools, len(c.tt))
		for _, node := range c.tt {
			assert.Contains(t, c.pools, node.Addr)
		}
	}
	assertClusterState()

	// cluster is unstable af
	for i := 0; i < 10; i++ {
		// find a usabel src/dst
		var srcStub, dstStub *stub
		for {
			srcStub = scl.randStub()
			dstStub = scl.randStub()
			if srcStub.addr == dstStub.addr {
				continue
			} else if slotRanges := srcStub.slotRanges(); len(slotRanges) == 0 {
				continue
			}
			break
		}

		// move src's first slot range to dst
		slotRange := srcStub.slotRanges()[0]
		t.Logf("moving %d:%d from %s to %s", slotRange[0], slotRange[1], srcStub.addr, dstStub.addr)
		scl.migrateSlotRange(dstStub.addr, slotRange[0], slotRange[1])
		assertClusterState()
	}
}

func TestGet(t *T) {
	c, _ := newTestCluster()
	for s := uint16(0); s < numSlots; s++ {
		require.Nil(t, c.Do(radix.Cmd(nil, "GET", slotKeys[s])))
	}
}

func TestDo(t *T) {
	c, scl := newTestCluster()
	stub0 := scl.stubForSlot(0)
	stub16k := scl.stubForSlot(16000)

	// sanity check before we start, these shouldn't have the same address
	require.NotEqual(t, stub0.addr, stub16k.addr)

	// basic Cmd
	k, v := slotKeys[0], randStr()
	require.Nil(t, c.Do(radix.Cmd(nil, "SET", k, v)))
	{
		var vgot string
		require.Nil(t, c.Do(radix.Cmd(&vgot, "GET", k)))
		assert.Equal(t, v, vgot)
	}

	// use doInner to hit the wrong node originally, Do should get a MOVED error
	// and end up at the correct node
	{
		var vgot string
		cmd := radix.Cmd(&vgot, "GET", k)
		require.Nil(t, c.doInner(cmd, stub16k.addr, false, 2))
		assert.Equal(t, v, vgot)
	}

	// start a migration and migrate the key, which should trigger an ASK when
	// we hit stub0 for the key
	{
		scl.migrateInit(stub16k.addr, 0)
		scl.migrateKey(k)
		var vgot string
		require.Nil(t, c.Do(radix.Cmd(&vgot, "GET", k)))
		assert.Equal(t, v, vgot)
		scl.migrateAllKeys(0)
		scl.migrateDone(0)
	}
}
