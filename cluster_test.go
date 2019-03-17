package radix

import (
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// clusterSlotKeys contains a random key for every slot. Unfortunately I haven't
// come up with a better way to do this than brute force. It takes less than a
// second on my laptop, so whatevs.
var clusterSlotKeys = func() [numSlots]string {
	var a [numSlots]string
	var found int
	for found < len(a) {
		// we get a set of random characters and try increasingly larger subsets
		// of that set until one is in a slot which hasn't been set yet. This is
		// optimal because it minimizes the number of reads from random needed
		// to fill a slot, and the keys being filled are of minimal size.
		k := []byte(randStr())
		for i := 1; i <= len(k); i++ {
			ksmall := k[:i]
			if a[ClusterSlot(ksmall)] == "" {
				a[ClusterSlot(ksmall)] = string(ksmall)
				found++
				break
			}
		}
	}
	return a
}()

func newTestCluster() (*Cluster, *clusterStub) {
	scl := newStubCluster(testTopo)
	return scl.newCluster(), scl
}

// sanity check that Cluster is a client
func TestClusterClient(t *T) {
	c, _ := newTestCluster()
	defer c.Close()
	assert.Implements(t, new(Client), c)
}

func TestClusterSync(t *T) {
	c, scl := newTestCluster()
	defer c.Close()
	assertClusterState := func() {
		require.Nil(t, c.Sync())
		c.l.RLock()
		defer c.l.RUnlock()
		assert.Equal(t, c.topo, scl.topo())
		assert.Len(t, c.pools, len(c.topo))
		for _, node := range c.topo {
			assert.Contains(t, c.pools, node.Addr)
		}
	}
	assertClusterState()

	// cluster is unstable af
	for i := 0; i < 10; i++ {
		// find a usabel src/dst
		var srcStub, dstStub *clusterNodeStub
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

func TestClusterGet(t *T) {
	c, _ := newTestCluster()
	defer c.Close()
	for s := uint16(0); s < numSlots; s++ {
		require.Nil(t, c.Do(Cmd(nil, "GET", clusterSlotKeys[s])))
	}
}

func TestClusterDo(t *T) {
	c, scl := newTestCluster()
	defer c.Close()
	stub0 := scl.stubForSlot(0)
	stub16k := scl.stubForSlot(16000)

	// sanity check before we start, these shouldn't have the same address
	require.NotEqual(t, stub0.addr, stub16k.addr)

	// basic Cmd
	k, v := clusterSlotKeys[0], randStr()
	require.Nil(t, c.Do(Cmd(nil, "SET", k, v)))
	{
		var vgot string
		require.Nil(t, c.Do(Cmd(&vgot, "GET", k)))
		assert.Equal(t, v, vgot)
	}

	// use doInner to hit the wrong node originally, Do should get a MOVED error
	// and end up at the correct node
	{
		var vgot string
		cmd := Cmd(&vgot, "GET", k)
		require.Nil(t, c.doInner(cmd, stub16k.addr, k, false, 2))
		assert.Equal(t, v, vgot)
	}

	// start a migration and migrate the key, which should trigger an ASK when
	// we hit stub0 for the key
	{
		scl.migrateInit(stub16k.addr, 0)
		scl.migrateKey(k)
		var vgot string
		require.Nil(t, c.Do(Cmd(&vgot, "GET", k)))
		assert.Equal(t, v, vgot)
		scl.migrateAllKeys(0)
		scl.migrateDone(0)
	}
}

func BenchmarkClusterDo(b *B) {
	c, _ := newTestCluster()
	defer c.Close()

	k, v := clusterSlotKeys[0], randStr()
	require.Nil(b, c.Do(Cmd(nil, "SET", k, v)))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		require.Nil(b, c.Do(Cmd(nil, "GET", k)))
	}
}

func TestClusterEval(t *T) {
	c, scl := newTestCluster()
	defer c.Close()
	key := clusterSlotKeys[0]
	dst := scl.stubForSlot(10000)
	scl.migrateInit(dst.addr, 0)
	// now, when interacting with key, the stub should return an ASK error

	eval := NewEvalScript(1, `return nil`)
	var rcv string
	err := c.Do(eval.Cmd(&rcv, key, "foo"))

	assert.Nil(t, err)
	assert.Equal(t, "EVAL: success!", rcv)
}
