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

/*
func assertMoved(t *T, err error, slot uint16, to string) {
	rerr, ok := err.(resp.Error)
	assert.True(t, ok)
	assert.Equal(t, fmt.Sprintf("MOVED %d %s", slot, to), rerr.Error())
}

func TestMoved(t *T) {
	c, scl := newTestCluster()
	// These two slots should be handled by totally different nodes
	slotA, slotB := uint16(1), uint16(16000)
	keyA := slotKeys[slotA]
	stubA, stubB := scl.stubForSlot(slotA), scl.stubForSlot(slotB)

	require.Nil(t, stubA.newClient().Do(radix.Cmd(nil, "SET", keyA, "foo")))

	// confirm that the stub is returning MOVED correctly
	err := stubB.newClient().Do(radix.Cmd(nil, "GET", keyA))
	assertMoved(t, err, slotA, stubA.addr)

	// confirm that cluster handles moves correctly, by first retrieving a conn
	// for an Action and then changing the node on which that action should be
	// taken
	var foo string
	var swapped bool
	err = c.Do(radix.WithConn([]byte(keyA), func(c radix.Conn) error {
		if !swapped {
			scl.swap(stubA.addr, stubB.addr)
			swapped = true
		}
		return radix.Cmd(&foo, "GET", keyA).Run(c)
	}))
	require.Nil(t, err, "%s", err)
	assert.Equal(t, "foo", foo)
	//t.Fatal("this shouldn't be working, cluster isn't actually checking for MOVED yet")
}
*/
