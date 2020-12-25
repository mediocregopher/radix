package radix

import (
	. "testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mediocregopher/radix/v3/trace"
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

func newTestCluster(opts ...ClusterOpt) (*Cluster, *clusterStub) {
	scl := newStubCluster(testTopo)
	return scl.newCluster(opts...), scl
}

// sanity check that Cluster is a client
func TestClusterClient(t *T) {
	c, _ := newTestCluster()
	defer c.Close()
	assert.Implements(t, new(Client), c)
}

func makeFailedFlagMap(addrs []string) map[string]bool {
	failedAddrsFlag := make(map[string]bool)
	for _, addr := range addrs {
		failedAddrsFlag[addr] = true
	}
	return failedAddrsFlag
}

func TestClusterInitSync(t *T) {
	scl := newStubCluster(testTopo)
	serverAddrs := scl.addrs()
	{
		c := scl.newCluster()
		err := c.Sync(true, false, true)
		assert.NotNil(t, err)
	}
	//part of the addresses are unavailable during the initialization
	//and recover after that, call Sync to test whether it can work
	{
		c, err := scl.newInitSyncErrorCluster(serverAddrs,
			makeFailedFlagMap(serverAddrs[0:len(serverAddrs)/2]),
			ClusterWithInitSyncSilent(true))
		require.Nil(t, err)
		defer c.Close()
		//TestClusterSync
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

	//part of the addresses are unavailable during the initialization
	//and recover after that, try Set and Get cmd to test whether it can work
	{
		c, err := scl.newInitSyncErrorCluster(serverAddrs,
			makeFailedFlagMap(serverAddrs[len(serverAddrs)/2:]),
			ClusterWithInitSyncSilent(true))
		require.Nil(t, err)
		defer c.Close()
		// find the address's slot
		var targetStub *clusterNodeStub
		for i := len(serverAddrs) / 2; i < len(serverAddrs); i++ {
			targetStub = scl.stubs[serverAddrs[i]]
			if slotRanges := targetStub.slotRanges(); len(slotRanges) != 0 {
				break
			}
		}
		require.NotNil(t, targetStub)
		client, _ := c.rpool(targetStub.addr)
		require.Nil(t, client)

		slotRanges := targetStub.slotRanges()[0]
		targetSlotNum := (slotRanges[1] + slotRanges[0]) / 2
		t.Logf("the target addr for set and get is %s, slotnum= %d", targetStub.addr, targetSlotNum)
		k, v := clusterSlotKeys[targetSlotNum], randStr()
		t.Logf("call set, key=%s, v=%s", k, v)
		require.Nil(t, c.Do(Cmd(nil, "SET", k, v)))
		var vgot string
		require.Nil(t, c.Do(Cmd(&vgot, "GET", k)))
		assert.Equal(t, v, vgot)
	}
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
	var lastRedirect trace.ClusterRedirected
	c, scl := newTestCluster(ClusterWithTrace(trace.ClusterTrace{
		Redirected: func(r trace.ClusterRedirected) { lastRedirect = r },
	}))
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
		assert.Equal(t, trace.ClusterRedirected{}, lastRedirect)
	}

	// use doInner to hit the wrong node originally, Do should get a MOVED error
	// and end up at the correct node
	{
		var vgot string
		cmd := Cmd(&vgot, "GET", k)
		require.Nil(t, c.doInner(cmd, stub16k.addr, k, false, doAttempts))
		assert.Equal(t, v, vgot)
		assert.Equal(t, trace.ClusterRedirected{
			Addr:          stub16k.addr,
			Key:           k,
			Moved:         true,
			RedirectCount: 1,
		}, lastRedirect)
	}

	// start a migration and migrate the key, which should trigger an ASK when
	// we hit stub0 for the key
	{
		scl.migrateInit(stub16k.addr, 0)
		scl.migrateKey(k)
		var vgot string
		require.Nil(t, c.Do(Cmd(&vgot, "GET", k)))
		assert.Equal(t, v, vgot)
		assert.Equal(t, trace.ClusterRedirected{
			Addr:          stub0.addr,
			Key:           k,
			Ask:           true,
			RedirectCount: 1,
		}, lastRedirect)
	}

	// Finish the migration, there should not be anymore redirects
	{
		scl.migrateAllKeys(0)
		scl.migrateDone(0)
		lastRedirect = trace.ClusterRedirected{}
		var vgot string
		require.Nil(t, c.Sync())
		require.Nil(t, c.Do(Cmd(&vgot, "GET", k)))
		assert.Equal(t, v, vgot)
		assert.Equal(t, trace.ClusterRedirected{}, lastRedirect)
	}
}

func TestClusterDoWhenDown(t *T) {
	var stub *clusterNodeStub

	var isDown bool

	c, scl := newTestCluster(
		ClusterOnDownDelayActionsBy(50*time.Millisecond),
		ClusterWithTrace(trace.ClusterTrace{
			StateChange: func(d trace.ClusterStateChange) {
				isDown = d.IsDown

				if d.IsDown {
					time.AfterFunc(75*time.Millisecond, func() {
						stub.addSlot(0)
					})
				}
			},
		}),
	)
	defer c.Close()

	stub = scl.stubForSlot(0)
	stub.removeSlot(0)

	k := clusterSlotKeys[0]

	err := c.Do(Cmd(nil, "GET", k))
	assert.EqualError(t, err, "CLUSTERDOWN Hash slot not served")
	assert.True(t, isDown)

	err = c.Do(Cmd(nil, "GET", k))
	assert.Nil(t, err)
	assert.False(t, isDown)
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

func TestClusterEvalRcvInterface(t *T) {
	c, scl := newTestCluster()
	defer c.Close()
	key := clusterSlotKeys[0]
	dst := scl.stubForSlot(10000)
	scl.migrateInit(dst.addr, 0)
	// now, when interacting with key, the stub should return an ASK error

	eval := NewEvalScript(1, `return nil`)
	var rcv interface{}
	err := c.Do(eval.Cmd(&rcv, key, "foo"))

	assert.Nil(t, err)
	assert.Equal(t, []byte("EVAL: success!"), rcv)
}

func TestClusterDoSecondary(t *T) {
	var redirects int
	c, _ := newTestCluster(
		ClusterWithTrace(trace.ClusterTrace{
			Redirected: func(trace.ClusterRedirected) {
				redirects++
			},
		}),
	)
	defer c.Close()

	key := clusterSlotKeys[0]
	value := randStr()

	require.NoError(t, c.Do(Cmd(nil, "SET", key, value)))

	var res1 string
	require.NoError(t, c.Do(Cmd(&res1, "GET", key)))
	require.Equal(t, value, res1)

	require.Zero(t, redirects)

	var res2 string
	assert.NoError(t, c.DoSecondary(Cmd(&res2, "GET", key)))
	assert.Equal(t, value, res2)
	assert.Equal(t, 1, redirects)

	var secAddr string
	for secAddr = range c.secondaries[c.addrForKey(key)] {
		break
	}
	sec, err := c.Client(secAddr)
	require.NoError(t, err)

	assert.NoError(t, sec.Do(Cmd(nil, "READONLY")))
	assert.Equal(t, 1, redirects)

	var res3 string
	assert.NoError(t, c.DoSecondary(Cmd(&res3, "GET", key)))
	assert.Equal(t, value, res3)
	assert.Equal(t, 1, redirects)

	assert.NoError(t, sec.Do(Cmd(nil, "READWRITE")))
	assert.Equal(t, 1, redirects)

	var res4 string
	assert.NoError(t, c.DoSecondary(Cmd(&res4, "GET", key)))
	assert.Equal(t, value, res4)
	assert.Equal(t, 2, redirects)
}

var clusterAddrs []string

func ExampleClusterPoolFunc_defaultClusterConnFunc() {
	NewCluster(clusterAddrs, ClusterPoolFunc(func(network, addr string) (Client, error) {
		return NewPool(network, addr, 4, PoolConnFunc(DefaultClusterConnFunc))
	}))
}
