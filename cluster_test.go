package radix

import (
	"context"
	. "testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mediocregopher/radix/v4/trace"
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

func newTestCluster(ctx context.Context, opts ...ClusterOpt) (*Cluster, *clusterStub) {
	scl := newStubCluster(testTopo)
	return scl.newCluster(ctx, opts...), scl
}

func TestClusterSync(t *T) {
	ctx := testCtx(t)
	c, scl := newTestCluster(ctx)
	defer c.Close()
	assertClusterState := func() {
		require.Nil(t, c.Sync(ctx))
		err := c.proc.WithRLock(func() error {
			assert.Equal(t, c.topo, scl.topo())
			assert.Len(t, c.pools, len(c.topo))
			for _, node := range c.topo {
				assert.Contains(t, c.pools, node.Addr)
			}
			return nil
		})
		assert.NoError(t, err)
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
	ctx := testCtx(t)
	c, _ := newTestCluster(ctx)
	defer c.Close()
	for s := uint16(0); s < numSlots; s++ {
		require.Nil(t, c.Do(ctx, Cmd(nil, "GET", clusterSlotKeys[s])))
	}
}

func TestClusterDo(t *T) {
	ctx := testCtx(t)
	var lastRedirect trace.ClusterRedirected
	c, scl := newTestCluster(ctx, ClusterWithTrace(trace.ClusterTrace{
		Redirected: func(r trace.ClusterRedirected) { lastRedirect = r },
	}))
	defer c.Close()
	stub0 := scl.stubForSlot(0)
	stub16k := scl.stubForSlot(16000)

	// sanity check before we start, these shouldn't have the same address
	require.NotEqual(t, stub0.addr, stub16k.addr)

	// basic Cmd
	k, v := clusterSlotKeys[0], randStr()
	require.Nil(t, c.Do(ctx, Cmd(nil, "SET", k, v)))
	{
		var vgot string
		require.Nil(t, c.Do(ctx, Cmd(&vgot, "GET", k)))
		assert.Equal(t, v, vgot)
		assert.Equal(t, trace.ClusterRedirected{}, lastRedirect)
	}

	// use doInner to hit the wrong node originally, Do should get a MOVED error
	// and end up at the correct node
	{
		clients, err := c.Clients()
		assert.NoError(t, err)

		var vgot string
		cmd := Cmd(&vgot, "GET", k)
		require.Nil(t, c.doInner(clusterDoInnerParams{
			ctx:      ctx,
			action:   cmd,
			addr:     stub16k.addr,
			client:   clients[stub16k.addr].Primary,
			key:      k,
			attempts: doAttempts,
		}))
		assert.Equal(t, v, vgot)
		lastRedirect.Context = nil
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
		require.Nil(t, c.Do(ctx, Cmd(&vgot, "GET", k)))
		assert.Equal(t, v, vgot)
		lastRedirect.Context = nil
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
		require.Nil(t, c.Sync(ctx))
		require.Nil(t, c.Do(ctx, Cmd(&vgot, "GET", k)))
		assert.Equal(t, v, vgot)
		assert.Equal(t, trace.ClusterRedirected{}, lastRedirect)
	}
}

func TestClusterDoWhenDown(t *T) {
	ctx := testCtx(t)
	var stub *clusterNodeStub
	var isDown bool
	c, scl := newTestCluster(ctx,
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

	err := c.Do(ctx, Cmd(nil, "GET", k))
	assert.EqualError(t, err, "CLUSTERDOWN Hash slot not served")
	assert.True(t, isDown)

	err = c.Do(ctx, Cmd(nil, "GET", k))
	assert.Nil(t, err)
	assert.False(t, isDown)
}

func BenchmarkClusterDo(b *B) {
	ctx := testCtx(b)
	c, _ := newTestCluster(ctx)
	defer c.Close()

	k, v := clusterSlotKeys[0], randStr()
	require.Nil(b, c.Do(ctx, Cmd(nil, "SET", k, v)))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		require.Nil(b, c.Do(ctx, Cmd(nil, "GET", k)))
	}
}

func TestClusterEval(t *T) {
	ctx := testCtx(t)
	c, scl := newTestCluster(ctx)
	defer c.Close()
	key := clusterSlotKeys[0]
	dst := scl.stubForSlot(10000)
	scl.migrateInit(dst.addr, 0)
	// now, when interacting with key, the stub should return an ASK error

	eval := NewEvalScript(`return nil`)
	var rcv string
	err := c.Do(ctx, eval.Cmd(&rcv, []string{key}, "foo"))

	assert.Nil(t, err)
	assert.Equal(t, "EVAL: success!", rcv)
}

func TestClusterEvalRcvInterface(t *T) {
	ctx := testCtx(t)
	c, scl := newTestCluster(ctx)
	defer c.Close()
	key := clusterSlotKeys[0]
	dst := scl.stubForSlot(10000)
	scl.migrateInit(dst.addr, 0)
	// now, when interacting with key, the stub should return an ASK error

	eval := NewEvalScript(`return nil`)
	var rcv interface{}
	err := c.Do(ctx, eval.Cmd(&rcv, []string{key}, "foo"))

	assert.Nil(t, err)
	assert.Equal(t, []byte("EVAL: success!"), rcv)
}

func TestClusterDoSecondary(t *T) {
	ctx := testCtx(t)
	var redirects int
	c, _ := newTestCluster(ctx,
		ClusterWithTrace(trace.ClusterTrace{
			Redirected: func(trace.ClusterRedirected) {
				redirects++
			},
		}),
	)
	defer c.Close()

	key := clusterSlotKeys[0]
	value := randStr()

	require.NoError(t, c.Do(ctx, Cmd(nil, "SET", key, value)))

	var res1 string
	require.NoError(t, c.Do(ctx, Cmd(&res1, "GET", key)))
	require.Equal(t, value, res1)

	require.Zero(t, redirects)

	var res2 string
	assert.NoError(t, c.DoSecondary(ctx, Cmd(&res2, "GET", key)))
	assert.Equal(t, value, res2)
	assert.Equal(t, 1, redirects)

	sec, _, err := c.clientForKey(key, false, true)
	assert.NoError(t, err)
	assert.NoError(t, sec.Do(ctx, Cmd(nil, "READONLY")))
	assert.Equal(t, 1, redirects)

	var res3 string
	assert.NoError(t, c.DoSecondary(ctx, Cmd(&res3, "GET", key)))
	assert.Equal(t, value, res3)
	assert.Equal(t, 1, redirects)

	assert.NoError(t, sec.Do(ctx, Cmd(nil, "READWRITE")))
	assert.Equal(t, 1, redirects)

	var res4 string
	assert.NoError(t, c.DoSecondary(ctx, Cmd(&res4, "GET", key)))
	assert.Equal(t, value, res4)
	assert.Equal(t, 2, redirects)
}

var clusterAddrs []string

func ExampleClusterPoolFunc_defaultClusterConnFunc() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	NewCluster(ctx, clusterAddrs, ClusterPoolFunc(func(ctx context.Context, network, addr string) (Client, error) {
		return NewPool(ctx, network, addr, 4, PoolConnFunc(DefaultClusterConnFunc))
	}))
}
