package radix

import (
	"io"
	"sync"
	"sync/atomic"
	. "testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/mediocregopher/radix/v3/trace"
)

func testPool(size int, opts ...PoolOpt) *Pool {
	pool, err := NewPool("tcp", "localhost:6379", size, opts...)
	if err != nil {
		panic(err)
	}
	<-pool.initDone
	return pool
}

func TestPool(t *T) {
	testEcho := func(c Conn) error {
		exp := randStr()
		var out string
		assert.Nil(t, c.Do(Cmd(&out, "ECHO", exp)))
		assert.Equal(t, exp, out)
		return nil
	}

	do := func(opts ...PoolOpt) {
		opts = append(opts, PoolOnFullClose())
		size := 10
		pool := testPool(size, opts...)
		var wg sync.WaitGroup
		for i := 0; i < size*4; i++ {
			wg.Add(1)
			go func() {
				for i := 0; i < 100; i++ {
					assert.NoError(t, pool.Do(WithConn("", testEcho)))
				}
				wg.Done()
			}()
		}
		wg.Wait()
		assert.Equal(t, size, pool.NumAvailConns())
		pool.Close()
		assert.Equal(t, 0, pool.NumAvailConns())
	}

	t.Run("onEmptyWait", func(t *T) { do(PoolOnEmptyWait()) })
	t.Run("onEmptyCreate", func(t *T) { do(PoolOnEmptyCreateAfter(0)) })
	t.Run("onEmptyCreateAfter", func(t *T) { do(PoolOnEmptyCreateAfter(1 * time.Second)) })
	// This one is expected to error, since this test empties the pool by design
	//t.Run("onEmptyErr", func(t *T) { do(PoolOnEmptyErrAfter(0)) })
	t.Run("onEmptyErrAfter", func(t *T) { do(PoolOnEmptyErrAfter(1 * time.Second)) })

	t.Run("withTrace", func(t *T) {
		var connCreatedCount int
		var connClosedCount int
		var doCompletedCount uint32
		var initializedAvailCount int
		pt := trace.PoolTrace{
			ConnCreated: func(done trace.PoolConnCreated) {
				connCreatedCount++
			},
			ConnClosed: func(closed trace.PoolConnClosed) {
				connClosedCount++
			},
			DoCompleted: func(completed trace.PoolDoCompleted) {
				atomic.AddUint32(&doCompletedCount, 1)
			},
			InitCompleted: func(completed trace.PoolInitCompleted) {
				initializedAvailCount = completed.AvailCount
			},
		}
		do(PoolWithTrace(pt))
		if initializedAvailCount != 10 {
			t.Fail()
		}
		if connCreatedCount != connClosedCount {
			t.Fail()
		}
		if doCompletedCount == 0 {
			t.Fail()
		}
	})
}

// Test all the different OnEmpty behaviors
func TestPoolGet(t *T) {
	getBlock := func(p *Pool) (time.Duration, error) {
		start := time.Now()
		_, err := p.get()
		return time.Since(start), err
	}

	// this one is a bit weird, cause it would block infinitely if we let it
	t.Run("onEmptyWait", func(t *T) {
		pool := testPool(1, PoolOnEmptyWait())
		conn, err := pool.get()
		assert.NoError(t, err)

		go func() {
			time.Sleep(2 * time.Second)
			pool.put(conn)
		}()
		took, err := getBlock(pool)
		assert.NoError(t, err)
		assert.True(t, took-2*time.Second < 20*time.Millisecond)
	})

	// the rest are pretty straightforward
	gen := func(mkOpt func(time.Duration) PoolOpt, d time.Duration, expErr error) func(*T) {
		return func(t *T) {
			pool := testPool(0, PoolOnFullClose(), mkOpt(d))
			took, err := getBlock(pool)
			assert.Equal(t, expErr, err)
			assert.True(t, took-d < 20*time.Millisecond)
		}
	}

	t.Run("onEmptyCreate", gen(PoolOnEmptyCreateAfter, 0, nil))
	t.Run("onEmptyCreateAfter", gen(PoolOnEmptyCreateAfter, 1*time.Second, nil))
	t.Run("onEmptyErr", gen(PoolOnEmptyErrAfter, 0, ErrPoolEmpty))
	t.Run("onEmptyErrAfter", gen(PoolOnEmptyErrAfter, 1*time.Second, ErrPoolEmpty))
}

func TestPoolOnFull(t *T) {
	t.Run("onFullClose", func(t *T) {
		pool := testPool(1, PoolOnFullClose())
		defer pool.Close()
		assert.Equal(t, 1, len(pool.pool))

		spc, err := pool.newConn(false, "TEST")
		assert.NoError(t, err)
		pool.put(spc)
		assert.Equal(t, 1, len(pool.pool))
	})

	t.Run("onFullBuffer", func(t *T) {
		pool := testPool(1, PoolOnFullBuffer(1, 1*time.Second))
		defer pool.Close()
		assert.Equal(t, 1, len(pool.pool))

		// putting a conn should overflow
		spc, err := pool.newConn(false, "TEST")
		assert.NoError(t, err)
		pool.put(spc)
		assert.Equal(t, 2, len(pool.pool))

		// another shouldn't, overflow is full
		spc, err = pool.newConn(false, "TEST")
		assert.NoError(t, err)
		pool.put(spc)
		assert.Equal(t, 2, len(pool.pool))

		// retrieve from the pool, drain shouldn't do anything because the
		// overflow is empty now
		<-pool.pool
		assert.Equal(t, 1, len(pool.pool))
		time.Sleep(2 * time.Second)
		assert.Equal(t, 1, len(pool.pool))

		// if both are full then drain should remove the overflow one
		spc, err = pool.newConn(false, "TEST")
		assert.NoError(t, err)
		pool.put(spc)
		assert.Equal(t, 2, len(pool.pool))
		time.Sleep(2 * time.Second)
		assert.Equal(t, 1, len(pool.pool))
	})
}

func TestPoolPut(t *T) {
	size := 10
	pool := testPool(size)

	assertPoolConns := func(exp int) {
		assert.Equal(t, exp, pool.NumAvailConns())
	}
	assertPoolConns(10)

	// Make sure that put does not accept a connection which has had a critical
	// network error
	pool.Do(WithConn("", func(conn Conn) error {
		assertPoolConns(9)
		conn.(*ioErrConn).lastIOErr = io.EOF
		return nil
	}))
	assertPoolConns(9)

	// Make sure that a put _does_ accept a connection which had a
	// marshal/unmarshal error
	pool.Do(WithConn("", func(conn Conn) error {
		assert.NotNil(t, conn.Do(FlatCmd(nil, "ECHO", "", func() {})))
		assert.Nil(t, conn.(*ioErrConn).lastIOErr)
		return nil
	}))
	assertPoolConns(9)

	// Make sure that a put _does_ accept a connection which had an app level
	// resp error
	pool.Do(WithConn("", func(conn Conn) error {
		assert.NotNil(t, Cmd(nil, "CMDDNE"))
		assert.Nil(t, conn.(*ioErrConn).lastIOErr)
		return nil
	}))
	assertPoolConns(9)

	// Make sure that closing the pool closes outstanding connections as well
	closeCh := make(chan bool)
	go func() {
		<-closeCh
		assert.Nil(t, pool.Close())
		closeCh <- true
	}()
	pool.Do(WithConn("", func(conn Conn) error {
		closeCh <- true
		<-closeCh
		return nil
	}))
	assertPoolConns(0)
}

func TestPoolClose(t *T) {
	pool := testPool(1)
	assert.NoError(t, pool.Do(Cmd(nil, "PING")))
	assert.NoError(t, pool.Close())
	assert.Error(t, errClientClosed, pool.Do(Cmd(nil, "PING")))
}
