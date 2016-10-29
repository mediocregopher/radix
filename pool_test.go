package radix

import (
	"io"
	"sync"
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testPool(size int) Pool {
	pool, err := NewPool("tcp", "localhost:6379", size, nil)
	if err != nil {
		panic(err)
	}
	return pool
}

func TestPool(t *T) {
	size := 10
	pool := testPool(size)

	testEcho := func(c Cmder) {
		exp := randStr()
		var out string
		assert.Nil(t, c.Cmd(&out, "ECHO", exp))
		assert.Equal(t, exp, out)
	}

	var wg sync.WaitGroup
	for i := 0; i < size*4; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 100; i++ {
				conn, err := pool.Get("")
				assert.Nil(t, err)
				testEcho(conn)
				conn.Done()
			}
			wg.Done()
		}()
	}

	pc := NewPoolCmder(pool)
	for i := 0; i < size*4; i++ {
		testEcho(pc)
	}

	wg.Wait()

	// TODO if there's ever an avail method it'd be good to use it here
	sp := pool.(*staticPool)
	assert.Equal(t, size, len(sp.pool))

	pool.Close()
	assert.Equal(t, 0, len(sp.pool))
}

func TestPut(t *T) {
	size := 10
	pool := testPool(size)

	// TODO if there's ever an avail method it'd be good to use it here
	sp := pool.(*staticPool)

	conn, err := pool.Get("")
	require.Nil(t, err)
	assert.Equal(t, 9, len(sp.pool))

	conn.(*staticPoolConn).lastIOErr = io.EOF
	conn.Done()

	// Make sure that put does not accept a connection which has had a critical
	// network error
	assert.Equal(t, 9, len(sp.pool))

	// Make sure that closing the pool closes outstanding connections as well
	conn, err = pool.Get("")
	require.Nil(t, err)
	assert.Equal(t, 8, len(sp.pool))

	sp.Close()
	assert.Equal(t, 0, len(sp.pool))
	conn.Done()
	assert.Equal(t, 0, len(sp.pool))
}
