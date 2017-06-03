package radix

import (
	"io"
	"sync"
	. "testing"

	"github.com/stretchr/testify/assert"
)

func testPool(size int) Client {
	pool, err := Pool("tcp", "localhost:6379", size, nil)
	if err != nil {
		panic(err)
	}
	return pool
}

func TestPool(t *T) {
	size := 10
	pool := testPool(size)

	testEcho := func(c Conn) {
		exp := randStr()
		var out string
		assert.Nil(t, c.Do(CmdNoKey(&out, "ECHO", exp)))
		assert.Equal(t, exp, out)
	}

	var wg sync.WaitGroup
	for i := 0; i < size*4; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 100; i++ {
				pool.Do(WithConn(nil, func(conn Conn) error {
					testEcho(conn)
					return nil
				}))
			}
			wg.Done()
		}()
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
	<-pool.(*staticPool).initDone

	// TODO if there's ever an avail method it'd be good to use it here
	assertPoolConns := func(exp int) {
		sp := pool.(*staticPool)
		assert.Equal(t, exp, len(sp.pool))
	}
	assertPoolConns(10)

	// Make sure that put does not accept a connection which has had a critical
	// network error
	pool.Do(WithConn(nil, func(conn Conn) error {
		assertPoolConns(9)
		conn.(*staticPoolConn).lastIOErr = io.EOF
		return nil
	}))
	assertPoolConns(9)

	// Make sure that a put _does_ accept a connection which had a
	// marshal/unmarshal error
	pool.Do(WithConn(nil, func(conn Conn) error {
		assert.NotNil(t, conn.Do(CmdNoKey(nil, "ECHO", func() {})))
		assert.Nil(t, conn.(*staticPoolConn).lastIOErr)
		return nil
	}))
	assertPoolConns(9)

	// Make sure that a put _does_ accept a connection which had an app level
	// resp error
	pool.Do(WithConn(nil, func(conn Conn) error {
		assert.NotNil(t, CmdNoKey(nil, "CMDDNE"))
		assert.Nil(t, conn.(*staticPoolConn).lastIOErr)
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
	pool.Do(WithConn(nil, func(conn Conn) error {
		closeCh <- true
		<-closeCh
		return nil
	}))
	assertPoolConns(0)
}
