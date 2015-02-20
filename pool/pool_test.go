package pool

import (
	"sync"
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPool(t *T) {
	size := 10
	pool, err := New("tcp", "localhost:6379", size)
	require.Nil(t, err)

	var wg sync.WaitGroup
	for i := 0; i < size*4; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 100; i++ {
				conn, err := pool.Get()
				assert.Nil(t, err)

				assert.Nil(t, conn.Cmd("ECHO", "HI").Err)

				pool.Put(conn)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	assert.Equal(t, size, len(pool.pool))

	pool.Empty()
	assert.Equal(t, 0, len(pool.pool))
}

func TestCmd(t *T) {
	size := 10
	pool, err := New("tcp", "localhost:6379", 10)
	require.Nil(t, err)

	var wg sync.WaitGroup
	for i := 0; i < size*4; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 100; i++ {
				assert.Nil(t, pool.Cmd("ECHO", "HI").Err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	assert.Equal(t, size, len(pool.pool))
}

func TestPut(t *T) {
	pool, err := New("tcp", "localhost:6379", 10)
	require.Nil(t, err)

	conn, err := pool.Get()
	require.Nil(t, err)
	assert.Equal(t, 9, len(pool.pool))

	conn.Close()
	assert.NotNil(t, conn.Cmd("PING").Err)
	pool.Put(conn)

	// Make sure that Put does not accept a connection which has had a critical
	// network error
	assert.Equal(t, 9, len(pool.pool))
}
