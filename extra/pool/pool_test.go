package pool

import (
	"github.com/fzzy/radix/redis"
	. "testing"
)

func TestPool(t *T) {
	pool, err := NewPool("tcp", "localhost:6379", 10)
	if err != nil {
		t.Fatal(err)
	}

	conns := make([]*redis.Client, 20)
	for i := range conns {
		if conns[i], err = pool.Get(); err != nil {
			t.Fatal(err)
		}
	}

	for i := range conns {
		pool.Put(conns[i])
	}

	pool.Empty()
}
