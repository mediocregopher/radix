package radix

import (
	"sync"
	. "testing"

	redigo "github.com/garyburd/redigo/redis"
)

// TODO if the package name is going to change then stuff in here should get
// renamed

func newRedigo() redigo.Conn {
	c, err := redigo.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		panic(err)
	}
	return c
}

func BenchmarkSerialGetSet(b *B) {
	radix := dial()
	b.Run("radix", func(b *B) {
		for i := 0; i < b.N; i++ {
			if err := radix.Do(Cmd(nil, "SET", "foo", "bar")); err != nil {
				b.Fatal(err)
			}
			var out string
			if err := radix.Do(Cmd(&out, "GET", "foo")); err != nil {
				b.Fatal(err)
			}
		}
	})

	red := newRedigo()
	b.Run("redigo", func(b *B) {
		for i := 0; i < b.N; i++ {
			if _, err := red.Do("SET", "foo", "bar"); err != nil {
				b.Fatal(err)
			}
			if _, err := redigo.String(red.Do("GET", "foo")); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkParallelGetSet(b *B) {
	const parallel = 100
	do := func(b *B, fn func()) {
		doCh := make(chan bool)
		wg := new(sync.WaitGroup)
		for i := 0; i < parallel; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range doCh {
					fn()
				}
			}()
		}
		for i := 0; i < b.N; i++ {
			doCh <- true
		}
		close(doCh)
		wg.Wait()
	}

	radix, err := NewPool("tcp", "127.0.0.1:6379", parallel, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("radix", func(b *B) {
		do(b, func() {
			err := radix.Do(WithConn("foo", func(conn Conn) error {
				if err := conn.Do(Cmd(nil, "SET", "foo", "bar")); err != nil {
					return err
				}
				var out string
				if err := conn.Do(Cmd(&out, "GET", "foo")); err != nil {
					return err
				}
				return nil
			}))
			if err != nil {
				b.Fatal(err)
			}
		})
	})

	red := &redigo.Pool{MaxIdle: parallel, Dial: func() (redigo.Conn, error) {
		return newRedigo(), nil
	}}
	b.Run("redigo", func(b *B) {
		do(b, func() {
			conn := red.Get()
			defer conn.Close()
			if _, err := conn.Do("SET", "foo", "bar"); err != nil {
				b.Fatal(err)
			}
			if _, err := redigo.String(conn.Do("GET", "foo")); err != nil {
				b.Fatal(err)
			}
		})
	})
}
