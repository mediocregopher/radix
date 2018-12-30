package radix

import (
	"runtime"
	. "testing"

	redigo "github.com/gomodule/redigo/redis"
)

func newRedigo() redigo.Conn {
	c, err := redigo.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		panic(err)
	}
	return c
}

func BenchmarkSerialGetSet(b *B) {
	radix, err := NewPool("tcp", "127.0.0.1:6379", 1)
	if err != nil {
		b.Fatal(err)
	}
	defer radix.Close()
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

	b.Run("radix_(pipelined)", func(b *B) {
		for i := 0; i < b.N; i++ {
			if err := radix.Do(Cmd(nil, "SET", "foo", "bar")); err != nil {
				b.Fatal(err)
			}
			var out string
			if err := radix.Do(Cmd(&out, "GET", "foo")); err != nil {
				b.Fatal(err)
			} else if out != "bar" {
				b.Fatal("got wrong value")
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
	parallel := runtime.GOMAXPROCS(0) * 8

	do := func(b *B, fn func()) {
		b.SetParallelism(parallel)
		b.RunParallel(func(pb *PB) {
			for pb.Next() {
				fn()
			}
		})
	}

	radix, err := NewPool("tcp", "127.0.0.1:6379", parallel)
	if err != nil {
		b.Fatal(err)
	}
	defer radix.Close()
	b.Run("radix", func(b *B) {
		do(b, func() {
			err := radix.Do(WithConn("foo", func(conn Conn) error {
				if err := conn.Do(Cmd(nil, "SET", "foo", "bar")); err != nil {
					return err
				}
				var out string
				if err := conn.Do(Cmd(&out, "GET", "foo")); err != nil {
					return err
				} else if out != "bar" {
					b.Fatal("got wrong value")
				}
				return nil
			}))
			if err != nil {
				b.Fatal(err)
			}
		})
	})

	b.Run("radix_(pipelined)", func(b *B) {
		do(b, func() {
			if err := radix.Do(Cmd(nil, "SET", "foo", "bar")); err != nil {
				b.Fatal(err)
			}
			var out string
			if err := radix.Do(Cmd(&out, "GET", "foo")); err != nil {
				b.Fatal(err)
			} else if out != "bar" {
				b.Fatal("got wrong value")
			}
		})
	})

	red := &redigo.Pool{MaxIdle: parallel, Dial: func() (redigo.Conn, error) {
		return newRedigo(), nil
	}}
	defer red.Close()
	b.Run("redigo", func(b *B) {
		do(b, func() {
			conn := red.Get()
			defer conn.Close()
			if _, err := conn.Do("SET", "foo", "bar"); err != nil {
				b.Fatal(err)
			}
			if out, err := redigo.String(conn.Do("GET", "foo")); err != nil {
				b.Fatal(err)
			} else if out != "bar" {
				b.Fatal("got wrong value")
			}
		})
	})
}
