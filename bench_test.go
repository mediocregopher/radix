package radix

import (
	"context"
	"runtime"
	. "testing"
	"time"

	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/redisconn"

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
	b.Run("radix", func(b *B) {
		radix, err := Dial("tcp", "127.0.0.1:6379")
		if err != nil {
			b.Fatal(err)
		}
		defer radix.Close()
		b.ResetTimer()
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

	b.Run("redigo", func(b *B) {
		red := newRedigo()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := red.Do("SET", "foo", "bar"); err != nil {
				b.Fatal(err)
			}
			if _, err := redigo.String(red.Do("GET", "foo")); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("redispipe", func(b *B) {
		pipe, err := redisconn.Connect(context.Background(), "127.0.0.1:6379", redisconn.Opts{
			Logger:     redisconn.NoopLogger{},
			WritePause: 150 * time.Microsecond,
		})
		defer pipe.Close()
		if err != nil {
			b.Fatal(err)
		}
		sync := redis.Sync{pipe}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if res := sync.Do("SET", "foo", "bar"); redis.AsError(res) != nil {
				b.Fatal(res)
			}
			if res := sync.Do("GET", "foo"); redis.AsError(res) != nil {
				b.Fatal(res)
			}
		}
	})

	b.Run("redispipe_pause0", func(b *B) {
		pipe, err := redisconn.Connect(context.Background(), "127.0.0.1:6379", redisconn.Opts{
			Logger:     redisconn.NoopLogger{},
			WritePause: -1,
		})
		defer pipe.Close()
		if err != nil {
			b.Fatal(err)
		}
		sync := redis.Sync{pipe}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if res := sync.Do("SET", "foo", "bar"); redis.AsError(res) != nil {
				b.Fatal(res)
			}
			if res := sync.Do("GET", "foo"); redis.AsError(res) != nil {
				b.Fatal(res)
			}
		}
	})
}

func BenchmarkParallelGetSet(b *B) {
	parallel := runtime.GOMAXPROCS(0) * 8

	do := func(b *B, fn func()) {
		b.ResetTimer()
		b.SetParallelism(parallel)
		b.RunParallel(func(pb *PB) {
			for pb.Next() {
				fn()
			}
		})
	}

	b.Run("radix", func(b *B) {
		b.Run("no pipelining", func(b *B) {
			radix, err := NewPool("tcp", "127.0.0.1:6379", parallel, PoolPipelineWindow(0, 0))
			if err != nil {
				b.Fatal(err)
			}
			defer radix.Close()
			<-radix.initDone

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

		b.Run("one pipeline", func(b *B) {
			radix, err := NewPool("tcp", "127.0.0.1:6379", parallel, PoolPipelineConcurrency(1))
			if err != nil {
				b.Fatal(err)
			}
			defer radix.Close()
			<-radix.initDone

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

		b.Run("default", func(b *B) {
			radix, err := NewPool("tcp", "127.0.0.1:6379", parallel, PoolPipelineConcurrency(parallel))
			if err != nil {
				b.Fatal(err)
			}
			defer radix.Close()
			<-radix.initDone

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
	})

	b.Run("redigo", func(b *B) {
		red := &redigo.Pool{MaxIdle: parallel, Dial: func() (redigo.Conn, error) {
			return newRedigo(), nil
		}}
		defer red.Close()
		fillRedigoPool(red)

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

	b.Run("redispipe", func(b *B) {
		pipe, err := redisconn.Connect(context.Background(), "127.0.0.1:6379", redisconn.Opts{
			Logger:     redisconn.NoopLogger{},
			WritePause: 150 * time.Microsecond,
		})
		if err != nil {
			b.Fatal(err)
		}
		defer pipe.Close()
		sync := redis.Sync{pipe}

		do(b, func() {
			if res := sync.Do("SET", "foo", "bar"); redis.AsError(res) != nil {
				b.Fatal(res)
			}
			if res := sync.Do("GET", "foo"); redis.AsError(res) != nil {
				b.Fatal(err)
			}
		})
	})
}

func fillRedigoPool(pool *redigo.Pool) {
	var conns []redigo.Conn
	for pool.MaxActive > pool.ActiveCount() {
		conns = append(conns, pool.Get())
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
}
