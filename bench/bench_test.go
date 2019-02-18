package bench

import (
	"context"
	"fmt"
	"runtime"
	. "testing"
	"time"

	redigo "github.com/gomodule/redigo/redis"
	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/redisconn"
	"github.com/mediocregopher/radix/v3"
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
		rad, err := radix.Dial("tcp", "127.0.0.1:6379")
		if err != nil {
			b.Fatal(err)
		}
		defer rad.Close()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := rad.Do(radix.Cmd(nil, "SET", "foo", "bar")); err != nil {
				b.Fatal(err)
			}
			var out string
			if err := rad.Do(radix.Cmd(&out, "GET", "foo")); err != nil {
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
	// parallel defines a multiplicand used for determining the number of goroutines
	// for running benchmarks. this value will be multiplied by GOMAXPROCS inside RunParallel.
	// since these benchmarks are mostly I/O bound and applications tend to have more
	// active goroutines accessing Redis than cores, especially with higher core numbers,
	// we set this to GOMAXPROCS so that we get GOMAXPROCS^2 connections.
	parallel := runtime.GOMAXPROCS(0)

	// multiply parallel with GOMAXPROCS to get the actual number of goroutines and thus
	// connections needed for the benchmarks.
	poolSize := parallel * runtime.GOMAXPROCS(0)

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
			pool, err := radix.NewPool("tcp", "127.0.0.1:6379", poolSize, radix.PoolPipelineWindow(0, 0))
			if err != nil {
				b.Fatal(err)
			}
			defer pool.Close()
			if err := waitFull(pool, poolSize); err != nil {
				b.Fatal(err)
			}

			do(b, func() {
				err := pool.Do(radix.WithConn("foo", func(conn radix.Conn) error {
					if err := conn.Do(radix.Cmd(nil, "SET", "foo", "bar")); err != nil {
						return err
					}
					var out string
					if err := conn.Do(radix.Cmd(&out, "GET", "foo")); err != nil {
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
			pool, err := radix.NewPool("tcp", "127.0.0.1:6379", poolSize, radix.PoolPipelineConcurrency(1))
			if err != nil {
				b.Fatal(err)
			}
			defer pool.Close()
			if err := waitFull(pool, poolSize); err != nil {
				b.Fatal(err)
			}

			do(b, func() {
				if err := pool.Do(radix.Cmd(nil, "SET", "foo", "bar")); err != nil {
					b.Fatal(err)
				}
				var out string
				if err := pool.Do(radix.Cmd(&out, "GET", "foo")); err != nil {
					b.Fatal(err)
				} else if out != "bar" {
					b.Fatal("got wrong value")
				}
			})
		})

		b.Run("default", func(b *B) {
			pool, err := radix.NewPool("tcp", "127.0.0.1:6379", poolSize, radix.PoolPipelineConcurrency(poolSize))
			if err != nil {
				b.Fatal(err)
			}
			defer pool.Close()
			if err := waitFull(pool, poolSize); err != nil {
				b.Fatal(err)
			}

			do(b, func() {
				if err := pool.Do(radix.Cmd(nil, "SET", "foo", "bar")); err != nil {
					b.Fatal(err)
				}
				var out string
				if err := pool.Do(radix.Cmd(&out, "GET", "foo")); err != nil {
					b.Fatal(err)
				} else if out != "bar" {
					b.Fatal("got wrong value")
				}
			})
		})
	})

	b.Run("redigo", func(b *B) {
		red := &redigo.Pool{MaxIdle: poolSize, Dial: func() (redigo.Conn, error) {
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
	for pool.MaxIdle > pool.ActiveCount() {
		conns = append(conns, pool.Get())
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
}

// waitFull blocks until pool.NumAvailConns reaches maxCons
func waitFull(pool *radix.Pool, maxCons int) error {
	timeout := time.After(10 * time.Second)
	for {
		select {
		case <-time.After(50 * time.Millisecond):
			if pool.NumAvailConns() == maxCons {
				return nil
			}
		case <-timeout:
			return fmt.Errorf("timeout waiting for pool to fill: reached %d, expected %d", pool.NumAvailConns(), maxCons)
		}
	}
}
