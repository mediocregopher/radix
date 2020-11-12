package bench

import (
	"context"
	"runtime"
	"strings"
	. "testing"
	"time"

	errors "golang.org/x/xerrors"

	redigo "github.com/gomodule/redigo/redis"
	redispipe "github.com/joomcode/redispipe/redis"
	redispipeconn "github.com/joomcode/redispipe/redisconn"
	"github.com/mediocregopher/radix/v3"
)

func newRedigo() redigo.Conn {
	c, err := redigo.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		panic(err)
	}
	return c
}

func newRedisPipe(writePause time.Duration) redispipe.Sync {
	pipe, err := redispipeconn.Connect(context.Background(), "127.0.0.1:6379", redispipeconn.Opts{
		Logger:     redispipeconn.NoopLogger{},
		WritePause: writePause,
	})
	if err != nil {
		panic(err)
	}
	return redispipe.Sync{S: pipe}
}

func radixGetSet(client radix.Client, key, val string) error {
	if err := client.Do(radix.Cmd(nil, "SET", key, val)); err != nil {
		return err
	}
	var out string
	if err := client.Do(radix.Cmd(&out, "GET", key)); err != nil {
		return err
	} else if out != val {
		return errors.New("got wrong value")
	}
	return nil
}

func BenchmarkSerialGetSet(b *B) {
	b.Run("radix", func(b *B) {
		rad, err := radix.Dial("tcp", "127.0.0.1:6379")
		if err != nil {
			b.Fatal(err)
		}
		defer rad.Close()
		// avoid overhead of converting from radix.Conn to radix.Client on each loop iteration
		client := radix.Client(rad)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := radixGetSet(client, "foo", "bar"); err != nil {
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
		sync := newRedisPipe(150 * time.Microsecond)
		defer sync.S.Close()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if res := sync.Do("SET", "foo", "bar"); redispipe.AsError(res) != nil {
				b.Fatal(res)
			} else if res := sync.Do("GET", "foo"); redispipe.AsError(res) != nil {
				b.Fatal(res)
			}
		}
	})

	b.Run("redispipe_pause0", func(b *B) {
		sync := newRedisPipe(-1)
		defer sync.S.Close()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if res := sync.Do("SET", "foo", "bar"); redispipe.AsError(res) != nil {
				b.Fatal(res)
			}
			if res := sync.Do("GET", "foo"); redispipe.AsError(res) != nil {
				b.Fatal(res)
			}
		}
	})
}

func BenchmarkSerialGetSetLargeArgs(b *B) {
	key := strings.Repeat("foo", 24)
	val := strings.Repeat("bar", 4096)

	b.Run("radix", func(b *B) {
		rad, err := radix.Dial("tcp", "127.0.0.1:6379")
		if err != nil {
			b.Fatal(err)
		}
		defer rad.Close()
		// avoid overhead of converting from radix.Conn to radix.Client on each loop iteration
		client := radix.Client(rad)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := radixGetSet(client, key, val); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("redigo", func(b *B) {
		red := newRedigo()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := red.Do("SET", key, val); err != nil {
				b.Fatal(err)
			}
			if _, err := redigo.String(red.Do("GET", key)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("redispipe", func(b *B) {
		sync := newRedisPipe(150 * time.Microsecond)
		defer sync.S.Close()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if res := sync.Do("SET", key, val); redispipe.AsError(res) != nil {
				b.Fatal(res)
			}
			if res := sync.Do("GET", key); redispipe.AsError(res) != nil {
				b.Fatal(res)
			}
		}
	})

	b.Run("redispipe_pause0", func(b *B) {
		sync := newRedisPipe(-1)
		defer sync.S.Close()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if res := sync.Do("SET", key, val); redispipe.AsError(res) != nil {
				b.Fatal(res)
			}
			if res := sync.Do("GET", key); redispipe.AsError(res) != nil {
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

	do := func(b *B, fn func() error) {
		b.ResetTimer()
		b.SetParallelism(parallel)
		b.RunParallel(func(pb *PB) {
			for pb.Next() {
				if err := fn(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}

	b.Run("radix", func(b *B) {
		mkRadixBench := func(opts ...radix.PoolOpt) func(b *B) {
			return func(b *B) {
				pool, err := radix.NewPool("tcp", "127.0.0.1:6379", poolSize, opts...)
				if err != nil {
					b.Fatal(err)
				}
				defer pool.Close()

				// wait for the pool to fill up
				for {
					time.Sleep(50 * time.Millisecond)
					if pool.NumAvailConns() >= poolSize {
						break
					}
				}

				// avoid overhead of boxing the pool on each loop iteration
				client := radix.Client(pool)
				b.ResetTimer()
				do(b, func() error {
					return radixGetSet(client, "foo", "bar")
				})
			}
		}

		b.Run("no pipeline", mkRadixBench(radix.PoolPipelineWindow(0, 0)))
		b.Run("one pipeline", mkRadixBench(radix.PoolPipelineConcurrency(1)))
		b.Run("default", mkRadixBench())
	})

	b.Run("redigo", func(b *B) {
		red := &redigo.Pool{MaxIdle: poolSize, Dial: func() (redigo.Conn, error) {
			return newRedigo(), nil
		}}
		defer red.Close()

		{ // make sure the pool is full
			var conns []redigo.Conn
			for red.MaxIdle > red.ActiveCount() {
				conns = append(conns, red.Get())
			}
			for _, conn := range conns {
				_ = conn.Close()
			}
		}

		do(b, func() error {
			conn := red.Get()
			if _, err := conn.Do("SET", "foo", "bar"); err != nil {
				conn.Close()
				return err
			}
			if out, err := redigo.String(conn.Do("GET", "foo")); err != nil {
				conn.Close()
				return err
			} else if out != "bar" {
				conn.Close()
				return errors.New("got wrong value")
			}
			return conn.Close()
		})
	})

	b.Run("redispipe", func(b *B) {
		sync := newRedisPipe(150 * time.Microsecond)
		defer sync.S.Close()
		do(b, func() error {
			if res := sync.Do("SET", "foo", "bar"); redispipe.AsError(res) != nil {
				return redispipe.AsError(res)
			} else if res := sync.Do("GET", "foo"); redispipe.AsError(res) != nil {
				return redispipe.AsError(res)
			}
			return nil
		})
	})
}
