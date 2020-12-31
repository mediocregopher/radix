package bench

import (
	"context"
	"os"
	"runtime"
	"strings"
	. "testing"
	"time"

	goredis "github.com/go-redis/redis"
	redigo "github.com/gomodule/redigo/redis"
	redispipe "github.com/joomcode/redispipe/redis"
	redispipeconn "github.com/joomcode/redispipe/redisconn"
	radixv3 "github.com/mediocregopher/radix/v3"
	"github.com/mediocregopher/radix/v4"
	"github.com/mediocregopher/radix/v4/trace"
)

func getEnv(varName, defaultVal string) string {
	if v := os.Getenv(varName); v != "" {
		return v
	}
	return defaultVal
}

var addr = getEnv("REDIS_ADDR", "127.0.0.1:6379")

func newRedisPipe(writePause time.Duration) redispipe.Sync {
	pipe, err := redispipeconn.Connect(context.Background(), addr, redispipeconn.Opts{
		Logger:     redispipeconn.NoopLogger{},
		WritePause: writePause,
	})
	if err != nil {
		panic(err)
	}
	return redispipe.Sync{S: pipe}
}

func radixV4SetGet(ctx context.Context, client radix.Client, key, val string) error {
	if err := client.Do(ctx, radix.Cmd(nil, "SET", key, val)); err != nil {
		return err
	}
	var out string
	return client.Do(ctx, radix.Cmd(&out, "GET", key))
}

func radixV4Pipeline(ctx context.Context, client radix.Client, p *radix.Pipeline, key, val string) error {
	var out string
	if p == nil {
		p = radix.NewPipeline()
	} else {
		p.Reset()
	}
	p.Append(radix.Cmd(nil, "SET", key, val))
	p.Append(radix.Cmd(&out, "GET", key))
	return client.Do(ctx, p)
}

func radixV3SetGet(client radixv3.Client, key, val string) error {
	if err := client.Do(radixv3.Cmd(nil, "SET", key, val)); err != nil {
		return err
	}
	var out string
	return client.Do(radixv3.Cmd(&out, "GET", key))
}

func radixV3Pipeline(client radixv3.Client, key, val string) error {
	var out string
	return client.Do(radixv3.Pipeline(
		radixv3.Cmd(nil, "SET", key, val),
		radixv3.Cmd(&out, "GET", key),
	))
}

func redigoSetGet(conn redigo.Conn, key, val string) error {
	if _, err := conn.Do("SET", key, val); err != nil {
		return err
	} else if _, err := redigo.String(conn.Do("GET", key)); err != nil {
		return err
	}
	return nil
}

func redigoPipeline(conn redigo.Conn, key, val string) error {
	if err := conn.Send("SET", key, val); err != nil {
		return err
	} else if _, err := redigo.String(conn.Do("GET", key)); err != nil {
		return err
	}
	return nil
}

func redisPipeSetGet(sync redispipe.Sync, key, val string) error {
	if err := redispipe.AsError(sync.Do("SET", key, val)); err != nil {
		return err
	} else if err := redispipe.AsError(sync.Do("GET", key)); err != nil {
		return err
	}
	return nil
}

func redisPipePipeline(sync redispipe.Sync, key, val string) error {
	ress := sync.SendMany([]redispipe.Request{
		redispipe.Req("SET", key, val),
		redispipe.Req("GET", key),
	})
	for _, i := range ress {
		if err, ok := i.(error); ok && err != nil {
			return err
		}
	}
	return nil
}

func goRedisSetGet(client *goredis.Client, key, val string) error {
	if err := client.Set(key, val, 0).Err(); err != nil {
		return err
	} else if err := client.Get(key).Err(); err != nil {
		return err
	}
	return nil
}

func goRedisPipeline(ctx context.Context, client *goredis.Client, key, val string) error {
	pipe := client.Pipeline()
	pipe.Set(key, val, 0)
	pipe.Get(key)
	_, err := pipe.Exec()
	return err
}

func BenchmarkDrivers(b *B) {
	type testParams struct {
		key, val    string
		pipeline    bool
		loop        func(func() error)
		parallelism int
	}

	type test struct {
		name string
		run  func(*B, testParams)
	}

	tests := []test{
		{"radixv4", func(b *B, params testParams) {
			ctx := context.Background()
			var client radix.Client

			if params.parallelism > 0 {
				initDoneCh := make(chan struct{})
				pool, err := radix.PoolConfig{
					Size: params.parallelism,
					Dialer: radix.Dialer{
						WriteFlushInterval: 0, // TODO
					},
					Trace: trace.PoolTrace{
						InitCompleted: func(trace.PoolInitCompleted) { close(initDoneCh) },
					},
				}.New(ctx, "tcp", addr)
				if err != nil {
					b.Fatal(err)
				}
				defer pool.Close()

				// wait for the pool to fill up
				<-initDoneCh
				client = pool
			} else {
				conn, err := radix.Dial(ctx, "tcp", addr)
				if err != nil {
					b.Fatal(err)
				}
				defer conn.Close()
				client = conn
			}

			if params.pipeline {
				var p *radix.Pipeline
				if params.parallelism == 0 {
					p = radix.NewPipeline()
				}
				params.loop(func() error {
					return radixV4Pipeline(ctx, client, p, params.key, params.val)
				})
			} else {
				params.loop(func() error {
					return radixV4SetGet(ctx, client, params.key, params.val)
				})
			}
		}},
		{"radixv3", func(b *B, params testParams) {
			var client radixv3.Client
			if params.parallelism > 0 {
				pool, err := radixv3.NewPool("tcp", addr, params.parallelism)
				if err != nil {
					b.Fatal(err)
				}
				defer pool.Close()

				// wait for the pool to fill up
				for {
					time.Sleep(50 * time.Millisecond)
					if pool.NumAvailConns() >= params.parallelism {
						break
					}
				}
				client = pool
			} else {
				conn, err := radixv3.Dial("tcp", addr)
				if err != nil {
					b.Fatal(err)
				}
				defer conn.Close()
				client = conn
			}

			if params.pipeline {
				params.loop(func() error {
					return radixV3Pipeline(client, params.key, params.val)
				})
			} else {
				params.loop(func() error {
					return radixV3SetGet(client, params.key, params.val)
				})
			}
		}},
		{"redigo", func(b *B, params testParams) {
			var getConn func() redigo.Conn
			var closeConn func(redigo.Conn)

			if params.parallelism > 0 {
				pool := &redigo.Pool{
					MaxIdle: params.parallelism,
					Dial: func() (redigo.Conn, error) {
						return redigo.Dial("tcp", addr)
					},
				}
				defer pool.Close()

				{ // make sure the pool is full
					var conns []redigo.Conn
					for pool.MaxIdle > pool.ActiveCount() {
						conns = append(conns, pool.Get())
					}
					for _, conn := range conns {
						_ = conn.Close()
					}
				}

				getConn = pool.Get
				closeConn = func(conn redigo.Conn) { conn.Close() }

			} else {
				conn, err := redigo.Dial("tcp", addr)
				if err != nil {
					b.Fatal(err)
				}
				defer conn.Close()
				getConn = func() redigo.Conn { return conn }
				closeConn = func(redigo.Conn) {}
			}

			if params.pipeline {
				params.loop(func() error {
					conn := getConn()
					defer closeConn(conn)
					return redigoPipeline(conn, params.key, params.val)
				})
			} else {
				params.loop(func() error {
					conn := getConn()
					defer closeConn(conn)
					return redigoSetGet(conn, params.key, params.val)
				})
			}
		}},
		{"redispipe_pause150us", func(b *B, params testParams) {
			sync := newRedisPipe(150 * time.Microsecond)
			defer sync.S.Close()
			if params.pipeline {
				params.loop(func() error {
					return redisPipePipeline(sync, params.key, params.val)
				})
			} else {
				params.loop(func() error {
					return redisPipeSetGet(sync, params.key, params.val)
				})
			}
		}},
		{"redispipe_pause0", func(b *B, params testParams) {
			sync := newRedisPipe(-1)
			defer sync.S.Close()
			if params.pipeline {
				params.loop(func() error {
					return redisPipePipeline(sync, params.key, params.val)
				})
			} else {
				params.loop(func() error {
					return redisPipeSetGet(sync, params.key, params.val)
				})
			}
		}},
		{"go-redis", func(b *B, params testParams) {
			// this creates a connection pool even if parallelism == 0, whereas
			// other packages are using only a single connection, so this might
			// not be a totally fair comparison.  If someone who understands the
			// go-redis API is reading this, please submit an MR.
			poolSize := 1
			if params.parallelism > 0 {
				poolSize = params.parallelism
			}

			c := goredis.NewClient(&goredis.Options{
				Addr:         addr,
				PoolSize:     poolSize,
				MinIdleConns: poolSize,
			})
			defer c.Close()

			if poolSize > 1 {
				for {
					time.Sleep(50 * time.Millisecond)
					if c.PoolStats().IdleConns >= (uint32)(poolSize) {
						break
					}
				}
			}

			if params.pipeline {
				ctx := context.Background()
				params.loop(func() error {
					return goRedisPipeline(ctx, c, params.key, params.val)
				})
			} else {
				params.loop(func() error {
					return goRedisSetGet(c, params.key, params.val)
				})
			}
		}},
	}

	runTests := func(b *B, params testParams) {
		for _, test := range tests {
			b.Run(test.name, func(b *B) {
				if params.parallelism > 0 {
					params.loop = func(cb func() error) {
						b.SetParallelism(params.parallelism)
						b.ResetTimer()
						b.RunParallel(func(pb *PB) {
							for pb.Next() {
								if err := cb(); err != nil {
									b.Fatal(err)
								}
							}
						})
					}
				} else {
					params.loop = func(cb func() error) {
						for i := 0; i < b.N; i++ {
							if err := cb(); err != nil {
								b.Fatal(err)
							}
						}
					}
				}
				test.run(b, params)
			})
		}
	}

	runKVSizes := func(b *B, params testParams) {
		tests := [][3]string{
			{"small kv", "foo", "bar"},
			{"large kv", strings.Repeat("foo", 24), strings.Repeat("bar", 4096)},
		}
		for _, test := range tests {
			b.Run(test[0], func(b *B) {
				params := params
				params.key, params.val = test[1], test[2]
				runTests(b, params)
			})
		}
	}

	runPipeline := func(b *B, params testParams) {
		b.Run("no_pipeline", func(b *B) { runKVSizes(b, params) })
		b.Run("pipeline", func(b *B) {
			params := params
			params.pipeline = true
			runKVSizes(b, params)
		})
	}

	runParallelism := func(b *B, params testParams) {
		b.Run("serial", func(b *B) { runPipeline(b, params) })
		b.Run("parallel", func(b *B) {
			params := params
			// parallelism defines a multiplicand used for determining the
			// number of goroutines for running benchmarks. this value will be
			// multiplied by GOMAXPROCS inside RunParallel. Since these
			// benchmarks are mostly I/O bound and applications tend to have
			// more active goroutines accessing Redis than cores, especially
			// with higher core numbers, we set this to GOMAXPROCS so that we
			// get GOMAXPROCS^2 connections.
			params.parallelism = runtime.GOMAXPROCS(0)
			runPipeline(b, params)
		})
	}

	runParallelism(b, testParams{})
}
