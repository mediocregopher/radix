package radix

import (
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
