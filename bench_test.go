package radix

import (
	"bufio"
	"io"
	. "testing"

	redigo "github.com/garyburd/redigo/redis"
	"github.com/mediocregopher/radix.v2/resp"
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

type rawCmd struct {
	rcv interface{}
	cmd [][]byte
}

func newRawCmd(rcv interface{}, cmd ...[]byte) *rawCmd {
	return &rawCmd{rcv: rcv, cmd: cmd}
}

func (rc *rawCmd) MarshalRESP(w io.Writer) error {
	if err := (resp.ArrayHeader{N: len(rc.cmd)}).MarshalRESP(w); err != nil {
		return err
	}
	for i := range rc.cmd {
		if err := (resp.BulkString{B: rc.cmd[i]}).MarshalRESP(w); err != nil {
			return err
		}
	}
	return nil
}

func (rc *rawCmd) UnmarshalRESP(br *bufio.Reader) error {
	return resp.Any{I: rc.rcv}.UnmarshalRESP(br)
}

func (rc *rawCmd) Run(conn Conn) error {
	// TODO 50% of allocations are these Encode and Decode points, converting
	// the rawCmd into an inteface
	if err := conn.Encode(rc); err != nil {
		return err
	}
	return conn.Decode(rc)
}

func (rc *rawCmd) Key() []byte {
	return rc.cmd[1]
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

	b.Run("radix-raw", func(b *B) {
		for i := 0; i < b.N; i++ {
			if err := radix.Do(newRawCmd(nil, []byte("SET"), []byte("foo"), []byte("bar"))); err != nil {
				b.Fatal(err)
			}
			var out string
			if err := radix.Do(newRawCmd(&out, []byte("GET"), []byte("foo"))); err != nil {
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
