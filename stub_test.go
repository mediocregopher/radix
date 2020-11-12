package radix

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	. "testing"
	"time"

	errors "golang.org/x/xerrors"

	"github.com/mediocregopher/radix/v3/resp/resp2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Watching the watchmen

func testStub() Conn {
	m := map[string]string{}
	return Stub("tcp", "127.0.0.1:6379", func(args []string) interface{} {
		switch args[0] {
		case "GET":
			return m[args[1]]
		case "SET":
			m[args[1]] = args[2]
			return nil
		case "ECHO":
			return args[1]
		default:
			return errors.Errorf("testStub doesn't support command %q", args[0])
		}
	})
}

func TestStub(t *T) {
	stub := testStub()

	{ // Basic test
		var foo string
		require.Nil(t, stub.Do(Cmd(nil, "SET", "foo", "a")))
		require.Nil(t, stub.Do(Cmd(&foo, "GET", "foo")))
		assert.Equal(t, "a", foo)
	}

	{ // Basic test with an int, to ensure marshalling/unmarshalling all works
		var foo int
		require.Nil(t, stub.Do(FlatCmd(nil, "SET", "foo", 1)))
		require.Nil(t, stub.Do(Cmd(&foo, "GET", "foo")))
		assert.Equal(t, 1, foo)
	}
}

func TestStubPipeline(t *T) {
	stub := testStub()
	var out string
	err := stub.Do(Pipeline(
		Cmd(nil, "SET", "foo", "bar"),
		Cmd(&out, "GET", "foo"),
	))

	require.Nil(t, err)
	assert.Equal(t, "bar", out)
}

func TestStubLockingTimeout(t *T) {
	stub := testStub()
	wg := new(sync.WaitGroup)
	c := 1000

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < c; i++ {
			require.Nil(t, stub.Encode(Cmd(nil, "ECHO", strconv.Itoa(i))))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < c; i++ {
			var j int
			require.Nil(t, stub.Decode(resp2.Any{I: &j}))
			assert.Equal(t, i, j)
		}
	}()

	wg.Wait()

	// test out timeout. do a write-then-read to ensure nothing bad happens
	// when there's actually data to read
	now := time.Now()
	conn := stub.NetConn()
	conn.SetDeadline(now.Add(2 * time.Second))
	require.Nil(t, stub.Encode(Cmd(nil, "ECHO", "1")))
	require.Nil(t, stub.Decode(resp2.Any{}))

	// now there's no data to read, should return after 2-ish seconds with a
	// timeout error
	err := stub.Decode(resp2.Any{})
	nerr, ok := err.(*net.OpError)
	assert.True(t, ok)
	assert.True(t, nerr.Timeout())
}

func ExampleStub() {
	m := map[string]string{}
	stub := Stub("tcp", "127.0.0.1:6379", func(args []string) interface{} {
		switch args[0] {
		case "GET":
			return m[args[1]]
		case "SET":
			m[args[1]] = args[2]
			return nil
		default:
			return errors.Errorf("this stub doesn't support command %q", args[0])
		}
	})

	stub.Do(Cmd(nil, "SET", "foo", "1"))

	var foo int
	stub.Do(Cmd(&foo, "GET", "foo"))
	fmt.Printf("foo: %d\n", foo)
}
