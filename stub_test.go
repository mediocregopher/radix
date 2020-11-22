package radix

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	. "testing"
	"time"

	"github.com/mediocregopher/radix/v4/resp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Watching the watchmen

func testStub() Conn {
	m := map[string]string{}
	return NewStubConn("tcp", "127.0.0.1:6379", func(_ context.Context, args []string) interface{} {
		switch args[0] {
		case "GET":
			return m[args[1]]
		case "SET":
			m[args[1]] = args[2]
			return nil
		case "ECHO":
			return args[1]
		default:
			return fmt.Errorf("testStub doesn't support command %q", args[0])
		}
	})
}

func TestStub(t *T) {
	ctx := testCtx(t)
	stub := testStub()

	{ // Basic test
		var foo string
		require.Nil(t, stub.Do(ctx, Cmd(nil, "SET", "foo", "a")))
		require.Nil(t, stub.Do(ctx, Cmd(&foo, "GET", "foo")))
		assert.Equal(t, "a", foo)
	}

	{ // Basic test with an int, to ensure marshalling/unmarshalling all works
		var foo int
		require.Nil(t, stub.Do(ctx, FlatCmd(nil, "SET", "foo", 1)))
		require.Nil(t, stub.Do(ctx, Cmd(&foo, "GET", "foo")))
		assert.Equal(t, 1, foo)
	}
}

func TestStubPipeline(t *T) {
	ctx := testCtx(t)
	stub := testStub()

	var out string
	p := NewPipeline()
	p.Append(Cmd(nil, "SET", "foo", "bar"))
	p.Append(Cmd(&out, "GET", "foo"))

	err := stub.Do(ctx, p)
	require.Nil(t, err)
	assert.Equal(t, "bar", out)
}

func TestStubLockingTimeout(t *T) {
	ctx := testCtx(t)
	stub := testStub()
	wg := new(sync.WaitGroup)
	c := 1000

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < c; i++ {
			m := Cmd(nil, "ECHO", strconv.Itoa(i)).(resp.Marshaler)
			require.Nil(t, stub.EncodeDecode(ctx, m, nil))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < c; i++ {
			var j int
			require.Nil(t, stub.EncodeDecode(ctx, nil, &j))
			assert.Equal(t, i, j)
		}
	}()

	wg.Wait()

	// test out timeout. do a write-then-read to ensure nothing bad happens
	// when there's actually data to read
	{
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		m := Cmd(nil, "ECHO", "1").(resp.Marshaler)
		assert.NoError(t, stub.EncodeDecode(ctx, m, new(int)))
		cancel()
	}

	// now there's no data to read, should return after 2-ish seconds with a
	// timeout error
	{
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := stub.EncodeDecode(ctx, nil, new(string))
		cancel()
		assert.Equal(t, context.DeadlineExceeded, err)
	}
}

func ExampleNewStubConn() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m := map[string]string{}
	stub := NewStubConn("tcp", "127.0.0.1:6379", func(_ context.Context, args []string) interface{} {
		switch args[0] {
		case "GET":
			return m[args[1]]
		case "SET":
			m[args[1]] = args[2]
			return nil
		default:
			return fmt.Errorf("this stub doesn't support command %q", args[0])
		}
	})

	stub.Do(ctx, Cmd(nil, "SET", "foo", "1"))

	var foo int
	stub.Do(ctx, Cmd(&foo, "GET", "foo"))
	fmt.Printf("foo: %d\n", foo)
}
