package radix

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	. "testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mediocregopher/radix/v4/resp"
	"github.com/mediocregopher/radix/v4/resp/resp3"
)

func TestCmdAction(t *T) {
	ctx := testCtx(t)
	c := dial()
	key, val := randStr(), randStr()

	require.Nil(t, c.Do(ctx, Cmd(nil, "SET", key, val)))
	var got string
	require.Nil(t, c.Do(ctx, Cmd(&got, "GET", key)))
	assert.Equal(t, val, got)

	// because BITOP is weird
	require.Nil(t, c.Do(ctx, Cmd(nil, "SET", key, val)))
	bitopCmd := Cmd(nil, "BITOP", "AND", key+key, key, key)
	assert.Equal(t, []string{key + key, key, key}, bitopCmd.Properties().Keys)
	require.Nil(t, c.Do(ctx, bitopCmd))
	var dstval string
	require.Nil(t, c.Do(ctx, Cmd(&dstval, "GET", key+key)))
	assert.Equal(t, val, dstval)
}

func TestCmdActionStreams(t *T) {
	ctx := testCtx(t)
	c := dial()
	key, val := randStr(), randStr()

	// talking about weird commands, here are XREAD and XREADGROUP
	group := randStr()
	consumer := randStr()
	skeys := []string{"1", "1-", "1-1", "s1", ""} // make sure that we can handle empty and ID-like keys
	for _, skey := range skeys {
		require.Nil(t, c.Do(ctx, Cmd(nil, "DEL", skey)))
		require.NoError(t, c.Do(ctx, Cmd(nil, "XADD", skey, "1-1", key, val)))
		require.NoError(t, c.Do(ctx, Cmd(nil, "XGROUP", "CREATE", skey, group, "0-0")))

		// so many possible arguments, so many tests...
		for _, args := range [][]string{
			{"XREAD", "STREAMS", skey, "0"},
			{"XREAD", "BLOCK", "1", "STREAMS", skey, "0"},
			{"XREAD", "COUNT", "1", "STREAMS", skey, "0"},
			{"XREAD", "COUNT", "1", "BLOCK", "1", "STREAMS", skey, "0"},
			{"XREADGROUP", "GROUP", group, consumer, "STREAMS", skey, ">"},
			{"XREADGROUP", "GROUP", group, consumer, "BLOCK", "1", "STREAMS", skey, ">"},
			{"XREADGROUP", "GROUP", group, consumer, "COUNT", "1", "STREAMS", skey, ">"},
			{"XREADGROUP", "GROUP", group, consumer, "COUNT", "1", "BLOCK", "1", "STREAMS", skey, ">"},
			{"XINFO", "CONSUMERS", skey, group},
			{"XINFO", "GROUPS", skey},
			{"XINFO", "STREAM", skey},
			{"XGROUP", "DELCONSUMER", skey, group, consumer},
			{"XGROUP", "DESTROY", skey, group},
			{"XGROUP", "CREATE", skey, group, "$"},
			{"XGROUP", "SETID", skey, group, "$"},
		} {
			xCmd := Cmd(nil, args[0], args[1:]...)
			assert.Equal(t, []string{skey}, xCmd.Properties().Keys)
			require.NoError(t, c.Do(ctx, xCmd))
		}
	}

	// test that we correctly handle multiple keys. we just use 3 keys since it's shorter
	for _, args := range [][]string{
		{"XREAD", "STREAMS", skeys[0], skeys[1], skeys[2], "0", "0", "0"},
		{"XREADGROUP", "GROUP", group, "test", "STREAMS", skeys[0], skeys[1], skeys[2], "0", ">", "0"},
	} {
		xCmd := Cmd(nil, args[0], args[1:]...)
		assert.Equal(t, skeys[:3], xCmd.Properties().Keys)
		require.NoError(t, c.Do(ctx, xCmd))
	}

	xCmd := Cmd(nil, "XINFO", "HELP")
	assert.Equal(t, []string(nil), xCmd.Properties().Keys)
	require.NoError(t, c.Do(ctx, xCmd))
}

func ExampleCmd() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := (PoolConfig{}).New(ctx, "tcp", "127.0.0.1:6379") // or any other client
	if err != nil {
		panic(err)
	}

	if err := client.Do(ctx, Cmd(nil, "SET", "foo", "bar")); err != nil {
		panic(err)
	}

	var fooVal string
	if err := client.Do(ctx, Cmd(&fooVal, "GET", "foo")); err != nil {
		panic(err)
	}
	fmt.Println(fooVal)
	// Output: bar
}

func TestFlatCmdAction(t *T) {
	ctx := testCtx(t)
	c := dial()
	key := randStr()
	m := map[string]string{
		randStr(): randStr(),
		randStr(): randStr(),
		randStr(): randStr(),
	}
	require.Nil(t, c.Do(ctx, FlatCmd(nil, "HMSET", key, m)))

	var got map[string]string
	require.Nil(t, c.Do(ctx, FlatCmd(&got, "HGETALL", key)))
	assert.Equal(t, m, got)
}

func TestFlatCmdActionNil(t *T) {
	ctx := testCtx(t)
	c := dial()
	defer c.Close()

	key := randStr()
	hashKey := randStr()

	assert.NoError(t, c.Do(ctx, FlatCmd(nil, "HMSET", key, hashKey, nil)))

	var mb Maybe
	assert.NoError(t, c.Do(ctx, Cmd(&mb, "HGET", key, hashKey)))
	assert.False(t, mb.Null)
	assert.False(t, mb.Empty)
}

func TestFlatCmdActionEmpty(t *T) {
	ctx := testCtx(t)
	c := dial()
	defer c.Close()

	key := randStr()

	var mb Maybe
	assert.NoError(t, c.Do(ctx, Cmd(&mb, "HGETALL", key)))
	assert.False(t, mb.Null)
	assert.True(t, mb.Empty)
}

func ExampleFlatCmd() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := (PoolConfig{}).New(ctx, "tcp", "127.0.0.1:6379") // or any other client
	if err != nil {
		panic(err)
	}

	// performs "SET" "foo" "1"
	err = client.Do(ctx, FlatCmd(nil, "SET", "foo", 1))
	if err != nil {
		panic(err)
	}

	// performs "SADD" "fooSet" "1" "2" "3"
	err = client.Do(ctx, FlatCmd(nil, "SADD", "fooSet", []string{"1", "2", "3"}))
	if err != nil {
		panic(err)
	}

	// performs "HMSET" "foohash" "a" "1" "b" "2" "c" "3"
	m := map[string]int{"a": 1, "b": 2, "c": 3}
	err = client.Do(ctx, FlatCmd(nil, "HMSET", "fooHash", m))
	if err != nil {
		panic(err)
	}
}

func TestEvalAction(t *T) {
	ctx := testCtx(t)
	getSet := NewEvalScript(`
		local prev = redis.call("GET", KEYS[1])
		redis.call("SET", KEYS[1], ARGV[1])
		return prev
		-- ` + randStr() /* so there's an eval everytime */ + `
	`)

	c := dial()
	key := randStr()
	val1, val2, val3 := randStr(), randStr(), randStr()

	{
		var res string
		err := c.Do(ctx, getSet.Cmd(&res, []string{key}, val1))
		require.Nil(t, err, "%s", err)
		assert.Empty(t, res)
	}

	{
		var res string
		err := c.Do(ctx, getSet.Cmd(&res, []string{key}, val2))
		require.Nil(t, err)
		assert.Equal(t, val1, res)
	}

	{
		var res string
		err := c.Do(ctx, getSet.FlatCmd(&res, []string{key}, val3))
		require.Nil(t, err)
		assert.Equal(t, val2, res)
	}
}

func TestEvalActionWithInterfaceRcv(t *T) {
	simpleScript := NewEvalScript(`
		return 123
		-- ` + randStr() /* so there's an eval everytime */ + `
	`)

	c := dial()
	{
		var res interface{}
		err := c.Do(testCtx(t), simpleScript.Cmd(&res, nil))
		require.Nil(t, err)
		assert.Equal(t, int64(123), res)
	}
}

func ExampleEvalScript() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// set as a global variable, this script is equivalent to the builtin GETSET
	// redis command
	var getSet = NewEvalScript(`
		local prev = redis.call("GET", KEYS[1])
		redis.call("SET", KEYS[1], ARGV[1])
		return prev
`)

	client, err := (PoolConfig{}).New(ctx, "tcp", "127.0.0.1:6379") // or any other client
	if err != nil {
		// handle error
	}

	key := "someKey"
	var prevVal string
	if err := client.Do(ctx, getSet.Cmd(&prevVal, []string{key}, "myVal")); err != nil {
		// handle error
	}

	fmt.Printf("value of key %q used to be %q\n", key, prevVal)
}

func TestPipelineAction(t *T) {
	t.Run("basic", func(t *T) {
		ctx := testCtx(t)
		c := dial()
		for i := 0; i < 10; i++ {
			ss := []string{
				randStr(),
				randStr(),
				randStr(),
			}
			out := make([]string, len(ss))
			p := NewPipeline()
			for i := range ss {
				p.Append(Cmd(&out[i], "ECHO", ss[i]))
			}
			require.Nil(t, c.Do(ctx, p))

			for i := range ss {
				assert.Equal(t, ss[i], out[i])
			}
		}
	})

	t.Run("errConnUsable", func(t *T) {
		ctx := testCtx(t)
		c := dial()

		// Setup
		k1 := randStr()
		k2 := randStr()
		kvs := map[string]string{
			k1: "foo",
			k2: "bar",
		}

		for k, v := range kvs {
			require.NoError(t, c.Do(ctx, Cmd(nil, "SET", k, v)))
		}

		var intRcv int
		var strRcv string
		pipeline := NewPipeline()
		pipeline.Append(Cmd(&intRcv, "GET", k1)) // unmarshal error
		pipeline.Append(Cmd(nil, "GET", k2))
		pipeline.Append(Cmd(&strRcv, "GET", k2))

		err := c.Do(ctx, pipeline)
		require.Error(t, err)
		assert.Zero(t, intRcv)
		assert.Equal(t, kvs[k2], strRcv)

		// make sure the connection is still usable
		err = c.Do(ctx, Cmd(&strRcv, "ECHO", k1))
		require.NoError(t, err)
		assert.Equal(t, k1, strRcv)
	})
}

func ExamplePipeline() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := (PoolConfig{}).New(ctx, "tcp", "127.0.0.1:6379") // or any other client
	if err != nil {
		// handle error
	}

	var fooVal string
	p := NewPipeline()
	p.Append(FlatCmd(nil, "SET", "foo", 1))
	p.Append(Cmd(&fooVal, "GET", "foo"))

	if err := client.Do(ctx, p); err != nil {
		// handle error
	}
	fmt.Printf("fooVal: %q\n", fooVal)

	// At this point the Pipeline cannot be used again, unless Reset is called.
	var barVal string
	p.Reset()
	p.Append(FlatCmd(nil, "SET", "bar", 2))
	p.Append(Cmd(&barVal, "GET", "bar"))

	if err := client.Do(ctx, p); err != nil {
		// handle error
	}
	fmt.Printf("barVal: %q\n", barVal)

	// Output: fooVal: "1"
	// barVal: "2"
}

func TestWithConnAction(t *T) {
	ctx := testCtx(t)
	c := dial()
	k, v := randStr(), 10

	err := c.Do(ctx, WithConn(k, func(ctx context.Context, conn Conn) error {
		require.Nil(t, conn.Do(ctx, FlatCmd(nil, "SET", k, v)))
		var out int
		require.Nil(t, conn.Do(ctx, Cmd(&out, "GET", k)))
		assert.Equal(t, v, out)
		return nil
	}))
	require.Nil(t, err)
}

func ExampleWithConn() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := (PoolConfig{}).New(ctx, "tcp", "127.0.0.1:6379") // or any other client
	if err != nil {
		// handle error
	}

	// This example retrieves the current integer value of `key` and sets its
	// new value to be the increment of that, all using the same connection
	// instance. NOTE that it does not do this atomically like the INCR command
	// would.
	key := "someKey"
	err = client.Do(ctx, WithConn(key, func(ctx context.Context, conn Conn) error {
		var curr int
		if err := conn.Do(ctx, Cmd(&curr, "GET", key)); err != nil {
			return err
		}

		curr++
		return conn.Do(ctx, FlatCmd(nil, "SET", key, curr))
	}))
	if err != nil {
		// handle error
	}
}

func ExampleWithConn_transaction() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := (PoolConfig{}).New(ctx, "tcp", "127.0.0.1:6379") // or any other client
	if err != nil {
		// handle error
	}

	// This example retrieves the current value of `key` and then sets a new
	// value on it in an atomic transaction.
	key := "someKey"
	var prevVal string

	err = client.Do(ctx, WithConn(key, func(ctx context.Context, c Conn) error {

		// Begin the transaction with a MULTI command
		if err := c.Do(ctx, Cmd(nil, "MULTI")); err != nil {
			return err
		}

		// If any of the calls after the MULTI call error it's important that
		// the transaction is discarded. This isn't strictly necessary if the
		// only possible error is a network error, as the connection would be
		// closed by the client anyway.
		var err error
		defer func() {
			if err != nil {
				// The return from DISCARD doesn't matter. If it's an error then
				// it's a network error and the Conn will be closed by the
				// client.
				c.Do(ctx, Cmd(nil, "DISCARD"))
			}
		}()

		// queue up the transaction's commands
		if err = c.Do(ctx, Cmd(nil, "GET", key)); err != nil {
			return err
		}
		if err = c.Do(ctx, Cmd(nil, "SET", key, "someOtherValue")); err != nil {
			return err
		}

		// execute the transaction, capturing the result in a Tuple. We only
		// care about the first element (the result from GET), so we discard the
		// second by setting nil.
		result := Tuple{&prevVal, nil}
		return c.Do(ctx, Cmd(&result, "EXEC"))
	}))
	if err != nil {
		// handle error
	}

	fmt.Printf("the value of key %q was %q\n", key, prevVal)
}

func TestMaybe(t *T) {
	mbtests := []struct {
		b       string
		isNull  bool
		isEmpty bool
	}{
		{b: "$-1\r\n", isNull: true},
		{b: "*-1\r\n", isNull: true},
		{b: "+foo\r\n"},
		{b: "-\r\n"},
		{b: "-foo\r\n"},
		{b: ":5\r\n"},
		{b: ":0\r\n"},
		{b: ":-5\r\n"},
		{b: "$0\r\n\r\n"},
		{b: "$3\r\nfoo\r\n"},
		{b: "$8\r\nfoo\r\nbar\r\n"},
		{b: "*2\r\n:1\r\n:2\r\n"},
		{b: "*0\r\n", isEmpty: true},
		{b: "~0\r\n", isEmpty: true},
		{b: "%0\r\n", isEmpty: true},
	}

	opts := resp.NewOpts()
	for i, mbt := range mbtests {
		t.Run(strconv.Itoa(i), func(t *T) {
			buf := bytes.NewBufferString(mbt.b)
			var rm resp3.RawMessage
			mb := Maybe{Rcv: &rm}
			assert.NoError(t, mb.UnmarshalRESP(bufio.NewReader(buf), opts))
			assert.Equal(t, mbt.b, string(rm))
			assert.Equal(t, mbt.isNull, mb.Null)
			assert.Equal(t, mbt.isEmpty, mb.Empty)
		})
	}
}

func ExampleMaybe() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := (PoolConfig{}).New(ctx, "tcp", "127.0.0.1:6379") // or any other client
	if err != nil {
		// handle error
	}

	var rcv int64
	mb := Maybe{Rcv: &rcv}
	if err := client.Do(ctx, Cmd(&mb, "GET", "foo")); err != nil {
		// handle error
	} else if mb.Null {
		fmt.Println("rcv is null")
	} else {
		fmt.Printf("rcv is %d\n", rcv)
	}

}

func TestTuple(t *T) {
	intPtr := func(i int) *int { return &i }
	strPtr := func(s string) *string { return &s }

	tests := []struct {
		in     string
		into   Tuple
		exp    Tuple
		expErr bool
	}{
		{
			in:   "*0\r\n",
			into: Tuple{},
			exp:  Tuple{},
		},
		{
			in:     "*0\r\n",
			into:   Tuple{new(int)},
			expErr: true,
		},
		{
			in:   "*1\r\n:1\r\n",
			into: Tuple{new(int)},
			exp:  Tuple{intPtr(1)},
		},
		{
			in:   "*2\r\n:1\r\n$3\r\nfoo\r\n",
			into: Tuple{new(int), new(string)},
			exp:  Tuple{intPtr(1), strPtr("foo")},
		},
		{
			in:     "*2\r\n:1\r\n$3\r\nfoo\r\n",
			into:   Tuple{new(int), new(string), new(string)},
			expErr: true,
		},
		{
			in:     "*2\r\n:1\r\n$3\r\nfoo\r\n",
			into:   Tuple{new(int), new(int)},
			expErr: true,
		},
	}

	opts := resp.NewOpts()
	for i, test := range tests {
		t.Run(fmt.Sprint(i), func(t *T) {
			t.Logf("in:%q", test.in)
			buf := bytes.NewBufferString(test.in)
			br := bufio.NewReader(buf)
			defer func() { assert.Empty(t, buf.Bytes()) }()
			defer func() { assert.Zero(t, br.Buffered()) }()

			err := test.into.UnmarshalRESP(br, opts)
			if test.expErr {
				assert.Error(t, err)
				assert.True(t, errors.As(err, new(resp.ErrConnUsable)))
				return
			}

			assert.Equal(t, test.exp, test.into)
		})
	}
}

var benchCmdActionKeys []string // global variable used to store the action keys in benchmarks

func BenchmarkCmdActionKeys(b *B) {
	for i := 0; i < b.N; i++ {
		benchCmdActionKeys = Cmd(nil, "GET", "a").Properties().Keys
	}
}

func BenchmarkFlatCmdActionKeys(b *B) {
	for i := 0; i < b.N; i++ {
		benchCmdActionKeys = FlatCmd(nil, "GET", "a").Properties().Keys
	}
}

func BenchmarkWithConnKeys(b *B) {
	for i := 0; i < b.N; i++ {
		benchCmdActionKeys = WithConn("a", func(context.Context, Conn) error {
			return nil
		}).Properties().Keys
	}
}
