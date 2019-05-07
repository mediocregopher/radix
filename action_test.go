package radix

import (
	"bufio"
	"bytes"
	"fmt"
	. "testing"

	"github.com/mediocregopher/radix/v3/resp/resp2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCmdAction(t *T) {
	c := dial()
	key, val := randStr(), randStr()

	require.Nil(t, c.Do(Cmd(nil, "SET", key, val)))
	var got string
	require.Nil(t, c.Do(Cmd(&got, "GET", key)))
	assert.Equal(t, val, got)

	// because BITOP is weird
	require.Nil(t, c.Do(Cmd(nil, "SET", key, val)))
	bitopCmd := Cmd(nil, "BITOP", "AND", key+key, key, key)
	assert.Equal(t, []string{key + key, key, key}, bitopCmd.Keys())
	require.Nil(t, c.Do(bitopCmd))
	var dstval string
	require.Nil(t, c.Do(Cmd(&dstval, "GET", key+key)))
	assert.Equal(t, val, dstval)
}

func TestCmdActionStreams(t *T) {
	c := dial()
	key, val := randStr(), randStr()

	// talking about weird commands, here are XREAD and XREADGROUP
	group := randStr()
	consumer := randStr()
	skeys := []string{"1", "1-", "1-1", "s1", ""} // make sure that we can handle empty and ID-like keys
	for _, skey := range skeys {
		require.Nil(t, c.Do(Cmd(nil, "DEL", skey)))
		require.NoError(t, c.Do(Cmd(nil, "XADD", skey, "1-1", key, val)))
		require.NoError(t, c.Do(Cmd(nil, "XGROUP", "CREATE", skey, group, "0-0")))

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
			assert.Equal(t, []string{skey}, xCmd.Keys())
			require.NoError(t, c.Do(xCmd))
		}
	}

	// test that we correctly handle multiple keys. we just use 3 keys since it's shorter
	for _, args := range [][]string{
		{"XREAD", "STREAMS", skeys[0], skeys[1], skeys[2], "0", "0", "0"},
		{"XREADGROUP", "GROUP", group, "test", "STREAMS", skeys[0], skeys[1], skeys[2], "0", ">", "0"},
	} {
		xCmd := Cmd(nil, args[0], args[1:]...)
		assert.Equal(t, skeys[:3], xCmd.Keys())
		require.NoError(t, c.Do(xCmd))
	}

	xCmd := Cmd(nil, "XINFO", "HELP")
	assert.Equal(t, []string(nil), xCmd.Keys())
	require.NoError(t, c.Do(xCmd))
}

func TestFlatCmdAction(t *T) {
	c := dial()
	key := randStr()
	m := map[string]string{
		randStr(): randStr(),
		randStr(): randStr(),
		randStr(): randStr(),
	}
	require.Nil(t, c.Do(FlatCmd(nil, "HMSET", key, m)))

	var got map[string]string
	require.Nil(t, c.Do(FlatCmd(&got, "HGETALL", key)))
	assert.Equal(t, m, got)
}

func TestFlatCmdActionNil(t *T) {
	c := dial()
	defer c.Close()

	key := randStr()
	hashKey := randStr()

	require.Nil(t, c.Do(FlatCmd(nil, "HMSET", key, hashKey, nil)))

	var nilVal MaybeNil
	require.Nil(t, c.Do(Cmd(&nilVal, "HGET", key, hashKey)))
	require.False(t, nilVal.Nil)
}

func TestEvalAction(t *T) {
	getSet := NewEvalScript(1, `
		local prev = redis.call("GET", KEYS[1])
		redis.call("SET", KEYS[1], ARGV[1])
		return prev
		-- `+randStr() /* so there's an eval everytime */ +`
	`)

	c := dial()
	key := randStr()
	val1, val2 := randStr(), randStr()

	{
		var res string
		err := c.Do(getSet.Cmd(&res, key, val1))
		require.Nil(t, err, "%s", err)
		assert.Empty(t, res)
	}

	{
		var res string
		err := c.Do(getSet.Cmd(&res, key, val2))
		require.Nil(t, err)
		assert.Equal(t, val1, res)
	}
}

func ExampleEvalScript() {
	// set as a global variable, this script is equivalent to the builtin GETSET
	// redis command
	var getSet = NewEvalScript(1, `
		local prev = redis.call("GET", KEYS[1])
		redis.call("SET", KEYS[1], ARGV[1])
		return prev
`)

	client, err := NewPool("tcp", "127.0.0.1:6379", 10) // or any other client
	if err != nil {
		// handle error
	}

	key := "someKey"
	var prevVal string
	if err := client.Do(getSet.Cmd(&prevVal, key, "myVal")); err != nil {
		// handle error
	}

	fmt.Printf("value of key %q used to be %q\n", key, prevVal)
}

func TestPipelineAction(t *T) {
	c := dial()
	for i := 0; i < 10; i++ {
		ss := []string{
			randStr(),
			randStr(),
			randStr(),
		}
		out := make([]string, len(ss))
		var cmds []CmdAction
		for i := range ss {
			cmds = append(cmds, Cmd(&out[i], "ECHO", ss[i]))
		}
		require.Nil(t, c.Do(Pipeline(cmds...)))

		for i := range ss {
			assert.Equal(t, ss[i], out[i])
		}
	}
}

func ExamplePipeline() {
	client, err := NewPool("tcp", "127.0.0.1:6379", 10) // or any other client
	if err != nil {
		// handle error
	}
	var fooVal string
	p := Pipeline(
		FlatCmd(nil, "SET", "foo", 1),
		Cmd(&fooVal, "GET", "foo"),
	)
	if err := client.Do(p); err != nil {
		// handle error
	}
	fmt.Printf("fooVal: %q\n", fooVal)
	// Output: fooVal: "1"
}

func TestWithConnAction(t *T) {
	c := dial()
	k, v := randStr(), 10

	err := c.Do(WithConn(k, func(conn Conn) error {
		require.Nil(t, conn.Do(FlatCmd(nil, "SET", k, v)))
		var out int
		require.Nil(t, conn.Do(Cmd(&out, "GET", k)))
		assert.Equal(t, v, out)
		return nil
	}))
	require.Nil(t, err)
}

func ExampleWithConn() {
	client, err := NewPool("tcp", "127.0.0.1:6379", 10) // or any other client
	if err != nil {
		// handle error
	}

	// This example retrieves the current integer value of `key` and sets its
	// new value to be the increment of that, all using the same connection
	// instance. NOTE that it does not do this atomically like the INCR command
	// would.
	key := "someKey"
	err = client.Do(WithConn(key, func(conn Conn) error {
		var curr int
		if err := conn.Do(Cmd(&curr, "GET", key)); err != nil {
			return err
		}

		curr++
		return conn.Do(FlatCmd(nil, "SET", key, curr))
	}))
	if err != nil {
		// handle error
	}
}

func ExampleWithConn_transaction() {
	client, err := NewPool("tcp", "127.0.0.1:6379", 10) // or any other client
	if err != nil {
		// handle error
	}

	// This example retrieves the current value of `key` and then sets a new
	// value on it in an atomic transaction.
	key := "someKey"
	var prevVal string

	err = client.Do(WithConn(key, func(c Conn) error {

		// Begin the transaction with a MULTI command
		if err := c.Do(Cmd(nil, "MULTI")); err != nil {
			return err
		}

		// If any of the calls after the MULTI call error it's important that
		// the transaction is discarded. This isn't strictly necessary if the
		// error was a network error, as the connection would be closed by the
		// client anyway, but it's important otherwise.
		var err error
		defer func() {
			if err != nil {
				// The return from DISCARD doesn't matter. If it's an error then
				// it's a network error and the Conn will be closed by the
				// client.
				c.Do(Cmd(nil, "DISCARD"))
			}
		}()

		// queue up the transaction's commands
		if err = c.Do(Cmd(nil, "GET", key)); err != nil {
			return err
		}
		if err = c.Do(Cmd(nil, "SET", key, "someOtherValue")); err != nil {
			return err
		}

		// execute the transaction, capturing the result
		var result []string
		if err = c.Do(Cmd(&result, "EXEC")); err != nil {
			return err
		}

		// capture the output of the first transaction command, i.e. the GET
		prevVal = result[0]
		return nil
	}))
	if err != nil {
		// handle error
	}

	fmt.Printf("the value of key %q was %q\n", key, prevVal)
}

func TestMaybeNil(t *T) {
	mntests := []struct {
		b     string
		isNil bool
	}{
		{b: "$-1\r\n", isNil: true},
		{b: "*-1\r\n", isNil: true},
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
	}

	for _, mnt := range mntests {
		buf := bytes.NewBufferString(mnt.b)
		{
			var rm resp2.RawMessage
			mn := MaybeNil{Rcv: &rm}
			require.Nil(t, mn.UnmarshalRESP(bufio.NewReader(buf)))
			if mnt.isNil {
				assert.True(t, mn.Nil)
			} else {
				assert.Equal(t, mnt.b, string(rm))
			}
		}
	}
}

func ExampleMaybeNil() {
	client, err := NewPool("tcp", "127.0.0.1:6379", 10) // or any other client
	if err != nil {
		// handle error
	}

	var rcv int64
	mn := MaybeNil{Rcv: &rcv}
	if err := client.Do(Cmd(&mn, "GET", "foo")); err != nil {
		// handle error
	} else if mn.Nil {
		fmt.Println("rcv is nil")
	} else {
		fmt.Printf("rcv is %d\n", rcv)
	}

}

var benchCmdActionKeys []string // global variable used to store the action keys in benchmarks

func BenchmarkCmdActionKeys(b *B) {
	for i := 0; i < b.N; i++ {
		benchCmdActionKeys = Cmd(nil, "GET", "a").Keys()
	}
}

func BenchmarkFlatCmdActionKeys(b *B) {
	for i := 0; i < b.N; i++ {
		benchCmdActionKeys = FlatCmd(nil, "GET", "a").Keys()
	}
}

func BenchmarkWithConnKeys(b *B) {
	for i := 0; i < b.N; i++ {
		benchCmdActionKeys = WithConn("a", func(Conn) error { return nil }).Keys()
	}
}
