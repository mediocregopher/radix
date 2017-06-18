package radix

import (
	. "testing"

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

func TestFlatAction(t *T) {
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
