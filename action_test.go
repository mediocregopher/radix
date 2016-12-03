package radix

import (
	. "testing"

	"github.com/levenlabs/golib/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCmd(t *T) {
	c := dial()
	key, val := randStr(), randStr()

	require.Nil(t, Cmd("SET", key, val).Run(c))
	var got string
	require.Nil(t, Cmd("GET", key).Into(&got).Run(c))
	assert.Equal(t, val, got)
}

func TestLuaCmd(t *T) {
	getset := `
		local res = redis.call("GET", KEYS[1])
		redis.call("SET", KEYS[1], ARGV[1])
		return res
	`
	getset += " -- " + randStr() // so it does has to do an eval every time

	c := dial()
	key := randStr()
	val1, val2 := randStr(), randStr()

	{
		var res string
		err := LuaCmd(getset, []string{key}, val1).Into(&res).Run(c)
		require.Nil(t, err, "%s", err)
		assert.Empty(t, res)
	}

	{
		var res string
		err := LuaCmd(getset, []string{key}, val2).Into(&res).Run(c)
		require.Nil(t, err)
		assert.Equal(t, val1, res)
	}
}

func TestPipelineAction(t *T) {
	c := dial()
	for i := 0; i < 10; i++ {
		ss := []string{
			testutil.RandStr(),
			testutil.RandStr(),
			testutil.RandStr(),
		}
		out := make([]string, len(ss))
		var p Pipeline
		for i := range ss {
			p = append(p, CmdNoKey("ECHO", ss[i]).Into(&out[i]))
		}
		require.Nil(t, p.Run(c))

		for i := range ss {
			assert.Equal(t, ss[i], out[i])
		}
	}
}
