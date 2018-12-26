package radix

import (
	. "testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPipelinedCmdAction(t *T) {
	c := NewPipelineClient(dial())
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

func TestPipelinedEvalAction(t *T) {
	getSet := NewEvalScript(1, `
		local prev = redis.call("GET", KEYS[1])
		redis.call("SET", KEYS[1], ARGV[1])
		return prev
		-- `+randStr() /* so there's an eval everytime */ +`
	`)

	c := NewPipelineClient(dial())
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