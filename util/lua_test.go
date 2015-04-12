package util

import (
	"crypto/rand"
	"fmt"
	. "testing"

	"github.com/mediocregopher/radix.v2/cluster"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// randTestScript returns a script, and a key and value which can be input into
// the script. The script will always have a different sha1 than the last.
func randTestScript() (string, string, string) {
	r := [][]byte{
		make([]byte, 10), // shard
		make([]byte, 10),
		make([]byte, 10),
		make([]byte, 10),
		make([]byte, 10),
	}
	for i := range r {
		if _, err := rand.Read(r[i]); err != nil {
			panic(err)
		}
		if i > 0 {
			r[i] = []byte(fmt.Sprintf(`{%x}%x`, string(r[0]), string(r[i])))
		}
	}
	script := fmt.Sprintf(
		`return redis.call('MSET', KEYS[1], ARGV[1], '%s', '%s')`,
		r[1], r[2],
	)
	return script, string(r[3]), string(r[4])
}

func TestLuaEval(t *T) {
	c1, err := redis.Dial("tcp", "127.0.0.1:6379")
	require.Nil(t, err)
	c2, err := cluster.New("127.0.0.1:7000")
	require.Nil(t, err)

	cs := []Cmder{c1, c2}

	for _, c := range cs {
		script, key, val := randTestScript()
		s, err := LuaEval(c, script, 1, key, val).Str()
		require.Nil(t, err)
		assert.Equal(t, "OK", s)

		// The second time the command will be hashed
		script, key, val = randTestScript()
		s, err = LuaEval(c, script, 1, key, val).Str()
		require.Nil(t, err)
		assert.Equal(t, "OK", s)

		s, err = c.Cmd("GET", key).Str()
		require.Nil(t, err)
		assert.Equal(t, val, s)
	}
}
