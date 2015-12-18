package sentinel

import (
	"crypto/rand"
	"encoding/hex"
	. "testing"
	"time"

	"github.com/mediocregopher/radix.v2/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests assume there is a master/slave running on ports 8000/7001, and a
// sentinel which tracks them under the name "test" on port 28000
//
// You can use `make start` to automatically set these up.

func getSentinel(t *T) *Client {
	s, err := NewClient("tcp", "127.0.0.1:28000", 10, "test")
	require.Nil(t, err)
	return s
}

func randStr() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func TestBasic(t *T) {
	s := getSentinel(t)
	k := randStr()

	c, err := s.GetMaster("test")
	require.Nil(t, err)
	require.Nil(t, c.Cmd("SET", k, "foo").Err)
	s.PutMaster("test", c)

	c, err = s.GetMaster("test")
	require.Nil(t, err)
	foo, err := c.Cmd("GET", k).Str()
	require.Nil(t, err)
	assert.Equal(t, "foo", foo)
	s.PutMaster("test", c)
}

// Test a basic manual failover
func TestFailover(t *T) {
	s := getSentinel(t)
	sc, err := redis.Dial("tcp", "127.0.0.1:28000")
	require.Nil(t, err)

	k := randStr()

	c, err := s.GetMaster("test")
	require.Nil(t, err)
	require.Nil(t, c.Cmd("SET", k, "foo").Err)
	s.PutMaster("test", c)

	require.Nil(t, sc.Cmd("SENTINEL", "FAILOVER", "test").Err)

	c, err = s.GetMaster("test")
	require.Nil(t, err)
	foo, err := c.Cmd("GET", k).Str()
	require.Nil(t, err)
	assert.Equal(t, "foo", foo)
	require.Nil(t, c.Cmd("SET", k, "bar").Err)
	s.PutMaster("test", c)

	time.Sleep(10 * time.Second)
	require.Nil(t, sc.Cmd("SENTINEL", "FAILOVER", "test").Err)

	c, err = s.GetMaster("test")
	require.Nil(t, err)
	bar, err := c.Cmd("GET", k).Str()
	require.Nil(t, err)
	assert.Equal(t, "bar", bar)
	s.PutMaster("test", c)
}
