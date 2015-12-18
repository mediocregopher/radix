package redis

import (
	"crypto/rand"
	"encoding/hex"
	. "testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func randStr() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func dial(t *T) *Client {
	client, err := DialTimeout("tcp", "127.0.0.1:6379", 10*time.Second)
	require.Nil(t, err)
	return client
}

func TestCmd(t *T) {
	c := dial(t)
	echo := randStr()
	v, err := c.Cmd("echo", echo).Str()
	require.Nil(t, err)
	assert.Equal(t, echo, v)

	// Test that a bad command properly returns an AppErr
	r := c.Cmd("non-existant-cmd")
	assert.Equal(t, AppErr, r.typ)
	assert.NotNil(t, r.Err)

	// Test that application level errors propagate correctly
	k := randStr()
	require.Nil(t, c.Cmd("sadd", k, randStr()).Err)
	_, err = c.Cmd("get", k).Str()
	assert.NotNil(t, "", err)

	// Test flattening out maps
	k = randStr()
	args := map[string]interface{}{
		"someBytes":  []byte("blah"),
		"someString": "foo",
		"someInt":    10,
		"someBool":   false,
	}
	require.Nil(t, c.Cmd("HMSET", k, args).Err)
	l, err := c.Cmd("HMGET", k, "someBytes", "someString", "someInt", "someBool").List()
	require.Nil(t, err)
	assert.Equal(t, []string{"blah", "foo", "10", "0"}, l)

}

func TestPipeline(t *T) {
	c := dial(t)
	// Do this multiple times to make sure pipeline resetting happens correctly
	for i := 0; i < 10; i++ {
		c.PipeAppend("echo", "foo")
		c.PipeAppend("echo", "bar")
		c.PipeAppend("echo", "zot")

		v, err := c.PipeResp().Str()
		require.Nil(t, err)
		assert.Equal(t, "foo", v)

		v, err = c.PipeResp().Str()
		require.Nil(t, err)
		assert.Equal(t, "bar", v)

		v, err = c.PipeResp().Str()
		require.Nil(t, err)
		assert.Equal(t, "zot", v)

		r := c.PipeResp()
		assert.Equal(t, AppErr, r.typ)
		assert.Equal(t, ErrPipelineEmpty, r.Err)
	}
}

func TestPipelineClear(t *T) {
	c := dial(t)
	// Do this multiple times to make sure pipeline resetting happens correctly
	for i := 0; i < 10; i++ {

		// Clearing an empty pipeline will return 0
		call, reply := c.PipeClear()
		assert.Equal(t, 0, call)
		assert.Equal(t, 0, reply)

		// Clearing pending calls
		c.PipeAppend("echo", "foo")
		c.PipeAppend("echo", "bar")
		call, reply = c.PipeClear()
		assert.Equal(t, 2, call)
		assert.Equal(t, 0, reply)

		r := c.PipeResp()
		assert.Equal(t, AppErr, r.typ)
		assert.Equal(t, ErrPipelineEmpty, r.Err)

		// Clearing pending replies
		c.PipeAppend("echo", "foo")
		c.PipeAppend("echo", "bar")
		c.PipeAppend("echo", "zot")

		v, err := c.PipeResp().Str()
		require.Nil(t, err)
		assert.Equal(t, "foo", v)

		v, err = c.PipeResp().Str()
		require.Nil(t, err)
		assert.Equal(t, "bar", v)

		call, reply = c.PipeClear()
		assert.Equal(t, 0, call)
		assert.Equal(t, 1, reply)

		r = c.PipeResp()
		assert.Equal(t, AppErr, r.typ)
		assert.Equal(t, ErrPipelineEmpty, r.Err)

		// Normal pipeline execution should succeed after PipeClear
		c.PipeAppend("echo", "foo")
		c.PipeAppend("echo", "bar")
		c.PipeAppend("echo", "zot")

		v, err = c.PipeResp().Str()
		require.Nil(t, err)
		assert.Equal(t, "foo", v)

		v, err = c.PipeResp().Str()
		require.Nil(t, err)
		assert.Equal(t, "bar", v)

		v, err = c.PipeResp().Str()
		require.Nil(t, err)
		assert.Equal(t, "zot", v)

		r = c.PipeResp()
		assert.Equal(t, AppErr, r.typ)
		assert.Equal(t, ErrPipelineEmpty, r.Err)
	}
}

func TestLastCritical(t *T) {
	c := dial(t)

	// LastCritical shouldn't get set for application errors
	assert.NotNil(t, c.Cmd("WHAT").Err)
	assert.Nil(t, c.LastCritical)

	c.Close()
	r := c.Cmd("WHAT")
	assert.Equal(t, true, r.IsType(IOErr))
	assert.NotNil(t, r.Err)
	assert.NotNil(t, c.LastCritical)
}

func TestKeyFromArg(t *T) {
	m := map[string]interface{}{
		"foo0": "foo0",
		"foo1": []byte("foo1"),
		"1":    1,
		"1.1":  1.1,
		"foo2": []string{"foo2", "bar"},
		"foo3": [][]string{{"foo3", "bar"}, {"baz", "buz"}},
	}

	for out, in := range m {
		key, err := KeyFromArgs(in)
		assert.Nil(t, err)
		assert.Equal(t, out, key)
	}
}
