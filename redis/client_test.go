package redis

import (
	. "testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dial(t *T) *Client {
	client, err := DialTimeout("tcp", "127.0.0.1:6379", 10*time.Second)
	require.Nil(t, err)
	return client
}

func TestCmd(t *T) {
	c := dial(t)
	v, err := c.Cmd("echo", "Hello, World!").Str()
	require.Nil(t, err)
	assert.Equal(t, "Hello, World!", v)

	// Test that a bad command properly returns an AppErr
	r := c.Cmd("non-existant-cmd")
	assert.Equal(t, AppErr, r.typ)
	assert.NotNil(t, r.Err)

	// Test that application level errors propagate correctly
	require.Nil(t, c.Cmd("sadd", "foo", "bar").Err)
	_, err = c.Cmd("get", "foo").Str()
	assert.NotNil(t, "", err)
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
