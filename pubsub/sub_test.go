package pubsub

import (
	"crypto/rand"
	"encoding/hex"
	. "testing"
	"time"

	radix "github.com/mediocregopher/radix.v2"
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

func testClients(t *T, timeout time.Duration) (radix.Conn, *SubConn) {
	pub, err := radix.DialTimeout("tcp", "localhost:6379", timeout)
	require.Nil(t, err)

	sub, err := radix.DialTimeout("tcp", "localhost:6379", timeout)
	require.Nil(t, err)

	return pub, New(sub)
}

func assertRead(t *T, sc *SubConn, timeout time.Duration) Message {
	select {
	case m := <-sc.Ch:
		return m
	case <-time.After(timeout):
		t.Fatal("timedout reading")
	}
	panic("shouldn't get here")
}

func assertNoRead(t *T, sc *SubConn, timeout time.Duration) {
	select {
	case m, ok := <-sc.Ch:
		if !ok {
			assert.Fail(t, "sc.Ch closed")
		} else {
			assert.Fail(t, "unexepcted Message off sc.Ch", "m:%#v", m)
		}
	case <-time.After(timeout):
	}
}

func assertClose(t *T, sc *SubConn) {
	sc.Close()
	_, ok := <-sc.Ch
	assert.False(t, ok)
	assert.Nil(t, sc.Err())
}

// Test that pubsub is still usable after a timeout
func TestTimeout(t *T) {
	pub, sub := testClients(t, 500*time.Millisecond)
	channel := randStr()
	sub.Subscribe(channel)

	time.Sleep(1 * time.Second)
	// the connection should have timed out at least by now

	require.Nil(t, radix.Cmd("PUBLISH", channel, "foo").Run(pub))
	m := assertRead(t, sub, 100*time.Millisecond)
	assert.Equal(t, channel, m.Channel)
	assert.Equal(t, []byte("foo"), m.Message)

	assertClose(t, sub)
}

func TestSubscribe(t *T) {
	pub, sub := testClients(t, 500*time.Millisecond)

	for i := 0; i < 100; i++ {
		channel := randStr()
		message := []byte(randStr())
		sub.Subscribe(channel)
		require.Nil(t, radix.Cmd("PUBLISH", channel, message).Run(pub))
		m := assertRead(t, sub, 100*time.Millisecond)
		assert.Equal(t, channel, m.Channel)
		assert.Equal(t, message, m.Message)

		sub.Unsubscribe(channel)
		require.Nil(t, radix.Cmd("PUBLISH", channel, message).Run(pub))
		// we don't bother waiting to read for a while to make sure the publish
		// didn't work, if it did we'll find out next loop around

		sub.Ping()
	}

	assertClose(t, sub)
}
