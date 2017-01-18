package pubsub

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
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

func conn(t *T) radix.Conn {
	c, err := radix.Dial("tcp", "localhost:6379")
	require.Nil(t, err)
	return c
}

func publish(t *T, c radix.Conn, ch, msg string) {
	require.Nil(t, radix.Cmd("PUBLISH", ch, msg).Run(c))
}

func assertMsgRead(t *T, msgCh <-chan Message) Message {
	select {
	case m := <-msgCh:
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("timedout reading")
	}
	panic("shouldn't get here")
}

func assertErrNoRead(t *T, errCh <-chan error) {
	select {
	case err, ok := <-errCh:
		if !ok {
			assert.Fail(t, "errCh closed")
		} else {
			assert.Fail(t, "unexpected Message off errCh", "err:%#v", err)
		}
	default:
	}
}

func TestPubSub(t *T) {
	c, msgCh, errCh := New(conn(t))

	ch, msgStr := randStr(), randStr()
	err := radix.Cmd("SUBSCRIBE", ch).Run(c)
	require.Nil(t, err)

	count := 100
	wg := new(sync.WaitGroup)

	wg.Add(1)
	go func() {
		pubC := conn(t)
		for i := 0; i < count; i++ {
			publish(t, pubC, ch, msgStr)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for i := 0; i < count; i++ {
			msg := assertMsgRead(t, msgCh)
			assert.Equal(t, Message{
				Type:    "message",
				Channel: ch,
				Message: []byte(msgStr),
			}, msg)
			assertErrNoRead(t, errCh)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for i := 0; i < count; i++ {
			var pingRes []string
			err := radix.CmdNoKey("PING").Into(&pingRes).Run(c)
			require.Nil(t, err)
			assert.Equal(t, []string{"pong", ""}, pingRes)
		}
		wg.Done()
	}()

	wg.Wait()

	c.Close()

	select {
	case _, ok := <-msgCh:
		assert.False(t, ok)
	case <-time.After(1 * time.Second):
		t.Fatal("msgCh blocked")
	}

	select {
	case err, ok := <-errCh:
		assert.Nil(t, err)
		assert.True(t, ok)
	case <-time.After(1 * time.Second):
		t.Fatal("errCh blocked")
	}

	select {
	case err, ok := <-errCh:
		assert.Nil(t, err)
		assert.False(t, ok)
	case <-time.After(1 * time.Second):
		t.Fatal("errCh blocked")
	}
}
