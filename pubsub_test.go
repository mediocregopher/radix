package radix

import (
	"strconv"
	"sync"
	. "testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func publish(t *T, c Conn, ch, msg string) {
	require.Nil(t, Cmd(nil, "PUBLISH", ch, msg).Run(c))
}

func assertMsgRead(t *T, msgCh <-chan PubSubMessage) PubSubMessage {
	select {
	case m := <-msgCh:
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("timedout reading")
	}
	panic("shouldn't get here")
}

func assertMsgNoRead(t *T, msgCh <-chan PubSubMessage) {
	select {
	case msg, ok := <-msgCh:
		if !ok {
			assert.Fail(t, "msgCh closed")
		} else {
			assert.Fail(t, "unexpected PubSubMessage off msgCh", "msg:%#v", msg)
		}
	default:
	}
}

func testSubscribe(t *T, c PubSubConn, pubCh chan int) {
	pubC := dial()
	msgCh := make(chan PubSubMessage, 1)

	ch1, ch2, msgStr := randStr(), randStr(), randStr()
	require.Nil(t, c.Subscribe(msgCh, ch1, ch2))

	pubChs := make([]chan int, 3)
	{
		for i := range pubChs {
			pubChs[i] = make(chan int)
		}
		go func() {
			for i := range pubCh {
				for _, innerPubCh := range pubChs {
					innerPubCh <- i
				}
			}
			for _, innerPubCh := range pubChs {
				close(innerPubCh)
			}
		}()
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		for i := range pubChs[0] {
			publish(t, pubC, ch1, msgStr+"_"+strconv.Itoa(i))
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for i := range pubChs[1] {
			msg := assertMsgRead(t, msgCh)
			assert.Equal(t, PubSubMessage{
				Type:    "message",
				Channel: ch1,
				Message: []byte(msgStr + "_" + strconv.Itoa(i)),
			}, msg)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for range pubChs[2] {
			require.Nil(t, c.Ping())
		}
		wg.Done()
	}()
	wg.Wait()

	require.Nil(t, c.Unsubscribe(msgCh, ch1))
	publish(t, pubC, ch1, msgStr)
	publish(t, pubC, ch2, msgStr)
	msg := assertMsgRead(t, msgCh)
	assert.Equal(t, PubSubMessage{
		Type:    "message",
		Channel: ch2,
		Message: []byte(msgStr),
	}, msg)

}

func TestPubSubSubscribe(t *T) {
	pubCh := make(chan int)
	go func() {
		for i := 0; i < 1000; i++ {
			pubCh <- i
		}
		close(pubCh)
	}()
	c := PubSub(dial())
	testSubscribe(t, c, pubCh)

	c.Close()
	assert.NotNil(t, c.Ping())
	assert.NotNil(t, c.Ping())
	assert.NotNil(t, c.Ping())
}

func TestPubSubPSubscribe(t *T) {
	pubC := dial()
	c := PubSub(dial())
	msgCh := make(chan PubSubMessage, 1)

	p1, p2, msgStr := randStr()+"_*", randStr()+"_*", randStr()
	ch1, ch2 := p1+"_"+randStr(), p2+"_"+randStr()
	require.Nil(t, c.PSubscribe(msgCh, p1, p2))

	count := 1000
	wg := new(sync.WaitGroup)

	wg.Add(1)
	go func() {
		for i := 0; i < count; i++ {
			publish(t, pubC, ch1, msgStr)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for i := 0; i < count; i++ {
			msg := assertMsgRead(t, msgCh)
			assert.Equal(t, PubSubMessage{
				Type:    "pmessage",
				Pattern: p1,
				Channel: ch1,
				Message: []byte(msgStr),
			}, msg)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for i := 0; i < count; i++ {
			require.Nil(t, c.Ping())
		}
		wg.Done()
	}()

	wg.Wait()

	require.Nil(t, c.PUnsubscribe(msgCh, p1))
	publish(t, pubC, ch1, msgStr)
	publish(t, pubC, ch2, msgStr)
	msg := assertMsgRead(t, msgCh)
	assert.Equal(t, PubSubMessage{
		Type:    "pmessage",
		Pattern: p2,
		Channel: ch2,
		Message: []byte(msgStr),
	}, msg)

	c.Close()
	assert.NotNil(t, c.Ping())
	assert.NotNil(t, c.Ping())
	assert.NotNil(t, c.Ping())
	publish(t, pubC, ch2, msgStr)
	time.Sleep(250 * time.Millisecond)
	assertMsgNoRead(t, msgCh)
}
