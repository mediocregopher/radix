package radix

import (
	"log"
	. "testing"
	"time"

	"github.com/mediocregopher/radix/v3/resp/resp2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPubSubStub(t *T) {
	conn, stubCh := PubSubStub("tcp", "127.0.0.1:6379", func(in []string) interface{} {
		return in
	})
	message := func(channel, val string) {
		stubCh <- PubSubMessage{Type: "message", Channel: channel, Message: []byte(val)}
		<-conn.(*pubSubStub).mDoneCh
	}
	pmessage := func(pattern, channel, val string) {
		stubCh <- PubSubMessage{Type: "pmessage", Pattern: pattern, Channel: channel, Message: []byte(val)}
		<-conn.(*pubSubStub).mDoneCh
	}

	assertEncode := func(in ...string) {
		require.Nil(t, conn.Encode(resp2.Any{I: in}))
	}
	assertDecode := func(exp ...string) {
		var into []string
		require.Nil(t, conn.Decode(resp2.Any{I: &into}))
		assert.Equal(t, exp, into)
	}

	assertEncode("foo")
	assertDecode("foo")

	// shouldn't do anything
	message("foo", "a")

	assertEncode("SUBSCRIBE", "foo", "bar")
	assertDecode("subscribe", "foo", "1")
	assertDecode("subscribe", "bar", "2")

	// should error because we're in pubsub mode
	assertEncode("wat")
	assert.Equal(t, errPubSubMode.Error(), conn.Decode(resp2.Any{}).Error())

	assertEncode("PING")
	assertDecode("pong", "")

	message("foo", "b")
	message("bar", "c")
	message("baz", "c")
	assertDecode("message", "foo", "b")
	assertDecode("message", "bar", "c")

	assertEncode("PSUBSCRIBE", "b*z")
	assertDecode("psubscribe", "b*z", "3")
	assertEncode("PSUBSCRIBE", "b[au]z")
	assertDecode("psubscribe", "b[au]z", "4")
	pmessage("b*z", "buz", "d")
	pmessage("b[au]z", "buz", "d")
	pmessage("b*z", "biz", "e")
	assertDecode("pmessage", "b*z", "buz", "d")
	assertDecode("pmessage", "b[au]z", "buz", "d")
	assertDecode("pmessage", "b*z", "biz", "e")

	assertEncode("UNSUBSCRIBE", "foo")
	assertDecode("unsubscribe", "foo", "3")
	message("foo", "f")
	message("bar", "g")
	assertDecode("message", "bar", "g")

	assertEncode("UNSUBSCRIBE", "bar")
	assertDecode("unsubscribe", "bar", "2")
	assertEncode("PUNSUBSCRIBE", "b*z")
	assertDecode("punsubscribe", "b*z", "1")
	assertEncode("PUNSUBSCRIBE", "b[au]z")
	assertDecode("punsubscribe", "b[au]z", "0")

	// No longer in pubsub mode, normal requests should work again
	assertEncode("wat")
	assertDecode("wat")
}

func ExamplePubSubStub() {
	// Make a pubsub stub conn which will return nil for everything except
	// pubsub commands (which will be handled automatically)
	stub, stubCh := PubSubStub("tcp", "127.0.0.1:6379", func([]string) interface{} {
		return nil
	})

	// These writes shouldn't do anything, initially, since we haven't
	// subscribed to anything
	go func() {
		for {
			stubCh <- PubSubMessage{
				Channel: "foo",
				Message: []byte("bar"),
			}
			time.Sleep(1 * time.Second)
		}
	}()

	// Use PubSub to wrap the stub like we would for a normal redis connection
	pstub := PubSub(stub)

	// Subscribe msgCh to "foo"
	msgCh := make(chan PubSubMessage)
	if err := pstub.Subscribe(msgCh, "foo"); err != nil {
		log.Fatal(err)
	}

	// now msgCh is subscribed the publishes being made by the go-routine above
	// will start being written to it
	for m := range msgCh {
		log.Printf("read m: %#v", m)
	}
}
