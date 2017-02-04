package radix

import (
	. "testing"

	"github.com/mediocregopher/radix.v2/resp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPubSubStub(t *T) {
	conn, stubCh := PubSubStub("tcp", "127.0.0.1:6379", func(in []string) interface{} {
		return in
	})
	message := func(channel, val string) {
		stubCh <- PubSubMessage{Channel: channel, Message: []byte(val)}
		<-conn.(*pubSubStub).mDoneCh
	}

	assertEncode := func(in ...string) {
		require.Nil(t, conn.Encode(resp.Any{I: in}))
	}
	assertDecode := func(exp ...string) {
		var into []string
		require.Nil(t, conn.Decode(resp.Any{I: &into}))
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
	assert.Equal(t, errPubSubMode.Error(), conn.Decode(resp.Any{}).Error())

	assertEncode("PING")
	assertDecode("pong", "")

	message("foo", "b")
	message("bar", "c")
	message("baz", "c")
	assertDecode("message", "foo", "b")
	assertDecode("message", "bar", "c")

	assertEncode("PSUBSCRIBE", "b*z")
	assertDecode("psubscribe", "b*z", "3")
	message("buz", "d")
	message("biz", "e")
	assertDecode("pmessage", "b*z", "buz", "d")
	assertDecode("pmessage", "b*z", "biz", "e")

	assertEncode("UNSUBSCRIBE", "foo")
	assertDecode("unsubscribe", "foo", "2")
	message("foo", "f")
	message("bar", "g")
	assertDecode("message", "bar", "g")

	assertEncode("UNSUBSCRIBE", "bar")
	assertDecode("unsubscribe", "bar", "1")
	assertEncode("PUNSUBSCRIBE", "b*z")
	assertDecode("punsubscribe", "b*z", "0")

	// No longer in pubsub mode, normal requests should work again
	assertEncode("wat")
	assertDecode("wat")
}
