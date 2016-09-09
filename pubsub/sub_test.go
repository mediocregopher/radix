package pubsub

import (
	"fmt"
	"testing"
	"time"

	"github.com/mediocregopher/radix.v2/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testClients(t *testing.T, timeout time.Duration) (*redis.Client, *SubClient) {
	pub, err := redis.DialTimeout("tcp", "localhost:6379", timeout)
	require.Nil(t, err)

	sub, err := redis.DialTimeout("tcp", "localhost:6379", timeout)
	require.Nil(t, err)

	return pub, NewSubClient(sub)
}

// Test that pubsub is still usable after a timeout
func TestTimeout(t *testing.T) {
	go func() {
		time.Sleep(10 * time.Second)
		t.Fatal()
	}()

	pub, sub := testClients(t, 500*time.Millisecond)
	require.Nil(t, sub.Subscribe("timeoutTestChannel").Err)

	r := sub.Receive() // should timeout after a second
	assert.Equal(t, Error, r.Type)
	assert.NotNil(t, r.Err)
	assert.True(t, r.Timeout())

	waitCh := make(chan struct{})
	go func() {
		r = sub.Receive()
		close(waitCh)
	}()
	require.Nil(t, pub.Cmd("PUBLISH", "timeoutTestChannel", "foo").Err)
	<-waitCh

	assert.Equal(t, Message, r.Type)
	assert.Equal(t, "timeoutTestChannel", r.Channel)
	assert.Equal(t, "foo", r.Message)
	assert.Nil(t, r.Err, "%s", r.Err)
	assert.False(t, r.Timeout())
}

func TestSubscribe(t *testing.T) {
	pub, err := redis.DialTimeout("tcp", "localhost:6379", time.Duration(10)*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	client, err := redis.DialTimeout("tcp", "localhost:6379", time.Duration(10)*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	sub := NewSubClient(client)

	channel := "subTestChannel"
	message := "Hello, World!"

	sr := sub.Subscribe(channel)
	if sr.Err != nil {
		t.Fatal(sr.Err)
	}

	if sr.Type != Subscribe {
		t.Fatal("Did not receive a subscribe reply")
	}

	if sr.SubCount != 1 {
		t.Fatal(fmt.Sprintf("Unexpected subscription count, Expected: 0, Found: %d", sr.SubCount))
	}

	r := pub.Cmd("PUBLISH", channel, message)
	if r.Err != nil {
		t.Fatal(r.Err)
	}

	subChan := make(chan *SubResp)
	go func() {
		subChan <- sub.Receive()
	}()

	select {
	case sr = <-subChan:
	case <-time.After(time.Duration(10) * time.Second):
		t.Fatal("Took too long to Receive message")
	}

	if sr.Err != nil {
		t.Fatal(sr.Err)
	}

	if sr.Type != Message {
		t.Fatal("Did not receive a message reply")
	}

	if sr.Message != message {
		t.Fatal(fmt.Sprintf("Did not recieve expected message '%s', instead got: '%s'", message, sr.Message))
	}

	sr = sub.Unsubscribe(channel)
	if sr.Err != nil {
		t.Fatal(sr.Err)
	}

	if sr.Type != Unsubscribe {
		t.Fatal("Did not receive a unsubscribe reply")
	}

	if sr.SubCount != 0 {
		t.Fatal(fmt.Sprintf("Unexpected subscription count, Expected: 0, Found: %d", sr.SubCount))
	}
}

func TestPSubscribe(t *testing.T) {
	pub, err := redis.DialTimeout("tcp", "localhost:6379", time.Duration(10)*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	client, err := redis.DialTimeout("tcp", "localhost:6379", time.Duration(10)*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	sub := NewSubClient(client)

	pattern := "patternThen*"
	message := "Hello, World!"

	sr := sub.PSubscribe(pattern)
	if sr.Err != nil {
		t.Fatal(sr.Err)
	}

	if sr.Type != Subscribe {
		t.Fatal("Did not receive a subscribe reply")
	}

	if sr.SubCount != 1 {
		t.Fatal(fmt.Sprintf("Unexpected subscription count, Expected: 0, Found: %d", sr.SubCount))
	}

	r := pub.Cmd("PUBLISH", "patternThenHello", message)
	if r.Err != nil {
		t.Fatal(r.Err)
	}

	subChan := make(chan *SubResp)
	go func() {
		subChan <- sub.Receive()
	}()

	select {
	case sr = <-subChan:
	case <-time.After(time.Duration(10) * time.Second):
		t.Fatal("Took too long to Receive message")
	}

	if sr.Err != nil {
		t.Fatal(sr.Err)
	}

	if sr.Type != Message {
		t.Fatal("Did not receive a message reply")
	}

	if sr.Pattern != pattern {
		t.Fatal(fmt.Sprintf("Did not recieve expected pattern '%s', instead got: '%s'", pattern, sr.Pattern))
	}

	if sr.Message != message {
		t.Fatal(fmt.Sprintf("Did not recieve expected message '%s', instead got: '%s'", message, sr.Message))
	}

	sr = sub.PUnsubscribe(pattern)
	if sr.Err != nil {
		t.Fatal(sr.Err)
	}

	if sr.Type != Unsubscribe {
		t.Fatal("Did not receive a unsubscribe reply")
	}

	if sr.SubCount != 0 {
		t.Fatal(fmt.Sprintf("Unexpected subscription count, Expected: 0, Found: %d", sr.SubCount))
	}
}
