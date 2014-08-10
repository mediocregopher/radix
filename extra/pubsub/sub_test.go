package pubsub

import (
	"fmt"
	"testing"
	"time"

	"github.com/fzzy/radix/redis"
)

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

	if sr.Type != SubscribeType {
		t.Fatal("Did not receive a subscribe reply")
	}

	if sr.SubCount != 1 {
		t.Fatal(fmt.Sprintf("Unexpected subscription count, Expected: 0, Found: %d", sr.SubCount))
	}

	r := pub.Cmd("PUBLISH", channel, message)
	if r.Err != nil {
		t.Fatal(r.Err)
	}

	subChan := make(chan *SubReply)
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

	if sr.Type != MessageType {
		t.Fatal("Did not receive a message reply")
	}

	if sr.Message != message {
		t.Fatal(fmt.Sprintf("Did not recieve expected message '%s', instead got: '%s'", message, sr.Message))
	}

	sr = sub.Unsubscribe(channel)
	if sr.Err != nil {
		t.Fatal(sr.Err)
	}

	if sr.Type != UnsubscribeType {
		t.Fatal("Did not receive a unsubscribe reply")
	}

	if sr.SubCount != 0 {
		t.Fatal(fmt.Sprintf("Unexpected subscription count, Expected: 0, Found: %d", sr.SubCount))
	}
}
