package redis

import (
	"fmt"
)

//* Public message

func newMessage(msg *message) *Message {
	return &Message{
		Pattern: msg.pattern,
		Channel: msg.channel,
		Payload: msg.payload,
	}
}

// Message describes a pubsub "message" or "pmessage" message
type Message struct {
	Pattern string
	Channel string
	Payload string
}

// String returns a string representation of the message.
func (m *Message) String() string {
	if m.Pattern != "" {
		return fmt.Sprintf("Message{Pattern: %s, Channel: %s, Payload: %s}",
			m.Pattern, m.Channel, m.Payload)
	}

	return fmt.Sprintf("Message{Channel: %s, Payload: %s}", m.Channel, m.Payload)
}

//* Private message

type messageType int

const (
	messageSubscribe messageType = iota
	messageUnsubscribe
	messagePsubscribe
	messagePunsubscribe
	messageMessage
	messagePmessage
	messageError
)

// message describes a pubsub message
type message struct {
	type_         messageType
	pattern       string
	channel       string
	subscriptions int
	payload       string
}
