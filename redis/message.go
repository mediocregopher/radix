package redis

import (
	"fmt"
)

//* Public message

type MessageType int

const (
	MessageMessage MessageType = iota
	MessagePmessage
)

// Message describes a pubsub "message" or "pmessage" message
type Message struct {
	Type    MessageType
	Pattern string
	Channel string
	Payload string
}

func newMessage(msg *message) *Message {
	return &Message{
		Type:    MessageType(msg.type_),
		Pattern: msg.pattern,
		Channel: msg.channel,
		Payload: msg.payload,
	}
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
