package redis

import (
	"fmt"
)

type MessageType int

const (
	MessageSubscribe MessageType = iota
	MessageUnsubscribe
	MessagePsubscribe
	MessagePunsubscribe
	MessageMessage
	MessagePmessage
)

// Message describes a pub/sub message
type Message struct {
	Type          MessageType
	Channel       string
	Pattern       string
	Subscriptions int
	Payload       string
}

// String returns a string representation of the message.
func (m *Message) String() string {
	var mtype string

	switch m.Type {
	case MessageSubscribe:
		mtype = "subscribe"
	case MessageUnsubscribe:
		mtype = "unsubscribe"
	case MessagePsubscribe:
		mtype = "psubscribe"
	case MessagePunsubscribe:
		mtype = "punsubscribe"
	case MessageMessage:
		mtype = "message"
	case MessagePmessage:
		mtype = "pmessage"
	default:
		mtype = "unknown"
	}

	return fmt.Sprintf("Message{Type: %s, Channel: %v, Pattern: %v, Subscriptions: %v, "+
		"Payload: %v}", mtype, m.Channel, m.Pattern, m.Subscriptions, m.Payload)
}
