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
	MessageError
)

// Message describes a pub/sub message
type Message struct {
	Type          MessageType
	Channel       string
	Pattern       string
	Subscriptions int
	Payload       string
	Error         error
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
	case MessageError:
		mtype = "error"
	default:
		mtype = "unknown"
	}

	return fmt.Sprintf("Message{Type: %s, Channel: %v, Pattern: %v, Subscriptions: %v, "+
		"Payload: %v, Error: %v}", mtype, m.Channel, m.Pattern, m.Subscriptions, m.Payload, m.Error)
}
