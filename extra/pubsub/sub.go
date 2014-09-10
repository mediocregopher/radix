package pubsub

import (
	"container/list"
	"errors"
	"net"

	"github.com/fzzy/radix/redis"
)

type SubReplyType uint8

const (
	ErrorReply SubReplyType = iota
	SubscribeReply
	UnsubscribeReply
	MessageReply
)

// SubClient wraps a Redis client to provide convenience methods for Pub/Sub functionality.
type SubClient struct {
	Client   *redis.Client
	messages *list.List
}

// SubReply wraps a Redis reply and provides convienient access to Pub/Sub info.
type SubReply struct {
	Type     SubReplyType // SubReply type
	Channel  string       // Channel reply is on (MessageReply)
	SubCount int          // Count of subs active after this action (SubscribeReply or UnsubscribeReply)
	Message  string       // Publish message (MessageReply)
	Err      error        // SubReply error (ErrorReply)
	Reply    *redis.Reply // Original Redis reply (MessageReply)
}

// Timeout determines if this SubReply is an error type
// due to a timeout reading from the network
func (r *SubReply) Timeout() bool {
	if r.Err == nil {
		return false
	}
	t, ok := r.Err.(*net.OpError)
	return ok && t.Timeout()
}

func NewSubClient(client *redis.Client) *SubClient {
	return &SubClient{client, &list.List{}}
}

// Subscribe makes a Redis "SUBSCRIBE" command on the provided channels
func (c *SubClient) Subscribe(channels ...interface{}) *SubReply {
	return c.filterMessages("SUBSCRIBE", channels...)
}

// PSubscribe makes a Redis "PSUBSCRIBE" command on the provided patterns
func (c *SubClient) PSubscribe(patterns ...interface{}) *SubReply {
	return c.filterMessages("PSUBSCRIBE", patterns...)
}

// Unsubscribe makes a Redis "UNSUBSCRIBE" command on the provided channels
func (c *SubClient) Unsubscribe(channels ...interface{}) *SubReply {
	return c.filterMessages("UNSUBSCRIBE", channels...)
}

// PUnsubscribe makes a Redis "PUNSUBSCRIBE" command on the provided patterns
func (c *SubClient) PUnsubscribe(patterns ...interface{}) *SubReply {
	return c.filterMessages("PUNSUBSCRIBE", patterns...)
}

// Receive returns the next publish reply on the Redis client.  It is possible
// Receive will timeout, and the *SubReply will be an ErrorReply. You can use
// the Timeout() method on SubReply to easily determine if that is the case. If
// this is the case you can call Receive again to continue listening for
// publishes
func (c *SubClient) Receive() *SubReply {
	return c.receive(false)
}

func (c *SubClient) receive(skipBuffer bool) *SubReply {
	if c.messages.Len() > 0 && !skipBuffer {
		v := c.messages.Remove(c.messages.Front())
		return v.(*SubReply)
	}
	r := c.Client.ReadReply()
	return c.parseReply(r)
}

func (c *SubClient) filterMessages(cmd string, names ...interface{}) *SubReply {
	r := c.Client.Cmd(cmd, names...)
	var sr *SubReply
	for i := 0; i < len(names); i++ {
		// If nil we know this is the first loop
		if sr == nil {
			sr = c.parseReply(r)
		} else {
			sr = c.receive(true)
		}
		if sr.Type == MessageReply {
			c.messages.PushBack(sr)
			i--
		}
	}
	return sr
}

func (c *SubClient) parseReply(reply *redis.Reply) *SubReply {
	sr := &SubReply{Reply: reply}
	switch reply.Type {
	case redis.MultiReply:
		if len(reply.Elems) < 3 {
			sr.Err = errors.New("reply is not formatted as a subscription reply")
			return sr
		}
	case redis.ErrorReply:
		sr.Err = reply.Err
		return sr
	default:
		sr.Err = errors.New("reply is not formatted as a subscription reply")
		return sr
	}

	rtype, err := reply.Elems[0].Str()
	if err != nil {
		sr.Err = errors.New("subscription multireply does not have string value for type")
		sr.Type = ErrorReply
		return sr
	}

	//first element
	switch rtype {
	case "subscribe", "psubscribe":
		sr.Type = SubscribeReply
		count, err := reply.Elems[2].Int()
		if err != nil {
			sr.Err = errors.New("subscribe reply does not have int value for sub count")
			sr.Type = ErrorReply
		} else {
			sr.SubCount = count
		}
	case "unsubscribe", "punsubscribe":
		sr.Type = UnsubscribeReply
		count, err := reply.Elems[2].Int()
		if err != nil {
			sr.Err = errors.New("unsubscribe reply does not have int value for sub count")
			sr.Type = ErrorReply
		} else {
			sr.SubCount = count
		}
	case "message", "pmessage":
		sr.Type = MessageReply
		channel, err := reply.Elems[1].Str()
		if err != nil {
			sr.Err = errors.New("subscription multireply does not have string value for channel")
			sr.Type = ErrorReply
			return sr
		}
		sr.Channel = channel
		msg, err := reply.Elems[2].Str()
		if err != nil {
			sr.Err = errors.New("message reply does not have string value for body")
			sr.Type = ErrorReply
		} else {
			sr.Message = msg
		}
	default:
		sr.Err = errors.New("suscription multireply has invalid type: " + rtype)
		sr.Type = ErrorReply
	}
	return sr
}
