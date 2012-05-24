// radix example program.

package main

import (
	"fmt"
	"github.com/fzzbt/radix/redis"
	"strconv"
	"time"
)

// handleReplyError prints an error message for the given reply.
func handleReplyError(rep *redis.Reply) {
	if rep.Error != nil {
		fmt.Println("redis: " + rep.Error.Error())
	} else {
		fmt.Println("redis: unexpected reply type")
	}
}

func main() {
	var c *redis.Client
	var err error

	c, err = redis.NewClient(redis.Configuration{
		Database: 8,
		// Timeout in seconds
		Timeout: 10,

		// Custom TCP/IP address or Unix path.
		// Path: "/tmp/redis.sock",
		// Address: "127.0.0.1:6379",
	})

	if err != nil {
		fmt.Printf("NewClient failed: %s\n", err)
	}

	defer c.Close()

	//** Blocking calls
	rep := c.Flushdb()
	if rep.Error != nil {
		fmt.Printf("redis: %s\n", rep.Error)
		return
	}

	mykeys := map[string]string{
		"mykey1": "myval1",
		"mykey2": "myval2",
		"mykey3": "myval3",
	}

	rep = c.Mset(mykeys)
	// Alternatively:
	// rep = c.Command("mset", "mykey1", "myval1", "mykey2", "myval2", "mykey3", "myval3")

	if rep.Error != nil {	
		fmt.Printf("redis: %s\n", rep.Error)
		return
	}

	rep = c.Get("mykey1")
	switch rep.Type {
	case redis.ReplyString:
		fmt.Printf("mykey1: %s\n", rep.Str())
	case redis.ReplyNil:
		fmt.Println("mykey1 does not exist")
		return
	case redis.ReplyError:
		fmt.Printf("redis: get failed: %s\n", rep.Error)
		return
	default:
		// Shouldn't generally happen
		fmt.Println("redis: unexpected reply type")
		return
	}

	//* Simplified error handling pattern
	rep = c.Get("mykey2")
	if rep.Type != redis.ReplyString {
		handleReplyError(rep)
		return
	}

	fmt.Printf("mykey2: %s\n", rep.Str())

	//* List handling
	mylist := []string{"foo", "bar", "qux"}
	rep = c.Rpush("mylist", mylist)
	// Alternativaly:
	// rep = c.Rpush("mylist", "foo", "bar", "qux")
	if rep.Error != nil {
		handleReplyError(rep)
		return
	}

	rep = c.Lrange("mylist", 0, -1)
	if rep.Error != nil {
		handleReplyError(rep)
		return
	}

	mylist, err = rep.Strings()
	if err != nil {
		handleReplyError(rep)
		return
	}

	fmt.Printf("mylist: %v\n", mylist)

	//* Hash handling
	rep = c.Hmset("myhash", mykeys)
	// Alternatively:
	// rep = c.Hmset("myhash", ""mykey1", "myval1", "mykey2", "myval2", "mykey3", "myval3")
	if rep.Error != nil {
		handleReplyError(rep)
		return
	}

	rep = c.Hgetall("myhash")
	if rep.Error != nil {
		handleReplyError(rep)
		return
	}

	myhash, err := rep.StringMap()
	if err != nil {
		handleReplyError(rep)
		return
	}

	fmt.Printf("myhash: %v\n", myhash)

	//* Multicalls
	rep = c.MultiCall(func(mc *redis.MultiCall) {
		mc.Set("multikey", "multival")
		mc.Get("multikey")
	})

	if rep.Error != nil {
		handleReplyError(rep)
		return
	}

	// Note that you can now assume that rep.Len() == 2.
	// Thus, rep.At(1) will not panic regardless whether all of the commands succeeded.
	if rep.At(1).Type != redis.ReplyString {
		handleReplyError(rep)
		return
	}

	fmt.Printf("multikey: %s\n", rep.At(1).Str())

	//* Transactions
	rep = c.Transaction(func(mc *redis.MultiCall) {
		mc.Set("trankey", "tranval")
		mc.Get("trankey")
	})

	if rep.Error != nil {
		handleReplyError(rep)
		return
	}

	if rep.At(1).Type != redis.ReplyString {
		handleReplyError(rep)
		return
	}

	fmt.Printf("trankey: %s\n", rep.At(1).Str())

	//* Complex transactions
	//  Atomic INCR replacement with transactions
	myIncr := func(key string) *redis.Reply {
		return c.MultiCall(func(mc *redis.MultiCall) {
			var curval int

			mc.Watch(key)
			mc.Get(key)
			rep := mc.Flush()

			if rep.Error != nil {
				return
			}

			if rep.At(1).Type == redis.ReplyString {
				var err error
				curval, err = strconv.Atoi(rep.At(1).Str())
				if err != nil {
					return
				}
			}
			nextval := curval + 1

			mc.Multi()
			mc.Set(key, nextval)
			mc.Exec()
		})
	}

	myIncr("ctrankey")
	myIncr("ctrankey")
	myIncr("ctrankey")

	rep = c.Get("ctrankey")
	if rep.Type != redis.ReplyString {
		handleReplyError(rep)
		return
	}

	fmt.Printf("ctrankey: %s\n", rep.Str())

	//** Asynchronous calls
	rep = c.Set("asynckey", "asyncval")
	if rep.Error != nil {
		handleReplyError(rep)
		return
	}

	fut := c.AsyncGet("asynckey")

	// do something here

	// block until reply is available
	rep = fut.Reply()
	if rep.Type != redis.ReplyString {
		handleReplyError(rep)
		return
	}

	fmt.Printf("asynckey: %s\n", rep.Str())

	//* Pub/sub
	msgHdlr := func(msg *redis.Message) {
		switch msg.Type {
		case redis.MessageMessage:
			fmt.Printf("Received message \"%s\" from channel \"%s\".\n", msg.Payload, msg.Channel)
		case redis.MessagePmessage:
			fmt.Printf("Received pattern message \"%s\" from channel \"%s\" with pattern "+
				"\"%s\".\n", msg.Payload, msg.Channel, msg.Pattern)
		default:
			fmt.Println("Received other message:", msg)
		}
	}

	sub, errr := c.Subscription(msgHdlr)
	if errr != nil {
		fmt.Printf("Failed to subscribe: '%s'!\n", errr)
		return
	}

	defer sub.Close()

	sub.Subscribe("chan1", "chan2")
	sub.Psubscribe("chan*")

	c.Publish("chan1", "foo")
	sub.Unsubscribe("chan1")
	c.Publish("chan2", "bar")

	// give some time for the message handler to receive the messages
	time.Sleep(time.Second)
}
