// example program

package main

import (
	"fmt"
	"github.com/fzzy/radix/redis"
	"os"
	"time"
)

func errHndlr(err error) {
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
}

func main() {
	conf := redis.DefaultConfig()
	conf.Database = 8                              // Database number 
	conf.Timeout = time.Duration(10) * time.Second // Socket timeout
	c, err := redis.Dial("tcp", "127.0.0.1:6379", conf)
	errHndlr(err)
	defer c.Close()

	r := c.Call("flushdb")
	errHndlr(r.Err)

	r = c.Call("echo", "Hello world!")
	errHndlr(r.Err)

	s, err := r.Str()
	errHndlr(err)
	fmt.Println("echo:", s)

	//* Strings
	r = c.Call("set", "mykey0", "myval0")
	errHndlr(r.Err)

	s, err = c.Call("get", "mykey0").Str()
	errHndlr(err)
	fmt.Println("mykey0:", s)

	myhash := map[string]string{
		"mykey1": "myval1",
		"mykey2": "myval2",
		"mykey3": "myval3",
	}

	// Alternatively:
	// c.Call("mset", "mykey1", "myval1", "mykey2", "myval2", "mykey3", "myval3")
	r = c.Call("mset", myhash)
	errHndlr(r.Err)

	ls, err := c.Call("mget", "mykey1", "mykey2", "mykey3").List()
	errHndlr(err)
	fmt.Println("mykeys values:", ls)

	//* List handling
	mylist := []string{"foo", "bar", "qux"}

	// Alternativaly:
	// c.Call("rpush", "mylist", "foo", "bar", "qux")
	r = c.Call("rpush", "mylist", mylist)
	errHndlr(r.Err)

	mylist, err = c.Call("lrange", "mylist", 0, -1).List()
	errHndlr(err)

	fmt.Println("mylist:", mylist)

	//* Hash handling

	// Alternatively:
	// c.Call("hmset", "myhash", ""mykey1", "myval1", "mykey2", "myval2", "mykey3", "myval3")
	r = c.Call("hmset", "myhash", myhash)
	errHndlr(r.Err)

	myhash, err = c.Call("hgetall", "myhash").Hash()
	errHndlr(err)

	fmt.Println("myhash:", myhash)

	//* Pipelining
	c.Append("set", "multikey", "multival")
	c.Append("get", "multikey")

	c.GetReply()     // set
	r = c.GetReply() // get
	errHndlr(r.Err)

	s, err = r.Str()
	errHndlr(err)
	fmt.Println("multikey:", s)

	/*
		 TODO: scripting
		       Pub/sub

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

		sub, err_ := c.Subscription(msgHdlr)
		if err_ != nil {
			fmt.Printf("Failed to subscribe: '%s'!\n", err_)
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
	*/
}
