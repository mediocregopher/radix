package main

import (
	"github.com/fzzbt/radix/redis"
)

var key string = "foo:rand:000000000000"
var testNames = []string{}
var testHandles = []func(string, *redis.Client, chan struct{}){}

func init() {
	testNames = append(testNames, "ping")
	testHandles = append(testHandles, pingHandle)

	testNames = append(testNames, "set")
	testHandles = append(testHandles, setHandle)

	testNames = append(testNames, "get")
	testHandles = append(testHandles, getHandle)
}

func pingHandle(data string, c *redis.Client, ch chan struct{}) {
	for _ = range ch {
		c.Ping()
	}
}

func setHandle(data string, c *redis.Client, ch chan struct{}) {
	for _ = range ch {
		c.Set(key, data)
	}
}

func getHandle(data string, c *redis.Client, ch chan struct{}) {
	for _ = range ch {
		c.Get(key)
	}
}
