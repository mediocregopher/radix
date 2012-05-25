package main

import (
    "github.com/fzzbt/radix/redis"
)

var key string = "foo:rand:000000000000"
var tests = make(map[string]func(string, *redis.Client, chan struct{}))

func init() {
    tests["set"] = setHandle
	tests["get"] = getHandle
}

func setHandle(data string, c *redis.Client, ch chan struct{}) {
    for _ = range ch {
        c.Set(key, data)
    }
}

func getHandle(data string, c *redis.Client, ch chan struct{}) {
    for _ = range ch {
        c.Set(key, data)
    }
}