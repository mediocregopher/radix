// Package util is a collection of helper functions for interacting with various
// parts of the radix.v2 package
package util

import "github.com/mediocregopher/radix.v2/redis"

// Cmder is an interface which can be used to interfchanbeably work with either
// redis.Client (the basic, single connection redis client), pool.Pool, or
// cluster.Cluster. All three implement a Cmd method (although, as is the case
// with Cluster, sometimes with different limitations), and therefore all three
// are Cmders
type Cmder interface {
	Cmd(cmd string, args ...interface{}) *redis.Resp
}
