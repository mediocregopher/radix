package main

import (
	"crypto/rand"
	"encoding/hex"
	"io"
	"log"
	"time"

	"github.com/mediocregopher/radix.v2/cluster"
)

func randString() string {
	b := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		log.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func main() {
	c, err := cluster.New("localhost:7000")
	if err != nil {
		log.Fatal(err)
	}

	oldKeys := make(chan string, 1000)

	doRand := time.Tick(100 * time.Millisecond)
	doOldRand := time.Tick(1 * time.Second)

	for {
		select {
		case <-doRand:
			key := randString()
			doGetSet(c, key)
			select {
			case oldKeys <- key:
			default:
			}

		case <-doOldRand:
			select {
			case key := <-oldKeys:
				doGetSet(c, key)
			default:
			}
		}
	}
}

func doGetSet(c *cluster.Cluster, key string) {
	if err := c.Cmd("GET", key).Err; err != nil {
		log.Printf("GET %s -> %s", key, err)
	}
	val := randString()
	if err := c.Cmd("SET", key, val).Err; err != nil {
		log.Printf("SET %s %s -> %s", key, val, err)
	}
}
