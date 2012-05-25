package main

import (
    "github.com/fzzbt/radix/redis"
	"flag"
	"fmt"
	"time"
	"os"
	"strings"
)

var connections *int = flag.Int("c", 50, "number of connections")
var requests *int = flag.Int("n", 10000, "number of request")
var dsize *int = flag.Int("d", 3, "data size")
//var cpuprof *string = flag.String("cpuprof", "", "filename for cpuprof")
//var memprof *string = flag.String("memprof", "", "filename for memprof")


// handleReplyError prints an error message for the given reply.
func handleReplyError(rep *redis.Reply) {
	if rep.Error != nil {
		fmt.Println("redis: " + rep.Error.Error())
	} else {
		fmt.Println("redis: unexpected reply type")
	}
}

// benchmark benchmarks the given function.
func benchmark(data string, handle func(string, *redis.Client, chan struct{})) time.Duration {
	c, err := redis.NewClient(redis.Configuration{
		Database: 8,
		Path: "/tmp/redis.sock",
		PoolSize: *connections,
	})

	if err != nil {
		fmt.Printf("NewClient failed: %s\n", err)
		os.Exit(1)
	}

	rep := c.Flushdb()
	if rep.Error != nil {
		handleReplyError(rep)
		os.Exit(1)
	}

	ch := make(chan struct{})
	start := time.Now()
	
    for i := 0; i < *connections; i++ {
        go handle(data, c, ch)
    }

    for i := 0; i < *requests; i++ {
        ch <- struct{}{}
    }

	dur := time.Now().Sub(start)
	c.Close()
	return dur
}

func run(name string, handle func(string, *redis.Client, chan struct{}), data string) {
    fmt.Printf("===== %s =====\n", strings.ToUpper(name))
	duration := benchmark(data, handle)
	rps := float64(*requests) / duration.Seconds()
	fmt.Println("Requests per second: ", rps)
	fmt.Println("Duration: ", duration)
	fmt.Println()
}

func main() {
	var data string

    flag.Parse()

	for i := 0; i < *dsize; i++ {
		data += "x"
	}

    fmt.Printf(
		"Connections: %d, Requests: %d Payload size: %d\n\n", 
		*connections, 
		*requests, 
		*dsize)

	args := flag.Args()
	if len(args) == 0 {
		// run all tests by default
		for i, name := range testNames {
			run(name, testHandles[i], data)
		}
	} else {
		for i, name := range testNames {
			for _, arg := range args {
				if arg == name {
					run(name, testHandles[i], data)
				}
			}
		}
	}
}
