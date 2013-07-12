package main

import (
	"flag"
	"fmt"
	"github.com/fzzy/radix/redis"
	"os"
	"runtime/pprof"
	"time"
)

var requests *int = flag.Int("n", 10000, "number of request")
var dsize *int = flag.Int("d", 3, "data size")
var cpuprof *string = flag.String("cpuprof", "", "filename for cpuprof")
var database *int = flag.Int("b", 8, "database used for testing (WILL BE FLUSHED!)")

func errHndlr(err error) {
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
}

// benchmark benchmarks the given command with the given parameters
// and displays the given test name.
func benchmark(c *redis.Client, testname string, command string, params ...interface{}) {
	fmt.Printf("===== %s =====\n", testname)
	start := time.Now()

	for i := 0; i < *requests; i++ {
		c.Cmd(command, params...)
	}

	duration := time.Now().Sub(start)
	rps := float64(*requests) / duration.Seconds()
	fmt.Println("Requests per second:", rps)
	fmt.Printf("Duration: %v\n\n", duration.Seconds())
}

func testIsSelected(args []string, name string) bool {
	for _, v := range args {
		if name == v {
			return true
		}
	}
	return false
}

func main() {
	var data string

	flag.Parse()
	// minimum amount of requests
	if *requests < 1000 {
		*requests = 1000
	}

	if *cpuprof != "" {
		f, err := os.Create(*cpuprof)
		if err != nil {
			fmt.Println(err)
			return
		}

		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	for i := 0; i < *dsize; i++ {
		data += "x"
	}

	c, err := redis.Dial("tcp", "127.0.0.1:6379")
	errHndlr(err)

	// select database
	r := c.Cmd("select", *database)
	errHndlr(err)

	r = c.Cmd("flushdb")
	errHndlr(r.Err)

	fmt.Printf(
		"Requests: %d, Payload: %d byte \n\n",
		*requests,
		*dsize)

	args := flag.Args()
	if len(args) == 0 {
		// run all tests by default
		args = []string{
			"ping",
			"set",
			"get",
			"incr",
			"lpush",
			"lpop",
			"sadd",
			"spop",
			"lrange",
			"lrange_100",
			"lrange_300",
			"lrange_450",
			"lrange_600",
			"mset",
		}
	}

	if testIsSelected(args, "ping") {
		benchmark(c, "PING", "ping")
	}
	if testIsSelected(args, "set") {
		benchmark(c, "SET", "set", "foo:rand:000000000000", data)
	}
	if testIsSelected(args, "get") {
		benchmark(c, "GET", "get", "foo:rand:000000000000")
	}
	if testIsSelected(args, "incr") {
		benchmark(c, "INCR", "incr", "counter:rand:000000000000")
	}
	if testIsSelected(args, "lpush") {
		benchmark(c, "LPUSH", "lpush", "mylist", data)
	}
	if testIsSelected(args, "lpop") {
		benchmark(c, "LPOP", "lpop", "mylist")
	}
	if testIsSelected(args, "sadd") {
		benchmark(c, "SADD", "sadd", "myset", "counter:rand:000000000000")
	}
	if testIsSelected(args, "spop") {
		benchmark(c, "SPOP", "spop", "myset")
	}
	if testIsSelected(args, "lrange") ||
		testIsSelected(args, "lrange_100") ||
		testIsSelected(args, "lrange_300") ||
		testIsSelected(args, "lrange_450") ||
		testIsSelected(args, "lrange_600") {
		benchmark(c, "LPUSH (needed to benchmark LRANGE)", "lpush", "mylist", data)
	}
	if testIsSelected(args, "lrange") || testIsSelected(args, "lrange_100") {
		benchmark(c, "LRANGE_100", "lrange", "mylist", 0, 99)
	}
	if testIsSelected(args, "lrange") || testIsSelected(args, "lrange_300") {
		benchmark(c, "LRANGE_300", "lrange", "mylist", 0, 299)
	}
	if testIsSelected(args, "lrange") || testIsSelected(args, "lrange_450") {
		benchmark(c, "LRANGE_450", "lrange", "mylist", 0, 449)
	}
	if testIsSelected(args, "lrange") || testIsSelected(args, "lrange_600") {
		benchmark(c, "LRANGE_600", "lrange", "mylist", 0, 599)
	}
	if testIsSelected(args, "mset") {
		args := make([]interface{}, 20)
		for i := 0; i < 20; i += 2 {
			args[i] = "foo:rand:000000000000"
			args[i+1] = data
		}
		benchmark(c, "MSET", "mset", args...)
	}

	r = c.Cmd("flushdb")
	errHndlr(r.Err)
}
