package main

import (
	"flag"
	"fmt"
	"github.com/fzzbt/radix/redis"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
)

var connections *int = flag.Int("c", 50, "number of connections")
var requests *int = flag.Int("n", 10000, "number of request")
var dsize *int = flag.Int("d", 3, "data size")
var cpuprof *string = flag.String("cpuprof", "", "filename for cpuprof")
var gomaxprocs *int = flag.Int("p", 8, "GOMAXPROCS value")
var database *int = flag.Int("b", 8, "database used for testing (WILL BE FLUSHED!)")
var conf redis.Config = redis.DefaultConfig()

func flushdb() *redis.Error {
	c := redis.NewClient(conf)
	defer c.Close()
	rep := c.Flushdb()
	return rep.Err
}

func test(c *redis.Client, ch chan struct{}, doneChan chan struct{}, command string, params ...interface{}) {
	for _ = range ch {
		r := c.Call(command, params...)
		if r.Err != nil {
			fmt.Println(r.Err)
			os.Exit(1)
		}
	}
	doneChan <- struct{}{}
}

// benchmark benchmarks the given command with the given parameters 
// and displays the given test name.
func benchmark(testname string, command string, params ...interface{}) {
	fmt.Printf("===== %s =====\n", testname)
	c := redis.NewClient(conf)
	defer c.Close()

	ch := make(chan struct{})
	doneChan := make(chan struct{})
	start := time.Now()

	for i := 0; i < *connections; i++ {
		go test(c, ch, doneChan, command, params...)
	}
	for i := 0; i < *requests; i++ {
		ch <- struct{}{}
	}
	close(ch)
	for i := 0; i < *connections; i++ {
		<-doneChan
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
	conf.Database = *database
	conf.PoolCapacity = *connections
	// minimum amount of requests
	if *requests < 1000 {
		*requests = 1000
	}
	runtime.GOMAXPROCS(*gomaxprocs)

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

	err := flushdb()
	if err != nil {
		fmt.Println("FLUSHDB failed:", err)
		return
	}

	fmt.Printf(
		"Connections: %d, Requests: %d, Payload: %d bytes, GOMAXPROCS: %d\n\n",
		*connections,
		*requests,
		*dsize,
		*gomaxprocs)

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
		benchmark("PING", "ping")
	}
	if testIsSelected(args, "set") {
		benchmark("SET", "set", "foo:rand:000000000000", data)
	}
	if testIsSelected(args, "get") {
		benchmark("GET", "get", "foo:rand:000000000000")
	}
	if testIsSelected(args, "incr") {
		benchmark("INCR", "incr", "counter:rand:000000000000")
	}
	if testIsSelected(args, "lpush") {
		benchmark("LPUSH", "lpush", "mylist", data)
	}
	if testIsSelected(args, "lpop") {
		benchmark("LPOP", "lpop", "mylist")
	}
	if testIsSelected(args, "sadd") {
		benchmark("SADD", "sadd", "myset", "counter:rand:000000000000")
	}
	if testIsSelected(args, "spop") {
		benchmark("SPOP", "spop", "myset")
	}
	if testIsSelected(args, "lrange") ||
		testIsSelected(args, "lrange_100") ||
		testIsSelected(args, "lrange_300") ||
		testIsSelected(args, "lrange_450") ||
		testIsSelected(args, "lrange_600") {
		benchmark("LPUSH (needed to benchmark LRANGE)", "lpush", "mylist", data)
	}
	if testIsSelected(args, "lrange") || testIsSelected(args, "lrange_100") {
		benchmark("LRANGE_100", "lrange", "mylist", 0, 99)
	}
	if testIsSelected(args, "lrange") || testIsSelected(args, "lrange_300") {
		benchmark("LRANGE_300", "lrange", "mylist", 0, 299)
	}
	if testIsSelected(args, "lrange") || testIsSelected(args, "lrange_450") {
		benchmark("LRANGE_450", "lrange", "mylist", 0, 449)
	}
	if testIsSelected(args, "lrange") || testIsSelected(args, "lrange_600") {
		benchmark("LRANGE_600", "lrange", "mylist", 0, 599)
	}
	if testIsSelected(args, "mset") {
		args := make([]interface{}, 20)
		for i := 0; i < 20; i += 2 {
			args[i] = "foo:rand:000000000000"
			args[i+1] = data
		}
		benchmark("MSET", "mset", args...)
	}
	flushdb()
}
