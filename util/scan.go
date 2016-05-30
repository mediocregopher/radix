package util

import (
	"errors"
	"strings"

	"github.com/mediocregopher/radix.v2/cluster"
	"github.com/mediocregopher/radix.v2/redis"
)

// ScanOpts are various parameters which can be passed into ScanWithOpts. Some
// fields are required depending on which type of scan is being done.
type ScanOpts struct {
	// The scan command to do, e.g. "SCAN", "HSCAN", etc...
	Command string

	// The key to perform the scan on. Only necessary when Command isn't "SCAN"
	Key string

	// An optional pattern to filter returned keys by
	Pattern string

	// An optional count hint to send to redis to indicate number of keys to
	// return per call. This does not affect the actual results of the scan
	// command, but it may be useful for optimizing certain datasets
	Count int
}

func doScanPart(c Cmder, o ScanOpts, cursor string) (string, []string, error) {
	cmd := strings.ToUpper(o.Command)
	args := make([]interface{}, 0, 4)
	if cmd != "SCAN" {
		args = append(args, o.Key)
	}
	args = append(args, cursor)
	if o.Pattern != "" {
		args = append(args, "MATCH", o.Pattern)
	}
	if o.Count > 0 {
		args = append(args, "COUNT", o.Count)
	}
	parts, err := c.Cmd(cmd, args...).Array()
	if err != nil {
		return "", nil, err
	}

	if len(parts) < 2 {
		return "", nil, errors.New("not enough parts returned")
	}

	if cursor, err = parts[0].Str(); err != nil {
		return "", nil, err
	}

	var keys []string
	if keys, err = parts[1].List(); err != nil {
		return "", nil, err
	}

	return cursor, keys, nil
}

func scanSingle(r Cmder, ch chan string, o ScanOpts) error {
	defer close(ch)

	var cursor string
	var keys []string
	var err error
	for {
		if cursor, keys, err = doScanPart(r, o, cursor); err != nil {
			return err
		}

		for i := range keys {
			if keys[i] != "" {
				ch <- keys[i]
			}
		}

		if cursor == "0" {
			return nil
		}
	}
}

// scanCluster is like Scan except it operates over a whole cluster. Unlike Scan
// it only works with SCAN and as such only takes in a pattern string.
func scanCluster(c *cluster.Cluster, ch chan string, o ScanOpts) error {
	defer close(ch)
	clients, err := c.GetEvery()
	if err != nil {
		return err
	}
	for _, client := range clients {
		defer c.Put(client)
	}

	for _, client := range clients {
		cch := make(chan string)
		var err error
		go func() {
			err = scanSingle(client, cch, o)
		}()
		for key := range cch {
			ch <- key
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// Scan is DEPRECATED. It contains an inherent race-condition and shouldn't be
// used. Use NewScanner instead.
//
// Scan is a helper function for performing any of the redis *SCAN functions. It
// takes in a channel which keys returned by redis will be written to, and
// returns an error should any occur. The input channel will always be closed
// when Scan returns, and *must* be read until it is closed.
//
// The key argument is only needed if cmd isn't SCAN
//
// Example SCAN command
//
//	ch := make(chan string)
//	var err error
//	go func() {
//		err = util.Scan(r, ch, "SCAN", "", "*")
//	}()
//	for key := range ch {
//		// do something with key
//	}
//	if err != nil {
//		// handle error
//	}
//
// Example HSCAN command
//
//	ch := make(chan string)
//	var err error
//	go func() {
//		err = util.Scan(r, ch, "HSCAN", "somekey", "*")
//	}()
//	for key := range ch {
//		// do something with key
//	}
//	if err != nil {
//		// handle error
//	}
//
func Scan(r Cmder, ch chan string, cmd, key, pattern string) error {
	// We're using ScanOpts here for esoteric reasons, this whole thing is
	// deprecated anyway, so who cares.
	o := ScanOpts{
		Command: cmd,
		Key:     key,
		Pattern: pattern,
	}
	if rr, ok := r.(*cluster.Cluster); ok && strings.ToUpper(o.Command) == "SCAN" {
		return scanCluster(rr, ch, o)
	}
	var cmdErr error
	err := withClientForKey(r, o.Key, func(c Cmder) {
		cmdErr = scanSingle(r, ch, o)
	})
	if err != nil {
		return err
	}
	return cmdErr
}

////////////////////////////////////////////////////////////////////////////////

// Scanner is used to iterate through the results of a SCAN call (or HSCAN,
// SSCAN, etc...). The Cmder may be a Client, Pool, or Cluster.
//
// Once created, call HasNext() on it to determine if there's a waiting value,
// then Next() to retrieve that value, then repeat. Once HasNext() returns
// false, call Err() to potentially retrieve an error which stopped the
// iteration.
//
// Example SCAN command
//
//	s := util.NewScanner(cmder, util.ScanOpts{Command: "SCAN"})
//	for s.HasNext() {
//		log.Printf("next: %q", s.Next())
//	}
//	if err := s.Err(); err != nil {
//		log.Fatal(err)
//	}
//
// Example HSCAN command
//
//	s := util.NewScanner(cmder, util.ScanOpts{Command: "HSCAN", Key: "somekey"})
//	for s.HasNext() {
//		log.Printf("next: %q", s.Next())
//	}
//	if err := s.Err(); err != nil {
//		log.Fatal(err)
//	}
//
// HasNext MUST be called before every call to Next. Err MUST be called after
// HasNext returns false.
type Scanner interface {
	HasNext() bool
	Next() string
	Err() error
}

type singleScanner struct {
	c Cmder
	o ScanOpts

	err    error
	cursor string
	buf    []string
}

// NewScanner initializes a Scanner struct with the given options and returns
// it.
func NewScanner(c Cmder, o ScanOpts) Scanner {
	if cc, ok := c.(*cluster.Cluster); ok && strings.ToUpper(o.Command) == "SCAN" {
		return &clusterScanner{c: cc, o: o}
	}
	return &singleScanner{
		c: c,
		o: o,
	}
}

func (s *singleScanner) HasNext() bool {
	for {
		if s.err != nil {
			return false
		}

		if len(s.buf) > 0 {
			if s.buf[0] == "" {
				s.buf = s.buf[1:]
				continue
			}
			return true
		}

		if s.cursor == "0" {
			return false
		}

		s.cursor, s.buf, s.err = doScanPart(s.c, s.o, s.cursor)
	}
}

func (s *singleScanner) Next() string {
	// we assume they called HasNext first, which garauntees that this won't
	// panic
	ret := s.buf[0]
	s.buf = s.buf[1:]
	return ret
}

func (s *singleScanner) Err() error {
	return s.err
}

type clusterScanner struct {
	c *cluster.Cluster
	o ScanOpts

	err         error
	clients     []*redis.Client
	currScanner Scanner
}

func (cs *clusterScanner) HasNext() bool {
	// if clients is nil then HasNext has never been called, and we need to get
	// some clients
	if cs.clients == nil {
		clientsM, err := cs.c.GetEvery()
		if err != nil {
			cs.err = err
			return false
		}

		cs.clients = make([]*redis.Client, 0, len(clientsM))
		for _, client := range clientsM {
			cs.clients = append(cs.clients, client)
		}
	}

	for {
		if len(cs.clients) == 0 || cs.err != nil {
			return false
		} else if cs.currScanner == nil {
			cs.currScanner = NewScanner(cs.clients[0], cs.o)
		}

		if cs.currScanner.HasNext() {
			return true
		}

		// if err isn't nil, cleanup will happen when Err is called on
		// clusterScanner
		cs.err = cs.currScanner.Err()
		cs.currScanner = nil
		cs.c.Put(cs.clients[0])
		cs.clients = cs.clients[1:]
	}
}

func (cs *clusterScanner) Next() string {
	return cs.currScanner.Next()
}

func (cs *clusterScanner) Err() error {
	// if Err is being called it means iteration is done and we should put back
	// all of the clients that still haven't been put back
	for _, client := range cs.clients {
		cs.c.Put(client)
	}
	cs.clients = nil
	return cs.err
}
