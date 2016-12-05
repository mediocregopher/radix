package radix

import (
	"errors"
	"strings"
)

// Scanner is used to iterate through the results of a SCAN call (or HSCAN,
// SSCAN, etc...)
//
// Once created, repeatedly call Next() on it to fill the passed in string
// pointer with the next result. Next will return false if there's no more
// results to retrieve or if an error occurred, at which point Close should be
// called to retrieve any error.
type Scanner interface {
	Next(*string) bool
	Close() error
}

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

func (o ScanOpts) cmd(cursor string) RawCmd {
	cmdStr := strings.ToUpper(o.Command)
	cmd := RawCmd{Cmd: []byte(cmdStr)}
	if cmdStr != "SCAN" {
		cmd.Keys = [][]byte{[]byte(o.Key)}
	}
	cmd.Args = append(cmd.Args, cursor)
	if o.Pattern != "" {
		cmd.Args = append(cmd.Args, "MATCH", o.Pattern)
	}
	if o.Count > 0 {
		cmd.Args = append(cmd.Args, "COUNT", o.Count)
	}
	return cmd
}

// ScanAllKeys is a shortcut ScanOpts which can be used to scan all keys
var ScanAllKeys = ScanOpts{
	Command: "SCAN",
}

type scanner struct {
	Conn
	ScanOpts
	res []string
	cur string
	err error
}

// NewScanner creates a new Scanner instance which will iterate over the redis
// instance's Conn using the ScanOpts.
//
// Example SCAN command
//
//	s := radix.NewScanner(conn, radix.ScanAllKeys)
//	var key string
//	for s.Next(&key) {
//		log.Printf("key: %q", key)
//	}
//	if err := s.Err(); err != nil {
//		log.Fatal(err)
//	}
//
// Example HSCAN command
//
//	s := radix.NewScanner(conn, radix.ScanOpts{Command: "HSCAN", Key: "somekey"})
//	var key string
//	for s.Next(&key) {
//		log.Printf("key", s.Next())
//	}
//	if err := s.Err(); err != nil {
//		log.Fatal(err)
//	}
//
func NewScanner(c Conn, o ScanOpts) Scanner {
	return &scanner{
		Conn:     c,
		ScanOpts: o,
		cur:      "0",
	}
}

func (s *scanner) Next(res *string) bool {
	for {
		if s.err != nil {
			return false
		}

		if len(s.res) > 0 {
			*res, s.res = s.res[0], s.res[1:]
			if *res == "" {
				continue
			}
			return true
		}

		if s.cur == "0" && s.res != nil {
			return false
		}

		var parts []interface{}
		cmd := s.cmd(s.cur).Into(&parts)
		if s.err = cmd.Run(s.Conn); s.err != nil {
			return false
		} else if len(parts) < 2 {
			s.err = errors.New("not enough parts returned")
			return false
		} else if s.res == nil {
			s.res = make([]string, 0, len(parts[1].([]interface{})))
		}
		s.cur = string(parts[0].([]byte))
		s.res = s.res[:0]
		for _, res := range parts[1].([]interface{}) {
			s.res = append(s.res, string(res.([]byte)))
		}
	}
}

func (s *scanner) Close() error {
	return s.err
}
