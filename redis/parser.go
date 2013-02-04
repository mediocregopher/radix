package redis

import (
	"bufio"
	"bytes"
	"errors"
	"strconv"
)

//* Common errors

var AuthError error = errors.New("authentication failed")
var LoadingError error = errors.New("server is busy loading dataset in memory")
var ParseError error = errors.New("parse error")
var TimeoutError error = errors.New("timeout error")

// Parse reads data from the given Reader and constructs a Reply.
func parse(reader *bufio.Reader) (r *Reply) {
	r = new(Reply)
	b, err := reader.ReadBytes('\n')
	if err != nil {
		r.Type = ErrorReply
		r.Err = err
		return
	}

	fb := b[0]
	b = b[1 : len(b)-2] // get rid of the first byte and the trailing \r\n
	switch fb {
	case '-':
		// error reply
		r.Type = ErrorReply
		if bytes.HasPrefix(b, []byte("LOADING")) {
			r.Err = LoadingError
		} else {
			r.Err = errors.New(string(b))
		}
	case '+':
		// status reply
		r.Type = StatusReply
		r.str = string(b)
	case ':':
		// integer reply
		i, err := strconv.ParseInt(string(b), 10, 64)
		if err != nil {
			r.Type = ErrorReply
			r.Err = ParseError
		} else {
			r.Type = IntegerReply
			r.int = i
		}
	case '$':
		// bulk reply
		i, err := strconv.Atoi(string(b))
		if err != nil {
			r.Type = ErrorReply
			r.Err = ParseError
		} else {
			if i == -1 {
				// null bulk reply (key not found)
				r.Type = NilReply
			} else {
				// bulk reply
				ir := i + 2
				br := make([]byte, ir)
				rc := 0

				for rc < ir {
					n, err := reader.Read(br[rc:])
					if err != nil {
						r.Type = ErrorReply
						r.Err = err
					}
					rc += n
				}
				s := string(br[0:i])
				r.Type = BulkReply
				r.str = s
			}
		}
	case '*':
		// multi bulk reply
		i, err := strconv.Atoi(string(b))
		if err != nil {
			r.Type = ErrorReply
			r.Err = ParseError
		} else {
			switch {
			case i == -1:
				// null multi bulk
				r.Type = NilReply
			case i >= 0:
				// multi bulk
				// parse the replies recursively
				r.Type = MultiReply
				r.Elems = make([]*Reply, i)
				for i := range r.Elems {
					r.Elems[i] = parse(reader)
				}
			default:
				// invalid multi bulk reply
				r.Type = ErrorReply
				r.Err = ParseError
			}
		}
	default:
		// invalid reply
		r.Type = ErrorReply
		r.Err = ParseError
	}
	return
}
