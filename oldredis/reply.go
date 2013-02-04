package redis

import (
	"errors"
	"strconv"
)

//* Reply

/*
ReplyType describes type of a reply.

Possbile values are:

ReplyStatus --  status reply
ReplyError -- error reply
ReplyInteger -- integer reply
ReplyNil -- nil reply
ReplyString -- string reply (bulk reply)
ReplyMulti -- multi-bulk or multicall reply
*/
type ReplyType uint8

const (
	ReplyStatus ReplyType = iota
	ReplyError
	ReplyInteger
	ReplyNil
	ReplyString
	ReplyMulti
)

// Reply holds a Redis reply.
type Reply struct {
	Type  ReplyType // Reply type
	Elems []*Reply  // Sub-replies
	Err   *Error    // Reply error
	str   string
	int   int64
}

// Str returns the reply value as a string or
// an error, if the reply type is not ReplyStatus or ReplyString.
func (r *Reply) Str() (string, error) {
	if r.Type == ReplyError {
		return "", r.Err
	}
	if !(r.Type == ReplyStatus || r.Type == ReplyString) {
		return "", errors.New("string value is not available for this reply type")
	}

	return r.str, nil
}

// Bytes is a convenience method for calling Reply.Str() and converting it to []byte.
func (r *Reply) Bytes() ([]byte, error) {
	if r.Type == ReplyError {
		return nil, r.Err
	}
	s, err := r.Str()
	if err != nil {
		return nil, err
	}

	return []byte(s), nil
}

// Int64 returns the reply value as a int64 or an error,
// if the reply type is not ReplyInteger or the reply type
// ReplyString could not be parsed to an int64.
func (r *Reply) Int64() (int64, error) {
	if r.Type == ReplyError {
		return 0, r.Err
	}
	if r.Type != ReplyInteger {
		if r.Type == ReplyString {
			i64, err := strconv.ParseInt(r.str, 10, 64)
			if err != nil {
				return 0, errors.New("failed to parse integer value from string value")
			} else {
				return i64, nil
			}
		}

		return 0, errors.New("integer value is not available for this reply type")
	}

	return r.int, nil
}

// Int is a convenience method for calling Reply.Int64() and converting it to int.
func (r *Reply) Int() (int, error) {
	if r.Type == ReplyError {
		return 0, r.Err
	}
	i64, err := r.Int64()
	if err != nil {
		return 0, err
	}

	return int(i64), nil
}

// Bool returns true, if the reply value equals to 1 or "1", otherwise false; or
// an error, if the reply type is not ReplyInteger or ReplyString.
func (r *Reply) Bool() (bool, error) {
	if r.Type == ReplyError {
		return false, r.Err
	}
	i, err := r.Int()
	if err == nil {
		if i == 1 {
			return true, nil
		}

		return false, nil
	}

	s, err := r.Str()
	if err == nil {
		if s == "1" {
			return true, nil
		}

		return false, nil
	}

	return false, errors.New("boolean value is not available for this reply type")
}

// List returns a multi-bulk reply as a slice of strings or an error.
// The reply type must be ReplyMulti and its elements' types must all be either ReplyString or ReplyNil.
// Nil elements are returned as empty strings.
// Useful for list commands.
func (r *Reply) List() ([]string, error) {
	if r.Type == ReplyError {
		return nil, r.Err
	}
	if r.Type != ReplyMulti {
		return nil, errors.New("reply type is not ReplyMulti")
	}

	strings := make([]string, len(r.Elems))
	for i, v := range r.Elems {
		if v.Type == ReplyString {
			strings[i] = v.str
		} else if v.Type == ReplyNil {
			strings[i] = ""
		} else {
			return nil, errors.New("element type is not ReplyString or ReplyNil")
		}
	}

	return strings, nil
}

// Map returns a multi-bulk reply as a map[string]string or an error.
// The reply type must be ReplyMulti, 
// it must have an even number of elements,
// they must be in a "key value key value..." order and
// values must all be either ReplyString or ReplyNil.
// Nil values are returned as empty strings.
// Useful for hash commands.
func (r *Reply) Hash() (map[string]string, error) {
	if r.Type == ReplyError {
		return nil, r.Err
	}
	rmap := map[string]string{}

	if r.Type != ReplyMulti {
		return nil, errors.New("reply type is not ReplyMulti")
	}

	if len(r.Elems)%2 != 0 {
		return nil, errors.New("reply has odd number of elements")
	}

	for i := 0; i < len(r.Elems)/2; i++ {
		var val string

		key, err := r.Elems[i*2].Str()
		if err != nil {
			return nil, errors.New("key element has no string reply")
		}

		v := r.Elems[i*2+1]
		if v.Type == ReplyString {
			val = v.str
			rmap[key] = val
		} else if v.Type == ReplyNil {
		} else {
			return nil, errors.New("value element type is not ReplyString or ReplyNil")
		}
	}

	return rmap, nil
}

// String returns a string representation of the reply and its sub-replies.
// This method is mainly used for debugging.
// Use method Reply.Str() for fetching a string reply.
func (r *Reply) String() string {
	switch r.Type {
	case ReplyError:
		return r.Err.Error()
	case ReplyStatus:
		fallthrough
	case ReplyString:
		return r.str
	case ReplyInteger:
		return strconv.FormatInt(r.int, 10)
	case ReplyNil:
		return "<nil>"
	case ReplyMulti:
		s := "[ "
		for _, e := range r.Elems {
			s = s + e.String() + " "
		}
		return s + "]"
	}

	// This should never execute
	return ""
}

//* Future

// Future is a channel for fetching the reply of an asynchronous call.
type Future chan *Reply

func newFuture() Future {
	return make(chan *Reply, 1)
}

// Reply returns the reply of the Future.
// It blocks until the reply is available.
func (f Future) Reply() *Reply {
	return <-f
}
