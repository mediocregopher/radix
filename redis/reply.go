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
	Type  ReplyType
	Elems []*Reply // Sub-replies
	Err   *Error
	str   string
	int   int64
}

// Nil returns true, if the reply is a nil reply, otherwise false.
func (r *Reply) Nil() bool {
	if r.Type == ReplyNil {
		return true
	}

	return false
}

// Str returns the reply value as a string or
// an error, if the reply type is not ReplyStatus or ReplyString.
func (r *Reply) Str() (string, error) {
	if !(r.Type == ReplyStatus || r.Type == ReplyString) {
		return "", errors.New("string value is not available for this reply type")
	}

	return r.str, nil
}

// Bytes is a convenience method for calling Reply.Str() and converting it to []byte.
func (r *Reply) Bytes() ([]byte, error) {
	s, err := r.Str()
	if err != nil {
		return nil, err
	}

	return []byte(s), nil
}

// Int64 returns the reply value as a int64 or
// an error, if the reply type is not ReplyInteger.
func (r *Reply) Int64() (int64, error) {
	if r.Type != ReplyInteger {
		return 0, errors.New("integer value is not available for this reply type")
	}

	return r.int, nil
}

// Int is a convenience method for calling Reply.Int64() and converting it to int.
func (r *Reply) Int() (int, error) {
	i64, err := r.Int64()
	if err != nil {
		return 0, err
	}

	return int(i64), nil
}

// Bool returns true, if the reply value equals to 1 or "1", otherwise false; or
// an error, if the reply type is not ReplyInteger or ReplyString.
func (r *Reply) Bool() (bool, error) {
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
// The reply type must be ReplyMulti and its elements must all be ReplyString.
// Useful for list commands.
func (r *Reply) List() ([]string, error) {
	if r.Type != ReplyMulti {
		return nil, errors.New("reply type is not ReplyMulti")
	}

	strings := make([]string, len(r.Elems))
	for i, v := range r.Elems {
		if v.Type != ReplyString {
			return nil, errors.New("sub-reply type is not ReplyString")
		}

		strings[i] = v.String()
	}

	return strings, nil
}

// Map returns a multi-bulk reply as a map[string]string or an error.
// The reply must have even number of elements and they must be in a 
// "key value key value..." order.
// Useful for hash commands.
func (r *Reply) Hash() (map[string]string, error) {
	rmap := map[string]string{}

	if r.Type != ReplyMulti {
		return nil, errors.New("reply type is not ReplyMulti")
	}

	if len(r.Elems)%2 != 0 {
		return nil, errors.New("reply has odd number of elements")
	}

	for i := 0; i < len(r.Elems)/2; i++ {
		key, err := r.Elems[i*2].Str()
		if err != nil {
			return nil, errors.New("key element has no string reply")
		}

		val, err := r.Elems[i*2+1].Str()
		if err != nil {
			return nil, errors.New("value element has no string reply")
		}

		rmap[key] = val
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
		s, _ := r.Str()
		return s
	case ReplyInteger:
		i, _ := r.Int()
		return strconv.Itoa(i)
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
