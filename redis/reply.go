package redis

import (
	"strconv"
)

//* Reply

/*
ReplyType describes the type of reply.

Possbile values are:

ReplyStatus --  status reply
ReplyError -- error reply
ReplyInteger -- integer reply
ReplyNil -- nil reply
ReplyString -- string reply
ReplyMulti -- multi-bulk or multi-command reply
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
	t     ReplyType
	str   *string
	int   *int64
	elems []*Reply
	err   error
}


// Type returns the type of the reply.
func (r *Reply) Type() ReplyType {
	return r.t
}

// Return true, if the reply is a nil reply, otherwise false.
func (r *Reply) Nil() bool {
	if r.t == ReplyNil {
		return true
	}

	return false
}

// Return the reply value as a string.
// It panics, if the reply type is not ReplyStatus, ReplyError or ReplyString.
func (r *Reply) Str() string {
	if !(r.t == ReplyStatus || r.t == ReplyError || r.t == ReplyString) {
		panic("string value is not available for this reply type")
	}

	return *r.str
}

// Bytes is a convenience method for []byte(Reply.String()).
func (r *Reply) Bytes() []byte {
	return []byte(r.Str())
}

// Return the reply value as a int64.
// It panics, if the reply type is not ReplyInteger.
func (r *Reply) Int64() int64 {
	if r.t != ReplyInteger {
		panic("integer value is not available for this reply type")
	}
	return *r.int
}

// Int is a convenience method for int(Reply.Int64()).
func (r *Reply) Int() int {
	return int(r.Int64())
}

// Return true, if the reply value equals to 1 or "1", otherwise false.
// It panics, if the reply type is not ReplyInteger or ReplyString.
func (r *Reply) Bool() bool {
	switch r.t {
	case ReplyInteger:
		if r.Int() == 1 {
			return true
		}

		return false
	case ReplyString:
		if r.Str() == "1" {
			return true
		}

		return false
	}

	panic("boolean value is not available for this reply type")
}

// Return the number of elements in a multi reply.
// It panics, if the reply type is not ReplyMulti or ReplyNil.
func (r *Reply) Len() int {
	if !(r.t == ReplyMulti || r.t == ReplyNil) {
		panic("length is not available for this reply type")
	}

	return len(r.elems)
}

// Return the elements (sub-replies) of a multi reply or nil, if the reply is nil reply.
// It panics, if the reply type is not ReplyMulti or ReplyNil.
func (r *Reply) Elems() []*Reply {
	if !(r.t == ReplyMulti || r.t == ReplyNil) {
		panic("reply type is not ReplyMulti or ReplyNil")
	}

	return r.elems
}

// Return a Reply of a multi reply by its index.
// It panics, if the reply type is not ReplyMulti or if the index is out of range.
func (r *Reply) At(i int) *Reply {
	if r.t != ReplyMulti {
		panic("reply type is not ReplyMulti")
	}

	if i < 0 || i >= len(r.elems) {
		panic("reply index out of range")
	}

	return r.elems[i]
}

// Return the error value of the reply or nil, if no there were no errors.
func (r *Reply) Error() error {
	return r.err
}

// Retur true if the reply had no error, otherwise false.
func (r *Reply) OK() bool {
	if r.err != nil {
		return false
	}

	return true
}

// Return a multi-bulk reply as a slice of strings or an error.
// The reply type must be a ReplyMulti or ReplyNil.
// An empty slice is returned, if the reply type is ReplyNil.
func (r *Reply) Strings() ([]string, error) {
	if r.Type() == ReplyNil {
		return []string{}, nil
	}

	if r.Type() != ReplyMulti {
		return nil, newError("reply type was not ReplyMulti or ReplyNil")
	}

	strings := make([]string, len(r.elems))
	for i, v := range r.elems {
		if v.Type() != ReplyString {
			return nil, newError("reply type was not ReplyString")
		}

		strings[i] = v.String()
	}

	return strings, nil
}

// Return a hash reply as a map[string]*Reply or an error.
// The reply must be a multi-bulk reply with "key value key value..."-style elements.
// The reply type must be a ReplyMulti or ReplyNil.
// An empty map is returned, if the reply type is ReplyNil.
func (r *Reply) Map() (map[string]*Reply, error) {
	rmap := map[string]*Reply{}

	if r.Type() == ReplyNil {
		return rmap, nil
	}

	if r.Type() != ReplyMulti {
		return nil, newError("reply type was not ReplyMulti or ReplyNil")
	}

	if r.Len()%2 != 0 {
		return nil, newError("reply has odd number of elements")
	}

	for i := 0; i < r.Len()/2; i++ {
		rkey := r.At(i * 2)
		if rkey.Type() != ReplyString {
			return nil, newError("key element was not a string reply")
		}
		key := rkey.Str()

		rmap[key] = r.At(i*2 + 1)
	}

	return rmap, nil
}

// Return a hash reply as a map[string]string or an error.
// The reply must be a multi-bulk reply with "key value key value..."-style elements.
// The reply type must be a ReplyMulti or ReplyNil.
// An empty map is returned, if the reply type is ReplyNil.
func (r *Reply) StringMap() (map[string]string, error) {
	rmap := map[string]string{}

	if r.Type() == ReplyNil {
		return rmap, nil
	}

	if r.Type() != ReplyMulti {
		return nil, newError("reply type was not ReplyMulti or ReplyNil")
	}

	if r.Len()%2 != 0 {
		return nil, newError("reply has odd number of elements")
	}

	for i := 0; i < r.Len()/2; i++ {
		rkey := r.At(i * 2)
		if rkey.Type() != ReplyString {
			return nil, newError("key element was not a string reply")
		}
		key := rkey.Str()

		rval := r.At(i*2 + 1)
		if rval.Type() != ReplyString {
			return nil, newError("value element was not a string reply")
		}
		val := rval.Str()

		rmap[key] = val
	}

	return rmap, nil
}

// Return a string representation of the reply and its sub-replies.
// This method is mainly used for debugging.
// Use method Reply.Str for fetching a string reply.
func (r *Reply) String() string {
	switch r.t {
	case ReplyStatus:
		fallthrough
	case ReplyError:
		fallthrough
	case ReplyString:
		return r.Str()
	case ReplyInteger:
		return strconv.Itoa(r.Int())
	case ReplyNil:
		return "<nil>"
	case ReplyMulti:
		s := "[ "
		for _, e := range r.elems {
			s = s + e.String() + " "
		}
		return s + "]"
	}

	// This should never execute.
	return ""
}

//* Future

// Future is a channel for fetching a reply of an asynchronous command.
type Future chan *Reply

// Create a new Future.
func newFuture() Future {
	return make(chan *Reply, 1)
}

// Set the Reply of the Future to given the given Reply.
func (f Future) setReply(r *Reply) {
	f <- r
}

// Return the Reply of the Future.
// Blocks until the Reply is available.
func (f Future) Reply() (r *Reply) {
	r = <-f
	f <- r
	return
}
