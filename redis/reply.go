package redis

import (
	"strconv"
)

//* Reply

type ReplyType uint

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

/* 
Type returns the type of the reply.

The possible types are:

ReplyStatus --  status reply
ReplyError -- error reply
ReplyInteger -- integer reply
ReplyNil -- nil reply
ReplyString -- string reply
ReplyMulti -- multi-bulk or multi-command reply
*/
func (r *Reply) Type() ReplyType {
	return r.t
}

// Str returns the reply value as a string.
// It panics, if the reply type is not ReplyStatus, ReplyError or ReplyString.
func (r *Reply) Str() string {
	if !(r.t == ReplyStatus || r.t == ReplyError || r.t == ReplyString) {
		panic("redis: string value is not available for this reply type")
	}

	return *r.str
}

// Bytes is a convenience method for []byte(Reply.String()).
func (r *Reply) Bytes() []byte {
	return []byte(r.Str())
}

// Int64 returns the reply value as a int64.
// It panics, if the reply type is not ReplyInteger.
func (r *Reply) Int64() int64 {
	if !(r.t == ReplyInteger) {
		panic("redis: integer value is not available for this reply type")
	}
	return *r.int
}

// Int is a convenience method for int(Reply.Int64()).
func (r *Reply) Int() int {
	return int(r.Int64())
}

// Len returns the number of elements in a multi reply.
// It panics, if the reply type is not ReplyMulti.
func (r *Reply) Len() int {
	if r.t != ReplyMulti {
		panic("redis: reply type is not ReplyMulti")
	}

	return len(r.elems)
}

// Elems returns the elements (sub-replies) of a multi reply.
// It panics, if the reply type is not ReplyMulti.
func (r *Reply) Elems() []*Reply {
	if r.t != ReplyMulti {
		panic("redis: reply type is not ReplyMulti")
	}

	return r.elems
}

// At returns a Reply of a multi reply by its index.
// It panics, if the reply type is not ReplyMulti or if the index is out of range.
func (r *Reply) At(i int) *Reply {
	if r.t != ReplyMulti {
		panic("redis: reply type is not ReplyMulti")
	}

	if i < 0 || i >= len(r.elems) {
		panic("redis: reply index out of range")
	}

	return r.elems[i]
}

// Error returns the error value of the reply or nil, if no there were no errors.
func (r *Reply) Error() error {
	return r.err
}

// OK returns true if the reply had no error, otherwise false.
func (r *Reply) OK() bool {
	if r.err != nil {
		return false
	}

	return true
}

/*
// Ints returns  as a slice of integer.
func (r *Reply) Ints() []int {
	if r.values == nil {
		return nil
	}

	ints := make([]int, len(r.values))
	for i, v := range r.values {
		ints[i] = v.Int()
	}

	return ints
}

// Strings returns all values as a slice of strings.
func (r *Reply) Strings() []string {
	if r.values == nil {
		return nil
	}

	strings := make([]string, len(r.values))
	for i, v := range r.values {
		strings[i] = *v.str
	}

	return strings
}

// KeyValue return the first value as key and the second as value.
// Will panic if there are less that two values in the result set.
func (r *Reply) KeyValue() *KeyValue {
	if r.Len() < 2 {
		panic("redis: Reply does not have enough values for KeyValue")
	}

	return &KeyValue{
		Key:   r.At(0).String(),
		Value: r.At(1),
	}
}

// Hash returns the values of the result set as hash.
func (r *Reply) Hash() Hash {
	var key string

	result := make(Hash)
	isVal := false

	for _, v := range r.values {
		if isVal {
			// Write every second value.
			result[key] = v
			isVal = false
		} else {
			// First value is always a key.
			key = v.String()
			isVal = true
		}
	}

	return result
}
*/

// String returns a string representation of the reply and its sub-replies.
// This method is mainly used for debugging.
// Use method Reply.Str() for fetching a string reply.
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

// Future just waits for a result set
// returned somewhere in the future.
type Future chan *Reply

// newFuture creates the new future.
func newFuture() Future {
	return make(chan *Reply, 1)
}

// setReply sets the result set.
func (f Future) setReply(r *Reply) {
	f <- r
}

// Reply blocks until the associated result set can be returned.
func (f Future) Reply() (r *Reply) {
	r = <-f
	f <- r
	return
}
