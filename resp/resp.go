// Package resp is an umbrella package which covers both the old RESP protocol
// (resp2) and the new one (resp3), allowing clients to choose which one they
// care to use
package resp

import (
	"bufio"
	"errors"
	"io"
)

// Marshaler is the interface implemented by types that can marshal themselves
// into valid RESP.
//
// NOTE that when implementing a custom Marshaler, especially when resp2.Any is
// used internally, it's important to keep track of whether a partial RESP
// message has already been written, and to use ErrConnUnusable when returning
// errors if a partial RESP message has been written.
type Marshaler interface {
	MarshalRESP(io.Writer) error
}

// Unmarshaler is the interface implemented by types that can unmarshal a RESP
// description of themselves. UnmarshalRESP should _always_ fully consume a RESP
// message off the reader, unless there is an error returned from the reader
// itself. Use ErrConnUsable when applicable.
//
// NOTE that, unlike Marshaler, Unmarshaler _must_ take in a *bufio.Reader.
type Unmarshaler interface {
	UnmarshalRESP(*bufio.Reader) error
}

// ErrConnUsable is used to wrap an error encountered while marshaling or
// unmarshaling a message onto connection. It declares that the network
// connection is still healthy and that there are no partially written/read
// messages on the stream.
type ErrConnUsable struct {
	Err error
}

// ErrConnUnusable takes an existing error and, if it is wrapped in an
// ErrConnUsable, unwraps the ErrConnUsable from around it.
func ErrConnUnusable(err error) error {
	if err == nil {
		return nil
	} else if errConnUsable := (ErrConnUsable{}); errors.As(err, &errConnUsable) {
		return errConnUsable.Err
	}
	return err
}

func (ed ErrConnUsable) Error() string {
	return ed.Err.Error()
}

// Unwrap implements the errors.Wrapper interface.
func (ed ErrConnUsable) Unwrap() error {
	return ed.Err
}
