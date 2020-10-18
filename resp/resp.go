// Package resp is an umbrella package which covers both the old RESP protocol
// (resp2) and the new one (resp3), allowing clients to choose which one they
// care to use
package resp

import (
	"bufio"
	"errors"
	"io"
)

// Opts are used to aid and affect marshaling and unmarshaling of RESP messages.
// Opts are not expected to be thread-safe.
//
// NewOpts should always be used to initialize a new Opts instance, even if some
// or all of the fields are expected to be changed. This way new fields may be
// added without breaking existing usages.
type Opts struct {
	// GetBytes returns a *[]byte from an internal pool, or a newly allocated
	// instance if the pool is empty. The returned instance will have a length
	// of zero.
	//
	// This field may not be nil.
	GetBytes func() *[]byte

	// PutBytes puts a *[]byte back into the pool so it can be re-used later via
	// GetBytes.
	//
	// This field may not be nil.
	PutBytes func(*[]byte)

	// GetReader returns an io.Reader which will read out the given bytes.
	//
	// This field may not be nil.
	GetReader func([]byte) io.Reader

	// MarshalDeterministic indicates that marshal operations should result in
	// deterministic results. This is largely used for ensuring map key/values
	// are marshaled in a deterministic order.
	MarshalDeterministic bool
}

const defaultBytePoolThreshold = 10000000 // ~10MB

// NewOpts returns an Opts instance which is suitable for most use-cases, and
// which may be modified if desired.
func NewOpts() *Opts {
	bp := newBytePool(defaultBytePoolThreshold)
	brp := newByteReaderPool()
	return &Opts{
		GetBytes:  bp.get,
		PutBytes:  bp.put,
		GetReader: brp.get,
	}
}

// Marshaler is the interface implemented by types that can marshal themselves
// into valid RESP. Opts may not be nil.
//
// NOTE It's important to keep track of whether a partial RESP message has been
// written to the Writer, and to use ErrConnUsable when returning errors if a
// partial RESP message has not been written.
type Marshaler interface {
	MarshalRESP(io.Writer, *Opts) error
}

// Unmarshaler is the interface implemented by types that can unmarshal a RESP
// description of themselves. Opts may not be nil.
//
// UnmarshalRESP should _always_ fully consume a RESP message off the reader,
// unless there is an error returned from the reader itself. Use ErrConnUsable
// when applicable.
type Unmarshaler interface {
	UnmarshalRESP(*bufio.Reader, *Opts) error
}

// ErrConnUsable is used to wrap an error encountered while marshaling or
// unmarshaling a message on a connection. It indicates that the network
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
