package radix

type ErrorFlag uint8

const (
	ErrorRedis ErrorFlag = iota
	ErrorConnection
	ErrorLoading
	ErrorAuth
	ErrorParse
	ErrorInvalidReply
	ErrorTimeout

	lenErrorFlags = 8
)

type Error struct {
	msg    string
	flags_ [lenErrorFlags]bool
}

// newError creates a new Error.
func newError(msg string, flags ...ErrorFlag) *Error {
	flags_ := [lenErrorFlags]bool{}

	for _, f := range flags {
		flags_[f] = true
	}

	err := &Error{
		msg:    msg,
		flags_: flags_,
	}

	return err
}

// newErrorExt creates a new Error with flags of the given error.
func newErrorExt(msg string, err *Error, flags ...ErrorFlag) *Error {
	return newError(msg, append(err.flags(), flags...)...)
}

// Error returns a string representation of the error.
func (e *Error) Error() string {
	return "redis: " + e.msg
}

// Test returns true, if any of the given error flags is set in the error, otherwise false.
func (e *Error) Test(flags ...ErrorFlag) bool {
	if len(flags) < 1 {
		panic("redis: invalid number of parameters")
	}

	for _, f := range flags {
		if e.flags_[f] {
			return true
		}
	}

	return false
}

// flags returns error flags of the error.
func (e *Error) flags() (errFlags []ErrorFlag) {
	for i, f := range e.flags_ {
		if f {
			errFlags = append(errFlags, ErrorFlag(i))
		}
	}

	return
}
