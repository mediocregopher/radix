package redis

type ErrorFlag uint8

const (
	ErrorRedis ErrorFlag = 1 << iota
	ErrorConnection
	ErrorLoading
	ErrorAuth
	ErrorParse
	ErrorTimeout
)

type Error struct {
	msg   string
	flags ErrorFlag
}

// newError creates a new Error.
func newError(msg string, flags ...ErrorFlag) *Error {
	err := new(Error)
	err.msg = msg
	for _, f := range flags {
		err.flags |= f
	}
	return err
}

// newErrorExt creates a new Error with flags of the given error and
// appends the error message from the given error to the end of the new one.
func newErrorExt(msg string, err *Error, flags ...ErrorFlag) *Error {
	return newError(msg+": "+err.Error(), append(flags, err.flags)...)
}

// Error returns a string representation of the error.
func (e *Error) Error() string {
	return e.msg
}

// Test returns true, if any of the given error flags is set in the error, otherwise false.
func (e *Error) Test(flags ...ErrorFlag) bool {
	for _, f := range flags {
		if e.flags&f > 0 {
			return true
		}
	}
	return false
}

// errmsg is a helper function for (panic) error messages.
func errmsg(msg string) string {
	return "redis: " + msg
}
