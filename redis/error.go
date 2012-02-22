package redis

type ErrorFlag uint8

const (
	RedisError ErrorFlag = iota
	SystemError
	ConnectionError
	LoadingError
	AuthError
	ParseError
	ResponseError

	lenErrorFlags = 7
)

type Error struct {
	msg     string
	flags   [lenErrorFlags]bool
}

// Create a new Error.
func newError(msg string, flags ...ErrorFlag) *Error {
	if len(flags) < 1 {
		panic("redis: invalid number of parameters")
	}

	flags_ := [lenErrorFlags]bool{} 

	for _, f := range flags {
		flags_[f] = true
	}

	err := &Error{
	msg: msg,
	flags: flags_,
	}

	return err
}

// Create a new Error with flags of the given error.
func newExtError(msg string, err Error, flags ...ErrorFlag) *Error {
	return newError(msg, append(err.flags(), flags...))
}

// Return a string representation of the error.
func (e *Error) String() string {
	return e.msg
}

// Return true, if any of the given error flags is set in the error, otherwise false.
func (e *Error) Test(flags ...ErrorFlag) bool {
	if len(flags) < 1 {
		panic("redis: invalid number of parameters")
	}

	for _, f := range flags {
		if e.flags[f] {
			return true
		}
	}

	return false
}

// Return error flags of the error.
func (e *Error) flags() (errFlags []ErrorFlag) {
	for i, f := range e.flags {
		if f {
			errFlags = append(errFlags, ErrorFlag(i))
		}
	}

	return
}
