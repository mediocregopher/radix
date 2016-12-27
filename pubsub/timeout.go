package pubsub

import (
	"bufio"
	"net"

	radix "github.com/mediocregopher/radix.v2"
	"github.com/mediocregopher/radix.v2/resp"
)

type timeoutOkConn struct {
	radix.Conn
}

// TimeoutOk returns a Conn which will _not_ call Close on itself if it sees a
// timeout error during a Decode call (though it will still return that error).
// It will still do so for all other read/write errors, however.
func TimeoutOk(c radix.Conn) radix.Conn {
	return &timeoutOkConn{c}
}

func (toc *timeoutOkConn) Decode(u resp.Unmarshaler) error {
	err := toc.Conn.Decode(timeoutOkUnmarshaler{u})
	if err == nil {
		return nil
	}
	if te, ok := err.(errTimeout); ok {
		err = te.err
	} else {
		toc.Close()
	}
	return err
}

type timeoutOkUnmarshaler struct {
	u resp.Unmarshaler
}

type errTimeout struct {
	err error
}

func (et errTimeout) Error() string { return et.err.Error() }

func (tou timeoutOkUnmarshaler) UnmarshalRESP(p *resp.Pool, br *bufio.Reader) error {
	err := tou.u.UnmarshalRESP(p, br)
	if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
		err = errTimeout{err}
	}
	return err
}
