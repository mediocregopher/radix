package radix

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/mediocregopher/radix/v4/internal/proc"
	"github.com/mediocregopher/radix/v4/resp"
	"github.com/mediocregopher/radix/v4/resp/resp3"
)

// Conn is a Client wrapping a single network connection which synchronously
// reads/writes data using the redis resp protocol.
//
// A Conn can be used directly as a Client, but in general you probably want to
// use a *Pool instead
type Conn interface {
	// The Do method merely calls the Action's Perform method with the Conn as
	// the argument.
	Client

	// EncodeDecode will encode marshal onto the connection, then decode a
	// response into unmarshalInto (see resp3.Marshal and resp3.Unmarshal,
	// respectively). If either parameter is nil then that step is skipped.
	EncodeDecode(ctx context.Context, marshal, unmarshalInto interface{}) error
}

////////////////////////////////////////////////////////////////////////////////

type connMarshalerUnmarshaler struct {
	ctx                    context.Context
	marshal, unmarshalInto interface{}
	errCh                  chan error
}

type conn struct {
	proc *proc.Proc

	conn         net.Conn
	rOpts, wOpts *resp.Opts
	br           resp.BufferedReader
	bw           resp.BufferedWriter
	rCh, wCh     chan connMarshalerUnmarshaler

	// errChPool is a buffered channel used as a makeshift pool of chan errors,
	// so we don't have to make a new one on every EncodeDecode call.
	errChPool chan chan error
}

var _ Conn = new(conn)

func (c *conn) Close() error {
	return c.proc.PrefixedClose(c.conn.Close, nil)
}

func (c *conn) reader(ctx context.Context) {
	doneCh := ctx.Done()
	for {
		select {
		case <-doneCh:
			return
		case mu := <-c.rCh:

			if err := mu.ctx.Err(); err != nil {
				mu.errCh <- err
				continue
			} else if deadline, ok := mu.ctx.Deadline(); ok {
				if err := c.conn.SetReadDeadline(deadline); err != nil {
					mu.errCh <- fmt.Errorf("setting read deadline to %v: %w", deadline, err)
					continue
				}
			} else if err := c.conn.SetReadDeadline(time.Time{}); err != nil {
				mu.errCh <- fmt.Errorf("unsetting read deadline: %w", err)
				continue
			}

			err := resp3.Unmarshal(c.br, mu.unmarshalInto, c.rOpts)
			if err != nil {
				// simplify things for the caller by translating network
				// timeouts into DeadlineExceeded, since that's actually what
				// happened.
				var netErr net.Error
				if errors.As(err, &netErr) && netErr.Timeout() {
					err = context.DeadlineExceeded
				}
			}

			mu.errCh <- err
		}
	}
}

func (c *conn) getErrCh() chan error {
	select {
	case errCh := <-c.errChPool:
		return errCh
	default:
		return make(chan error, 1)
	}
}

func (c *conn) putErrCh(errCh chan error) {
	select {
	case c.errChPool <- errCh:
	default:
	}
}

func (c *conn) EncodeDecode(ctx context.Context, m, u interface{}) error {
	mu := connMarshalerUnmarshaler{
		ctx:           ctx,
		marshal:       m,
		unmarshalInto: u,
		errCh:         c.getErrCh(),
	}
	doneCh := ctx.Done()
	closedCh := c.proc.ClosedCh()

	select {
	case <-doneCh:
		return ctx.Err()
	case <-closedCh:
		return proc.ErrClosed
	case c.wCh <- mu:
	}

	select {
	case <-doneCh:
		return ctx.Err()
	case <-closedCh:
		return proc.ErrClosed
	case err := <-mu.errCh:
		// it's important that we only put the error channel back in the pool if
		// it's actually been used, otherwise it might still end up with
		// something written to it.
		c.putErrCh(mu.errCh)
		return err
	}
}

func (c *conn) Do(ctx context.Context, a Action) error {
	return a.Perform(ctx, c)
}

func (c *conn) Addr() net.Addr {
	return c.conn.RemoteAddr()
}

////////////////////////////////////////////////////////////////////////////////

type connAddrWrap struct {
	net.Conn
	addr net.Addr
}

func (c connAddrWrap) RemoteAddr() net.Addr {
	return c.addr
}

////////////////////////////////////////////////////////////////////////////////

// Dialer is used to create Conns with particular settings. All fields are
// optional, all methods are thread-safe.
type Dialer struct {
	// CustomConn indicates that this callback should be used in place of Dial
	// when Dial is called. All behavior of Dialer/Dial is superceded when this
	// is set.
	CustomConn func(ctx context.Context, network, addr string) (Conn, error)

	// AuthPass will cause Dial to perform an AUTH command once the connection
	// is created, using AuthUser (if given) and AuthPass.
	//
	// If this is set and a redis URI is passed to Dial which also has a password
	// set, this takes precedence.
	AuthUser, AuthPass string

	// SelectDB will cause Dial to perform a SELECT command once the connection
	// is created, using the given database index.
	//
	// If this is set and a redis URI is passed to Dial which also has a
	// database index set, this takes precedence.
	SelectDB string

	// NetDialer is used to create the underlying network connection.
	//
	// Defaults to net.Dialer.
	NetDialer interface {
		DialContext(context.Context, string, string) (net.Conn, error)
	}

	// WriteFlushInterval indicates how often the Conn should flush writes
	// to the underlying net.Conn.
	//
	// Conn uses a bufio.Writer to write data to the underlying net.Conn, and so
	// requires Flush to be called on that bufio.Writer in order for the data to
	// be fully written. By delaying Flush calls until multiple concurrent
	// EncodeDecode calls have been made Conn can reduce system calls and
	// significantly improve performance in that case.
	//
	// All EncodeDecode calls will be delayed up to WriteFlushInterval, with one
	// exception: if more than WriteFlushInterval has elapsed since the last
	// EncodeDecode call then the next EncodeDecode will Flush immediately. This
	// allows Conns to behave well during both low and high activity periods.
	//
	// Defaults to 0, indicating Flush will be called upon each EncodeDecode
	// call without delay. -1 may be used as an equivalent to 0.
	WriteFlushInterval time.Duration

	// NewRespOpts returns a fresh instance of a *resp.Opts to be used by the
	// underlying connection. This maybe be called more than once.
	//
	// Defaults to resp.NewOpts.
	NewRespOpts func() *resp.Opts
}

func (d Dialer) withDefaults() Dialer {
	if d.NetDialer == nil {
		d.NetDialer = new(net.Dialer)
	}
	if d.NewRespOpts == nil {
		d.NewRespOpts = resp.NewOpts
	}
	return d
}

func parseRedisURL(urlStr string, d Dialer) (string, Dialer) {
	// do a quick check before we bust out url.Parse, in case that is very
	// unperformant
	if !strings.HasPrefix(urlStr, "redis://") {
		return urlStr, d
	}

	u, err := url.Parse(urlStr)
	if err != nil {
		return urlStr, d
	}

	q := u.Query()

	if d.AuthUser == "" {
		d.AuthUser = q.Get("username")
		if n := u.User.Username(); n != "" {
			d.AuthUser = n
		}
	}

	if d.AuthPass == "" {
		d.AuthPass = q.Get("password")
		if p, ok := u.User.Password(); ok {
			d.AuthPass = p
		}
	}

	if d.SelectDB == "" {
		d.SelectDB = q.Get("db")
		if u.Path != "" && u.Path != "/" {
			d.SelectDB = u.Path[1:]
		}
	}

	return u.Host, d
}

// Dial creates a Conn using the Dialer configuration.
//
// In place of a host:port address, Dial also accepts a URI, as per:
// 	https://www.iana.org/assignments/uri-schemes/prov/redis
// If the URI has an AUTH password or db specified Dial will attempt to perform
// the AUTH and/or SELECT as well.
func (d Dialer) Dial(ctx context.Context, network, addr string) (Conn, error) {
	if d.CustomConn != nil {
		return d.CustomConn(ctx, network, addr)
	}

	d = d.withDefaults()
	addr, d = parseRedisURL(addr, d)

	netConn, err := d.NetDialer.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}

	// wrap the conn so that it will return exactly what was used for dialing
	// when Addr is called. If Conn's RemoteAddr is used then it returns the
	// fully resolved form of the host.
	netConn = connAddrWrap{
		Conn: netConn,
		addr: rawAddr{network: network, addr: addr},
	}

	// If the netConn is a net.TCPConn (or some wrapper for it) and so can have
	// keepalive enabled, do so with a sane (though slightly aggressive)
	// default.
	{
		type keepaliveConn interface {
			SetKeepAlive(bool) error
			SetKeepAlivePeriod(time.Duration) error
		}

		if kaConn, ok := netConn.(keepaliveConn); ok {
			if err = kaConn.SetKeepAlive(true); err != nil {
				netConn.Close()
				return nil, err
			} else if err = kaConn.SetKeepAlivePeriod(10 * time.Second); err != nil {
				netConn.Close()
				return nil, err
			}
		}
	}

	conn := &conn{
		proc:      proc.New(),
		conn:      netConn,
		rOpts:     d.NewRespOpts(),
		wOpts:     d.NewRespOpts(),
		rCh:       make(chan connMarshalerUnmarshaler, 128),
		wCh:       make(chan connMarshalerUnmarshaler, 128),
		errChPool: make(chan chan error, 16),
	}

	conn.br = conn.rOpts.GetBufferedReader(netConn)
	conn.bw = conn.wOpts.GetBufferedWriter(netConn)

	conn.proc.Run(conn.reader)
	conn.proc.Run(func(ctx context.Context) {
		i := d.WriteFlushInterval
		if i < 0 {
			i = 0
		}
		conn.writer(ctx, i)
	})

	if d.AuthUser != "" {
		if err := conn.Do(ctx, Cmd(nil, "AUTH", d.AuthUser, d.AuthPass)); err != nil {
			conn.Close()
			return nil, err
		}
	} else if d.AuthPass != "" {
		if err := conn.Do(ctx, Cmd(nil, "AUTH", d.AuthPass)); err != nil {
			conn.Close()
			return nil, err
		}
	}

	if d.SelectDB != "" {
		if err := conn.Do(ctx, Cmd(nil, "SELECT", d.SelectDB)); err != nil {
			conn.Close()
			return nil, err
		}
	}

	return conn, nil
}

// Dial is a shortcut for calling Dial on a zero-value Dialer.
func Dial(ctx context.Context, network, addr string) (Conn, error) {
	return (Dialer{}).Dial(ctx, network, addr)
}
