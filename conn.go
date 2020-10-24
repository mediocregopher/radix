package radix

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
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

// ConnFunc is a function which returns an initialized, ready-to-be-used Conn.
// Functions like NewPool or NewCluster take in a ConnFunc in order to allow for
// things like calls to AUTH on each new connection, setting timeouts, custom
// Conn implementations, etc... See the package docs for more details.
type ConnFunc func(ctx context.Context, network, addr string) (Conn, error)

// DefaultConnFunc is a ConnFunc which will return a Conn for a redis instance
// using sane defaults.
var DefaultConnFunc = func(ctx context.Context, network, addr string) (Conn, error) {
	return Dial(ctx, network, addr)
}

////////////////////////////////////////////////////////////////////////////////

type connMarshalerUnmarshaler struct {
	ctx                    context.Context
	marshal, unmarshalInto interface{}
	errCh                  chan error
}

type conn struct {
	proc *proc.Proc

	conn     net.Conn
	brw      *bufio.ReadWriter
	rCh, wCh chan connMarshalerUnmarshaler

	// errChPool is a buffered channel used as a makeshift pool of chan errors,s
	// owe don't have to make a new one on every EncodeDecode call.
	errChPool chan chan error
}

var _ Conn = new(conn)

// NewConn takes an existing net.Conn and wraps it to support the Conn interface
// of this package. The Read and Write methods on the original net.Conn should
// not be used after calling this method.
func NewConn(netConn net.Conn) Conn {
	c := &conn{
		proc:      proc.New(),
		conn:      netConn,
		brw:       bufio.NewReadWriter(bufio.NewReader(netConn), bufio.NewWriter(netConn)),
		rCh:       make(chan connMarshalerUnmarshaler, 128),
		wCh:       make(chan connMarshalerUnmarshaler, 128),
		errChPool: make(chan chan error, 16),
	}
	c.proc.Run(c.reader)
	c.proc.Run(c.writer)
	return c
}

func (c *conn) Close() error {
	return c.proc.PrefixedClose(c.conn.Close, nil)
}

func (c *conn) newRESPOpts() *resp.Opts {
	return resp.NewOpts()
}

func (c *conn) writer(ctx context.Context) {
	doneCh := ctx.Done()
	opts := c.newRESPOpts()
	for {
		select {
		case <-doneCh:
			return
		case mu := <-c.wCh:
			if mu.marshal != nil {
				if err := mu.ctx.Err(); err != nil {
					mu.errCh <- err
					continue
				} else if deadline, ok := mu.ctx.Deadline(); ok {
					if err := c.conn.SetWriteDeadline(deadline); err != nil {
						mu.errCh <- fmt.Errorf("setting write deadline to %v: %w", deadline, err)
						continue
					}
				} else if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
					mu.errCh <- fmt.Errorf("unsetting write deadline: %w", err)
					continue
				}

				if err := resp3.Marshal(c.brw.Writer, mu.marshal, opts); err != nil {
					mu.errCh <- err
					continue
				} else if err := c.brw.Writer.Flush(); err != nil {
					mu.errCh <- err
					continue
				}
			}

			// if there's no unmarshaler then don't forward to the reader
			if mu.unmarshalInto == nil {
				mu.errCh <- nil
				continue
			}

			select {
			case <-doneCh:
				return
			case c.rCh <- mu:
			}
		}
	}
}

func (c *conn) reader(ctx context.Context) {
	doneCh := ctx.Done()
	opts := c.newRESPOpts()
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

			err := resp3.Unmarshal(c.brw.Reader, mu.unmarshalInto, opts)
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

type dialOpts struct {
	authUser, authPass string
	selectDB           string
	useTLSConfig       bool
	tlsConfig          *tls.Config
}

// DialOpt is an optional behavior which can be applied to the Dial function to
// effect its behavior, or the behavior of the Conn it creates.
type DialOpt func(*dialOpts)

const defaultAuthUser = "default"

// DialAuthPass will cause Dial to perform an AUTH command once the connection
// is created, using the given pass.
//
// If this is set and a redis URI is passed to Dial which also has a password
// set, this takes precedence.
//
// Using DialAuthPass is equivalent to calling DialAuthUser with user "default"
// and is kept for compatibility with older package versions.
func DialAuthPass(pass string) DialOpt {
	return DialAuthUser(defaultAuthUser, pass)
}

// DialAuthUser will cause Dial to perform an AUTH command once the connection
// is created, using the given user and pass.
//
// If this is set and a redis URI is passed to Dial which also has a username
// and password set, this takes precedence.
func DialAuthUser(user, pass string) DialOpt {
	return func(do *dialOpts) {
		do.authUser = user
		do.authPass = pass
	}
}

// DialSelectDB will cause Dial to perform a SELECT command once the connection
// is created, using the given database index.
//
// If this is set and a redis URI is passed to Dial which also has a database
// index set, this takes precedence.
func DialSelectDB(db int) DialOpt {
	return func(do *dialOpts) {
		do.selectDB = strconv.Itoa(db)
	}
}

// DialUseTLS will cause Dial to perform a TLS handshake using the provided
// config. If config is nil the config is interpreted as equivalent to the zero
// configuration. See https://golang.org/pkg/crypto/tls/#Config
func DialUseTLS(config *tls.Config) DialOpt {
	return func(do *dialOpts) {
		do.tlsConfig = config
		do.useTLSConfig = true
	}
}

func parseRedisURL(urlStr string) (string, []DialOpt) {
	// do a quick check before we bust out url.Parse, in case that is very
	// unperformant
	if !strings.HasPrefix(urlStr, "redis://") {
		return urlStr, nil
	}

	u, err := url.Parse(urlStr)
	if err != nil {
		return urlStr, nil
	}

	q := u.Query()

	username := defaultAuthUser
	if n := u.User.Username(); n != "" {
		username = n
	} else if n := q.Get("username"); n != "" {
		username = n
	}

	password := q.Get("password")
	if p, ok := u.User.Password(); ok {
		password = p
	}

	opts := []DialOpt{
		DialAuthUser(username, password),
	}

	dbStr := q.Get("db")
	if u.Path != "" && u.Path != "/" {
		dbStr = u.Path[1:]
	}

	if dbStr, err := strconv.Atoi(dbStr); err == nil {
		opts = append(opts, DialSelectDB(dbStr))
	}

	return u.Host, opts
}

// Dial is a ConnFunc which creates a Conn using net.Dial and NewConn. It takes
// in a number of options which can overwrite its default behavior as well.
//
// In place of a host:port address, Dial also accepts a URI, as per:
// 	https://www.iana.org/assignments/uri-schemes/prov/redis
// If the URI has an AUTH password or db specified Dial will attempt to perform
// the AUTH and/or SELECT as well.
//
// If either DialAuthPass or DialSelectDB is used it overwrites the associated
// value passed in by the URI.
//
// The default options Dial uses are:
//
//	DialTimeout(10 * time.Second)
//
func Dial(ctx context.Context, network, addr string, opts ...DialOpt) (Conn, error) {
	var do dialOpts
	addr, addrOpts := parseRedisURL(addr)
	for _, opt := range addrOpts {
		opt(&do)
	}
	for _, opt := range opts {
		opt(&do)
	}

	var dialer interface {
		DialContext(context.Context, string, string) (net.Conn, error)
	}

	if do.useTLSConfig {
		dialer = &tls.Dialer{
			Config: do.tlsConfig,
		}
	} else {
		dialer = &net.Dialer{}
	}

	netConn, err := dialer.DialContext(ctx, network, addr)
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

	conn := NewConn(netConn)

	if do.authUser != "" && do.authUser != defaultAuthUser {
		if err := conn.Do(ctx, Cmd(nil, "AUTH", do.authUser, do.authPass)); err != nil {
			conn.Close()
			return nil, err
		}
	} else if do.authPass != "" {
		if err := conn.Do(ctx, Cmd(nil, "AUTH", do.authPass)); err != nil {
			conn.Close()
			return nil, err
		}
	}

	if do.selectDB != "" {
		if err := conn.Do(ctx, Cmd(nil, "SELECT", do.selectDB)); err != nil {
			conn.Close()
			return nil, err
		}
	}

	return conn, nil
}
