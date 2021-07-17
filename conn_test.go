package radix

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	. "testing"
	"time"

	"github.com/mediocregopher/radix/v4/internal/proc"
	"github.com/mediocregopher/radix/v4/resp/resp3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloseBehavior(t *T) {
	ctx := testCtx(t)
	c := dial()

	// sanity check
	var out string
	require.Nil(t, c.Do(ctx, Cmd(&out, "ECHO", "foo")))
	assert.Equal(t, "foo", out)

	c.Close()
	require.NotNil(t, c.Do(ctx, Cmd(&out, "ECHO", "foo")))
}

func TestDialURI(t *T) {
	ctx := testCtx(t)
	c, err := Dial(ctx, "tcp", "redis://127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	} else if err := c.Do(ctx, Cmd(nil, "PING")); err != nil {
		t.Fatal(err)
	}
}

func TestDialAuth(t *T) {
	type testCase struct {
		url, dialOptUser, dialOptPass string
	}

	runTests := func(t *T, tests []testCase, allowedErrs []string) {
		t.Helper()
		ctx := testCtx(t)
		for _, test := range tests {
			var dialer Dialer
			if test.dialOptUser != "" {
				dialer.AuthUser = test.dialOptUser
			}
			if test.dialOptPass != "" {
				dialer.AuthPass = test.dialOptPass
			}
			_, err := dialer.Dial(ctx, "tcp", test.url)

			// unwrap the error
			if rErr := (resp3.SimpleError{}); errors.As(err, &rErr) {
				err = rErr
			}

			// It's difficult to test _which_ password is being sent, but it's easy
			// enough to tell that one was sent because redis returns an error if one
			// isn't set in the config
			assert.Errorf(t, err, "expected authentication error, got nil")
			assert.Containsf(t, allowedErrs, err.Error(), "one of %v expected, got %v (test:%#v)", allowedErrs, err, test)
		}
	}

	t.Run("Password only", func(t *T) {
		runTests(t, []testCase{
			{url: "redis://:myPass@127.0.0.1:6379"},
			{url: "redis://127.0.0.1:6379?password=myPass"},
			{url: "127.0.0.1:6379", dialOptPass: "myPass"},
		}, []string{
			"ERR Client sent AUTH, but no password is set",
			// Redis 6 only
			"ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?",
		})
	})

	t.Run("Username and password", func(t *T) {
		conn := dial()
		defer conn.Close()

		requireRedisVersion(t, conn, 6, 0, 0)

		runTests(t, []testCase{
			{url: "redis://user:myPass@127.0.0.1:6379"},
			{url: "redis://127.0.0.1:6379?username=mediocregopher"},
			{url: "127.0.0.1:6379", dialOptUser: "mediocregopher"},
			{url: "redis://127.0.0.1:6379?username=mediocregopher&password=myPass"},
			{url: "127.0.0.1:6379", dialOptUser: "mediocregopher", dialOptPass: "myPass"},
		}, []string{
			"WRONGPASS invalid username-password pair",
			"WRONGPASS invalid username-password pair or user is disabled.",
		})
	})
}

func TestDialSelect(t *T) {
	ctx := testCtx(t)

	// unfortunately this is the best way to discover the currently selected
	// database, and it's janky af
	assertDB := func(c Conn) bool {
		name := randStr()
		if err := c.Do(ctx, Cmd(nil, "CLIENT", "SETNAME", name)); err != nil {
			t.Fatal(err)
		}

		var list string
		if err := c.Do(ctx, Cmd(&list, "CLIENT", "LIST")); err != nil {
			t.Fatal(err)
		}

		line := regexp.MustCompile(".*name=" + name + ".*").FindString(list)
		if line == "" {
			t.Fatalf("line messed up:%q (list:%q name:%q)", line, list, name)
		}

		return strings.Index(line, " db=9 ") > 0
	}

	tests := []struct {
		url           string
		dialOptSelect int
	}{
		{url: "redis://127.0.0.1:6379/9"},
		{url: "redis://127.0.0.1:6379?db=9"},
		{url: "redis://127.0.0.1:6379", dialOptSelect: 9},
		// DialOpt should overwrite URI
		{url: "redis://127.0.0.1:6379/8", dialOptSelect: 9},
	}

	for _, test := range tests {
		var dialer Dialer
		if test.dialOptSelect > 0 {
			dialer.SelectDB = strconv.Itoa(test.dialOptSelect)
		}
		c, err := dialer.Dial(ctx, "tcp", test.url)
		if err != nil {
			t.Fatalf("got err connecting:%v (test:%#v)", err, test)
		}

		if !assertDB(c) {
			t.Fatalf("db not set to 9 (test:%#v)", test)
		}
	}
}

func TestConnConcurrentMarshalUnmarshal(t *T) {
	ctx := testCtx(t)
	conn := dial()
	vv := make([]string, 100)
	for i := range vv {
		vv[i] = fmt.Sprint(i)
	}

	// we can't guarantee that the unmarshal starts before its corresponding
	// marshal, but hopefully within one of these iterations it'll happen.

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range vv {
			var got string
			assert.NoError(t, conn.EncodeDecode(ctx, nil, &got))
			assert.Equal(t, vv[i], got)
		}
	}()

	for i := range vv {
		assert.NoError(t, conn.EncodeDecode(ctx, []string{"ECHO", vv[i]}, nil))
	}
	wg.Wait()
}

func TestConnDeadlineExceeded(t *T) {

	const timeoutStdErr = 0.1
	mkTimeout := func(base time.Duration) time.Duration {
		err := rand.Int63n(int64(base)*2) - int64(base)
		err = int64(float64(err) * timeoutStdErr)
		return base + time.Duration(err)
	}

	// NOTE once we're not supporting 1.15, use `errors.Is(err, net.ErrClosed)`
	isNetClosed := func(err error) bool {
		if opErr := new(net.OpError); errors.As(err, &opErr) {
			return opErr.Err.Error() == "use of closed network connection"
		}
		return false
	}

	t.Run("echo", func(t *T) {
		const p, n = 10, 100
		const initTimeout = time.Second
		conn := dial()
		defer conn.Close()
		ctx := testCtx(t)

		var wg sync.WaitGroup
		wg.Add(p)

		var numSuccesses, numTimeouts, numClosed uint64

		for i := 0; i < p; i++ {
			go func(workerID int) {
				timeout := initTimeout
				defer wg.Done()
				for i := 0; i < n; {
					if err := ctx.Err(); err != nil {
						panic(err)
					}

					str := fmt.Sprintf("%d-%da-%s", workerID, i, randStr())
					str2 := fmt.Sprintf("%d-%db-%s", workerID, i, randStr())
					into := ""
					into2 := ""

					pipeline := NewPipeline()
					pipeline.Append(Cmd(&into, "ECHO", str))
					pipeline.Append(Cmd(&into2, "ECHO", str2))

					innerCtx, cancel := context.WithTimeout(ctx, mkTimeout(timeout))
					//err := conn.Do(innerCtx, Cmd(&into, "ECHO", str))
					err := conn.Do(innerCtx, pipeline)
					cancel()

					// We want to see at least one successful operation and one
					// timeout. But once we hit a timeout the connection may be
					// unusable (closed). By halving the timeout after every
					// successful operation we can ensure we find this sweet
					// spot in disparate test environments.
					timeout /= 2

					if err != nil {
						//log.Printf("%q %q err:%v", str, str2, err)
						isTimeout := errors.Is(err, context.DeadlineExceeded)
						isClosed := errors.Is(err, proc.ErrClosed) || isNetClosed(err)
						if !assert.True(t, isTimeout || isClosed, "err:%v", err) {
							return
						}
						if isTimeout {
							atomic.AddUint64(&numTimeouts, 1)
						}
						if isClosed {
							atomic.AddUint64(&numClosed, 1)
						}
						return

					} else if !assert.Equal(t, str, into) {
						return
					} else if !assert.Equal(t, str2, into2) {
						return
					}

					atomic.AddUint64(&numSuccesses, 1)
					i++
				}
			}(i)
		}

		wg.Wait()
		assert.NotZero(t, numTimeouts, "number of timeouts")
		t.Logf("successes:%d timeouts:%d closed:%d", numSuccesses, numTimeouts, numClosed)
	})

	t.Run("pubsub", func(t *T) {
		const n = 100

		subConn, pubConn := dial(), dial()
		defer subConn.Close()
		defer pubConn.Close()

		ctx := testCtx(t)
		ch := randStr()
		timeout := time.Second

		err := subConn.Do(ctx, Cmd(nil, "SUBSCRIBE", ch))
		assert.NoError(t, err)

		for i := 0; i < n; i++ {
			err := pubConn.Do(ctx, FlatCmd(nil, "PUBLISH", ch, i))
			assert.NoError(t, err)
		}

		var numSuccesses, numTimeouts uint64
		for i := 0; i < n; {
			if err := ctx.Err(); err != nil {
				panic(err)
			}

			innerCtx, cancel := context.WithTimeout(ctx, timeout)

			var msg PubSubMessage
			err := subConn.EncodeDecode(innerCtx, nil, &msg)
			cancel()

			// We want to see at least one successful operation and one timeout.
			// But once we hit a timeout the connection may be unusable
			// (closed). By halving the timeout after every successful operation
			// we can ensure we find this sweet spot in disparate test
			// environments.
			timeout /= 2

			if err != nil {
				isTimeout := errors.Is(err, context.DeadlineExceeded)
				isClosed := errors.Is(err, proc.ErrClosed) || isNetClosed(err)
				if !assert.True(t, isTimeout || isClosed, "err:%v", err) {
					return
				}
				if isTimeout {
					numTimeouts++
				}
				break
			} else {
				if !assert.Equal(t, fmt.Sprint(i), string(msg.Message)) {
					return
				}
				numSuccesses++
				i++
			}
		}

		assert.NotZero(t, numTimeouts, "timeouts")
		assert.NotZero(t, numSuccesses, "successes")
		t.Logf("successes:%d timeouts:%d", numSuccesses, numTimeouts)
	})
}
