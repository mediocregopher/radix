package radix

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	. "testing"
	"time"

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
	const n = 100

	subConn, pubConn := dial(), dial()
	ctx := testCtx(t)
	ch := randStr()

	err := subConn.Do(ctx, Cmd(nil, "SUBSCRIBE", ch))
	assert.NoError(t, err)

	for i := 0; i < n; i++ {
		err := pubConn.Do(ctx, FlatCmd(nil, "PUBLISH", ch, i))
		assert.NoError(t, err)
	}

	var numTimeouts uint64
	for i := 0; i < n; {
		if err := ctx.Err(); err != nil {
			panic(err)
		}

		// the time here needs to be small enough that we get a timeout reading
		// sometimes, but not so small that it times out everytime. By having
		// the timeout slowly increase after every previous timeout error we can
		// ensure we find this value in disparate test environments.
		innerCtx, cancel := context.WithTimeout(ctx, time.Duration(numTimeouts)*time.Microsecond)

		var msg PubSubMessage
		err := subConn.EncodeDecode(innerCtx, nil, &msg)
		cancel()

		if err != nil {
			assert.True(t, errors.Is(err, context.DeadlineExceeded), "err:%v", err)
			numTimeouts++
		} else {
			if !assert.Equal(t, fmt.Sprint(i), string(msg.Message)) {
				return
			}
			i++
		}
	}

	t.Logf("successes:%d timeouts:%v", n, numTimeouts)
}
