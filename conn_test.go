package radix

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	. "testing"

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
			var opts []DialOpt
			if test.dialOptUser != "" {
				opts = append(opts, DialAuthUser(test.dialOptUser, test.dialOptPass))
			} else if test.dialOptPass != "" {
				opts = append(opts, DialAuthPass(test.dialOptPass))
			}
			_, err := Dial(ctx, "tcp", test.url, opts...)

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
		var opts []DialOpt
		if test.dialOptSelect > 0 {
			opts = append(opts, DialSelectDB(test.dialOptSelect))
		}
		c, err := Dial(ctx, "tcp", test.url, opts...)
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
