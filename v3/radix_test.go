package radix

import (
	"crypto/rand"
	"encoding/hex"
	"regexp"
	"strings"
	. "testing"
	"time"

	"github.com/mediocregopher/mediocre-go-lib/mrand"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func randStr() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func dial(opts ...DialOpt) Conn {
	c, err := Dial("tcp", "127.0.0.1:6379", opts...)
	if err != nil {
		panic(err)
	}
	return c
}

func TestCloseBehavior(t *T) {
	c := dial()

	// sanity check
	var out string
	require.Nil(t, c.Do(Cmd(&out, "ECHO", "foo")))
	assert.Equal(t, "foo", out)

	c.Close()
	require.NotNil(t, c.Do(Cmd(&out, "ECHO", "foo")))
	require.NotNil(t, c.NetConn().SetDeadline(time.Now()))
}

func TestDialURI(t *T) {
	c, err := Dial("tcp", "redis://127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	} else if err := c.Do(Cmd(nil, "PING")); err != nil {
		t.Fatal(err)
	}
}

func TestDialAuth(t *T) {
	// It's difficult to test _which_ password is being sent, but it's easy
	// enough to tell that one was sent because redis returns an error if one
	// isn't set in the config
	tests := []struct{ url, dialOptPass string }{
		{url: "redis://user:myPass@127.0.0.1:6379"},
		{url: "redis://:myPass@127.0.0.1:6379"},
		{url: "redis://127.0.0.1:6379?password=myPass"},
		{url: "127.0.0.1:6379", dialOptPass: "myPass"},
	}

	for _, test := range tests {
		var opts []DialOpt
		if test.dialOptPass != "" {
			opts = append(opts, DialAuthPass(test.dialOptPass))
		}
		_, err := Dial("tcp", test.url, opts...)
		if err == nil || err.Error() != "ERR Client sent AUTH, but no password is set" {
			t.Fatalf(`error "ERR Client sent AUTH..." expected, got: %v (test:%#v)`, err, test)
		}
	}
}

func TestDialSelect(t *T) {

	// unfortunately this is the best way to discover the currently selected
	// database, and it's janky af
	assertDB := func(c Conn) bool {
		name := mrand.Hex(8)
		if err := c.Do(Cmd(nil, "CLIENT", "SETNAME", name)); err != nil {
			t.Fatal(err)
		}

		var list string
		if err := c.Do(Cmd(&list, "CLIENT", "LIST")); err != nil {
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
		c, err := Dial("tcp", test.url, opts...)
		if err != nil {
			t.Fatalf("got err connecting:%v (test:%#v)", err, test)
		}

		if !assertDB(c) {
			t.Fatalf("db not set to 9 (test:%#v)", test)
		}
	}
}
