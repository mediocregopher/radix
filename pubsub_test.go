package radix

import (
	"context"
	"errors"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tilinna/clock"
)

type pubSubTestHarness struct {
	t     *testing.T
	ctx   context.Context
	conn  PubSubConn
	pubCh chan<- PubSubMessage

	eventCh chan string
	clock   *clock.Mock

	nextMsg    PubSubMessage
	nextErr    error
	nextDoneCh chan struct{}
}

func newPubSubTestHarness(t *testing.T) *pubSubTestHarness {
	h := &pubSubTestHarness{
		t:       t,
		eventCh: make(chan string),
		clock:   clock.NewMock(time.Now().Truncate(time.Hour)),
	}

	var cancelFn context.CancelFunc
	h.ctx, cancelFn = context.WithTimeout(context.Background(), 2*time.Second)
	h.t.Cleanup(cancelFn)

	var stub Conn
	stub, h.pubCh = NewPubSubConnStub("", "", nil)
	h.conn = pubSubConfig{
		clock:       h.clock,
		testEventCh: h.eventCh,
	}.new(stub)

	return h
}

func (h *pubSubTestHarness) assertEvent(expEvent string, fn func()) {
	h.t.Helper()
	select {
	case gotEvent := <-h.eventCh:
		if !assert.Equal(h.t, expEvent, gotEvent) {
			return
		}
	case <-h.ctx.Done():
		h.t.Fatalf("expected event %q", expEvent)
		return
	}

	if fn != nil {
		fn()
	}

	h.eventCh <- ""
}

func (h *pubSubTestHarness) startNext(ctx context.Context) {
	h.t.Helper()
	h.nextDoneCh = make(chan struct{})
	go func() {
		h.nextMsg, h.nextErr = h.conn.Next(ctx)
		close(h.nextDoneCh)
	}()
}

func (h *pubSubTestHarness) assertNext(expMsg PubSubMessage, expErr error) {
	h.t.Helper()
	select {
	case <-h.nextDoneCh:
	case <-h.ctx.Done():
		h.t.Fatal("expected call to Next to have completed, but it hasn't")
	}

	if expErr != nil {
		assert.Equal(h.t, expErr, h.nextErr)
	} else {
		assert.Equal(h.t, expMsg, h.nextMsg)
	}
}

func TestPubSubConn(t *testing.T) {

	t.Run("ping", func(t *testing.T) {
		h := newPubSubTestHarness(t)
		ctx, cancel := context.WithCancel(h.ctx)
		h.startNext(ctx)

		// go through a single full ping cycle
		h.assertEvent("next-top", nil)
		h.assertEvent("wrapped-ctx", func() {
			h.clock.Add(pubSubDefaultPingInterval)
		})
		h.assertEvent("decode-returned", nil)
		h.assertEvent("next-top", nil)
		h.assertEvent("pinged", nil)
		h.assertEvent("wrapped-ctx", nil)
		h.assertEvent("decode-returned", nil)

		// do it again just to be sure
		h.assertEvent("next-top", nil)
		h.assertEvent("wrapped-ctx", func() {
			h.clock.Add(pubSubDefaultPingInterval)
		})
		h.assertEvent("decode-returned", nil)
		h.assertEvent("next-top", nil)
		h.assertEvent("pinged", nil)
		h.assertEvent("wrapped-ctx", nil)
		h.assertEvent("decode-returned", nil)

		// now return
		h.assertEvent("next-top", nil)
		cancel()
		h.assertEvent("wrapped-ctx", nil)
		h.assertEvent("decode-returned", nil)

		h.assertNext(PubSubMessage{}, context.Canceled)
	})

	t.Run("sub", func(t *testing.T) {
		h := newPubSubTestHarness(t)
		assert.NoError(h.t, h.conn.Subscribe(h.ctx, "foo"))

		// first pull in the response from the SUBSCRIBE call
		h.startNext(h.ctx)
		h.assertEvent("next-top", nil)
		h.assertEvent("wrapped-ctx", nil)
		h.assertEvent("decode-returned", nil)
		h.assertEvent("next-top", nil)

		// loop once for fun
		h.assertEvent("wrapped-ctx", func() {
			h.clock.Add(pubSubCtxWrapTimeout)
		})
		h.assertEvent("decode-returned", nil)
		h.assertEvent("next-top", nil)

		// now publish and read that
		msg := PubSubMessage{Type: "message", Channel: "foo", Message: []byte("bar")}
		h.pubCh <- msg
		h.assertEvent("wrapped-ctx", nil)
		h.assertEvent("decode-returned", nil)
		h.assertNext(msg, nil)
	})
}

func ExamplePubSubConn() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a normal redis connection
	conn, err := Dial(ctx, "tcp", "127.0.0.1:6379")
	if err != nil {
		panic(err)
	}

	// Pass that connection into PubSub, conn should never get used after this
	pconn := (PubSubConfig{}).New(conn)
	defer pconn.Close() // this will close Conn as well

	// Subscribe to a channel called "myChannel".
	if err := pconn.Subscribe(ctx, "myChannel"); err != nil {
		panic(err)
	}

	for {
		msg, err := pconn.Next(ctx)
		if errors.Is(err, context.DeadlineExceeded) {
			break
		} else if err != nil {
			panic(err)
		}

		log.Printf("publish to channel %q received: %q", msg.Channel, msg.Message)
	}
}

func ExamplePersistentPubSubConnConfig_cluster() {
	// Example of how to use a persistent PubSubConn with a Cluster.

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Initialize the cluster in any way you see fit
	cluster, err := (ClusterConfig{}).New(ctx, []string{"127.0.0.1:6379"})
	if err != nil {
		panic(err)
	}

	// Have PersistentPubSubConfig pick a random cluster node everytime it wants
	// to make a new connection. If the node fails PersistentPubSubConfig will
	// automatically pick a new node to connect to.
	pconn, err := (PersistentPubSubConnConfig{}).New(ctx, func() (string, string, error) {
		clients, err := cluster.Clients()
		if err != nil {
			return "", "", err
		}

		for addr := range clients {
			return "tcp", addr, nil
		}
		return "", "", errors.New("no clients in the cluster")
	})
	if err != nil {
		panic(err)
	}

	// Use the PubSubConn as normal.
	if err := pconn.Subscribe(ctx, "myChannel"); err != nil {
		panic(err)
	}

	for {
		msg, err := pconn.Next(ctx)
		if errors.Is(err, context.DeadlineExceeded) {
			break
		} else if err != nil {
			panic(err)
		}

		log.Printf("publish to channel %q received: %q", msg.Channel, msg.Message)
	}
}
