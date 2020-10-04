package radix

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	. "testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func publish(t *T, c Conn, ch, msg string) {
	require.Nil(t, c.Do(testCtx(t), Cmd(nil, "PUBLISH", ch, msg)))
}

func assertMsgRead(t *T, msgCh <-chan PubSubMessage) PubSubMessage {
	select {
	case m := <-msgCh:
		return m
	case <-time.After(5 * time.Second):
		panic("timedout reading")
	}
}

func assertMsgNoRead(t *T, msgCh <-chan PubSubMessage) {
	select {
	case msg, ok := <-msgCh:
		if !ok {
			assert.Fail(t, "msgCh closed")
		} else {
			assert.Fail(t, "unexpected PubSubMessage off msgCh", "msg:%#v", msg)
		}
	default:
	}
}

func testSubscribe(t *T, c PubSubConn, pubCh chan int) {
	ctx := testCtx(t)
	pubC := dial()
	msgCh := make(chan PubSubMessage, 1)

	ch1, ch2, msgStr := randStr(), randStr(), randStr()
	require.Nil(t, c.Subscribe(ctx, msgCh, ch1, ch2))

	pubChs := make([]chan int, 3)
	{
		for i := range pubChs {
			pubChs[i] = make(chan int)
		}
		go func() {
			for i := range pubCh {
				for _, innerPubCh := range pubChs {
					innerPubCh <- i
				}
			}
			for _, innerPubCh := range pubChs {
				close(innerPubCh)
			}
		}()
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		for i := range pubChs[0] {
			publish(t, pubC, ch1, msgStr+"_"+strconv.Itoa(i))
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for i := range pubChs[1] {
			msg := assertMsgRead(t, msgCh)
			assert.Equal(t, PubSubMessage{
				Type:    "message",
				Channel: ch1,
				Message: []byte(msgStr + "_" + strconv.Itoa(i)),
			}, msg)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for range pubChs[2] {
			require.Nil(t, c.Ping(ctx))
		}
		wg.Done()
	}()
	wg.Wait()

	require.Nil(t, c.Unsubscribe(ctx, msgCh, ch1))
	publish(t, pubC, ch1, msgStr)
	publish(t, pubC, ch2, msgStr)
	msg := assertMsgRead(t, msgCh)
	assert.Equal(t, PubSubMessage{
		Type:    "message",
		Channel: ch2,
		Message: []byte(msgStr),
	}, msg)

}

func TestPubSubSubscribe(t *T) {
	ctx := testCtx(t)
	pubCh := make(chan int)
	go func() {
		for i := 0; i < 1000; i++ {
			pubCh <- i
		}
		close(pubCh)
	}()
	c := NewPubSubConn(dial())
	testSubscribe(t, c, pubCh)

	assert.NoError(t, c.Close())
	assert.NotNil(t, c.Ping(ctx))
	assert.NotNil(t, c.Ping(ctx))
	assert.NotNil(t, c.Ping(ctx))
}

func TestPubSubPSubscribe(t *T) {
	ctx := testCtx(t)
	pubC := dial()
	c := NewPubSubConn(dial())
	msgCh := make(chan PubSubMessage, 1)

	p1, p2, msgStr := randStr()+"_*", randStr()+"_*", randStr()
	ch1, ch2 := p1+"_"+randStr(), p2+"_"+randStr()
	p3, p4 := randStr()+"_?", randStr()+"_[ae]"
	ch3, ch4 := p3[:len(p3)-len("?")]+"a", p4[:len(p4)-len("[ae]")]+"a"
	require.Nil(t, c.PSubscribe(ctx, msgCh, p1, p2, p3, p4))

	count := 1000
	wg := new(sync.WaitGroup)

	wg.Add(1)
	go func() {
		for i := 0; i < count; i++ {
			msgStr := fmt.Sprintf("%s-%d", msgStr, i)
			publish(t, pubC, ch1, msgStr)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for i := 0; i < count; i++ {
			msg := assertMsgRead(t, msgCh)
			assert.Equal(t, PubSubMessage{
				Type:    "pmessage",
				Pattern: p1,
				Channel: ch1,
				Message: []byte(fmt.Sprintf("%s-%d", msgStr, i)),
			}, msg)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for i := 0; i < count; i++ {
			require.Nil(t, c.Ping(ctx))
		}
		wg.Done()
	}()

	wg.Wait()

	require.Nil(t, c.PUnsubscribe(ctx, msgCh, p1))
	publish(t, pubC, ch1, msgStr)
	publish(t, pubC, ch2, msgStr)
	msg := assertMsgRead(t, msgCh)
	assert.Equal(t, PubSubMessage{
		Type:    "pmessage",
		Pattern: p2,
		Channel: ch2,
		Message: []byte(msgStr),
	}, msg)

	publish(t, pubC, ch3, msgStr)
	msg = assertMsgRead(t, msgCh)
	assert.Equal(t, PubSubMessage{
		Type:    "pmessage",
		Pattern: p3,
		Channel: ch3,
		Message: []byte(msgStr),
	}, msg)

	publish(t, pubC, ch4, msgStr)
	msg = assertMsgRead(t, msgCh)
	assert.Equal(t, PubSubMessage{
		Type:    "pmessage",
		Pattern: p4,
		Channel: ch4,
		Message: []byte(msgStr),
	}, msg)

	c.Close()
	assert.NotNil(t, c.Ping(ctx))
	assert.NotNil(t, c.Ping(ctx))
	assert.NotNil(t, c.Ping(ctx))
	publish(t, pubC, ch2, msgStr)
	time.Sleep(250 * time.Millisecond)
	assertMsgNoRead(t, msgCh)
}

func TestPubSubMixedSubscribe(t *T) {
	ctx := testCtx(t)
	pubC := dial()
	defer pubC.Close()

	c := NewPubSubConn(dial())
	defer func() {
		assert.NoError(t, c.Close())
	}()

	msgCh := make(chan PubSubMessage, 2)

	const msgStr = "bar"

	require.Nil(t, c.Subscribe(ctx, msgCh, "foo"))
	require.Nil(t, c.PSubscribe(ctx, msgCh, "f[aeiou]o"))

	publish(t, pubC, "foo", msgStr)

	msg1, msg2 := assertMsgRead(t, msgCh), assertMsgRead(t, msgCh)

	// If we received the pmessage first we must swap msg1 and msg1.
	if msg1.Type == "pmessage" {
		msg1, msg2 = msg2, msg1
	}

	assert.Equal(t, PubSubMessage{
		Type:    "message",
		Channel: "foo",
		Message: []byte(msgStr),
	}, msg1)

	assert.Equal(t, PubSubMessage{
		Type:    "pmessage",
		Channel: "foo",
		Pattern: "f[aeiou]o",
		Message: []byte(msgStr),
	}, msg2)
}

// Ensure that PubSubConn properly handles the case where the Conn it's reading
// from returns a timeout error
func TestPubSubTimeout(t *T) {
	ctx := testCtx(t)
	c, pubC := NewPubSubConn(dial()), dial()
	c.(*pubSubConn).testEventCh = make(chan string, 1)

	ch, msgCh := randStr(), make(chan PubSubMessage, 1)
	require.Nil(t, c.Subscribe(ctx, msgCh, ch))

	msgStr := randStr()
	doneCh := make(chan struct{})
	defer func() {
		<-doneCh
	}()
	go func() {
		time.Sleep(pubSubTimeout + time.Second)
		assert.NoError(t, pubC.Do(ctx, Cmd(nil, "PUBLISH", ch, msgStr)))
		close(doneCh)
	}()

	assert.Equal(t, "timeout", <-c.(*pubSubConn).testEventCh)
	msg := assertMsgRead(t, msgCh)
	assert.Equal(t, msgStr, string(msg.Message))
}

// This attempts to catch weird race conditions which might occur due to
// subscribing/unsubscribing quickly on an active channel.
func TestPubSubChaotic(t *T) {
	ctx := testCtx(t)
	c, pubC := NewPubSubConn(dial()), dial()
	defer func() {
		assert.NoError(t, c.Close())
	}()

	ch, msgStr := randStr(), randStr()

	stopCh := make(chan struct{})
	defer func() {
		stopCh <- struct{}{}
		<-stopCh
	}()
	go func() {
		for {
			select {
			case <-stopCh:
				stopCh <- struct{}{}
				return
			default:
				publish(t, pubC, ch, msgStr)
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	msgCh := make(chan PubSubMessage, 100)
	require.Nil(t, c.Subscribe(ctx, msgCh, ch))

	stopAfter := time.After(5 * time.Second)
	toggleTicker := time.Tick(250 * time.Millisecond)
	subbed := true
	for {
		waitFor := time.NewTimer(100 * time.Millisecond)
		select {
		case <-stopAfter:
			return
		case <-waitFor.C:
			if subbed {
				t.Fatal("waited too long to receive message")
			}
		case msg := <-msgCh:
			waitFor.Stop()
			assert.Equal(t, msgStr, string(msg.Message))
		case <-toggleTicker:
			waitFor.Stop()
			if subbed {
				require.Nil(t, c.Unsubscribe(ctx, msgCh, ch))
			} else {
				require.Nil(t, c.Subscribe(ctx, msgCh, ch))
			}
			subbed = !subbed
		}
	}
}

func BenchmarkPubSub(b *B) {
	ctx := testCtx(b)
	c, pubC := NewPubSubConn(dial()), dial()
	defer c.Close()
	defer pubC.Close()

	msg := randStr()
	msgCh := make(chan PubSubMessage, 1)
	require.Nil(b, c.Subscribe(ctx, msgCh, "benchmark"))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := pubC.Do(ctx, Cmd(nil, "PUBLISH", "benchmark", msg)); err != nil {
			b.Fatal(err)
		}
		<-msgCh
	}
}

func ExampleNewPubSubConn() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a normal redis connection
	conn, err := Dial(ctx, "tcp", "127.0.0.1:6379")
	if err != nil {
		panic(err)
	}

	// Pass that connection into PubSub, conn should never get used after this
	ps := NewPubSubConn(conn)
	defer ps.Close() // this will close Conn as well

	// Subscribe to a channel called "myChannel". All publishes to "myChannel"
	// will get sent to msgCh after this
	msgCh := make(chan PubSubMessage)
	if err := ps.Subscribe(ctx, msgCh, "myChannel"); err != nil {
		panic(err)
	}

	// It's optional, but generally advisable, to periodically Ping the
	// connection to ensure it's still alive. This should be done in a separate
	// go-routine from that which is reading from msgCh.
	errCh := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if err := ps.Ping(ctx); err != nil {
				errCh <- err
				return
			}
		}
	}()

	for {
		select {
		case msg := <-msgCh:
			log.Printf("publish to channel %q received: %q", msg.Channel, msg.Message)
		case err := <-errCh:
			panic(err)
		}
	}
}

func ExampleNewPersistentPubSubConn_cluster() {
	// Example of how to use PersistentPubSub with a Cluster instance.

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Initialize the cluster in any way you see fit
	cluster, err := NewCluster(ctx, []string{"127.0.0.1:6379"})
	if err != nil {
		panic(err)
	}

	// Have PersistentPubSub pick a random cluster node everytime it wants to
	// make a new connection. If the node fails PersistentPubSub will
	// automatically pick a new node to connect to.
	ps, err := NewPersistentPubSubConn(ctx, "", "",
		PersistentPubSubConnFunc(func(ctx context.Context, _ string, _ string) (Conn, error) {
			topo := cluster.Topo()
			node := topo[rand.Intn(len(topo))]
			return Dial(ctx, "tcp", node.Addr)
		},
		))
	if err != nil {
		panic(err)
	}

	// Use the PubSubConn as normal.
	msgCh := make(chan PubSubMessage)
	ps.Subscribe(ctx, msgCh, "myChannel")
	for msg := range msgCh {
		log.Printf("publish to channel %q received: %q", msg.Channel, msg.Message)
	}
}
