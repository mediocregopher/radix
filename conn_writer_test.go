package radix

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/mediocregopher/radix/v4/resp"
	"github.com/mediocregopher/radix/v4/resp/resp3"
)

type nullWriteDeadliner struct{}

func (nullWriteDeadliner) SetWriteDeadline(time.Time) error { return nil }

type mockTicker struct {
	paused bool
}

func (t *mockTicker) Stop() {
	t.paused = true
}

func (t *mockTicker) Reset(time.Duration) {
	t.paused = false
}

type mockBufioWriter struct {
	buffered, contents []byte
	errCh              chan error
}

func (bw *mockBufioWriter) nextErr() error {
	select {
	case err := <-bw.errCh:
		return err
	default:
		return nil
	}
}

func (bw *mockBufioWriter) Write(b []byte) (int, error) {
	if err := bw.nextErr(); err != nil {
		return 0, err
	}
	bw.buffered = append(bw.buffered, b...)
	return len(b), nil
}

func (bw *mockBufioWriter) Flush() error {
	if err := bw.nextErr(); err != nil {
		return err
	}
	bw.contents = append(bw.contents, bw.buffered...)
	bw.buffered = bw.buffered[:0]
	return nil
}

func TestConnWriter(t *testing.T) {
	type mu struct {
		marshal      string
		unmarshal    bool
		expUnmarshal bool
		expErr       bool
	}

	type step struct {
		mu              *mu
		tick            bool
		bwErr, bwNilErr bool
	}

	type test struct {
		descr          string
		flushInterval  bool
		steps          []step
		expBufContents string
	}

	tests := []test{
		{
			descr: "single",
			steps: []step{
				{mu: &mu{marshal: "a", unmarshal: true, expUnmarshal: true}},
			},
			expBufContents: "a",
		},
		{
			descr: "single marshal error",
			steps: []step{
				{bwErr: true},
				{mu: &mu{marshal: "a", unmarshal: true, expErr: true}},
			},
		},
		{
			descr: "single flush error",
			steps: []step{
				{bwNilErr: true}, {bwErr: true},
				{mu: &mu{marshal: "a", unmarshal: true, expErr: true}},
			},
		},
		{
			descr:         "single",
			flushInterval: true,
			steps: []step{
				{mu: &mu{marshal: "a", unmarshal: true, expUnmarshal: true}},
			},
			expBufContents: "a",
		},
		{
			descr:         "single marshal error",
			flushInterval: true,
			steps: []step{
				{bwErr: true},
				{mu: &mu{marshal: "a", unmarshal: true, expErr: true}},
			},
		},
		{
			descr:         "single flush error",
			flushInterval: true,
			steps: []step{
				{bwNilErr: true}, {bwErr: true},
				{mu: &mu{marshal: "a", unmarshal: true, expErr: true}},
			},
		},
		{
			descr: "multi",
			steps: []step{
				{mu: &mu{marshal: "a", unmarshal: true, expUnmarshal: true}},
				{mu: &mu{marshal: "b", unmarshal: true, expUnmarshal: true}},
				{mu: &mu{marshal: "c", unmarshal: true, expUnmarshal: true}},
			},
			expBufContents: "abc",
		},
		{
			descr: "multi marshal error",
			steps: []step{
				{bwErr: true},
				{mu: &mu{marshal: "a", unmarshal: true, expErr: true}},
				{mu: &mu{marshal: "b", unmarshal: true, expUnmarshal: true}},
				{mu: &mu{marshal: "c", unmarshal: true, expUnmarshal: true}},
			},
			expBufContents: "bc",
		},
		{
			descr: "multi flush error",
			steps: []step{
				{bwNilErr: true},
				{bwErr: true},
				{mu: &mu{marshal: "a", unmarshal: true, expErr: true}},
				{mu: &mu{marshal: "b", unmarshal: true, expUnmarshal: true}},
				{mu: &mu{marshal: "c", unmarshal: true, expUnmarshal: true}},
			},
			expBufContents: "abc",
		},
		{
			descr:         "multi",
			flushInterval: true,
			steps: []step{
				{mu: &mu{marshal: "a", unmarshal: true, expUnmarshal: true}},
				{mu: &mu{marshal: "b", unmarshal: true, expUnmarshal: true}},
				{mu: &mu{marshal: "c", unmarshal: true, expUnmarshal: true}},
				{tick: true},
			},
			expBufContents: "abc",
		},
		{
			descr:         "multi marshal error",
			flushInterval: true,
			steps: []step{
				{bwNilErr: true}, {bwNilErr: true}, // allow the first marshaler
				{mu: &mu{marshal: "a", unmarshal: true, expUnmarshal: true}},
				{bwErr: true},
				{mu: &mu{marshal: "b", unmarshal: true, expErr: true}},
				{mu: &mu{marshal: "c", unmarshal: true, expUnmarshal: true}},
				{tick: true},
			},
			expBufContents: "ac",
		},
		{
			descr:         "multi flush error",
			flushInterval: true,
			steps: []step{
				{bwNilErr: true}, {bwNilErr: true}, // allow the first marshal/flush
				{mu: &mu{marshal: "a", unmarshal: true, expUnmarshal: true}},
				{bwNilErr: true}, {bwNilErr: true}, // allow the second/third marshals
				{mu: &mu{marshal: "b", unmarshal: true, expErr: true}},
				{mu: &mu{marshal: "c", unmarshal: true, expErr: true}},
				{bwErr: true}, // deny the flush
				{tick: true},
			},
			expBufContents: "a",
		},
		{
			descr:         "multi wait",
			flushInterval: true,
			steps: []step{
				{mu: &mu{marshal: "a", unmarshal: true, expUnmarshal: true}},
				{mu: &mu{marshal: "b", unmarshal: true, expUnmarshal: true}},
				{mu: &mu{marshal: "c", unmarshal: true, expUnmarshal: true}},
				{tick: true},
				{tick: true},
				{mu: &mu{marshal: "d", unmarshal: true, expUnmarshal: true}},
			},
			expBufContents: "abcd",
		},
		{
			descr:         "multi wait2",
			flushInterval: true,
			steps: []step{
				{mu: &mu{marshal: "a", unmarshal: true, expUnmarshal: true}},
				{mu: &mu{marshal: "b", unmarshal: true, expUnmarshal: true}},
				{mu: &mu{marshal: "c", unmarshal: true, expUnmarshal: true}},
				{tick: true},
				{tick: true},
				{mu: &mu{marshal: "d", unmarshal: true, expUnmarshal: true}},
				{mu: &mu{marshal: "e", unmarshal: true, expUnmarshal: true}},
				{tick: true},
			},
			expBufContents: "abcde",
		},
		{
			descr: "multi no marshal",
			steps: []step{
				{mu: &mu{unmarshal: true, expUnmarshal: true}},
				{mu: &mu{marshal: "a", unmarshal: true, expUnmarshal: true}},
			},
			expBufContents: "a",
		},
		{
			descr: "multi no unmarshal",
			steps: []step{
				{mu: &mu{marshal: "a", unmarshal: true, expUnmarshal: true}},
				{mu: &mu{marshal: "b"}},
			},
			expBufContents: "ab",
		},
		{
			descr:         "multi no marshal",
			flushInterval: true,
			steps: []step{
				{mu: &mu{unmarshal: true, expUnmarshal: true}},
				{mu: &mu{marshal: "a", unmarshal: true, expUnmarshal: true}},
				{tick: true},
			},
			expBufContents: "a",
		},
		{
			descr:         "multi no unmarshal",
			flushInterval: true,
			steps: []step{
				{mu: &mu{marshal: "a", unmarshal: true, expUnmarshal: true}},
				{mu: &mu{marshal: "b"}},
				{tick: true},
			},
			expBufContents: "ab",
		},
	}

	ternaryStr := func(pred bool, t, f string) string {
		if pred {
			return t
		}
		return f
	}

	for _, test := range tests {
		name := fmt.Sprintf(
			"%s/%s",
			ternaryStr(test.flushInterval, "with flush", "without flush"),
			test.descr,
		)
		t.Run(name, func(t *testing.T) {
			// setup
			wCh := make(chan connMarshalerUnmarshaler)
			rCh := make(chan connMarshalerUnmarshaler, len(test.steps))
			bw := &mockBufioWriter{
				errCh: make(chan error, len(test.steps)),
			}
			doneCh := make(chan struct{})
			var ticker *mockTicker
			var tickerCh chan time.Time
			cw := &connWriter{
				wCh:         wCh,
				rCh:         rCh,
				bw:          bw,
				conn:        nullWriteDeadliner{},
				opts:        resp.NewOpts(),
				doneCh:      doneCh,
				eventLoopCh: make(chan struct{}),
			}
			if test.flushInterval {
				ticker = new(mockTicker)
				tickerCh = make(chan time.Time)
				cw.flushInterval = 1
				cw.flushTicker = ticker
				cw.flushTickerCh = tickerCh
			}

			cleanedUpCh := make(chan struct{})
			go func() {
				cw.run()
				close(cleanedUpCh)
			}()
			cleanup := func() {
				close(doneCh)
				select {
				case <-cleanedUpCh:
				case <-time.After(5 * time.Second):
					t.Fatal("took too long to clean up")
				}
			}

			type muTup struct {
				mu                       mu
				connMarshalerUnmarshaler connMarshalerUnmarshaler
			}

			var expMUs []muTup

			// setup complete, start the actual test
			for i, step := range test.steps {
				switch {
				case step.mu != nil:
					mu := connMarshalerUnmarshaler{
						ctx:   context.Background(),
						errCh: make(chan error, 1),
					}
					if step.mu.marshal != "" {
						mu.marshal = resp3.RawMessage(step.mu.marshal)
					}
					if step.mu.unmarshal {
						mu.unmarshalInto = new(resp3.RawMessage)
					}
					wCh <- mu
					expMUs = append(expMUs, muTup{
						mu:                       *step.mu,
						connMarshalerUnmarshaler: mu,
					})

				case step.tick:
					if ticker.paused {
						t.Errorf("tick performed at step %d but ticker is paused", i)
					}
					tickerCh <- time.Time{}

				case step.bwErr:
					bw.errCh <- errors.New("test error")

				case step.bwNilErr:
					bw.errCh <- nil

				default:
					panic(fmt.Sprintf("bad step %#v", step))
				}

				select {
				case cw.eventLoopCh <- struct{}{}:
				case <-time.After(5 * time.Second):
					t.Errorf("timed out waiting for event loop after step %d (%+v)", i, step)
				}
			}
			cleanup()

			// check that all connMarshalerUnmarshaler got expected results
			for _, expMU := range expMUs {
				if expMU.mu.expUnmarshal {
					select {
					case err := <-expMU.connMarshalerUnmarshaler.errCh:
						t.Errorf("expected no error for %+v but got %v", expMU.mu, err)
					default:
					}
					select {
					case gotMU := <-rCh:
						if !reflect.DeepEqual(gotMU, expMU.connMarshalerUnmarshaler) {
							t.Errorf("expected connMarshalerUnmarshaler %+v but got %+v", expMU.mu, gotMU)
						}
					default:
						t.Errorf("expected to read from rCh for %+v, but it's empty", expMU.mu)
					}

				} else if expMU.mu.expErr {
					select {
					case <-expMU.connMarshalerUnmarshaler.errCh:
					default:
						t.Errorf("errCh for %+v never got written", expMU.mu)
					}
				}
			}

			// check that the write buffer got expected results
			if bufContents := string(bw.contents); bufContents != test.expBufContents {
				t.Errorf("expected %q to have been written, but got %q", test.expBufContents, bufContents)
			}
		})
	}
}
