package logstream_test

import (
	"testing"
	"time"

	"github.com/gogo/goserverless/internal/logstream"
)

func TestPublishDoesNotBlockOnSlowSubscriber(t *testing.T) {
	hub := logstream.NewHub()
	events, unsubscribe := hub.Subscribe("slow-consumer")

	published := make(chan struct{})
	go func() {
		defer close(published)
		for i := 0; i < 1_000; i++ {
			hub.Publish("slow-consumer", 1, "log line", false)
		}
	}()

	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-published:
		unsubscribe()
		return
	case <-timer.C:
	}

	drained := make(chan struct{})
	go func() {
		for range events {
		}
		close(drained)
	}()
	select {
	case <-published:
	case <-time.After(2 * time.Second):
		t.Fatal("publisher stayed blocked after the subscriber resumed")
	}
	unsubscribe()
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("subscriber did not close")
	}
	t.Fatal("publishing blocked behind a subscriber that stopped consuming")
}
