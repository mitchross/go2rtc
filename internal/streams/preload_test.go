package streams

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/stretchr/testify/require"
)

type preloadTestProducer struct {
	core.Connection
	done chan struct{}
	once sync.Once
}

func newPreloadTestProducer() *preloadTestProducer {
	media := &core.Media{
		Kind:      core.KindVideo,
		Direction: core.DirectionRecvonly,
		Codecs:    []*core.Codec{{Name: core.CodecH264, ClockRate: 90000}},
	}
	return &preloadTestProducer{
		Connection: core.Connection{ID: core.NewID(), FormatName: "preloadtest", Medias: []*core.Media{media}},
		done:       make(chan struct{}),
	}
}

func (p *preloadTestProducer) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	for _, receiver := range p.Receivers {
		if receiver.Codec == codec {
			return receiver, nil
		}
	}
	receiver := core.NewReceiver(media, codec)
	p.Receivers = append(p.Receivers, receiver)
	return receiver, nil
}

func (p *preloadTestProducer) Start() error {
	<-p.done
	return nil
}

func (p *preloadTestProducer) Stop() error {
	p.once.Do(func() { close(p.done) })
	return nil
}

func TestPreloadRetriesUntilSourceIsOnline(t *testing.T) {
	var dials atomic.Int32
	HandleFunc("preloadtest", func(string) (core.Producer, error) {
		if dials.Add(1) <= 2 {
			return nil, errors.New("source offline")
		}
		return newPreloadTestProducer(), nil
	})

	stream, err := New("preload_retry", "preloadtest:camera")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = DelPreload("preload_retry")
		Delete("preload_retry")
	})

	done := make(chan struct{})
	go func() {
		addPreloadRetry("preload_retry", "video", []time.Duration{time.Millisecond})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("preload was not retried")
	}

	require.Equal(t, int32(3), dials.Load())
	require.Contains(t, GetPreloads(), "preload_retry")
	stream.mu.Lock()
	require.Len(t, stream.consumers, 1)
	stream.mu.Unlock()
}

func TestPreloadRetryStopsForMissingStream(t *testing.T) {
	done := make(chan struct{})
	go func() {
		addPreloadRetry("preload_missing", "video", []time.Duration{time.Millisecond})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("preload kept retrying a missing stream")
	}
	require.NotContains(t, GetPreloads(), "preload_missing")
}
