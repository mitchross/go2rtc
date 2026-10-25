package streams

import (
	"fmt"
	"maps"
	"net/url"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/probe"
)

type Preload struct {
	stream *Stream      // Don't include the stream in JSON to avoid leaking secrets.
	Cons   *probe.Probe `json:"consumer"`
	Query  string       `json:"query"`
}

var preloads = map[string]*Preload{}
var preloadsMu sync.Mutex

func AddPreload(name, rawQuery string) error {
	if rawQuery == "" {
		rawQuery = "video&audio"
	}

	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return err
	}

	stream := Get(name)
	if stream == nil {
		return fmt.Errorf("streams: stream not found: %s", name)
	}
	cons := probe.Create("preload", query)

	// Dial without the lock: a failing source (ex. Nest retries) would block every other preload.
	if err = stream.AddConsumer(cons); err != nil {
		return err
	}

	preloadsMu.Lock()
	old := preloads[name]
	preloads[name] = &Preload{stream: stream, Cons: cons, Query: rawQuery}
	preloadsMu.Unlock()

	if old != nil {
		old.stream.RemoveConsumer(old.Cons)
	}
	return nil
}

// preloadRetryDelays is the backoff for config preloads whose first dial fails.
var preloadRetryDelays = []time.Duration{5 * time.Second, 15 * time.Second, 30 * time.Second, time.Minute}

// addPreloadRetry keeps trying a config preload, so a source that is offline at
// startup still gets its persistent consumer once it comes back.
func addPreloadRetry(name, rawQuery string, delays []time.Duration) {
	for attempt := 0; ; attempt++ {
		err := AddPreload(name, rawQuery)
		if err == nil {
			return
		}
		if Get(name) == nil {
			log.Error().Err(err).Caller().Send()
			return
		}

		delay := delays[min(attempt, len(delays)-1)]
		log.Warn().Err(err).Str("stream", name).Stringer("retry", delay).Msg("[streams] preload failed")
		time.Sleep(delay)
	}
}

func DelPreload(name string) error {
	preloadsMu.Lock()
	defer preloadsMu.Unlock()

	if p := preloads[name]; p != nil {
		p.stream.RemoveConsumer(p.Cons)
		delete(preloads, name)
		return nil
	}

	return fmt.Errorf("streams: preload not found: %s", name)
}

func GetPreloads() map[string]*Preload {
	preloadsMu.Lock()
	defer preloadsMu.Unlock()
	return maps.Clone(preloads)
}
