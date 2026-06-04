package rtmp

import (
	"sync"
	"time"

	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/mediamtx/internal/stream"
	"github.com/bluenviron/mediamtx/internal/unit"
)

type bufferEntry struct {
	pts   time.Duration
	media *description.Media
	forma format.Format
	u     *unit.Unit
}

// Buffer is a jitter buffer that re-clocks RTMP packet delivery based on PTS timing.
// It accumulates bufferTime worth of data, then delivers packets spaced according
// to their PTS deltas, smoothing out irregular arrival timing from unstable connections.
type Buffer struct {
	bufferTime time.Duration
	subStream  **stream.SubStream

	mu      sync.Mutex
	entries []bufferEntry
	notify  chan struct{}
	done    chan struct{}
	closed  bool
}

// NewBuffer creates a new RTMP jitter buffer.
// If bufferTime is 0, Push calls write directly with no buffering.
// When bufferTime > 0, call Close() to stop the delivery goroutine and flush.
func NewBuffer(bufferTime time.Duration, subStream **stream.SubStream) *Buffer {
	b := &Buffer{
		bufferTime: bufferTime,
		subStream:  subStream,
	}
	if bufferTime > 0 {
		b.notify = make(chan struct{}, 1)
		b.done = make(chan struct{})
		go b.deliverLoop()
	}
	return b
}

// Push adds a unit to the buffer. pts is the raw stream-relative presentation
// timestamp used to schedule delivery timing.
func (b *Buffer) Push(pts time.Duration, medi *description.Media, forma format.Format, u *unit.Unit) {
	if b.bufferTime == 0 {
		(*b.subStream).WriteUnit(medi, forma, u)
		return
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.entries = append(b.entries, bufferEntry{
		pts:   pts,
		media: medi,
		forma: forma,
		u:     u,
	})
	b.mu.Unlock()

	// Signal the delivery goroutine that new data is available.
	select {
	case b.notify <- struct{}{}:
	default:
	}
}

// deliverLoop runs in a goroutine. It waits for the buffer to fill to bufferTime,
// then delivers packets paced by PTS deltas.
func (b *Buffer) deliverLoop() {
	defer close(b.done)

	// Phase 1: Wait for buffer to accumulate bufferTime worth of data.
	for {
		b.mu.Lock()
		ready := b.bufferReady()
		b.mu.Unlock()

		if ready {
			break
		}

		if _, ok := <-b.notify; !ok {
			// Channel closed, flush remaining.
			b.flushAll()
			return
		}
	}

	// Phase 2: Deliver packets paced by PTS.
	// Anchor the first packet's PTS to the current wall-clock time.
	b.mu.Lock()
	if len(b.entries) == 0 {
		b.mu.Unlock()
		return
	}
	firstPTS := b.entries[0].pts
	b.mu.Unlock()

	wallStart := time.Now()

	for {
		b.mu.Lock()
		if len(b.entries) == 0 {
			if b.closed {
				b.mu.Unlock()
				return
			}
			b.mu.Unlock()
			// Wait for more data.
			if _, ok := <-b.notify; !ok {
				return
			}
			continue
		}

		entry := b.entries[0]
		b.mu.Unlock()

		// Calculate when this packet should be delivered based on PTS offset from first packet.
		ptsOffset := entry.pts - firstPTS
		deliverAt := wallStart.Add(ptsOffset)
		now := time.Now()

		if deliverAt.After(now) {
			// Sleep until delivery time, but wake up if closed.
			timer := time.NewTimer(deliverAt.Sub(now))
			select {
			case <-timer.C:
			case <-b.notify:
				timer.Stop()
				// Check if we were closed.
				b.mu.Lock()
				wasClosed := b.closed
				b.mu.Unlock()
				if wasClosed {
					b.flushAll()
					return
				}
				// Re-check timing on next iteration.
				continue
			}
		}

		// Deliver the packet.
		b.mu.Lock()
		if len(b.entries) == 0 {
			b.mu.Unlock()
			continue
		}
		entry = b.entries[0]
		b.entries = b.entries[1:]
		b.mu.Unlock()

		(*b.subStream).WriteUnit(entry.media, entry.forma, entry.u)
	}
}

// bufferReady returns true when the buffer has accumulated bufferTime worth of PTS span.
// Must be called with b.mu held.
func (b *Buffer) bufferReady() bool {
	if len(b.entries) < 2 {
		return false
	}
	span := b.entries[len(b.entries)-1].pts - b.entries[0].pts
	return span >= b.bufferTime
}

// Flush stops the delivery goroutine and forwards all remaining buffered packets.
// Call this when the publisher disconnects.
func (b *Buffer) Flush() {
	if b.bufferTime == 0 {
		return
	}

	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()

	// Wake up the delivery goroutine so it can exit.
	close(b.notify)

	// Wait for the delivery goroutine to finish.
	<-b.done
}

// flushAll delivers all remaining buffered packets immediately.
func (b *Buffer) flushAll() {
	b.mu.Lock()
	toFlush := b.entries
	b.entries = nil
	b.mu.Unlock()

	for _, e := range toFlush {
		(*b.subStream).WriteUnit(e.media, e.forma, e.u)
	}
}
