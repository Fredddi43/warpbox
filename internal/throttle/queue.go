// Package throttle implements a blocking request queue with a token-bucket rate limiter.
//
// Warpbox never fails fast. Burst traffic from Plex is queued and trickled
// to the TorBox API at a safe rate below 300 requests per minute.
package throttle

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const queueBufferSize = 1024

// Request represents a queued API call.
type Request struct {
	Label   string
	Execute func(ctx context.Context) error
}

// Queue is a rate-limited, blocking queue for TorBox API requests.
type Queue struct {
	mu         sync.Mutex
	items      chan Request
	rate       time.Duration // base spacing between calls (from configured RPM)
	backoff    float64       // adaptive multiplier (>=1.0); widens spacing on observed 429s, decays on success
	lastCall   time.Time
	totalCalls int64
	callWindow []time.Time

	successfulCalls int64
	failedCalls     int64
	http429Calls    int64
}

// Stats returns throttle statistics for the landing page.
type Stats struct {
	TotalCalls        int64
	SuccessfulCalls   int64
	FailedCalls       int64
	HTTP429Calls      int64
	CallsLastMinute   int
	RequestsPerMinute int
}

// NewQueue creates a new throttle queue.
// requestsPerMinute sets the maximum sustained call rate.
func NewQueue(requestsPerMinute int) *Queue {
	return &Queue{
		rate:    time.Minute / time.Duration(requestsPerMinute),
		backoff: 1.0,
		items:   make(chan Request, queueBufferSize),
	}
}

// Adaptive backoff constants. On each observed 429 the spacing between ALL
// queued API calls widens multiplicatively (so requestdl, mylist, and every
// hang/poll re-fetch slow together until TorBox stops rate-limiting); each
// successful call decays it back toward the configured base rate. This
// self-tunes to TorBox's real per-endpoint ceiling without hard-coding it.
const (
	throttleBackoffGrow  = 1.5
	throttleBackoffMax   = 8.0
	throttleBackoffDecay = 0.95
)

// Stats returns current throttle statistics.
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Count calls in the last 60 seconds.
	now := time.Now()
	cutoff := now.Add(-60 * time.Second)
	recent := 0
	for _, t := range q.callWindow {
		if t.After(cutoff) {
			recent++
		}
	}

	return Stats{
		TotalCalls:        q.totalCalls,
		SuccessfulCalls:   q.successfulCalls,
		FailedCalls:       q.failedCalls,
		HTTP429Calls:      q.http429Calls,
		CallsLastMinute:   recent,
		RequestsPerMinute: int(time.Minute / q.rate),
	}
}

// Enqueue adds a request to the blocking queue.
// If the queue buffer is full, Enqueue blocks until space is available.
func (q *Queue) Enqueue(r Request) {
	q.items <- r
}

// processLoop runs in a goroutine, receiving and executing requests at the
// configured rate. It blocks on the channel when the queue is empty and
// exits cleanly when the context is cancelled.
func (q *Queue) processLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case r := <-q.items:
			// Enforce minimum spacing between calls, widened by the adaptive
			// backoff multiplier when TorBox has recently been rate-limiting.
			q.mu.Lock()
			eff := time.Duration(float64(q.rate) * q.backoff)
			q.mu.Unlock()
			elapsed := time.Since(q.lastCall)
			if elapsed < eff {
				select {
				case <-ctx.Done():
					return
				case <-time.After(eff - elapsed):
				}
			}

			err := r.Execute(ctx)

			q.mu.Lock()
			q.lastCall = time.Now()
			q.totalCalls++
			if err != nil {
				q.failedCalls++
				// Log throttle-level failure at DEBUG only. The caller (e.g. handleGet)
				// owns the ERROR-level log with full context (torrent_id, file_id, etc.).
				slog.Debug("throttle request failed", "label", r.Label, "error", err)
			} else {
				q.successfulCalls++
				// Recover toward the base rate as calls succeed.
				if q.backoff > 1.0 {
					q.backoff *= throttleBackoffDecay
					if q.backoff < 1.0 {
						q.backoff = 1.0
					}
				}
			}
			q.callWindow = append(q.callWindow, q.lastCall)
			// Keep the window trimmed to roughly the last 60 seconds.
			cutoff := q.lastCall.Add(-60 * time.Second)
			for len(q.callWindow) > 0 && q.callWindow[0].Before(cutoff) {
				q.callWindow = q.callWindow[1:]
			}
			q.mu.Unlock()
		}
	}
}

// Record429 increments the 429 counter and widens the adaptive spacing so that
// ALL queued callers (requestdl, mylist, hang/poll re-fetches) slow together
// until TorBox stops rate-limiting. Called from the TorBox client's 429 hook.
func (q *Queue) Record429() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.http429Calls++
	q.backoff *= throttleBackoffGrow
	if q.backoff > throttleBackoffMax {
		q.backoff = throttleBackoffMax
	}
	slog.Warn("throttle: 429 observed, widening API call spacing (adaptive backoff)",
		"backoff_multiplier", q.backoff,
		"effective_rpm", int(float64(time.Minute)/(float64(q.rate)*q.backoff)),
	)
}

// Start launches the processing loop in a background goroutine.
func (q *Queue) Start(ctx context.Context) {
	go q.processLoop(ctx)
}
