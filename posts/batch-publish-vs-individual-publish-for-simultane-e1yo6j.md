# Batch Publish vs Individual Publish for Simultaneous Realtime Updates in Live Polls

Batch publish is the safer default for a live e-commerce poll with many simultaneous realtime updates, provided every envelope carries an ordered sequence and presence is reconciled from leases. Individual publish remains the right escape hatch for state changes whose timing is part of the product behavior. The deciding measure is presence accuracy during stalls and reconnects, not a tidy message-per-second graph.

Short answer: collect updates for a bounded window, emit one contiguous batch, acknowledge only after client application, and issue a snapshot whenever the sequence or poll version has a gap. Start with a 100 ms window and a 256-event cap, then tune against p99 freshness and queue depth in the actual shopping session.

## What page fired the alert?

I distrust a dashboard that says “connected users” without showing which observation produced the number. At 3am I want the page, the last applied sequence, and the lease age. A socket that existed five seconds ago is not proof that a shopper saw the current question.

For a live poll, define presence as “the session has a valid lease and has applied the current poll version.” Store `session_id`, `last_seen`, `poll_version`, and an expiry time. Store answers separately as idempotent events. This distinction prevents a delayed heartbeat from resurrecting a session that has already expired.

One burst can expose the flaw. Suppose 8,000 shoppers answer within a 200 ms promotion window while 5 percent pause for two seconds. Immediate fan-out puts thousands of writes behind the same slow connections; a reconnecting browser then races a snapshot against buffered answers. The screen can look plausible while its count is stale. A batch supplies one ordering point, but only sequence validation and lease rules make that ordering useful.

## How should batch and individual updates be shaped?

Treat a batch as a typed envelope, not an opaque JSON string. Include the stream, first and last sequence, creation time, and events. Presence renewals are leases or transitions, never an endless log. Answer identity should be stable, such as `(session_id, answer_id)`, so a retry cannot double-count.

```go
type Event struct {
	Seq       uint64 `json:"seq"`
	SessionID string `json:"session_id"`
	Kind      string `json:"kind"`
	PollVer   uint64 `json:"poll_version"`
	Value     string `json:"value,omitempty"`
}

type Batch struct {
	Stream   string  `json:"stream"`
	FirstSeq uint64  `json:"first_seq"`
	LastSeq  uint64  `json:"last_seq"`
	Created  int64   `json:"created_unix_ms"`
	Events   []Event `json:"events"`
}

func flush(stream string, events []Event, now time.Time) Batch {
	return Batch{
		Stream:   stream,
		FirstSeq: events[0].Seq,
		LastSeq:  events[len(events)-1].Seq,
		Created:  now.UnixMilli(),
		Events:   events,
	}
}
```

The client acknowledges `LastSeq` only after applying every event through it. If the next batch starts at 412 and the client has applied 409, it requests recovery; it does not infer what 410 and 411 meant. This is the small rule that turns a delivery choice into a correctness contract.

Individual publish is appropriate for a poll close, a moderation decision, or a payment-state transition where milliseconds change behavior. Send those immediately, but keep the same sequence space and acknowledgement semantics. “Immediate” should describe the deadline, not permission to bypass ordering.

That distinction matters.

## Reconnect recovery and backpressure

A reconnecting browser sends its session identifier and last applied sequence. If the missing range is still retained, return that contiguous range. If the gap exceeds retention, the lease is expired, or the poll version changed, return a snapshot with a new base sequence. Do not replay a worker-buffered heartbeat just because it arrived late.

It failed once in a test run.

The failure was instructive: a paused tab resumed with sequence 811, while the service had already compacted history through 790 and advanced the poll from version 14 to 15. The reconnect code tried to append the two buffered presence renewals before checking the version, briefly showing the shopper as active on the old question. The fix was procedural rather than clever: validate lease and version first, choose range or snapshot second, and apply events third. That order also makes the logs answer the incident question: which page fired, which base sequence was selected, and why?

Backpressure needs a policy written down before the incident. Drop intermediate presence renewals; never drop an answer that the poll counts. Coalesce repeated answers only when the business rule permits it. Bound each connection queue and record its oldest queued sequence. An unbounded queue turns one stalled mobile radio into process-wide memory pressure. The queue limit should be observable and boring: when it trips, mark the connection for snapshot recovery, increment a counter, and let the next healthy write establish a clean base instead of hiding loss behind retries.

## How do you verify the choice before rollout?

Instrument event creation, batch enqueue, transport write, and client apply as four timestamps. Watch p50 and p99 freshness, contiguous-gap failures, snapshot rate, queue depth, and expired leases that later attempt an answer. The connected gauge is context, not a verdict.

Run a deterministic load test with 10,000 viewers, pause 5 percent for two seconds, and reconnect them in waves. Assert that applied sequences are contiguous, expired sessions are absent from the presence count, and a close event precedes any accepted answer. Sweep the collection window from 50 to 250 ms. Keep the smallest window that meets the p99 freshness promise without sustained queue growth.

Rollback is a dispatcher setting: route only the timing-sensitive event kinds through individual delivery, or shorten the batch window and lower its cap. Preserve the envelope, sequence checks, and snapshot path. Removing those checks during an incident makes the next reconnect harder to explain.

## References

- https://www.w3.org/TR/webrtc/
- https://developer.mozilla.org/en-US/docs/Web/API/WebSocket
- https://html.spec.whatwg.org/multipage/server-sent-events.html
