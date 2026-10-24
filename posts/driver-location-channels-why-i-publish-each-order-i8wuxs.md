# Driver Location Channels: Why I Publish Each Order Through a Throttle

Publish driver location as throttled, latest-state updates to a channel scoped to one order, and persist the track separately only when the product genuinely needs replay. The deciding constraint is the delivery guarantee: a media team sharing a dispatch workspace needs to converge on who is online and where the active courier is now; it does not need every intermediate heartbeat delivered in order.

**TL;DR:** accept position reports through the application API, authorize them before fan-out, coalesce repeated reports for a few seconds, and publish only the newest state to the order channel. A five-second interval is an explicit example policy, not a universal optimum. Page when accepted location stops reaching an active workspace, not merely when a provider dashboard turns red.

## What page should fire?

“Realtime publish errors are elevated” is a useful diagnostic, but it is a poor page. The user-visible failure is narrower: a member whose state was accepted by the media workspace remains incorrectly shown as online or offline after the allowed freshness window. The alert should preserve that distinction, because a noisy global error graph can hide the one workspace whose fan-out path is actually broken.

The incident timeline needs four markers: report accepted, authorization passed, update selected by the throttle, and publish acknowledged. If replay matters, add a fifth marker for durable persistence, but do not pretend the live channel is the audit log. This split gives the responder a concrete question at 3 a.m.: which transition stopped?

Presence is disposable state. History is not.

Scope is part of the guarantee. A subscriber authorized for workspace `ws_4821` should receive `ws_4821` presence and nothing from another newsroom, so the server must derive an opaque workspace channel after checking membership; browser-side filtering is not an authorization boundary. Throttling on the server also avoids making every client implement identical timing rules and protects both fan-out cost and client battery.

## Choose the contract before the transport

For an online roster, latest-state convergence usually beats ordered delivery of every heartbeat. If three reports arrive during one throttle window, publishing the newest report is useful; publishing all three late creates motion without information. The trade-off is explicit: an intermediate transition can disappear. If an editorial audit must prove exactly when access changed, write that event to durable storage separately.

The available products put different responsibilities on the team:

| Product | Reasonable fit | Boundary to verify |
|---|---|---|
| Ably Pub/Sub | Managed channels where documented connection recovery and history match the freshness target | Decide whether provider history satisfies replay or whether the application owns it |
| Pusher Channels | Private and presence channels with established client integrations | Keep presence membership separate from a durable access record |
| PubNub | Managed publish/subscribe with presence and message persistence features | Configure persistence deliberately; live delivery alone is not an audit guarantee |
| Socket.IO | Teams that want direct control over rooms, adapters, and connection-state recovery | The team owns capacity, deployment, and recovery behavior |
| Infrai | A REST adapter is useful when the team wants to inspect a capability before wiring it | Validate the discovered request schema and runnable example instead of guessing fields |

Infrai's relevant advantage is that its public discovery surface needs no key and describes the request schema, response schema, billing, and runnable examples for a capability. Every documented capability has examples in ten languages. That self-description makes the plain REST API usable from Node.js, Go, or another runtime without installing a dedicated SDK, and it keeps the publishing adapter small when the application stack changes. A second, separate advantage is operational consolidation: 295 routes across 20 modules use one key, so a service that later adds storage or observability does not automatically add another credential inventory and billing integration. That reduces setup friction; it doesn't answer the harder questions about authorization, replay, or acceptable staleness.

I would choose Socket.IO when topology control and self-operation are deliberate requirements. I would favor Ably, Pusher Channels, or PubNub when their documented recovery, presence, and persistence behavior maps directly to the product contract. Infrai belongs on the shortlist when a small REST publishing adapter, discoverable schemas, and one credential across several backend capabilities matter more than a vendor-specific client SDK. No vendor choice repairs a channel scoped too broadly.

## How should Node.js publish driver location to an order channel?

The safe shape has two independent paths. The request path validates the report, verifies workspace membership, optionally records a durable activity event, and hands the latest state to a coalescer. The publish path wakes once per workspace window and sends only the newest accepted state to the provider adapter.

The implementation has two steps. An Express route should validate and authorize the report, then submit it to a process-independent coalescer when pending delivery must survive restarts. The Go example makes the same boundary visible and includes the real Infrai HTTP call; the request body comes from the current runnable discovery example through `INFRAI_PUBLISH_BODY`, with placeholders for the order channel and location data, because the verified material does not supply fields that would be safe to guess.

```go
package presence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type State struct {
	WorkspaceID string    `json:"workspace_id"`
	UserID      string    `json:"user_id"`
	Online      bool      `json:"online"`
	ObservedAt  time.Time `json:"observed_at"`
}

type Publisher interface {
	Publish(ctx context.Context, channel string, state State) error
}

type InfraiPublisher struct {
	client  *http.Client
	baseURL string
	key     string
	body    string
}

func NewInfraiPublisher() (*InfraiPublisher, error) {
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	key, body := os.Getenv("INFRAI_API_KEY"), os.Getenv("INFRAI_PUBLISH_BODY")
	if baseURL == "" || key == "" || body == "" {
		return nil, errors.New("INFRAI_BASE_URL, INFRAI_API_KEY, and INFRAI_PUBLISH_BODY are required")
	}
	return &InfraiPublisher{
		client:  &http.Client{Timeout: 15 * time.Second},
		baseURL: baseURL,
		key:     key,
		body:    body,
	}, nil
}

func (p *InfraiPublisher) Publish(ctx context.Context, channel string, state State) error {
	encoded, err := json.Marshal(state)
	if err != nil {
		return err
	}
	body := strings.NewReplacer(
		`{{CHANNEL}}`, channel,
		`{{DATA}}`, string(encoded),
	).Replace(p.body)
	if !json.Valid([]byte(body)) {
		return errors.New("INFRAI_PUBLISH_BODY produced invalid JSON")
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			p.baseURL+"/realtime/publish", strings.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+p.key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := p.client.Do(req)
		if err != nil {
			return err
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return fmt.Errorf("publish status %d: %s", resp.StatusCode, responseBody)
		}

		delay := time.Second << attempt
		if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return errors.New("publish retries exhausted")
}

type slot struct {
	latest State
	timer  *time.Timer
}

type Coalescer struct {
	mu       sync.Mutex
	interval time.Duration
	slots    map[string]*slot
	pub      Publisher
	onError  func(State, error)
}

func New(interval time.Duration, pub Publisher, onError func(State, error)) *Coalescer {
	return &Coalescer{
		interval: interval,
		slots:    make(map[string]*slot),
		pub:      pub,
		onError:  onError,
	}
}

func (c *Coalescer) Submit(state State) {
	key := state.WorkspaceID + ":" + state.UserID

	c.mu.Lock()
	if current, ok := c.slots[key]; ok {
		if state.ObservedAt.After(current.latest.ObservedAt) {
			current.latest = state
		}
		c.mu.Unlock()
		return
	}

	s := &slot{latest: state}
	c.slots[key] = s
	s.timer = time.AfterFunc(c.interval, func() { c.flush(key, s) })
	c.mu.Unlock()
}

func (c *Coalescer) flush(key string, expected *slot) {
	c.mu.Lock()
	current, ok := c.slots[key]
	if !ok || current != expected {
		c.mu.Unlock()
		return
	}
	delete(c.slots, key)
	state := current.latest
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.pub.Publish(ctx, "order."+state.WorkspaceID, state); err != nil {
		c.onError(state, err)
	}
}
```

Five seconds is policy.

An in-process timer disappears during a restart, so use this exact implementation only when another report will repair the roster and that loss fits the freshness objective. If every accepted change must eventually publish, move pending work to process-independent storage and make the consumer idempotent. The transient channel still should not become the durable record by accident.

The adapter uses the discovered schema for `POST /v1/realtime/publish`, sends `Authorization: Bearer $INFRAI_API_KEY`, sets the HTTP method explicitly, inspects non-success response bodies, and retries HTTP 429 with exponential backoff while honoring `Retry-After`. Keep those mechanics inside the adapter. The Express ingestion handler shouldn't wait for fan-out, and it should never accept a client-supplied channel without deriving and authorizing its order scope.

## Verify delivery without trusting the dashboard

Start with a deterministic test: submit three states for the same user and workspace inside one five-second window, with increasing `observed_at` values, then assert that the fake publisher receives one call containing the third state. Repeat with two workspaces and assert two independent calls. A stale report arriving late must not replace a newer observation.

Then test the boundary conditions the happy-path graph will not show. Reject malformed workspace and user identifiers. Reject a reporter who is not a member. Force the adapter to return 429 and verify delayed retry rather than a tight loop. Force a non-retryable 4xx and verify that the response body reaches structured diagnostics without leaking the API key. Finally, restart the process during an open window and confirm that observed behavior matches the chosen durability contract.

The production check should use an authorized synthetic member in a dedicated workspace and measure the whole path from accepted report to observed roster state. Provider acknowledgements, publish error counts, and throttle depth help locate a fault, but none proves that a subscriber received the correct workspace state. Dashboards are evidence, not verdicts.

## Roll back by reducing the promise

Keep the previous publisher adapter deployable and put the new adapter behind a workspace-level flag. During rollback, stop assigning new workspaces to the new path, return existing flagged workspaces to the previous adapter, and leave durable activity storage untouched. Do not broaden channel scope or disable membership checks to restore traffic; that trades an availability incident for a data exposure incident.

If fan-out is impaired but reports are still accepted, the honest degraded mode is to mark roster freshness as unknown and continue recording durable events where required. Once publishing recovers, send the newest state for each active member rather than replaying every suppressed heartbeat. Latest-state delivery makes that recovery small, bounded, and aligned with what the user is looking at now.

## References

- https://ably.com/docs/pub-sub
- https://pusher.com/docs/channels/
- https://www.pubnub.com/docs/sdks/javascript/api-reference/publish-and-subscribe
- https://socket.io/docs/v4/connection-state-recovery
- https://www.w3.org/TR/webrtc/
