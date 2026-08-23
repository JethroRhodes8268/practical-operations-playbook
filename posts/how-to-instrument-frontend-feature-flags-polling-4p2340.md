# How to Instrument Frontend Feature Flags — Polling API Fallback Config Under Failure

Short answer: poll a versioned flag document, keep a deliberately conservative fallback in the client, and page only when flag evaluation is credibly linked to failed notification delivery rather than when the poller merely has a bad minute.

That distinction is the whole design. A fintech notification service may send login warnings, payment confirmations, and low-urgency product messages through the same delivery path, yet those outcomes don't carry the same operational weight. If a React screen cannot refresh its feature flags API, the immediate question is not “is polling red?” It is “which config did the user evaluate, and did that decision contribute to a notification that now requires human action?”

I've carried the pager through alerts that meant nothing and missed the one that mattered. The postmortem invariant is blunt: a frontend control-plane symptom is evidence, not an incident, until it is joined to a user-visible delivery outcome.

## Reconstruct the notification failure before changing the poller

Treat the browser poller as a small state machine with three inputs: a compiled fallback config, the last validated remote document, and a newly fetched candidate. On startup, evaluate the fallback immediately. After a successful fetch, accept the candidate only if its schema and version are valid, retain that accepted document across later transient fetch failures, and add jitter before the next request so a large client population does not move in lockstep.

The order matters. Replacing a valid cached document with fallback after one timeout can cause more behavior changes than the timeout itself. Keeping an unvalidated response is worse: transport success says nothing about whether the document contains every required key or a usable value. The React component should consume an evaluated snapshot from a dedicated flag store; it should not own timers, network parsing, and business behavior in one effect. That boundary also makes cleanup on unmount and tests with a fake clock unremarkable instead of fragile.

Cache wins.

Use explicit semantics for every flag. In this notification flow, `delivery_status_panel` can safely default to `false`: hiding an optional status panel does not stop the underlying notification. A flag that changes payment authorization would demand a different fallback and a different review process. There is no universal “safe default.” The owner of the affected behavior has to choose it before deployment.

One subtle point gets missed during otherwise competent implementations: record the source of each evaluation as `remote`, `cached`, or `fallback`, but don't turn that source into a user identifier. Prometheus instrumentation guidance warns that every unique label combination creates another time series and specifically cautions against high-cardinality labels. An account ID, notification ID, document version, or raw error string therefore belongs in bounded logs or traces, not in metric labels.

## What should a React frontend feature flags API polling page reveal?

The bounded incident scenario is a notification delivery regression during an ordinary flag refresh failure. A dashboard shows an elevated count of frontend polling errors; meanwhile, a smaller but important set of payment confirmations is not reaching recipients. Paging on the first signal wakes someone for expired browser sessions, offline laptops, blocked requests, and brief network loss. Paging only on the second signal gives no clue whether a recent configuration decision changed the path. The useful alert joins them without pretending correlation is proof.

I start the postmortem backward: what page fired, what decision was expected from the responder, and which evidence would have shortened that decision? If the page merely says “feature flag API errors,” it has no operational verb. If it says “critical notification delivery failures exceed the service objective, and fallback evaluations rose in the same window,” the responder can compare a delivery failure by class, the evaluated flag source, and the deployment timeline. That's enough to investigate. It still isn't enough to declare the flag system guilty.

The minimum useful signals are deliberately small:

| Signal | Stable dimensions | Why it exists |
| --- | --- | --- |
| Flag evaluations | flag name, source | Shows whether remote, cached, or fallback state was used |
| Poll attempts | outcome class | Separates accepted documents from timeout, transport, and validation outcomes |
| Notification delivery | notification class, outcome | Anchors the page to user-visible impact |
| Config age | none, or flag document | Reveals stale accepted state without using version as a label |

Keep the label vocabularies bounded in code. `outcome_class="transport"` is useful; the complete error message is not. `notification_class="payment_confirmation"` can be reasonable if the set is reviewed and finite; `recipient_id` is never a sensible metric label. Logs can carry a request correlation identifier and the document version for investigation, with whatever retention and access controls the financial system requires.

Don't page on fallback use alone.

Fallback is a designed operating state, not automatically a fault. Alert on delivery impact first, then use poll outcome and evaluation source to qualify or route the page. A ticket or daytime warning can cover prolonged config age when delivery remains healthy. This is where signal quality beats dashboard completeness: fewer signals, each attached to a decision.

## Put the preventative contract in executable code

The following Go program is a runnable reference harness for the polling contract. It models the state a JavaScript flag store should preserve: fallback is available immediately, a validated remote document becomes the cached document, and a rejected candidate cannot erase the last known valid state. The endpoint is pseudonymous on purpose; the contract matters more than a particular control plane.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Config struct {
	Version string          `json:"version"`
	Flags   map[string]bool `json:"flags"`
}

type Snapshot struct {
	Config Config
	Source string
}

type Poller struct {
	client   *http.Client
	endpoint string
	fallback Config
	cached   *Config
}

func (p *Poller) Current() Snapshot {
	if p.cached != nil {
		return Snapshot{Config: *p.cached, Source: "cached"}
	}
	return Snapshot{Config: p.fallback, Source: "fallback"}
}

func (p *Poller) Refresh(ctx context.Context) (Snapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.endpoint, nil)
	if err != nil {
		return p.Current(), err
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return p.Current(), fmt.Errorf("fetch config: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return p.Current(), fmt.Errorf("fetch config: status %d", resp.StatusCode)
	}

	var candidate Config
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&candidate); err != nil {
		return p.Current(), fmt.Errorf("decode config: %w", err)
	}
	if candidate.Version == "" {
		return p.Current(), errors.New("validate config: empty version")
	}
	if _, ok := candidate.Flags["delivery_status_panel"]; !ok {
		return p.Current(), errors.New("validate config: missing required flag")
	}

	p.cached = &candidate
	return Snapshot{Config: candidate, Source: "remote"}, nil
}

func main() {
	poller := Poller{
		client:   &http.Client{Timeout: 2 * time.Second},
		endpoint: "https://config.example/frontend-config",
		fallback: Config{
			Version: "compiled",
			Flags: map[string]bool{
				"delivery_status_panel": false,
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	snapshot, err := poller.Refresh(ctx)
	if err != nil {
		fmt.Printf("source=%s refresh_error=%q\n", snapshot.Source, err)
		return
	}
	fmt.Printf("source=%s version=%s enabled=%t\n",
		snapshot.Source,
		snapshot.Config.Version,
		snapshot.Config.Flags["delivery_status_panel"],
	)
}
```

The browser version needs two additions around this same state machine: a randomized delay around the normal poll interval and cancellation through the browser's request-abort mechanism when its owner unmounts.

Instrumentation should sit at the transitions. Increment one poll counter after classifying the outcome, increment one evaluation counter when the application actually reads a flag, and observe config age from the accepted document's timestamp if the contract supplies one. Do not increment “evaluation” merely because a document was fetched. Fetches and decisions answer different questions, and merging them produces a reassuring graph that cannot explain behavior.

## Prove the page with a failure-injection drill

Run the poller through startup without network access, a valid response followed by a timeout, malformed JSON, a missing required flag, component teardown, and a later valid version. The important assertion after the timeout is `cached`, not `fallback`.

Test the page separately.

A useful drill begins with healthy notification delivery and a forced poll transport failure; the expected result is diagnostic telemetry and no page. Next, allow one valid config to be accepted, force another poll failure, and verify that evaluations report `cached` while delivery remains healthy. Then inject failed payment-confirmation delivery without touching the poller; the delivery objective should page, while the flag evidence stays quiet and prevents a false causal story. Only in the final case do the delivery failures and fallback evaluations rise in the same window. The page should still describe impact first and correlation second. This sequence checks four distinct claims — fallback availability, cache retention, alert sensitivity, and alert specificity — without relying on a dashboard screenshot or asking a responder to infer which transition occurred. Run it before deployment with a fake clock and stub transport, then repeat the alert-routing portion in an environment where paging destinations are safe to exercise.

## Choose signal quality over noise, with limits

This approach is suitable when flags tune optional frontend behavior and the client can operate safely from a compiled or previously validated snapshot. The catch is that polling creates an interval in which clients can hold older decisions; the exact interval is a product and risk choice, not an observability choice. Your mileage may vary with browser suspension and mobile network behavior, so determine the interval with controlled tests against the actual client population rather than copying a number from another service.

It is not suitable when a decision must be authoritative at transaction time. Keep payment authorization, sanctions checks, and other security-sensitive enforcement on the server, where the request can be evaluated against current policy. Nor should a frontend fallback be used to conceal a broken core workflow. If disabling the flagged path prevents a required payment confirmation from being produced, the feature boundary is wrong; move the required behavior outside the flag before tuning alerts.

There is also a team cost. Someone must own the flag schema, the safe default, the removal date, the notification-class vocabulary, and the alert response. A flag with no owner tends to become permanent configuration; a metric label with no owner tends to acquire IDs. Review both in the same change, because the code path and its evidence fail together during an incident.

The final decision rule is narrow: page on material delivery failure, enrich the page with bounded flag-source and poll-outcome signals, and preserve the last validated config while the frontend retries. Dashboards can help explore after that. They do not get to define the incident.

## References

- https://prometheus.io/docs/practices/instrumentation/
- https://logback.qos.ch/manual/appenders.html
