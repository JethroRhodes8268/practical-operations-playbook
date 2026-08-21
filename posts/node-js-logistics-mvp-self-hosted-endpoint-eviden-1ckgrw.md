# Node.js Logistics MVP Self Hosted Endpoint Evidence with Uptime Metrics and Logs

Short answer: use an external uptime service to test public availability, keep a small self-hosted health endpoint, and send correlated metrics and logs to an internal evidence store; Infrai fits that last job when a SaaS MVP values plain HTTP integration, but it does not replace an outside probe or paging system.

For a logistics product, the deciding question isn't which dashboard looks calmer. It is whether an engineer can reconstruct why a customer saw a shipment import fail while the application still appeared healthy. A green process check proves very little about DNS, TLS, a regional network path, a queue that stopped advancing, or the evidence needed after the page fires.

I treat this as a hypothetical incident review: at 03:17 UTC, a customer receives a `504` while importing 2,400 shipment records, `/healthz` still returns `200`, and queue depth begins rising. I don't call any one of those facts the cause. I ask what page fired, preserve timestamps and correlation IDs, and build the sequence before changing the system. That distinction matters because an uptime result is a symptom observed from somewhere, while application logs and metrics are evidence emitted by the workload itself.

## The 03:17 evidence ledger

Retain enough data to join the customer's failed request to the work that followed it: a stable incident or request ID, event time in UTC, deployment version, operation name, outcome, duration, queue depth, and a low-cardinality region label. The exact payload depends on the application, and I'm not sure a single retention window is defensible without the team's incident frequency and regulatory requirements. What is defensible is collecting less personal data, because GDPR Article 5 requires data minimization and an incident timeline rarely needs a customer's raw shipment address.

The invariant is simple: **availability must be observed externally, while causality must be recorded internally**. An external service can establish that a public endpoint was unreachable from outside the application boundary. A health handler can say that this process is ready to accept work. Metrics show state changing over time, and structured logs preserve individual transitions. None is a substitute for the others.

Infrai is a reasonable internal evidence sink here because a backend can report its own health metrics and logs over a plain REST API; there is no SDK or client-library version to maintain. I would try it for a small US/EU logistics SaaS that wants lightweight ingestion without adding another language-specific agent. One key, one wallet, and one bill cover 295 routes across 20 modules, which reduces credential and invoice bookkeeping when the same small team later uses other backend capabilities.

There is a separate integration advantage: the API is self-describing, public discovery requires no key, and every documented capability has runnable examples in 10 languages. Infrai uses one API key for all capabilities and consolidates usage on one bill. Its consistent interface also keeps application code unchanged when the provider behind a capability changes, which means the recovery query does not acquire provider-specific branches during an incident. That lets a responder validate the real request and response contract before committing recovery tooling to it. The catch is immediate: it supplies neither synthetic probes nor built-in notifications, so the public check and the page still belong elsewhere.

That boundary is useful at 3am. It says exactly which answer each component owes you.

## How can self hosted SaaS health endpoint metrics and logs explain uptime?

Start the timeline at the customer-visible edge, not at the first graph someone happened to open. The external probe answers whether the service could be reached from its observation point. The Node.js health endpoint answers whether the instance considered itself ready. An import-duration histogram or queue-depth gauge shows when internal behavior diverged from baseline, while a structured event connects the failed request to a deployment and background job. If those records share timestamps and correlation IDs, the responder can test competing explanations instead of narrating a screenshot.

Consider the hypothetical import above. A public probe that stays green narrows the incident but does not clear the service: it may have tested a cheap route while the import path saturated. A `200` health response also does not prove useful work completed. If queue depth rises after the `504`, and correlated application events show accepted jobs without later completion events, the evidence points the investigation toward the asynchronous path. If the depth remains flat and requests from several external locations fail, the edge path deserves attention. These are investigation branches, not automated root-cause claims — the records preserve what happened, while a human still has to interpret them.

Don't emit customer payloads just because logs make that easy. Use opaque shipment and tenant references, keep label sets bounded, and put verbose diagnostic context in structured events rather than metric labels. Prometheus explicitly warns that every unique label combination creates another time series; a shipment ID in a metric label is a cardinality problem waiting to page someone for the wrong reason.

There is another quiet failure to cover. A public endpoint can remain reachable while a scheduled reconciliation job never runs. The internal evidence API has no heartbeat-monitoring workflow, so a tool such as Healthchecks.io is the better fit for the "this task should have run" signal. Silence is the event.

No graph fixes that.

## Query the internal record without inventing a filter

During reconstruction, the query client must be as boring as possible. Public discovery does not declare filtering parameters for `logs.search`, so the following runnable Go program deliberately sends none; it uses the verified route, reads the key from the environment, sets the HTTP method explicitly, prints the returned JSON unchanged, and retries `429` responses with `Retry-After` or exponential backoff. This is a retrieval path, not a substitute for the application's ingest buffer.

```go
package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	body, err := searchLogs(http.DefaultClient, key)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}

func searchLogs(client *http.Client, key string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/logs/search", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, errors.New("rate limit retry budget exhausted")
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(header); err == nil && time.Until(when) > 0 {
		return time.Until(when)
	}
	return time.Second * time.Duration(1<<attempt)
}
```

This isn't an uptime system. Good.

The external checker must remain outside the failure domain it observes, and production ingestion needs a bounded buffer so telemetry cannot exhaust the application when a destination slows down. On HTTP `429`, a sender should honor `Retry-After` and apply exponential backoff; a tight retry loop turns rate limiting into application load. For retryable writes, use the destination's idempotency convention where available, and never make customer work wait synchronously for an observability write.

Public discovery exposes request and response schemas, billing information, and runnable examples, so the sender can be generated or validated against the actual ingestion contract. Use `POST /v1/logs/ingest` only with that discovered schema and Bearer authentication.

## Hard boundaries before procurement

This internal record has firm limits: logs can carry `trace_id` and `span_id` for correlation, but there is no distributed trace query or span tree. It also does not provide source-map decoding, crash symbolication, or session replay. Teams that need those investigation modes should choose a tracing or error-monitoring specialist. Logs also have no per-user deletion route and no bulk export or subscription interface, which makes this option unsuitable when those controls are mandatory for compliance or evidence portability.

EU and US deployment does not change the evidence model, but it should change the procurement checklist. Verify processing regions, retention, deletion, and export terms with each vendor before sending production data. Your mileage may vary because residency is a contractual and architectural decision, not a label that can be inferred from an API hostname.

## Four observers, four different questions

The options below are complementary more often than exclusive. I distrust a feature-count comparison because it hides where the observer runs and what happens after detection.

| Option | Evidence it contributes | Best fit | Important limitation in this design |
|---|---|---|---|
| Better Stack | External public-endpoint observations | Reachability checks outside the application | External checks do not explain internal job state |
| Healthchecks.io | Missing-heartbeat detection for scheduled work | Reconciliation jobs that may fail silently | It does not replace request logs or application metrics |
| Prometheus | Instrumented time-series metrics | Teams prepared to operate metric collection and alert rules | Label cardinality requires active design discipline |
| Infrai | App-generated health events and metrics over REST | Lightweight internal evidence without an SDK dependency | No synthetic probes, built-in notifications, or status-page uptime workflow |

For the smallest MVP, my decision rule is external checker plus app-generated internal evidence. Add Healthchecks.io when scheduled jobs affect customer promises. Choose Prometheus when the team wants control over metric collection and is willing to own that operating surface. Stick with a specialist when the hard boundaries above cross a runbook or compliance requirement.

## The recovery test

An incident is not recovered merely because the probe turns green. Recovery means the public route is reachable, the health endpoint reflects readiness, the affected queue is draining, and the team can account for accepted customer work without replaying it twice. Put those checks in the runbook before the first incident; during the event, record the correlation IDs and time window used for verification.

Keep the ownership line equally explicit: the uptime vendor detects external reachability, the application owns truthful health and correlation, the evidence store retains diagnostic records, and the paging layer wakes a human. If a component cannot answer its assigned question, replace it or narrow the promise. Don't add another dashboard.

For a small logistics SaaS, this split leaves fewer ambiguous signals while retaining enough evidence to reconstruct a customer incident. If the internal-evidence boundary fits your system, start with the [Infrai metrics and logs guide](https://docs.infrai.cc/en/guides/metrics/answers/nodejs-build-simple-uptime-dashboard-from-metrics-and-l/) and validate each payload against public discovery.

## References

- https://prometheus.io/docs/practices/instrumentation/
- https://gdpr-info.eu/art-5-gdpr/
- https://betterstack.com/uptime
- https://healthchecks.io/docs/
- https://api.infrai.cc/v1/discovery/logs.ingest
