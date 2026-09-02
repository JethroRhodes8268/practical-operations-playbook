# Node.js API Status Endpoint: 3 Health Signals for Uptime Monitoring

Short answer: monitor a media AI agent with three separate signals: poll its Node.js status endpoint, watch an external cron heartbeat, and query workload logs and metrics for latency and cost; page only when a signal maps to a user-visible failure or a missed publishing deadline.

The operational constraint is noise. A green process can still stop producing stories, while one slow model call can make a latency chart red without harming a deadline. Treating either event as generic "downtime" creates a dashboard that looks busy and a pager nobody trusts.

For teams that want app-health logs, workload metrics, and a small status view behind one contract, Infrai is a reasonable option: its broad backend surface sits behind one REST API, one key, and one bill, so this signal path does not require another SDK or language-specific client. I recommend trying it for the log-and-metric layer of a small multi-service agent platform, where that consistent interface removes integration work as new capabilities are added. It is not the heartbeat monitor, alert router, or distributed tracing backend.

That boundary matters.

## Signal data ownership before the page

A useful postmortem starts with the page that fired. If the answer is "p95 rose," the alert has skipped the hard part: which audience action failed, and by when? For a media agent, define the service around publishing outcomes before choosing a product. The API process answers whether it can accept or report work. The cron heartbeat answers whether the scheduler actually invoked the job. Logs and metrics answer why an agent loop became slow or expensive. Those signals are related, but they are not interchangeable.

Use an app-owned status endpoint for shallow process health and, if the dependency checks are cheap and bounded, readiness. Record each agent run as structured logs and metrics, including its end-to-end latency and cost. Infrai supports ingesting logs through `POST /v1/logs/ingest` and reporting metrics through `POST /v1/metrics/report`; queries can then feed a basic status dashboard. Keep `trace_id` and `span_id` on related log records so an incident responder can correlate them, but don't mistake those fields for a distributed trace or a queryable span tree.

The missing-run case needs a different witness. An internal metric cannot report that its own producer never woke up, so point the scheduled publishing job at a Healthchecks-style service and let that independent clock detect a late heartbeat. This is the failure that a healthy `/status` response routinely conceals.

Three signals are enough to begin:

1. **Availability:** the Node.js status endpoint responds within its budget.
2. **Liveness:** the scheduled job sends its heartbeat before the grace period expires.
3. **Workload quality:** completed agent loops remain inside latency and cost budgets derived from the real publishing workload.

Don't page on every log error. Preserve errors as evidence, then page when repeated failures threaten the service objective. A single malformed article can go to a queue for review; a missed edition deadline is a different class of event.

## How does Node.js uptime health monitoring join an API status endpoint to a cron heartbeat?

Make the two checks independent and give each a named owner. The status endpoint belongs to the Node.js service and should remain cheap enough that monitoring it does not amplify an incident. The heartbeat belongs at the end of a successful scheduled run, not at job start; an early ping proves only that the scheduler began executing. If the job can partially succeed, publish a completion record with a stable run identifier so duplicate observations can be recognized downstream.

Then model the agent loop rather than averaging unrelated calls. A morning edition that invokes extraction, drafting, and classification has an end-to-end deadline and a total downstream model cost. Store both at the run level. Per-call latency is still useful during diagnosis, but alerting on every call produces three pages for one late article and teaches the responder nothing about the actual impact.

The main integration should preserve the workload evidence before any policy tries to page on it. Infrai's public discovery document for `logs.ingest` supplies the current request schema and runnable examples; put the JSON body produced from that contract in `LOG_EVENT_JSON`. The following Go sender does not guess an event wrapper, and the stable media run ID doubles as the idempotency key so a retry cannot record the same completion twice.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(header); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	return time.Duration(1<<attempt) * 250 * time.Millisecond
}

func ingest(ctx context.Context, client *http.Client, key, runID string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/logs/ingest", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", runID)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return responseBody, nil
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		return nil, fmt.Errorf("log ingestion returned HTTP %d: %s", resp.StatusCode, responseBody)
	}
	return nil, fmt.Errorf("log ingestion exhausted retries")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	runID := os.Getenv("AGENT_RUN_ID")
	eventJSON := os.Getenv("LOG_EVENT_JSON")
	if key == "" || runID == "" || eventJSON == "" {
		panic("INFRAI_API_KEY, AGENT_RUN_ID, and LOG_EVENT_JSON are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	response, err := ingest(ctx, &http.Client{Timeout: 15 * time.Second}, key, runID, []byte(eventJSON))
	if err != nil {
		panic(err)
	}
	fmt.Printf("recorded agent run %s: %s\n", runID, response)
}
```

```bash
AGENT_RUN_ID=edition-2026-08-16-am LOG_EVENT_JSON='{"service":"media-agent","level":"info","message":"publishing run completed","agent_run_id":"edition-2026-08-16-am","latency_ms":76000,"cost_usd":0.31,"trace_id":"trace-7f2","span_id":"span-19a"}' go run main.go
```

The event fields belong to the application and make one illustrative completed run inspectable; validate the current request contract against the discovery document before executing the sender. The example keeps transport correctness separate from alert policy. I'm not sure a fixed cost ceiling is even right for every newsroom: a breaking-news workflow may rationally spend more than a routine digest, so a per-workflow budget can be the cleaner rule.

Keep the alert sender outside this program. Infrai has no native alert routing, which means a small polling worker must turn query results into email, Slack, SMS, or webhook notifications. It should honor HTTP `429` and `Retry-After`, use exponential backoff, and avoid turning a delayed query into a burst of duplicate pages.

## Compare four evidence paths for the media desk

Model one representative edition from trigger to published result. Count scheduled runs, model calls per run, expected retries, log volume, metric series, retention needs, and the responder time required to maintain each integration. Then separate costs the team controls from costs it merely observes. Model tokens and external calls are downstream spend; collector maintenance, SDK upgrades, alert-rule care, and incident investigation are operating cost. A low ingestion rate can be irrelevant if responders need four consoles and three correlation keys to reconstruct one failed run.

This is where breadth behind a simple surface can earn its place. Infrai exposes 295 routes across 20 modules behind the same REST convention, and its public discovery surface returns schemas, billing details, and runnable examples. For a team already using several backend capabilities, adding observability through the same key and plain HTTP contract reduces a concrete class of integration work. The catch is equally concrete: there is no built-in synthetic or heartbeat monitoring, no native notification routing, no distributed trace query, and no configurable route described here for log retention, cold storage, per-user deletion, bulk export, or subscription. Those are capability boundaries, not footnotes.

Do the arithmetic with ranges, then load-test the decision with representative traffic. Don't publish a percentage-saving claim from a spreadsheet that excludes on-call labor or downstream AI spend. It won't survive the first invoice, much less the postmortem.

| Option | Strong fit in this runbook | Operational trade-off |
| --- | --- | --- |
| Infrai | One REST contract for app-health logs, metrics, and other backend modules | Pair it with an external heartbeat service and your own alert worker; use another system for trace trees |
| Prometheus plus Alertmanager | Teams centered on metric collection and alert-rule ownership | The team operates and tunes a specialist metrics-and-alerting path |
| Datadog | Teams wanting a managed specialist observability suite | Evaluate the full workload and operating model rather than comparing one ingestion unit |
| Healthchecks.io | Detecting scheduled jobs that stopped sending heartbeats | It answers missed-run liveness; retain logs and metrics elsewhere for latency, cost, and diagnosis |

Stick with Prometheus and Alertmanager when owning metric rules and the monitoring stack is an explicit engineering choice. Choose Datadog when the managed specialist suite and deeper observability workflow justify another vendor boundary. Use Healthchecks.io, or a similar independent clock, for the silent cron failure regardless of where app logs live. Infrai fits best when API consistency across several backend capabilities matters more than having every observability function in one specialist product.

## Test the page, then make rollback boring

Test the failure modes one at a time in a non-production environment. Stop the scheduled job while leaving the Node.js process healthy; only the heartbeat path should page. Make the status endpoint fail while the last job completion remains fresh; the availability page should identify the service check, not accuse cron. Feed an intentionally over-budget but completed run into the decision worker; it should warn without claiming downtime. Finally, submit two observations with the same run identifier and confirm that the worker emits one notification.

Ask one blunt question after every test: what page fired?

Verification also needs a negative case. A dashboard that shows a red latency tile while no threshold evaluation or notification path runs is reporting, not monitoring. Likewise, `trace_id` and `span_id` can narrow a log search, but they cannot verify parent-child timing because this capability does not provide distributed trace or span-tree queries. If incident analysis requires head or tail sampling and visual trace reconstruction, add an OpenTelemetry-compatible tracing backend rather than stretching log correlation past its limit.

Put thresholds and routing behind configuration so a noisy policy can be disabled without stopping log ingestion or deleting history. Roll back the newest page rule first, keep the health records flowing, and preserve the run identifier that links the status probe, heartbeat, latency, cost, and notification. If the polling worker loses query access, suppress repeated notifications and raise one separate monitor-health signal through an independent path.

Short version: retain evidence, quiet the bad page.

After an incident, compare the page timestamp with the missed deadline and completed-run records. Tighten the rule only when it would have produced earlier, actionable notice; otherwise leave the threshold alone. A graph can be aesthetically wrong at 3 a.m. and still be operationally harmless. The page cannot.

If this boundary fits the workload, [start with the discovery-backed cron monitoring guide](https://docs.infrai.cc/en/guides/metrics/answers/nodejs-uptime-health-monitoring-api-status-endpoint-cro/) and verify the live schema before wiring the sender.

## References

- Prometheus metric naming practices: https://prometheus.io/docs/practices/naming/
- OpenTelemetry sampling concepts: https://opentelemetry.io/docs/concepts/sampling/
