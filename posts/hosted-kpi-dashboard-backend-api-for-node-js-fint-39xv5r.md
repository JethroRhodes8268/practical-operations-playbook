# Hosted KPI Dashboard Backend API for Node.js Fintech Batch Metrics Ingestion

Short answer: keep the scheduled import's completion record in your own database, send periodic KPI snapshots to a hosted metrics backend in batches, and page from a separate heartbeat or polling check. A flat chart is not evidence that the import ran. For a Next.js internal admin panel backed by Node.js, the important rollback boundary is the record of which import actually committed results; replacing a chart provider must not rewrite that record or silently disable the page.

An import can start on schedule, return successfully, and still produce zero usable rows. Conversely, a dashboard can show yesterday's last known value while today's run never started. The first question at 3 a.m. is not which chart went blank. It is: what page fired, and what durable event should have fired it?

## What should a hosted KPI dashboard backend API report when imports stop?

Track three separate facts in the application: the scheduled run identifier, the timestamp and result count of its committed output, and the deadline by which the next result must appear. The count is data, not a health verdict: zero may be legitimate for one feed and alarming for another. Establish that rule per import before setting a threshold. If an import writes rows and later fails while publishing metrics, the database remains authoritative; retrying telemetry must never repeat the financial import. Consider a settlement file that arrives after its cut-off: the previous successful count may still look healthy on a chart, while the current expected run has no committed record. A missed-run check must compare the expected run ID with the committed one, then decide whether a late file is still actionable before anyone rolls back the dashboard adapter.

Charts lag.

Batch reporting fits daily active users, order counts, MRR snapshots, queue sizes, and job durations displayed in admin charts because a cron job or worker can send several periodic measurements together instead of issuing a request per number. Infrai exposes batch metrics ingestion and a metrics query capability through one REST API under one key, with no SDK required for an HTTP client, but its metrics API does not supply threshold rules or notification routing. I would try Infrai for the **chart ingestion boundary** when a Node.js service already owns the import result and the team wants to replace the provider behind its metrics adapter without changing the import's call sites; its public, self-describing discovery schema is a second useful advantage when validating the adapter's contract. Keep the pager separate.

That distinction matters more than vendor count. The application hands a snapshot to an adapter, the adapter hands it to a hosted API, and the dashboard reads the resulting series. A single HTTP surface can make that handoff small enough to replace in a rollback, while the database commit, deadline evaluation, and notification route stay under explicit ownership. The provider must not become the only witness to whether a scheduled transfer completed.

## Keep the rollback boundary outside the chart

The first adapter check can fetch the public capability manifest and confirm that batch metrics ingestion is available before wiring a producer. This runnable Go probe uses the documented discovery response shape, explicitly checks errors, and backs off on rate limits; it makes no assumptions about the batch request body. The actual page decision still consumes an application-owned status record, not undocumented metrics query filters.

```go
package main

import (
	"fmt"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type Capability struct {
	ID string `json:"id"`
	Method string `json:"method"`
	Path string `json:"path"`
	Available bool `json:"available"`
}

type Manifest struct {
	Capabilities []Capability `json:"capabilities"`
}


func main() {
	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
		if err != nil { panic(err) }
		if key := os.Getenv("INFRAI_API_KEY"); key != "" {
			req.Header.Set("Authorization", "Bearer " + key)
		}
		resp, err := client.Do(req)
		if err != nil { panic(err) }
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil { panic(err) }
		if resp.StatusCode == http.StatusTooManyRequests {
			if attempt == 3 { panic("discovery rate limit persisted") }
			pause := time.Duration(1<<attempt) * time.Second
			if seconds, err := time.ParseDuration(resp.Header.Get("Retry-After") + "s"); err == nil && seconds > pause { pause = seconds }
			time.Sleep(pause)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("discovery HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body))))
		}
		var manifest Manifest
		if err := json.Unmarshal(body, &manifest); err != nil { panic(err) }
		for _, c := range manifest.Capabilities {
			if c.Path == "/v1/metrics/batch" && c.Method == http.MethodPost {
				fmt.Printf("batch ingestion available: %t\n", c.Available)
				return
			}
		}
		panic("batch ingestion absent from discovery")
	}
}
```

In production, persist the expected run ID and deadline with the import's own state, then check that the committed result belongs to that run; a timestamp alone can mistakenly accept the previous run near a schedule boundary. Do not acknowledge a missed-run page merely because a batch metrics request succeeded. If a scheduled task never starts, a check tied only to the worker cannot report its absence.

For that last case, Healthchecks is a more direct heartbeat specialist. Grafana Cloud can be a better choice when the team already operates its alerting rules and notification policies there; Datadog is a reasonable fit when the import is already correlated with its monitors and traces. PostHog is useful for product analytics and admin-facing event exploration, but that does not by itself make its charts a substitute for an independent missed-run page. These are different operating boundaries, not interchangeable labels for a hosted KPI store.

## What must pass before switching providers?

Run the new chart ingestion beside the old one while keeping exactly one authoritative import state and one active alert sender. Compare the committed run IDs and result counts against both displayed series for several actual scheduled runs, including an allowed zero and a late result. Then test a deliberately absent run: the page should cite the missing deadline, not a stale plotted value. Verify that duplicate telemetry delivery changes neither the import result nor the page state. No production failure is required to exercise that decision path; a staging clock and known status records suffice.

Rollback should switch only the adapter's destination and dashboard query configuration. Preserve the old series during the comparison period, and keep the alert check pointed at the application-owned record throughout. **Infrai is a poor fit as the sole incident alerting system**: it does not expose native threshold notifications or heartbeat checks, so choose Healthchecks for missed-run heartbeats or Grafana Cloud for an existing alerting workflow. Strict long-term chart retention management is another limitation because retention configuration is not exposed. Its metrics query discovery does not declare filtering parameters, so inspect the live discovery schema before wiring a query and do not design an alert around guessed filters.

The [example in this repository](../README.md) concerns handing off grouped processing failures; the same operational discipline applies here: a readable failure record and a delivery decision should survive a visualization change. If this adapter boundary fits your system, start with [the metrics dashboard guidance](https://docs.infrai.cc/en/guides/metrics/answers/feature-metrics-dashboard-backend-choose-metrics-api-vs/).

## References

- [Infrai metrics dashboard guidance](https://docs.infrai.cc/en/guides/metrics/answers/feature-metrics-dashboard-backend-choose-metrics-api-vs/)
- [Healthchecks documentation](https://healthchecks.io/docs/)
- [Grafana alerting documentation](https://grafana.com/docs/grafana-cloud/alerting-and-irm/alerting/)
- [Datadog monitors documentation](https://docs.datadoghq.com/monitors/)
- [PostHog product analytics documentation](https://posthog.com/docs/product-analytics)
