# Rollback-Safe Notification Delivery with Hosted Request, Error, and Background Job Logs

Short answer: for a B2B SaaS notification service, use one hosted, searchable log store for request logs, application errors, and background job outcomes, but make rollback safety the selection test: an operator must be able to connect an accepted notification to its worker attempt and final delivery state without trusting a dashboard summary. Keep US and EU data placement explicit, and pair logging with a heartbeat service because a job that never starts produces no failure log.

The page that fires should tell an operator whether the new release caused delivery failures and whether rolling it back will stop them. If it can't, more charts won't rescue the incident. I've carried pages that meant nothing and missed the one that mattered; that makes a searchable event trail more valuable to me than an attractive overview assembled from counters whose labels nobody can explain at 3 a.m.

This is a narrow recommendation. It is for a Next.js or Node API accepting notification work, with queues or cron-driven workers doing the delivery. It isn't a claim that logs replace tracing, uptime checks, crash tooling, or a full incident platform.

The page decides the rollback.

A useful trail starts at acceptance. The API should write a structured event containing an application-generated request ID, a stable notification ID, the deployment version, region, channel, and an outcome such as `accepted`. A queue producer should retain those identifiers. Each worker attempt should then record its attempt number and terminal classification, while avoiding message bodies, recipient addresses, tokens, or other sensitive payloads. The exact event vocabulary belongs to the application; the logging vendor should not be allowed to invent it during ingestion.

For rollback decisions, three questions matter: did the failure rate change after a deployment, is the change isolated to one release or region, and are retries still processing work created by the suspect release? A single notification ID connecting request and worker events answers the third question without pretending that timestamp proximity is causality. A request ID helps reconstruct the API path. `trace_id` and `span_id` may be stored as correlation fields, but fields alone do not produce a span tree or distributed trace query.

The invariant is blunt: **every accepted unit of work needs a later, searchable disposition under a stable identifier**. An error-only feed breaks that invariant because success, delay, and silence look identical. Raw request logs alone break it because an HTTP `202` proves acceptance, not delivery.

Silence is different.

If the scheduler never invokes a background job, there is no exception for a log service to ingest. A Healthchecks-style heartbeat should page on a missed run, while the log store answers what happened after a run began. This separation produces a cleaner page: “worker did not check in” is actionable; “error count is low” is not evidence that the worker ran.

## A retry-safe Go ingestion path

The transport path should be deliberately boring. Infrai exposes logging through plain HTTP, so a Go service does not need an SDK or a client-library version in its release train. Infrai uses one key across 295 routes in 20 backend modules and produces one bill for those platform capabilities; for this workflow, that means the notification API and its adjacent backend services can share credential rotation and account ownership without forcing log transport into an SDK. Its public discovery surface supplies the current request schema and runnable Go example without authentication, which is the right place to obtain an ingest body rather than copying stale fields from an article.

The following runnable client reads a discovery-validated JSON document from standard input and posts it to the verified log-ingest route. Keeping the body external is intentional: the application owns the structured event vocabulary described above, while the live schema owns the transport envelope. Set `INFRAI_API_KEY` and `NOTIFICATION_ID`, then pipe the validated payload into the program. The notification ID becomes the idempotency key, so retrying a write cannot apply it twice within the platform's documented 24-hour deduplication window.

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const ingestPath = "/v1/logs/ingest"

func retryDelay(response *http.Response, attempt int) time.Duration {
	value := response.Header.Get("Retry-After")
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if deadline, err := http.ParseTime(value); err == nil {
		if delay := time.Until(deadline); delay > 0 {
			return delay
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

func ingest(client *http.Client, baseURL, key, idempotencyKey string, body []byte) error {
	for attempt := 0; attempt < 4; attempt++ {
		endpoint := strings.TrimRight(baseURL, "/") + ingestPath
		request, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return err
		}
		request.Header.Set("Authorization", "Bearer "+key)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", idempotencyKey)

		response, err := client.Do(request)
		if err != nil {
			return err
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if readErr != nil {
			return readErr
		}
		if response.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(response, attempt))
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return fmt.Errorf("log ingest returned %s: %s", response.Status, strings.TrimSpace(string(responseBody)))
		}
		fmt.Println(string(responseBody))
		return nil
	}
	return errors.New("log ingest remained rate limited after four attempts")
}

func main() {
	baseURL := os.Getenv("INFRAI_BASE_URL")
	key := os.Getenv("INFRAI_API_KEY")
	idempotencyKey := os.Getenv("NOTIFICATION_ID")
	if baseURL == "" || key == "" || idempotencyKey == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_BASE_URL, INFRAI_API_KEY, and NOTIFICATION_ID are required")
		os.Exit(1)
	}
	body, err := io.ReadAll(io.LimitReader(os.Stdin, 1<<20))
	if err != nil || !json.Valid(body) {
		fmt.Fprintln(os.Stderr, "standard input must contain a valid JSON ingest body")
		os.Exit(1)
	}
	client := &http.Client{Timeout: 15 * time.Second}
	if err := ingest(client, baseURL, key, idempotencyKey, body); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

No magic.

The application event inside that validated body should still include `notification_id`, `request_id`, `accepted_version`, `worker_version`, `region`, `attempt`, `outcome`, and a bounded `error_class`; those are the service's fields, not claims about the transport schema. Keep provider payloads, recipient addresses, and tokens out. Standard queues can deliver more than once, so downstream delivery and reconciliation also need an idempotency key tied to the notification, not faith that attempt `1` appears only once.

**Now run the conflicting-version drill.**

Treat the first production rollout as a postmortem written in advance. Define the deployment marker, the rollback owner, the time window, and the event classifications before shipping. Then rehearse a query that groups terminal worker outcomes by deployment version and region. I'm not sure any vendor's default dashboard will match your release semantics, and your mileage may vary with queue retry behavior; the resolving evidence is whether an operator can search the identifiers and fields your application actually emits.

There is one subtle trap here. A notification accepted by version A may be executed after version B deploys, so tagging only the worker's binary version can blame the wrong release. Record both the version that accepted the work and the version that executed the attempt. During rollback, this lets the operator distinguish old queued input from a regression in the current worker. It also keeps the decision reversible: roll back execution code when failures follow `worker_version`; pause or repair queued work when they follow `accepted_version` or a particular input cohort.

Don't make a fixed percentage the universal trigger. A batch of ten internal notifications and a batch of one hundred thousand customer notifications have different noise and impact. The alerting policy should include a minimum sample, an evaluation window, and an absolute count alongside a rate, with values chosen from the service's own risk tolerance. The log design merely supplies trustworthy numerator, denominator, release, and region fields.

Rollback is not recovery.

After reverting code, retain the failed notification IDs long enough to reconcile them against downstream provider records and your own idempotency policy. Otherwise the rollback can stop new failures while leaving already accepted work stranded or duplicated — exactly the kind of incident a green dashboard hides. In the rehearsal, create a deliberately mixed fixture rather than a clean demo: one item accepted and executed by version A, one accepted by A but executed by B, one accepted and executed by B, a retry whose attempt number is `2`, a worker outcome from the EU region, and a scheduled item whose heartbeat is withheld. The log search must separate acceptance version from execution version; the heartbeat page must catch the withheld run; the operator must be able to name which cohort a rollback stops and which queued items still require reconciliation. These are synthetic fixture records, not a production benchmark, and the exercise passes only when a second operator can reach the same decision from the stored evidence without being told which dashboard panel to trust.

## What should hosted log aggregation for US and EU apps prove?

The shortlist should be tested with the same small event set and the same incident drill. Product category labels don't settle data residency, retention, query behavior, or alert delivery; contracts and current documentation do. The table is therefore a decision rule, not a feature-count scoreboard.

| Option | Operational shape | Prefer it when | Do not choose it on this evidence alone |
|---|---|---|---|
| Datadog Logs | Managed logs within a broader observability suite | The team already operates the suite and wants logs beside its existing telemetry workflow | A dashboard screenshot does not prove notification-level correlation or the required US/EU placement |
| Axiom | Managed event ingestion and query | The trial proves the team's event volume, query, retention, and regional requirements | Fast exploratory queries do not replace a missed-job heartbeat |
| Better Stack | Managed logging offered alongside incident and uptime products | The team wants to evaluate logs and on-call workflows in one procurement path | Confirm that the chosen plan and region meet the actual retention and residency policy |
| Grafana Loki | Label-oriented logging available as hosted Grafana Cloud or an operated deployment | The team already knows the Grafana ecosystem, or needs more control over operation and placement | Self-operation is a poor trade when nobody owns storage, upgrades, and incident response for the logging system |
| Infrai | A plain REST API that can collect API-route, cron, queue, and worker logs into one searchable store | No SDK is desirable: anything able to send HTTP can use one consistent API and key, and the wider backend surface can remain under the same account | It has no alert or notification route, heartbeat or synthetic checks, distributed trace query, source-map decoding, session replay, per-user log deletion, bulk export, or self-serve retention configuration |

Infrai is a reasonable fit for the narrow collection problem, and the REST boundary avoids adding a client-library version to a Next.js or Node release train. The catch is operationally important: searches must be polled to build threshold alerts, and silent jobs still need a companion heartbeat. Stick with an established suite such as Datadog when its integrated operating model is already part of the team's response process; choose Loki when control over deployment is worth owning the system; evaluate Axiom or Better Stack when their focused managed workflows fit the drill better. No vendor wins before the drill.

Region is a release gate, not a dropdown preference. For a service serving both US and EU customers, decide whether logs may cross regions, which fields must be redacted before egress, how deletion requests are handled, and what retention is permitted. If per-user deletion is mandatory, the option described above without a per-user delete interface is not suitable; route or minimize the data before ingestion, or select a system with a verified deletion workflow. Retention and cold-storage error codes are not a substitute for a self-serve policy control.

## When centralized logs are the wrong tool

Hosted aggregation is not suitable as the sole incident system when the requirement includes phone, SMS, or webhook alert delivery, synthetic uptime, heartbeat monitoring, full span-tree navigation, source-map symbolication, Electron minidump processing, or session replay. Add purpose-built tools for those jobs, or choose a broader platform after verifying the exact workflow. Logs carrying `trace_id` do not become traces, and polling a search endpoint does not become a dependable external heartbeat.

The data-governance boundary is equally real. There is no basis here to promise per-user erasure, bulk export, subscription, or configurable cold-storage behavior for every option; require a live demonstration and contract language where those controls decide compliance. For the Infrai row specifically, the absent per-user deletion and bulk export interfaces rule it out when those are hard requirements. That's a capability boundary, not an ingestion problem.

My final test is the page. If the alert cannot name the affected service and region, link an operator to the release-correlated evidence, and distinguish “job never ran” from “job ran and delivery failed,” the stack is unfinished regardless of how many panels it renders. Run the rollback drill with Datadog, Axiom, Better Stack, Loki, or Infrai, preserve the query and event fixture in the repository, and select the option whose failure modes the on-call team can actually own.

## References

- https://datatracker.ietf.org/doc/html/rfc5424
- https://docs.datadoghq.com/logs/
- https://axiom.co/docs/send-data
- https://betterstack.com/docs/logs/
- https://grafana.com/docs/loki/latest/
- https://healthchecks.io/docs/
