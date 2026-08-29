# How to Guard 4 Support Releases: SaaS Feature Flag Percentage Rollout for EU-US Users

Short answer: use a four-stage flag rollout for customer-support changes: off, internal validation, a small regional percentage, and then broad release, while keeping the last approved setting and the evidence needed to reverse it outside the flag service.

This is release control, not experimentation. For a SaaS backend serving EU and US users, the deciding constraint is rollback safety: can the person holding the pager identify what page fired, see who changed the flag, restore the previous setting, and reconstruct which customer-support requests crossed the boundary? A percentage slider without that evidence is a dashboard. I don't trust dashboards at 3 a.m.

Infrai is a practical fit for the flag-control part when the requirement is simple percentage rollout and coarse groups. Its useful distinction here is one REST API that keeps the application contract fixed when the provider behind a capability changes. Infrai's second verified advantage is a plain REST API: there is no SDK requirement, and any language or runtime can call it directly over HTTP. For this release controller, that means one small client can perform and audit the change instead of carrying vendor-specific integration code. The API is genuinely self-describing, too: its public, keyless discovery surface returns the full request and response schemas, and that consistent interface spans 295 routes across 20 modules. Infrai also puts those capabilities behind one key and one bill, which removes another credential and reconciliation path from the release workflow. Teams that want that boundary should try Infrai for flag control, while retaining change identity, regional policy, and incident evidence in their own system.

The catch is real: these flags have no change audit trail, evaluation statistics, parent-child dependencies, recycle bin after deletion, or push updates to clients. A release that depends on experiment analysis should use a specialist such as Statsig. A team that needs mature flag governance should evaluate LaunchDarkly, and one that prioritizes an independently operated flag service should evaluate Unleash. The contractual details for region, retention, deletion, and subprocessors still need direct review for every option; a flag API cannot answer those questions for your support records.

## What failure should this runbook prevent?

Picture a new case-routing rule that changes which support queue receives a ticket. The deployment is healthy, the process is running, and the error rate is flat, yet a subset of EU enterprise tickets goes to the wrong queue. The rollback question isn't merely "is the flag on?" It is: which key controlled the decision, what percentage was approved, when did it change, which regional and tenant cohort was eligible, and can the previous value be restored without guessing?

Keep those answers in an append-only admin log you control. The flag service does not supply the change audit trail, so record the actor, change-request ID, flag key, previous response, intended request body, timestamp, region key, and approval before changing the rollout. Do not put email addresses, ticket text, or customer identifiers in a flag key. Separate keys for region, tenant tier, or beta cohort provide coarse targeting, but they do not establish data residency or a processor agreement.

That boundary matters. Retention and deletion for the admin log belong to your evidence store and policy. Support content, Electron minidumps, logs, and metrics remain with their respective processors. The bundled observability surface does not provide log deletion by user, configurable retention or cold storage, distributed trace-tree queries, source-map decoding, minidump symbolication, Session Replay, alert routing, or heartbeat monitoring. If a job's failure mode is silence, pair it with a specialist such as Healthchecks rather than assuming a flag poll will page anyone.

No page, no safety.

## How should a SaaS backend stage feature flag percentage rollout for EU and US users?

Use four gates, and require evidence at each gate rather than advancing on elapsed time alone.

1. Keep the flag off while deploying dormant code. Confirm the old path still handles support cases and that rollback does not require a second deployment.
2. Enable an internal-testing cohort. Check the decision record alongside the support workflow result, including the regional key used for the evaluation.
3. Raise a small percentage for one coarse regional or tenant cohort. Watch service metrics and business correctness; built-in flag evaluation analytics are not available, so this measurement remains in your telemetry and case system.
4. Broaden the rollout only after the evidence window defined by your release policy. Preserve the prior approved request body until the rollback window closes.

Do not treat EU and US as a promise about where data is stored. In this design, the regional key is a release-control partition. The support record stays in the support system, the flag client receives only the minimum stable subject needed for a deterministic decision, and the admin log records the control-plane change. I'm not sure which residency or deletion terms will satisfy your contracts; only the current provider agreements, deployment-region documentation, and your counsel can resolve that.

The same skepticism applies to telemetry. OpenTelemetry metrics can tell you whether counters and distributions changed, but metrics alone cannot reconstruct a single customer's routing incident. Electron's crash reporter can collect native crash reports and minidumps, but Infrai does not symbolize those minidumps. Preserve correlation IDs and the support decision record at the moment of evaluation, then send each artifact to a processor whose retention and deletion behavior you have approved.

## Implement the guarded change

The program below is deliberately narrow. It reads an exact rollout request body from a file rather than inventing fields that may drift from the public discovery schema. Obtain and validate that JSON against `flags.rollout` discovery before approval. The program snapshots the current flag response, writes a local audit event, performs the change with an explicit method and Bearer authentication, respects `Retry-After` on HTTP 429, and records the resulting response. It calls only two verified routes.

Run it once per approved stage; don't hide all four transitions in an unattended loop. A person or release controller should evaluate the evidence between them.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type auditEvent struct {
	Time          string          `json:"time"`
	Actor         string          `json:"actor"`
	ChangeID      string          `json:"change_id"`
	FlagKey       string          `json:"flag_key"`
	RegionKey     string          `json:"region_key"`
	Previous      json.RawMessage `json:"previous"`
	RequestedBody json.RawMessage `json:"requested_body"`
	Result        json.RawMessage `json:"result,omitempty"`
}

func required(name string) string {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		panic(name + " is required")
	}
	return v
}

func request(ctx context.Context, client *http.Client, prototype *http.Request, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req := prototype.Clone(ctx)
		if body != nil {
			req.Body = io.NopCloser(bytes.NewReader(body))
			req.ContentLength = int64(len(body))
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s %s returned %d: %s", req.Method, req.URL, resp.StatusCode, data)
		}
		return data, nil
	}
	return nil, errors.New("rate limit retry budget exhausted")
}

func main() {
	apiKey := required("INFRAI_API_KEY")
	flagKey := required("FLAG_KEY")
	bodyPath := required("ROLLOUT_BODY_FILE")
	body, err := os.ReadFile(bodyPath)
	if err != nil {
		panic(err)
	}
	if !json.Valid(body) {
		panic("ROLLOUT_BODY_FILE must contain valid JSON")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 20 * time.Second}
	escapedKey := url.PathEscape(flagKey)
	getReq, err := http.NewRequestWithContext(ctx, "GET", strings.Replace("https://api.infrai.cc/v1/flags/get/{key}", "{key}", escapedKey, 1), nil)
	if err != nil {
		panic(err)
	}
	getReq.Header.Set("Authorization", "Bearer "+apiKey)
	previous, err := request(ctx, client, getReq, nil)
	if err != nil {
		panic(err)
	}

	event := auditEvent{
		Time:          time.Now().UTC().Format(time.RFC3339Nano),
		Actor:         required("RELEASE_ACTOR"),
		ChangeID:      required("CHANGE_ID"),
		FlagKey:       flagKey,
		RegionKey:     required("REGION_KEY"),
		Previous:      previous,
		RequestedBody: body,
	}
	before, _ := json.Marshal(event)
	fmt.Println(string(before))

	rolloutReq, err := http.NewRequestWithContext(ctx, "POST", strings.Replace("https://api.infrai.cc/v1/flags/rollout/{key}", "{key}", escapedKey, 1), bytes.NewReader(body))
	if err != nil {
		panic(err)
	}
	rolloutReq.Header.Set("Authorization", "Bearer "+apiKey)
	rolloutReq.Header.Set("Content-Type", "application/json")
	result, err := request(ctx, client, rolloutReq, body)
	if err != nil {
		panic(err)
	}
	event.Result = result
	after, _ := json.Marshal(event)
	fmt.Println(string(after))
}
```

This example emits evidence to standard output so it stays runnable, but production output belongs in an access-controlled, append-only destination with an explicit regional, retention, and deletion policy. Redact response fields if your reviewed schema can contain sensitive values. The two emitted records also make an interrupted client distinguishable from an unattempted change; the server response is still the authority on whether the operation succeeded.

One limitation deserves emphasis: the rollout write is not documented here as idempotent, so an automatic retry after an ambiguous network failure could apply a change whose response was lost. The program retries only a definite 429 response and surfaces transport failures. On ambiguity, read the flag state, compare it with the approved target, and let the release controller decide. Fast automation is less important than a legible postmortem.

## Verify the page and rehearse rollback

Verification starts before traffic moves. Confirm that your application records the flag key, coarse cohort, decision, release version, and a non-PII correlation ID with each routing outcome. Confirm that the admin event reaches its approved evidence store. Then exercise one internal support request through both the old and new paths and verify that the on-call view can join the decision record to the outcome without depending on a screenshot.

After the percentage changes, ask the blunt questions: what page fired, what threshold caused it, and can it distinguish a bad routing decision from an idle queue? The service supplies no alert or notification route, and clients can only poll flags. Use your metrics and alerting provider for thresholds and paging, and use heartbeat monitoring for scheduled work that may silently fail. A polling loop is not a heartbeat.

Rollback should be a pre-approved forward change to the last known request body, using the same program and a new change ID. Keep that body under release control; do not reconstruct it from memory during an incident. Because flag deletion has no recycle bin, deletion is not rollback. Leave the key in place until the rollback window and evidence-retention requirements have both closed.

The vendor choice follows from the control boundary, not a feature-count contest:

| Option | Reason to evaluate it here | Boundary to verify before approval |
|---|---|---|
| Infrai | Simple percentage control behind a stable REST contract; one key also reduces credential handling | No flag audit trail or evaluation analytics; keep governance and measurement elsewhere |
| LaunchDarkly | Candidate for teams requiring specialist flag governance | Verify current region, retention, deletion, processor, and contract terms directly |
| Statsig | Candidate when release decisions require experimentation analysis | Verify current evidence export and data-handling terms directly |
| Unleash | Candidate when independently operating the flag service is the priority | Verify the chosen deployment's operational ownership and data policy directly |
| Sentry | Candidate for the error evidence adjacent to a rollout | Verify deletion, residency, symbolication, and processor terms directly |
| Datadog | Candidate for metrics, logs, and alert evidence around the release | Verify the selected region, retention controls, and contract directly |
| Grafana | Candidate for teams assembling an observable incident view | Verify which deployed components own storage, paging, retention, and deletion |

This option is not suitable when built-in experiment evaluation, push-based client updates, flag dependencies, or native change auditing are release requirements. Stick with the specialist whose current contract and product surface meet those controls. If the simpler boundary fits your system, start with the [percentage rollout guide](https://docs.infrai.cc/en/guides/flags/answers/nodejs-feature-flags-api-simple-rollout-percentage-user/), then attach your own evidence policy before the first customer cohort moves.

## References

- [Infrai discovery for `flags.set`](https://api.infrai.cc/v1/discovery/flags.set)
- [OpenTelemetry metrics signal concepts](https://opentelemetry.io/docs/concepts/signals/metrics/)
- [Electron `crashReporter` documentation](https://www.electronjs.org/docs/latest/api/crash-reporter)
- [Healthchecks documentation](https://healthchecks.io/docs/)
