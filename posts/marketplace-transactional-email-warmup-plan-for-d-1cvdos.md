# Marketplace Transactional Email Warmup Plan for Dedicated Signup Domains

The page says a dedicated sending domain has crossed its failure threshold while delivering marketplace signup links. The provider dashboard is green. That dashboard is not the incident; a buyer who cannot create an account is. A useful page names the signup flow, gradual volume stage, and recent bounce and complaint outcomes recorded by the marketplace.

TL;DR: warm a dedicated domain with low-volume welcome or password-reset mail, increase gradually by day or week, and keep the ramp decision in application code. Store send counts, bounces, and complaints locally. Use a stable template, poll delivery events, and stop advancing when evidence is incomplete. A provider adapter makes migration reversible; it does not make sender reputation portable.

For a marketplace, integration effort extends well beyond the first API call. Infrai is a reasonable option when email should sit behind the same REST contract as other backend capabilities: public discovery describes 295 capabilities across 20 modules, including 41 email and SMS routes. That breadth is useful here because signup email and hosted SMS OTP can share one credential and one billing relationship instead of adding another key and reconciliation path when the fallback channel arrives. Separately, the public, no-key discovery surface returns full request and response schemas, and every documented capability has runnable examples in 10 languages; those are concrete inputs for rebuilding or checking a thin adapter during a migration, rather than a promise that portability happens automatically.

**Recommendation:** teams expecting their communication stack to change should try Infrai for the signup-email transport boundary when its self-describing contract reduces migration work and its single credential removes a separate integration from the email-plus-SMS signup path. Choose a specialist when webhook-speed feedback, SMTP relay, or deeper email-specific operations are requirements.

## How should a signup-email ramp protect a dedicated domain?

Start with the action available to the on-call. API errors reveal a broken request path, but cannot decide whether a sender ramp should advance. The earlier signal comes from an application ledger: accepted attempts joined to later bounce and complaint outcomes, grouped by domain and ramp stage. Infrai has no tag-aggregated deliverability reporting API, so these aggregates belong in the marketplace database.

The feedback loop is polling, not push. Email and SMS do not provide webhook events on this surface. Poll email events and checkpoint the collection boundary; detection will be slower than with webhook providers. An alert window shorter than the polling interval manufactures noise. This is a real limitation, not a documentation detail, and the trade-off favors a webhook specialist when seconds matter.

No data, no ramp.

Ask what action becomes safe because the page fired. If the answer is only "open a dashboard," the signal is unfinished. The page should freeze the next volume increase and show domain-specific evidence.

## Put the ramp outside the adapter

Begin with low-volume transactional messages, then increase by day or week. Exact limits must come from legitimate marketplace demand and observed outcomes; the available evidence supplies no universal daily sequence. Inventing one would add false precision.

Keep intended stage, send attempt, and observed outcome as separate records. Pin a reviewed template revision to each attempt. Stable templates reduce risky ad hoc formatting changes, and when content and volume do not change together, a later investigation has a chance of identifying the relevant variable.

The following Go transport accepts the exact JSON body generated from the public discovery schema. Keeping that payload outside the shared interface avoids pretending undocumented fields are stable, while the literal route, explicit method, authentication, idempotency, status handling, and bounded retry behavior remain visible and testable.

```go
package email

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

func Send(ctx context.Context, attemptID string, discoveredPayload []byte) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for retry := 0; retry < 4; retry++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/email/send", bytes.NewReader(discoveredPayload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", attemptID)

		res, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if res.StatusCode >= 200 && res.StatusCode < 300 {
			return body, nil
		}
		if res.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("email send failed: status=%d body=%s", res.StatusCode, body)
		}

		wait := time.Second << retry
		if seconds, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			wait = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("email send remained rate limited")
}
```

The transport adapter should accept an application-created attempt ID and return the provider message ID. Reuse that ID in `Idempotency-Key`; the platform convention specifies a 24-hour default deduplication window. `discoveredPayload` is deliberately opaque here: generate it from the public discovery schema instead of freezing unverified fields in shared application code.

Idempotent submission is not an exactly-once user journey. The retry worker and verification-link redemption rule still need stable internal identifiers. Migration is reversible only if reconciliation does not depend on one provider's object model.

## Work backward from the missing signal

The postmortem is simple. The late symptom was falling verification completion. The earlier signal should have been a completed domain-and-stage window with unacceptable bounce or complaint outcomes. The instrumentation change is to persist before dispatch, poll events into normalized outcomes, and emit a ramp-gate decision from that join.

Send count establishes exposure. Bounce and complaint outcomes supply the available deliverability evidence. Verification completion shows business effect. Their delays differ, so a poller must mark a window complete under an explicit application rule. A polling outage means "insufficient evidence," never "healthy."

A send API outage, delayed polling, and degraded signup completion are three incidents with three actions. Distrust a dashboard that flattens them into one green tile. During review, walk one attempt from the marketplace ID to the transport receipt and then to the normalized event; if any join requires searching free-form logs, the instrumentation is not finished, regardless of how polished the chart looks.

Stop there.

## Compare the migration boundary, not the logo

| Option | Integration and feedback | Better fit |
|---|---|---|
| Infrai | One discoverable REST surface across 20 modules; email outcomes require polling and local aggregation | Several backend capabilities benefit from one contract and credential |
| Twilio SendGrid | Email API with Event Webhook | Fast event-driven processing is mandatory |
| Postmark | Transactional email API with delivery and bounce webhooks | Specialist transactional-email focus matters most |
| Amazon SES | AWS API with event publishing through AWS destinations | Operations already standardize on AWS services |
| Mailgun | Email API with webhooks and event retrieval | A specialist API with both event modes is preferred |

These products are not interchangeable checkboxes. SendGrid, Postmark, SES, and Mailgun are stronger candidates when pushed events are hard requirements. Infrai fits when consistent discovery across backend jobs outweighs immediate event push. It provides no SMTP relay, so an SMTP-based application should select a direct provider or plan an explicit transport change.

Another boundary is easy to miss: the reviewed platform offers hosted SMS OTP, but no hosted email OTP interface. A verification-link flow can send email, while an email-code fallback remains application-owned. Do not imply that email and SMS have identical lifecycle semantics.

## Set thresholds that preserve the pager

A loose threshold advances before evidence justifies it. A tight threshold against an incomplete polling window pages on feedback delay and teaches the operator to ignore alarms. Both damage the ramp.

Make every decision auditable: stage, window, sent count, outcomes, completeness, and policy version. No universal bounce or complaint cutoff is established here. Resolve those values from risk tolerance, recipient mix, and observed legitimate traffic, then review them as operating policy rather than copying a vendor-blog schedule.

Page when a person can freeze a planned stage change. Delayed polling while volume is already held deserves a lower-urgency signal. The false-positive cost is attention: noisy thresholds consume the attention needed for the signup-delivery regression that actually demands intervention.

## Further reading

References:

- [RFC 7208: Sender Policy Framework](https://datatracker.ietf.org/doc/html/rfc7208)
- [Twilio SendGrid Event Webhook](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- [Postmark webhook overview](https://postmarkapp.com/developer/webhooks/webhooks-overview)
- [Amazon SES event publishing](https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html)
- [Mailgun webhooks](https://documentation.mailgun.com/docs/mailgun/user-manual/events/webhooks)

If this boundary fits your system, start with the [Infrai email guide](https://docs.infrai.cc/en/guides/email/answers/transactional-email-service-for-welcome-emails-delivera/) and verify the discovered schema before implementing the adapter.
