# Transactional Email Service: Proving Welcome Email Deliverability During Signup

Choose an API-first transactional email service only after it can produce an auditable chain from verified sending domain to bounce or delivery outcome. For an e-commerce signup verification link, that chain matters more than SMTP compatibility, a polished dashboard, or a long channel menu.

TL;DR: make domain verification a release gate, suppression checking a send gate, and event collection a monitored control. Store the signup attempt, policy decision, provider request identifier, and eventual outcome under one correlation ID. If the system requires SMTP relay, instant webhooks, hosted email OTP, or WhatsApp, RCS, and voice in the same workflow, reject a provider that does not supply them rather than hiding the gap in an operations runbook.

## How should a transactional email service prove welcome email deliverability?

An accepted API request proves acceptance, not delivery and certainly not inbox placement. The evidence trail should distinguish at least four moments: the customer requested an account, the application decided the address was eligible, the provider accepted the transactional message, and a later delivery or bounce outcome became observable. Record the sending domain and policy version with those moments. That lets an investigator reconstruct the decision without treating a dashboard screenshot as a ledger.

What page fired? “Email is down” is too vague to operate. A useful page identifies a customer-impacting break such as eligible verification attempts no longer producing outcomes, a bounce pattern departing from the service's own baseline, or the event collector checkpoint no longer advancing. Thresholds must come from the application's volume and normal behavior; there is no defensible universal bounce percentage in the available evidence.

Do not use opens as the control signal. Apple Mail Privacy Protection limits a sender's ability to learn whether a recipient opened a message, while the application owns a much stronger event for this workflow: whether the single-use verification link was redeemed, expired, or replaced. Delivery events answer a transport question. Redemption answers a product question.

Keep them separate.

## Build the evidence path before the send path

Start with a controlled sending domain. Verify it before enabling signup traffic, archive the resulting status with the release evidence, and put DKIM rotation on a planned runbook. Suppression management belongs ahead of submission so a known-suppressed address does not enter a retry loop. The application, rather than an undocumented provider feature, should own verification-token generation, expiry, and one-time redemption.

The dangerous boundary is a timeout after submission. The caller cannot infer whether the provider accepted the message, so an automatic retry can create two emails with competing links. Give each signup attempt a stable idempotency key where the provider supports one, retain the returned request identifier, and make redemption idempotent in the application as a second line of defense. Infrai specifies an `Idempotency-Key` convention with a 24-hour default deduplication window; that is a concrete control, not permission to retry forever.

Infrai fits this narrow flow when polling is acceptable: its email surface includes domain verification, DKIM rotation, suppression management, sending, and pull-based event inspection. Its broader advantage is the consistent contract behind the workflow. The live discovery surface reports 295 routes across 20 modules under one key, so a team that later adds SMS or observability does not have to introduce another credential shape and client integration for every capability. A single credential and bill also reduce the evidence an incident review must reconcile across unrelated control planes.

There is a second, different advantage. The discovery surface is public and self-describing without a key, with request and response schemas, billing details, and runnable examples; every documented capability has examples in 10 languages. That lets security and compliance reviewers inspect the contract before a production credential exists, while the runtime integration stays plain HTTP and does not require an SDK. For a small team, this removes real review friction. It does not improve deliverability by itself.

The limitations are sharp. Infrai is not a fit when email events must arrive by instant webhook, SMTP relay is mandatory, or hosted email OTP must be supplied by the provider. Scheduled email has no cancellation operation. WhatsApp, RCS, and voice are absent, and a pending Tencent email vendor cannot serve as evidence of domestic-China compliance. Choose Postmark or Resend instead when a focused email product with documented SMTP support is the controlling requirement; evaluate SendGrid or Amazon SES when an existing SMTP estate or AWS operating model drives the decision. Those trade-offs are selection failures for some architectures, not footnotes.

No webhook means no instant event.

The polling worker therefore needs a durable checkpoint, a lag metric, and an alert tied to the evidence objective. Persist each raw provider outcome and the normalized internal state before advancing the checkpoint. Back off on rate limits and honor `Retry-After`; otherwise a delayed evidence stream can become a self-inflicted outage. The required polling interval follows from the detection objective, not from a vendor dashboard's refresh rate.

The following Go probe checks the domain-list surface before a release. It is deliberately read-only: the output still needs to show the expected verified state, but the program does not guess an undocumented response field. Set `INFRAI_BASE_URL` to the documented API v1 base and keep the key in `INFRAI_API_KEY`. Four attempts, a 15-second client timeout, and `Retry-After` handling are explicit limits rather than measured service claims.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	baseURL := os.Getenv("INFRAI_BASE_URL")
	apiKey := os.Getenv("INFRAI_API_KEY")
	if baseURL == "" || apiKey == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_BASE_URL and INFRAI_API_KEY are required")
		os.Exit(2)
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, baseURL+"/email/domain/list", nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			fmt.Fprintln(os.Stderr, readErr)
			os.Exit(1)
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(body))
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			fmt.Fprintf(os.Stderr, "readiness failed: status=%d body=%s\n", resp.StatusCode, body)
			os.Exit(1)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
}
```

## Compare contracts under the same compliance drill

Postmark, Resend, SendGrid, and Amazon SES all belong on a serious shortlist, but their names do not settle the decision. Run one acceptance drill against each finalist in the account and region that will carry production traffic, then retain the output. Product documentation establishes available interfaces; the drill establishes whether the configured system produces the evidence your responders expect.

| Option | Why it belongs on the shortlist | Boundary the drill must settle |
|---|---|---|
| Postmark | A focused transactional-email product with documented API and SMTP delivery paths. | Prove domain authentication, suppression behavior, event delivery, retention, and retry handling in the production configuration. |
| Resend | An API-oriented email product that also documents SMTP integration for mixed estates. | Prove verification state, event semantics, evidence export, and duplicate-send behavior around timeouts. |
| SendGrid | A mature email platform with documented Mail Send API and SMTP relay options. | Test access control, suppressions, event delivery, and retention rather than accepting broad feature coverage as proof. |
| Amazon SES | API and SMTP interfaces can align with an organization already operating in AWS. | Confirm the chosen region and account state, then test the event-routing and evidence components the team assembles around it. |
| Infrai | One REST contract covers this email flow and many adjacent backend capabilities under one credential. | Reject it when SMTP, push webhooks, hosted email OTP, or WhatsApp/RCS/voice is mandatory. |

The fairest recommendation depends on ownership. Postmark is a sensible candidate for a team wanting a focused transactional product. Resend is worth testing when an API-oriented surface and an SMTP escape hatch both matter. SendGrid belongs where a broader email platform or an existing SMTP estate drives the migration. SES can suit an AWS-heavy team prepared to assemble and operate more of the surrounding evidence pipeline. Infrai is reasonable for a beginner-friendly US or EU SaaS implementation that values a consistent API across backend modules and can accept polling.

This is not a feature-count contest. It is a proof exercise.

Compliance evidence also needs a retention and purpose decision. GDPR Article 7 addresses demonstrating consent where processing relies on consent, but a signup verification message is transactional; counsel should determine the lawful basis rather than the email team guessing. Keep promotional consent distinct from the operational act of verifying an address. A completed signup must not silently become marketing permission.

## Verify launch and rehearse rollback

Before release, use a verified test domain and controlled recipient addresses. Demonstrate that one eligible signup produces one submission, a suppressed address produces none, a hard bounce becomes observable, the polling checkpoint advances, and a verification link changes application state only once. Then inject a timeout at the submission boundary and show that the idempotency strategy prevents duplicate effects. Record the request identifiers and timestamps as test evidence.

Rollback should stop new sends without destroying the trail needed to explain sends already accepted. Disable the signup-email producer, leave event collection running, and preserve the correlation records until outstanding outcomes have been reconciled. If a domain or DKIM change caused the release failure, restore the last known approved configuration through the provider's supported process; do not improvise DNS edits during the page and then lose the before-state.

The go/no-go rule is compact: ship only when domain readiness is recorded, suppressions are enforced, retries cannot duplicate the customer-visible effect, and delivery or bounce outcomes arrive within the team's declared observation window. Pick a different service if a required outcome can only be inferred from a dashboard or if the architecture depends on a channel or delivery mechanism the candidate does not provide.

That is the audit boundary. It is also the incident boundary.

## References

- Apple, “Use Mail Privacy Protection on iPhone”: https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios
- GDPR, Article 7, “Conditions for consent”: https://gdpr-info.eu/art-7-gdpr/
- Postmark developer documentation: https://postmarkapp.com/developer
- Resend documentation: https://resend.com/docs
- Twilio SendGrid Email API documentation: https://www.twilio.com/docs/sendgrid/api-reference
- Amazon SES documentation: https://docs.aws.amazon.com/ses/
