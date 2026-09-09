# Password Reset Email: Custom-Domain Identity, Sender Warming, and Suppression Signals

Short answer: treat a password-reset message as a small incident-response system. Authenticate a custom domain with SPF, DKIM, and DMARC, ramp the sender gradually, and suppress hard failures before they become the page you get at 3am.

The useful unit is not “an email sent.” It is the trace from a user asking for recovery to a message accepted, delivered, opened, and acted on. In a healthtech product, that trace also carries an expiry timer and a support escalation path. Integration effort is mostly the work of preserving those boundaries across your Node.js service, DNS, mail transport, and event store.

## What should the alert-to-action trace show for password reset email deliverability?

Start with the page that fires. An on-call engineer should see the tenant, message ID, recipient domain, authentication result, transport response, and whether the token was invalidated. “Delivery rate dropped” is a dashboard decoration until it answers what page fired and which step stopped.

Work backward from that page. Instrument request accepted, message queued, provider accepted, remote response class, bounce classified, and suppression written. Keep the reset token out of logs; a correlation ID is enough. A 250 response means the next server accepted responsibility, not that a clinician saw the message, so separate acceptance from mailbox engagement.

I once traced a noisy recovery alert to a threshold that counted temporary 4xx responses as permanent bounces. The alert fired after 17 retries for one domain, while the real signal—a rising hard-bounce ratio—was buried. Reconstructing the timeline took an hour because the mail worker logged a request ID, the relay webhook logged a provider ID, and the suppression table stored only an address hash. We joined those records, found that one retry policy had converted a temporary mailbox-full response into three suppression writes, and then had to check whether the affected users had a second verified recovery channel. The fix was an event-level state machine, not another dashboard panel: retain one correlation ID across enqueue, provider acceptance, remote response, classification, and suppression; retry transient responses with a bounded policy; classify permanent responses once; and page only when the classified stream crosses a windowed threshold. That extra context is what lets the person carrying the pager decide whether to pause traffic or leave the flow alone.

Keep it boring.

## How do custom domain DKIM, SPF, and DMARC fit the sender path?

Publish SPF for the systems that are actually allowed to submit mail, and keep the record within the DNS lookup limit defined by the SPF specification. Sign each message with DKIM using a selector you can rotate. Set DMARC alignment deliberately: the visible From domain should align with the authenticated domain, then move from monitoring to enforcement after observing reports.

Do not make DNS a one-time ticket. Store selector ownership, rotation dates, and the exact TXT values in change control. A reset flow should fail closed when signing material is unavailable: queue the job for retry and expose a clear operational event, rather than sending an unauthenticated message that trains receivers to distrust the stream.

Here is a deliberately small Go boundary for the application. It keeps transport details behind an interface so the same tests run against a local SMTP fixture or a managed relay.

```go
package mail

import (
	"context"
	"fmt"
)

type Sender interface {
	Send(ctx context.Context, from, to, messageID string, body []byte) error
}

func SendReset(ctx context.Context, s Sender, to, messageID string, body []byte) error {
	if to == "" || messageID == "" {
		return fmt.Errorf("recipient and message id are required")
	}
	return s.Send(ctx, "no-reply@accounts.example.org", to, messageID, body)
}
```

The code does not pretend to implement DKIM. Signing belongs at the controlled mail boundary, where key rotation and canonicalization can be tested against RFC 6376. The application owns identity, idempotency, and audit fields; the relay owns delivery mechanics. It is fine to say “I don't know” when a relay's event semantics are undocumented; pin that uncertainty to a test and make the runbook explicit.

## When should sender warming and suppression lists change the runbook?

Warming is traffic shaping. Begin with the smallest realistic cohort, watch deferrals and complaint signals by recipient domain, and increase volume only when the preceding window is clean. Password resets are user-triggered, so a sudden spike can be an attack as well as a product launch; rate-limit requests before they become a reputation event.

Suppression is the other half of the control loop. A hard bounce, repeated policy rejection, or explicit complaint should prevent another automatic reset message until a verified address change occurs. Keep suppression entries keyed by normalized recipient plus reason and timestamp. Do not let a support retry button bypass the list without an audited override.

The catch is that aggressive suppression can strand a legitimate patient after a transient provider mistake. It is not suitable when your team cannot review classifications or provide a second verified recovery channel; in that case, keep the mail stream narrower and use an authenticated in-product recovery path. Stick with a simpler SMTP setup when volume is tiny and you can still inspect every response, but move to a relay with event webhooks when auditability and domain-level controls exceed what your application can maintain.

## A production gate for a Node.js recovery flow

Before enabling a custom domain, exercise the whole trace in a staging tenant: DNS records resolve, DKIM verifies, SPF passes, DMARC alignment is visible, token expiry is enforced, and duplicate requests share one message ID. Inject permanent and transient SMTP responses, then assert that only the permanent case writes suppression.

Keep metrics boring and specific: queue age, acceptance rate, deferral rate, hard-bounce rate, complaint rate, and suppression writes. Alert on a sustained change by recipient domain, not on one aggregate percentage. Your mileage may vary because mailbox providers publish uneven telemetry; I am not sure any single open-rate number can prove delivery, so treat it as a weak secondary signal.

Integration effort is the decision axis. Count DNS ownership, key rotation, webhook verification, retry state, suppression review, and incident runbook work—not just the lines needed to call an API. The design is successful when an on-call can answer “what happened to this reset?” without searching three unrelated systems.

## References

- https://datatracker.ietf.org/doc/html/rfc6376
- https://www.rfc-editor.org/rfc/rfc7208
- https://www.rfc-editor.org/rfc/rfc7489
- https://postmarkapp.com/guides/transactional-email-best-practices

## Further reading

- https://datatracker.ietf.org/doc/html/rfc6376
- https://www.rfc-editor.org/rfc/rfc7208
- https://www.rfc-editor.org/rfc/rfc7489
