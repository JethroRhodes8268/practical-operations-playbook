# Build SaaS Event Alert Emails in Node.js: DKIM Setup for Custom Domains (and Queue Triage)

TL;DR: The best way to build SaaS event alert emails in Node.js is to put contact-form events into an explicit outbox before handing them to an email transport, verify a custom domain with SPF and DKIM, publish DMARC deliberately, and route delivery outcomes back to the support queue. Treat an accepted handoff as one checkpoint, not proof that a support agent received or handled the message.

For an e-commerce site, the useful event is not “an email was sent.” It is “the shipping question for order 84721 reached the right queue, with enough evidence to retry or escalate without creating a duplicate.” That changes the design. A custom domain and a polished template matter, but the pager question is still the same: what signal fired, and which one did we trust?

That distinction matters.

## How should a Node.js SaaS route event alert emails?

At 3 a.m., I start with the event ID, not the transport response.

Define the contract around three states: accepted by the application, accepted by the outbound transport, and resolved by the support workflow. They are different facts. The first proves that the event was durably recorded. The second proves that the transport accepted a request. The third requires a delivery or failure signal plus a queue action. Collapsing these into a boolean called `sent` makes an incident look healthy until a customer asks why nobody answered.

The contact form should create an event with a stable identifier, queue classification, recipient policy, and rendered-content version. Select the support address from server-side rules such as `order_status`, `country`, and `topic`; keep the original customer address in `Reply-To`, and keep the authenticated sender on a domain the organization controls. A user-supplied address should never become the apparent sender.

```go
type ContactEvent struct {
    ID              string
    OrderID         string
    Topic           string
    Region          string
    Queue           string
    ReplyTo         string
    TemplateVersion string
    Attempt         int
    State           string
}
```

The important field is the event ID. It travels through the outbox, message header, transport callback, and support queue. When the same event appears twice, an operator can distinguish a retry from a second customer submission. A retry policy without that identity is a duplicate generator wearing a reliability badge.

The tradeoff is deliberate: queue-first delivery adds storage and reconciliation work, but direct sending makes a process timeout indistinguishable from a transport timeout. For a low-volume internal form, direct sending may be acceptable; for customer-facing order questions, losing ownership of one event is the more expensive failure.

## The 3 a.m. signal is queue age, not an open rate

The first useful alert is not an open-rate dip. Open measurements can be affected by privacy features, including Apple’s Mail Privacy Protection, so an apparent open is not a reliable acknowledgement from a person. Delivery status, bounce classification, queue age, and an agent’s disposition are closer to the operational question.

That is a deliberately narrow claim.

Ask four questions during an incident: did the application persist the event, did the worker claim it, did the transport accept it, and did the receiving workflow record the outcome? Each answer needs a timestamp and the same event ID. A dashboard that only counts calls to a send function is a decorative status light.

The alert policy should page on a broken obligation, not every transient error. Growing outbox age can indicate a worker or dependency problem. A rise in permanent failures can indicate recipient policy or authentication trouble. A missing callback can indicate an incomplete feedback path. Those signals deserve different owners and runbooks.

For the storefront case, keep two operational paths. The normal path sends the notification to the mapped support queue. The exception path writes a review item containing the event ID, reason, and safe-to-retry state. It should not silently redirect every failure to a personal mailbox; that creates an unowned queue and makes audit work harder.

## Treat the sending domain as release state

Domain verification is a release prerequisite, not a worker retry. Publish the DNS records required by the selected mail transport, verify SPF authorization for the actual sending path, and publish a DKIM public key under the selector used to sign messages. Then publish a DMARC record that aligns with the sender domain and the organization’s rollout policy. The exact DNS values belong to the transport and domain owner; copying a record from one environment into another is a reliable way to sign with the wrong identity.

Use a controlled `From` domain, put the customer address in `Reply-To`, and preserve the event ID in a header that does not expose sensitive order data. Keep the visible body useful without assuming images load. Include plain text and HTML alternatives, a concise subject with the queue-relevant topic, and a reference that lets an agent find the order through the authenticated application rather than putting private details in the subject.

Templates deserve versioning because a delivery incident and a content incident have different rollback paths. Store a template version with the event. Test both alternatives, escaped customer input, missing optional fields, and a long topic name. A template that looks correct in a browser can still be confusing in a narrow mail client, and readable content is not automatically authenticated.

Use an explicit version such as `v3` in test fixtures and deployment notes, then keep `v2` available until the new path has produced verified outcomes. This is a small operational detail, but it prevents a content rollback from becoming a replay of already-submitted events.

In a Node.js service, the transport belongs behind an interface while the outbox owns state transitions. This Go example shows the boundary without pretending that a transport call is a delivery receipt:

```go
type Mailer interface {
    Submit(ctx context.Context, message Message) (string, error)
}

type Message struct {
    EventID string
    From    string
    ReplyTo string
    To      string
    Subject string
    Text    string
    HTML    string
}

func Dispatch(ctx context.Context, store Store, mailer Mailer, id string) error {
    event, err := store.Claim(id)
    if err != nil {
        return err
    }

    message := Message{
        EventID: event.ID,
        From:    "support@alerts.example.com",
        ReplyTo: event.ReplyTo,
        To:      event.Queue,
        Subject: "Storefront contact: " + event.Topic,
        Text:    RenderText(event),
        HTML:    RenderHTML(event),
    }

    transportID, err := mailer.Submit(ctx, message)
    if err != nil {
        return store.MarkRetryable(id, err.Error())
    }
    return store.MarkSubmitted(id, transportID)
}
```

`MarkSubmitted` means the request crossed the transport boundary. It must not mark support work complete. A callback or polling adapter should later record the delivery result, and the queue should remain open until an agent or explicit automation resolves it. The adapter also needs idempotency: receiving the same outcome twice should update one event, not create two tickets.

## How do you verify and roll back the release?

Verify in layers. First, submit a test event through the production-shaped application path with a synthetic order reference and a real queue mapping. Confirm that the outbox row exists before the worker runs. Next, inspect the message identity and authentication results at a mailbox that exposes them; check the sender, `Reply-To`, DKIM selector, SPF authorization, and DMARC alignment. Finally, confirm that a delivery or failure outcome returns to the same event ID and that the support queue shows one item.

Do not use an open pixel as the pass condition. It answers a different, noisy question. A successful check is a chain: persisted event, one claimed attempt, transport acceptance, authenticated message, recorded outcome, and visible queue item. Capture those checkpoints in the deployment record so a later incident does not depend on somebody remembering what a green dashboard meant.

The rollback should be boring. Keep the previous template version available, stop selecting the new version for fresh events, and let already-submitted messages retain their recorded version. If authentication records changed, restore the last known-good DNS configuration through the domain owner’s change process. If the queue mapping is wrong, pause dispatch for that mapping, preserve outbox events, correct the rule, and replay only events whose state makes replay safe. This is the long part of the runbook because the dangerous failure is not a single rejected message; it is a recovery that loses the relationship between the storefront event, the message identity, and the support item.

A queue that can be paused is easier to reason about than a sender that retries blindly. The target is not a perfect dashboard. It is enough evidence to answer what happened to one customer’s request and a controlled way to continue after the answer is known.

This approach is a poor fit for a one-off broadcast where there is no support queue to resolve an event. It is also the wrong place to infer human attention from opens; use delivery and workflow outcomes for the decision that matters.

## References

- https://support.google.com/a/answer/81126
- https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios
- https://www.rfc-editor.org/rfc/rfc6376
- https://www.rfc-editor.org/rfc/rfc7489
- https://www.rfc-editor.org/rfc/rfc5322
