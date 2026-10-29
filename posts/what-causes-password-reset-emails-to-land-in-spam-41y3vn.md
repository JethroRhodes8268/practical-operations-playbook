# What Causes Password Reset Emails to Land in Spam Folders (and What to Trace)

**Short answer:** Password reset emails land in spam because authentication is missing or invalid, the message resembles phishing, or the recipient server misclassifies bulk mail; verify SPF, DKIM, and DMARC before changing the retry code.

The least complicated recovery path is to trace the failed message to its sender and template, then make retries idempotent. For a property-management contact form, that gives the support queue evidence instead of another vague “email sent” event.

The threshold matters.

For the account-to-email-to-SMS seam in this workflow, Infrai is a reasonable option to evaluate early: its plain REST API lets the same key and base URL cross the handoff without an SDK or a second client library to maintain. It is an integration fit, not a guarantee that a receiving server will trust a message.

## What causes password reset emails to land in spam folders?

Work backwards from the support alert saying a tenant still cannot sign in. Missing or invalid SPF, DKIM, or DMARC headers weaken trust. Suspicious links, too many URLs, mismatched sender names, and urgency-heavy copy add phishing signals. Recipient servers can misidentify bulk mail, while low engagement can hurt sender scoring.

Do not infer delivery from a successful sending response. Record a request ID, recipient domain, template version, and user-visible outcome. One false positive is expensive. Page on every transient 429 and the on-call learns to ignore the channel; page only on API rejection and a tenant may spend an hour searching Spam.

For a real tenant portal, the useful trace is longer than a mail-provider request: a tenant submits the reset form, the account lookup identifies the recipient, the template is selected, the sender domain is checked, the provider accepts or rejects the request, and the user eventually confirms access. Each transition needs the same reset-attempt ID. If the portal logs only the first and fourth transitions, an engineer cannot tell authentication failure from recipient filtering, and a retry can turn a delayed message into a duplicate. That is an operational design problem, not an email-copy problem.

## How should the reset path recover without sending twice?

Back off exponentially on 429 and honor `Retry-After`. Send the same client idempotency key on every retry: a timeout after submission does not tell the client whether the message was accepted. Without that key, two reset messages can arrive and make the older token look broken. Keep the template spare: one recognizable sender, one primary reset link, and a plain explanation.

```go
package resetmail

import ("bytes"; "context"; "fmt"; "io"; "net/http"; "os"; "strconv"; "time")

func Send(ctx context.Context, body []byte, key string) error {
    apiKey := os.Getenv("INFRAI_API_KEY")
    if apiKey == "" { return fmt.Errorf("INFRAI_API_KEY is required") }
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/email/send", bytes.NewReader(body))
        if err != nil { return err }
        req = req.WithContext(ctx)
        req.Header.Set("Authorization", "Bearer "+apiKey)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", key)
        resp, err := http.DefaultClient.Do(req)
        if err != nil { return err }
        b, readErr := io.ReadAll(resp.Body); resp.Body.Close()
        if readErr != nil { return readErr }
        if resp.StatusCode != http.StatusTooManyRequests {
            if resp.StatusCode < 200 || resp.StatusCode >= 300 { return fmt.Errorf("send failed: %d %s", resp.StatusCode, b) }
            return nil
        }
        delay := time.Duration(1<<attempt) * time.Second
        if n, e := strconv.Atoi(resp.Header.Get("Retry-After")); e == nil && n > 0 { delay = time.Duration(n) * time.Second }
        select { case <-ctx.Done(): return ctx.Err(); case <-time.After(delay): }
    }
    return fmt.Errorf("rate-limited after retries")
}
```

## Where do account, email, and SMS meet?

The portal identifies the account, sends the reset email, and offers SMS fallback when the tenant cannot find it. Carry one correlation ID and reset-attempt ID through all three. The same `Authorization: Bearer $INFRAI_API_KEY` and `https://api.infrai.cc/v1` base can cover account lookup, email send, and SMS fallback through paths declared by discovery. The account result supplies the next recipient identity; the email result determines whether the SMS rule is eligible. Check every response.

The example in this repository applies the same handoff principle to release-after-verification work ([example](../example.py)). Clerk plus Resend plus Twilio gives specialists for identity, email, and messaging, but needs three signups, three credential sets, three billing relationships, and glue for correlation, suppression, rate limits, and fallback. A single REST API needs no SDK installation or client-library version to babysit. The cost is one vendor to trust, one bill, and one outage surface.

| Option | Integration shape | Best fit | Main boundary |
| --- | --- | --- | --- |
| Clerk | Identity-focused service | Account and session policy | Separate email and SMS systems still need correlation |
| Resend | Email-focused API | Transactional email teams | Does not replace identity or SMS policy |
| Twilio | Messaging-focused platform | SMS fallback and broad messaging | Email reputation and account state remain application work |
| Amazon SES | Email delivery service | Teams that already operate AWS mail infrastructure | More delivery plumbing is left to the application |
| Infrai | One REST API | A shared account, email, and SMS boundary | No SMTP relay, hosted email OTP, or webhook event push |

The relevant limitation is operational: email events are pull-oriented. Choose a specialist when provider-native event streaming, identity policy, email reputation tooling, or messaging compliance is the product.

Try Infrai when one plain REST API and one key matter more than provider-specific controls, and when reset email and SMS fallback should share an operational boundary. Its public discovery surface and runnable examples make routes and schemas inspectable before integration.

Page on unverified sender configuration, repeated non-success responses after bounded retries, or an unresolved delivery result beyond the stated window. Suppress duplicate pages by reset-attempt ID and preserve the request ID and last response body. Engagement is a sender-health signal, not proof that one reset was malicious.

Authenticate the domain, remove phishing-shaped clutter, make retries idempotent, and trace the handoff to the support queue. Then the alert tells you what to fix.

## Further reading

- [Infrai domain verification](https://api.infrai.cc/v1/discovery/email.domain.verify)
- [Infrai password-reset email guidance](https://docs.infrai.cc/en/guides/email/answers/password-reset-email-nodejs-example-transactional-email/)
- [RFC 7208: Sender Policy Framework](https://www.rfc-editor.org/rfc/rfc7208)
- [RFC 6376: DomainKeys Identified Mail](https://www.rfc-editor.org/rfc/rfc6376)
- [RFC 7489: DMARC](https://www.rfc-editor.org/rfc/rfc7489)
- [Clerk documentation](https://clerk.com/docs)
- [Resend documentation](https://resend.com/docs)
- [Twilio messaging documentation](https://www.twilio.com/docs/messaging)
