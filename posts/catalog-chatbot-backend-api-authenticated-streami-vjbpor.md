# Catalog Chatbot Backend API: Authenticated Streaming with Validated Writes

Choose a backend that can authenticate the user, stream provisional progress, and withhold catalog writes until attribute checks pass. For a B2B SaaS chatbot enriching messy product descriptions, that publication boundary should decide the quality-versus-latency trade-off before the choice of client SDK.

**Short answer:** Keep the authenticated web app's chatbot behind a small server-owned API, treat streamed suggestions as provisional, and publish only validated attributes. A simple backend API alternative to a provider SDK is useful when its authentication, cancellation, stream termination, and validation behavior are explicit enough to test. An earlier first token is a poor substitute for a usable catalog record.

This runbook uses an illustrative description, "blue work glove, pack of 12, size unclear," to make the boundary concrete. The number 12 describes a pack; it isn't evidence for a size. That distinction matters more than how quickly a plausible size appears in the chat window.

## What page fired when progress looked like completion?

Start the postmortem with that question. A dashboard showing successful HTTP responses cannot establish that an enrichment produced a supported attribute, and a stream that opened successfully provides even less evidence about the eventual catalog write. The failure mode to design against is specific: the browser displays a candidate, another component treats that display as acceptance, and an unsupported value reaches the catalog before validation finishes. This is a failure scenario, not a report of a measured incident.

Define completion first.

For the glove example, a proposed policy could accept color only when the description explicitly supports it, preserve the pack quantity separately, and leave size unknown unless an approved source supplies it. A model can suggest an interpretation; the application must decide which evidence permits publication. Prompt instructions can express the extraction task, but they shouldn't be the only place that a mandatory field rule exists.

The page should correspond to the consequence. Under this design, a write attempted without a validation decision is a publication-control violation; a rejected candidate is an expected quality outcome that needs a rate and examples before it deserves an alert. Keep slow completion, rejected enrichment, and unauthorized access distinguishable. Combining them into one red chart would force the responder to reconstruct the failure class while a queue continues to grow.

## How should an authenticated chatbot backend API gate catalog writes?

The browser authenticates to the application backend. That backend derives the tenant and catalog permissions from its trusted authentication context, authorizes the particular item, and invokes generation with the permitted description. A tenant identifier in a request body is an input to check, not proof of ownership. Keep upstream credentials on the server.

For the application protocol, define separate provisional and terminal events. A progress event may report that extraction is running. A candidate event, if exposed at all, must carry an explicit provisional state. An accepted event should refer to a stored result and its revision; a rejected event should explain which application rule prevented publication without returning another tenant's data or internal credentials. These are proposed application event names, not a promise about any provider's wire protocol.

One terminal decision.

Choose the browser transport around authentication. The standard EventSource constructor exposes a URL and a credentials option, but no arbitrary request-header option; that boundary matters if the existing application uses an Authorization header. A fetch request can carry the application's supported credentials and consume a streamed response, although its reader must then implement the chosen framing and reconnection behavior. Neither transport removes the need to authorize the catalog item before starting work.

Server-sent events define an event-stream format, including event boundaries and reconnection fields. They don't define an application-level catalog transaction. If a connection drops after persistence but before the browser receives the accepted event, the browser needs a way to retrieve the result by operation identifier. Replaying partial text is insufficient. Record the operation's terminal outcome alongside the write, and make repeated publication attempts resolve to that outcome using a database transaction or equivalent concurrency control.

## Make the safe implementation smaller than the prompt

Keep generation, validation, and persistence separate in the code even if they initially run within one service. The following Go example deliberately narrows the policy to a single color attribute: it accepts exactly the color permitted by a caller's trusted evidence rule and refuses an ambiguous or unsupported candidate. It is an application boundary, not a general semantic validator or a complete HTTP handler.

```go
package enrichment

import (
    "context"
    "errors"
)

type Candidate struct {
    Color string
}

type Generate func(context.Context, string) (Candidate, error)

// Commit must atomically persist the result and operation outcome.
// The caller must bind it to an authorized tenant and catalog item.
type Commit func(context.Context, string, Candidate) error

func Enrich(
    ctx context.Context,
    operationID string,
    description string,
    allowedColor string,
    generate Generate,
    commit Commit,
) error {
    candidate, err := generate(ctx, description)
    if err != nil {
        return err
    }
    if allowedColor == "" || candidate.Color != allowedColor {
        return errors.New("color requires supported evidence")
    }
    if err := ctx.Err(); err != nil {
        return err
    }
    return commit(ctx, operationID, candidate)
}
```

For the example description, an approved evidence rule could supply "blue" as allowedColor. The rule must come from trusted application logic or reviewed evidence; passing a second model guess into that parameter would merely move the uncertainty. The omitted size and pack fields require their own rules, and an empty color is rejected here by design. A broader catalog may prefer partial acceptance, but that requires an explicit field-level policy.

The HTTP layer should carry the request context into generation and storage calls. Go's request context is canceled when the client connection closes, among other conditions documented by net/http; cancellation still depends on downstream operations honoring that context. The check before commit does not make cancellation atomic with a database write. If cancellation races with persistence, the recorded operation outcome must settle what happened.

That's the awkward case to test.

A completed write should remain discoverable even if its response was lost. Conversely, a canceled generation should not quietly launch an unlimited background retry. If enrichment must survive the browser disconnecting, model it as a durable job with separate authorization and status retrieval rather than implying that the HTTP request owns its lifetime. The extra machinery buys recoverability, while adding queue management and another state transition for the team to operate.

## Verify quality against useful completion time

Before deployment, build a reviewed fixture set from descriptions the application is authorized to process. Include the glove description, missing units, contradictory sizes, and descriptions containing instructions to disregard the extraction rules. These are proposed test inputs, not benchmark results. Record which fields are supported and which must remain unknown so that a fluent answer cannot pass merely because it looks reasonable.

Measure time to the first visible progress event separately from time to a validated, persisted result. Track supported-field acceptance and unsupported-field publication separately as well. A prompt change that makes progress appear earlier while increasing rejected results has changed the user experience without necessarily improving the catalog workflow; a stricter validator may increase useful completion time while protecting downstream consumers. State which trade-off the application accepts before the rollout.

Use release gates that correspond to those decisions (a visible candidate is still provisional):

- Verify that a user cannot start or retrieve another tenant's enrichment operation, including through a reused operation identifier.
- Interrupt the connection before generation, during candidate delivery, and immediately after persistence; check the recorded result and the browser's recovery behavior.
- Repeat the same operation and race two publication attempts; verify that storage enforces the intended single outcome.
- Confirm that unsupported size values remain unpublished even when the generated text is syntactically valid.

A successful response code is only one observation. Preserve an operation identifier, policy revision, prompt revision, rejection reason, and timing for each stage so a responder can distinguish generation delay from validation delay and persistence delay. Bound retention and access to source descriptions; copying every product description into every log stream makes incident review harder to control. The telemetry schema should support the investigation without becoming a second uncontrolled catalog.

## Roll back publication before widening generation

Deploy a changed extraction policy against a limited slice of authorized traffic, with the previous publication policy available and a reviewed fixture comparison attached to the release. If unsupported attributes begin passing, stop new publication under that policy, retain operation outcomes, and route affected work for review. Continuing to stream progress while silently accepting suspect records would conceal the very boundary this design is supposed to enforce.

Rollback needs revision checks. Restoring an older policy doesn't identify which existing attributes it would reject, and rewriting every touched item could overwrite a later human correction. Use the recorded policy and item revisions to find affected candidates, then apply the application's reviewed correction procedure.

The selection rule stays narrow: choose the smallest authenticated backend contract that lets the team prove when an enrichment became publishable, recover its terminal outcome, and explain its useful completion time. Keep provisional streaming responsive, but make catalog acceptance depend on evidence. The page should identify which guarantee failed.

## References

- Server-sent events, event-stream interpretation, and EventSource interface: https://html.spec.whatwg.org/multipage/server-sent-events.html
- Fetch API request credentials and response body streaming: https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API/Using_Fetch
- Go HTTP request contexts and cancellation behavior: https://pkg.go.dev/net/http#Request.Context
- Go context cancellation contract: https://pkg.go.dev/context
- Prompt design techniques and examples: https://www.promptingguide.ai
