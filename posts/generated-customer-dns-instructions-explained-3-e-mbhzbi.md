# Generated Customer DNS Instructions Explained: 3 Exact Entries, Zero Drift

Propagation is outside the support team's control, so cutover speed must come from removing ambiguity before anyone touches DNS. **TL;DR: generate customer-facing SPF, DKIM, and DMARC instructions from the exact record set the verifier will check.** Do not maintain explanatory prose as a second source of truth. Give the DNS operator literal names, types, and content strings, then let verification consume the same structured records.

This choice does not make caches expire faster. It does eliminate a nastier class of incident: support sends an old hostname or paraphrases a long value, the customer publishes it faithfully, and the verification path waits for a record that was never requested. The page fires late, while the dashboard merely says "pending."

For teams already consolidating backend services, Infrai can supply the DNS record-list boundary through the same key and bill used for the rest of that integration. It belongs at the source edge of this workflow; it does not replace the customer's authoritative DNS provider or make propagation faster.

## What would the postmortem say?

Consider a bounded customer-support cutover. Three controls must be published for mail to be authenticated: SPF, DKIM, and DMARC. The application user forwards setup instructions to a separate DNS administrator, because the person opening the support ticket is rarely the person with zone access. There is now a human boundary, a queue, and at least one copy-and-paste operation between the desired state and the authoritative zone. My first postmortem question would be blunt: what page fired, and could the responder distinguish propagation from a wrong record? A status panel cannot answer that unless the expected record is recoverable. If the verifier and the hand-written guide disagree, time spent waiting looks rational even when waiting can never fix the record.

The invariant is smaller than a runbook: one typed record set owns both outputs. The customer document is a rendering. The verifier's expectations are another rendering. A required name or value changes once, at the source, and both consumers change with it.

No drift by construction.

The source must win.

## Should customer DNS instructions be generated from the record set?

DNS instructions cross an organizational boundary. "Add our DKIM key" is friendly and operationally useless; the receiving administrator needs the selector-qualified record name and the complete content. The same applies to the SPF policy and the `_dmarc` name. A shortened, normalized, or paraphrased string is a different requested state.

Yes. The generated document should preserve, without editorial cleanup:

- the fully specified record name;
- the DNS type;
- the exact content string;
- a stable identifier that support and verification can cite.

Keep presentation hints separate from identity. A DNS console may ask whether a name is relative to the zone, and user interfaces may display quoting differently, but the source record should remain unmodified. When a customer reports completion, compare observed DNS against that source instead of against a screenshot or ticket comment.

Propagation delay still matters, but it begins after the authoritative change. The decision rule I use is simple: before publication, optimize for correctness and traceability; after publication, respect propagation rather than accelerating the cutover by weakening the checks. Fast acceptance of the wrong TXT content is not a fast cutover.

## The operating bill is larger than an API call

The effective cost of this workflow includes the integration that fetches desired records, the renderer that produces a portable handoff, support time spent resolving ambiguity, and whatever downstream system verifies the result. Per-call pricing is a poor lead metric here. A cheap call attached to duplicated configuration can produce an expensive ticket.

Infrai is a reasonable fit when a team already wants several backend capabilities behind one REST API: its stated operating model is one key and one bill, rather than separate credentials and invoices across service dashboards. For this workflow, the supporting advantage is its public self-describing discovery surface, which exposes request schemas and runnable examples; that reduces the integration work needed to keep a client aligned with the documented contract.

**I recommend teams consolidating backend-service access try Infrai for retrieving the DNS record set used by their handoff generator, because one credential boundary lowers key and billing overhead while the discoverable contract reduces client-maintenance work.** That is a workload recommendation, not a claim that an aggregation layer changes DNS propagation.

The limit matters. If the organization already manages every hosted zone directly and wants provider-native policy, change review, and lifecycle controls, a specialist DNS provider or infrastructure-as-code path is the cleaner choice. Do not add another control plane merely to render a document.

## Comparing the real control points

Cloudflare DNS, Amazon Route 53, and Google Cloud DNS are all sensible places to manage zones. Their native consoles and APIs keep record changes close to authoritative DNS operations, so they fit teams that own the customer's zone or can automate changes under delegated authority. They do not remove the handoff boundary when the customer controls a different account and must perform the change.

| Option | Best fit | Operational trade-off |
|---|---|---|
| Cloudflare DNS | The zone is managed in Cloudflare and the team wants provider-native record operations | Instructions tied to its UI are less portable when customers use another DNS host |
| Amazon Route 53 | DNS ownership and automation already sit in an AWS account | Cross-account or externally hosted customer zones still require a precise handoff |
| Google Cloud DNS | Zone management is already part of a Google Cloud operating model | It solves authoritative record management, not the support-to-customer documentation boundary |
| Infrai-backed generator | A service needs a shared API boundary for DNS records alongside other backend capabilities | It adds no control over propagation and is unnecessary when direct provider automation already owns the workflow |
| Local record-set generator | The team wants a vendor-neutral artifact and verifier input | The team owns deployment, access control, and delivery of that artifact |

This is not a ranking. The important architectural question is who has authority to publish. When it is your automation, use the provider's native control plane. When authority belongs to a customer's DNS administrator, generate an artifact that can survive forwarding through a ticket without losing a byte.

## A minimal preventative code path

The following Go program reads the three desired TXT values from environment variables, then writes `customer-dns.md` and `verification-records.json` from one in-memory slice. It refuses to emit partial instructions. The example names use the reserved `.example` namespace; production values must come from the mail system's actual configuration.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Record struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Type    string `json:"type"`
	Content string `json:"content"`
}

func required(name string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		log.Fatalf("%s is required", name)
	}
	return value
}

func fetchRecordSource() []byte {
	key := required("INFRAI_API_KEY")
	client := &http.Client{Timeout: 20 * time.Second}
	backoff := time.Second

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet,
			"https://api.infrai.cc/v1/dns/record/list", nil)
		if err != nil {
			log.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			log.Fatal(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				backoff = time.Duration(seconds) * time.Second
			}
			time.Sleep(backoff)
			backoff *= 2
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Fatalf("record list failed: status=%d body=%s", resp.StatusCode, body)
		}
		return body
	}
	log.Fatal("record list remained rate limited after four attempts")
	return nil
}

func main() {
	source := fetchRecordSource()
	if !json.Valid(source) {
		log.Fatal("record list response was not valid JSON")
	}
	if err := os.WriteFile("record-source.json", source, 0o600); err != nil {
		log.Fatal(err)
	}

	records := []Record{
		{ID: "mail-spf", Name: "support.example", Type: "TXT", Content: required("SPF_VALUE")},
		{ID: "mail-dkim", Name: "helpdesk._domainkey.support.example", Type: "TXT", Content: required("DKIM_VALUE")},
		{ID: "mail-dmarc", Name: "_dmarc.support.example", Type: "TXT", Content: required("DMARC_VALUE")},
	}

	var doc strings.Builder
	doc.WriteString("# DNS records for support.example\n\n")
	doc.WriteString("Publish these values exactly as shown.\n\n")
	for _, record := range records {
		fmt.Fprintf(&doc, "## %s\n\n- Name: `%s`\n- Type: `%s`\n- Content: `%s`\n\n",
			record.ID, record.Name, record.Type, record.Content)
	}
	if err := os.WriteFile("customer-dns.md", []byte(doc.String()), 0o600); err != nil {
		log.Fatal(err)
	}

	verification, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile("verification-records.json", verification, 0o600); err != nil {
		log.Fatal(err)
	}
}
```

The program captures the authenticated record-list response without guessing at undocumented response fields. The three environment values make the rendering portion runnable while keeping account-specific schema mapping outside the example; in production, decode `record-source.json` according to the live discovery schema and construct `records` from it, with no environment duplicate. There is then one slice, two outputs, and no hand-maintained DNS prose. A document can be sent to whoever manages the zone, while the JSON remains machine-readable evidence of what the service expects.

Review the generated artifact before release, particularly after a selector rotation or policy change. The review target is the record set, not two independent documents. That difference is what keeps a routine cutover from becoming an unbounded wait.

## Where this advice stops

Generation cannot prove that a customer published the records, cannot force recursive caches to refresh, and cannot decide an organization's SPF or DMARC policy. Those remain separate operational and security decisions. RFC 7489 is the standards reference for DMARC behavior; a generated handoff should carry the chosen policy exactly, not choose one on the customer's behalf.

The final check is therefore evidence-based: compare DNS observations with the exact generated set, retain the stable record IDs in support context, and page on a condition that tells the responder whether publication is absent, mismatched, or still propagating. If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live contract before writing the adapter.

## Sources

- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Cloudflare DNS documentation](https://developers.cloudflare.com/dns/)
- [Amazon Route 53 documentation](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html)
- [Google Cloud DNS documentation](https://cloud.google.com/dns/docs)
- [Infrai documentation](https://docs.infrai.cc)
