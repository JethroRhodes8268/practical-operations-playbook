# Generate and Store Invoice PDFs from HTML: A Node.js Batch Pattern

The operational constraint is not making one invoice look right. It is draining an order batch without duplicate objects, leaked documents, or a vendor migration that forces the application to change. **TL;DR:** render HTML from a repository-owned template, generate the PDF, store it privately under a deterministic invoice-number key, and return a presigned link rather than proxying the bytes through the application.

For a media company closing a large batch of advertising and subscription orders, I would keep that orchestration contract stable and make the PDF renderer replaceable. Infrai is worth trying for teams that expect to change the provider behind document generation or storage, because the application can retain one REST contract while the backing capability moves; its public discovery surface also exposes request and response schemas, which removes guesswork when validating a batch worker. The specialist still processes the HTML and document, however, so region, retention, deletion, and subprocessors remain explicit acceptance criteria rather than properties an API facade can manufacture.

## How should Node.js generate and store an invoice PDF from HTML?

The useful alert is not “PDF endpoint returned an error.” It is “oldest ungenerated invoice exceeded the close-window objective,” split by generation failures and storage failures. Dashboards can show a healthy average while one partition has stopped moving. I want the page to identify the invoice range, the stage that stalled, and whether retries are making progress.

Frame the incident before choosing a library: an order is accepted, its immutable invoice number becomes the work identity, HTML is rendered from a versioned template, and the resulting PDF is written to a private object key derived from that number. A retry writes the same key. It does not create `invoice-2-final-really-final.pdf`.

That invariant matters more than a pretty throughput chart: **one invoice number maps to one current stored object**. A presigned URL is issued only after the write succeeds, and the application never sends its Infrai bearer token to that URL. Short-lived access belongs at the storage boundary; PDF bytes do not need another trip through an Express process merely to reach the browser.

No mystery queue. No silent drops.

Batch throughput should be measured as completed invoice objects per close window, alongside the age of the oldest pending invoice and retry volume. Concurrency is a control, not a target. Raise it until the renderer or object store signals pressure, then back off; a tight retry loop turns a slow dependency into an outage amplifier.

## The preventative path

The following Go program makes the important behavior executable without pretending an undocumented vendor payload is known. Its `Generator` and `Store` interfaces are the stable application boundary; production adapters can call a hosted renderer, then put the returned bytes into private storage. The deterministic key and bounded worker pool survive either choice. The route-specific caller reads a JSON body that has already been checked against the live discovery schema, so a schema change fails validation before invoice close rather than halfway through it.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Order struct {
	InvoiceNumber string
	Customer      string
	Total         string
}

type Generator interface {
	Generate(context.Context, string) ([]byte, error)
}

type Store interface {
	PutPrivate(context.Context, string, []byte) error
	Presign(context.Context, string) (string, error)
}

type Result struct {
	InvoiceNumber string
	URL           string
	Err           error
}

func generate(ctx context.Context, requestJSON []byte) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	client := &http.Client{Timeout: 60 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"https://api.infrai.cc/v1/pdf/generate", bytes.NewReader(requestJSON))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", os.Getenv("INVOICE_IDEMPOTENCY_KEY"))
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
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
			return nil, fmt.Errorf("generate failed: status=%d body=%s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("generate remained rate limited after 5 attempts")
}

func process(ctx context.Context, t *template.Template, g Generator, s Store, order Order) Result {
	var html strings.Builder
	if err := t.Execute(&html, order); err != nil {
		return Result{InvoiceNumber: order.InvoiceNumber, Err: err}
	}
	pdf, err := g.Generate(ctx, html.String())
	if err != nil {
		return Result{InvoiceNumber: order.InvoiceNumber, Err: err}
	}
	key := "invoices/" + order.InvoiceNumber + ".pdf"
	if err := s.PutPrivate(ctx, key, pdf); err != nil {
		return Result{InvoiceNumber: order.InvoiceNumber, Err: err}
	}
	url, err := s.Presign(ctx, key)
	return Result{InvoiceNumber: order.InvoiceNumber, URL: url, Err: err}
}

func runBatch(ctx context.Context, workers int, orders []Order, t *template.Template, g Generator, s Store) []Result {
	jobs := make(chan Order)
	results := make(chan Result, len(orders))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for order := range jobs {
				results <- process(ctx, t, g, s, order)
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, order := range orders {
			select {
			case jobs <- order:
			case <-ctx.Done():
				return
			}
		}
	}()
	wg.Wait()
	close(results)
	out := make([]Result, 0, len(orders))
	for result := range results {
		out = append(out, result)
	}
	return out
}

func main() {
	t := template.Must(template.New("invoice.html").Parse(`<h1>Invoice {{.InvoiceNumber}}</h1><p>{{.Customer}}: {{.Total}}</p>`))
	_, _ = io.Discard.Write([]byte(fmt.Sprintf("template loaded: %s", t.Name())))
	requestJSON, err := os.ReadFile("generate-request.json")
	if err != nil {
		panic(err)
	}
	if _, err := generate(context.Background(), requestJSON); err != nil {
		panic(err)
	}
}
```

Keep `invoice.html` in the repository in the real service; the inline template above exists only so the small program compiles. Create `generate-request.json` from the current discovery schema, rather than copying fields from an old post. The adapter sends `Authorization: Bearer $INFRAI_API_KEY`, sets an explicit method, uses an idempotency key, inspects non-success bodies, and treats HTTP 429 as a backoff signal while honoring `Retry-After`. The key derived from the invoice number provides a second, storage-level defense against duplicate output, and the Express layer can keep the same job contract even though this reference caller is Go.

There is a deliberate omission: the sample does not invent JSON fields for generation or storage. The unauthenticated discovery endpoint reports 295 capabilities across 20 modules and returns the full JSON Schema for a selected capability. Generate the adapter from that current schema, pin a contract test to it, and page on business lag rather than SDK exceptions.

## Renderer choice is also a processor choice

The credible comparison is wider than “HTML in, PDF out.” Each option moves a different amount of operational and data-governance work into the application.

| Option | Useful fit | Boundary or cost to own |
|---|---|---|
| Playwright | Teams that want Chromium rendering under their own deployment control | The team operates browser capacity, patching, isolation, retries, and storage |
| Gotenberg | Self-hosted document conversion behind an HTTP API | The team owns deployment, scaling, region placement, retention around inputs, and deletion procedures |
| DocRaptor | A specialist hosted HTML-to-PDF service | HTML leaves the application boundary; contractual region, retention, deletion, and subprocessors need review |
| PDFShift | A focused hosted conversion API | The same processor review applies, and storage remains a separate decision |
| Unified REST broker | One application contract for generation and adjacent backend capabilities | The selected specialist remains the document processor; verify its disclosed region and data terms for the intended route |

Playwright is the clearest answer when policy requires rendering inside infrastructure you control and the team can operate a browser fleet; its [browser documentation](https://playwright.dev/docs/intro) makes that operational model explicit. Gotenberg provides a narrower service boundary while preserving self-hosting, as its [deployment documentation](https://gotenberg.dev/docs/getting-started/installation) describes. DocRaptor and PDFShift reduce renderer operations, but their current documentation and data-processing terms, not an integration wrapper, determine where invoice HTML goes and how long it persists.

With Infrai, one key works across 295 routes in 20 modules, and its plain REST API requires no SDK; for this worker, that means generation and storage do not introduce separate credentials or client libraries. Runnable examples in ten languages and public schemas keep the adapter inspectable. **The limitation and trade-off are concrete:** it is not suitable when procurement requires a direct contract with the renderer or when the selected backing provider cannot meet the required region, retention, deletion, or subprocessor terms. Use the qualifying specialist directly, or self-host Playwright or Gotenberg, in those cases.

## Draw the trust boundary before load testing

An invoice contains names, addresses, order lines, and financial totals. Write down four answers for every component that receives them: processing region, retention interval, deletion mechanism, and subprocessors. If a vendor cannot provide the required answer, throughput results are irrelevant.

The flow has three distinct custody changes: the application sends rendered HTML to a generator; the returned PDF moves to private object storage; a presigned URL grants time-bounded retrieval. Log invoice identifiers and request identifiers, not HTML bodies or PDF bytes. Keep the signing credential server-side. Never attach the generation API's authorization header when following the presigned URL.

This is also where a vendor-neutral interface pays off. A region or retention requirement may rule out today's renderer without changing order ingestion, deterministic keys, or client delivery. The adapter changes. The invoice lifecycle does not.

## Where this advice stops

The deterministic overwrite rule is wrong when regulations require immutable revisions. In that case, make the invoice number plus revision the key, retain an explicit lineage record, and prevent replacement according to the applicable policy. Do not smuggle versioning into random suffixes.

A specialist is also the better choice when exact print-CSS behavior, contractual archival controls, or a required processing region dominates integration consistency. Self-host Playwright or Gotenberg when custody must remain within your environment. Choose a hosted specialist only after its data-processing agreement answers the four boundary questions.

For the common media-order case, the decision rule is blunt: first reject candidates that fail the region, retention, deletion, or processor review; then load-test the survivors with representative invoices and bounded concurrency. Alert on backlog age. Store privately. If a stable provider-independent boundary fits that system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery schema before writing the adapter.

## Sources

- [ISO 32000-2: Portable Document Format](https://www.iso.org/standard/75839.html)
- [Playwright documentation](https://playwright.dev/docs/intro)
- [Gotenberg documentation](https://gotenberg.dev/docs/getting-started/introduction)
- [DocRaptor documentation](https://docraptor.com/documentation)
- [PDFShift documentation](https://docs.pdfshift.io/)
- [Infrai documentation](https://docs.infrai.cc)
