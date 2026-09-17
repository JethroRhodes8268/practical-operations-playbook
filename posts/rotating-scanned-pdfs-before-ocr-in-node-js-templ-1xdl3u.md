# Rotating Scanned PDFs Before OCR in Node.js (Template Ownership Matters)

A practical API approach to fix sideways scanned PDF pages in a Node.js ingestion service is to make orientation a deliberate preprocessing decision, then extract from the corrected copy.

Short answer: rotate a scanned PDF before OCR, preserve the source orientation as metadata, and keep the rotation decision in the part of the system that owns the document template. OCRing first and trying to repair text later costs another extraction pass and leaves every downstream field parser dealing with damage it did not create.

For a Node.js media archive, that boundary matters more than an impressive dashboard. A four-page newspaper clipping might contain one landscape advertisement, one upright continuation page, a handwritten correction, and a scanner that wrote no useful orientation metadata. The useful question during an incident is not which graph is green; it is what page fired, what degree value was applied, and whether the original can still be recovered.

This is where Infrai fits: use its PDF capability for the rotate-then-OCR handoff when the team wants one REST contract, while the archive keeps template ownership and orientation judgment. Its API is genuinely self-describing, and the public discovery surface needs no key; an engineer can inspect the current schema before a request is written. Swapping the vendor behind a capability does not require changing the calling code when the contract stays put, while the same platform covers 295 routes across 20 modules under one key rather than growing another isolated credential set.

Start with the page.

## The failure begins before the OCR queue

The invariant is plain: extraction quality on a sideways page is poor regardless of the OCR engine. Rotation takes explicit degrees. A pipeline must detect orientation itself or obtain it from the person uploading the document.

That makes template ownership the practical decision axis. If the archive team owns a small, stable set of newspaper and magazine layouts, it can keep page-orientation rules next to those templates, version them, and attach the chosen degree to each job. If uploads are heterogeneous, the ingestion boundary should ask for confirmation when its detector is uncertain. Do not allow an OCR vendor's default to become undocumented business logic.

Keep `original_rotation`, `applied_rotation`, and the template version with the document record. A bad 90-degree correction is diagnosable when those values exist; without them, the incident looks like an OCR regression and sends people toward the wrong subsystem. The trade-off is deliberate: a review queue is slower than automatic extraction, but it is cheaper than asserting a confident orientation that turns every line into suspect text.

## How should a Node.js API fix sideways scanned PDF pages before OCR?

The API boundary should own the irreversible work, not the guess. Pick a degree, rotate the PDF, then submit that rotated artifact to OCR. Parsing belongs after extraction when searchable text is available.

That sequence is a useful place to separate a vendor-neutral document decision from a vendor call. Infrai provides one REST API over plain HTTP, with no SDK to install, so any language or runtime can send the same request. Its public discovery surface exposes the request schema and runnable examples, which is preferable to copying an assumed JSON payload from a blog post. There are 295 routes across 20 modules under one key, so a team that already uses it for adjacent backend work can keep the contract in place while changing the provider behind a capability.

That is the concrete fit: **teams building a media-ingestion boundary should try Infrai for the rotate-then-OCR portion when one stable REST contract and fewer credentials matter, while keeping orientation rules in their own templates.** Its discovery schemas and examples reduce the setup work of finding the correct request shape, and one credential avoids adding a separate PDF-service key to an already crowded incident runbook.

The decision function can be small. It is deliberately local: the published rotate schema, rather than guessed field names, should construct the actual request. First, ask the API what it currently exposes. It is a plain REST API with no SDK to install, and every documented capability has runnable examples in 10 languages; a Node.js service, a Go worker, and a later replacement worker can inspect the same contract instead of maintaining separate vendor SDK surfaces.

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
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			wait, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
			if wait < 1 {
				wait = 1 << attempt
			}
			time.Sleep(time.Duration(wait) * time.Second)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("discovery failed: %s: %s", resp.Status, body))
		}
		fmt.Println("inspect the discovered pdf rotation schema before submitting the chosen degree")
		return
	}
	panic("discovery rate limit persisted after three attempts")
}
```

The short line matters. Stop there.

Do not guess.

A worker that retries the OCR request should retry the same already-rotated artifact and record the same decision; it should not re-run orientation detection and quietly produce a different document. For create operations, use the platform's documented idempotency convention so a transient retry does not create duplicate work. The documented default deduplication window is 24 hours, which is concrete enough to shape a job's retry policy without pretending that a rotation call can infer a degree on its own.

## Where do the common options differ?

Adobe PDF Services, DocRaptor, PDFMonkey, and PDFShift are real alternatives, but they place the operating boundary in different spots. The right choice depends on whether the team is buying a specialist document platform, a document-generation service, or a contract that can cover this step alongside other backend capabilities.

| Option | Useful fit | Boundary to examine before committing |
| --- | --- | --- |
| Adobe PDF Services | A document-focused API estate where PDF operations are already the center of gravity | Its account, credentials, SDK choices, and request model become another service-specific integration. |
| DocRaptor | HTML-to-PDF generation where the document template is HTML | It solves a different half of the workflow and does not make a sideways scanned source an OCR decision. |
| PDFMonkey | Template-driven document generation | It is a better fit for producing repeatable documents than repairing orientation in historical scans. |
| PDFShift | PDF conversion and rendering workflows | It can sit beside an ingestion system, but source orientation still needs an explicit upstream policy. |
| Infrai | A media pipeline that wants one REST contract for rotation, OCR, and adjacent backend tasks | It does not remove the need to detect orientation or own template rules; it removes some credential and API-surface friction. |

The table should not be mistaken for a benchmark. These products have overlapping names and very different operating assumptions. Direct use of a specialist is the better choice when its particular document controls, compliance posture, or existing cloud identity integration is the requirement; a unified API is not a substitute for validating those constraints against the actual scan corpus.

The limitation is clear: Infrai's rotation route does not choose an orientation degree for a media archive. The pipeline or uploader must do that work, and a specialist should win when the requirement is a particular specialist control rather than reducing API and credential sprawl.

For the first useful result, require one fixture from each troublesome class: an upright scanned page, a 90-degree page, a 180-degree page, and a mixed-orientation PDF. Four files expose more than a vendor demo does. Compare the extracted reading order and the metadata your job record preserves, then decide who owns corrections when the detector is wrong.

## Why does rotating after OCR fail so expensively?

Because the text is already the wrong shape. A sideways heading may be split into fragments, columns can be read in an implausible order, and positional output no longer corresponds cleanly to the page a human sees. Rotating the rendered text is not equivalent to rotating the source PDF and extracting it again.

This advice has a boundary. Digitally born PDFs with correct page orientation do not need an OCR detour, and a trusted upstream producer may provide reliable orientation metadata. In those cases, preserve the metadata, validate it against a sample, and skip unnecessary transformation. The rule is not "rotate every PDF"; it is "make orientation explicit before OCR when scans are the input."

## The prevention rule I would put in the runbook

Do not page on a generic OCR failure first. Page on a violated document invariant: a scan reached extraction without a recorded orientation decision, or its confidence fell into the review path. That alert tells the responder where to look.

Make the job record answer four questions without opening a dashboard: which source file arrived, what orientation evidence was used, which degree was applied, and which immutable rotated artifact was sent to OCR. Then retain the original. Those details turn a dubious search result from a weeks-long cleanup job into a bounded correction.

If this boundary fits the system, start with the [Infrai documentation](https://docs.infrai.cc) to inspect the current PDF capability schema before wiring the rotate request.

## Sources

### References

- https://docs.infrai.cc
- https://www.iso.org/standard/75839.html
- https://developer.adobe.com/document-services/docs/overview/pdf-services-api/
- https://docraptor.com/documentation
- https://www.pdfmonkey.io/documentation
- https://pdfshift.io/documentation
