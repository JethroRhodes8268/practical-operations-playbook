# Vector Search Explained: Grounded Go Pipelines for Help Center Incidents

A small e-commerce SaaS should evaluate pgvector and a hosted vector API by testing which option makes a bad help-center ingestion easy to detect, replay, and reverse. **TL;DR:** put both paths through the same corpus publication, citation, failure, and rollback exercise before comparing their total operational burden. The Postgres extension path keeps index work in the application database; a hosted API moves it across another service boundary. Neither category is inherently cheapest once recovery work, citation failures, and on-call ownership enter the calculation.

Retrieval-augmented generation couples a retriever with generation so an answer can draw on external evidence rather than model parameters alone. For a storefront help center, the operational unit is therefore not merely an embedding. It is a traceable passage tied to the article, revision, locale, and source location that a customer can inspect. If those fields disappear during chunking, a fast nearest-neighbor result can still produce an answer that support cannot defend.

The bill is one line item. The page that fires at 3am is another.

## What should page when retrieval goes wrong?

A dashboard full of green averages does not answer the incident question. What page fired? A useful alert should correspond to customer-visible loss of grounding: a sharp rise in answers with no eligible citation, an ingestion revision that never becomes queryable, or retrieval that returns a passage from the wrong locale or an unpublished article. CPU, index size, and request latency remain useful diagnostic signals, but none proves that cited evidence supports the answer.

Start with a canary set drawn from the actual help center: shipping cutoffs, return windows, subscription cancellation, warranty limits, and a deliberately unpublished draft. Each canary needs an expected article identifier and revision, plus forbidden identifiers where the distinction matters. Run it after every corpus publication and on a schedule. Alert on failed grounding checks; attach the query, corpus revision, eligible filters, returned passage identifiers, and ingestion run identifier to the event. A responder should not have to reconstruct these from three dashboards.

Keep the generation layer out of the first diagnostic pass. For each canary, inspect retrieved passages and source metadata before judging prose. This separates a retrieval failure from an answer-synthesis failure and exposes the awkward case in which an answer sounds right while citing a stale policy.

No citation, no answer.

That refusal rule has a product cost: some customers receive a bounded fallback instead of fluent prose. The alternative is harder to defend. A response about refunds or delivery promises should fail closed when the system cannot produce eligible evidence, because confident unsupported text moves the incident from search quality into customer trust.

## Treat ingestion as a publication, not an upload

Chunking should produce an immutable candidate revision before it changes live retrieval. Parse the source, normalize it, split it into passages, assign stable identifiers, preserve source locations, compute embeddings, and validate the completed candidate. Only then should an atomic publication pointer make that revision eligible for queries. An interrupted run stays invisible; a retry writes the same logical passage identifiers instead of multiplying documents.

A compact record can carry the evidence chain without binding the application to an index implementation:

```go
type Passage struct {
	ID              string
	ArticleID       string
	ArticleRevision string
	CorpusRevision  string
	Locale          string
	SourceURL       string
	Heading         string
	Ordinal         int
	Content         string
	ContentHash     string
	Published       bool
	Embedding       []float32
}

type CandidateIndex interface {
	Put(ctx context.Context, passages []Passage) error
	Validate(ctx context.Context, corpusRevision string) error
	Publish(ctx context.Context, corpusRevision string) error
	Retire(ctx context.Context, corpusRevision string) error
}
```

The fields do more work than they first appear to. `ArticleRevision` lets a citation identify the policy text that was retrieved. `CorpusRevision` creates a rollback boundary across many articles. `Ordinal` and `Heading` reconstruct a readable location. `ContentHash` makes retries and change detection explicit. `Published` prevents a draft from becoming eligible merely because its vector exists.

Chunk boundaries deserve suspicion because they determine what a citation means. A passage that joins the end of a return-policy section to the beginning of an unrelated warranty section may look relevant while being impossible to quote honestly. Preserve heading boundaries where possible, retain enough overlap to avoid separating a condition from its rule, and reject chunks that cannot point back to one coherent source location. There is no universal chunk size supported by the cited research; measure with the help-center documents and questions that matter.

## Should managed vector search power a small SaaS help center?

The decisive comparison is the shape of the failure domain, not a feature checklist.

| Decision boundary | Postgres extension path | Hosted API path |
| --- | --- | --- |
| Publication metadata | Can share relational transactions and joins | Must remain consistent across a service boundary |
| Resource isolation | Retrieval can compete with database work | Index work is outside the application database |
| Operator responsibility | Team owns capacity, maintenance, and recovery | Team owns integration, credentials, and provider failure handling |
| Grounding duty | Application-owned | Application-owned |

With the pgvector path, vector records can live beside relational metadata. That may simplify publication state and citation joins when Postgres is already an understood dependency. Its limitation is shared fate: ingestion and retrieval consume an operational system whose transactional workloads may be more important. Capacity limits, maintenance, backups, query contention, and recovery remain the team's responsibility. It is not suitable when the team cannot bound that contention or restore the index confidently.

A hosted vector API moves index operation behind a service boundary. This can reduce the machinery the application team directly runs, but the trade-off is network behavior, external limits, credential handling, and a second data model that must stay consistent with the canonical help-center revision. It is not suitable when policy forbids sending the indexed content across that boundary, or when the team cannot test degraded service behavior. The application still owns chunk correctness, metadata completeness, publication semantics, grounding tests, and the answer shown to a customer. Outsourcing the index does not outsource evidence.

Use the same narrow interface for both paths:

```go
type Filter struct {
	CorpusRevision string
	Locale         string
	PublishedOnly  bool
}

type Hit struct {
	PassageID string
	Score     float32
}

type VectorStore interface {
	Upsert(ctx context.Context, passages []Passage) error
	Search(ctx context.Context, vector []float32, filter Filter, limit int) ([]Hit, error)
	DeleteRevision(ctx context.Context, corpusRevision string) error
}
```

This interface is intentionally dull. It does not pretend that score scales are portable, nor does it hide publication state in an untyped map. An implementation may translate the filter into SQL or a hosted request, while the caller applies the same eligibility contract and records the same incident context. Avoid a lowest-common-denominator abstraction for every imaginable feature; the boundary only needs to protect the help-center invariants that must survive a change.

Run both candidates against one replayable corpus snapshot and one canary suite. Record end-to-end publication time, retrieval latency under expected concurrency, citation eligibility failures, recovery steps, and the operator time required to explain a miss. Then account for service charges where applicable, database capacity, backups, observability, upgrades, and on-call work. A small invoice does not compensate for an index nobody can restore, and spare database capacity is not free once retrieval creates contention.

## Build a verification trail the responder can replay

Every answer attempt should emit a structured trace with a request identifier, query hash or suitably protected query reference, active corpus revision, locale, filters, retrieved passage identifiers, cited passage identifiers, timings, and final disposition such as answered or refused. Do not log raw customer text by default; help-center search can contain account details or order information. Retention and access should follow the application's data-handling rules.

Verification needs layers. Unit tests cover stable chunk identifiers, metadata preservation, and filter construction. Integration tests publish a small candidate corpus through the real adapter and confirm that an interrupted candidate is not searchable. Retrieval evaluation checks that known questions surface eligible evidence. End-to-end tests confirm that an answer cites only passages returned for the active revision and refuses when none qualify.

One metric cannot carry this. Recall-oriented retrieval measures can reveal missing evidence, while citation checks reveal whether the answer points to retrieved, eligible source material. Latency distributions show customer impact, and publication lag shows whether a new help-center revision has reached search. Look at them together, split by locale and corpus revision, because an aggregate can stay calm while one newly published policy is absent.

The deployment sequence should be boring: create a candidate revision, run structural validation, execute canaries, shadow representative queries if policy permits, publish the revision, then watch both grounding failures and resource pressure. Keep the previous revision queryable throughout the observation window. Destructive cleanup comes later.

## Roll back the corpus before debugging the prose

When a release causes citation failures, move the publication pointer back to the last validated corpus revision. Do not start by tuning prompts against a broken or partial index. Stop the failing ingestion run, preserve its manifest and traces, and compare missing or unexpected passage identifiers against the source revision. If storage cannot switch revisions atomically, route reads through application-owned publication metadata so incomplete candidates remain ineligible.

Rollback must be rehearsed with the chosen storage adapter. Measure how long it takes for the previous revision to serve queries again, verify canaries after the switch, and confirm that caches do not continue serving the retired revision. The two storage categories may require different mechanics, but the externally visible acceptance test stays fixed: the active corpus identifier matches the intended revision, every test answer cites eligible passages from it, and the unpublished draft remains absent.

Only after service is restored should the team diagnose whether parsing, chunking, embedding, filtering, publication, or generation introduced the fault. The postmortem should name the missing control and add a test or alert at that boundary. Do not settle for `bad search results` as a cause.

The practical decision is conditional. Existing Postgres competence and controlled contention favor the extension path; a need to isolate index operations can favor the hosted path, provided its data boundary and degraded modes are acceptable. Immutable corpus revisions, explicit eligibility filters, canary questions, citation refusal, and a tested rollback are what make either choice supportable.

## References

- https://arxiv.org/abs/2005.11401
