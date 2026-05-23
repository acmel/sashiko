# Pre-existing Findings Knowledge Base

## Motivation

When sashiko reviews a patch series, the LLM frequently discovers issues
in the surrounding code that pre-date the patch — buffer overflows, missing
error checks, logic bugs in functions the patch merely calls or modifies
nearby.  These findings are valuable: they represent real bugs that a human
reviewer should know about.

Today, these pre-existing findings are stored in the `findings` table with
`preexisting = 1`, surfaced in the review output, and then effectively
forgotten.  If the same file is touched in a future patch series, sashiko
rediscovers the same issues from scratch — spending the same tokens, the
same API latency, the same cost.  Worse, different sashiko instances
reviewing the same subsystem will independently discover the same pre-existing
bugs, multiplying the waste.

The goal is to **capture the tokens we already invested** in discovering
pre-existing issues, build a reusable knowledge base from them, and inject
that knowledge into future reviews so the LLM can:

1. Skip rediscovering known issues (saving tokens and time)
2. Distinguish known pre-existing problems from new ones more reliably
3. Focus its analysis budget on what the patch actually changes

This also opens the door to **sharing findings across instances** — a team
reviewing kernel patches could export their pre-existing findings database
so that others benefit from the collective analysis without repeating
expensive LLM calls.

## Current State

- `findings` table (`src/schema.sql:121-131`) already has:
  - `preexisting INTEGER` — 0/1 flag set by the LLM during review
  - `problem TEXT` — description of the issue
  - `severity INTEGER` — Low(1)/Medium(2)/High(3)/Critical(4)
  - `locations TEXT` — JSON with file paths and line numbers
  - `review_id` → links to `reviews` → `patches` → `patchsets`

- `Finding` struct (`src/db.rs:138-145`) mirrors this schema

- `process_patch_review` (`src/reviewer.rs:1317-1343`) already parses
  the `preexisting` flag from LLM output and stores it

- Review notifications (`src/reviewer.rs:1372-1475`) already separate
  pre-existing from new findings for email reporting

## Design

### Phase 1: Knowledge Base Table and Deduplication

Create a `known_findings` table that accumulates unique pre-existing
findings, deduplicated across reviews:

```sql
CREATE TABLE IF NOT EXISTS known_findings (
    id INTEGER PRIMARY KEY,
    fingerprint TEXT UNIQUE NOT NULL,  -- hash of normalized problem + location
    file_path TEXT NOT NULL,           -- primary file where issue was found
    function_name TEXT,                -- function scope, if identifiable
    problem TEXT NOT NULL,             -- issue description (from LLM)
    severity INTEGER NOT NULL,        -- severity level
    suggestion TEXT,                   -- fix suggestion (from LLM)
    first_seen_review_id INTEGER,     -- review that first discovered this
    first_seen_at INTEGER NOT NULL,   -- unix timestamp
    last_seen_review_id INTEGER,      -- most recent review confirming it
    last_seen_at INTEGER NOT NULL,    -- unix timestamp
    times_seen INTEGER DEFAULT 1,     -- how many reviews found this
    status TEXT DEFAULT 'open',       -- open, fixed, false_positive, wont_fix
    tokens_spent INTEGER DEFAULT 0,   -- cumulative tokens spent rediscovering
    FOREIGN KEY(first_seen_review_id) REFERENCES reviews(id),
    FOREIGN KEY(last_seen_review_id) REFERENCES reviews(id)
);
```

**Fingerprinting**: hash of `(normalized_file_path, function_name, normalized_problem)`.
The normalization strips line numbers (which shift between versions) and
lowercases/trims the problem text.  Two findings about "missing NULL check
in perf_event__process_compress()" from different reviews should collapse
to one known_finding.

**Population**: after `process_patch_review` stores findings, a post-step
inserts or updates `known_findings` for each finding with `preexisting = 1`.
On insert: record `first_seen_*` fields.  On conflict (same fingerprint):
update `last_seen_*`, increment `times_seen`, accumulate `tokens_spent`.

### Phase 2: Inject Known Findings into Reviews

Before calling the LLM for a patch review, query `known_findings` for any
open findings matching the files touched by the patch:

```sql
SELECT problem, severity, suggestion, file_path, function_name, times_seen
FROM known_findings
WHERE status = 'open'
  AND file_path IN (... files touched by patch ...)
ORDER BY severity DESC, times_seen DESC
```

Inject these as a structured context block in the review prompt:

```
## Known Pre-existing Issues in Touched Files

The following issues have been previously identified in files this patch
touches.  They pre-date this patch.  Do NOT report them as new findings.
If the patch fixes any of them, note that in the summary.

1. [High] perf_event__process_compress(): missing bounds check on
   compressed data size (seen 4 times across reviews)
2. [Medium] perf_session__process_event(): unchecked return value from
   decompress_buffer() (seen 2 times)
```

This serves two purposes:
- **Saves tokens**: the LLM doesn't need to rediscover these issues
- **Improves accuracy**: explicit "do not re-report" instructions reduce
  false positives where pre-existing issues get flagged as patch-introduced

### Phase 3: Feedback Loop — Detecting Fixes

When a known finding is injected into a review and the LLM reports that
the patch *fixes* it, update the finding's status to `fixed` and record
which patchset/review resolved it.  This keeps the knowledge base current
and prevents stale findings from being injected forever.

Similarly, if a human marks a finding as `false_positive` or `wont_fix`
via the CLI or API, stop injecting it.

### Phase 4: Export/Import for Sharing

Add CLI commands and API endpoints for exporting and importing the
knowledge base:

```
sashiko-cli findings export --format json > known-findings.json
sashiko-cli findings import known-findings.json
```

The export format includes fingerprints, so importing into a different
instance deduplicates automatically.  This lets teams share their
accumulated knowledge — one person's review investment benefits everyone.

A future enhancement could be a shared findings server that instances
push to and pull from, but the file-based export/import is the pragmatic
first step.

### Phase 5: Token Savings Tracking

Track how many tokens the knowledge base saves per review:

- When injecting known findings, count tokens in the injected context
- When the review completes without rediscovering those issues, credit
  the estimated savings (previous token cost for similar findings)
- Surface this in the same cache stats UI: "Known findings: 3 injected,
  ~15k tokens saved"

This closes the observability loop: you can see not just response cache
savings but also knowledge base savings.

## Files to Modify

- `src/schema.sql` — add `known_findings` table
- `src/db.rs` — add `KnownFinding` struct, CRUD methods, query by file paths
- `src/reviewer.rs` — post-review population step, pre-review injection
- `src/bin/review.rs` — accept known findings in input payload, format prompt
- `src/bin/sashiko-cli.rs` — `findings` subcommand (list, export, import, mark)
- `src/api.rs` — endpoints for findings management
- `src/web/templates/` — findings display in web UI

## Verification

1. Run a review on a patch touching files with known pre-existing issues
2. Verify findings are stored in `known_findings` with correct fingerprints
3. Run a second review on a patch touching the same files
4. Verify known findings are injected into the prompt
5. Verify token usage is lower on the second review
6. Verify `sashiko-cli findings export | sashiko-cli findings import` roundtrips
7. All existing tests pass, new integration tests for dedup and injection
