# Review Cost Metrics and Observability

## Motivation

We track response cache hit/miss/token stats per review, but that's only
one dimension of the cost picture.  To understand the true cost of a review
and identify optimization opportunities, we need visibility into:

- **How much does each review cost?** — total tokens, estimated dollar cost
- **Where does the time go?** — LLM latency vs git ops vs worktree setup
- **How reliable is the pipeline?** — retry rates, failure modes, rate limits
- **What drives cost variation?** — prompt composition, diff size, context size

This data lets us answer practical questions:
- "How much did reviewing this 28-patch series cost in API credits?"
- "Are we spending more on system prompt overhead or diff context?"
- "How often do rate limits add latency to reviews?"
- "Which patches are outliers in cost, and why?"

## Current State

What we already track per review:

- `ai_interactions` table (`src/schema.sql:135-147`):
  - `tokens_in`, `tokens_out`, `tokens_cached` per LLM call
  - `provider`, `model`
  - `created_at` timestamp
  - `input_context`, `output_raw` (full payloads)

- `reviews` table: `cache_hits`, `cache_misses`, `cache_tokens_saved`,
  `cache_tokens_stored` (added in this branch)

- `AiUsage` struct (`src/ai/mod.rs:277-287`):
  `prompt_tokens`, `completion_tokens`, `total_tokens`, `cached_tokens`

What we **don't** track:

- Wall-clock time for the LLM call (API latency)
- Wall-clock time for the full review pipeline (git + worktree + LLM + DB)
- Retry count and retry reasons (rate limit vs transient error)
- Prompt composition breakdown (system vs diff vs context tokens)
- Estimated cost in currency (provider-specific pricing)
- Aggregated cost per patchset

## Design

### Phase 1: Per-Review Timing and Retry Tracking

Add columns to the `reviews` table:

```sql
ALTER TABLE reviews ADD COLUMN llm_duration_ms INTEGER;     -- LLM API call time
ALTER TABLE reviews ADD COLUMN total_duration_ms INTEGER;    -- full pipeline time
ALTER TABLE reviews ADD COLUMN retry_count INTEGER DEFAULT 0;
ALTER TABLE reviews ADD COLUMN rate_limit_count INTEGER DEFAULT 0;
```

**Implementation in `process_patch_review`** (`src/reviewer.rs:1038`):

1. Record `pipeline_start = Instant::now()` at function entry
2. Record `llm_start`/`llm_end` around `run_review_tool()` call
3. Track retries: increment `retry_count` on each retry loop iteration,
   `rate_limit_count` specifically for `AiErrorClass::RateLimit`
4. Pass all four values to `complete_review()`

**Display**:
- CLI `show`: add `{1.2s LLM, 3.4s total}` after cache stats on each patch
- CLI `show`: patchset-level summary: `Timing: 45.2s LLM, 78.3s total, 2 retries`
- Web UI: add timing to the expandable stats panel
- API: include in review JSON response

### Phase 2: Prompt Composition Breakdown

Track how the token budget is distributed across prompt components.
Add to `ai_interactions`:

```sql
ALTER TABLE ai_interactions ADD COLUMN tokens_system INTEGER;
ALTER TABLE ai_interactions ADD COLUMN tokens_diff INTEGER;
ALTER TABLE ai_interactions ADD COLUMN tokens_context INTEGER;
```

**Implementation in `run_review_tool`** or the review binary:

The review binary constructs the prompt from:
- System instructions (review guidelines, tool definitions)
- Diff content (the actual patch)
- File context (surrounding code, baseline files)
- Known findings (future, from the knowledge base plan)

Use the provider's `estimate_tokens()` on each component before assembling
the full prompt.  Store the breakdown in `ai_interactions`.

**Display**:
- CLI `show --verbose`: per-patch token breakdown pie
- Web UI: stacked bar in the expandable stats panel
- Useful for identifying patches where context dwarfs the diff

### Phase 3: Cost Estimation

Add provider-specific pricing to `ProviderCapabilities` (`src/ai/mod.rs`):

```rust
pub struct ProviderCapabilities {
    pub model_name: String,
    pub supports_tools: bool,
    pub supports_json_mode: bool,
    pub supports_system: bool,
    pub input_cost_per_mtok: Option<f64>,   // $/1M input tokens
    pub output_cost_per_mtok: Option<f64>,  // $/1M output tokens
    pub cached_cost_per_mtok: Option<f64>,  // $/1M cached input tokens
}
```

Populate from known pricing for each provider (Gemini, Claude, etc.).
Allow override in Settings.toml for custom/enterprise pricing:

```toml
[ai.pricing]
input_cost_per_mtok = 1.25
output_cost_per_mtok = 5.00
cached_cost_per_mtok = 0.31
```

**Estimated cost per review**:
```
cost = (tokens_in - tokens_cached) * input_rate
     + tokens_cached * cached_rate
     + tokens_out * output_rate
```

**Display**:
- CLI `show`: per-patch `{$0.12}` and patchset total `Cost: $3.47`
- CLI `show`: also show what it would have cost without cache: `$14.20 without cache`
- Web UI: cost column in patch table, total in header
- API: `estimated_cost_usd` field in review JSON

### Phase 4: Patchset-Level Aggregation

Add a `patchset_metrics` view or materialized summary:

```sql
CREATE VIEW patchset_metrics AS
SELECT
    ps.id AS patchset_id,
    COUNT(r.id) AS review_count,
    SUM(ai.tokens_in) AS total_tokens_in,
    SUM(ai.tokens_out) AS total_tokens_out,
    SUM(ai.tokens_cached) AS total_tokens_cached,
    SUM(r.cache_tokens_saved) AS total_cache_tokens_saved,
    SUM(r.llm_duration_ms) AS total_llm_ms,
    SUM(r.total_duration_ms) AS total_pipeline_ms,
    SUM(r.retry_count) AS total_retries,
    SUM(r.rate_limit_count) AS total_rate_limits
FROM patchsets ps
JOIN reviews r ON r.patchset_id = ps.id
LEFT JOIN ai_interactions ai ON r.interaction_id = ai.id
GROUP BY ps.id;
```

This powers the patchset-level summary lines in CLI and web UI without
re-querying individual reviews.

### Phase 5: Historical Dashboard Data

Add an API endpoint that returns cost trends over time:

```
GET /api/metrics/cost?days=30
```

Returns daily aggregates: total tokens, estimated cost, cache savings,
retry rate, average review duration.  This powers a future dashboard
showing cost trends and cache effectiveness over time.

## Files to Modify

- `src/schema.sql` — new columns on `reviews`, `ai_interactions`; view
- `src/db.rs` — migration, updated `complete_review()` signature, aggregation queries
- `src/reviewer.rs` — timing instrumentation, retry counting
- `src/ai/mod.rs` — pricing fields in `ProviderCapabilities`
- `src/ai/claude.rs`, `gemini.rs`, `openai.rs`, `bedrock.rs` — populate pricing
- `src/settings.rs` — optional pricing override in config
- `src/bin/sashiko-cli.rs` — display timing and cost in `show`
- `src/api.rs` — cost fields in responses, metrics endpoint
- `src/web/templates/` — timing and cost in expandable panels

## Verification

1. Run a review, verify `llm_duration_ms` and `total_duration_ms` are populated
2. Trigger a rate limit (or simulate), verify `rate_limit_count` increments
3. `sashiko-cli show` displays timing and cost per patch and aggregate
4. Web UI shows timing in the expandable stats panel
5. Cost estimates match manual calculation from token counts
6. All existing tests pass, new tests for timing and cost calculation
