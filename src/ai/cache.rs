use anyhow::Result;
use async_trait::async_trait;
use regex::Regex;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, info};

use super::{AiProvider, AiRequest, AiResponse, CacheStats, ProviderCapabilities};

pub fn fmt_thousands(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            result.push('.');
        }
        result.push(c);
    }
    result
}

pub fn fmt_bytes(n: u64) -> String {
    if n >= 1_073_741_824 {
        format!("{:.1} GB", n as f64 / 1_073_741_824.0)
    } else if n >= 1_048_576 {
        format!("{:.1} MB", n as f64 / 1_048_576.0)
    } else if n >= 1_024 {
        format!("{:.1} KB", n as f64 / 1_024.0)
    } else {
        format!("{} B", n)
    }
}

pub fn fmt_tokens(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}k", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

/// Strip dates, timestamps, and commit hashes from the serialized request
/// so that re-reviews of the same logical patch produce the same cache key.
/// The actual request sent to the LLM is unchanged — this only affects hashing.
fn scrub_nondeterministic_content(s: &str) -> String {
    use std::sync::LazyLock;

    // "the current date is Wednesday, June 02, 2026" — daily date in system prompt
    static RE_DATE_FACT: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"the current date is [A-Z][a-z]+, [A-Z][a-z]+ \d{2}, \d{4}").unwrap()
    });
    // "commit <40-hex SHA>" line from git show output
    static RE_COMMIT: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"commit [0-9a-f]{40}").unwrap());
    // "Date:   <anything>" line from git show output (committer date)
    static RE_DATE_LINE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"Date:\s+[^\n\\]+").unwrap());

    let result = RE_DATE_FACT.replace_all(s, "the current date is <DATE>");
    let result = RE_COMMIT.replace_all(&result, "commit <HASH>");
    RE_DATE_LINE
        .replace_all(&result, "Date: <DATE>")
        .into_owned()
}

pub struct CachingAiProvider {
    inner: Arc<dyn AiProvider>,
    conn: libsql::Connection,
    cache_path: String,
    max_entries: u64,
    max_size_mb: u64,
    session_start: i64,
    hits_this: AtomicU64,
    hits_prev: AtomicU64,
    tokens_saved_this: AtomicU64,
    tokens_saved_prev: AtomicU64,
    misses: AtomicU64,
    tokens_stored: AtomicU64,
}

impl CachingAiProvider {
    pub async fn new(
        inner: Arc<dyn AiProvider>,
        cache_path: &str,
        ttl_days: u64,
        max_entries: u64,
        max_size_mb: u64,
    ) -> Result<Self> {
        let file_size = std::fs::metadata(cache_path).map(|m| m.len()).unwrap_or(0);
        info!(
            "Opening response cache ({}, {}), this may take a moment...",
            cache_path,
            fmt_bytes(file_size)
        );
        let start = Instant::now();
        let db = libsql::Builder::new_local(cache_path).build().await?;
        let conn = db.connect()?;

        let _ = conn
            .query("PRAGMA journal_mode=WAL;", ())
            .await?
            .next()
            .await;
        let _ = conn
            .query("PRAGMA busy_timeout = 5000;", ())
            .await?
            .next()
            .await;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS response_cache (
                request_hash TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                model TEXT NOT NULL,
                request_json TEXT NOT NULL,
                response_json TEXT NOT NULL,
                tokens_saved INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL
            );",
        )
        .await?;

        // Schema migration: silently ignore "duplicate column" errors on already-migrated DBs
        let _ = conn
            .execute(
                "ALTER TABLE response_cache ADD COLUMN hit_count INTEGER NOT NULL DEFAULT 0",
                (),
            )
            .await;
        let _ = conn
            .execute(
                "ALTER TABLE response_cache ADD COLUMN last_accessed_at INTEGER NOT NULL DEFAULT 0",
                (),
            )
            .await;
        let _ = conn
            .execute(
                "UPDATE response_cache SET last_accessed_at = created_at WHERE last_accessed_at = 0",
                (),
            )
            .await;
        let _ = conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_cache_eviction ON response_cache(hit_count, last_accessed_at)",
                (),
            )
            .await;

        let cutoff = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
            - ttl_days as i64 * 86400;
        let result = conn
            .execute(
                "DELETE FROM response_cache WHERE created_at < ?",
                libsql::params![cutoff],
            )
            .await;
        if let Ok(reaped) = result
            && reaped > 0
        {
            info!(
                "Response cache: reaped {} expired entries (>{} days old)",
                reaped, ttl_days
            );
        }

        let mut total_entries: u64 = 0;
        // tokens_stored = SUM(tokens_saved) — total tokens across all unique entries,
        // i.e. potential savings if each entry is hit once.
        let mut total_tokens_stored: u64 = 0;
        if let Ok(Some(row)) = conn
            .query(
                "SELECT COUNT(*), COALESCE(SUM(tokens_saved), 0) FROM response_cache",
                (),
            )
            .await?
            .next()
            .await
        {
            total_entries = row.get::<u64>(0).unwrap_or(0);
            total_tokens_stored = row.get::<u64>(1).unwrap_or(0);
        }

        let session_start = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        info!(
            "Response cache ready: {} entries, {} tokens stored, {:.2}s to load",
            fmt_thousands(total_entries),
            fmt_thousands(total_tokens_stored),
            start.elapsed().as_secs_f64()
        );

        Ok(Self {
            inner,
            conn,
            cache_path: cache_path.to_string(),
            max_entries,
            max_size_mb,
            session_start,
            hits_this: AtomicU64::new(0),
            hits_prev: AtomicU64::new(0),
            tokens_saved_this: AtomicU64::new(0),
            tokens_saved_prev: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            tokens_stored: AtomicU64::new(0),
        })
    }

    /// Compute a SHA-256 cache key from the AI request, normalizing away fields
    /// that vary across re-reviews of the same logical patch but don't change the
    /// AI's analysis.
    ///
    /// ## What's INCLUDED in the cache key (semantic content):
    /// - System prompt text (minus scrubbed portions below)
    /// - All messages (user, assistant, tool results) — the full conversation
    /// - Tool declarations (names, descriptions, parameters)
    /// - Temperature and response format settings
    ///
    /// ## What's EXCLUDED from the cache key (nondeterministic noise):
    /// - `context_tag` — logging label like `[ps:123 p:1 s:4]`, varies per patchset
    /// - `thought_signature` / `thoughtSignature` — opaque provider tokens that
    ///   change across sessions (scrubbed recursively from all nested messages)
    /// - Daily date string — `"the current date is Wednesday, June 02, 2026"` in
    ///   the system prompt changes every day; stripped via regex
    /// - Commit hashes — `"commit <40-hex>"` lines from `git show` output embedded
    ///   in the system prompt; re-applying the same patch via `git am` produces a
    ///   different hash (different committer timestamp), but the diff is identical
    /// - Committer date — `"Date:   <timestamp>"` lines from `git show` output;
    ///   same reason as commit hashes
    ///
    /// ## Cache hit expectations:
    /// - Same patch re-reviewed (even days later): turn 1 of each stage hits cache
    /// - Same patch, turns 2+: unlikely — model's tool call choices diverge
    /// - Different patches: never — the diff content differs in the system prompt
    /// - Stages 8-11: almost never — input aggregates prior stages' AI output
    fn compute_cache_key(request: &AiRequest) -> String {
        let mut val = serde_json::to_value(request).unwrap_or_default();
        if let serde_json::Value::Object(ref mut map) = val {
            map.remove("context_tag");
        }
        super::scrub_thought_signatures(&mut val);
        let mut canonical = serde_json::to_string(&val).unwrap_or_default();
        canonical = scrub_nondeterministic_content(&canonical);
        let hash = Sha256::digest(canonical.as_bytes());
        hash.iter().map(|b| format!("{:02x}", b)).collect()
    }

    // Evict lowest-value entries when cache exceeds configured limits.
    // Eviction order: lowest hit_count first, oldest last_accessed_at as tiebreaker.
    async fn maybe_evict(&self) {
        if self.max_entries > 0
            && let Ok(count) = self.get_entry_count().await
            && count > self.max_entries
        {
            let excess = count - self.max_entries;
            let evicted = self
                .conn
                .execute(
                    "DELETE FROM response_cache WHERE request_hash IN (
                        SELECT request_hash FROM response_cache
                        ORDER BY hit_count ASC, last_accessed_at ASC
                        LIMIT ?
                    )",
                    libsql::params![excess as i64],
                )
                .await
                .unwrap_or(0);
            if evicted > 0 {
                info!(
                    "Cache eviction: removed {} entries (entry limit {})",
                    evicted, self.max_entries
                );
            }
        }

        if self.max_size_mb > 0 {
            let file_size = std::fs::metadata(&self.cache_path)
                .map(|m| m.len())
                .unwrap_or(0);
            let limit_bytes = self.max_size_mb * 1_048_576;
            if file_size > limit_bytes {
                // Evict 10% at a time — SQLite reuses freed pages internally so the
                // file won't shrink immediately, but it stops growing.
                let count = self.get_entry_count().await.unwrap_or(0);
                let batch = (count / 10).max(1);
                let evicted = self
                    .conn
                    .execute(
                        "DELETE FROM response_cache WHERE request_hash IN (
                            SELECT request_hash FROM response_cache
                            ORDER BY hit_count ASC, last_accessed_at ASC
                            LIMIT ?
                        )",
                        libsql::params![batch as i64],
                    )
                    .await
                    .unwrap_or(0);
                if evicted > 0 {
                    info!(
                        "Cache eviction: removed {} entries (file {} exceeds {} MB limit)",
                        evicted,
                        fmt_bytes(file_size),
                        self.max_size_mb
                    );
                }
            }
        }
    }

    async fn get_entry_count(&self) -> Result<u64> {
        let mut rows = self
            .conn
            .query("SELECT COUNT(*) FROM response_cache", ())
            .await?;
        if let Some(row) = rows.next().await? {
            Ok(row.get::<u64>(0).unwrap_or(0))
        } else {
            Ok(0)
        }
    }
}

#[async_trait]
impl AiProvider for CachingAiProvider {
    async fn generate_content(&self, request: AiRequest) -> Result<AiResponse> {
        let hash = Self::compute_cache_key(&request);
        let hash_prefix = &hash[..12];

        let mut rows = self
            .conn
            .query(
                "SELECT response_json, tokens_saved, created_at FROM response_cache WHERE request_hash = ?",
                libsql::params![hash.clone()],
            )
            .await?;

        if let Some(row) = rows.next().await? {
            let response_json: String = row.get(0)?;
            let tokens_saved: i64 = row.get(1)?;
            let created_at: i64 = row.get(2)?;
            if let Ok(mut resp) = serde_json::from_str::<AiResponse>(&response_json) {
                // Evict poisoned entries: empty responses that were cached before
                // this guard existed.  Without eviction they replay on every retry,
                // turning a transient AI failure into a permanent one.
                let has_content = resp.content.as_ref().is_some_and(|c| !c.trim().is_empty());
                let has_tool_calls = resp.tool_calls.as_ref().is_some_and(|tc| !tc.is_empty());
                if !has_content && !has_tool_calls {
                    debug!(
                        "Evicting poisoned cache entry [{}]: no content or tool calls",
                        hash_prefix
                    );
                    let _ = self
                        .conn
                        .execute(
                            "DELETE FROM response_cache WHERE request_hash = ?",
                            libsql::params![hash.clone()],
                        )
                        .await;
                    // Fall through to cache miss path below
                } else {
                    let (origin, total) = if created_at >= self.session_start {
                        self.hits_this.fetch_add(1, Ordering::Relaxed);
                        let t = self
                            .tokens_saved_this
                            .fetch_add(tokens_saved as u64, Ordering::Relaxed)
                            + tokens_saved as u64;
                        ("this session", t)
                    } else {
                        self.hits_prev.fetch_add(1, Ordering::Relaxed);
                        let t = self
                            .tokens_saved_prev
                            .fetch_add(tokens_saved as u64, Ordering::Relaxed)
                            + tokens_saved as u64;
                        ("previous session", t)
                    };
                    info!(
                        "Cache hit [{}] ({}) — {} tokens saved (total {}: {})",
                        hash_prefix,
                        origin,
                        fmt_thousands(tokens_saved as u64),
                        origin,
                        fmt_thousands(total)
                    );
                    // Fire-and-forget: don't let bookkeeping failures break the cache hit path
                    let now_hit = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as i64;
                    let _ = self
                        .conn
                        .execute(
                            "UPDATE response_cache SET hit_count = hit_count + 1, last_accessed_at = ? WHERE request_hash = ?",
                            libsql::params![now_hit, hash.clone()],
                        )
                        .await;
                    if let Some(ref mut usage) = resp.usage {
                        usage.cached_tokens =
                            Some(usage.cached_tokens.unwrap_or(0) + usage.prompt_tokens);
                    }
                    resp.cache_key = Some(hash.clone());
                    return Ok(resp);
                }
            }
        }

        debug!("Cache miss [{}]", hash_prefix);
        self.misses.fetch_add(1, Ordering::Relaxed);

        let mut resp = self.inner.generate_content(request.clone()).await?;

        // Never cache empty responses — they poison retries (same hash →
        // same empty result on every attempt, turning a transient failure
        // into a permanent one).
        let has_content = resp.content.as_ref().is_some_and(|c| !c.trim().is_empty());
        let has_tool_calls = resp.tool_calls.as_ref().is_some_and(|tc| !tc.is_empty());
        if !has_content && !has_tool_calls {
            debug!(
                "Skipping cache store [{}]: response has no content or tool calls",
                hash_prefix
            );
            return Ok(resp);
        }

        let response_json = serde_json::to_string(&resp)?;
        let request_json = serde_json::to_string(&request)?;
        let caps = self.inner.get_capabilities();
        let tokens_saved = resp
            .usage
            .as_ref()
            .map(|u| u.prompt_tokens + u.completion_tokens)
            .unwrap_or(0);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let _ = self
            .conn
            .execute(
                "INSERT OR REPLACE INTO response_cache (request_hash, provider, model, request_json, response_json, tokens_saved, created_at, hit_count, last_accessed_at) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)",
                libsql::params![
                    hash.clone(),
                    caps.model_name.clone(),
                    caps.model_name,
                    request_json,
                    response_json,
                    tokens_saved as i64,
                    now,
                    now
                ],
            )
            .await;

        self.tokens_stored
            .fetch_add(tokens_saved as u64, Ordering::Relaxed);

        self.maybe_evict().await;

        resp.cache_key = Some(hash);
        Ok(resp)
    }

    async fn invalidate_cache_entry(&self, key: &str) {
        let prefix = &key[..key.len().min(12)];
        info!("Invalidating poisoned cache entry [{}]", prefix);
        let _ = self
            .conn
            .execute(
                "DELETE FROM response_cache WHERE request_hash = ?",
                libsql::params![key],
            )
            .await;
    }

    fn estimate_tokens(&self, request: &AiRequest) -> usize {
        self.inner.estimate_tokens(request)
    }

    fn get_capabilities(&self) -> ProviderCapabilities {
        self.inner.get_capabilities()
    }

    fn cache_stats(&self) -> Option<CacheStats> {
        Some(CacheStats {
            hits_this_session: self.hits_this.load(Ordering::Relaxed),
            hits_prev_session: self.hits_prev.load(Ordering::Relaxed),
            tokens_saved_this_session: self.tokens_saved_this.load(Ordering::Relaxed),
            tokens_saved_prev_session: self.tokens_saved_prev.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            tokens_stored: self.tokens_stored.load(Ordering::Relaxed),
        })
    }
}

// Free functions for querying the cache DB from the API layer.
// These take a raw connection so the API server doesn't need access to CachingAiProvider.

#[derive(Debug, Clone, serde::Serialize)]
pub struct CacheEntrySummary {
    pub request_hash: String,
    pub provider: String,
    pub model: String,
    pub context: String,
    pub tokens_saved: i64,
    pub hit_count: i64,
    pub created_at: i64,
    pub last_accessed_at: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CacheEntryDetail {
    pub request_hash: String,
    pub provider: String,
    pub model: String,
    pub context: String,
    pub tokens_saved: i64,
    pub hit_count: i64,
    pub created_at: i64,
    pub last_accessed_at: i64,
    pub request_json: serde_json::Value,
    pub response_json: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CacheOverview {
    pub total_entries: u64,
    /// Sum of tokens_saved * hit_count: actual tokens avoided by cache hits (lifetime).
    pub lifetime_tokens_saved: u64,
    /// Sum of tokens_saved: tokens that would be saved if every entry were hit once.
    pub tokens_stored: u64,
    /// Sum of hit_count across all entries (lifetime, all sessions).
    pub lifetime_hits: u64,
    pub db_size_bytes: u64,
    pub max_entries: u64,
    pub max_size_mb: u64,
    pub entries_by_model: Vec<ModelCount>,
    pub hit_distribution: Vec<HitBucket>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ModelCount {
    pub model: String,
    pub count: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct HitBucket {
    pub bucket: String,
    pub count: u64,
}

pub async fn query_cache_overview(
    conn: &libsql::Connection,
    cache_path: &str,
    max_entries: u64,
    max_size_mb: u64,
) -> Result<CacheOverview> {
    let mut totals = conn
        .query(
            "SELECT COUNT(*), COALESCE(SUM(tokens_saved), 0), COALESCE(SUM(hit_count), 0), \
             COALESCE(SUM(tokens_saved * hit_count), 0) FROM response_cache",
            (),
        )
        .await?;
    let (total_entries, tokens_stored, lifetime_hits, lifetime_tokens_saved) =
        if let Some(row) = totals.next().await? {
            (
                row.get::<u64>(0).unwrap_or(0),
                row.get::<u64>(1).unwrap_or(0),
                row.get::<u64>(2).unwrap_or(0),
                row.get::<u64>(3).unwrap_or(0),
            )
        } else {
            (0, 0, 0, 0)
        };

    let db_size_bytes = std::fs::metadata(cache_path).map(|m| m.len()).unwrap_or(0);

    let mut by_model = Vec::new();
    let mut rows = conn
        .query(
            "SELECT model, COUNT(*) FROM response_cache GROUP BY model ORDER BY COUNT(*) DESC",
            (),
        )
        .await?;
    while let Some(row) = rows.next().await? {
        by_model.push(ModelCount {
            model: row.get::<String>(0).unwrap_or_default(),
            count: row.get::<u64>(1).unwrap_or(0),
        });
    }

    // Bucket distribution: group entries by hit_count ranges
    let mut hit_dist = Vec::new();
    let mut rows = conn
        .query(
            "SELECT
                CASE
                    WHEN hit_count = 0 THEN '0'
                    WHEN hit_count BETWEEN 1 AND 5 THEN '1-5'
                    WHEN hit_count BETWEEN 6 AND 20 THEN '6-20'
                    ELSE '21+'
                END AS bucket,
                COUNT(*)
            FROM response_cache GROUP BY bucket ORDER BY MIN(hit_count)",
            (),
        )
        .await?;
    while let Some(row) = rows.next().await? {
        hit_dist.push(HitBucket {
            bucket: row.get::<String>(0).unwrap_or_default(),
            count: row.get::<u64>(1).unwrap_or(0),
        });
    }

    Ok(CacheOverview {
        total_entries,
        lifetime_tokens_saved,
        tokens_stored,
        lifetime_hits,
        db_size_bytes,
        max_entries,
        max_size_mb,
        entries_by_model: by_model,
        hit_distribution: hit_dist,
    })
}

pub async fn query_cache_entries(
    conn: &libsql::Connection,
    page: u32,
    per_page: u32,
    sort_by: &str,
    sort_order: &str,
) -> Result<(Vec<CacheEntrySummary>, u64)> {
    let mut count_rows = conn
        .query("SELECT COUNT(*) FROM response_cache", ())
        .await?;
    let total = if let Some(row) = count_rows.next().await? {
        row.get::<u64>(0).unwrap_or(0)
    } else {
        0
    };

    // Allowlist sort columns to prevent SQL injection
    let col = match sort_by {
        "tokens_saved" => "tokens_saved",
        "last_accessed_at" => "last_accessed_at",
        "created_at" => "created_at",
        _ => "hit_count",
    };
    let dir = if sort_order == "asc" { "ASC" } else { "DESC" };
    let offset = (page.saturating_sub(1)) * per_page;

    // json_extract pulls context_tag from the stored request for display;
    // it was stripped from the cache key but remains in the stored JSON.
    let sql = format!(
        "SELECT request_hash, provider, model, tokens_saved, hit_count, created_at, last_accessed_at, \
         COALESCE(json_extract(request_json, '$.context_tag'), '') \
         FROM response_cache ORDER BY {} {} LIMIT ? OFFSET ?",
        col, dir
    );
    let mut rows = conn
        .query(&sql, libsql::params![per_page as i64, offset as i64])
        .await?;

    let mut entries = Vec::new();
    while let Some(row) = rows.next().await? {
        entries.push(CacheEntrySummary {
            request_hash: row.get::<String>(0).unwrap_or_default(),
            provider: row.get::<String>(1).unwrap_or_default(),
            model: row.get::<String>(2).unwrap_or_default(),
            context: row.get::<String>(7).unwrap_or_default(),
            tokens_saved: row.get::<i64>(3).unwrap_or(0),
            hit_count: row.get::<i64>(4).unwrap_or(0),
            created_at: row.get::<i64>(5).unwrap_or(0),
            last_accessed_at: row.get::<i64>(6).unwrap_or(0),
        });
    }

    Ok((entries, total))
}

pub async fn query_cache_entry_detail(
    conn: &libsql::Connection,
    hash: &str,
) -> Result<Option<CacheEntryDetail>> {
    let mut rows = conn
        .query(
            "SELECT request_hash, provider, model, tokens_saved, hit_count, created_at, \
             last_accessed_at, request_json, response_json, \
             COALESCE(json_extract(request_json, '$.context_tag'), '') \
             FROM response_cache WHERE request_hash = ?",
            libsql::params![hash],
        )
        .await?;

    if let Some(row) = rows.next().await? {
        let req_str: String = row.get(7)?;
        let resp_str: String = row.get(8)?;
        Ok(Some(CacheEntryDetail {
            request_hash: row.get::<String>(0).unwrap_or_default(),
            provider: row.get::<String>(1).unwrap_or_default(),
            model: row.get::<String>(2).unwrap_or_default(),
            context: row.get::<String>(9).unwrap_or_default(),
            tokens_saved: row.get::<i64>(3).unwrap_or(0),
            hit_count: row.get::<i64>(4).unwrap_or(0),
            created_at: row.get::<i64>(5).unwrap_or(0),
            last_accessed_at: row.get::<i64>(6).unwrap_or(0),
            request_json: serde_json::from_str(&req_str).unwrap_or_default(),
            response_json: serde_json::from_str(&resp_str).unwrap_or_default(),
        }))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_poisoned_empty_response_evicted_on_read() {
        let conn = setup_test_db().await;
        let empty_resp = serde_json::json!({
            "content": null,
            "tool_calls": null,
            "usage": {"prompt_tokens": 100, "completion_tokens": 0, "total_tokens": 100},
            "truncated": false
        });
        conn.execute(
            "INSERT INTO response_cache (request_hash, provider, model, request_json, response_json, tokens_saved, created_at) VALUES ('poisoned', 'test', 'test', '{}', ?, 100, 1000)",
            libsql::params![empty_resp.to_string()],
        ).await.unwrap();

        let resp: AiResponse = serde_json::from_str(&empty_resp.to_string()).unwrap();
        let has_content = resp.content.as_ref().is_some_and(|c| !c.trim().is_empty());
        let has_tool_calls = resp.tool_calls.as_ref().is_some_and(|tc| !tc.is_empty());
        assert!(
            !has_content && !has_tool_calls,
            "empty response should trigger eviction"
        );

        let good_resp = serde_json::json!({
            "content": "{\"concerns\": [], \"dismissed_concerns\": []}",
            "tool_calls": null,
            "usage": {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150},
            "truncated": false
        });
        let good: AiResponse = serde_json::from_str(&good_resp.to_string()).unwrap();
        let has_content = good.content.as_ref().is_some_and(|c| !c.trim().is_empty());
        assert!(has_content, "non-empty response should pass the guard");
    }

    #[tokio::test]
    async fn test_invalidate_cache_entry() {
        let conn = setup_test_db().await;
        conn.execute(
            "INSERT INTO response_cache (request_hash, provider, model, request_json, response_json, tokens_saved, created_at) VALUES ('bad_entry', 'test', 'test', '{}', '{}', 100, 1000)",
            libsql::params![],
        ).await.unwrap();

        let mut rows = conn
            .query(
                "SELECT COUNT(*) FROM response_cache WHERE request_hash = 'bad_entry'",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            1
        );

        let _ = conn
            .execute(
                "DELETE FROM response_cache WHERE request_hash = ?",
                libsql::params!["bad_entry"],
            )
            .await;

        let mut rows = conn
            .query(
                "SELECT COUNT(*) FROM response_cache WHERE request_hash = 'bad_entry'",
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            0
        );
    }

    #[test]
    fn test_cache_key_not_serialized() {
        let resp = AiResponse {
            content: Some("test".to_string()),
            thought: None,
            thought_signature: None,
            tool_calls: None,
            usage: None,
            truncated: false,
            cache_key: Some("abc123".to_string()),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(
            !json.contains("cache_key"),
            "cache_key must not appear in serialized JSON"
        );
        assert!(
            !json.contains("abc123"),
            "cache_key value must not appear in serialized JSON"
        );

        let parsed: AiResponse = serde_json::from_str(&json).unwrap();
        assert!(parsed.cache_key.is_none());
    }

    #[test]
    fn test_fmt_tokens() {
        assert_eq!(fmt_tokens(0), "0");
        assert_eq!(fmt_tokens(500), "500");
        assert_eq!(fmt_tokens(999), "999");
        assert_eq!(fmt_tokens(1_000), "1.0k");
        assert_eq!(fmt_tokens(1_500), "1.5k");
        assert_eq!(fmt_tokens(42_100), "42.1k");
        assert_eq!(fmt_tokens(999_999), "1000.0k");
        assert_eq!(fmt_tokens(1_000_000), "1.0M");
        assert_eq!(fmt_tokens(1_500_000), "1.5M");
    }

    #[test]
    fn test_fmt_thousands() {
        assert_eq!(fmt_thousands(0), "0");
        assert_eq!(fmt_thousands(999), "999");
        assert_eq!(fmt_thousands(1_000), "1.000");
        assert_eq!(fmt_thousands(1_234_567), "1.234.567");
    }

    // Helper: create an in-memory cache DB with the full schema for testing
    // the SQL logic without needing a real AiProvider.
    async fn setup_test_db() -> libsql::Connection {
        let db = libsql::Builder::new_local(":memory:")
            .build()
            .await
            .unwrap();
        let conn = db.connect().unwrap();
        conn.execute_batch(
            "CREATE TABLE response_cache (
                request_hash TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                model TEXT NOT NULL,
                request_json TEXT NOT NULL,
                response_json TEXT NOT NULL,
                tokens_saved INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                hit_count INTEGER NOT NULL DEFAULT 0,
                last_accessed_at INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX idx_cache_eviction ON response_cache(hit_count, last_accessed_at);",
        )
        .await
        .unwrap();
        conn
    }

    async fn insert_test_entry(
        conn: &libsql::Connection,
        hash: &str,
        model: &str,
        tokens_saved: i64,
        hit_count: i64,
        last_accessed_at: i64,
    ) {
        conn.execute(
            "INSERT INTO response_cache (request_hash, provider, model, request_json, response_json, tokens_saved, created_at, hit_count, last_accessed_at) VALUES (?, ?, ?, '{}', '{}', ?, 1000, ?, ?)",
            libsql::params![hash, model, model, tokens_saved, hit_count, last_accessed_at],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_hit_count_increments() {
        let conn = setup_test_db().await;
        insert_test_entry(&conn, "abc", "test-model", 100, 0, 1000).await;

        // Simulate 3 cache hits
        for ts in [2000, 3000, 4000] {
            conn.execute(
                "UPDATE response_cache SET hit_count = hit_count + 1, last_accessed_at = ? WHERE request_hash = ?",
                libsql::params![ts, "abc"],
            )
            .await
            .unwrap();
        }

        let mut rows = conn
            .query(
                "SELECT hit_count, last_accessed_at FROM response_cache WHERE request_hash = 'abc'",
                (),
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert_eq!(row.get::<i64>(0).unwrap(), 3);
        assert_eq!(row.get::<i64>(1).unwrap(), 4000);
    }

    #[tokio::test]
    async fn test_eviction_by_max_entries() {
        let conn = setup_test_db().await;
        // Insert 7 entries with varying hit counts
        for i in 0..7 {
            insert_test_entry(&conn, &format!("hash_{}", i), "model", 100, i, 1000 + i).await;
        }

        // Evict down to 5 — should remove the 2 with lowest hit_count (0 and 1)
        let excess = 7u64 - 5;
        conn.execute(
            "DELETE FROM response_cache WHERE request_hash IN (
                SELECT request_hash FROM response_cache
                ORDER BY hit_count ASC, last_accessed_at ASC LIMIT ?
            )",
            libsql::params![excess as i64],
        )
        .await
        .unwrap();

        let mut rows = conn
            .query("SELECT COUNT(*) FROM response_cache", ())
            .await
            .unwrap();
        let count = rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap();
        assert_eq!(count, 5);

        // Verify the survivors are the ones with highest hit counts (2..6)
        let mut rows = conn
            .query(
                "SELECT request_hash FROM response_cache ORDER BY hit_count ASC",
                (),
            )
            .await
            .unwrap();
        let mut surviving = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            surviving.push(row.get::<String>(0).unwrap());
        }
        assert_eq!(
            surviving,
            vec!["hash_2", "hash_3", "hash_4", "hash_5", "hash_6"]
        );
    }

    #[tokio::test]
    async fn test_eviction_tiebreak_by_last_accessed() {
        let conn = setup_test_db().await;
        // All same hit_count, different last_accessed_at
        insert_test_entry(&conn, "old", "model", 100, 5, 1000).await;
        insert_test_entry(&conn, "mid", "model", 100, 5, 2000).await;
        insert_test_entry(&conn, "new", "model", 100, 5, 3000).await;

        // Evict 1 — should remove "old" (same hit_count, oldest access)
        conn.execute(
            "DELETE FROM response_cache WHERE request_hash IN (
                SELECT request_hash FROM response_cache
                ORDER BY hit_count ASC, last_accessed_at ASC LIMIT 1
            )",
            libsql::params![],
        )
        .await
        .unwrap();

        let mut rows = conn
            .query(
                "SELECT request_hash FROM response_cache ORDER BY last_accessed_at ASC",
                (),
            )
            .await
            .unwrap();
        let mut surviving = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            surviving.push(row.get::<String>(0).unwrap());
        }
        assert_eq!(surviving, vec!["mid", "new"]);
    }

    #[test]
    fn test_scrub_nondeterministic_content() {
        // Daily date in system prompt
        let input = r#"the current date is Wednesday, June 02, 2026. Do something."#;
        let scrubbed = scrub_nondeterministic_content(input);
        assert_eq!(scrubbed, "the current date is <DATE>. Do something.");

        // Different day produces the same scrubbed output
        let input2 = r#"the current date is Thursday, June 03, 2026. Do something."#;
        assert_eq!(scrub_nondeterministic_content(input2), scrubbed);

        // Commit hash from git show
        let input = "commit a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2\nAuthor: Joe";
        let scrubbed = scrub_nondeterministic_content(input);
        assert!(scrubbed.starts_with("commit <HASH>"));

        // Date line from git show (committer date)
        let input = "Date:   Mon Jun 1 10:30:00 2026 -0300\n\n    msg";
        let scrubbed = scrub_nondeterministic_content(input);
        assert!(scrubbed.starts_with("Date: <DATE>"));

        // Same patch content with different dates/hashes produces identical output
        let v1 = "the current date is Monday, June 01, 2026\\ncommit aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\\nDate:   Mon Jun 1 10:00:00 2026 -0300\\nsome diff";
        let v2 = "the current date is Tuesday, June 02, 2026\\ncommit bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\\nDate:   Tue Jun 2 11:00:00 2026 -0300\\nsome diff";
        assert_eq!(
            scrub_nondeterministic_content(v1),
            scrub_nondeterministic_content(v2)
        );
    }
}
