# Fix: `sashiko-cli list` blocks when reviewer is busy

## Context

The daemon uses a single `libsql::Connection` (wrapped in `Arc<Database>`)
shared by every component: the web API (axum read handlers), the reviewer
(writes), the DB worker, the email worker, and the metrics task.  When the
reviewer is doing a long-running write or waiting for Gemini with a DB
operation in flight, the API's read queries serialize behind it on the same
connection — causing `sashiko-cli list` (which calls `/api/patchsets`) to
hang until the reviewer releases the connection.

SQLite in WAL mode supports concurrent readers alongside a single writer,
but only when they use **separate connections**.  The current code creates
one connection and drops the `libsql::Database` handle.

## Approach

Store the `libsql::Database` handle in the `Database` struct and add a
method to create independent read connections.  Give the web API its own
`Database` instance with a separate connection so reads never block behind
reviewer writes.

## File: `src/db.rs`

### Changes

1. **Store `libsql::Database`** in the struct (wrapped in `Arc` since
   `libsql::Database` is not Clone):
   ```rust
   pub struct Database {
       db: Arc<libsql::Database>,
       pub conn: libsql::Connection,
   }
   ```

2. **Keep `db` alive** in `Database::new()` — wrap in `Arc` after building
   and store it alongside the connection.

3. **Add `new_connection(&self)` method** that creates a fresh `Database`
   with its own connection from the same underlying `libsql::Database`,
   applying the same PRAGMAs (WAL, busy_timeout):
   ```rust
   pub async fn new_connection(&self) -> Result<Self> {
       let conn = self.db.connect()?;
       let _ = conn.query("PRAGMA journal_mode=WAL;", ()).await?.next().await;
       let _ = conn.query("PRAGMA busy_timeout = 5000;", ()).await?.next().await;
       Ok(Self { db: self.db.clone(), conn })
   }
   ```

## File: `src/main.rs`

### Changes

After creating the main `Database`, call `db.new_connection()` to create a
separate instance for the API:

```rust
let api_db = Arc::new(db.new_connection().await?);
```

Pass `api_db` to `start_api()` instead of `db.clone()`.  All other
components (reviewer, DB worker, email worker, metrics) keep using the
original `db`.

## What this does NOT do

- Does not add a full connection pool — one read connection for the API
  is enough since axum handlers are async and the queries are fast.
- Does not change any DB methods or query logic.
- Does not touch the response_cache.db connection (separate DB entirely).

## Verification

1. `cargo fmt -- --check` — clean
2. `cargo clippy --all-targets` — no warnings
3. `cargo test` — all pass
4. Manual: start a review, run `sashiko-cli list` while the reviewer is
   processing — it should respond immediately instead of blocking
