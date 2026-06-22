# Relation Archiving & Replication

This is the authoritative guide to **configuring** and **managing** relation
replication in cozodb: continuously copying a relation's committed rows to
durable object storage (S3-compatible), then pruning the cozo copy once the rows
are safely stored.

> **Mental model.** cozo is the *hot, queryable* store. Replication writes a
> typed Parquet copy of every committed row to a **datalake** (S3, then
> Iceberg/Delta/DuckDB/Trino/raw Parquet). A **watermark** guarantees cozo never
> deletes a row that has not been durably copied first. The datalake owns
> long-term retention, GDPR deletes, compaction, and analytics. **cozo never
> deletes from object storage** — that is the datalake's job.

```
   :put / :update              replicate_pending/2            archive/3
   (commit_now() stamps   ──▶  copy rows ts > watermark   ──▶  delete rows ts <= watermark
    a monotonic ts)            to Parquet on S3,                from the live relation
                               advance the watermark            (now safely on S3)
```

---

## 1. Runtime prerequisites (read first)

Replication will not work until these are in place.

### 1.1 The `archive` build feature

The NIF must be built with the `cozo/archive` cargo feature (enabled in
`native/cozodb/Cargo.toml` `default` and `new-rocksdb-default`). Without it,
every archive call returns `{error, _}` mentioning *"requires the 'archive'
feature"*.

### 1.2 AWS / S3 credentials — in the BEAM OS environment

cozo **never stores credentials**. It reads the standard AWS SDK environment
chain at the moment it builds an S3 client. These must be present in the OS
environment of the **running BEAM node** (via `vm.args` `-env`, the release
`env.sh`, systemd `Environment=`, or the container env) — *before* the node
boots. A `.env` file is **not** auto-loaded at runtime (only the test suites do
that).

| Variable | Purpose |
|---|---|
| `AWS_ACCESS_KEY_ID` | credential |
| `AWS_SECRET_ACCESS_KEY` | credential |
| `AWS_SESSION_TOKEN` | optional (STS / assumed role) |
| `AWS_REGION` | bucket region (`auto` for some providers) |
| `AWS_ENDPOINT_URL_S3` / `AWS_ENDPOINT_URL` / `AWS_ENDPOINT` | endpoint for non-AWS S3 (Tigris, R2, MinIO, Wasabi, B2). cozo bridges all three names automatically. |
| `COZO_ARCHIVE_SKIP_IAM_PROBE` | `1`/`true`/`yes` disables the no-DeleteObject probe (§5.3). Avoid in production. |

### 1.3 IAM policy — **no DeleteObject**

Grant exactly `PutObject` + `GetObject` + `ListBucket`, and **not**
`DeleteObject`:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:PutObject", "s3:GetObject", "s3:ListBucket"],
    "Resource": [
      "arn:aws:s3:::your-bucket",
      "arn:aws:s3:::your-bucket/your-prefix/*"
    ]
  }]
}
```

On the first drain to a fresh destination, cozo runs an **IAM probe** that
refuses to proceed if the credentials can delete objects — so a compromised cozo
process can never erase the archive. If your provider only has coarse access
keys (read vs read-write, no per-action scoping), you may need
`COZO_ARCHIVE_SKIP_IAM_PROBE=1` (§5.3).

---

## 2. Configuration

### 2.1 Declare a commit-timestamp column

Every archived relation needs an `Int` column defaulting to `commit_now()`:

```erlang
cozodb:run(Db, <<
  ":create orders {"
  "  id: Int =>"
  "  customer: String,"
  "  amount: Float,"
  "  updated_at: Int default commit_now()"
  "}"
>>).
```

`commit_now()` is a **durable, strictly-monotonic, per-relation commit clock**
(a hybrid logical clock seeded from wall-clock microseconds). Properties you can
rely on:

- It is **only** valid as a bare column default (`default commit_now()`). Using
  it in a query body or a compound expression (`commit_now() + 1`) is an error.
- On `:put` it stamps the row; on partial `:update` it is **re-stamped even if
  you don't bind the column** — so an updated row gets a fresh timestamp and
  cannot be archived until it has been re-replicated.
- It is **strictly increasing within a relation** and never repeats, even for
  writes in the same microsecond. This is what makes the watermark safe (§5.1).

### 2.2 Configure the relation for archiving

```erlang
%% Minimal: timestamp column only (no destination yet — cannot replicate).
ok = cozodb:archive_config_put(Db, orders, updated_at).

%% With a destination and options.
ok = cozodb:archive_config_put(Db, orders, updated_at, #{
    uri => <<"s3://my-bucket/cozo/orders/">>,
    encryption => sse_kms,
    kms_arn => <<"arn:aws:kms:eu-west-1:111122223333:key/abcd-...">>,
    max_rows_per_segment => 100000
}).
```

`Opts` keys (all optional):

| Key | Values | Meaning |
|---|---|---|
| `uri` | `s3://bucket/prefix/`, `file:///abs/path/`, or a bare local path | Replication destination. **Required before `replicate_pending/2`.** |
| `encryption` | `none` (default) \| `sse_s3` \| `sse_kms` | S3 server-side encryption mode. |
| `kms_arn` | binary/list | Required **iff** `encryption` is `sse_kms`; forbidden otherwise. |
| `max_rows_per_segment` | `pos_integer()` (default `100000`) | Per-segment row cap. ~100k rows ≈ 50 MB Parquet, the size analytics tools prefer. |

> **Positional rule (important).** The underlying CozoScript option slots are
> positional, so `encryption` requires `uri`, and `kms_arn` requires
> `encryption`. Violating this raises `badarg`. In practice you always provide
> `uri` (you need it to replicate), so this is rarely a concern.

`RelName`, the timestamp column, atoms, lists, and binaries are all accepted.

### 2.3 (Recommended for large relations) a timestamp index

By default a drain full-scans the relation and sorts in memory. If you create an
index whose single leading column is the timestamp column, the replicator
**range-scans that index from the watermark** instead:

```erlang
cozodb:run(Db, <<"::index create orders:by_ts {updated_at}">>).
```

Indices in CozoDB MUST be created at table creation time as any existing data on the table will not be indexed. It is transparent — drains use
it automatically when present and fall back to a full scan when absent.

### 2.4 Inspect / change / remove configuration

```erlang
%% All configs / one relation. Columns:
%%   relation, timestamp_column, staging_dir, encryption, kms_key_arn, max_rows_per_segment
{ok, #{rows := Rows}} = cozodb:archive_config_get(Db).
{ok, #{rows := [Row]}} = cozodb:archive_config_get(Db, orders).

%% Re-running archive_config_put/3,4 upserts (overwrites) the config.

%% Remove config + watermark for a relation.
ok = cozodb:archive_config_remove(Db, orders).
```

---

## 3. Management (the operational lifecycle)

The steady-state loop, per configured relation, is **replicate, then archive**,
on a schedule:

```erlang
%% (rows are written normally; commit_now() stamps updated_at)

%% 1. Replicate: copy rows past the watermark to S3, advance the watermark.
{ok, Rep} = cozodb:replicate_pending(Db, orders),

%% 2. Archive: delete from cozo the rows that are now safely on S3.
{ok, Arc} = cozodb:archive(Db, orders, <<"?[id] := *orders{id}">>).
```

### 3.1 `replicate_pending/2`

Scans for rows whose timestamp is past the watermark, writes one or more Parquet
segments to the configured destination, records a manifest row per segment, and
advances the watermark. **Idempotent**: a call with no new rows is a no-op.

Returns a single summary row:

```erlang
{ok, #{rows := [[<<"OK">>, RowsReplicated, SegmentsWritten, OldWatermark, NewWatermark]]}}.
```

- Uploads happen **outside** any database transaction — a long drain does not
  block concurrent writers.
- Segments are **content-addressed** (id and filename derive from the Parquet
  SHA-256), so re-running a drain that failed mid-upload re-PUTs identical
  objects and upserts the same manifest row rather than creating duplicates.
- Per-segment detail lives in `cozo_archive_segments` (§4).

### 3.2 `archive/3`

Runs `KeyQuery` (a query producing the relation's key column(s)) and, for each
returned row whose stored timestamp is **at or below the watermark**, deletes it
from the live relation. Rows still pending replication (`ts > watermark`) are
skipped.

```erlang
{ok, #{rows := [[<<"OK">>, Archived, Skipped, Missing, Watermark]]}} =
    cozodb:archive(Db, orders, <<"?[id] := *orders{id}">>).
```

- `Archived` — deleted (already replicated). `Skipped` — still pending. `Missing`
  — key in the query result but not in the store.
- Scope the query to bound the work on large relations (e.g. only old rows).
- **Triggers do NOT fire** on archive — it deletes via the raw store layer (a
  bulk-load semantic, not a logical change). Use `:rm` if you need rm-triggers
  (but see §5.2).

### 3.3 Restore — `import_parquet/3`

Reads a Parquet segment back into a relation. `Uri` may be a local path, a
`file://` URI, or an **`s3://bucket/key`** URI (fetched via the object store and
decoded in memory). Columns are matched to the relation schema by name.

```erlang
%% Find the segment(s) you need from the manifest, then restore.
{ok, #{rows := [[S3Path | _]]}} =
    cozodb:run(Db, <<"?[f] := *cozo_archive_segments{relation: 'orders', file_path: f}">>),
{ok, #{rows := [[<<"OK">>, RowsImported]]}} =
    cozodb:import_parquet(Db, orders, S3Path).
```

`import_parquet/3` does **not** fire triggers and refuses relations carrying
HNSW/FTS/LSH indices.

### 3.4 `advance_watermark/3` — admin / disaster recovery

Sets the watermark directly. The replicator normally owns the watermark; use
this only to force a known value (e.g. after restoring from a backup). It
refuses if the relation is not configured for archiving.

```erlang
{ok, _} = cozodb:advance_watermark(Db, orders, 1782147351116336).
```

### 3.5 Scheduling & cadence

Replication is **operator-driven** (there is no background thread). Drive it
from an OTP worker / cron:

- **Replicate before archive**, as separate calls.
- **Drain frequently** so the backlog (and each drain's memory + S3 work) stays
  small. Keep `max_rows_per_segment` modest.
- **Prefer low-write windows** for large catch-up drains.
- **Never let two drains of the same relation overlap** — serialise them per
  relation in your scheduler.

---

## 4. Monitoring (system relations)

These are ordinary cozo relations — query them for dashboards and health checks.

| Relation | Key → values |
|---|---|
| `cozo_archive_config` | `relation` → `timestamp_column, staging_dir?, encryption?, kms_key_arn?, max_rows_per_segment?` |
| `cozo_archive_watermark` | `relation` → `last_safe_commit_ts` |
| `cozo_archive_segments` | `segment_id` → `relation, file_path, lower_commit_ts, upper_commit_ts, key_count, sha256, status, written_at` |

```erlang
%% Current watermark per relation.
cozodb:run(Db, <<"?[r, w] := *cozo_archive_watermark{relation: r, last_safe_commit_ts: w}">>).

%% Segments written for a relation, newest first.
cozodb:run(Db, <<
  "?[id, path, lo, hi, n, t] := "
  "  *cozo_archive_segments{segment_id: id, relation: 'orders', file_path: path, "
  "    lower_commit_ts: lo, upper_commit_ts: hi, key_count: n, written_at: t} "
  ":order -t"
>>).

%% Replication lag: rows in the live relation past the watermark (not yet replicated).
cozodb:run(Db, <<
  "wm[w] := *cozo_archive_watermark{relation: 'orders', last_safe_commit_ts: w} "
  "?[count(id)] := *orders{id, updated_at: ts}, wm[w], ts > w"
>>).
```

---

## 5. Guarantees, semantics & caveats (design around these)

### 5.1 The watermark contract

- A row is **deletable by `archive/3`** only once `ts <= watermark`.
- The watermark only advances after a segment carrying that timestamp is durably
  PUT.
- Because `commit_now()` is strictly monotonic per relation, a row written after
  a drain always gets `ts > new_watermark` and is therefore never deleted before
  it has been replicated. **This is the core safety property.**

### 5.2 At-least-once, and what the datalake must do

Replication is **at-least-once**. A drain that uploads some segments and then
fails before recording them leaves those objects on S3; the retry re-uploads the
same rows. Content-addressing makes the *common* retry idempotent (same bytes →
same object key → same manifest row), but if the underlying rows changed between
attempts the tail segment can differ and leave an orphan.

Therefore the **consuming datalake must dedupe by the cozo primary key**
(segments carry the key columns), and you should run a **reaper** that lists S3
and removes objects not present in `cozo_archive_segments`.

**Do not `:rm` rows you intend to keep.** The replicator only sees rows
currently present; a direct `:rm` (bypassing `archive/3`) removes a row from cozo
without ever replicating it. Always use the replicate → `archive/3` flow.

### 5.3 The IAM probe and its escape hatch

The no-DeleteObject rule is enforced by a probe on first use of a destination
(cached per process thereafter). If your S3-compatible provider cannot express
per-action IAM, set `COZO_ARCHIVE_SKIP_IAM_PROBE=1` — but understand this
**defeats the guarantee** that a compromised cozo process cannot delete archived
segments. It prints a loud warning each time and should be alarmed on in
production.

### 5.4 Limits

- **No HNSW / FTS / LSH.** Relations with vector/full-text/LSH indices cannot be
  replicated, archived, or parquet-imported — all three refuse with a clear
  error.
- **Supported column types:** `Bool, Int, Float, String, Bytes, Uuid, Validity,
  List of those`. `Json` and other exotic types are not exportable yet.
- **Collection phase is in memory.** The set of rows past the watermark is held
  in memory during a drain (bounded by how far behind the watermark is, not the
  relation size). Drain frequently to keep this small.
- **Restore is one segment at a time** via `import_parquet/3`; restoring a range
  means importing each relevant segment (find them in `cozo_archive_segments`).

---

## 6. Complete worked example

```erlang
{ok, Db} = cozodb:open(rocksdb, "/var/lib/cozo/db"),

%% --- configure ---
{ok, _} = cozodb:run(Db, <<
  ":create orders {id: Int => customer: String, amount: Float, "
  "updated_at: Int default commit_now()}"
>>),
cozodb:run(Db, <<"::index create orders:by_ts {updated_at}">>),   %% optional, for scale
ok = cozodb:archive_config_put(Db, orders, updated_at, #{
    uri => <<"s3://my-bucket/cozo/orders/">>,
    encryption => sse_kms,
    kms_arn => <<"arn:aws:kms:eu-west-1:111122223333:key/abcd">>
}),

%% --- normal writes ---
{ok, _} = cozodb:run(Db, <<
  "?[id, customer, amount] <- [[1, 'alice', 12.5], [2, 'bob', 7.0]] "
  ":put orders {id => customer, amount}"
>>),

%% --- manage (run on a schedule) ---
{ok, #{rows := [[<<"OK">>, NReplicated, _Segs, _Old, _New]]}} =
    cozodb:replicate_pending(Db, orders),
{ok, #{rows := [[<<"OK">>, NArchived, _Skip, _Miss, _Wm]]}} =
    cozodb:archive(Db, orders, <<"?[id] := *orders{id}">>),

%% --- restore (if ever needed) ---
{ok, #{rows := [[S3Path | _]]}} =
    cozodb:run(Db, <<"?[f] := *cozo_archive_segments{relation: 'orders', file_path: f}">>),
{ok, _} = cozodb:import_parquet(Db, orders, S3Path).
```

---

## 7. API summary

| Function | Purpose | Returns |
|---|---|---|
| `archive_config_put(Db, Rel, TsCol)` | configure (no destination) | `ok \| {error,_}` |
| `archive_config_put(Db, Rel, TsCol, Opts)` | configure with `uri`/`encryption`/`kms_arn`/`max_rows_per_segment` | `ok \| {error,_}` |
| `archive_config_get(Db)` / `archive_config_get(Db, Rel)` | list configuration(s) | `query_return()` |
| `archive_config_remove(Db, Rel)` | remove config + watermark | `ok \| {error,_}` |
| `replicate_pending(Db, Rel)` | drain rows past the watermark to S3 | `[status, rows_replicated, segments_written, old_watermark, new_watermark]` |
| `archive(Db, Rel, KeyQuery)` | delete replicated rows from the live relation | `[status, archived, skipped, missing, watermark]` |
| `import_parquet(Db, Rel, Uri)` | restore a segment (local / `file://` / `s3://`) | `[status, rows]` |
| `advance_watermark(Db, Rel, Ts)` | admin/DR: set the watermark directly | `[status, watermark]` |
