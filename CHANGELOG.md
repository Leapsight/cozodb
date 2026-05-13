# CHANGELOG
# 0.3.10
* Upgraded cozo libr with a fix to read-only transactions. They now use RocksDB snapshots to avoid issues with multithreading and locking.

# 0.3.9
* Added migration tool
# 0.3.8
* Upgraded cozo dependency to v0.8.7-leapsight

# 0.3.7

## Bug Fixes
* **Fixed cross-allocator SIGSEGV during RocksDB Open in containers (Docker, ECS, K8s).** Root cause: `cozo/rocksdb-jemalloc` enables `unprefixed_malloc_on_supported_platforms` in cozorocks, which defines `malloc`/`free` as LOCAL symbols inside `cozodb.so`. Rust 1.73+ adds `-Bsymbolic-non-weak-functions` when linking cdylib crates, binding all C++ `free()` calls to jemalloc's `free` — even when the pointer was allocated by glibc (e.g., via `getline()` in `rocksdb::PosixHelper::GetQueueSysfsFileValueOfFd()`). This cross-allocator mismatch crashed in `_rjem_je_free_default`. Fixed in the cozo fork (v0.8.6-leapsight) by patching `cozorocks/build.rs` to replace `getline()+free()` with `fgets()` using a stack buffer in `io_posix.cc` at build time — RocksDB C++ continues to use jemalloc for memory management (critical for preventing 15GB+ memory growth under load).
* Added eager per-thread jemalloc TLS warmup in all dirty-scheduled NIF functions and global warmup in `on_load` (defense-in-depth for jemalloc TSD initialization on BEAM dirty IO scheduler threads).
* Fixed `COZODB_JEMALLOC_BACKGROUND_THREAD` runtime default contradicting the compile-time `malloc_conf` setting. The runtime `configure_jemalloc()` was overriding `background_thread:false` (baked in at compile time) back to `true`, causing signal handling conflicts with the BEAM VM in containers. Default is now `false`; opt-in via `COZODB_JEMALLOC_BACKGROUND_THREAD=true`.

## Improvements
* Benchmark Dockerfile (`benchmark/Dockerfile`) rewritten to realistically simulate a consumer application that depends on cozodb — uses `mix deps.get && mix compile` which triggers the full rebar3 pre_hooks chain automatically, instead of manual `make cargo-build` and `.so` copying.
* Added `benchmark/Dockerfile.wellos` as a reference production multi-stage Dockerfile with:
  - Compile-time jemalloc env vars (`JEMALLOC_SYS_WITH_MALLOC_CONF`, `JEMALLOC_SYS_WITH_LG_PAGE`, `CXXFLAGS`) correctly placed in the build stage
  - Conditional `liburing` support via `COZODB_IO_URING` build arg (defaults to `false`)
  - Runtime-only packages in the runner stage (no `-dev` headers)
  - Removed unnecessary runtime dependencies (`libsodium`, `libsnappy`, `liblz4`, `liburing`) — all statically linked into their respective NIFs

# 0.3.6
* Change NIF `COZODB_JEMALLOC_BACKGROUND_THREAD` to `false` by default inline with COZO's side

# 0.3.5
* Update to cozo v0.8.4-leapsight making `options` file the priority
# 0.3.4
* Makes io-uring an option disabled by default (AWS ECS crashes)
* Reverts harcoded values in Cozo/cozorocks that where left during mem leak fix and allow them to be passed as ENV VARS. `option` is the main way to set those values, ENV VARS will override them

# 0.3.3

## Bug Fixes
* Fixed silent crash on AWS Graviton (aarch64 Linux) caused by jemalloc's profiling feature. The `profiling` feature compiles jemalloc with `-DJEMALLOC_PROF`, which adds platform-specific stack unwinding via libunwind. On aarch64 Linux in container environments (ECS, Docker), this initialization runs during `dlopen()` and can crash the BEAM VM before any Erlang error handling can catch it.

## Changes
* Removed `profiling` from the default jemalloc dependency features. Memory statistics (`stats`) are still included and work as before.
* Added opt-in `jemalloc-profiling` Cargo feature for heap profiling support (`dump_heap_profile/1`). Enable with `--features jemalloc-profiling` when building.
* `dump_heap_profile/1` now returns `{error, "profiling_not_compiled"}` when the `jemalloc-profiling` feature is not enabled.

# 0.3.2
## Bug Fixes
* Fixed an issue when deploying in Docker. Jemalloc crashes when deploying in Docker Compose on Apple Silicon due to differences in OS page sizes - Added documentation to README

# 0.3.1

## Bug Fixes
* Fixed "cannot allocate memory in static TLS block" error when loading NIF in Docker containers by enabling `disable-initial-exec-tls` for jemalloc


# 0.3.0

## Breaking Changes
* Database handles now use NIF `ResourceArc` instead of regular references
  - Handles are automatically cleaned up when garbage collected
  - `close/1` is now a graceful shutdown hint (database closes on GC)

## New Features

### New RocksDB Backend (`newrocksdb`)
* Added alternative RocksDB backend using the official `rust-rocksdb` crate
* Supports comprehensive configuration via 70+ `COZO_ROCKSDB_*` environment variables:
  - General options (create_if_missing, paranoid_checks, max_open_files, etc.)
  - Parallelism (increase_parallelism, max_background_jobs, max_subcompactions)
  - Write buffer/memtable settings
  - Compaction configuration (style, triggers, level settings)
  - Compression options (lz4, zstd, snappy, etc.)
  - Block-based table options (block_size, block_cache, bloom filters)
  - Blob storage (BlobDB) configuration
  - WAL settings
  - Direct I/O options
  - Statistics collection
* **Note:** The two RocksDB backends (`rocksdb`/cozorocks and `newrocksdb`/rust-rocksdb) are **mutually exclusive** due to allocator conflicts. Build with one or the other using `COZODB_BACKEND` environment variable.

### Memory Management APIs
* `memory_stats/0` - Get jemalloc memory statistics (allocated, active, resident, mapped, retained)
* `purge_jemalloc/0` - Force jemalloc to return unused memory to the OS
* `set_jemalloc_decay/2` - Configure jemalloc decay times for memory return aggressiveness
* `dump_heap_profile/1` - Dump jemalloc heap profile for analysis with `jeprof`

### RocksDB Memory Control
* `rocksdb_memory_stats/1` - Get per-database RocksDB memory statistics
* `flush_memtables/1` - Force flush memtables to disk to release memory
* `clear_block_cache/0` - Clear the shared RocksDB block cache (process-global)
* `set_block_cache_capacity/1` - Dynamically adjust block cache size
* `get_block_cache_stats/0` - Get block cache statistics

### jemalloc Integration
* Added jemalloc as optional memory allocator (enabled by default)
* Configurable via environment variables at startup:
  - `COZODB_JEMALLOC_DIRTY_DECAY_MS` - Dirty page decay time (default: 1000ms)
  - `COZODB_JEMALLOC_MUZZY_DECAY_MS` - Muzzy page decay time (default: 1000ms)
  - `COZODB_JEMALLOC_BACKGROUND_THREAD` - Enable background purging (default: true)
  - `COZODB_JEMALLOC_NARENAS` - Number of arenas (optional)
* Unified memory management between Rust NIF and RocksDB C++ via `cozo/rocksdb-jemalloc`

## Build System
* Added `COZODB_BACKEND` environment variable support in Makefile
  - `make build` or `make build-rocksdb` - Build with cozorocks (default)
  - `COZODB_BACKEND=newrocksdb make build` or `make build-newrocksdb` - Build with rust-rocksdb
* Build options work with `make`, `rebar3 compile`, and `mix compile` via env vars

## Improvements
* Improved Erlang serialization for better performance
* Updated Cozo dependency to v0.8.2-leapsight (forked with newrocksdb support)
* Added `newrocksdb` test groups with graceful skip when backend not compiled
* Comprehensive README documentation for storage engines, backends, and configuration

## Bug Fixes
* Fixed memory issues with jemalloc configuration
* Fixed jemalloc config warnings

# 0.2.10
* Upgraded Cozo dependency which now offers 3 new temporal functions
    1. expand_daily(h0, h1, tz, start, end) - Lines 3926-4007
        - Expands daily recurrence to concrete intervals                       
        - Signature: (i64, i64, String, i64, i64) -> [[i64, i64]]               
    2. expand_monthly(day_of_month, h0, h1, tz, start, end) - Lines 4009-4109
        - Expands monthly recurrence to concrete intervals
        - Day clamping for months with fewer days (e.g., Feb 28/29)
        - Signature: (i64, i64, i64, String, i64, i64) -> [[i64, i64]]      
    3. expand_yearly(month, day, h0, h1, tz, start, end) - Lines 4111-4211
        - Expands yearly recurrence to concrete intervals
        - Skips Feb 29 on non-leap years (doesn't clamp to Feb 28)
        - Signature: (i64, i64, i64, i64, String, i64, i64) -> [[i64, i64]]
    4. Key Features
        - All timestamps in milliseconds
        - Proper DST handling using chrono-tz
        - End-of-day support (h1=1440 uses next day's midnight)
        - Input validation for month, day, day_of_month ranges
        - Timezone validation with IANA timezone strings
        - Overlap filtering - only includes intervals that overlap with [start, end]

# 0.2.9
* Fix error return types (broken with new cozo update in previous commit)

# 0.2.8
* Upgraded to latest version of forked cozo containing new temporal functions
* Improved capture of Cozo errors

# 0.2.7
* Fix rust dep graph_builder compilation issue with latest rayon (pinned working versions)

# 0.2.5
* Added `telemetry_registry` event declarations
# 0.2.0
* Require OTP27 with the new `json` module
