%% =============================================================================
%%  cozodb.erl -
%%
%%  Copyright (c) 2023-2025 Leapsight. All rights reserved.
%%
%%  Licensed under the Apache License, Version 2.0 (the "License");
%%  you may not use this file except in compliance with the License.
%%  You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%%  Unless required by applicable law or agreed to in writing, software
%%  distributed under the License is distributed on an "AS IS" BASIS,
%%  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%  See the License for the specific language governing permissions and
%%  limitations under the License.
%% =============================================================================

-module(cozodb).

-moduledoc #{format => "text/markdown"}.
-moduledoc """
<a href="https://www.cozodb.org/" target="_">CozoDB</a> is a A FOSS embeddable,
transactional, relational-graph-vector database with time travelling capability,
perfect as the long-term memory for LLMs and AI.

This module implements the Erlang (BEAM) bindings for CozoDB via a NIF built using [Rustler](https://github.com/rusterlium/rustler).

## Key Features

- **Transactional Operations:** Supports atomic transactions for batch operations
- **Hybrid Data Model:** Combines relational, graph, and vector paradigms
- **Time Travelling:** Query historical versions of your data
- **Flexible Indexing:** with support for
  - Covering Indices
  - Proximity Indices:
    - HNSW (Hierarchical Navigable Small World): Fast approximate nearest neighbor searches
    - MinHash-LSH: Locality sensitive hashing for similarity searches
    - Full-Text Search (FTS): Efficient text matching


## Usage Overview

The API is divided into several sections:

### Database Lifecycle

- **Opening/Closing:**
  Use `open/0`, `open/1`, `open/2`, and `open/3` to create or open a database.
  Use `close/1` to gracefully shut down a database, freeing allocated NIF resources.

- **Backup & Restore:**
  Functions such as `backup/2`, `restore/2`, and `import_from_backup/3` enable exporting and restoring your database.

### Data Operations

- **Import/Export:**
  The `import/2` and `export/2,3` functions allow for batch data ingestion and extraction, ensuring consistency via transactions.

- **Running Scripts:**
  Use `run/2` and `run/3` to execute CozoScript commands for querying or modifying the database.

### System Catalogue

- **Relations and Columns:**
  Functions like `relations/1`, `columns/2`, and `describe/3` let you inspect and manage stored relations.

- **Indices and Triggers:**
  Create indices using `create_index/4` and manage triggers via `triggers/2`, `set_triggers/3`, and `delete_triggers/2`.

### Advanced Features

- **Query Explanation:**
  The `explain/2` function provides insights into query execution.

- **Monitoring & Maintenance:**
  Functions such as `running/1`, `kill/2`, and `compact/1` help monitor and maintain database health.

## Indexing Examples

## Datalog Programs
See the [CozoScript Tutorial](https://docs.cozodb.org/en/latest/tutorial.html).

### HSNW

```
{ok, _} = Module:create_index(Db, "table_hnsw_fun", "my_hsnw_index", #{
    type => hnsw,
    dim => 128,
    m => 50,
    ef_construction => 20,
    dtype => f32,
    distance => l2,
    fields => [v],
    filter => <<"k != `foo`">>,
    extend_candidates => false,
    keep_pruned_connections => false
}).
```

### LSH Indices

```
{ok, _} = Module:create_index(Db, "table_lsh_fun", "my_lsh_index", #{
    type => lsh,
    extractor => v,
    extract_filter => "!is_null(v)",
    tokenizer => simple,
    filters => [alphanumonly],
    n_perm => 200,
    target_threshold => 0.7,
    n_gram => 3,
    false_positive_weight => 1.0,
    false_negative_weight => 1.0
}).
```

### FTS Indices
You can create an FTS index using `create_index/4`.

The following example creates and index called `my_fts_index` on the relation
`rel_a`.

```
{ok, _} = Module:create_index(Db, "rel_a", "my_fts_index", #{
    type => fts,
    extractor => v,
    extract_filter => "!is_null(v)",
    tokenizer => simple,
    filters => [alphanumonly]
}).
```

You can always use Cozo Script directly via `run/2`. For example, the
following script is equivalent to the previous example.

```
::fts create rel_a:my_fts_index {
    extractor: v,
    extract_filter: !is_null(v),
    tokenizer: Simple,
    filters: [],
}
```
For more details, see the <a href="https://docs.cozodb.org/en/latest/vector.html#full-text-search-fts" target="_">FTS documentation</a>.

## Memory Management

This NIF uses [jemalloc](https://jemalloc.net/) as the global allocator for both Rust
and RocksDB (C++) allocations. This provides unified memory management and statistics
across the entire NIF.

### Memory Functions

- `memory_stats/0` - Get jemalloc memory statistics (allocated, resident, mapped, retained)
- `rocksdb_memory_stats/1` - Get RocksDB-specific memory usage (memtables, block cache)
- `purge_jemalloc/0` - Force immediate return of unused memory to the OS
- `set_jemalloc_decay/2` - Configure how aggressively memory is returned to the OS
- `get_jemalloc_decay/0` - Query current decay settings

### Understanding jemalloc Memory Retention

jemalloc retains freed memory in "dirty pages" and "muzzy pages" for potential reuse,
which improves performance but can make memory appear "leaked" when it's actually
just cached. The decay times control how long this memory is held:

- **Dirty pages**: Recently freed memory that still contains data
- **Muzzy pages**: Dirty pages that have been purged but VM mapping is retained

By default, jemalloc holds memory for 10 seconds before returning it to the OS.
This NIF configures the most aggressive setting: **0ms (immediate return)**.

This ensures memory is returned to the OS as soon as it's freed, which is important
for long-running Erlang applications where memory visibility and predictable resource
usage are critical.

### Tuning Memory Return

You can control memory return behavior at runtime:

```erlang
%% Check current decay settings
{ok, Settings} = cozodb:get_jemalloc_decay().
%% #{<<"dirty_decay_ms">> => 0, <<"muzzy_decay_ms">> => 0}

%% Most aggressive - immediate return to OS (our default)
%% Lowest memory usage, optimal for most Erlang applications
ok = cozodb:set_jemalloc_decay(0, 0).

%% Balanced - 1 second decay
%% Slight performance improvement under very high allocation rates
ok = cozodb:set_jemalloc_decay(1000, 1000).

%% Less aggressive - original jemalloc default (10 seconds)
%% Best performance but significantly higher memory retention
ok = cozodb:set_jemalloc_decay(10000, 10000).

%% Disable decay entirely - maximum memory retention
ok = cozodb:set_jemalloc_decay(-1, -1).
```

### Environment Variables

jemalloc behavior can be tuned at startup via environment variables:

- `COZODB_JEMALLOC_BACKGROUND_THREAD` - Enable async purging (default: true)
- `COZODB_JEMALLOC_NARENAS` - Limit arena count (optional, e.g., 4 or 8 for many threads)
- `COZODB_JEMALLOC_DIRTY_DECAY_MS` - Dirty page decay time (default: 1000ms)
- `COZODB_JEMALLOC_MUZZY_DECAY_MS` - Muzzy page decay time (default: 1000ms)

Example for aggressive memory return:
```
COZODB_JEMALLOC_DIRTY_DECAY_MS=0 COZODB_JEMALLOC_MUZZY_DECAY_MS=0 rebar3 shell
```

Example with limited arenas (useful with many dirty schedulers):
```
COZODB_JEMALLOC_NARENAS=8 rebar3 shell
```

### Recommended Tuning Strategy

1. **Most applications**: Use the default (1000ms with background_thread) - balanced RSS and latency
2. **Lowest RSS**: Set decay to 0ms - aggressive memory return, may impact latency under load
3. **Many threads/schedulers**: Set `COZODB_JEMALLOC_NARENAS=8` to reduce per-arena overhead
4. **Memory debugging**: Set decay to 0ms; call `purge_jemalloc/0` to force immediate cleanup
5. **Benchmarking**: Compare 0ms vs 1000ms vs 5000ms to find optimal setting for your workload

## System Operations
* Query Management: Monitor running queries with running/1 and terminate problematic queries using kill/2.
""".

-include("cargo.hrl").
-include("cozodb.hrl").
-include_lib("kernel/include/logger.hrl").

-define(APP, cozodb).
-define(NIF_NOT_LOADED,
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]})
).
-define(IS_DB_HANDLE(X), (is_map(X) andalso is_map_key(resource, X))).
-define(GET_RESOURCE(X), maps:get(resource, X)).

-opaque db_handle() :: #{resource := reference(), engine := binary(), path := binary()}.
-type relations() :: #{
    relation_name() => #{
        headers => [binary()],
        rows => [row()]
    }
}.
-type relation_name() :: binary().
-type relation_spec() ::
    binary()
    | #{
        keys => [
            column_name()
            | {column_name(), column_spec()}
        ],
        columns => [
            column_name()
            | {column_name(), column_spec()}
        ]
    }.
-type column_spec() ::
    undefined
    | #{
        type => column_type(),
        nullable => boolean(),
        default => binary()
    }.
-type column_name() :: binary().
-type column_type() :: column_atomic_type() | column_composite_type().
-type column_atomic_type() ::
    any
    | bool
    | bytes
    | json
    | int
    | float
    | string
    | uuid
    | validity.
-type column_composite_type() ::
    {list, column_atomic_type()}
    | {list, column_atomic_type(), Size :: pos_integer()}
    | {tuple, [column_atomic_type()]}
    | {vector, 32 | 64, Size :: pos_integer()}.

-type engine() :: mem | sqlite | rocksdb | newrocksdb.
%% mem - In-memory storage (no persistence)
%% sqlite - SQLite backend (good for small datasets, single-writer)
%% rocksdb - RocksDB via cozorocks C++ FFI (current default, high-performance)
%% newrocksdb - RocksDB via rust-rocksdb crate (comprehensive env var config)
-type engine_opts() :: map().
-type path() :: file:filename() | binary().
-type index_spec() ::
    covering_index_spec()
    | hnsw_index_spec()
    | lsh_index_spec()
    | fts_index_spec().
-type covering_index_spec() :: #{
    type := covering,
    fields := [column_name()]
}.
-type hnsw_index_spec() :: #{
    type := hnsw,
    dim := pos_integer(),
    m := pos_integer(),
    ef_construction := pos_integer(),
    fields := [column_name()],
    dtype => f32 | f64,
    distance => l2 | cosine | ip,
    filter => hnsw_filter(),
    extend_candidates => boolean(),
    keep_pruned_connections => boolean()
}.
-type lsh_index_spec() :: #{
    type := lsh,
    extractor := column_name(),
    tokenizer := tokenizer(),
    n_perm := pos_integer(),
    n_gram := pos_integer(),
    target_threshold := float(),
    extract_filter => extract_filter(),
    filters => [token_filter()],
    false_positive_weight => float(),
    false_negative_weight => float()
}.
-type fts_index_spec() :: #{
    type := fts,
    extractor => column_name(),
    extract_filter => extract_filter(),
    tokenizer => tokenizer(),
    filters => [token_filter()]
}.
-type hnsw_filter() :: string().
-type extract_filter() :: string().
-type tokenizer() ::
    raw
    | simple
    | whitespace
    | ngram
    | {ngram, MinGram :: pos_integer(), MaxGram :: pos_integer(), PrefixOnly :: boolean()}
    | {cangjie, default | all | search | unicode}.
-type token_filter() ::
    lowercase
    | alphanumonly
    | asciifolding
    | {stemmer, Lang :: string()}
    | {stopwords, Lang :: string()}.
-type query_opts() :: #{
    encoding => json | undefined,
    read_only => boolean(),
    parameters =>
        #{
            Key ::
                atom() | binary() =>
                    Value :: any()
        }
        | [
            {
                Key :: atom() | binary(),
                Value :: any()
            }
        ]
}.
-type query_return() ::
    {ok, query_result()}
    | {ok, Json :: binary()}
    | {error, Reason :: any()}.
-type query_result() :: #{
    headers := [column_name()],
    rows := [row()],
    count := integer(),
    next => [row()] | null,
    % secs
    took => float()
}.
-type row() :: list(value()).
-type value() ::
    null
    | boolean()
    | integer()
    | float()
    | list()
    | binary()
    | validity()
    | json().
-type json() :: {json, binary()}.
-type validity() :: {float(), boolean()}.
-type export_opts() :: #{encoding => json}.
-type info() :: #{engine := binary(), path := binary()}.
-type trigger_spec() :: #{trigger_event() => script()}.
-type trigger_event() :: on_put | on_remove | on_replace.
-type script() :: list() | binary().

-export_type([db_handle/0]).
-export_type([column_name/0]).
-export_type([index_spec/0]).
-export_type([info/0]).
-export_type([query_result/0]).
-export_type([query_return/0]).
-export_type([relation_spec/0]).
-export_type([row/0]).
-export_type([script/0]).
-export_type([trigger_spec/0]).

%% API: Basics
-export([close/1]).
-export([open/0]).
-export([open/1]).
-export([open/2]).
-export([open/3]).
-export([run/2]).
-export([run/3]).

%% API: System Catalogue
-export([columns/2]).
-export([create_index/4]).
-export([create_relation/3]).
-export([describe/3]).
-export([drop_index/2]).
-export([drop_index/3]).
-export([indices/2]).
-export([relations/1]).
-export([remove_relation/2]).
-export([remove_relations/2]).
-export([triggers/2]).
-export([set_triggers/3]).
-export([delete_triggers/2]).

%% API: Utils
-export([explain/2]).
-export([info/1]).
-export([rows_to_maps/1]).

%% API: Operations
-export([backup/2]).
-export([export/2]).
-export([export/3]).
-export([import/2]).
-export([import_from_backup/3]).
-export([register_callback/2]).
-export([restore/2]).
-export([unregister_callback/2]).
%% -export([register_fixed_rule/2]).
%% -export([unregister_fixed_rule/2]).

%% API: Monitor
-export([running/1]).
-export([kill/2]).

%% API: Maintenance
-export([compact/1]).

%% API: Debug/Profiling
-export([memory_stats/0]).
-export([flush_memtables/1]).
-export([rocksdb_memory_stats/1]).
-export([dump_heap_profile/1]).
%% Block cache control (process-global)
-export([clear_block_cache/0]).
-export([set_block_cache_capacity/1]).
-export([get_block_cache_stats/0]).
%% jemalloc control
-export([purge_jemalloc/0]).
-export([set_jemalloc_decay/2]).
-export([get_jemalloc_decay/0]).

-on_load(init/0).

%% =============================================================================
%% TELEMETRY_REGISTRY DECLARATIONS
%% =============================================================================

-telemetry_event(#{
    event => [cozodb, run, start],
    description =>
        <<"Emitted at the start of the CozoScript execution">>,
    measurements => <<
        "#{system_time => non_neg_integer(), "
        "monotonic_time => non_neg_integer()}"
    >>,
    metadata => <<"#{}">>
}).
-telemetry_event(#{
    event => [cozodb, run, stop],
    description =>
        <<"Emitted at the end of the CozoScript execution">>,
    measurements => <<"#{duration => non_neg_integer()}">>,
    metadata => <<"#{}">>
}).
-telemetry_event(#{
    event => [cozodb, run, exception],
    description =>
        <<"Emitted when the CozoScript execution failed">>,
    measurements => <<"">>,
    metadata => <<"#{error => any()}">>
}).

%% =============================================================================
%% API
%% =============================================================================

-doc """
Opens a database with the default engine (aka backend).
""".
-spec open() ->
    {ok, db_handle()} | {error, Reason :: any()} | no_return().

open() ->
    Engine = application:get_env(?APP, engine, mem),
    open(Engine).

-doc """
Opens a database with the provided Engine (aka backend) in the `/tmp` path.
""".
-spec open(Engine :: engine()) ->
    {ok, db_handle()} | {error, Reason :: any()} | no_return().

open(Engine) ->
    DataDir = application:get_env(?APP, data_dir, "/tmp"),
    Path = filename:join([DataDir, "db"]),
    open(Engine, Path).

-doc """
Opens a database with the provided Engine (aka backend) and path.
""".
-spec open(Engine :: engine(), Path :: path()) ->
    {ok, db_handle()} | {error, Reason :: any()} | no_return().

open(Engine, Path) ->
    open(Engine, Path, engine_opts(Engine)).

-doc """
Creates or opens an existing database.

The database has to be explicitely closed using `close/1` for Erlang
to release the allocated ErlNIF resources.
* `Path` is ignored when `Engine` is `mem`
* `Opts` apply only to `tikv` engine

## Engines

### `rocksdb` (cozorocks)
The original RocksDB backend using the cozorocks C++ FFI bridge.
To define options for RocksDB you should make sure a RocksDB configuration file
named `config` is present at `Path` before you call this function.

### `newrocksdb` (rust-rocksdb)
An alternative RocksDB backend using the official rust-rocksdb crate.
This engine supports comprehensive configuration via environment variables.

All options are configured using `COZO_ROCKSDB_*` environment variables:

#### General Options
- `COZO_ROCKSDB_CREATE_IF_MISSING` - Create DB if not exists (default: true for new)
- `COZO_ROCKSDB_PARANOID_CHECKS` - Enable paranoid checks (default: false)
- `COZO_ROCKSDB_MAX_OPEN_FILES` - Max open file handles (default: -1 unlimited)
- `COZO_ROCKSDB_MAX_FILE_OPENING_THREADS` - Threads for opening files

#### Parallelism
- `COZO_ROCKSDB_PARALLELISM` - Set overall parallelism level
- `COZO_ROCKSDB_MAX_BACKGROUND_JOBS` - Max background jobs
- `COZO_ROCKSDB_MAX_SUBCOMPACTIONS` - Max subcompactions

#### Write Buffer
- `COZO_ROCKSDB_WRITE_BUFFER_SIZE` - Write buffer size in bytes (default: 64MB)
- `COZO_ROCKSDB_MAX_WRITE_BUFFER_NUMBER` - Max write buffers (default: 2)
- `COZO_ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE` - Min buffers before merge

#### Compaction
- `COZO_ROCKSDB_COMPACTION_STYLE` - level, universal, or fifo (default: level)
- `COZO_ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER` - L0 compaction trigger
- `COZO_ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER` - L0 slowdown trigger
- `COZO_ROCKSDB_LEVEL0_STOP_WRITES_TRIGGER` - L0 stop writes trigger
- `COZO_ROCKSDB_MAX_BYTES_FOR_LEVEL_BASE` - Max bytes for L1
- `COZO_ROCKSDB_MAX_BYTES_FOR_LEVEL_MULTIPLIER` - Level size multiplier
- `COZO_ROCKSDB_TARGET_FILE_SIZE_BASE` - Target SST file size

#### Compression
- `COZO_ROCKSDB_COMPRESSION_TYPE` - none, snappy, zlib, lz4, lz4hc, zstd
- `COZO_ROCKSDB_BOTTOMMOST_COMPRESSION_TYPE` - Compression for bottommost level

#### Block-Based Table Options
- `COZO_ROCKSDB_BLOCK_SIZE` - Block size in bytes (default: 4KB)
- `COZO_ROCKSDB_BLOCK_CACHE_SIZE` - Block cache size in MB (default: 8MB)
- `COZO_ROCKSDB_BLOOM_FILTER_BITS_PER_KEY` - Bloom filter bits (default: 10)
- `COZO_ROCKSDB_BLOOM_FILTER_BLOCK_BASED` - Use block-based bloom filter

#### Blob Storage (BlobDB)
- `COZO_ROCKSDB_ENABLE_BLOB_FILES` - Enable blob files (default: false)
- `COZO_ROCKSDB_MIN_BLOB_SIZE` - Min size to store in blob
- `COZO_ROCKSDB_BLOB_FILE_SIZE` - Target blob file size
- `COZO_ROCKSDB_BLOB_COMPRESSION_TYPE` - Blob compression type
- `COZO_ROCKSDB_ENABLE_BLOB_GC` - Enable blob garbage collection
- `COZO_ROCKSDB_BLOB_GC_AGE_CUTOFF` - Blob GC age cutoff (0.0-1.0)
- `COZO_ROCKSDB_BLOB_GC_FORCE_THRESHOLD` - Force GC threshold (0.0-1.0)

#### Write-Ahead Log (WAL)
- `COZO_ROCKSDB_WAL_DIR` - WAL directory path
- `COZO_ROCKSDB_WAL_TTL_SECONDS` - WAL time-to-live in seconds
- `COZO_ROCKSDB_WAL_SIZE_LIMIT_MB` - WAL size limit in MB
- `COZO_ROCKSDB_MAX_TOTAL_WAL_SIZE` - Max total WAL size in bytes

#### I/O Options
- `COZO_ROCKSDB_USE_DIRECT_READS` - Use O_DIRECT for reads (default: false)
- `COZO_ROCKSDB_USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION` - Direct I/O for flush
- `COZO_ROCKSDB_ALLOW_MMAP_READS` - Allow mmap for reads (default: false)
- `COZO_ROCKSDB_ALLOW_MMAP_WRITES` - Allow mmap for writes (default: false)
- `COZO_ROCKSDB_BYTES_PER_SYNC` - Bytes written before sync
- `COZO_ROCKSDB_WRITABLE_FILE_MAX_BUFFER_SIZE` - Max writable file buffer

#### Statistics
- `COZO_ROCKSDB_ENABLE_STATISTICS` - Enable statistics (default: false)

Example:
```erlang
%% Configure via environment before opening
os:putenv("COZO_ROCKSDB_WRITE_BUFFER_SIZE", "134217728"),
os:putenv("COZO_ROCKSDB_MAX_OPEN_FILES", "1000"),
os:putenv("COZO_ROCKSDB_COMPRESSION_TYPE", "lz4"),
{ok, Db} = cozodb:open(newrocksdb, "/path/to/db").
```
""".
-spec open(Engine :: engine(), Path :: path(), Opts :: engine_opts()) ->
    {ok, db_handle()} | {error, Reason :: any()} | no_return().

open(Engine, Path, Opts) when is_list(Path), Path =/= [], is_map(Opts) ->
    open(Engine, list_to_binary(Path), Opts);
open(Engine, Path, Opts) when
    is_atom(Engine), is_binary(Path), is_map(Opts)
->
    Engine == mem orelse Engine == sqlite orelse Engine == rocksdb orelse
        Engine == newrocksdb orelse
        ?ERROR(badarg, [Engine, Path, Opts], #{
            1 => "the value must be the atom `mem`, `rocksdb`, `sqlite` or `newrocksdb`"
        }),

    Path =/= <<>> orelse
        ?ERROR(badarg, [Engine, Path, Opts], #{
            2 => "a nonempty string or binary"
        }),

    new(atom_to_binary(Engine), Path, term_to_json_object(Opts)).

-doc """
Closes the database.

Note: With the ResourceArc-based implementation, this function is a no-op.
The database is automatically closed when the handle is garbage collected.
This function is kept for backwards compatibility.
""".
-spec close(DbHandle :: db_handle()) -> ok.

close(DbHandle) when ?IS_DB_HANDLE(DbHandle) ->
    %% No-op: ResourceArc is automatically cleaned up by Erlang GC
    ok.

-doc """
Returns metadata about the database handle.
""".
-spec info(DbHandle :: db_handle()) -> info().

info(DbHandle) when ?IS_DB_HANDLE(DbHandle) ->
    #{
        engine => maps:get(engine, DbHandle),
        path => maps:get(path, DbHandle)
    }.

-doc "".
-spec run(DbHandle :: db_handle(), Script :: script()) ->
    query_return() | no_return().

run(DbHandle, Script) when
    Script == "";
    Script == <<>>
->
    ?ERROR(badarg, [DbHandle, Script], #{
        1 => "script cannot be empty"
    });
run(DbHandle, Script) when is_list(Script) ->
    run(DbHandle, list_to_binary(Script));
run(DbHandle, Script) when ?IS_DB_HANDLE(DbHandle) andalso is_binary(Script) ->
    ReadOnly = false,
    Meta = #{script => Script, db_handle => DbHandle, options => #{}},
    run_script_span(DbHandle, Script, #{}, ReadOnly, Meta).

-doc """

""".
-spec run(
    DbHandle :: db_handle(), Script :: list() | binary(), Opts :: query_opts()
) -> query_return().

run(DbHandle, Script, Opts) when is_list(Script) ->
    run(DbHandle, list_to_binary(Script), Opts);
run(DbHandle, Script, #{encoding := json} = Opts) when
    ?IS_DB_HANDLE(DbHandle) andalso is_binary(Script)
->
    Params = term_to_json_object(maps:get(parameters, Opts, #{})),
    ReadOnly = maps:get(read_only, Opts, false),
    run_script_json_res_nif(?GET_RESOURCE(DbHandle), Script, Params, ReadOnly);
run(DbHandle, Script, #{encoding := map} = Opts) when
    ?IS_DB_HANDLE(DbHandle) andalso is_binary(Script)
->
    Params = term_to_json_object(maps:get(parameters, Opts, #{})),
    ReadOnly = maps:get(read_only, Opts, false),
    run_script_str_res_nif(?GET_RESOURCE(DbHandle), Script, Params, ReadOnly);
run(DbHandle, Script, Opts) when
    ?IS_DB_HANDLE(DbHandle) andalso is_binary(Script) andalso is_map(Opts)
->
    Params = maps:get(parameters, Opts, #{}),
    ReadOnly = maps:get(read_only, Opts, false),
    Meta = #{script => Script, db_handle => DbHandle, options => Opts},
    run_script_span(DbHandle, Script, Params, ReadOnly, Meta).

-doc """
Import data into a database. The data are imported inside a transaction, so
that either all imports are successful, or none are. If conflicts arise
because of concurrent modification to the database, via either CosoScript
queries or other imports, the transaction will fail.
The relations to import into must exist beforehand, and the data given must
match the schema defined.
This API can be used to batch-put or remove data from several stored
relations atomically. The data parameter can contain relation names such as
"rel_a", or relation names prefixed by a minus sign such as "-rel_a". For the
former case, every row given for the relation will be put into the database,
i.e. upsert semantics. For the latter case, the corresponding rows are
removed from the database, and you should only specify the key part of the
rows. As for rm in CozoScript, it is not an error to remove non-existent
rows.
%%
### Erlang Example ===
```
#{
   rel_a => #{
       headers => ["x", "y"],
       rows => [[1, 2], [3, 4]]
   },
   rel_b => #{
       headers => ["z"],
       rows => []
   }
}
```
""".
-spec import(DbHandle :: db_handle(), Relations :: iodata() | relations()) ->
    ok | {error, Reason :: any()}.

import(DbHandle, Relations) when
    ?IS_DB_HANDLE(DbHandle) andalso is_map(Relations)
->
    import(DbHandle, term_to_json_object(Relations));
import(DbHandle, Relations) when
    ?IS_DB_HANDLE(DbHandle) andalso is_binary(Relations)
->
    import_relations_res_nif(?GET_RESOURCE(DbHandle), Relations);
import(DbHandle, Relations) when
    ?IS_DB_HANDLE(DbHandle) andalso is_list(Relations)
->
    import_relations_res_nif(?GET_RESOURCE(DbHandle), iolist_to_binary(Relations)).

-doc """
Export the specified relations in `Relations`.
It is guaranteed that the exported data form a consistent snapshot of what
was stored in the database.
Returns a map with binary keys for the names of relations, and values as maps
containing the `headers` and `rows` of the relation.
""".
-spec export(
    DbHandle :: db_handle(), RelNames :: [relation_name()] | binary()
) ->
    {ok, relations() | binary()} | {error, Reason :: any()}.

export(DbHandle, RelNames) ->
    export(DbHandle, RelNames, #{}).

-doc """
Export the specified relations in `Relations`.
It is guaranteed that the exported data form a consistent snapshot of what
was stored in the database.
Returns a map with binary keys for the names of relations, and values as maps
containing the `headers` and `rows` of the relation.
""".
-spec export(
    DbHandle :: db_handle(),
    RelNames :: [relation_name()] | binary(),
    Opts :: export_opts()
) ->
    {ok, relations() | binary()} | {error, Reason :: any()}.

export(DbHandle, RelNames, #{encoding := json}) when
    ?IS_DB_HANDLE(DbHandle) andalso is_binary(RelNames)
->
    export_relations_json_res_nif(?GET_RESOURCE(DbHandle), RelNames);
export(DbHandle, RelNames, #{encoding := json}) when
    ?IS_DB_HANDLE(DbHandle) andalso is_list(RelNames)
->
    export_relations_json_res_nif(?GET_RESOURCE(DbHandle), RelNames);
export(DbHandle, RelNames, _) when
    ?IS_DB_HANDLE(DbHandle) andalso is_list(RelNames)
->
    export_relations_res_nif(?GET_RESOURCE(DbHandle), RelNames).

-doc """
Exports the database to a SQLite file at `Path`.
To restore the database using this file see {@link restore/2}.
""".
-spec backup(DbHandle :: db_handle(), Path :: path()) ->
    ok | {error, Reason :: any()}.

backup(DbHandle, Path) when is_list(Path), Path =/= [] ->
    backup(DbHandle, list_to_binary(Path));
backup(DbHandle, Path) when ?IS_DB_HANDLE(DbHandle), is_binary(Path) ->
    backup_res_nif(?GET_RESOURCE(DbHandle), Path).

-doc """

""".
-spec restore(DbHandle :: db_handle(), Path :: path()) ->
    ok | {error, Reason :: any()}.

restore(DbHandle, Path) when is_list(Path), Path =/= [] ->
    restore(DbHandle, list_to_binary(Path));
restore(DbHandle, Path) when ?IS_DB_HANDLE(DbHandle), is_binary(Path) ->
    restore_res_nif(?GET_RESOURCE(DbHandle), Path).

-doc """

""".
-spec import_from_backup(
    DbHandle :: db_handle(), Path :: path(), Relations :: []
) ->
    ok | {error, Reason :: any()}.

import_from_backup(DbHandle, Path, Relations) when is_list(Path), Path =/= [] ->
    import_from_backup(DbHandle, list_to_binary(Path), Relations);
import_from_backup(DbHandle, Path, Relations) when
    ?IS_DB_HANDLE(DbHandle), is_binary(Path), is_list(Relations)
->
    import_from_backup_res_nif(?GET_RESOURCE(DbHandle), Path, Relations).

%% =============================================================================
%% API: System Catalogue
%% =============================================================================

-doc """
List all existing relations.
""".
-spec relations(DbHandle :: db_handle()) -> query_return().

relations(DbHandle) ->
    run(DbHandle, <<"::relations">>).

-doc """

""".
-spec create_relation(
    DbHandle :: db_handle(),
    RelName :: atom() | binary() | list(),
    Spec :: relation_spec()
) ->
    ok | {error, Reason :: any()} | no_return().

create_relation(DbHandle, RelName, Spec) when is_atom(RelName) ->
    create_relation(DbHandle, atom_to_binary(RelName), Spec);
create_relation(DbHandle, RelName, Spec) when is_list(RelName) ->
    create_relation(DbHandle, list_to_binary(RelName), Spec);
create_relation(DbHandle, RelName, Spec) when is_binary(RelName), is_map(Spec) ->
    Encoded =
        try
            cozodb_script_utils:encode_relation_spec(Spec)
        catch
            error:{EReason, Message} ->
                ?ERROR(EReason, [DbHandle, RelName, Spec], #{3 => Message})
        end,

    Query = [<<":create">>, $\s, RelName, $\s, Encoded],

    case run(DbHandle, iolist_to_binary(Query)) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, format_error(?FUNCTION_NAME, Reason)}
    end.

-doc """
Removes a relation
""".
-spec remove_relation(DbHandle :: db_handle(), RelName :: binary() | string()) ->
    ok | {error, Reason :: any()}.

remove_relation(DbHandle, RelName) when is_list(RelName) ->
    remove_relation(DbHandle, list_to_binary(RelName));
remove_relation(DbHandle, RelName) when is_binary(RelName) ->
    case run(DbHandle, <<"::remove", $\s, RelName/binary>>) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, format_error(?FUNCTION_NAME, Reason)}
    end.

-doc """
List columns for relation
""".
-spec remove_relations(DbHandle :: db_handle(), RelNames :: [binary()]) ->
    ok | {error, Reason :: any()}.

remove_relations(DbHandle, RelNames0) ->
    RelNames = iolist_to_binary(lists:join(", ", RelNames0)),
    case run(DbHandle, <<"::remove", $\s, RelNames/binary>>) of
        {ok, _} ->
            ok;
        {error, _} = Error ->
            Error
    end.

-doc """
Create index for relation
""".
-spec describe(
    DbHandle :: db_handle(),
    RelName :: binary() | list(),
    Desc :: binary() | list()
) -> query_return().

describe(DbHandle, RelName, Desc) when is_list(RelName) ->
    describe(DbHandle, list_to_binary(RelName), Desc);
describe(DbHandle, RelName, Desc) when is_list(Desc) ->
    describe(DbHandle, RelName, list_to_binary(Desc));
describe(DbHandle, RelName, Desc) ->
    Cmd = <<"::describe", $\s, RelName/binary, $\s, Desc/binary, "?">>,
    run(DbHandle, Cmd).

-doc """
List columns for relation
""".
-spec columns(DbHandle :: db_handle(), RelName :: binary() | list()) ->
    query_return().

columns(DbHandle, RelName) when is_list(RelName) ->
    columns(DbHandle, list_to_binary(RelName));
columns(DbHandle, RelName) ->
    run(DbHandle, <<"::columns", $\s, RelName/binary>>).

-doc """
List indices for relation
""".
-spec indices(DbHandle :: db_handle(), RelName :: binary() | list()) ->
    query_return().

indices(DbHandle, RelName) when is_list(RelName) ->
    indices(DbHandle, list_to_binary(RelName));
indices(DbHandle, RelName) ->
    run(DbHandle, <<"::indices", $\s, RelName/binary>>).

-doc """
Create index for relation
### Hierarchical Navigable Small World (HNSW) Index
The parameters are:
* The dimension `dim` and the data type `dtype` (defaults to `F32`) has to
match the dimensions of any vector you index
* The fields parameter is a list of fields in the table that should be
indexed
* The indexed fields must only contain vectors of the same dimension and
data type, or null, or a list of vectors of the same dimension and data
type
* The distance parameter is the distance metric to use: the options are L2 (
default), Cosine and IP
* The m controls the maximal number of outgoing connections from each node
in the graph
* The ef_construction parameter is the number of nearest neighbors to use
when building the index: see the HNSW paper for details
* The filter parameter, when given, is bound to the fields of the original
relation and only those rows for which the expression evaluates to true are
indexed
* The extend_candidates parameter is a boolean (default false) that
controls whether the index should extend the candidate list with the nearest
neighbors of the nearest neighbors
* The keep_pruned_connections parameter is a boolean (default false) that
controls whether the index should keep pruned connections.

#### Example
```
1> Spec = #{
 type => hnsw,
 dim => 128,
 m => 50,
 ef_construction => 20,
 dtype => f32,
 distance => l2,
 fields => [v],
 filter => <<"k != `foo`">>,
 extend_candidates => false,
 keep_pruned_connections => false
}.
2> create_index(Db, my_relation, Spec).
ok
```
""".
-spec create_index(
    DbHandle :: db_handle(),
    RelName :: binary() | list(),
    Name :: binary() | list(),
    Spec :: index_spec()
) -> ok | {error, Reason :: any()} | no_return().

create_index(DbHandle, RelName, Name, #{type := Type} = Spec0) when
    is_map(Spec0)
->
    Spec = cozodb_script_utils:encode_index_spec(Spec0),
    IndexOp = index_type_op(Type),
    Query = iolist_to_binary([
        $:, $:, IndexOp, $\s, "create", $\s, RelName, $:, Name, $\s, Spec
    ]),
    case run(DbHandle, Query) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, format_error(?FUNCTION_NAME, Reason)}
    end;
create_index(DbHandle, RelName, Name, Spec) ->
    ?ERROR(badarg, [DbHandle, RelName, Name, Spec], #{
        4 =>
            "invalid value for field `type`. "
            "Valid values are `covering`, `hnsw`, `lsh` and `fts`"
    }).

-doc """
Drop index with fully qualified name.
""".
-spec drop_index(DbHandle :: db_handle(), FQN :: binary() | list()) ->
    ok | {error, Reason :: any()} | no_return().

drop_index(DbHandle, FQN) when is_list(FQN) ->
    drop_index(DbHandle, list_to_binary(FQN));
drop_index(DbHandle, FQN) when is_binary(FQN) ->
    Cmd = <<"::index drop", $\s, FQN/binary>>,
    case run(DbHandle, Cmd) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, format_error(?FUNCTION_NAME, Reason)}
    end.

-doc """
Create index for relation
""".
-spec drop_index(
    DbHandle :: db_handle(),
    RelName :: binary() | list(),
    Name :: binary() | list()
) -> ok | {error, Reason :: any()}.

drop_index(DbHandle, RelName, Name) when is_list(RelName) ->
    drop_index(DbHandle, list_to_binary(RelName), Name);
drop_index(DbHandle, RelName, Name) when is_list(Name) ->
    drop_index(DbHandle, RelName, list_to_binary(Name));
drop_index(DbHandle, RelName, Name) when is_binary(RelName), is_binary(Name) ->
    drop_index(DbHandle, <<RelName/binary, ":", Name/binary>>).

-doc """
Returns the list of triggers.
""".
-spec triggers(DbHandle :: db_handle(), RelName :: binary() | list()) ->
    query_return().

triggers(DbHandle, RelName) when is_list(RelName) ->
    triggers(DbHandle, list_to_binary(RelName));
triggers(DbHandle, RelName) ->
    run(DbHandle, <<"::show_triggers", $\s, RelName/binary>>).

-doc """

""".
-spec set_triggers(
    DbHandle :: db_handle(),
    RelName :: binary() | list(),
    Specs :: [trigger_spec()]
) -> query_return().

set_triggers(DbHandle, RelName, Spec) when is_list(RelName) ->
    set_triggers(DbHandle, list_to_binary(RelName), Spec);
set_triggers(DbHandle, RelName, Spec) when is_binary(RelName), is_list(Spec) ->
    Triggers = cozodb_script_utils:encode_triggers_spec(Spec),
    Cmd = iolist_to_binary([
        <<"::set_triggers", $\s, RelName/binary, $\n>> | Triggers
    ]),
    run(DbHandle, Cmd).

-doc """
Calls `set_triggers/3` with and empty specs list.
""".
-spec delete_triggers(DbHandle :: db_handle(), RelName :: binary() | list()) ->
    query_return().

delete_triggers(DbHandle, RelName) ->
    set_triggers(DbHandle, RelName, []).

-doc """

""".
-spec register_callback(DbHandle :: db_handle(), RelName :: binary()) ->
    ok.

register_callback(DbHandle, RelName) when is_list(RelName) ->
    register_callback(DbHandle, list_to_binary(RelName));
register_callback(DbHandle, RelName) when
    ?IS_DB_HANDLE(DbHandle) andalso is_binary(RelName)
->
    register_callback_res_nif(?GET_RESOURCE(DbHandle), RelName).

-doc """

""".
-spec unregister_callback(DbHandle :: db_handle(), Id :: integer()) ->
    boolean().

unregister_callback(DbHandle, Id) when
    ?IS_DB_HANDLE(DbHandle) andalso is_integer(Id)
->
    unregister_callback_res_nif(?GET_RESOURCE(DbHandle), Id).

%% =============================================================================
%% API: Monitor
%% =============================================================================

-doc """

""".
-spec running(DbHandle :: db_handle()) -> query_result().

running(DbHandle) ->
    run(DbHandle, <<"::running">>).

-doc """
Kill the running query associated with identifier `Id`.
See `running/1` to get the list of running queries and their
identifiers.
""".
-spec kill(DbHandle :: db_handle(), Id :: binary()) -> query_result().

kill(DbHandle, Id) when is_binary(Id) ->
    run(DbHandle, <<"::kill", $\s, Id/binary>>).

%% =============================================================================
%% API: Maintenance
%% =============================================================================

-doc """

""".
-spec compact(DbHandle :: db_handle()) -> ok | {error, Reason :: any()}.

compact(Dbhandle) ->
    case run(Dbhandle, <<"::compact">>) of
        {ok, _} ->
            ok;
        {error, _} = Error ->
            Error
    end.

%% =============================================================================
%% API: Debug/Profiling
%% =============================================================================

-doc """
Returns memory statistics from the jemalloc allocator used by the NIF.

This function is useful for debugging memory usage issues in the Rust NIF layer.
The returned map contains:
- `allocated`: Total bytes allocated by the application
- `resident`: Total bytes in physically resident data pages
- `mapped`: Total bytes in active memory mappings
- `retained`: Total bytes retained (not returned to OS)
- `db_handles`: Number of open database handles
- `callback_registrations`: Number of active callback registrations

Note: This only shows memory used by the Rust NIF, not the Erlang VM.
Use `erlang:memory/0` to see Erlang VM memory usage.
""".
-spec memory_stats() -> {ok, map()} | {error, term()}.

memory_stats() ->
    memory_stats_nif().

-doc """
Flush all RocksDB memtables to disk.
This forces a memtable flush which can help release memory held by memtables.
Only works for RocksDB storage backend.
""".
-spec flush_memtables(DbHandle :: db_handle()) -> ok | {error, term()}.

flush_memtables(DbHandle) when ?IS_DB_HANDLE(DbHandle) ->
    flush_memtables_res_nif(?GET_RESOURCE(DbHandle)).

-doc """
Get RocksDB memory statistics.

Returns a map containing:
- `memtable_size`: Current size of all memtables (bytes)
- `block_cache_usage`: Block cache usage (bytes)
- `block_cache_pinned`: Pinned entries in block cache (bytes)
- `table_readers_mem`: Estimated memory used by table readers (bytes)
- `total`: Sum of the above (bytes)

Returns `{error, not_rocksdb}` for non-RocksDB storage backends.
""".
-spec rocksdb_memory_stats(DbHandle :: db_handle()) -> {ok, map()} | {error, term()}.

rocksdb_memory_stats(DbHandle) when ?IS_DB_HANDLE(DbHandle) ->
    rocksdb_memory_stats_res_nif(?GET_RESOURCE(DbHandle)).

-doc """
Dump a jemalloc heap profile to the specified file path.

IMPORTANT: For this to work, the application MUST be started with:
```
MALLOC_CONF="prof:true,prof_prefix:jeprof.out" erl ...
```

The profile can then be analyzed with:
```
jeprof --svg /path/to/beam.smp /path/to/profile.heap > heap.svg
```

Returns:
- `{ok, Path}` - Profile dumped successfully to the given path
- `{error, profiling_not_enabled}` - jemalloc profiling not enabled (need MALLOC_CONF=prof:true)
- `{error, not_jemalloc}` - Not using jemalloc allocator
- `{error, Reason}` - Other error
""".
-spec dump_heap_profile(Path :: string() | binary()) -> {ok, string()} | {error, term()}.

dump_heap_profile(Path) when is_list(Path) ->
    dump_heap_profile(list_to_binary(Path));
dump_heap_profile(Path) when is_binary(Path) ->
    dump_heap_profile_nif(Path).

%% -----------------------------------------------------------------------------
%% @doc Clear all entries from the shared RocksDB block cache.
%%
%% This is a process-global operation that affects ALL RocksDB databases
%% in this BEAM process. After clearing, reads will repopulate the cache
%% as needed, so this provides temporary memory relief.
%%
%% Returns:
%% - `ok` - Cache cleared successfully
%% @end
%% -----------------------------------------------------------------------------
-doc """
Clear all entries from the shared RocksDB block cache.

This releases memory currently used by the block cache but keeps the cache
structure intact. New reads will repopulate the cache as needed.

**Note**: This is a process-global operation affecting ALL RocksDB databases.
""".
-spec clear_block_cache() -> ok.

clear_block_cache() ->
    clear_block_cache_nif().

%% -----------------------------------------------------------------------------
%% @doc Set the capacity of the shared RocksDB block cache in MB.
%%
%% This is a process-global operation that affects ALL RocksDB databases.
%% Setting capacity to 0 effectively disables caching.
%% Setting to a smaller value will trigger eviction of excess entries.
%%
%% Returns:
%% - `ok` - Capacity set successfully
%% @end
%% -----------------------------------------------------------------------------
-doc """
Set the capacity of the shared RocksDB block cache in megabytes.

Setting to 0 effectively disables caching. Setting to a smaller value than
the current usage will trigger eviction of excess entries.

**Note**: This is a process-global operation affecting ALL RocksDB databases.
""".
-spec set_block_cache_capacity(CapacityMB :: non_neg_integer()) -> ok.

set_block_cache_capacity(CapacityMB) when is_integer(CapacityMB), CapacityMB >= 0 ->
    set_block_cache_capacity_nif(CapacityMB).

%% -----------------------------------------------------------------------------
%% @doc Get statistics about the shared RocksDB block cache.
%%
%% Returns a map with:
%% - `capacity` - Total capacity in bytes
%% - `usage` - Current usage in bytes
%% - `pinned_usage` - Pinned entries in bytes (cannot be evicted)
%% @end
%% -----------------------------------------------------------------------------
-doc """
Get statistics about the shared RocksDB block cache.

Returns `{ok, Stats}` where Stats is a map containing:
- `capacity` - Total capacity in bytes
- `usage` - Current usage in bytes
- `pinned_usage` - Pinned entries in bytes (cannot be evicted)

**Note**: This is a process-global statistic covering ALL RocksDB databases.
""".
-spec get_block_cache_stats() -> {ok, map()}.

get_block_cache_stats() ->
    get_block_cache_stats_nif().

%% -----------------------------------------------------------------------------
%% @doc Force jemalloc to return unused memory to the operating system.
%%
%% This purges dirty pages from all jemalloc arenas, making them available
%% to the OS. This can help reduce RSS when memory is no longer needed.
%%
%% Returns:
%% - `{ok, PurgedBytes}` - Number of bytes returned to OS (approximate)
%% - `{error, not_jemalloc}` - Not using jemalloc allocator
%% - `{error, Reason}` - Other error
%% @end
%% -----------------------------------------------------------------------------
-doc """
Force jemalloc to return unused memory to the operating system.

This purges dirty pages from all jemalloc arenas, making them available
to the OS. Useful after large operations to reduce RSS.

Returns:
- `{ok, PurgedBytes}` - Approximate bytes returned to OS
- `{error, not_jemalloc}` - Not using jemalloc allocator
- `{error, Reason}` - Other error
""".
-spec purge_jemalloc() -> {ok, non_neg_integer()} | {error, term()}.

purge_jemalloc() ->
    purge_jemalloc_nif().

-doc """
Configure jemalloc decay times to control how aggressively memory is returned to the OS.

Arguments:
- `DirtyDecayMs` - Time in milliseconds before dirty pages are purged
  - 0 = immediate return (most aggressive, may impact performance)
  - -1 = disable decay (jemalloc default behavior, holds memory longer)
  - 1000 = our default (1 second, 10x more aggressive than jemalloc's 10s default)
- `MuzzyDecayMs` - Time in milliseconds before muzzy pages are purged (same options)

Dirty pages contain recently freed data. Muzzy pages are dirty pages that have been
purged but the virtual memory mapping is retained for potential reuse.

Example:
```erlang
%% Most aggressive - immediate return to OS
cozodb:set_jemalloc_decay(0, 0).

%% Default (set at NIF load time)
cozodb:set_jemalloc_decay(1000, 1000).

%% jemalloc default - least aggressive
cozodb:set_jemalloc_decay(10000, 10000).
```

Returns:
- `ok` - Settings applied successfully
- `{error, not_jemalloc}` - Not using jemalloc allocator
- `{error, Reason}` - Other error
""".
-spec set_jemalloc_decay(DirtyDecayMs :: integer(), MuzzyDecayMs :: integer()) ->
    ok | {error, term()}.

set_jemalloc_decay(DirtyDecayMs, MuzzyDecayMs) when
    is_integer(DirtyDecayMs), is_integer(MuzzyDecayMs)
->
    set_jemalloc_decay_nif(DirtyDecayMs, MuzzyDecayMs).

-doc """
Get current jemalloc decay settings.

Returns:
- `{ok, #{dirty_decay_ms => integer(), muzzy_decay_ms => integer()}}` - Current settings
- `{error, not_jemalloc}` - Not using jemalloc allocator
""".
-spec get_jemalloc_decay() -> {ok, map()} | {error, term()}.

get_jemalloc_decay() ->
    get_jemalloc_decay_nif().

%% =============================================================================
%% API: Utils
%% =============================================================================

-doc """

""".
-spec explain(DbHandle :: db_handle(), Query :: binary() | list()) ->
    query_return().

explain(DbHandle, Query) when is_list(Query) ->
    explain(DbHandle, list_to_binary(Query));
explain(DbHandle, Query) when is_binary(Query) ->
    run(DbHandle, <<"::explain", ${, $\s, Query/binary, $\s, $}>>).

-doc """
Util function that takes a query_result() as argument and returns a list
of rows as maps.
""".
-spec rows_to_maps(query_result()) -> map().

rows_to_maps(#{headers := Headers, rows := Rows, next := _N}) ->
    lists:foldl(
        fun(Row, Acc) ->
            [maps:from_list(lists:zip(Headers, Row)) | Acc]
        end,
        [],
        Rows
    ).

%% =============================================================================
%% PRIVATE: NIFs
%% =============================================================================

%% @private
%% Called by on_load directive
init() ->
    Crate = ?APP,
    %% Rustler macro
    ?load_nif_from_crate(Crate, 0).

%% @private
%% Calls native/cozodb/src/lib.rs::open_res_with_options - returns ResourceArc directly (no HANDLES)
-spec open_res_opts_nif(Engine :: binary(), Path :: binary(), Opts :: binary()) ->
    {ok, reference()} | {error, Reason :: any()}.

open_res_opts_nif(_Engine, _Path, _Opts) ->
    ?NIF_NOT_LOADED.

%% @private
run_script_span(DbHandle, Script, Params, ReadOnly, Meta) when
    is_list(Params)
->
    run_script_span(DbHandle, Script, maps:from_list(Params), ReadOnly, Meta);
run_script_span(DbHandle, Script, Params, ReadOnly, Meta) when is_map(Params) ->
    Resource = ?GET_RESOURCE(DbHandle),
    telemetry:span([cozodb, run], Meta, fun() ->
        case run_script_res_nif(Resource, Script, Params, ReadOnly) of
            {ok, _} = OK ->
                {OK, Meta};
            {error, Reason} ->
                Formatted = format_error(Reason),
                Error = {error, Formatted},
                {Error, Meta#{error => Formatted}}
        end
    end).

%% @private
-spec memory_stats_nif() ->
    {ok, map()} | {error, term()}.

memory_stats_nif() ->
    ?NIF_NOT_LOADED.

%% @private
-spec dump_heap_profile_nif(Path :: binary()) ->
    {ok, binary()} | {error, term()}.

dump_heap_profile_nif(_Path) ->
    ?NIF_NOT_LOADED.

%% @private
-spec clear_block_cache_nif() -> ok.

clear_block_cache_nif() ->
    ?NIF_NOT_LOADED.

%% @private
-spec set_block_cache_capacity_nif(CapacityMB :: non_neg_integer()) -> ok.

set_block_cache_capacity_nif(_CapacityMB) ->
    ?NIF_NOT_LOADED.

%% @private
-spec get_block_cache_stats_nif() -> {ok, map()}.

get_block_cache_stats_nif() ->
    ?NIF_NOT_LOADED.

%% @private
-spec purge_jemalloc_nif() -> {ok, non_neg_integer()} | {error, term()}.

purge_jemalloc_nif() ->
    ?NIF_NOT_LOADED.

%% @private
-spec set_jemalloc_decay_nif(DirtyDecayMs :: integer(), MuzzyDecayMs :: integer()) ->
    ok | {error, term()}.

set_jemalloc_decay_nif(_DirtyDecayMs, _MuzzyDecayMs) ->
    ?NIF_NOT_LOADED.

%% @private
-spec get_jemalloc_decay_nif() -> {ok, map()} | {error, term()}.

get_jemalloc_decay_nif() ->
    ?NIF_NOT_LOADED.

%% -----------------------------------------------------------------------------
%% Resource-based NIF stubs (lock-free operations)
%% -----------------------------------------------------------------------------

%% @private
%% Calls native/cozodb/src/lib.rs::run_script_res
-spec run_script_res_nif(
    Resource :: reference(),
    Script :: binary(),
    Params :: map(),
    ReadOnly :: boolean()
) -> query_return().

run_script_res_nif(_Resource, _Script, _Params, _ReadOnly) ->
    ?NIF_NOT_LOADED.

%% @private
%% Calls native/cozodb/src/lib.rs::run_script_str_res
-spec run_script_str_res_nif(
    Resource :: reference(),
    Script :: binary(),
    Params :: binary(),
    ReadOnly :: boolean()
) -> {ok, binary()}.

run_script_str_res_nif(_Resource, _Script, _Params, _ReadOnly) ->
    ?NIF_NOT_LOADED.

%% @private
%% Calls native/cozodb/src/lib.rs::run_script_json_res
-spec run_script_json_res_nif(
    Resource :: reference(),
    Script :: binary(),
    Params :: binary(),
    ReadOnly :: boolean()
) -> {ok, binary()}.

run_script_json_res_nif(_Resource, _Script, _Params, _ReadOnly) ->
    ?NIF_NOT_LOADED.

%% @private
-spec import_relations_res_nif(Resource :: reference(), Relations :: binary()) ->
    ok | {error, Reason :: any()}.

import_relations_res_nif(_Resource, _Relations) ->
    ?NIF_NOT_LOADED.

%% @private
-spec export_relations_res_nif(Resource :: reference(), Relations :: [binary()]) ->
    {ok, map()} | {error, Reason :: any()}.

export_relations_res_nif(_Resource, _Relations) ->
    ?NIF_NOT_LOADED.

%% @private
-spec export_relations_json_res_nif(Resource :: reference(), Relations :: [binary()]) ->
    {ok, binary()} | {error, Reason :: any()}.

export_relations_json_res_nif(_Resource, _Relations) ->
    ?NIF_NOT_LOADED.

%% @private
-spec backup_res_nif(Resource :: reference(), Path :: binary()) ->
    ok | {error, Reason :: any()}.

backup_res_nif(_Resource, _Path) ->
    ?NIF_NOT_LOADED.

%% @private
-spec restore_res_nif(Resource :: reference(), Path :: binary()) ->
    ok | {error, Reason :: any()}.

restore_res_nif(_Resource, _Path) ->
    ?NIF_NOT_LOADED.

%% @private
-spec import_from_backup_res_nif(
    Resource :: reference(), Path :: binary(), Relations :: [binary()]
) ->
    ok | {error, Reason :: any()}.

import_from_backup_res_nif(_Resource, _Path, _Relations) ->
    ?NIF_NOT_LOADED.

%% @private
-spec register_callback_res_nif(Resource :: reference(), RelName :: binary()) ->
    {ok, integer()}.

register_callback_res_nif(_Resource, _RelName) ->
    ?NIF_NOT_LOADED.

%% @private
-spec unregister_callback_res_nif(Resource :: reference(), Id :: integer()) ->
    boolean().

unregister_callback_res_nif(_Resource, _Id) ->
    ?NIF_NOT_LOADED.

%% @private
-spec flush_memtables_res_nif(Resource :: reference()) ->
    ok | {error, term()}.

flush_memtables_res_nif(_Resource) ->
    ?NIF_NOT_LOADED.

%% @private
-spec rocksdb_memory_stats_res_nif(Resource :: reference()) ->
    {ok, map()} | {error, term()}.

rocksdb_memory_stats_res_nif(_Resource) ->
    ?NIF_NOT_LOADED.

%% =============================================================================
%% PRIVATE: UTILS
%% =============================================================================

%% @private
%% Opens a database and returns a handle containing the ResourceArc.
%% This uses the lock-free path internally - no HANDLES mutex.
new(Engine, Path, Opts) when
    is_binary(Engine), is_binary(Path), is_binary(Opts)
->
    try
        case open_res_opts_nif(Engine, Path, Opts) of
            {ok, Resource} ->
                %% Wrap in a map to maintain backwards-compatible API
                {ok, #{resource => Resource, engine => Engine, path => Path}};
            {error, _} = Error ->
                Error
        end
    catch
        Class:Reason:Stacktrace ->
            ?LOG_ERROR(#{
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {error, Reason}
    end.

%% @private
format_error(#{message := Msg} = Map) ->
    Map#{message => format_error(Msg)};
format_error(<<"Running query is killed before completion">>) ->
    timeout;
format_error(Reason) ->
    Reason.

%% @private

format_error(Op, #{message := Msg} = Map) ->
    Map#{message => format_error(Op, Msg)};
format_error(Op, Reason) when is_binary(Reason) ->
    %% #{FUNCTION_NAME => [{MatchRule, Cozo string pattern, Return]}
    AllRules = #{
        create_relation => [
            {match_suffix, <<"conflicts with an existing one">>, already_exists}
        ],
        create_index => [
            {match_suffix, <<"already exists">>, already_exists}
        ],
        remove_relation => [
            {match_prefix, <<"Cannot find requested stored relation">>, not_found}
        ]
    },

    %% Predicated used in lists:search
    Pred = fun
        ({match_suffix, Pattern, _Format}) ->
            Suffix = binary:longest_common_suffix([Reason, Pattern]),
            Suffix == byte_size(Pattern);
        ({match_prefix, Pattern, _Format}) ->
            Suffix = binary:longest_common_prefix([Reason, Pattern]),
            Suffix == byte_size(Pattern);
        (_) ->
            false
    end,

    %% Get rules associated with Op
    OpRules = maps:get(Op, AllRules, []),

    %% Try to match rules and return the term defined by the rule,
    %% otherwsie return Reason
    case lists:search(Pred, OpRules) of
        {value, {_, _, Return}} ->
            Return;
        false ->
            Reason
    end;
format_error(_, Reason) ->
    Reason.

%% @private
engine_opts(mem) ->
    #{};
engine_opts(sqlite) ->
    application:get_env(?APP, sqlite_options, #{});
engine_opts(rocksdb) ->
    application:get_env(?APP, rocksdb_options, #{});
engine_opts(_Other) ->
    #{}.

%% @private
index_type_op(covering) -> <<"index">>;
index_type_op(fts) -> <<"fts">>;
index_type_op(hnsw) -> <<"hnsw">>;
index_type_op(lsh) -> <<"lsh">>;
index_type_op(_) -> error(badarg).

%% @private
-spec term_to_json_object(Term :: map() | [{atom() | binary(), any()}]) ->
    iodata() | no_return().

term_to_json_object(Term) when is_list(Term) ->
    term_to_json_object(maps:from_list(Term));
term_to_json_object(Term) when is_map(Term) ->
    try
        iolist_to_binary(json:encode(Term))
    catch
        error:function_clause:Stacktrace ->
            Msg =
                case Stacktrace of
                    [{json, key, [Key, _], _} | _] ->
                        lists:flatten(
                            io_lib:format("invalid JSON key '~p'", [Key])
                        );
                    _ ->
                        "json encoding failed"
                end,
            error({badarg, Msg})
    end.

%% =============================================================================
%% TESTS
%% =============================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.
