%% =============================================================================
%%  cozodb_migrator.erl -
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

-module(cozodb_migrator).

-moduledoc """
Public API for the CozoDB migration system.

CozoDB is a Datalog-based database with no built-in `ALTER TABLE` mechanism.
Schema changes require recreating relations (export data, drop, create with
new schema, re-import). This module provides a structured, versioned migration
system to manage those changes safely.

All mutation operations (`migrate/2,3`, `rollback/3`, `baseline/3`) are
serialized through `cozodb_migrator_server` to prevent concurrent execution
within a node.

## Writing Migrations

Each migration is an Erlang module implementing the `cozodb_migration`
behaviour. Modules must follow the naming convention:

```
cozodb_migration_YYYYMMDDHHMMSS_description
```

The timestamp prefix is the migration's version and determines execution
order. For example:

```erlang
-module(cozodb_migration_20260302120000_create_users).
-behaviour(cozodb_migration).
-export([up/1, down/1]).

up(Db) ->
    cozodb:create_relation(Db, <<"users">>, #{
        keys => [{<<"id">>, #{type => int}}],
        columns => [
            {<<"name">>, #{type => string}},
            {<<"email">>, #{type => string}}
        ]
    }).

down(Db) ->
    {ok, _} = cozodb:remove_relation(Db, <<"users">>),
    ok.
```

### Callbacks

- `up/1` (required) - Apply the migration. Receives the database handle.
- `down/1` (optional) - Reverse the migration. Required for rollback support.
- `backup/0` (optional) - Return `true` to trigger a database backup before
  this migration runs.
- `description/0` (optional) - Return a human-readable binary description.

## Applying Migrations

Pass the database handle and the list of all migration modules. The migrator
determines which are pending and applies them in version order:

```erlang
Migrations = [
    cozodb_migration_20260302120000_create_users,
    cozodb_migration_20260302130000_create_posts,
    cozodb_migration_20260303090000_add_posts_index
],

%% Apply all pending migrations
ok = cozodb_migrator:migrate(Db, Migrations).
```

### Options

Use `migrate/3` to pass options:

```erlang
%% Apply only up to a specific version
ok = cozodb_migrator:migrate(Db, Migrations, #{
    to => 20260302130000
}).

%% Preview pending migrations without executing them
{ok, Pending} = cozodb_migrator:migrate(Db, Migrations, #{
    dry_run => true
}).
```

## Checking Status

`status/2` returns the state of every known migration. This is a read-only
operation:

```erlang
{ok, StatusList} = cozodb_migrator:status(Db, Migrations).
%% Each element is a map:
%% #{
%%     version => 20260302120000,
%%     module => cozodb_migration_20260302120000_create_users,
%%     name => <<"create_users">>,
%%     checksum => <<"a1b2c3...">>,
%%     status => <<"applied">> | <<"pending">> | <<"failed">> | <<"rolled_back">>
%% }
```

## Rolling Back

Rollback the last N applied migrations. Each migration being rolled back must
export a `down/1` callback:

```erlang
%% Rollback the most recent migration
ok = cozodb_migrator:rollback(Db, Migrations, 1).

%% Rollback the last 3
ok = cozodb_migrator:rollback(Db, Migrations, 3).
```

## Baselining an Existing Database

When adopting migrations for a database that already has a schema in place,
use `baseline/3` to mark all migrations up to a version as applied without
executing them:

```erlang
ok = cozodb_migrator:baseline(Db, Migrations, 20260302130000).
```

## Schema Change Helpers

Since CozoDB has no `ALTER TABLE`, the `cozodb_migrator_utils` module
provides helpers for common migration patterns:

### Recreating a Relation (Add/Remove/Rename Columns)

```erlang
up(Db) ->
    cozodb_migrator_utils:recreate_relation(
        Db,
        <<"users">>,
        #{
            keys => [{<<"id">>, #{type => int}}],
            columns => [
                {<<"name">>, #{type => string}},
                {<<"age">>, #{type => int}}
            ]
        },
        #{
            %% Provide defaults for new columns not in the old data
            column_mapping => #{<<"age">> => 0},
            %% Or apply arbitrary per-row transforms
            transform => fun(Row) ->
                Row#{<<"name">> => string:titlecase(maps:get(<<"name">>, Row))}
            end
        }
    ).
```

Options for `recreate_relation/4`:
- `column_mapping` - Map of new column names to default values
- `transform` - Function applied to each row (as a map) during re-import
- `skip_indices` - Don't recreate indices (default: `false`)
- `skip_triggers` - Don't recreate triggers (default: `false`)

### Rebuilding an Index

CozoDB indices created after data insertion don't retroactively index
existing rows. Use `reindex/4` to rebuild:

```erlang
up(Db) ->
    cozodb_migrator_utils:reindex(
        Db,
        <<"users">>,
        <<"users_by_email">>,
        #{type => covering, fields => [<<"email">>]}
    ).
```

### Other Helpers

```erlang
%% Check if a relation exists
true = cozodb_migrator_utils:relation_exists(Db, <<"users">>).

%% Create only if it doesn't exist
ok = cozodb_migrator_utils:ensure_relation(Db, <<"users">>, Spec).

%% Rename a relation
ok = cozodb_migrator_utils:rename_relation(Db, <<"old_name">>, <<"new_name">>).

%% Copy data between relations with optional transform
ok = cozodb_migrator_utils:copy_relation(Db, <<"source">>, <<"target">>,
    fun(Row) -> Row end
).
```

## Integrity Checks

On each `migrate/2,3` call, the migrator validates that the SHA-256 checksums
of already-applied migration modules match the checksums recorded at the time
they were applied. If a migration module has been modified after being applied,
the migrator returns `{error, {checksum_mismatch, [...]}}`.

## Telemetry

The following `telemetry` spans are emitted:

- `[cozodb, migrator, migrate]` - Wraps the full `migrate` call
- `[cozodb, migrator, rollback]` - Wraps the full `rollback` call
- `[cozodb, migrator, migration, up]` - Wraps each individual `up/1` callback
- `[cozodb, migrator, migration, down]` - Wraps each individual `down/1` callback
""".

-export([
    migrate/2,
    migrate/3,
    rollback/3,
    status/2,
    baseline/3
]).

-type migrate_opts() :: #{
    to => cozodb_migration:version(),
    dry_run => boolean()
}.

%% =============================================================================
%% API
%% =============================================================================

-doc """
Apply all pending migrations.
""".
-spec migrate(cozodb:db_handle(), [module()]) -> ok | {error, term()}.

migrate(Db, Migrations) ->
    migrate(Db, Migrations, #{}).

-doc """
Apply pending migrations with options.

Options:
- `to` - Only apply migrations up to (and including) this version
- `dry_run` - Return the list of pending migrations without executing them
""".
-spec migrate(cozodb:db_handle(), [module()], migrate_opts()) ->
    ok | {ok, [cozodb_migration:migration_info()]} | {error, term()}.

migrate(Db, Migrations, Opts) ->
    telemetry:span(
        [cozodb, migrator, migrate],
        #{db => Db},
        fun() ->
            Result = gen_server:call(
                cozodb_migrator_server,
                {migrate, Db, Migrations, Opts},
                infinity
            ),
            {Result, #{db => Db}}
        end
    ).

-doc """
Rollback the last `Steps` applied migrations.
""".
-spec rollback(cozodb:db_handle(), [module()], pos_integer()) ->
    ok | {error, term()}.

rollback(Db, Migrations, Steps) ->
    telemetry:span(
        [cozodb, migrator, rollback],
        #{db => Db},
        fun() ->
            Result = gen_server:call(
                cozodb_migrator_server,
                {rollback, Db, Migrations, Steps},
                infinity
            ),
            {Result, #{db => Db}}
        end
    ).

-doc """
Return the status of all known migrations.

This is a read-only operation that does not go through the gen_server.

Returns a list of migration info maps, each with a `status` field that is
one of:
- `<<"applied">>` - Migration has been applied
- `<<"failed">>` - Migration failed during execution
- `<<"rolled_back">>` - Migration was rolled back
- `<<"pending">>` - Migration has not been applied yet
""".
-spec status(cozodb:db_handle(), [module()]) ->
    {ok, [cozodb_migration:migration_info()]} | {error, term()}.

status(Db, Migrations) ->
    maybe
        ok ?= cozodb_migrator_history:ensure_relation(Db),
        {ok, Sorted} ?= cozodb_migration:sort(Migrations),
        {ok, Applied} ?= cozodb_migrator_history:applied_versions(Db),
        AppliedMap = maps:from_list([{V, {C, S}} || {V, C, S} <- Applied]),
        StatusList = lists:map(
            fun({Version, Module}) ->
                Info = #{
                    version => Version,
                    module => Module,
                    name => cozodb_migration:name(Module),
                    checksum => cozodb_migration:checksum(Module)
                },
                case maps:find(Version, AppliedMap) of
                    {ok, {_Checksum, Status}} ->
                        Info#{status => Status};
                    error ->
                        Info#{status => <<"pending">>}
                end
            end,
            Sorted
        ),
        {ok, StatusList}
    end.

-doc """
Mark all migrations up to `Version` as applied without executing them.

Use this when adopting the migration system for an existing database
that already has the schema in place.
""".
-spec baseline(cozodb:db_handle(), [module()], cozodb_migration:version()) ->
    ok | {error, term()}.

baseline(Db, Migrations, Version) ->
    gen_server:call(
        cozodb_migrator_server,
        {baseline, Db, Migrations, Version},
        infinity
    ).
