%% =============================================================================
%%  cozodb_migrator_history.erl -
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

-module(cozodb_migrator_history).

-moduledoc """
Manages the `cozodb_migrations` relation in CozoDB for tracking migration history.

The relation schema:
```
cozodb_migrations {
    version: Int =>
        name: String,
        checksum: String,
        applied_at: Float,
        execution_time_ms: Int,
        status: String
}
```

Status values: `"applied"`, `"failed"`, `"rolled_back"`
""".

-export([
    ensure_relation/1,
    applied_versions/1,
    record_migration/2,
    record_failure/2,
    record_rollback/2,
    delete_record/2,
    validate_checksums/2
]).

-define(RELATION_NAME, <<"cozodb_migrations">>).

-define(RELATION_SPEC, #{
    keys => [{<<"version">>, #{type => int}}],
    columns => [
        {<<"name">>, #{type => string}},
        {<<"checksum">>, #{type => string}},
        {<<"applied_at">>, #{type => float}},
        {<<"execution_time_ms">>, #{type => int}},
        {<<"status">>, #{type => string}}
    ]
}).

%% =============================================================================
%% API
%% =============================================================================

-doc """
Ensure the `cozodb_migrations` relation exists. Safe to call multiple times.
""".
-spec ensure_relation(cozodb:db_handle()) -> ok | {error, term()}.

ensure_relation(Db) ->
    cozodb_migrator_utils:ensure_relation(Db, ?RELATION_NAME, ?RELATION_SPEC).

-doc """
Return the list of applied migrations from the `cozodb_migrations` relation.

Returns `[{Version, Checksum, Status}]` sorted by version ascending.
""".
-spec applied_versions(cozodb:db_handle()) ->
    {ok, [{cozodb_migration:version(), binary(), binary()}]} | {error, term()}.

applied_versions(Db) ->
    Query = <<"?[version, checksum, status] := *cozodb_migrations{version, checksum, status}">>,
    case cozodb:run(Db, Query) of
        {ok, #{rows := Rows}} ->
            Parsed = [{V, C, S} || [V, C, S] <- Rows],
            Sorted = lists:keysort(1, Parsed),
            {ok, Sorted};
        {error, _} = Error ->
            Error
    end.

-doc """
Record a successfully applied migration.
""".
-spec record_migration(cozodb:db_handle(), cozodb_migration:migration_info()) ->
    ok | {error, term()}.

record_migration(Db, Info) ->
    import_record(Db, Info, <<"applied">>).

-doc """
Record a failed migration attempt.
""".
-spec record_failure(cozodb:db_handle(), cozodb_migration:migration_info()) ->
    ok | {error, term()}.

record_failure(Db, Info) ->
    import_record(Db, Info, <<"failed">>).

-doc """
Record a rolled-back migration. Updates the existing row's status to
`"rolled_back"`.
""".
-spec record_rollback(cozodb:db_handle(), cozodb_migration:version()) ->
    ok | {error, term()}.

record_rollback(Db, Version) ->
    %% Query the existing record, then re-import with rolled_back status
    Query = iolist_to_binary([
        <<"?[version, name, checksum, applied_at, execution_time_ms] := ">>,
        <<"*cozodb_migrations{version, name, checksum, applied_at, execution_time_ms}, ">>,
        <<"version == ">>,
        integer_to_binary(Version)
    ]),
    case cozodb:run(Db, Query) of
        {ok, #{rows := [[V, Name, Checksum, _AppliedAt, ExecTime]]}} ->
            Now = erlang:system_time(millisecond) / 1000,
            cozodb:import(Db, #{
                ?RELATION_NAME => #{
                    headers => [
                        <<"version">>,
                        <<"name">>,
                        <<"checksum">>,
                        <<"applied_at">>,
                        <<"execution_time_ms">>,
                        <<"status">>
                    ],
                    rows => [[V, Name, Checksum, Now, ExecTime, <<"rolled_back">>]]
                }
            });
        {ok, #{rows := []}} ->
            {error, {not_found, Version}};
        {error, _} = Error ->
            Error
    end.

-doc """
Delete a migration record from `cozodb_migrations`.
""".
-spec delete_record(cozodb:db_handle(), cozodb_migration:version()) ->
    ok | {error, term()}.

delete_record(Db, Version) ->
    RemoveKey = <<"-cozodb_migrations">>,
    %% First query to get the full row
    Query = iolist_to_binary([
        <<"?[version, name, checksum, applied_at, execution_time_ms, status] := ">>,
        <<"*cozodb_migrations{version, name, checksum, applied_at, execution_time_ms, status}, ">>,
        <<"version == ">>,
        integer_to_binary(Version)
    ]),
    case cozodb:run(Db, Query) of
        {ok, #{rows := [Row]}} ->
            cozodb:import(Db, #{
                RemoveKey => #{
                    headers => [
                        <<"version">>,
                        <<"name">>,
                        <<"checksum">>,
                        <<"applied_at">>,
                        <<"execution_time_ms">>,
                        <<"status">>
                    ],
                    rows => [Row]
                }
            });
        {ok, #{rows := []}} ->
            ok;
        {error, _} = Error ->
            Error
    end.

-doc """
Validate that stored checksums match current migration module checksums.

Returns `ok` if all applied migrations have matching checksums, or
`{error, {checksum_mismatch, [{Version, StoredChecksum, ComputedChecksum}]}}`
if any mismatch is found.
""".
-spec validate_checksums(
    cozodb:db_handle(),
    [{cozodb_migration:version(), module()}]
) -> ok | {error, term()}.

validate_checksums(Db, SortedMigrations) ->
    case applied_versions(Db) of
        {ok, Applied} ->
            AppliedMap = maps:from_list([{V, C} || {V, C, _S} <- Applied]),
            Mismatches = lists:filtermap(
                fun({Version, Module}) ->
                    case maps:find(Version, AppliedMap) of
                        {ok, StoredChecksum} ->
                            ComputedChecksum = cozodb_migration:checksum(Module),
                            case StoredChecksum =:= ComputedChecksum of
                                true ->
                                    false;
                                false ->
                                    {true, {Version, StoredChecksum, ComputedChecksum}}
                            end;
                        error ->
                            false
                    end
                end,
                SortedMigrations
            ),
            case Mismatches of
                [] -> ok;
                _ -> {error, {checksum_mismatch, Mismatches}}
            end;
        {error, _} = Error ->
            Error
    end.

%% =============================================================================
%% PRIVATE
%% =============================================================================

-spec import_record(cozodb:db_handle(), cozodb_migration:migration_info(), binary()) ->
    ok | {error, term()}.

import_record(Db, Info, Status) ->
    #{
        version := Version,
        name := Name,
        checksum := Checksum,
        execution_time_ms := ExecTimeMs
    } = Info,
    Now = erlang:system_time(millisecond) / 1000,
    cozodb:import(Db, #{
        ?RELATION_NAME => #{
            headers => [
                <<"version">>,
                <<"name">>,
                <<"checksum">>,
                <<"applied_at">>,
                <<"execution_time_ms">>,
                <<"status">>
            ],
            rows => [[Version, Name, Checksum, Now, ExecTimeMs, Status]]
        }
    }).
