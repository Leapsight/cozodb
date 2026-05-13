%% =============================================================================
%%  cozodb_migrator_runner.erl -
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

-module(cozodb_migrator_runner).

-moduledoc """
Executes individual migration modules with timing, optional backup,
and telemetry integration.
""".

-export([
    run_up/3,
    run_down/3
]).

-type run_opts() :: #{
    backup_path => binary()
}.

%% =============================================================================
%% API
%% =============================================================================

-doc """
Execute a migration's `up/1` callback.

If the migration requests a backup (via `backup/0` returning `true`),
a backup is taken before execution. The backup path is derived from
`backup_path` in opts, defaulting to `/tmp/cozodb_migration_backup_VERSION`.

Wraps the execution in a telemetry span `[cozodb, migrator, migration, up]`.
""".
-spec run_up(cozodb:db_handle(), module(), run_opts()) ->
    {ok, non_neg_integer()} | {error, term()}.

run_up(Db, Module, Opts) ->
    maybe
        ok ?= maybe_backup(Db, Module, Opts),
        run_callback(Db, Module, up, Opts)
    end.

-doc """
Execute a migration's `down/1` callback.

Returns `{error, no_down_callback}` if the module doesn't export `down/1`.

Wraps the execution in a telemetry span `[cozodb, migrator, migration, down]`.
""".
-spec run_down(cozodb:db_handle(), module(), run_opts()) ->
    {ok, non_neg_integer()} | {error, term()}.

run_down(Db, Module, Opts) ->
    case cozodb_migration:has_down(Module) of
        true ->
            run_callback(Db, Module, down, Opts);
        false ->
            {error, no_down_callback}
    end.

%% =============================================================================
%% PRIVATE
%% =============================================================================

-spec maybe_backup(cozodb:db_handle(), module(), run_opts()) ->
    ok | {error, term()}.

maybe_backup(Db, Module, Opts) ->
    case cozodb_migration:has_backup(Module) of
        true ->
            {ok, Version} = cozodb_migration:version(Module),
            BackupPath = maps:get(
                backup_path,
                Opts,
                iolist_to_binary([
                    <<"/tmp/cozodb_migration_backup_">>,
                    integer_to_binary(Version)
                ])
            ),
            %% Remove any existing backup file to avoid conflicts
            _ = file:delete(BackupPath),
            cozodb:backup(Db, BackupPath);
        false ->
            ok
    end.

-spec run_callback(cozodb:db_handle(), module(), up | down, run_opts()) ->
    {ok, non_neg_integer()} | {error, term()}.

run_callback(Db, Module, Direction, _Opts) ->
    {ok, Version} = cozodb_migration:version(Module),
    Meta = #{
        module => Module,
        version => Version,
        direction => Direction
    },
    T0 = erlang:monotonic_time(millisecond),
    try
        telemetry:span(
            [cozodb, migrator, migration, Direction],
            Meta,
            fun() ->
                Result = Module:Direction(Db),
                {Result, Meta}
            end
        )
    of
        ok ->
            {ok, erlang:monotonic_time(millisecond) - T0};
        {error, _} = Error ->
            Error
    catch
        Class:Reason:Stacktrace ->
            {error, {exception, Class, Reason, Stacktrace}}
    end.
