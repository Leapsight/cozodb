%% =============================================================================
%%  cozodb_migrator_server.erl -
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

-module(cozodb_migrator_server).

-moduledoc """
A gen_server providing intra-node serialization for migration operations.

All mutation operations (migrate, rollback, baseline) are serialized
through this server to prevent concurrent migration execution.
""".

-behaviour(gen_server).

-export([start_link/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-record(state, {}).

%% =============================================================================
%% API
%% =============================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================

init([]) ->
    {ok, #state{}}.

handle_call({migrate, Db, Migrations, Opts}, _From, State) ->
    Result = do_migrate(Db, Migrations, Opts),
    {reply, Result, State};
handle_call({rollback, Db, Migrations, Steps}, _From, State) ->
    Result = do_rollback(Db, Migrations, Steps),
    {reply, Result, State};
handle_call({baseline, Db, Migrations, Version}, _From, State) ->
    Result = do_baseline(Db, Migrations, Version),
    {reply, Result, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%% =============================================================================
%% PRIVATE
%% =============================================================================

-spec do_migrate(cozodb:db_handle(), [module()], map()) ->
    ok | {ok, [cozodb_migration:migration_info()]} | {error, term()}.

do_migrate(Db, Migrations, Opts) ->
    maybe
        %% 1. Ensure _migrations relation exists
        ok ?= cozodb_migrator_history:ensure_relation(Db),
        %% 2. Sort migrations by version
        {ok, Sorted} ?= cozodb_migration:sort(Migrations),
        %% 3. Validate checksums of already-applied migrations
        ok ?= cozodb_migrator_history:validate_checksums(Db, Sorted),
        %% 4. Determine pending migrations
        {ok, Applied} ?= cozodb_migrator_history:applied_versions(Db),
        AppliedSet = applied_set(Applied),
        Pending = [
            {V, M}
         || {V, M} <- Sorted,
            not maps:is_key(V, AppliedSet)
        ],
        %% 5. Apply version filter
        Filtered = apply_version_filter(Pending, Opts),
        %% 6. Handle dry_run
        case maps:get(dry_run, Opts, false) of
            true ->
                {ok, [build_migration_info(V, M) || {V, M} <- Filtered]};
            false ->
                run_pending(Db, Filtered, Opts)
        end
    end.

-spec do_rollback(cozodb:db_handle(), [module()], pos_integer()) ->
    ok | {error, term()}.

do_rollback(Db, Migrations, Steps) ->
    maybe
        ok ?= cozodb_migrator_history:ensure_relation(Db),
        {ok, Sorted} ?= cozodb_migration:sort(Migrations),
        {ok, Applied} ?= cozodb_migrator_history:applied_versions(Db),
        %% Get applied versions in reverse order
        AppliedVersions = lists:reverse([V || {V, _C, S} <- Applied, S =:= <<"applied">>]),
        %% Take Steps from top
        ToRollback = lists:sublist(AppliedVersions, Steps),
        %% Build module lookup
        ModuleLookup = maps:from_list(Sorted),
        run_rollbacks(Db, ToRollback, ModuleLookup)
    end.

-spec do_baseline(cozodb:db_handle(), [module()], cozodb_migration:version()) ->
    ok | {error, term()}.

do_baseline(Db, Migrations, Version) ->
    maybe
        ok ?= cozodb_migrator_history:ensure_relation(Db),
        {ok, Sorted} ?= cozodb_migration:sort(Migrations),
        %% Record all migrations up to Version as applied (with zero execution time)
        ToBaseline = [{V, M} || {V, M} <- Sorted, V =< Version],
        baseline_all(Db, ToBaseline)
    end.

-spec run_pending(cozodb:db_handle(), [{cozodb_migration:version(), module()}], map()) ->
    ok | {error, term()}.

run_pending(_Db, [], _Opts) ->
    ok;
run_pending(Db, [{Version, Module} | Rest], Opts) ->
    Info = build_migration_info(Version, Module),
    case cozodb_migrator_runner:run_up(Db, Module, Opts) of
        {ok, DurationMs} ->
            ok = cozodb_migrator_history:record_migration(
                Db, Info#{execution_time_ms => DurationMs}
            ),
            run_pending(Db, Rest, Opts);
        {error, Reason} ->
            _ = cozodb_migrator_history:record_failure(
                Db, Info#{execution_time_ms => 0}
            ),
            {error, {migration_failed, Version, Reason}}
    end.

-spec run_rollbacks(cozodb:db_handle(), [cozodb_migration:version()], map()) ->
    ok | {error, term()}.

run_rollbacks(_Db, [], _ModuleLookup) ->
    ok;
run_rollbacks(Db, [Version | Rest], ModuleLookup) ->
    case maps:find(Version, ModuleLookup) of
        {ok, Module} ->
            case cozodb_migrator_runner:run_down(Db, Module, #{}) of
                {ok, _DurationMs} ->
                    case cozodb_migrator_history:record_rollback(Db, Version) of
                        ok ->
                            run_rollbacks(Db, Rest, ModuleLookup);
                        {error, _} = Error ->
                            Error
                    end;
                {error, _} = Error ->
                    Error
            end;
        error ->
            {error, {unknown_migration, Version}}
    end.

-spec baseline_all(cozodb:db_handle(), [{cozodb_migration:version(), module()}]) ->
    ok | {error, term()}.

baseline_all(_Db, []) ->
    ok;
baseline_all(Db, [{Version, Module} | Rest]) ->
    Info = build_migration_info(Version, Module),
    case
        cozodb_migrator_history:record_migration(
            Db, Info#{execution_time_ms => 0}
        )
    of
        ok -> baseline_all(Db, Rest);
        {error, _} = Error -> Error
    end.

-spec applied_set([{cozodb_migration:version(), binary(), binary()}]) ->
    #{cozodb_migration:version() => true}.

applied_set(Applied) ->
    maps:from_list([{V, true} || {V, _C, S} <- Applied, S =:= <<"applied">>]).

-spec apply_version_filter(
    [{cozodb_migration:version(), module()}], map()
) -> [{cozodb_migration:version(), module()}].

apply_version_filter(Pending, #{to := TargetVersion}) ->
    [{V, M} || {V, M} <- Pending, V =< TargetVersion];
apply_version_filter(Pending, _Opts) ->
    Pending.

-spec build_migration_info(cozodb_migration:version(), module()) ->
    cozodb_migration:migration_info().

build_migration_info(Version, Module) ->
    #{
        version => Version,
        module => Module,
        name => cozodb_migration:name(Module),
        checksum => cozodb_migration:checksum(Module)
    }.
