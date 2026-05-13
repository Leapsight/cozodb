%% =============================================================================
%%  cozodb_migration.erl -
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

-module(cozodb_migration).

-moduledoc """
Behaviour definition for CozoDB migrations.

Migration modules must follow the naming convention:
`cozodb_migration_YYYYMMDDHHMMSS_description`

For example: `cozodb_migration_20260302120000_create_users`

## Required Callbacks

- `up/1` - Apply the migration forward

## Optional Callbacks

- `down/1` - Reverse the migration (for rollback support)
- `backup/0` - Return `true` to trigger a backup before this migration
- `description/0` - Human-readable description of what this migration does
""".

%% Required callback
-callback up(cozodb:db_handle()) -> ok | {error, term()}.

%% Optional callbacks
-callback down(cozodb:db_handle()) -> ok | {error, term()}.
-callback backup() -> boolean().
-callback description() -> binary().

-optional_callbacks([down/1, backup/0, description/0]).

-export_type([version/0, migration_info/0]).

-export([
    version/1,
    name/1,
    checksum/1,
    has_down/1,
    has_backup/1,
    description/1,
    sort/1
]).

%% Types
-type version() :: pos_integer().
-type migration_info() :: #{
    version := version(),
    module := module(),
    name := binary(),
    checksum := binary(),
    status => binary(),
    applied_at => float(),
    execution_time_ms => non_neg_integer()
}.

%% =============================================================================
%% API
%% =============================================================================

-doc """
Extract the UTC timestamp version from a migration module name.

The module name must match the pattern `cozodb_migration_YYYYMMDDHHMMSS_*`.
""".
-spec version(module()) -> {ok, version()} | {error, term()}.

version(Module) when is_atom(Module) ->
    Name = atom_to_list(Module),
    case Name of
        "cozodb_migration_" ++ Rest ->
            case string:slice(Rest, 0, 14) of
                Digits when length(Digits) =:= 14 ->
                    try list_to_integer(Digits) of
                        V when V > 0 -> {ok, V};
                        _ -> {error, {invalid_version, Module}}
                    catch
                        error:badarg -> {error, {invalid_version, Module}}
                    end;
                _ ->
                    {error, {invalid_version, Module}}
            end;
        _ ->
            {error, {invalid_module_name, Module}}
    end.

-doc """
Extract the human-readable name from a migration module.

For `cozodb_migration_20260302120000_create_users`, returns
`<<"create_users">>`.
""".
-spec name(module()) -> binary().

name(Module) when is_atom(Module) ->
    Name = atom_to_list(Module),
    case Name of
        "cozodb_migration_" ++ Rest ->
            case string:slice(Rest, 14) of
                [$_ | Desc] when Desc =/= [] ->
                    list_to_binary(Desc);
                _ ->
                    list_to_binary(Rest)
            end;
        _ ->
            atom_to_binary(Module)
    end.

-doc """
Compute the SHA-256 checksum of a migration module's abstract code.

Uses `beam_lib:chunks/2` to extract abstract code, strips line numbers
and file attributes for stability. Falls back to BEAM binary if no
debug_info is available.
""".
-spec checksum(module()) -> binary().

checksum(Module) when is_atom(Module) ->
    Beam = get_beam_binary(Module),
    Data =
        case beam_lib:chunks(Beam, [abstract_code]) of
            {ok, {Module, [{abstract_code, {raw_abstract_v1, Forms}}]}} ->
                Stripped = strip_forms(Forms),
                term_to_binary(Stripped);
            _ ->
                Beam
        end,
    Hash = crypto:hash(sha256, Data),
    hex_encode(Hash).

-doc """
Check if the migration module exports a `down/1` callback.
""".
-spec has_down(module()) -> boolean().

has_down(Module) when is_atom(Module) ->
    erlang:function_exported(Module, down, 1).

-doc """
Check if the migration module exports a `backup/0` callback and it returns
`true`.
""".
-spec has_backup(module()) -> boolean().

has_backup(Module) when is_atom(Module) ->
    erlang:function_exported(Module, backup, 0) andalso Module:backup().

-doc """
Get the human-readable description of a migration.

Calls `Module:description/0` if exported, otherwise falls back to
`name/1`.
""".
-spec description(module()) -> binary().

description(Module) when is_atom(Module) ->
    case erlang:function_exported(Module, description, 0) of
        true -> Module:description();
        false -> name(Module)
    end.

-doc """
Sort migration modules by version, detecting duplicate versions.

Returns `{ok, [{Version, Module}]}` sorted ascending by version,
or `{error, {duplicate_versions, [Version]}}` if duplicates are found.
""".
-spec sort([module()]) ->
    {ok, [{version(), module()}]} | {error, term()}.

sort(Modules) when is_list(Modules) ->
    case extract_versions(Modules, []) of
        {ok, VersionedModules} ->
            Sorted = lists:keysort(1, VersionedModules),
            case find_duplicates(Sorted) of
                [] ->
                    {ok, Sorted};
                Dups ->
                    {error, {duplicate_versions, Dups}}
            end;
        {error, _} = Error ->
            Error
    end.

%% =============================================================================
%% PRIVATE
%% =============================================================================

-spec extract_versions([module()], [{version(), module()}]) ->
    {ok, [{version(), module()}]} | {error, term()}.

extract_versions([], Acc) ->
    {ok, Acc};
extract_versions([Module | Rest], Acc) ->
    case version(Module) of
        {ok, V} ->
            extract_versions(Rest, [{V, Module} | Acc]);
        {error, _} = Error ->
            Error
    end.

-spec find_duplicates([{version(), module()}]) -> [version()].

find_duplicates(Sorted) ->
    find_duplicates(Sorted, []).

find_duplicates([{V, _}, {V, _} | Rest], Acc) ->
    find_duplicates([{V, undefined} | Rest], [V | Acc]);
find_duplicates([_ | Rest], Acc) ->
    find_duplicates(Rest, Acc);
find_duplicates([], Acc) ->
    lists:reverse(Acc).

-spec get_beam_binary(module()) -> binary().

get_beam_binary(Module) ->
    case code:get_object_code(Module) of
        {Module, Binary, _Filename} ->
            Binary;
        error ->
            %% Module may have been loaded via code:load_binary/3
            %% (e.g. compiled with compile:file/2 with [binary] option)
            case code:which(Module) of
                Path when is_list(Path) ->
                    case file:read_file(Path) of
                        {ok, Binary} -> Binary;
                        {error, _} -> erlang:error({no_code, Module})
                    end;
                _ ->
                    erlang:error({no_code, Module})
            end
    end.

-spec strip_forms([erl_parse:abstract_form()]) -> [erl_parse:abstract_form()].

strip_forms(Forms) ->
    [strip_form(F) || F <- Forms, not is_file_attribute(F)].

is_file_attribute({attribute, _, file, _}) -> true;
is_file_attribute(_) -> false.

strip_form(Form) ->
    erl_parse:map_anno(fun(_) -> 0 end, Form).

-spec hex_encode(binary()) -> binary().

hex_encode(Bin) ->
    <<<<(hex_digit(H)), (hex_digit(L))>> || <<H:4, L:4>> <= Bin>>.

hex_digit(N) when N < 10 -> $0 + N;
hex_digit(N) -> $a + N - 10.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

version_valid_test() ->
    ?assertEqual(
        {ok, 20260302120000},
        version(cozodb_migration_20260302120000_create_users)
    ).

version_no_description_test() ->
    ?assertEqual(
        {ok, 20260302120000},
        version(cozodb_migration_20260302120000)
    ).

version_invalid_prefix_test() ->
    ?assertMatch(
        {error, {invalid_module_name, _}},
        version(some_other_module)
    ).

version_non_numeric_test() ->
    ?assertMatch(
        {error, {invalid_version, _}},
        version(cozodb_migration_abcdefghijklmn_test)
    ).

name_with_description_test() ->
    ?assertEqual(
        <<"create_users">>,
        name(cozodb_migration_20260302120000_create_users)
    ).

name_without_description_test() ->
    ?assertEqual(
        <<"20260302120000">>,
        name(cozodb_migration_20260302120000)
    ).

name_non_migration_test() ->
    ?assertEqual(
        <<"some_module">>,
        name(some_module)
    ).

hex_encode_test() ->
    ?assertEqual(<<"deadbeef">>, hex_encode(<<16#DE, 16#AD, 16#BE, 16#EF>>)).

sort_empty_test() ->
    ?assertEqual({ok, []}, sort([])).

-endif.
