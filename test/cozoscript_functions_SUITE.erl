%% =============================================================================
%%  cozodb_SUITE.erl -
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
%%
%% =============================================================================
-module(cozoscript_functions_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("cozodb_test.hrl").

-define(DB_DIR_TEST, "./cozodb_test_SUITE_data").
%% -define(ENGINES, [mem, sqlite, rocksdb]).
%% -define(ENGINES, [mem, rocksdb]).
-define(ENGINES, [sqlite]).

-compile(export_all).

%% -----------------------------------------------------------------------------
%% Function: suite() -> Info
%% Info = [tuple()]
%% -----------------------------------------------------------------------------
suite() -> [{timetrap, {seconds, 240}}].

%% -----------------------------------------------------------------------------
%% Function: init_per_suite(Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% -----------------------------------------------------------------------------
init_per_suite(Config) ->
    TMPDir = os:getenv("COZODB_TMP_DIR", "/tmp/cozodb"),
    %% ok = application:set_env(cozodb, priv_dir, ?DB_DIR_TEST),
    case proplists:get_value(create_path, Config, true) of
        true ->
            _ = catch file:del_dir_r(TMPDir),
            _ = catch file:make_dir(TMPDir);
        _ ->
            ok
    end,
    [{tmp_dir, TMPDir} | Config].

%% -----------------------------------------------------------------------------
%% Function: end_per_suite(Config0) -> term() | {save_config,Config1}
%% Config0 = Config1 = [tuple()]
%% -----------------------------------------------------------------------------
end_per_suite(Config) ->
    TMPDir = os:getenv("COZODB_TMP_DIR", "/tmp/cozodb"),
    case proplists:get_value(clean_path, Config, true) of
        false ->
            ok;
        _ ->
            ok = file:del_dir_r(TMPDir)
    end,
    ok = application:unset_env(cozodb, data_dir),
    ok.

%% -----------------------------------------------------------------------------
%% Function: init_per_group(GroupName, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% GroupName = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% -----------------------------------------------------------------------------
init_per_group(sqlite, Config) ->
    [{db_engine, sqlite} | Config];
init_per_group(mem, Config) ->
    [{db_engine, mem} | Config];
init_per_group(rocksdb, Config) ->
    [{db_engine, rocksdb} | Config].

%% -----------------------------------------------------------------------------
%% Function: end_per_group(GroupName, Config0) ->
%%               term() | {save_config,Config1}
%% GroupName = atom()
%% Config0 = Config1 = [tuple()]
%% -----------------------------------------------------------------------------
end_per_group(_GroupName, _Config) ->
    ok.

%% -----------------------------------------------------------------------------
%% Function: init_per_testcase(TestCase, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% -----------------------------------------------------------------------------
init_per_testcase(TestCase, Config) ->
    TMPDir = ?config(tmp_dir, Config),
    Path0 = filename:join([TMPDir, TestCase]),
    ok = file:make_dir(Path0),

    Path =
        case ?config(db_engine, Config) of
            sqlite ->
                Filename = integer_to_list(erlang:system_time()) ++ ".sqlite",
                filename:join([Path0, Filename]);
            _ ->
                Path0
        end,
    [{db_path, Path} | Config].

%% -----------------------------------------------------------------------------
%% Function: end_per_testcase(TestCase, Config0) ->
%%               term() | {save_config,Config1} | {fail,Reason}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% -----------------------------------------------------------------------------
end_per_testcase(_TestCase, Config) ->
    _ = catch file:del_dir_r(?config(db_path, Config)),
    ok.

%% -----------------------------------------------------------------------------
%% Function: groups() -> [Group]
%% Group = {GroupName,Properties,GroupsAndTestCases}
%% GroupName = atom()
%% Properties = [parallel | sequence | Shuffle | {RepeatType,N}]
%% GroupsAndTestCases = [Group | {group,GroupName} | TestCase]
%% TestCase = atom()
%% Shuffle = shuffle | {shuffle,{integer(),integer(),integer()}}
%% RepeatType = repeat | repeat_until_all_ok | repeat_until_all_fail |
%%              repeat_until_any_ok | repeat_until_any_fail
%% N = integer() | forever
%% -----------------------------------------------------------------------------
groups() ->
    [
        {mem, all_cases()},
        {rocksdb, all_cases()},
        {sqlite, all_cases()}
    ].

all_cases() ->
    [
        interval
    ].

%% -----------------------------------------------------------------------------
%% Function: all() -> GroupsAndTestCases | {skip,Reason}
%% GroupsAndTestCases = [{group,GroupName} | TestCase]
%% GroupName = atom()
%% TestCase = atom()
%% Reason = term()
%% -----------------------------------------------------------------------------

all() ->
    [
        {group, mem, []},
        {group, rocksdb, []},
        {group, sqlite, []}
    ].

%% -----------------------------------------------------------------------------
%% Function: TestCase() -> Info
%% Info = [tuple()]
%% -----------------------------------------------------------------------------

interval(_Config) ->
    % open a new database
    {ok, Db} = cozodb:open(),


    {ok, #{rows := Rows}} = cozodb:run(Db, """
        data[x, y] <- [[1,2], [2,3], [4,5]]
        ?[z, len, contains] :=
            data[x,y],
            z = interval(x,y),
            len = interval_len(z),
            contains = interval_contains(z, 1)
    """),

    ?assertEqual(
        [
            [[1, 2], 1, true],
            [[2, 3], 1, false],
            [[4, 5], 1, false]
        ],
        Rows
    ),
    % close an already closed database
    ok = cozodb:close(Db).

