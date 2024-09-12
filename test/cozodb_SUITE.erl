%% =============================================================================
%%  cozodb_SUITE.erl -
%%
%%  Copyright (c) 2023 Leapsight Holdings Limited. All rights reserved.
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
%% Based on original work by Mathieu Kerjouan
%%
%%
%%  Based on original work by Mathieu Kerjouan with the following notice:
%%
%%  -------------------------------------------------------------------
%%  Copyright (c) 2023 Mathieu Kerjouan
%%
%%  Redistribution and use in source and binary forms, with or without
%%  modification, are permitted provided that the following conditions
%%  are met:
%%
%%  1. Redistributions of source code must retain the above copyright
%%  notice, this list of conditions and the following disclaimer.
%%
%%  2. Redistributions in binary form must reproduce the above
%%  copyright notice, this list of conditions and the following
%%  disclaimer in the documentation and/or other materials provided
%%  with the distribution.
%%
%%  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
%%  CONTRIBUTORS “AS IS” AND ANY EXPRESS OR IMPLIED WARRANTIES,
%%  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
%%  MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
%%  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS
%%  BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
%%  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
%%  TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
%%  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
%%  ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
%%  TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
%%  THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
%%  SUCH DAMAGE.
%%
%%  @copyright 2023 Mathieu Kerjouan
%%  @author Mathieu Kerjouan
%%
%% =============================================================================
-module(cozodb_SUITE).

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
suite() -> [{timetrap,{seconds, 240}}].

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
    [{tmp_dir, TMPDir}|Config].

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

    Path = case ?config(db_engine, Config) of
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
        {mem, param_cases()},
        {rocksdb, all_cases()},
        {sqlite, all_cases()}
    ].


all_cases() ->
    [
        simple
        , tutorial_intro
        , tutorial_expressions
        , tutorial_rules
        , tutorial_stored_relations
        , tutorial_command_blocks
        , tutorial_graphs
        , multi_spawn
        , maintenance_commands
        , system_commands
        , index
        , hnsw
        , lsh
        , fts1
        , fts2
        , air_routes
    ].

param_cases() ->
    [
        param_null,
        param_boolean,
        param_integer,
        param_float,
        param_string,
        param_json,
        param_integer_list,
        param_mixed_list,
        param_mixed_list_atoms
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
        {group, sqlite, []},
        sqlite,
        rocksdb
    ].


%% -----------------------------------------------------------------------------
%% Function: TestCase() -> Info
%% Info = [tuple()]
%% -----------------------------------------------------------------------------

simple(_Config) ->
    % open a new database
    {ok, Db} = cozodb:open(),

    % query using string
    % {ok, _} = cozodb:run(Db, "?[] <- [[1, 2, 3]]"),
    {ok, _} = cozodb:run(Db, "?[] <- [[1, 2, 3]]"),

    % query using binary
    {ok, _} = cozodb:run(Db, <<"?[] <- [[1, 2, 3]]">>),

    % query using a list of string
    {ok, _} = cozodb:run(Db, ["?[] <- [[1, 2, 3]]"]),

    % wrong term used for queries
    ?assertError(function_clause, cozodb:run(Db, query)),
    ?assertError(badarg, cozodb:run(Db, "")),
    ?assertError(function_clause, cozodb:run(Db, "", "")),

    % close database
    %% ok = cozodb:close(Db),

    % wrong engine
    ?assertError(badarg, cozodb:open(test)),

    % not existing path
    % does not work correctly on github workflow
    % {error, _} = cozodb:open(mem, "/not/existing/path"),

    % bad path
    ?assertError(function_clause, cozodb:open(mem, a_wrong_path)),

    % bad configuration provided
    ?assertError(function_clause, cozodb:open(mem, "/tmp", "not a map")),

    % close an already closed database
    ok = cozodb:close(Db).

%% -----------------------------------------------------------------------------
%% https://docs.cozodb.org/en/latest/tutorial.html#First-steps
%% -----------------------------------------------------------------------------

tutorial_intro(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    Res = cozodb:open(Engine, Path),
    ?assertMatch({ok, _}, Res),
    {ok, Db} = Res,
    ?QUERY_OK(Db, "?[] <- [['hello', 'world', 'Cozo!']]"),
    ?QUERY_OK(Db, "?[] <- [[1, 2, 3], ['a', 'b', 'c']]"),
    ?QUERY_OK(Db, "?[] <- [[1.5, 2.5, 3, 4, 5.5],"
              "['aA', 'bB', 'cC', 'dD', 'eE'],"
              "[true, false, null, -1.4e-2, \"A string with double quotes\"]]"),
    ?QUERY_OK(Db, "?[] <- [[1], [2], [1], [2], [1]]"),
    cozodb:close(Db).

%% -----------------------------------------------------------------------------
%% https://docs.cozodb.org/en/latest/tutorial.html#Expressions
%% -----------------------------------------------------------------------------

tutorial_expressions(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(Engine, Path),
    ?QUERY_OK(Db, "?[] <- [["
              "1 + 2,"
              "3 / 4,"
              "5 == 6,"
              "7 > 8,"
              "true || false,"
              "false && true,"
              "lowercase('HELLO'),"
              "rand_float(),"
              "union([1, 2, 3], [3, 4, 5], [5, 6, 7])"
              "]]"),
    ?QUERY_OK(Db, "a[x, y] <- [[1, 2], [3, 4]]"
              "b[y, z] <- [[2, 3], [2, 4]]"
              "?[x, y, z] := a[x, y], b[y, z]"
              "?[x, y, z] := a[x, y], not b[y, _], z = null"),
    cozodb:close(Db).

%% -----------------------------------------------------------------------------
%% https://docs.cozodb.org/en/latest/tutorial.html#Joins,-made-easy
%% -----------------------------------------------------------------------------

tutorial_rules(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(Engine, Path),
    ?QUERY_OK(Db, "?[first, second, third] <- [[1, 2, 3], ['a', 'b', 'c']]"),
    ?QUERY_OK(Db, "rule[first, second, third] <- [[1, 2, 3], ['a', 'b', 'c']]"
              "?[a, b, c] := rule[a, b, c]"),
    ?QUERY_OK(Db, "rule[first, second, third] <- [[1, 2, 3], ['a', 'b', 'c']]"
              "?[c, b] := rule[a, b, c]"),
    ?QUERY_OK(Db, "?[c, b] := rule[a, b, c], is_num(a)"
              "rule[first, second, third] <- [[1, 2, 3], ['a', 'b', 'c']]"),
    ?QUERY_OK(Db, "rule[first, second, third] <- [[1, 2, 3], ['a', 'b', 'c']]"
              "?[c, b] := rule['a', b, c]"),
    ?QUERY_OK(Db, "rule[first, second, third] <- [[1, 2, 3], ['a', 'b', 'c']]"
              "?[c, b, d] := rule[a, b, c], is_num(a), d = a + b + 2*c"),
    ?QUERY_OK(Db, "?[x, y] := x in [1, 2, 3], y in ['x', 'y']"),
    cozodb:close(Db).

%% -----------------------------------------------------------------------------
%% https://docs.cozodb.org/en/latest/tutorial.html#Joins,-made-easy
%% -----------------------------------------------------------------------------

tutorial_joins(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    ?QUERY_OK("r1[] <- [[1, 'a'], [2, 'b']]"
              "r2[] <- [[2, 'B'], [3, 'C']]"
              "?[l1, l2] := r1[a, l1], r2[b, l2]"),

    ?QUERY_OK("r1[] <- [[1, 'a'], [2, 'b']]"
              "r2[] <- [[2, 'B'], [3, 'C']]"
              "?[l1, l2] := r1[a, l1],"
              "             r2[a, l2]"),

    ?QUERY_OK("a[x, y] <- [[1, 2], [3, 4]]"
              "b[y, z] <- [[2, 3], [2, 4]]"
              "?[x, y, z] := a[x, y], b[y, z]"
              "?[x, y, z] := a[x, y], not b[y, _], z = null").

%% -----------------------------------------------------------------------------
%% https://docs.cozodb.org/en/latest/tutorial.html#Stored-relations
%% -----------------------------------------------------------------------------

tutorial_stored_relations(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(Engine, Path),
    ?IQUERY_LOG(Db, ":create stored {c1, c2}"),
    ?IQUERY_LOG(Db, ":create dept_info {"
                "company_name: String,"
                "department_name: String,"
                "=>"
                "head_count: Int default 0,"
                "address: String,"
                "}"),
    ?IQUERY_LOG(Db, "?[a, b, c] <- [[1, 'a', 'A'],"
                "[2, 'b', 'B'],"
                "[3, 'c', 'C'],"
                "[4, 'd', 'D']]"),
    ?IQUERY_LOG(Db, ":create fd {a, b => c}"),
    ?IQUERY_LOG(Db, "?[a, b, c] := *fd[a, b, c]"),
    ?IQUERY_LOG(Db, "?[a, b, c] <- [[3, 'c', 'CCCCCCC']]"),
    ?IQUERY_LOG(Db, ":put fd {a, b => c}"
                "?[a, b, c] := *fd[a, b, c]"),
    ?IQUERY_LOG(Db, "::relations"),
    ?IQUERY_LOG(Db, "::columns stored"),

    % @todo: crash ?IQUERY_LOG(Db, "?[a, b] := *stored[a, b]"),
    % @todo: crash ?IQUERY_LOG(Db, "?[a, b] := *stored{l2: b, l1: a}"),
    % @todo: crash ?IQUERY_LOG(Db, "?[l2] := *stored{l2}"),
    % ?IQUERY_LOG(Db, "?[l1, l2] <- [['e', 'E']]"
    %		":rm stored {l1, l2}"),

    ?IQUERY_LOG(Db, "?[l1, l2] := *stored[l1, l2]"),
    ?IQUERY_LOG(Db, "::remove stored"),
    ?IQUERY_LOG(Db, "::relations"),
    ?IQUERY_LOG(Db, "::remove fd"),
    cozodb:close(Db).

%% -----------------------------------------------------------------------------
%% https://docs.cozodb.org/en/latest/tutorial.html#Command-blocks
%% -----------------------------------------------------------------------------

tutorial_command_blocks(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(Engine, Path),
    ?IQUERY_LOG(Db, "{?[a] <- [[1], [2], [3]]; :replace test {a}}"
                "{?[a] <- []; :replace test2 {a}}"
                "%swap test test2"
                "%return test"),
    ok = cozodb:close(Db).

%% -----------------------------------------------------------------------------
%% https://docs.cozodb.org/en/latest/tutorial.html#Graphs
%% -----------------------------------------------------------------------------

tutorial_graphs(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(Engine, Path),
    ?IQUERY_LOG(Db, "?[loving, loved] <- [['alice', 'eve'],"
                "['bob', 'alice'],"
                "['eve', 'alice'],"
                "['eve', 'bob'],"
                "['eve', 'charlie'],"
                "['charlie', 'eve'],"
                "['david', 'george'],"
                "['george', 'george']]"
                ":replace love {loving, loved}"),

    ?IQUERY_LOG(Db, "?[loved_by_b_e] := *love['eve', loved_by_b_e],"
                "*love['bob', loved_by_b_e]"),

    ?IQUERY_LOG(Db, "?[loved_by_b_e] := *love['eve', loved_by_b_e] or *love['bob', loved_by_b_e],"
                "loved_by_b_e != 'bob',"
                "loved_by_b_e != 'eve'"),

    ?IQUERY_LOG(Db, "?[loved_by_b_e] := *love['eve', loved_by_b_e],"
               "loved_by_b_e != 'bob',"
               "loved_by_b_e != 'eve'"
               "?[loved_by_b_e] := *love['bob', loved_by_b_e],"
               "loved_by_b_e != 'bob',"
               "loved_by_b_e != 'eve'"),

    ?IQUERY_LOG(Db, "?[loved] := *love[person, loved], !ends_with(person, 'e')"),

    ?IQUERY_LOG(Db, "?[loved_by_e_not_b] := *love['eve', loved_by_e_not_b],"
                "not *love['bob', loved_by_e_not_b]"),

    % @todo: crash to fix
    % ?IQUERY_LOG(Db, "?[not_loved_by_b] := not *love['bob', not_loved_by_b]"),

    ?IQUERY_LOG(Db, "the_population[p] := *love[p, _a]"
                "the_population[p] := *love[_a, p]"
                "?[not_loved_by_b] := the_population[not_loved_by_b],"
                "not *love['bob', not_loved_by_b]"),

    ?IQUERY_LOG(Db, "alice_love_chain[person] := *love['alice', person]"
                "alice_love_chain[person] := alice_love_chain[in_person],"
                "*love[in_person, person]"
                "?[chained] := alice_love_chain[chained]"),

    ?IQUERY_LOG(Db, "alice_love_chain[person] := alice_love_chain[in_person],"
                "*love[in_person, person]"
                "?[chained] := alice_love_chain[chained]"),

    ?IQUERY_LOG(Db, "?[loving, loved] := *love{ loving, loved }"
                ":limit 1"),

    ?IQUERY_LOG(Db, "?[loving, loved] := *love{ loving, loved }"
                ":order -loved, loving"
                ":offset 1"),

    ?IQUERY_LOG(Db, "?[loving, loved] := *love{ loving, loved }"
                ":limit 1"),

    ?IQUERY_LOG(Db, "?[loving, loved] := *love{ loving, loved }\n"
                ":order -loved, loving\n"
                ":offset 1"),

    ?IQUERY_LOG(Db, "?[] <~ Constant(data: [['hello', 'world', 'Cozo!']])"),

    ?IQUERY_LOG(Db, "?[person, page_rank] <~ PageRank(*love[])\n"
                ":order -page_rank"),

    ?IQUERY_LOG(Db, "::remove love"),
    ok = cozodb:close(Db).

%% -----------------------------------------------------------------------------
%% https://docs.cozodb.org/en/latest/tutorial.html#Extended-example:-the-air-routes-dataset
%% -----------------------------------------------------------------------------

air_routes(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(Engine, Path),

    %% Setup database relations

    ?IQUERY_LOG(Db, "{:create airport {"
                "code: String"
                "=>"
                "icao: String,"
                "desc: String,"
                "region: String,"
                "runways: Int,"
                "longest: Float,"
                "elev: Float,"
                "country: String,"
                "city: String,"
                "lat: Float,"
                "lon: Float"
                "}}"),

    ?IQUERY_LOG(Db, "{:create country {"
                "code: String"
                "=>"
                "desc: String"
                "}}"),

    ?IQUERY_LOG(Db, "{:create continent {"
                "code: String"
                "=>"
                "desc: String"
                "}}"),

    ?IQUERY_LOG(Db, "{:create contain { entity: String, contained: String }}"),

    ?IQUERY_LOG(Db, "{:create route { fr: String, to: String => dist: Float }}"),

    {ok, #{rows := Rows}} = cozodb:relations(Db),
    RelNames = [hd(Row) || Row <- Rows],

     %% Test data sources directory
    DataDir = proplists:get_value(data_dir, Config, "test/cozodb_SUITE_data"),
    DataFile = filename:join(DataDir, "air-routes.json"),
    {ok, RawAirRoutes} = file:read_file(DataFile),
    {ok, AirRoutes} = thoas:decode(RawAirRoutes),
    cozodb:import(Db, AirRoutes),

    %% cozodb:import_from_backup(Db, DataFile, RelNames),


    ?IQUERY_LOG(Db, "?[code, city, desc, region, runways, lat, lon]"
                ":= *airport{code, city, desc, region, runways, lat, lon}"
                ":limit 5"),

    ?IQUERY_LOG(Db, "?[code, city, desc, region, runways, lat, lon]"
                ":= *airport{code, city, desc, region, runways, lat, lon}"
                ":order -runways"
                ":limit 10"),

    ?IQUERY_LOG(Db, "?[count(code)] := *airport{code}"),

    ?IQUERY_LOG(Db, "?[count(initial), initial]"
                ":= *airport{code}, initial = first(chars(code))"
                ":order initial"),

    ?IQUERY_LOG(Db, "?[count(r), count_unique(r), sum(r), min(r), max(r), mean(r), std_dev(r)]"
                ":= *airport{runways: r}"),

    ?IQUERY_LOG(Db, "?[desc] := *country{code, desc}, not *airport{country: code}"),

    ?IQUERY_LOG(Db, "?[fr, to, dist] := *route{fr, to, dist}"
                ":limit 10"),

    ?IQUERY_LOG(Db, "?[code, desc] := *airport{code, desc}, not *route{fr: code}, not *route{to: code}"),

    ?IQUERY_LOG(Db, "route_count[fr, count(fr)] := *route{fr}"
                "?[code, n] := route_count[code, n]"
                ":sort -n"
                ":limit 5"),

    ?IQUERY_LOG(Db, "routes[unique(r)] := *contain['EU', fr],"
                "*route{fr, to},"
                "*airport{code: to, country: 'US'},"
                "r = [fr, to]"
                "?[n] := routes[rs], n = length(rs)"),

    ?IQUERY_LOG(Db, "?[count_unique(to)] := *contain['EU', fr],"
                "*route{fr, to},"
                "*airport{code: to, country: 'US'}"),

    ?IQUERY_LOG(Db, "?[code, count(code)]"
                ":= *airport{code, city: 'London', region: 'GB-ENG'}, *route{fr: code}"),

    ?IQUERY_LOG(Db, "lon_uk_airports[code] := *airport{code, city: 'London', region: 'GB-ENG'}"
                "one_hop[to] := lon_uk_airports[fr], *route{fr, to}, not lon_uk_airports[to];"
                "?[count_unique(a3)] := one_hop[a2], *route{fr: a2, to: a3}, not lon_uk_airports[a3];"),

    ?IQUERY_LOG(Db, "?[city, dist] := *route{fr: 'LGW', to, dist},"
                "*airport{code: to, city}"
                ":order -dist"
                ":limit 10"),

    ?IQUERY_LOG(Db, "?[code, desc, lon, lat] := *airport{lon, lat, code, desc}, lon > -0.1, lon < 0.1"),

    ?IQUERY_LOG(Db, "h_box[lon, lat] := *airport{code: 'LHR', lon, lat}"
                "?[code, desc] := h_box[lhr_lon, lhr_lat], *airport{code, lon, lat, desc},"
                "abs(lhr_lon - lon) < 1, abs(lhr_lat - lat) < 1"),

    ?IQUERY_LOG(Db, "?[deg_diff] := *airport{code: 'SFO', lat: a_lat, lon: a_lon},"
                "*airport{code: 'NRT', lat: b_lat, lon: b_lon},"
                "deg_diff = rad_to_deg(haversine_deg_input(a_lat, a_lon, b_lat, b_lon))"),

    ?IQUERY_LOG(Db, "shortest[b, min(dist)] := *route{fr: 'LHR', to: b, dist}"
                "shortest[b, min(dist)] := shortest[c, d1],"
                "*route{fr: c, to: b, dist: d2},"
                "dist = d1 + d2"
                "?[dist] := shortest['YPO', dist]"),

    %% From https://docs.cozodb.org/en/latest/tutorial.html
    %% You will find that the query does not complete in a reasonable amount of
    %% time, despite it being equivalent to the original query. Why?
    %% In the changed query, you are asking the database to compute the all-pair
    %% shortest path, and then extract the answer to a particular shortest path.
    %% Normally Cozo would apply a technique called magic set rewrite so that
    %% only the needed answer would be calculated. However, in the changed query
    %% the presence of the aggregation operator min prevents that. In this case,
    %% applying the rewrite to the variable a would still yield the correct
    %% answer, but rewriting in any other way would give complete nonsense, and
    %% in the more general case with recursive aggregations this is a can of
    %% worms.
    %% So as explained in the chapter about execution, magic set rewrites are
    %% only applied to rules without aggregations or recursions for the moment,
    %% until we are sure of the exact conditions under which the rewrites are
    %% safe. So for now at least the database executes the query as written,
    %% computing the result of the shortest rule containing more than ten
    %% million rows (to be exact, 3700 * 3700 = 13,690,000 rows) first!
    %%
    %% The bottom line is, be mindful of the cardinality of the return sets of
    %% recursive rules.
    %%
    %% ?IQUERY_LOG(Db, "shortest[a, b, min(dist)] := *route{fr: a, to: b, dist}"
    %%             "shortest[a, b, min(dist)] := shortest[a, c, d1],"
    %%             "*route{fr: c, to: b, dist: d2},"
    %%             "dist = d1 + d2"
    %%             "?[dist] := shortest['LHR', 'YPO', dist]"),

    ?IQUERY_LOG(Db, "starting[] <- [['LHR']]"
                "goal[] <- [['YPO']]"
                "?[starting, goal, distance, path] <~ ShortestPathDijkstra(*route[], starting[], goal[])"),

    ?IQUERY_LOG(Db, "starting[] <- [['LHR']]"
                "goal[] <- [['YPO']]"
                "?[starting, goal, distance, path] <~ KShortestPathYen(*route[], starting[], goal[], k: 10)"),

    ?IQUERY_LOG(Db, "code_lat_lon[code, lat, lon] := *airport{code, lat, lon}"
                "starting[code, lat, lon] := code = 'LHR', *airport{code, lat, lon};"
                "goal[code, lat, lon] := code = 'YPO', *airport{code, lat, lon};"
                "?[] <~ ShortestPathAStar(*route[],"
                "code_lat_lon[node, lat1, lon1],"
                "starting[],"
                "goal[goal, lat2, lon2],"
                "heuristic: haversine_deg_input(lat1, lon1, lat2, lon2) * 3963);"),

    ?IQUERY_LOG(Db, "rank[code, score] <~ PageRank(*route[a, b])"
                "?[code, desc, score] := rank[code, score], *airport{code, desc}"
                ":limit 10;"
                ":order -score"),

    %% @todo: fix crash:
    %% thread '<unnamed>' panicked at 'index out of bounds: the len is 0 but the index is 0', cozo-core/src/fixed_rule/algos/all_pairs_shortest_path.rs:80:24
    %% note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
    %% fatal runtime error: failed to initiate panic, error 5
    %% Aborted
    %% ?IQUERY_LOG(Db, "centrality[code, score] <~ BetweennessCentrality(*route[a, b])"
    %%    "?[code, desc, score] := centrality[code, score], *airport{code, desc}"
    %%    ":limit 10;"
    %%    ":order -score"),

    ?IQUERY_LOG(Db, "community[detailed_cluster, code] <~ CommunityDetectionLouvain(*route[a, b])"
                "?[code, cluster, detailed_cluster] := community[detailed_cluster, code], cluster = first(detailed_cluster)"
                ":replace community {code => cluster, detailed_cluster}"),

    ?IQUERY_LOG(Db, "community[code] := *community{code: 'LGW', cluster}, *community{code, cluster}"
                "?[country, count(code)] :="
                "community[code],"
                "*airport{code, desc, country: country_code},"
                "*country{code: country_code, desc: country},"
                ":order -count(code)"
                ":limit 5"),

    ?IQUERY_LOG(Db, "community[code] := *community{code: 'JFK', cluster}, *community{code, cluster}"
                "?[country, count(code)] :="
                "community[code],"
                "*airport{code, desc, country: country_code},"
                "*country{code: country_code, desc: country},"
                ":order -count(code)"
                ":limit 5"),

    ?IQUERY_LOG(Db, "?[desc, country_desc] := *airport{code: 'FRA', desc, country: country_code}, *country{code: country_code, desc: country_desc}"),

    ?IQUERY_LOG(Db, "community[code] := *community{code: 'FRA', cluster}, *community{code, cluster}"
                "?[country, count(code)] :="
                "community[code],"
                "*airport{code, desc, country: country_code},"
                "*country{code: country_code, desc: country},"
                ":order -count(code)"
                ":limit 5"),

    ?IQUERY_LOG(Db, "community[code] := *community{code: 'SIN', cluster}, *community{code, cluster}"
                "?[country, count(code)] :="
                "community[code],"
                "*airport{code, desc, country: country_code},"
                "*country{code: country_code, desc: country},"
                ":order -count(code)"
                ":limit 5"),

    ?IQUERY_LOG(Db, "?[desc, country_desc] := *airport{code: 'SIN', desc, country: country_code}, *country{code: country_code, desc: country_desc}"),

    ?IQUERY_LOG(Db, "?[fr_cluster, to_cluster, count(dist), sum(dist)] := *route{fr, to, dist},"
                "*community{code: fr, cluster: fr_cluster},"
                "*community{code: to, cluster: to_cluster}"
                ":replace super_route {fr_cluster, to_cluster => n_routes=count(dist), total_distance=sum(dist)}"),

    ?IQUERY_LOG(Db, "?[fr_cluster, to_cluster, n_routes, total_distance] := *super_route{fr_cluster, to_cluster, n_routes, total_distance}, fr_cluster < 2"),

    ?IQUERY_LOG(Db, "?[cluster, score] <~ PageRank(*super_route[])"
                ":order -score"
                ":limit 5"),

    ok = cozodb:close(Db).

%% -----------------------------------------------------------------------------
%%
%% -----------------------------------------------------------------------------
sqlite(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    % create a new cozo database using sqlite
    {ok, Db} = cozodb:open(sqlite, filename:join([Path, "data.sqlite"])),

    #{path := DbPath} = cozodb:info(Db),
    true = filelib:is_file(DbPath),
    {ok, _} = cozodb:run(Db, "?[] <- [[1, 2, 3]]"),
    %% {ok, _} = cozodb:create_relations(Db, "stored" "{c1, c2}"),
    %% {ok, _} = cozodb:run(Db, ":create dept_info {"
    %%       "company_name: String,"
    %%       "department_name: String,"
    %%       "=>"
    %%       "head_count: Int default 0,"
    %%       "address: String,"
    %%       "}"),
    %% {ok, _} = cozodb:run(Db, "?[a, b, c] <- [[1, 'a', 'A'],"
    %%           "[2, 'b', 'B'],"
    %%           "[3, 'c', 'C'],"
    %%           "[4, 'd', 'D']]"),
    cozodb:close(Db),

    % Reopen the database
    {ok, Db2} = cozodb:open(sqlite, DbPath),
    #{path := DbPath} = cozodb:info(Db2),
    true = filelib:is_file(DbPath),
    {ok, _} = cozodb:run(Db2, "?[] <- [[1, 2, 3]]"),
    {ok, _} = cozodb:run(Db2, "?[a, b, c] <- [[1, 'a', 'A'],"
           "[2, 'b', 'B'],"
           "[3, 'c', 'C'],"
           "[4, 'd', 'D']]"),
    ok = cozodb:close(Db2),

    % cleanup the file
    file:delete(DbPath).

%% -----------------------------------------------------------------------------
%%
%% -----------------------------------------------------------------------------

rocksdb(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(rocksdb, Path),
    #{path := DbPath} = cozodb:info(Db),
    true = filelib:is_dir(DbPath),
    {ok, _} = cozodb:run(Db, "?[] <- [[1, 2, 3]]"),
    %% {ok, _} = cozodb:create_relations(Db, "stored", [{c1, c2}]),
    %% {ok, _} = cozodb:run(Db, ":create dept_info {"
    %%       "company_name: String,"
    %%       "department_name: String,"
    %%       "=>"
    %%       "head_count: Int default 0,"
    %%       "address: String,"
    %%       "}"),
    %% {ok, _} = cozodb:run(Db, "?[a, b, c] <- [[1, 'a', 'A'],"
    %%           "[2, 'b', 'B'],"
    %%           "[3, 'c', 'C'],"
    %%           "[4, 'd', 'D']]"),
    cozodb:close(Db),

    {ok, Db2} = cozodb:open(rocksdb, DbPath),
    #{path := DbPath} = cozodb:info(Db2),
    true = filelib:is_dir(DbPath),
    {ok, _} = cozodb:run(Db2, "?[] <- [[1, 2, 3]]"),
    {ok, _} = cozodb:run(Db2, "?[a, b, c] <- [[1, 'a', 'A'],"
           "[2, 'b', 'B'],"
           "[3, 'c', 'C'],"
           "[4, 'd', 'D']]"),
    cozodb:close(Db2).

%% -----------------------------------------------------------------------------
%%
%% -----------------------------------------------------------------------------
maintenance_commands(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    EngineName = atom_to_list(Engine),
    {ok, Db} = cozodb:open(Engine, Path),
    #{path := DbPath} = cozodb:info(Db),

    {ok, _} = cozodb:run(Db, "?[] <- [[1,2,3]]"),

    ok = cozodb:create_relation(Db, "stored", #{
        columns => ["c1"]
    }),
    ok = cozodb:create_relation(Db, "stored2", #{
        columns => [c1, "c2", <<"c3">>]
    }),
    ok = cozodb:create_relation(Db, stored3, #{
        columns => [c1]
    }),
    ok = cozodb:create_relation(Db, stored4, #{
        columns => [c1, c2, c3]
    }),
    ok = cozodb:create_relation(Db, stored5, #{
        columns => ["c1", c2, "c3", c4]
    }),
    % import/export relations
    % wrong json lead to an error
    ?assertError(
        {badarg,"invalid JSON key '{}'"},
        cozodb:import(Db, #{ {} => {} })
    ),
    Relations = #{stored => #{headers => [c1,c2], rows => []}},
    ok = cozodb:import(Db, Relations),
    {ok, _} = cozodb:export(Db, [<<"stored">>]),

    % backup/restore database
    BackupPath = binary_to_list(DbPath) ++ "_" ++ EngineName ++ ".backup",
    ok = cozodb:backup(Db, BackupPath),

    %% Destroy db completely
    case Engine of
        sqlite ->
            file:delete(Path);
        _ ->
            ok = cozodb:close(Db),
            ok = file:del_dir_r(Path),
            ok = file:make_dir(Path)
    end,


    {ok, Db1} = cozodb:open(Engine, Path),
    ok = cozodb:restore(Db1, BackupPath),
    ct:pal(info, ?LOW_IMPORTANCE, "~p", [{BackupPath}]),

    % restore from json
    % {ok, Backup} = file:read_file(BackupPath),
    % BackupJson = binary_to_list(Backup),
    % {ok, _} = cozodb:import_from_backup(Db, BackupJson),

    % delete relation one by one
    ok = cozodb:remove_relation(Db1, "stored"),
    ok = cozodb:remove_relation(Db1, "stored2"),

    % delete many relations
    ok = cozodb:remove_relations(Db1, ["stored3", "stored4", "stored5"]),

    ok = cozodb:close(Db1).


%% -----------------------------------------------------------------------------
%%
%% -----------------------------------------------------------------------------
system_commands(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(Engine, Path),
    #{path := _DbPath} = cozodb:info(Db),

    % create relations
    ok = cozodb:create_relation(Db, "stored", #{
        columns => ["c1"]
    }),
    ok = cozodb:create_relation(Db, "stored2", #{
        columns => [c1, "c2", <<"c3">>]
    }),
    ok = cozodb:create_relation(Db, stored3, #{
        columns => [c1]
    }),
    ok = cozodb:create_relation(Db, stored4, #{
        columns => [c1, c2, c3]
    }),
    ok = cozodb:create_relation(Db, stored5, #{
        columns => ["c1", c2, "c3", c4]
    }),

    % list available relations
    {ok, _} = cozodb:relations(Db),

    % explain a query
    {ok, _} = cozodb:explain(Db, "?[] <- [[1,2,3]]"),

    % list columns and indices
    {ok, _} = cozodb:columns(Db, "stored"),
    {ok, _} = cozodb:indices(Db, "stored"),

    % error during describe
    % {ok, _} = cozodb:describe(Db, "stored", "test").

    {ok, _} = cozodb:triggers(Db, "stored"),
    {ok, _} = cozodb:running(Db),
    ok = cozodb:compact(Db),

    %% % individual access level
    %% {ok, _} = cozodb:set_access_level(Db, hidden, "stored"),
    %% {ok, _} = cozodb:set_access_level(Db, read_only, "stored"),
    %% {ok, _} = cozodb:set_access_level(Db, protected, "stored"),
    %% {ok, _} = cozodb:set_access_level(Db, normal, "stored"),

    %% % multi set access level
    %% {ok, _} = cozodb:set_access_levels(Db, hidden, ["stored2", "stored3"]),
    %% {ok, _} = cozodb:set_access_levels(Db, read_only, ["stored2", "stored3"]),
    %% {ok, _} = cozodb:set_access_levels(Db, protected, ["stored2", "stored3"]),
    %% {ok, _} = cozodb:set_access_levels(Db, normal, ["stored2", "stored3"]),

    % remove relations
    ok = cozodb:remove_relation(Db, "stored"),
    ok = cozodb:remove_relations(Db, ["stored2", "stored3", "stored4"]),

    % close database
    cozodb:close(Db).

%% -----------------------------------------------------------------------------
%%
%% -----------------------------------------------------------------------------

index(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(Engine, Path),
    {ok, _} = cozodb:run(Db, ":create r {a => b}"),
    ok = cozodb:create_index(Db, "r", "idx", #{
        type => covering,
        fields => [b, a]
    }),
    {ok, _} = cozodb:indices(Db, "r"),
    ok = cozodb:drop_index(Db, "r:idx"),
    cozodb:close(Db).

%% -----------------------------------------------------------------------------
%%
%% -----------------------------------------------------------------------------

hnsw(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(Engine, Path),
    ok = cozodb:create_relation(Db, "table_hnsw_fun", #{
        keys => [{k, string}],
        columns => [{v, {vector, 32, 128}}]
    }),
    ok = cozodb:create_index(Db, "table_hnsw_fun", "my_hsnw_index", #{
        type => hnsw,
        dim => 128,
        m => 50,
        ef_construction => 20,
        dtype => f32,
        distance => l2,
        fields => [v],
        filter => <<"k != 'foo'">>,
        extend_candidates => false,
        keep_pruned_connections => false
    }),

    ok = cozodb:drop_index(Db, "table_hnsw_fun:my_hsnw_index"),
    cozodb:close(Db).

%% -----------------------------------------------------------------------------
%%
%% -----------------------------------------------------------------------------

lsh(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(Engine, Path),
    ok = cozodb:create_relation(Db, "table_lsh_fun", #{
        keys => [{k, string}],
        columns => [{v, #{type => string, nullable => true}}]
    }),
    ok = cozodb:create_index(Db, "table_lsh_fun", "my_lsh_index", #{
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
    }),
    ok = cozodb:drop_index(Db, "table_lsh_fun", "my_lsh_index"),
    cozodb:close(Db).

%% -----------------------------------------------------------------------------
%%
%% -----------------------------------------------------------------------------

fts1(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(Engine, Path),
    ok = cozodb:create_relation(Db, "table_fts_fun", #{
        keys => [{k, string}],
        columns => [{v, #{type => string, nullable => true}}]
    }),
    ok = cozodb:create_index(Db, "table_fts_fun", "my_fts_index", #{
        type => fts,
        extractor => v,
        extract_filter => "!is_null(v)",
        tokenizer => simple,
        filters => [alphanumonly]
    }),
    ok = cozodb:drop_index(Db, "table_fts_fun", "my_fts_index"),
    cozodb:close(Db).


fts2(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(Engine, Path),
    ok = cozodb:create_relation(Db, "table_fts_fun", #{
        keys => [{k, string}],
        columns => [{v, #{type => string, nullable => true}}]
    }),
    ok = cozodb:create_index(Db, "table_fts_fun", "my_fts_index", #{
        type => fts,
        extractor => v,
        extract_filter => "!is_null(v)",
        tokenizer => {ngram, 3, 3, false},
        filters => [lowercase, {stemmer, "english"}, {stopwords, "en"}]
    }),
    ok = cozodb:drop_index(Db, "table_fts_fun", "my_fts_index"),
    cozodb:close(Db).

%% -----------------------------------------------------------------------------
%% Function: TestCase() -> Info
%% Info = [tuple()]
%% -----------------------------------------------------------------------------

multi_spawn(_Config) ->
    [ cozo_spawn(100)
    , cozo_spawn(1_000)
    , cozo_spawn(10_000)
    % , cozo_spawn(100_000)
    ].

cozo_spawn(Counter) ->
  Open = fun() -> {ok, Db} = cozodb:open(), Db end,
  Dbs = [ Open() || _ <- lists:seq(1, Counter) ],
  Run = fun(Db) -> spawn(cozodb, run, [Db, "?[] <- [[1, 2, 3]]"]) end,
  [ Run(Db) || Db <- Dbs ],
  [ cozodb:close(Db) || Db <- Dbs ].





%% =============================================================================
%% PARAMS
%% =============================================================================

param_null(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(Engine, Path),
    ?assertMatch(
        {ok, #{rows := [ [null] ]}},
        cozodb:run(Db, <<"?[x] := x = $a">>, #{parameters => #{<<"a">> => null}})
    ).

param_boolean(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(Engine, Path),
    ?assertMatch(
        {ok, #{rows := [ [true] ]}},
        cozodb:run(Db, <<"?[x] := x = $a">>, #{parameters => #{<<"a">> => true}})
    ),
    ?assertMatch(
        {ok, #{rows := [ [false] ]}},
        cozodb:run(Db, <<"?[x] := x = $a">>, #{parameters => #{<<"a">> => false}})
    ).

param_integer(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(Engine, Path),
    ?assertMatch(
        {ok, #{rows := [ [1] ]}},
        cozodb:run(Db, <<"?[x] := x = $a">>, #{parameters => #{<<"a">> => 1}})
    ),
    ?assertMatch(
        {ok, #{rows := [ [0] ]}},
        cozodb:run(Db, <<"?[x] := x = $a">>, #{parameters => #{<<"a">> => 0}})
    ),
    ?assertMatch(
        {ok, #{rows := [ [-1] ]}},
        cozodb:run(Db, <<"?[x] := x = $a">>, #{parameters => #{<<"a">> => -1}})
    ).

param_float(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(Engine, Path),
    ?assertMatch(
        {ok, #{rows := [ [1.0] ]}},
        cozodb:run(Db, <<"?[x] := x = $a">>, #{parameters => #{<<"a">> => 1.0}})
    ),
    ?assertMatch(
        {ok, #{rows := [ [0.0] ]}},
        cozodb:run(Db, <<"?[x] := x = $a">>, #{parameters => #{<<"a">> => 0.0}})
    ),
    ?assertMatch(
        {ok, #{rows := [ [-1.0] ]}},
        cozodb:run(Db, <<"?[x] := x = $a">>, #{parameters => #{<<"a">> => -1.0}})
    ).

param_string(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(Engine, Path),
    ?assertMatch(
        {ok, #{rows := [ [<<"Hello World">>] ]}},
        cozodb:run(
            Db,
            <<"?[x] := x = $a">>,
            #{parameters => #{<<"a">> => <<"Hello World">>}}
        )
    ).

param_json(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(Engine, Path),
    Json = thoas:encode(#{foo => bar}),
    ?assertMatch(
        {ok, #{rows := [ [Json] ]}},
        cozodb:run(
            Db,
            <<"?[x] := x = $a">>,
            #{parameters => #{<<"a">> => Json}}
        )
    ).

param_integer_list(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(Engine, Path),
    ?assertMatch(
        {ok, #{rows := [ [[1, 2, 3]] ]}},
        cozodb:run(
            Db,
            <<"?[x] := x = $a">>,
            #{parameters => #{<<"a">> => [1, 2, 3]}}
        )
    ),
    ?assertMatch(
        {ok, #{rows := [ [1, 2, 3] ]}},
        cozodb:run(
            Db,
            <<"
                aux[x, y, z] <- $a
                ?[x, y, z] := aux[x, y, z]
            ">>,
            #{parameters => #{<<"a">> => [ [1, 2, 3] ]}}
        )
    ).

param_mixed_list(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(Engine, Path),
    ?assertMatch(
        {ok, #{rows := [ [1, 2.0, <<"Hello World">>, null, [1, 2, 3]] ]}},
        cozodb:run(
            Db,
            <<"
                aux[x, y, z, q, r] <- $a
                ?[x, y, z, q, r] := aux[x, y, z, q, r]
            ">>,
            #{parameters =>
                #{<<"a">> => [ [1, 2.0, <<"Hello World">>, null, [1, 2, 3]] ]}
            }
        )
    ).


param_mixed_list_atoms(Config) ->
    Engine = ?config(db_engine, Config),
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(Engine, Path),
    ?assertMatch(
        {ok, #{rows := [ [1, 2.0, <<"hello">>, null, [1, 2, 3]] ]}},
        cozodb:run(
            Db,
            <<"
                aux[x, y, z, q, r] <- $a
                ?[x, y, z, q, r] := aux[x, y, z, q, r]
            ">>,
            #{parameters =>
                %% param a as atom
                %% 3rd column value as atom
                #{a => [ [1, 2.0, hello, null, [1, 2, 3]] ]}
            }
        )
    ).

