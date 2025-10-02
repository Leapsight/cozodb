%% =============================================================================
%% cozodb_error_handling_SUITE.erl -
%%
%% Copyright (c) 2023-2025 Leapsight. All rights reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%    http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%% =============================================================================

-module(cozodb_error_handling_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_testcase/2]).
-export([end_per_testcase/2]).

-export([invalid_syntax_error/1]).
-export([nonexistent_relation_error/1]).
-export([type_mismatch_error/1]).
-export([invalid_function_error/1]).
-export([unbound_variable_error/1]).
-export([parse_error_with_position/1]).
-export([multiple_errors_in_script/1]).
-export([error_fields_structure/1]).
-export([error_in_json_mode/1]).
-export([error_in_read_only_mode/1]).
-export([transact_assertion_failure_structured/1]).
-export([duplicate_relation_error/1]).
-export([relation_not_found_error/1]).
-export([column_not_found_error/1]).
-export([data_coercion_error/1]).
-export([null_value_error/1]).
-export([aggregation_not_found_error/1]).
-export([unsafe_negation_error/1]).
-export([constant_rule_arity_mismatch/1]).
-export([imperative_on_permanent_relation/1]).

%% =============================================================================
%% CT CALLBACKS
%% =============================================================================

all() ->
    [
        invalid_syntax_error,
        nonexistent_relation_error,
        type_mismatch_error,
        invalid_function_error,
        unbound_variable_error,
        parse_error_with_position,
        multiple_errors_in_script,
        error_fields_structure,
        error_in_json_mode,
        error_in_read_only_mode,
        transact_assertion_failure_structured,
        duplicate_relation_error,
        relation_not_found_error,
        column_not_found_error,
        data_coercion_error,
        null_value_error,
        aggregation_not_found_error,
        unsafe_negation_error,
        constant_rule_arity_mismatch,
        imperative_on_permanent_relation
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(cozodb),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(cozodb),
    ok.

init_per_testcase(_TestCase, Config) ->
    {ok, Db} = cozodb:open(mem),
    [{db, Db} | Config].

end_per_testcase(_TestCase, Config) ->
    Db = ?config(db, Config),
    ok = cozodb:close(Db),
    ok.

%% =============================================================================
%% TEST CASES
%% =============================================================================


invalid_syntax_error(Config) ->
    Db = ?config(db, Config),
    Query = "INVALID SYNTAX HERE",

    {error, ErrorMap} = cozodb:run(Db, Query),

    ?assert(is_map(ErrorMap)),
    ?assertMatch(#{message := _}, ErrorMap),

    Message = maps:get(message, ErrorMap),
    ?assert(is_binary(Message)),
    ?assertNotEqual(<<>>, Message),

    ct:log("Invalid syntax error message: ~p", [Message]),
    ?assertMatch(#{message := <<_/binary>>}, ErrorMap),
    ok.

nonexistent_relation_error(Config) ->
    Db = ?config(db, Config),
    Query = "?[x] := nonexistent[x]",

    {error, ErrorMap} = cozodb:run(Db, Query),

    ?assert(is_map(ErrorMap)),
    ?assertMatch(#{message := _}, ErrorMap),

    Message = maps:get(message, ErrorMap),
    ?assert(is_binary(Message)),

    ct:log("Non-existent relation error: ~p", [ErrorMap]),

    %% Check that the error mentions the missing rule/relation
    ?assert(binary:match(Message, <<"nonexistent">>) =/= nomatch),
    ?assert(binary:match(Message, <<"not found">>) =/= nomatch),
    ok.

type_mismatch_error(Config) ->
    Db = ?config(db, Config),
    Query = "?[x] := x = 'string' + 123",

    {error, ErrorMap} = cozodb:run(Db, Query),

    ?assert(is_map(ErrorMap)),
    ?assertMatch(#{message := _}, ErrorMap),

    Message = maps:get(message, ErrorMap),
    ct:log("Type mismatch error: ~p", [ErrorMap]),

    %% Check that it's an evaluation error
    ?assert(binary:match(Message, <<"Evaluation">>) =/= nomatch orelse
            binary:match(Message, <<"evaluation">>) =/= nomatch),
    ok.

invalid_function_error(Config) ->
    Db = ?config(db, Config),
    Query = "?[x] := x = invalid_function()",

    {error, ErrorMap} = cozodb:run(Db, Query),

    ?assert(is_map(ErrorMap)),
    ?assertMatch(#{message := _}, ErrorMap),

    Message = maps:get(message, ErrorMap),
    ct:log("Invalid function error: ~p", [ErrorMap]),

    %% Check that the error mentions the invalid function
    ?assert(binary:match(Message, <<"invalid_function">>) =/= nomatch),
    ?assert(binary:match(Message, <<"No implementation">>) =/= nomatch),
    ok.

unbound_variable_error(Config) ->
    Db = ?config(db, Config),
    Query = "?[x] := x.missing_column",

    {error, ErrorMap} = cozodb:run(Db, Query),

    ?assert(is_map(ErrorMap)),
    ?assertMatch(#{message := _}, ErrorMap),

    Message = maps:get(message, ErrorMap),
    ct:log("Unbound variable error: ~p", [ErrorMap]),

    %% Check that it mentions unbound variable
    ?assert(binary:match(Message, <<"unbound">>) =/= nomatch orelse
            binary:match(Message, <<"Atom">>) =/= nomatch),
    ok.

parse_error_with_position(Config) ->
    Db = ?config(db, Config),
    Query = "?[x :=",  %% Incomplete query

    {error, ErrorMap} = cozodb:run(Db, Query),

    ?assert(is_map(ErrorMap)),
    ?assertMatch(#{message := _}, ErrorMap),

    Message = maps:get(message, ErrorMap),
    ct:log("Parse error with position: ~p", [ErrorMap]),

    %% Check that the error mentions position or unexpected end
    ?assert(binary:match(Message, <<"unexpected">>) =/= nomatch orelse
            binary:match(Message, <<"parser">>) =/= nomatch),
    ok.

multiple_errors_in_script(Config) ->
    Db = ?config(db, Config),
    %% Script with multiple potential issues
    Query = "?[x, y] := x = unknown_func(), y = another_unknown()",

    {error, ErrorMap} = cozodb:run(Db, Query),

    ?assert(is_map(ErrorMap)),
    ?assertMatch(#{message := _}, ErrorMap),

    Message = maps:get(message, ErrorMap),
    ct:log("Multiple errors: ~p", [ErrorMap]),

    %% Should get at least one error about unknown function
    ?assert(is_binary(Message)),
    ?assertNotEqual(<<>>, Message),
    ok.

error_fields_structure(Config) ->
    Db = ?config(db, Config),
    Query = "INVALID QUERY",

    {error, ErrorMap} = cozodb:run(Db, Query),

    ?assert(is_map(ErrorMap)),

    %% Check required fields
    ?assertMatch(#{message := _}, ErrorMap),

    %% Check optional fields that might be present
    Keys = maps:keys(ErrorMap),
    ct:log("Error map keys: ~p", [Keys]),
    ct:log("Full error map: ~p", [ErrorMap]),

    %% Message should always be present and be a binary
    Message = maps:get(message, ErrorMap),
    ?assert(is_binary(Message)),
    ?assertNotEqual(<<>>, Message),

    %% Check optional fields if present
    case maps:get(severity, ErrorMap, undefined) of
        undefined -> ok;
        Severity ->
            ?assert(is_binary(Severity)),
            ct:log("Severity: ~p", [Severity])
    end,

    case maps:get(code, ErrorMap, undefined) of
        undefined -> ok;
        Code ->
            ?assert(is_binary(Code)),
            ct:log("Code: ~p", [Code])
    end,

    case maps:get(help, ErrorMap, undefined) of
        undefined -> ok;
        Help ->
            ?assert(is_binary(Help)),
            ct:log("Help: ~p", [Help])
    end,
    ok.

error_in_json_mode(Config) ->
    Db = ?config(db, Config),
    Query = "INVALID SYNTAX",

    %% Test error handling with JSON encoding
    Result = cozodb:run(Db, Query, #{encoding => json}),

    ?assertMatch({error, _}, Result),

    {error, ErrorInfo} = Result,

    %% Even in JSON mode, errors should be returned as maps
    ?assert(is_map(ErrorInfo)),
    ?assertMatch(#{message := _}, ErrorInfo),

    ct:log("Error in JSON mode: ~p", [ErrorInfo]),
    ok.

error_in_read_only_mode(Config) ->
    Db = ?config(db, Config),

    %% First, create a relation
    CreateQuery = ":create test_rel {x: Int}",
    {ok, _} = cozodb:run(Db, CreateQuery),

    %% Try to modify in read-only mode (should fail)
    ModifyQuery = ":create another_rel {y: Int}",
    Result = cozodb:run(Db, ModifyQuery, #{read_only => true}),

    ?assertMatch({error, _}, Result),

    {error, ErrorMap} = Result,
    ?assert(is_map(ErrorMap)),
    ?assertMatch(#{message := _}, ErrorMap),

    Message = maps:get(message, ErrorMap),
    ct:log("Read-only mode error: ~p", [ErrorMap]),

    %% Should mention that it's a read-only violation or similar
    ?assert(is_binary(Message)),
    ?assertNotEqual(<<>>, Message),
    ok.

transact_assertion_failure_structured(Config) ->
    Db = ?config(db, Config),

    %% First, create a relation and insert a row
    CreateQuery = ":create foo {a => b}",
    {ok, _} = cozodb:run(Db, CreateQuery),

    InsertQuery = "?[a, b] <- [['x', 1]]\n :insert foo{a, b}",
    {ok, _} = cozodb:run(Db, InsertQuery),

    %% Try to insert the same key again (should fail with TransactAssertionFailure)
    {error, ErrorMap} = cozodb:run(Db, InsertQuery),

    ct:log("TransactAssertionFailure error: ~p", [ErrorMap]),

    %% Verify the error structure
    ?assert(is_map(ErrorMap)),

    %% Should have message field with the full error chain
    ?assertMatch(#{message := _}, ErrorMap),
    Message = maps:get(message, ErrorMap),
    ?assert(is_binary(Message)),

    %% The message should contain information from both the source error and the wrapper
    %% Source error: "Assertion failure for ... of foo: ..."
    %% Wrapper: "when executing against relation 'foo'"
    ?assert(binary:match(Message, <<"Assertion failure">>) =/= nomatch),
    ?assert(binary:match(Message, <<"foo">>) =/= nomatch),

    ct:log("Full error message: ~p", [Message]),

    ok.

%% Test duplicate relation creation error
duplicate_relation_error(Config) ->
    Db = ?config(db, Config),

    %% Create a relation
    R1 = #{keys => [<<"a">>], columns => [<<"b">>]},
    ok = cozodb:create_relation(Db, <<"foo">>, R1),

    %% Try to create it again
    ?assertEqual(
        {error, #{message => already_exists}},
        cozodb:create_relation(Db, <<"foo">>, R1)
    ),

    R2 = #{keys => [<<"a">>], columns => [<<"b">>, <<"c">>]},

    ?assertEqual(
        {error, #{message => already_exists}},
        cozodb:create_relation(Db, <<"foo">>, R2)
    ).

%% Test relation not found error
relation_not_found_error(Config) ->
    Db = ?config(db, Config),

    %% Try to query a non-existent stored relation
    Query = "?[a] := *nonexistent_stored{a}",
    {error, ErrorMap} = cozodb:run(Db, Query),

    ct:log("Relation not found error: ~p", [ErrorMap]),

    ?assert(is_map(ErrorMap)),
    ?assertMatch(#{message := _}, ErrorMap),
    Message = maps:get(message, ErrorMap),
    ?assert(is_binary(Message)),

    %% Should mention the relation name
    ?assert(binary:match(Message, <<"nonexistent_stored">>) =/= nomatch),

    ok.

%% Test column not found error
column_not_found_error(Config) ->
    Db = ?config(db, Config),

    %% Create a relation
    CreateQuery = ":create test_rel {a: Int, b: Int}",
    {ok, _} = cozodb:run(Db, CreateQuery),

    %% Try to access a column that doesn't exist
    Query = "?[c] := *test_rel{c}",
    {error, ErrorMap} = cozodb:run(Db, Query),

    ct:log("Column not found error: ~p", [ErrorMap]),

    ?assert(is_map(ErrorMap)),
    ?assertMatch(#{message := _}, ErrorMap),
    Message = maps:get(message, ErrorMap),
    ?assert(is_binary(Message)),
    ?assertNotEqual(<<>>, Message),

    ok.

%% Test data coercion error
data_coercion_error(Config) ->
    Db = ?config(db, Config),

    %% Create a relation with typed columns
    CreateQuery = ":create test_rel {a: Int}",
    {ok, _} = cozodb:run(Db, CreateQuery),

    %% Try to insert wrong type
    InsertQuery = "?[a] <- [['not_an_int']] :put test_rel {a}",
    {error, ErrorMap} = cozodb:run(Db, InsertQuery),

    ct:log("Data coercion error: ~p", [ErrorMap]),

    ?assert(is_map(ErrorMap)),
    ?assertMatch(#{message := _}, ErrorMap),
    Message = maps:get(message, ErrorMap),
    ?assert(is_binary(Message)),

    %% Should mention type or coercion
    ?assert(
        binary:match(Message, <<"type">>) =/= nomatch orelse
        binary:match(Message, <<"coercion">>) =/= nomatch orelse
        binary:match(Message, <<"Int">>) =/= nomatch
    ),

    ok.

%% Test null value error for non-null type
null_value_error(Config) ->
    Db = ?config(db, Config),

    %% Create a relation with non-nullable column
    CreateQuery = ":create test_rel {a: Int}",
    {ok, _} = cozodb:run(Db, CreateQuery),

    %% Try to insert null value
    InsertQuery = "?[a] <- [[null]] :put test_rel {a}",
    {error, ErrorMap} = cozodb:run(Db, InsertQuery),

    ct:log("Null value error: ~p", [ErrorMap]),

    ?assert(is_map(ErrorMap)),
    ?assertMatch(#{message := _}, ErrorMap),
    Message = maps:get(message, ErrorMap),
    ?assert(is_binary(Message)),

    %% Should mention null or type
    ?assert(
        binary:match(Message, <<"null">>) =/= nomatch orelse
        binary:match(Message, <<"Null">>) =/= nomatch orelse
        binary:match(Message, <<"type">>) =/= nomatch
    ),

    ok.

%% Test aggregation not found error
aggregation_not_found_error(Config) ->
    Db = ?config(db, Config),

    %% Try to use non-existent aggregation
    Query = "?[sum_a] := a in [1, 2, 3], sum_a = nonexistent_agg(a)",
    {error, ErrorMap} = cozodb:run(Db, Query),

    ct:log("Aggregation not found error: ~p", [ErrorMap]),

    ?assert(is_map(ErrorMap)),
    ?assertMatch(#{message := _}, ErrorMap),
    Message = maps:get(message, ErrorMap),
    ?assert(is_binary(Message)),

    %% Should mention aggregation or function name
    ?assert(
        binary:match(Message, <<"nonexistent_agg">>) =/= nomatch orelse
        binary:match(Message, <<"aggregation">>) =/= nomatch orelse
        binary:match(Message, <<"Aggregation">>) =/= nomatch
    ),

    ok.

%% Test unsafe negation error
unsafe_negation_error(Config) ->
    Db = ?config(db, Config),

    %% Create a relation
    CreateQuery = ":create test_rel {a: Int}",
    {ok, _} = cozodb:run(Db, CreateQuery),

    %% Try unsafe negation (negating without binding all variables first)
    Query = "?[a] := not *test_rel{a}",
    {error, ErrorMap} = cozodb:run(Db, Query),

    ct:log("Unsafe negation error: ~p", [ErrorMap]),

    ?assert(is_map(ErrorMap)),
    ?assertMatch(#{message := _}, ErrorMap),
    Message = maps:get(message, ErrorMap),
    ?assert(is_binary(Message)),

    %% Should mention negation or unbound
    ?assert(
        binary:match(Message, <<"negation">>) =/= nomatch orelse
        binary:match(Message, <<"unbound">>) =/= nomatch orelse
        binary:match(Message, <<"unsafe">>) =/= nomatch
    ),

    ok.

%% Test constant rule arity mismatch
constant_rule_arity_mismatch(Config) ->
    Db = ?config(db, Config),

    %% Define a constant rule with mismatched arity
    Query = "r[a, b] <- [[1, 2, 3]] ?[x, y] := r[x, y]",
    {error, ErrorMap} = cozodb:run(Db, Query),

    ct:log("Constant rule arity mismatch: ~p", [ErrorMap]),

    ?assert(is_map(ErrorMap)),
    ?assertMatch(#{message := _}, ErrorMap),
    Message = maps:get(message, ErrorMap),
    ?assert(is_binary(Message)),

    %% Should mention arity
    ?assert(
        binary:match(Message, <<"arity">>) =/= nomatch orelse
        binary:match(Message, <<"Arity">>) =/= nomatch
    ),

    ok.

%% Test fixed rule/algorithm not found error
imperative_on_permanent_relation(Config) ->
    Db = ?config(db, Config),

    %% Try to use a non-existent fixed rule/algorithm
    Query = "?[] <~ NonExistentAlgorithm()",
    {error, ErrorMap} = cozodb:run(Db, Query),

    ct:log("Fixed rule not found error: ~p", [ErrorMap]),

    ?assert(is_map(ErrorMap)),
    ?assertMatch(#{message := _}, ErrorMap),
    Message = maps:get(message, ErrorMap),
    ?assert(is_binary(Message)),

    %% Should mention the algorithm/rule name or "not found"
    ?assert(
        binary:match(Message, <<"NonExistentAlgorithm">>) =/= nomatch orelse
        binary:match(Message, <<"not found">>) =/= nomatch orelse
        binary:match(Message, <<"cannot">>) =/= nomatch
    ),

    ok.