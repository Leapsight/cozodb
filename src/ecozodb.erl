-module(ecozodb).

-export([new/2]).
-export([new/3]).
-export([run/2]).
-export([run/3]).
-export([debug_run_script/2]).

-export([test_1/0]).
-export([rows_1/0]).

-export([ add/2
        , my_map/0
        , my_map2/0
        , my_list/0
        , my_maps/0
        , my_string/1
        , my_tuple/0
        , unit_enum_echo/1
        , tagged_enum_echo/1
        , untagged_enum_echo/1
        ]).

-type engine() :: mem | sqlite | rocksdb | sled | tikv.

-include("cargo.hrl").
-on_load(init/0).
-define(NOT_LOADED, not_loaded(?LINE)).



%% =============================================================================
%% API
%% =============================================================================

-spec new(Engine :: engine(), Path :: list()) -> reference().

new(Engine, []) ->
    new(Engine, <<>>, <<>>).


%% =============================================================================
%% API
%% =============================================================================

-spec new(Engine :: engine(), Path :: list(), Opts :: term()) -> reference().

new(Engine, [], Opts) ->
    new(Engine, <<>>, Opts);

new(Engine, Path, Opts) ->
    ?NOT_LOADED.

%% r1[] <- [[1, 'a'], [2, 'b']]
%% r2[] <- [[2, 'B'], [3, 'C']]
%% ?[v1, v2] := r1[k1, v1], r2[k1, v2]

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
run(Db, Script) when is_list(Script) ->
    run(Db, list_to_binary(Script));

run(Db, Script) when is_binary(Script) ->
    run_script(Db, Script).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
run(Db, Script, Opts) when is_list(Script) ->
    run(Db, list_to_binary(Script), Opts);

run(Db, Script, #{return := json} = Opts) when is_binary(Script) ->
    run_script_json(Db, Script);

run(Db, Script, Opts) when is_binary(Script), is_map(Opts) ->
    run_script(Db, Script).



debug_run_script(Db, Script) ->
    ?NOT_LOADED.





%% =============================================================================
%% PRIVATE
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
run_script(Db, Script) ->
    ?NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
run_script_json(Db, Script) ->
    ?NOT_LOADED.






%% =============================================================================
%% EXAMPLES
%% =============================================================================





test_1() ->
    Q = <<"r1[] <- [[1, 'a'], [2, 'b']]\nr2[] <- [[2, 'B'], [3, 'C']]\n?[v1, v2] := r1[k1, v1], r2[k1, v2]">>,
    Ref = ecozodb:new(mem, <<>>, <<>>),
    ecozodb:run_script_json(Ref, Q).


rows_1() ->
    ?NOT_LOADED.


%%

add(_A, _B) ->
    ?NOT_LOADED.

my_map() ->
    ?NOT_LOADED.

my_map2() ->
    ?NOT_LOADED.

my_list() ->
    ?NOT_LOADED.

my_maps() ->
    ?NOT_LOADED.

my_string(Value) ->
    ?NOT_LOADED.

my_tuple() ->
    ?NOT_LOADED.

unit_enum_echo(_Atom) ->
    ?NOT_LOADED.

tagged_enum_echo(_Tagged) ->
    ?NOT_LOADED.

untagged_enum_echo(_Untagged) ->
    ?NOT_LOADED.




%% =============================================================================
%% NIF
%% =============================================================================




init() ->
    ?load_nif_from_crate(ecozodb, 0).

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).





%% =============================================================================
%% TESTS
%% =============================================================================




-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

add_test() ->
    ?assertEqual(4, add(2, 2)).

my_map_test() ->
    ?assertEqual(#{lhs => 33, rhs => 21}, my_map()).

my_maps_test() ->
    ?assertEqual([#{lhs => 33, rhs => 21}, #{lhs => 33, rhs => 21}], my_maps()).

my_tuple_test() ->
    ?assertEqual({33, 21}, my_tuple()).

unit_enum_echo_test() ->
    ?assertEqual(foo_bar, unit_enum_echo(foo_bar)),
    ?assertEqual(baz, unit_enum_echo(baz)).

tagged_enum_echo_test() ->
    ?assertEqual(foo, tagged_enum_echo(foo)),
    ?assertEqual({bar, <<"string">>}, tagged_enum_echo({bar, <<"string">>})),
    ?assertEqual({baz,#{a => 1, b => 2}}, tagged_enum_echo({baz,#{a => 1, b => 2}})).

untagged_enum_echo_test() ->
    ?assertEqual(123, untagged_enum_echo(123)),
    ?assertEqual(<<"string">>, untagged_enum_echo(<<"string">>)).

-endif.
