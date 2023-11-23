%% =============================================================================
%%  cozodb.erl -
%%
%%  Copyright (c) 2020 Leapsight Holdings Limited. All rights reserved.
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

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-module(cozodb).

-include("cargo.hrl").
-include_lib("kernel/include/logger.hrl").

-define(APP, cozodb).
-define(NIF_NOT_LOADED,
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]})
).

-type engine()          ::  mem | sqlite | rocksdb.
-type path()            ::  filename:filename().
-type db_opts()         ::  map().
-type query_opts()      ::  #{
                                encoding => json | undefined                        }.


-export([open/0]).
-export([open/1]).
-export([open/2]).
-export([open/3]).
-export([close/1]).
-export([run/2]).
-export([run/3]).


-on_load(init/0).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec open() -> {ok, reference()} | {error, Reason :: any()}.

open() ->
    Engine = application:get_env(?APP, engine, mem),
    open(Engine).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec open(Engine :: engine()) -> {ok, reference()} | {error, Reason :: any()}.

open(Engine) ->
    DataDir = application:get_env(?APP, data_dir, "/tmp"),
    Path = filename:join([DataDir, "db"]),
    open(Engine, Path).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec open(Engine :: engine(), Path :: path()) ->
    {ok, reference()} | {error, Reason :: any()}.

open(mem, Path) when is_list(Path), Path =/= [] ->
    open(mem, Path, #{});

open(rocksdb, Path) when is_list(Path), Path =/= [] ->
    Opts = application:get_env(?APP, rocksdb_options, #{}),
    open(rocksdb, Path, Opts);

open(sqlite, Path) when is_list(Path), Path =/= [] ->
    Opts = application:get_env(?APP, sqlite_options, #{}),
    open(sqlite, Path, Opts).


%% -----------------------------------------------------------------------------
%% @doc
%% `Path' is ignored when `Engine' is `mem'.
%% @end
%% -----------------------------------------------------------------------------
-spec open(Engine :: engine(), Path :: path(), Opts :: db_opts()) ->
    {ok, reference()} | {error, Reason :: any()}.

open(mem, Path, Opts) when is_list(Path), Path =/= [], is_map(Opts) ->
    do_open(<<"mem">>, list_to_binary(Path), encode_db_opts(Opts));

open(rocksdb, Path, Opts) when is_list(Path), Path =/= [], is_map(Opts) ->
    do_open(<<"rocksdb">>, list_to_binary(Path), encode_db_opts(Opts));

open(sqlite, Path, Opts) when is_list(Path), Path =/= [], is_map(Opts) ->
    do_open(<<"sqlite">>, list_to_binary(Path), encode_db_opts(Opts)).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec close(DbRef :: reference()) -> return.

close(DbRef) ->
    ?NIF_NOT_LOADED.



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec run(DbRef :: reference(), Script :: list()) -> any().

run(DbRef, Script) when is_list(Script) ->
    run(DbRef, list_to_binary(Script));

run(DbRef, Script) when is_binary(Script) ->
    run_script(DbRef, Script).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
run(Db, Script, Opts) when is_list(Script) ->
    run(Db, list_to_binary(Script), Opts);

run(Db, Script, #{format := json} = Opts) when is_binary(Script) ->
    run_script_json(Db, Script);

run(Db, Script, Opts) when is_binary(Script), is_map(Opts) ->
    run_script(Db, Script).




%% =============================================================================
%% PRIVATE: NIFs
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @private
%% @doc Called by on_load directive
%% @end
%% -----------------------------------------------------------------------------
init() ->
    Crate = ?APP,
    ?load_nif_from_crate(Crate, 0).


%% -----------------------------------------------------------------------------
%% @private
%% @doc Calls native/cozodb/src/lib.rs::new
%% @end
%% -----------------------------------------------------------------------------
-spec new(Engine :: binary(), Path :: binary(), Opts :: binary()) ->
    {ok, reference()} | {error, Reason :: any()}.

new(Engine, Path, Opts) ->
    ?NIF_NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @private
%% @doc Calls native/cozodb/src/lib.rs::run_script
%% @end
%% -----------------------------------------------------------------------------
-spec run_script(DbRef :: engine(), Script :: binary()) ->
    {ok, Headers :: [binary()], Rows :: [list()]}.

run_script(Db, Script) ->
    ?NIF_NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @private
%% @doc Calls native/cozodb/src/lib.rs::run_script_json
%% @end
%% -----------------------------------------------------------------------------
run_script_json(Db, Script) ->
    ?NIF_NOT_LOADED.





%% =============================================================================
%% PRIVATE: UTILS
%% =============================================================================



%% @private
do_open(Engine, Path, Opts)
when is_binary(Engine), is_binary(Path), is_binary(Opts) ->
    try
        %% Call NIF
        new(Engine, Path, Opts)
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
-spec encode_db_opts(Opts :: db_opts()) -> list().

encode_db_opts(Opts) when is_map(Opts) ->
    Encoder = json_encoder(),
    Encoder:encode(Opts).


%%--------------------------------------------------------------------
%% @doc returns the default json encoder (thoas)
%% @end
%%--------------------------------------------------------------------
-spec json_encoder() -> atom().

json_encoder() ->
    application:get_env(?APP, json_parser, thoas).


%%--------------------------------------------------------------------
%% @doc returns the default json decoder (thoas)
%% @end
%%--------------------------------------------------------------------
-spec json_decoder() -> atom().

json_decoder() ->
    application:get_env(?APP, json_parser, thoas).



%% =============================================================================
%% TESTS
%% =============================================================================




-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


-endif.
