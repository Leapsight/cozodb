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
-include("cozodb.hrl").
-include_lib("kernel/include/logger.hrl").

-define(APP, cozodb).
-define(NIF_NOT_LOADED,
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]})
).

-record(cozo_named_rows, {
    headers             ::  [binary()],
    rows                ::  [list()],
    next                ::  [list()] | undefined,
    took                ::  integer()
}).

-type named_rows_rec()  ::  #cozo_named_rows{}.
-type named_rows_map()  ::  #{
                                headers := [binary()],
                                rows := [list()],
                                next := [list()] | undefined
                            }.
-type engine()          ::  mem | sqlite | rocksdb.
-type path()            ::  filename:filename().
-type db_opts()         ::  map().
-type query_opts()      ::  #{
                                return_type => json | record | map,
                                mutability => boolean(),
                                params => map()
                            }.
-type query_return()    ::  {ok, named_rows_rec()}
                            | {ok, named_rows_map()}
                            | {ok, Json :: binary()}
                            | {error, Reason :: any()}.

-export([open/0]).
-export([open/1]).
-export([open/2]).
-export([open/3]).
-export([close/1]).
-export([run/2]).
-export([run/3]).

%% -export([info/1]).

%% -export([export_relations/2]).
%% -export([import_relations/2]).
%% -export([backup_db/2]).
%% -export([restore_backup/2]).
%% -export([import_from_backup/2]).
%% -export([register_callback/2]).
%% -export([unregister_callback/2]).
%% -export([register_fixed_rule/2]).
%% -export([unregister_fixed_rule/2]).


-on_load(init/0).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% The database will automatically be closed when the BEAM VM releases it for
%% garbage collection.
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
    open(sqlite, Path, Opts);

open(Engine, Path) when is_atom(Engine), is_list(Path) ->
    ?ERROR(badarg, [Engine, Path], #{
        1 => "engine is not valid. Valid engines are mem, rocksdb and sqlite"
    }).



%% -----------------------------------------------------------------------------
%% @doc
%% `Path' is ignored when `Engine' is `mem'.
%% @end
%% -----------------------------------------------------------------------------
-spec open(Engine :: engine(), Path :: path(), Opts :: db_opts()) ->
    {ok, reference()} | {error, Reason :: any()}.

open(mem, Path, Opts) when is_list(Path), Path =/= [], is_map(Opts) ->
    do_open(<<"mem">>, list_to_binary(Path), encode_map(Opts));

open(rocksdb, Path, Opts) when is_list(Path), Path =/= [], is_map(Opts) ->
    do_open(<<"rocksdb">>, list_to_binary(Path), encode_map(Opts));

open(sqlite, Path, Opts) when is_list(Path), Path =/= [], is_map(Opts) ->
    do_open(<<"sqlite">>, list_to_binary(Path), encode_map(Opts)).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec close(DbRef :: reference()) -> ok | {error, Reason :: any()}.

close(_DbRef) ->
    ?NIF_NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec run(DbRef :: reference(), Script :: list() | binary()) -> query_return().

run(DbRef, Script) when Script == ""; Script == <<>> ->
    ?ERROR(badarg, [DbRef, Script], #{
        1 => "script cannot be empty"
    });

run(DbRef, Script) when is_list(Script) ->
    run(DbRef, list_to_binary(Script));

run(DbRef, Script) when is_reference(DbRef), is_binary(Script) ->
    Params = encode_map(#{}),
    Mutability = false,
    run_script(DbRef, Script, Params, Mutability).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec run(
    DbRef :: reference(), Script :: list() | binary(), Opts :: query_opts()) -> query_return().

run(DbRef, Script, Opts) when is_list(Script) ->
    run(DbRef, list_to_binary(Script), Opts);

run(DbRef, Script, #{return_type := json} = Opts)
when is_reference(DbRef), is_binary(Script) ->
    Params = encode_map(maps:get(params, Opts, #{})),
    Mutability = maps:get(mutability, Opts, false),
    run_script_json(DbRef, Script, Params, Mutability);

run(DbRef, Script, #{return_type := map} = Opts)
when is_reference(DbRef), is_binary(Script) ->
    Params = encode_map(maps:get(params, Opts, #{})),
    Mutability = maps:get(mutability, Opts, false),
    run_script_str(DbRef, Script, Params, Mutability);

run(DbRef, Script, Opts)
when is_reference(DbRef), is_binary(Script), is_map(Opts) ->
    Params = encode_map(maps:get(params, Opts, #{})),
    Mutability = maps:get(mutability, Opts, false),
    run_script(DbRef, Script, Params, Mutability).




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

new(_Engine, _Path, _Opts) ->
    ?NIF_NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @private
%% @doc Calls native/cozodb/src/lib.rs::run_script
%% @end
%% -----------------------------------------------------------------------------
-spec run_script(
    DbRef :: engine(),
    Script :: binary(),
    Params :: binary(),
    Mutability :: boolean()) ->
    {ok, Json :: binary()}.

run_script(_Db, _Script, _Params, _Mutability) ->
    ?NIF_NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @private
%% @doc Calls native/cozodb/src/lib.rs::run_script_json
%% @end
%% -----------------------------------------------------------------------------
-spec run_script_json(
    DbRef :: engine(),
    Script :: binary(),
    Params :: binary(),
    Mutability :: boolean()) ->
    {ok, Json :: binary()}.

run_script_json(_Db, _Script, _Params, _Mutability) ->
    ?NIF_NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @private
%% @doc Calls native/cozodb/src/lib.rs::run_script_json
%% @end
%% -----------------------------------------------------------------------------
-spec run_script_str(
    DbRef :: engine(),
    Script :: binary(),
    Params :: binary(),
    Mutability :: boolean()) ->
    {ok, Json :: binary()}.

run_script_str(_Db, _Script, _Params, _Mutability) ->
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
-spec encode_map(Opts :: db_opts()) -> list().

encode_map(Opts) when is_map(Opts) ->
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
%% -spec json_decoder() -> atom().

%% json_decoder() ->
%%     application:get_env(?APP, json_parser, thoas).



%% =============================================================================
%% TESTS
%% =============================================================================




-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


-endif.
