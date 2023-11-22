-module(cozodb).

-include("cargo.hrl").
-include_lib("kernel/include/logger.hrl").


-define(APP, cozodb).
-define(NIF_NOT_LOADED,
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]})
).

-type engine()          :: mem | sqlite | rocksdb.
-type db_opts()         :: map().
-type query_opts()      :: #{
                                encoding => json | undefined                        }.
-export([open/0]).
-export([open/1]).
-export([open/2]).
-export([open/3]).
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
-spec open() -> reference().

open() ->
    Engine = application:get_env(?APP, engine, mem),
    open(Engine).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec open(Engine :: engine()) -> reference().

open(Engine) ->
    DataDir = application:get_env(?APP, data_dir, "/tmp"),
    Path = filename:join([DataDir, "db"]),
    open(Engine, Path).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec open(
    Engine :: engine(), Path :: filename:filename()) -> reference().

open(Engine, Path) when is_list(Path), Path =/= [] ->
    Opts = case Engine of
        sqlite ->
            application:get_env(?APP, sqlite_options, #{});
        rocksdb ->
            application:get_env(?APP, rocksdb_options, #{});
        _ ->
            #{}
    end,
    open(Engine, Path, Opts).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec open(
    Engine :: engine(), Path :: filename:filename(), Opts :: db_opts()) ->
    {ok, reference()} | {error, any()}.

open(Engine, Path, Opts) when is_list(Path), Path =/= [], is_map(Opts) ->
    %% Call NIF
    try
        new(Engine, list_to_binary(Path), encode_db_opts(Opts))
    catch
        Class:Reason:Stacktrace ->
            ?LOG_ERROR(#{
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {error, Reason}
    end;

open(_, _, _) ->
    error(badarg).


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
new(Engine, Path, Opts) ->
    ?NIF_NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @private
%% @doc Calls native/cozodb/src/lib.rs::run_script
%% @end
%% -----------------------------------------------------------------------------
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
