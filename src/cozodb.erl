%% =============================================================================
%%  cozodb.erl -
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
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @doc
%% <a href="https://www.cozodb.org/" target="_">CozoDB</a> is a A FOSS embeddable, transactional,
%% relational-graph-vector database with time travelling capability, perfect as
%% the long-term memory for LLMs and AI.
%%
%% This library implements the CozoDB bindings for Erlang (BEAM) as a NIF using
%% Rustler.
%% == Datalog Programs ==
%% == Stored Relations ==
%% == Working with Indices ==
%% === Covering Indices ===
%% === Proximity Indices ===
%% These kinds of indices allow Cozo to perform fast searches for similar data.
%% Cozo comes with three proximity index types:
%% <ul>
%% <li>The Hierarchincal Navigable Small World (HNSW) index is a graph-based
%% index that allows for fast approximate nearest neighbor searches</li>
%% <li>The MinHash-LSH index is a locality sensitive hash index that allows for
%% fast approximate nearest neighbor searches</li>
%% <li>The Full-text Search (FTS) index allows for fast string matches.</li>
%% </ul>
%%
%% ==== HSNW ====
%%
%% ```
%% {ok, _} = Module:create_index(Db, "table_hnsw_fun", "my_hsnw_index", #{
%%     type => hnsw,
%%     dim => 128,
%%     m => 50,
%%     ef_construction => 20,
%%     dtype => f32,
%%     distance => l2,
%%     fields => [v],
%%     filter => <<"k != 'foo'">>,
%%     extend_candidates => false,
%%     keep_pruned_connections => false
%% }).
%% '''
%%
%% ==== LSH Indices ====
%%
%% ```
%% {ok, _} = Module:create_index(Db, "table_lsh_fun", "my_lsh_index", #{
%%     type => lsh,
%%     extractor => v,
%%     extract_filter => "!is_null(v)",
%%     tokenizer => simple,
%%     filters => [alphanumonly],
%%     n_perm => 200,
%%     target_threshold => 0.7,
%%     n_gram => 3,
%%     false_positive_weight => 1.0,
%%     false_negative_weight => 1.0
%% }).
%% '''
%%
%% ==== FTS Indices ====
%% You can create an FTS index using {@link create_index/4}.
%%
%% The following example creates and index called `my_fts_index' on the relation
%% `rel_a'.
%%
%% ```
%% {ok, _} = Module:create_index(Db, "rel_a", "my_fts_index", #{
%%     type => fts,
%%     extractor => v,
%%     extract_filter => "!is_null(v)",
%%     tokenizer => simple,
%%     filters => [alphanumonly]
%% }).
%% '''
%%
%% You can always use Cozo Script directly via {@link run/2}. For example, the
%% following script is equivalent to the previous example.
%%
%% ```
%% ::fts create rel_a:my_fts_index {
%%     extractor: v,
%%     extract_filter: !is_null(v),
%%     tokenizer: Simple,
%%     filters: [],
%% }
%% '''
%% Check the full <a href="https://docs.cozodb.org/en/latest/vector.html#full-text-search-fts" target="_">FTS documentation</a>.
%% == System Operations ==
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
-define(IS_DB_HANDLE(X), (is_map(X) orelse is_reference(X))).

-opaque db_handle()           ::  map() | reference().
-type relations()           ::  #{
                                    relation_name() => #{
                                        headers => [binary()],
                                        rows => [row()]
                                    }
                                }.
-type relation_name()       ::  binary().
-type relation_spec()       ::  binary()
                                | #{
                                        keys => [
                                            column_name()
                                            | {column_name(), column_spec()}
                                        ],
                                        columns => [
                                            column_name()
                                            | {column_name(), column_spec()}
                                        ]
                                    }.
-type column_spec()         ::  undefined
                                |#{
                                    type => column_type(),
                                    nullable => boolean(),
                                    default => binary()
                                }.
-type column_name()         ::  binary().
-type column_type()         ::  column_atomic_type() | column_composite_type().
-type column_atomic_type()  ::  any  % Any
                                | bool % Bool
                                | bytes % Bytes
                                | json % Json
                                | int % Int
                                | float % Float
                                | string % String
                                | uuid % Uuid
                                | validity. % Validity
-type column_composite_type() ::
                                {list, column_atomic_type()}
                                | {list,
                                    column_atomic_type(), Size :: pos_integer()}
                                | {tuple, [column_atomic_type()]}
                                | {vector, 32|64, Size :: pos_integer()}.

-type engine()              ::  mem | sqlite | rocksdb.
-type engine_opts()         ::  map().
-type path()                ::  file:filename() | binary().
-type index_spec()          ::  covering_index_spec()
                                | hnsw_index_spec()
                                | lsh_index_spec()
                                | fts_index_spec().
-type covering_index_spec() ::  #{
                                    type := covering,
                                    fields := [column_name()]
                                }.
-type hnsw_index_spec()     ::  #{
                                    type := hnsw,
                                    dim := pos_integer(),
                                    m := pos_integer(),
                                    ef_construction := pos_integer(),
                                    fields := [column_name()],
                                    dtype => f32 | f64,
                                    distance => l2 | cosine | ip,
                                    filter => hnsw_filter(),
                                    extend_candidates => boolean(),
                                    keep_pruned_connections => boolean()
                                }.
-type lsh_index_spec()      ::  #{
                                    type := lsh,
                                    extractor := column_name(),
                                    tokenizer := tokenizer(),
                                    n_perm := pos_integer(),
                                    n_gram := pos_integer(),
                                    target_threshold := float(),
                                    extract_filter => extract_filter(),
                                    filters => [token_filter()],
                                    false_positive_weight => float(),
                                    false_negative_weight=> float()
                                }.
-type fts_index_spec()      ::  #{
                                    type := fts,
                                    extractor => column_name(),
                                    extract_filter => extract_filter(),
                                    tokenizer => tokenizer(),
                                    filters => [token_filter()]
                                }.
-type hnsw_filter()         ::  string().
-type extract_filter()      ::  string().
-type tokenizer()           ::  raw
                                | simple
                                | whitespace
                                | ngram % {ngram, 1, 1, false}
                                | {ngram,
                                    MinGram :: pos_integer(),
                                    MaxGram :: pos_integer(),
                                    PrefixOnly :: boolean()}
                                | {cangjie, default | all | search | unicode}.
-type token_filter()        ::  lowercase
                                | alphanumonly
                                | asciifolding
                                | {stemmer, Lang :: string()}
                                | {stopwords, Lang :: string()}.
-type query_opts()          ::  #{
                                    encoding => json | undefined,
                                    read_only => boolean(),
                                    params => map()
                                }.
-type query_return()        ::  {ok, query_result()}
                                | {ok, Json :: binary()}
                                | {error, Reason :: any()}.
-type query_result()        ::  #{
                                    headers := [column_name()],
                                    rows := [row()],
                                    count := integer(),
                                    next => [row()] | null,
                                    took => float() % secs
                                }.
-type row()                 ::  list(value()).
-type value()               ::  null | boolean() | integer() | float() | list()
                                | binary() | validity() | json().
-type json()                ::  {json, binary()}.
-type validity()            ::  {float(), boolean()}.
-type export_opts()         ::  #{encoding => json}.
-type info()                ::  #{engine := binary(), path := binary()}.
-type trigger_spec()        ::  #{trigger_event() => script()}.
-type trigger_event()       ::  on_put | on_remove | on_replace.
-type script()              ::  list() | binary().

-export_type([db_handle/0]).
-export_type([column_name/0]).
-export_type([index_spec/0]).
-export_type([info/0]).
-export_type([query_result/0]).
-export_type([relation_spec/0]).
-export_type([row/0]).
-export_type([script/0]).
-export_type([trigger_spec/0]).


%% API: Basics
-export([close/1]).
-export([open/0]).
-export([open/1]).
-export([open/2]).
-export([open/3]).
-export([run/2]).
-export([run/3]).



%% API: System Catalogue
-export([columns/2]).
-export([create_index/4]).
-export([create_relation/3]).
-export([describe/3]).
-export([drop_index/2]).
-export([drop_index/3]).
-export([indices/2]).
-export([relations/1]).
-export([remove_relation/2]).
-export([remove_relations/2]).
-export([triggers/2]).
-export([set_triggers/3]).
-export([delete_triggers/2]).

%% API: Utils
-export([explain/2]).
-export([info/1]).
-export([rows_to_maps/1]).
-export([resource/1]).

%% API: Operations
-export([backup/2]).
-export([export/2]).
-export([export/3]).
-export([import/2]).
-export([import_from_backup/3]).
-export([register_callback/2]).
-export([restore/2]).
-export([unregister_callback/2]).
%% -export([register_fixed_rule/2]).
%% -export([unregister_fixed_rule/2]).

%% API: Monitor
-export([running/1]).
-export([kill/2]).

%% API: Maintenance
-export([compact/1]).


-on_load(init/0).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Opens
%% @end
%% -----------------------------------------------------------------------------
-spec open() ->
    {ok, db_handle()} | {error, Reason :: any()} | no_return().

open() ->
    Engine = application:get_env(?APP, engine, mem),
    open(Engine).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec open(Engine :: engine()) ->
    {ok, db_handle()} | {error, Reason :: any()} | no_return().

open(Engine) ->
    DataDir = application:get_env(?APP, data_dir, "/tmp"),
    Path = filename:join([DataDir, "db"]),
    open(Engine, Path).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec open(Engine :: engine(), Path :: path()) ->
    {ok, db_handle()} | {error, Reason :: any()} | no_return().

open(Engine, Path) ->
    open(Engine, Path, engine_opts(Engine)).



%% -----------------------------------------------------------------------------
%% @doc Creates or opens an existing database.
%% `Path' is ignored when `Engine' is `mem'.
%% The database has to be explicitely closed using {@link close/1} for Erlang
%% to release the allocated ErlNIF resources.
%% `Opts' is ignored for every engine except `tikv'.
%% == RocksDB ==
%% To define options for RocksDB you should make sure a file named "config" is
%% present on `Path' before you call this function.
%% @end
%% -----------------------------------------------------------------------------
-spec open(Engine :: engine(), Path :: path(), Opts :: engine_opts()) ->
    {ok, db_handle()} | {error, Reason :: any()} | no_return().

open(Engine, Path, Opts) when is_list(Path), Path =/= [], is_map(Opts) ->
    open(Engine, list_to_binary(Path), Opts);

open(Engine, Path, Opts)
when is_atom(Engine), is_binary(Path), is_map(Opts) ->
    Engine == mem orelse Engine == sqlite orelse Engine == rocksdb orelse
        ?ERROR(badarg, [Engine, Path, Opts], #{
            1 => "the value must be the atom 'mem', 'rocksdb' or 'sqlite'"
        }),

    Path =/= <<>> orelse
        ?ERROR(badarg, [Engine, Path, Opts], #{
            2 => "a nonempty string or binary"
        }),

    new(atom_to_binary(Engine), Path, map_to_json(Opts)).


%% -----------------------------------------------------------------------------
%% @doc Closes the database.
%% Notice that the call is asyncronous and the database might take a while to
%% close and a subsequent invocation to {@link open/3} with the same `path'
%% might fail.
%% @end
%% -----------------------------------------------------------------------------
-spec close(DbHandle :: db_handle()) -> ok | {error, Reason :: any()}.

close(DbHandle) when ?IS_DB_HANDLE(DbHandle) ->
    close_nif(DbHandle).


%% -----------------------------------------------------------------------------
%% @doc Returns the CozoDB DBInstance as a NIF Resource.
%% For testing and planned extensions, you SHALL NOT use this function.
%% @end
%% -----------------------------------------------------------------------------
-spec resource(DbHandle :: db_handle()) -> {ok, reference()} | {error, any()}.

resource(DbHandle) when ?IS_DB_HANDLE(DbHandle) ->
    resource_nif(DbHandle).


%% -----------------------------------------------------------------------------
%% @doc Closes the database.
%% Notice that the call is asyncronous and the database might take a while to
%% close and a subsequent invocation to {@link open/3} with the same `path'
%% might fail.
%% @end
%% -----------------------------------------------------------------------------
-spec info(DbHandle :: db_handle()) -> info().

info(DbHandle) when ?IS_DB_HANDLE(DbHandle) ->
    info_nif(DbHandle).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec run(DbHandle :: db_handle(), Script :: script()) -> query_return().

run(DbHandle, Script) when Script == ""; Script == <<>> ->
    ?ERROR(badarg, [DbHandle, Script], #{
        1 => "script cannot be empty"
    });

run(DbHandle, Script) when is_list(Script) ->
    run(DbHandle, list_to_binary(Script));

run(DbHandle, Script) when ?IS_DB_HANDLE(DbHandle) andalso is_binary(Script) ->
    Params = map_to_json(#{}),
    ReadOnly = false,
    Meta = #{script => Script, db_handle => DbHandle, options => #{}},
    run_script_span(DbHandle, Script, Params, ReadOnly, Meta).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec run(
    DbHandle :: db_handle(), Script :: list() | binary(), Opts :: query_opts()) -> query_return().

run(DbHandle, Script, Opts) when is_list(Script) ->
    run(DbHandle, list_to_binary(Script), Opts);

run(DbHandle, Query, #{sparql := true} = Opts) ->
    case parse_sparql_nif(DbHandle, Query) of
        {ok, Script} ->
            run(DbHandle, Script, Opts);
        {error, _} = Error ->
            Error
    end;

run(DbHandle, Script, #{encoding := json} = Opts)
when ?IS_DB_HANDLE(DbHandle) andalso is_binary(Script) ->
    Params = map_to_json(maps:get(params, Opts, #{})),
    ReadOnly = maps:get(read_only, Opts, false),
    run_script_json_nif(DbHandle, Script, Params, ReadOnly);

run(DbHandle, Script, #{encoding := map} = Opts)
when ?IS_DB_HANDLE(DbHandle) andalso is_binary(Script) ->
    Params = map_to_json(maps:get(params, Opts, #{})),
    ReadOnly = maps:get(read_only, Opts, false),
    run_script_str_nif(DbHandle, Script, Params, ReadOnly);

run(DbHandle, Script, Opts)
when ?IS_DB_HANDLE(DbHandle) andalso is_binary(Script) andalso is_map(Opts) ->
    Params = map_to_json(maps:get(params, Opts, #{})),
    ReadOnly = maps:get(read_only, Opts, false),
    Meta = #{script => Script, db_handle => DbHandle, options => Opts},
    run_script_span(DbHandle, Script, Params, ReadOnly, Meta).


%% -----------------------------------------------------------------------------
%% @doc
%% Import data into a database. The data are imported inside a transaction, so
%% that either all imports are successful, or none are. If conflicts arise
%% because of concurrent modification to the database, via either CosoScript
%% queries or other imports, the transaction will fail.
%% The relations to import into must exist beforehand, and the data given must
%% match the schema defined.
%% This API can be used to batch-put or remove data from several stored
%% relations atomically. The data parameter can contain relation names such as
%% "rel_a", or relation names prefixed by a minus sign such as "-rel_a". For the
%% former case, every row given for the relation will be put into the database,
%% i.e. upsert semantics. For the latter case, the corresponding rows are
%% removed from the database, and you should only specify the key part of the
%% rows. As for rm in CozoScript, it is not an error to remove non-existent
%% rows.
%%
%% === Erlang Example ===
%% ```
%% #{
%%    rel_a => #{
%%        headers => ["x", "y"],
%%        rows => [[1, 2], [3, 4]]
%%    },
%%    rel_b => #{
%%        headers => ["z"],
%%        rows => []
%%    }
%% }
%% '''
%% @end
%% -----------------------------------------------------------------------------
-spec import(DbHandle :: db_handle(), Relations :: binary() | relations()) ->
    ok | {error, Reason :: any()}.

import(DbHandle, Relations)
when ?IS_DB_HANDLE(DbHandle) andalso is_map(Relations) ->
    import(DbHandle, map_to_json(Relations));

import(DbHandle, Relations)
when ?IS_DB_HANDLE(DbHandle) andalso is_binary(Relations) ->
    import_relations_nif(DbHandle, Relations).


%% -----------------------------------------------------------------------------
%% @doc
%% Export the specified relations in `Relations'.
%% It is guaranteed that the exported data form a consistent snapshot of what
%% was stored in the database.
%% Returns a map with binary keys for the names of relations, and values as maps
%% containing the `headers' and `rows' of the relation.
%% @end
%% -----------------------------------------------------------------------------
-spec export(
    DbHandle :: db_handle(), RelNames :: [relation_name()] | binary()) ->
    ok | {error, Reason :: any()}.

export(DbHandle, RelNames) ->
    export(DbHandle, RelNames, #{}).


%% -----------------------------------------------------------------------------
%% @doc
%% Export the specified relations in `Relations'.
%% It is guaranteed that the exported data form a consistent snapshot of what
%% was stored in the database.
%% Returns a map with binary keys for the names of relations, and values as maps
%% containing the `headers' and `rows' of the relation.
%% @end
%% -----------------------------------------------------------------------------
-spec export(
    DbHandle :: db_handle(),
    RelNames :: [relation_name()] | binary(),
    Opts :: export_opts()) ->
    {ok, relations() | binary()} | {error, Reason :: any()}.

export(DbHandle, RelNames, #{encoding := json})
when ?IS_DB_HANDLE(DbHandle) andalso is_binary(RelNames) ->
    export_relations_json_nif(DbHandle, RelNames);

export(DbHandle, RelNames, #{encoding := json})
when ?IS_DB_HANDLE(DbHandle) andalso is_list(RelNames) ->
    export_relations_json_nif(DbHandle, RelNames);

export(DbHandle, RelNames, _)
when ?IS_DB_HANDLE(DbHandle) andalso is_list(RelNames) ->
    export_relations_nif(DbHandle, RelNames).



%% -----------------------------------------------------------------------------
%% @doc Exports the database to a SQLite file at `Path'.
%% To restore the database using this file see {@link restore/2}.
%% @end
%% -----------------------------------------------------------------------------
-spec backup(DbHandle :: db_handle(), Path :: path()) ->
    ok | {error, Reason :: any()}.

backup(DbHandle, Path) when is_list(Path), Path =/= [] ->
    backup(DbHandle, list_to_binary(Path));

backup(DbHandle, Path) when is_binary(Path) ->
    backup_nif(DbHandle, Path).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec restore(DbHandle :: db_handle(), Path :: path()) ->
    ok | {error, Reason :: any()}.

restore(DbHandle, Path) when is_list(Path), Path =/= [] ->
    restore(DbHandle, list_to_binary(Path));

restore(DbHandle, Path) when is_binary(Path) ->
    restore_nif(DbHandle, Path).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec import_from_backup(
    DbHandle :: db_handle(), Path :: path(), Relations :: []) ->
    ok | {error, Reason :: any()}.

import_from_backup(DbHandle, Path, Relations) when is_list(Path), Path =/= [] ->
    import_from_backup(DbHandle, list_to_binary(Path), Relations);

import_from_backup(DbHandle, Path, Relations)
when is_binary(Path), is_list(Relations) ->
    import_from_backup_nif(DbHandle, Path, Relations).



%% =============================================================================
%% API: System Catalogue
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc List all existing relations.
%% @end
%% -----------------------------------------------------------------------------
-spec relations(DbHandle :: db_handle()) -> query_return().

relations(DbHandle) ->
    run(DbHandle, <<"::relations">>).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec create_relation(
    DbHandle :: db_handle(),
    RelName :: atom() | binary() | list(),
    Spec :: relation_spec()) ->
    ok | {error, Reason :: any()} | no_return().

create_relation(DbHandle, RelName, Spec) when is_atom(RelName) ->
    create_relation(DbHandle, atom_to_binary(RelName), Spec);

create_relation(DbHandle, RelName, Spec) when is_list(RelName) ->
    create_relation(DbHandle, list_to_binary(RelName), Spec);

create_relation(DbHandle, RelName, Spec) when is_binary(RelName), is_map(Spec) ->
    Encoded =
        try
            cozodb_script_utils:encode_relation_spec(Spec)
        catch
            error:{EReason, Message} ->
                ?ERROR(EReason, [DbHandle, RelName, Spec], #{3 => Message})
        end,

    Query = [<<":create">>, $\s, RelName, $\s, Encoded],

    case run(DbHandle, iolist_to_binary(Query)) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, format_reason(?FUNCTION_NAME, Reason)}
    end.


%% -----------------------------------------------------------------------------
%% @doc List columns for relation
%% @end
%% -----------------------------------------------------------------------------
-spec remove_relation(
    DbHandle :: db_handle(), RelNames :: [binary() | list()]) ->
    query_return().

remove_relation(DbHandle, RelName) when is_list(RelName) ->
    remove_relation(DbHandle, list_to_binary(RelName));

remove_relation(DbHandle, RelNames) ->
    case run(DbHandle, <<"::remove", $\s, RelNames/binary>>) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, format_reason(?FUNCTION_NAME, Reason)}
    end.


%% -----------------------------------------------------------------------------
%% @doc List columns for relation
%% @end
%% -----------------------------------------------------------------------------
-spec remove_relations(
    DbHandle :: db_handle(), RelNames :: [binary() | list()]) ->
    ok | {error, Reason :: any()}.

remove_relations(DbHandle, RelNames0) ->
    RelNames = iolist_to_binary(lists:join(", ", RelNames0)),
    case run(DbHandle, <<"::remove", $\s, RelNames/binary>>) of
        {ok, _} ->
            ok;
        {error, _} = Error ->
            Error
    end.


%% -----------------------------------------------------------------------------
%% @doc Create index for relation
%% @end
%% -----------------------------------------------------------------------------
-spec describe(
    DbHandle :: db_handle(),
    RelName :: binary() | list(),
    Desc :: binary() | list()) -> query_return().

describe(DbHandle, RelName, Desc) when is_list(RelName) ->
    describe(DbHandle, list_to_binary(RelName), Desc);

describe(DbHandle, RelName, Desc) when is_list(Desc) ->
    describe(DbHandle, RelName, list_to_binary(Desc));

describe(DbHandle, RelName, Desc) ->
    Cmd = <<"::describe", $\s, RelName/binary, $\s, Desc/binary, "?">>,
    run(DbHandle, Cmd).


%% -----------------------------------------------------------------------------
%% @doc List columns for relation
%% @end
%% -----------------------------------------------------------------------------
-spec columns(DbHandle :: db_handle(), RelName :: binary() | list()) ->
    query_return().

columns(DbHandle, RelName) when is_list(RelName) ->
    columns(DbHandle, list_to_binary(RelName));

columns(DbHandle, RelName) ->
    run(DbHandle, <<"::columns", $\s, RelName/binary>>).


%% -----------------------------------------------------------------------------
%% @doc List indices for relation
%% @end
%% -----------------------------------------------------------------------------
-spec indices(DbHandle :: db_handle(), RelName :: binary() | list()) ->
    query_return().

indices(DbHandle, RelName) when is_list(RelName) ->
    indices(DbHandle, list_to_binary(RelName));

indices(DbHandle, RelName) ->
    run(DbHandle, <<"::indices", $\s, RelName/binary>>).


%% -----------------------------------------------------------------------------
%% @doc Create index for relation
%%
%% === Hierarchical Navigable Small World (HNSW) Index
%% The parameters are:
%% <ul>
%% <li>The dimension `dim' and the data type `dtype' (defaults to `F32') has to
%% match the dimensions of any vector you index.</li>
%% <li> The fields parameter is a list of fields in the table that should be
%% indexed.</li>
%% <li>The indexed fields must only contain vectors of the same dimension and
%% data type, or null, or a list of vectors of the same dimension and data
%% type.</li>
%% <li>The distance parameter is the distance metric to use: the options are L2 (
%% default), Cosine and IP.</li>
%% <li>The m controls the maximal number of outgoing connections from each node
%% in the graph.</li>
%% <li>The ef_construction parameter is the number of nearest neighbors to use
%% when building the index: see the HNSW paper for details.</li>
%% <li>The filter parameter, when given, is bound to the fields of the original
%% relation and only those rows for which the expression evaluates to true are
%% indexed.</li>
%% <li>The extend_candidates parameter is a boolean (default false) that
%% controls whether the index should extend the candidate list with the nearest
%% neighbors of the nearest neighbors.</li>
%% <li>The keep_pruned_connections parameter is a boolean (default false) that
%% controls whether the index should keep pruned connections.
%% </li>
%% </ul>
%%
%% ==== Example ====
%% ```
%% 1> Spec = #{
%%     type => hnsw,
%%     dim => 128,
%%     m => 50,
%%     ef_construction => 20,
%%     dtype => f32,
%%     distance => l2,
%%     fields => [v],
%%     filter => <<"k != 'foo'">>,
%%     extend_candidates => false,
%%     keep_pruned_connections => false
%% }.
%% 2> create_index(Db, my_relation, Spec).
%% ok
%% '''
%% @end
%% -----------------------------------------------------------------------------
-spec create_index(
    DbHandle :: db_handle(),
    RelName :: binary() | list(),
    Name :: binary() | list(),
    Spec :: index_spec()) -> ok | {error, Reason :: any()} | no_return().

create_index(DbHandle, RelName, Name, #{type := Type} = Spec0)
when is_map(Spec0) ->
    Spec = cozodb_script_utils:encode_index_spec(Spec0),
    IndexOp = index_type_op(Type),
    Query = iolist_to_binary([
        $:, $:, IndexOp, $\s, "create", $\s, RelName, $:, Name, $\s, Spec
    ]),
    case run(DbHandle, Query) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, format_reason(?FUNCTION_NAME, Reason)}
    end;

create_index(DbHandle, RelName, Name, Spec) ->
    ?ERROR(badarg, [DbHandle, RelName, Name, Spec], #{
        4 =>
            "invalid value for field 'type'. "
            "Valid values are 'covering', 'hnsw', 'lsh' and 'fts'"
    }).


%% -----------------------------------------------------------------------------
%% @doc Drop index with fully qualified name.
%% @end
%% -----------------------------------------------------------------------------
-spec drop_index(DbHandle :: db_handle(), FQN :: binary() | list()) ->
    ok | {error, Reason :: any()} | no_return().

drop_index(DbHandle, FQN) when is_list(FQN) ->
    drop_index(DbHandle, list_to_binary(FQN));

drop_index(DbHandle, FQN) when is_binary(FQN) ->
    Cmd = <<"::index drop", $\s, FQN/binary>>,
    case run(DbHandle, Cmd) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, format_reason(?FUNCTION_NAME, Reason)}
    end.


%% -----------------------------------------------------------------------------
%% @doc Create index for relation
%% @end
%% -----------------------------------------------------------------------------
-spec drop_index(
    DbHandle :: db_handle(),
    RelName :: binary() | list(),
    Name :: binary() | list()) -> query_return().

drop_index(DbHandle, RelName, Name) when is_list(RelName) ->
    drop_index(DbHandle, list_to_binary(RelName), Name);

drop_index(DbHandle, RelName, Name) when is_list(Name) ->
    drop_index(DbHandle, RelName, list_to_binary(Name));

drop_index(DbHandle, RelName, Name) when is_binary(RelName), is_binary(Name) ->
    drop_index(DbHandle, <<RelName/binary, ":", Name/binary>>).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec triggers(DbHandle :: db_handle(), RelName :: binary() | list()) ->
    query_return().

triggers(DbHandle, RelName) when is_list(RelName) ->
    triggers(DbHandle, list_to_binary(RelName));

triggers(DbHandle, RelName) ->
    run(DbHandle, <<"::show_triggers", $\s, RelName/binary>>).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec set_triggers(
    DbHandle :: db_handle(),
    RelName :: binary() | list(),
    Specs :: [trigger_spec()]) -> query_return().

set_triggers(DbHandle, RelName, Spec) when is_list(RelName) ->
    set_triggers(DbHandle, list_to_binary(RelName), Spec);

set_triggers(DbHandle, RelName, Spec) when is_binary(RelName), is_list(Spec) ->
    Triggers = cozodb_script_utils:encode_triggers_spec(Spec),
    Cmd = iolist_to_binary([
        <<"::set_triggers", $\s, RelName/binary, $\n>> | Triggers
    ]),
    run(DbHandle, Cmd).


%% -----------------------------------------------------------------------------
%% @doc Calls {@link set_triggers/3} with and empty specs list.
%% @end
%% -----------------------------------------------------------------------------
-spec delete_triggers(DbHandle :: db_handle(), RelName :: binary() | list()) ->
    query_return().

delete_triggers(DbHandle, RelName) ->
    set_triggers(DbHandle, RelName, []).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec register_callback(DbHandle :: db_handle(), RelName :: binary()) ->
    ok.

register_callback(DbHandle, RelName) when is_list(RelName) ->
    register_callback(DbHandle, list_to_binary(RelName));

register_callback(DbHandle, RelName)
when ?IS_DB_HANDLE(DbHandle) andalso is_binary(RelName)->
    register_callback_nif(DbHandle, RelName).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec unregister_callback(DbHandle :: db_handle(), Id :: integer()) ->
    boolean().

unregister_callback(DbHandle, Id)
when ?IS_DB_HANDLE(DbHandle) andalso is_integer(Id)->
    unregister_callback_nif(DbHandle, Id).


%% =============================================================================
%% API: Monitor
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec running(DbHandle :: db_handle()) -> query_result().

running(DbHandle) ->
    run(DbHandle, <<"::running">>).


%% -----------------------------------------------------------------------------
%% @doc Kill the running query associated with identifier `Id'.
%% See {@link running/1} to get the list of running queries and their
%% identifiers.
%% @end
%% -----------------------------------------------------------------------------
-spec kill(DbHandle :: db_handle(), Id :: binary()) -> query_result().

kill(DbHandle, Id) when is_binary(Id) ->
    run(DbHandle, <<"::kill", $\s, Id/binary>>).



%% =============================================================================
%% API: Maintenance
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec compact(DbHandle :: db_handle()) -> ok | {error, Reason :: any()}.

compact(Dbhandle) ->
    case run(Dbhandle, <<"::compact">>) of
        {ok, _} ->
            ok;
        {error, _} = Error ->
            Error
    end.



%% =============================================================================
%% API: Utils
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec explain(DbHandle :: db_handle(), Query :: binary() | list()) ->
    query_return().

explain(DbHandle, Query) when is_list(Query) ->
    explain(DbHandle, list_to_binary(Query));

explain(DbHandle, Query) when is_binary(Query) ->
    run(DbHandle, <<"::explain", ${, $\s, Query/binary, $\s, $}>>).



%% -----------------------------------------------------------------------------
%% @doc Util function that takes a query_result() as argument and returns a list
%% of rows as maps.
%% @end
%% -----------------------------------------------------------------------------
-spec rows_to_maps(query_result()) -> map().

rows_to_maps(#{headers := Headers, rows := Rows, next := _N}) ->
    lists:foldl(
        fun(Row, Acc) ->
            [maps:from_list(lists:zip(Headers, Row)) | Acc]
        end,
        [],
        Rows
    ).





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
    %% Rustler macro
    ?load_nif_from_crate(Crate, 0).


%% -----------------------------------------------------------------------------
%% @private
%% @doc Calls native/cozodb/src/lib.rs::new
%% @end
%% -----------------------------------------------------------------------------
-spec new_nif(Engine :: binary(), Path :: binary(), Opts :: binary()) ->
    {ok, db_handle()} | {error, Reason :: any()}.

new_nif(_Engine, _Path, _Opts) ->
    ?NIF_NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec resource_nif(DbHandle :: db_handle()) ->
    {ok, reference()} | {error, Reason :: any()}.

resource_nif(_DbHandle) ->
    ?NIF_NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec info_nif(DbHandle :: db_handle()) ->
    #{engine := binary(), path := binary()}.

info_nif(_DbHandle) ->
    ?NIF_NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec close_nif(DbHandle :: db_handle()) ->
    {ok, info()} | {error, Reason :: any()}.

close_nif(_DbHandle) ->
    ?NIF_NOT_LOADED.

%% @private
run_script_span(DbHandle, Script, Params, ReadOnly, Meta) ->
    telemetry:span([cozodb, run], Meta, fun() ->
        case run_script_nif(DbHandle, Script, Params, ReadOnly) of
            {ok, _} = OK ->
                {OK, Meta};
            {error, Reason} = Error ->
                {Error, Meta#{error => Reason}}
        end
    end).

%% -----------------------------------------------------------------------------
%% @private
%% @doc Calls native/cozodb/src/lib.rs::run_script
%% @end
%% -----------------------------------------------------------------------------
-spec run_script_nif(
    DbHandle :: db_handle(),
    Script :: binary(),
    Params :: binary(),
    ReadOnly :: boolean()) ->
    {ok, Json :: binary()}.

run_script_nif(_DbHandle, _Script, _Params, _ReadOnly) ->
    ?NIF_NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @private
%% @doc Calls native/cozodb/src/lib.rs::run_script_json
%% @end
%% -----------------------------------------------------------------------------
-spec run_script_json_nif(
    DbHandle :: db_handle(),
    Script :: binary(),
    Params :: binary(),
    ReadOnly :: boolean()) ->
    {ok, Json :: binary()}.

run_script_json_nif(_DbHandle, _Script, _Params, _ReadOnly) ->
    ?NIF_NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @private
%% @doc Calls native/cozodb/src/lib.rs::run_script_json
%% @end
%% -----------------------------------------------------------------------------
-spec run_script_str_nif(
    DbHandle :: db_handle(),
    Script :: binary(),
    Params :: binary(),
    ReadOnly :: boolean()) ->
    {ok, Json :: binary()}.

run_script_str_nif(_DbHandle, _Script, _Params, _ReadOnly) ->
    ?NIF_NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @private
%% @doc Calls native/cozodb/src/lib.rs::run_script
%% @end
%% -----------------------------------------------------------------------------
-spec parse_sparql_nif(DbHandle :: db_handle(), Query :: binary()) ->
    {ok, Script :: binary()}.

parse_sparql_nif(_DbHandle, _Query) ->
    ?NIF_NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec import_relations_nif(DbHandle :: db_handle(), Relations :: binary()) ->
    ok | {error, Reason :: any()}.

import_relations_nif(_DbHandle, _Relations) ->
    ?NIF_NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec export_relations_nif(DbHandle :: db_handle(), Relations :: binary()) ->
    {ok, relations()} | {error, Reason :: any()}.

export_relations_nif(_DbHandle, _Relations) ->
    ?NIF_NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec export_relations_json_nif(DbHandle :: db_handle(), EncRelNames :: binary()) ->
    {ok, relations()} | {error, Reason :: any()}.

export_relations_json_nif(_DbHandle, _EncRelNames) ->
    ?NIF_NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec backup_nif(DbHandle :: db_handle(), Path :: path()) ->
    ok | {error, Reason :: any()}.

backup_nif(_DbHandle, _Path) ->
    ?NIF_NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec restore_nif(DbHandle :: db_handle(), Path :: path()) ->
    ok | {error, Reason :: any()}.

restore_nif(_DbHandle, _Path) ->
    ?NIF_NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec import_from_backup_nif(
    DbHandle :: db_handle(), Path :: binary(), Relations :: binary()) ->
    ok | {error, Reason :: any()}.

import_from_backup_nif(_DbHandle, _Path, _Relations) ->
    ?NIF_NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec register_callback_nif(DbHandle :: db_handle(), RelName :: binary()) ->
    ok.

register_callback_nif(_DbHandle, _RelName) ->
    ?NIF_NOT_LOADED.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec unregister_callback_nif(DbHandle :: db_handle(), Id :: integer()) ->
    ok.

unregister_callback_nif(_DbHandle, _Id) ->
    ?NIF_NOT_LOADED.



%% =============================================================================
%% PRIVATE: UTILS
%% =============================================================================



%% @private
new(Engine, Path, Opts)
when is_binary(Engine), is_binary(Path), is_binary(Opts) ->
    try
        new_nif(Engine, Path, Opts)
    catch
        Class:Reason:Stacktrace ->
            ?LOG_ERROR(#{
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {error, Reason}
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
format_reason(Op, Reason) when is_binary(Reason) ->
    %% #{FUNCTION_NAME => [{MatchRule, Cozo string pattern, Return]}
    AllRules = #{
        create_relation => [
            {match_suffix,
                <<"conflicts with an existing one">>, already_exists}
        ],
        create_index => [
            {match_suffix, <<"already exists">>, already_exists}
        ],
        remove_relation => [
            {match_prefix,
                <<"Cannot find requested stored relation">>, not_found}
        ]
    },

    %% Predicated used in lists:search
    Pred = fun
        ({match_suffix, Pattern, _Format}) ->
            Suffix = binary:longest_common_suffix([Reason, Pattern]),
            Suffix == byte_size(Pattern);
        ({match_prefix, Pattern, _Format}) ->
            Suffix = binary:longest_common_prefix([Reason, Pattern]),
            Suffix == byte_size(Pattern);
        (_) ->
            false
    end,

    %% Get rules associated with Op
    OpRules = maps:get(Op, AllRules, []),

    %% Try to match rules and return the term defined by the rule,
    %% otherwsie return Reason
    case lists:search(Pred, OpRules) of
        {value, {_, _, Return}} ->
            Return;
        false ->
            Reason
    end;

format_reason(_, Reason) ->
    Reason.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
engine_opts(mem) ->
    #{};

engine_opts(sqlite) ->
    application:get_env(?APP, sqlite_options, #{});

engine_opts(rocksdb) ->
    application:get_env(?APP, rocksdb_options, #{});

engine_opts(_Other) ->
    #{}.


%% @private
index_type_op(covering) -> <<"index">>;
index_type_op(fts) -> <<"fts">>;
index_type_op(hnsw) -> <<"hnsw">>;
index_type_op(lsh) -> <<"lsh">>;
index_type_op(_) -> error(badarg).



%% @private
-spec map_to_json(Term :: map()) -> binary() | no_return().

map_to_json(Term) when is_map(Term) ->
    Encoder = json_encoder(),
    try
        Encoder:encode(Term)
    catch
        error:function_clause:Stacktrace ->
            Cause =
                case Stacktrace of
                    [{thoas_encode, key, [Key, _], _} | _] ->
                        lists:flatten(
                            io_lib:format("invalid JSON key '~p'", [Key])
                        );
                    _ ->
                        "json encoding failed"
                end,
            error({badarg, Cause})
    end.



%%--------------------------------------------------------------------
%% @doc returns the default json encoder (thoas)
%% @end
%%--------------------------------------------------------------------
-spec json_encoder() -> atom().

json_encoder() ->
    application:get_env(?APP, json_parser, thoas).




%% =============================================================================
%% TESTS
%% =============================================================================




-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


-endif.
