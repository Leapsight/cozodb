%% =============================================================================
%%  cozodb_migrator_utils.erl -
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

-module(cozodb_migrator_utils).

-moduledoc """
Helper functions for common CozoDB migration patterns.

These utilities handle schema changes in CozoDB using a rename-based approach
that keeps data movement within the CozoDB engine via CozoScript queries,
avoiding loading all data into Erlang process memory.

For relations without an Erlang transform function, data is copied using a
single CozoScript `:put` query — zero Erlang memory. When an Erlang transform
function is needed, data is processed in bounded-memory batches using
cursor-based pagination.
""".

-export([
    recreate_relation/3,
    recreate_relation/4,
    reindex/4,
    copy_relation/3,
    copy_relation/4,
    copy_relation/5,
    rename_relation/3,
    relation_exists/2,
    ensure_relation/3
]).

-define(DEFAULT_BATCH_SIZE, 10_000).

-type recreate_opts() :: #{
    column_mapping => #{binary() => term()},
    transform => fun((map()) -> map()),
    skip_indices => boolean(),
    skip_triggers => boolean(),
    batch_size => pos_integer()
}.

%% =============================================================================
%% API
%% =============================================================================

-doc """
Recreate a relation with a new schema, preserving data.
""".
-spec recreate_relation(
    cozodb:db_handle(), binary(), cozodb:relation_spec()
) -> ok | {error, term()}.

recreate_relation(Db, RelName, NewSpec) ->
    recreate_relation(Db, RelName, NewSpec, #{}).

-doc """
Recreate a relation with a new schema and options.

Uses a rename-based approach to avoid loading all data into memory:
1. Clean up any stale temp relation from a prior failed attempt
2. Capture indices and triggers
3. Drop all indices (makes rename safe — CozoDB rename bug with indices)
4. Rename original to temp (zero-copy, data stays on disk)
5. Create new relation with new schema + indices
6. Copy data: CozoScript `:put` (zero memory) or batched (bounded memory)
7. Restore triggers, drop temp

Options:
- `column_mapping` - Map of new column names to default values for columns
  not present in the old data
- `transform` - Function applied to each row (as a map) during re-import.
  When provided, uses batched cursor-based processing with bounded memory.
- `skip_indices` - Don't recreate indices (default: `false`)
- `skip_triggers` - Don't recreate triggers (default: `false`)
- `batch_size` - Number of rows per batch when transform is used
  (default: 10000)
""".
-spec recreate_relation(
    cozodb:db_handle(), binary(), cozodb:relation_spec(), recreate_opts()
) -> ok | {error, term()}.

recreate_relation(Db, RelName, NewSpec, Opts) ->
    TmpName = generate_tmp_name(RelName),
    maybe
        %% 1. Clean up any stale temp relation from a prior failed attempt
        ok ?= cleanup_stale_tmp(Db, RelName, TmpName),
        %% 2. Capture indices (unless skipping)
        {ok, Indices} ?= capture_indices(Db, RelName, Opts),
        %% 3. Capture triggers (unless skipping)
        {ok, Triggers} ?= capture_triggers(Db, RelName, Opts),
        %% 4. Get old column info
        {ok, {OldKeys, OldVals}} ?= columns_from_info(Db, RelName),
        %% 5. Drop all indices (makes rename safe)
        ok ?= drop_all_indices(Db, RelName, Indices),
        %% 6. Rename original to temp (zero-copy, no indices)
        ok ?= rename_relation(Db, RelName, TmpName),
        %% 7. Create with new schema
        ok ?= cozodb:create_relation(Db, RelName, NewSpec),
        %% 8. Recreate indices on the empty new relation
        ok ?= restore_indices(Db, RelName, Indices),
        %% 9. Get new column info
        {ok, {NewKeys, NewVals}} ?= columns_from_info(Db, RelName),
        %% 10. Copy data
        Mapping = maps:get(column_mapping, Opts, #{}),
        ok ?=
            case maps:get(transform, Opts, undefined) of
                undefined ->
                    %% Zero Erlang memory — single CozoScript :put
                    Script = build_copy_script(
                        TmpName,
                        RelName,
                        {OldKeys, OldVals},
                        {NewKeys, NewVals},
                        Mapping
                    ),
                    case cozodb:run(Db, Script) of
                        {ok, _} -> ok;
                        {error, _} = CopyErr -> CopyErr
                    end;
                TransformFun ->
                    %% Bounded memory — batched cursor processing
                    BatchSize = maps:get(batch_size, Opts, ?DEFAULT_BATCH_SIZE),
                    copy_batched(
                        Db,
                        TmpName,
                        RelName,
                        OldKeys,
                        OldKeys ++ OldVals,
                        NewKeys,
                        NewKeys ++ NewVals,
                        TransformFun,
                        Mapping,
                        BatchSize
                    )
            end,
        %% 11. Restore triggers
        ok ?= restore_triggers(Db, RelName, Triggers),
        %% 12. Drop temp relation (clean, no indices attached)
        ok ?= cozodb:remove_relation(Db, TmpName),
        ok
    end.

-doc """
Replace an index on a relation, preserving data.

Uses the rename-based approach: captures all indices and triggers, renames
the relation to a temp name, recreates with the same schema, restores all
indices except the one being replaced, creates the new index, copies data
via CozoScript, then restores triggers and drops temp.
""".
-spec reindex(
    cozodb:db_handle(), binary(), binary(), cozodb:index_spec()
) -> ok | {error, term()}.

reindex(Db, RelName, IndexName, IndexSpec) ->
    TmpName = generate_tmp_name(RelName),
    maybe
        %% 1. Clean up any stale temp relation
        ok ?= cleanup_stale_tmp(Db, RelName, TmpName),
        %% 2. Capture ALL indices
        {ok, Indices} ?= capture_indices(Db, RelName, #{}),
        %% 3. Capture triggers
        {ok, Triggers} ?= capture_triggers(Db, RelName, #{}),
        %% 4. Get column info
        {ok, {Keys, Vals}} ?= columns_from_info(Db, RelName),
        %% 5. Build same-schema spec from columns
        {ok, RebuiltSpec} ?= build_relation_spec_from_columns(Db, RelName),
        %% 6. Drop all indices (makes rename safe)
        ok ?= drop_all_indices(Db, RelName, Indices),
        %% 7. Rename to temp (zero-copy, no indices)
        ok ?= rename_relation(Db, RelName, TmpName),
        %% 8. Create with rebuilt spec
        ok ?= cozodb:create_relation(Db, RelName, RebuiltSpec),
        %% 9. Restore all indices EXCEPT the one being replaced
        ok ?= restore_indices_except(Db, RelName, Indices, IndexName),
        %% 10. Create new index
        ok ?= cozodb:create_index(Db, RelName, IndexName, IndexSpec),
        %% 11. Copy data via CozoScript (zero Erlang memory)
        Script = build_copy_script(
            TmpName,
            RelName,
            {Keys, Vals},
            {Keys, Vals},
            #{}
        ),
        {ok, _} ?= cozodb:run(Db, Script),
        %% 12. Restore triggers
        ok ?= restore_triggers(Db, RelName, Triggers),
        %% 13. Drop temp relation (clean, no indices)
        ok ?= cozodb:remove_relation(Db, TmpName),
        ok
    end.

-doc """
Copy data from one existing relation to another using CozoScript.

Both relations must already exist. Data is copied entirely within the
CozoDB engine — zero Erlang memory usage.
""".
-spec copy_relation(cozodb:db_handle(), binary(), binary()) ->
    ok | {error, term()}.

copy_relation(Db, Source, Target) ->
    maybe
        {ok, {SrcKeys, SrcVals}} ?= columns_from_info(Db, Source),
        {ok, {TgtKeys, TgtVals}} ?= columns_from_info(Db, Target),
        Script = build_copy_script(
            Source,
            Target,
            {SrcKeys, SrcVals},
            {TgtKeys, TgtVals},
            #{}
        ),
        {ok, _} ?= cozodb:run(Db, Script),
        ok
    end.

-doc """
Copy data from one relation to another, applying a transform function.

The target relation must already exist. Uses batched cursor-based processing
with bounded memory.
""".
-spec copy_relation(
    cozodb:db_handle(), binary(), binary(), fun((map()) -> map())
) -> ok | {error, term()}.

copy_relation(Db, Source, Target, Transform) ->
    copy_relation(Db, Source, Target, Transform, #{}).

-doc """
Copy data from one relation to another with transform and options.

The target relation must already exist. Uses batched cursor-based processing
with bounded memory.

Options:
- `batch_size` - Number of rows per batch (default: 10000)
- `column_mapping` - Map of column names to default values
""".
-spec copy_relation(
    cozodb:db_handle(), binary(), binary(), fun((map()) -> map()), map()
) -> ok | {error, term()}.

copy_relation(Db, Source, Target, Transform, Opts) ->
    maybe
        {ok, {SrcKeys, SrcVals}} ?= columns_from_info(Db, Source),
        {ok, {TgtKeys, TgtVals}} ?= columns_from_info(Db, Target),
        BatchSize = maps:get(batch_size, Opts, ?DEFAULT_BATCH_SIZE),
        Mapping = maps:get(column_mapping, Opts, #{}),
        ok ?=
            copy_batched(
                Db,
                Source,
                Target,
                SrcKeys,
                SrcKeys ++ SrcVals,
                TgtKeys,
                TgtKeys ++ TgtVals,
                Transform,
                Mapping,
                BatchSize
            ),
        ok
    end.

-doc """
Rename a relation using CozoScript `::rename`.
""".
-spec rename_relation(cozodb:db_handle(), binary(), binary()) ->
    ok | {error, term()}.

rename_relation(Db, OldName, NewName) ->
    Script = <<"::rename ", OldName/binary, " -> ", NewName/binary>>,
    case cozodb:run(Db, Script) of
        {ok, _} -> ok;
        {error, _} = Error -> Error
    end.

-doc """
Check whether a relation exists.
""".
-spec relation_exists(cozodb:db_handle(), binary()) -> boolean().

relation_exists(Db, RelName) ->
    case cozodb:columns(Db, RelName) of
        {ok, _} -> true;
        {error, _} -> false
    end.

-doc """
Create a relation if it doesn't already exist.

Returns `ok` both when creating a new relation and when the relation
already exists.
""".
-spec ensure_relation(
    cozodb:db_handle(), binary(), cozodb:relation_spec()
) -> ok | {error, term()}.

ensure_relation(Db, RelName, Spec) ->
    case cozodb:create_relation(Db, RelName, Spec) of
        ok ->
            ok;
        {error, already_exists} ->
            ok;
        {error, #{message := already_exists}} ->
            ok;
        {error, _} = Error ->
            Error
    end.

%% =============================================================================
%% PRIVATE — Column Introspection
%% =============================================================================

-spec columns_from_info(cozodb:db_handle(), binary()) ->
    {ok, {[binary()], [binary()]}} | {error, term()}.

columns_from_info(Db, RelName) ->
    case cozodb:columns(Db, RelName) of
        {ok, #{rows := ColRows}} ->
            %% ::columns returns rows like [Name, IsKey, Type, ...]
            %% Partition into keys and values preserving order
            {Keys, Vals} = lists:foldl(
                fun([Name, IsKey | _], {KAcc, VAcc}) ->
                    case IsKey of
                        true -> {KAcc ++ [Name], VAcc};
                        false -> {KAcc, VAcc ++ [Name]}
                    end
                end,
                {[], []},
                ColRows
            ),
            {ok, {Keys, Vals}};
        {error, _} = Error ->
            Error
    end.

-spec build_relation_spec_from_columns(cozodb:db_handle(), binary()) ->
    {ok, cozodb:relation_spec()} | {error, term()}.

build_relation_spec_from_columns(Db, RelName) ->
    case cozodb:columns(Db, RelName) of
        {ok, #{rows := ColRows}} ->
            {KeySpecs, ValSpecs} = lists:foldl(
                fun([Name, IsKey, TypeStr | _], {KAcc, VAcc}) ->
                    Type = decode_column_type(TypeStr),
                    Entry = {Name, Type},
                    case IsKey of
                        true -> {KAcc ++ [Entry], VAcc};
                        false -> {KAcc, VAcc ++ [Entry]}
                    end
                end,
                {[], []},
                ColRows
            ),
            {ok, #{keys => KeySpecs, columns => ValSpecs}};
        {error, _} = Error ->
            Error
    end.

%% =============================================================================
%% PRIVATE — CozoScript Generation
%% =============================================================================

-spec build_copy_script(
    binary(),
    binary(),
    {[binary()], [binary()]},
    {[binary()], [binary()]},
    #{binary() => term()}
) -> binary().

build_copy_script(Source, Target, {SrcKeys, SrcVals}, {TgtKeys, TgtVals}, Mapping) ->
    SrcAll = SrcKeys ++ SrcVals,
    TgtAll = TgtKeys ++ TgtVals,
    SrcSet = sets:from_list(SrcAll, [{version, 2}]),

    %% Columns in target that exist in source — read from source
    ReadCols = [C || C <- TgtAll, sets:is_element(C, SrcSet)],
    %% Columns in target that are new — need defaults from Mapping
    NewCols = [C || C <- TgtAll, not sets:is_element(C, SrcSet)],

    %% Build the rule head: ?[col1, col2, ...]
    HeadVars = lists:join(<<", ">>, TgtAll),
    Head = iolist_to_binary([<<"?[">>, HeadVars, <<"]">>]),

    %% Build the rule body: *source{read_cols...}, new_col = default, ...
    SrcBindVars = lists:join(<<", ">>, ReadCols),
    SrcAtom = iolist_to_binary([<<"*">>, Source, <<"{">>, SrcBindVars, <<"}">>]),

    DefaultBindings = lists:map(
        fun(Col) ->
            Default = maps:get(Col, Mapping, null),
            iolist_to_binary([Col, <<" = ">>, encode_cozo_value(Default)])
        end,
        NewCols
    ),

    BodyParts = [SrcAtom | DefaultBindings],
    Body = lists:join(<<", ">>, BodyParts),

    %% Build the :put command
    TgtKeyVars = lists:join(<<", ">>, TgtKeys),
    TgtValVars = lists:join(<<", ">>, TgtVals),
    PutCmd =
        case TgtVals of
            [] ->
                iolist_to_binary([<<":put ">>, Target, <<" {">>, TgtKeyVars, <<"}">>]);
            _ ->
                iolist_to_binary([
                    <<":put ">>,
                    Target,
                    <<" {">>,
                    TgtKeyVars,
                    <<" => ">>,
                    TgtValVars,
                    <<"}">>
                ])
        end,

    iolist_to_binary([Head, <<" := ">>, Body, <<"\n">>, PutCmd]).

-spec build_cursor_query(
    binary(), [binary()], [binary()], undefined | [term()], pos_integer()
) -> {binary(), map()}.

build_cursor_query(Source, AllColumns, KeyColumns, undefined, BatchSize) ->
    %% First batch — no cursor
    ColVars = lists:join(<<", ">>, AllColumns),
    KeyVars = lists:join(<<", ">>, KeyColumns),
    Script = iolist_to_binary([
        <<"?[">>,
        ColVars,
        <<"] := *">>,
        Source,
        <<"{">>,
        ColVars,
        <<"}\n">>,
        <<":order ">>,
        KeyVars,
        <<"\n">>,
        <<":limit $batch_size">>
    ]),
    {Script, #{<<"batch_size">> => BatchSize}};
build_cursor_query(Source, AllColumns, KeyColumns, CursorValues, BatchSize) ->
    %% Subsequent batches — lexicographic cursor with multiple rules
    ColVars = lists:join(<<", ">>, AllColumns),
    KeyVars = lists:join(<<", ">>, KeyColumns),

    %% Generate one rule per key prefix for lexicographic comparison:
    %% For keys [k1, k2, k3]:
    %%   Rule 1: k1 > $c1
    %%   Rule 2: k1 == $c1, k2 > $c2
    %%   Rule 3: k1 == $c1, k2 == $c2, k3 > $c3
    NumKeys = length(KeyColumns),
    Rules = lists:map(
        fun(RuleIdx) ->
            %% Keys before this index: equality constraints
            EqParts = lists:map(
                fun(I) ->
                    K = lists:nth(I, KeyColumns),
                    CParam = cursor_param_name(I),
                    iolist_to_binary([K, <<" == $">>, CParam])
                end,
                lists:seq(1, RuleIdx - 1)
            ),
            %% The key at this index: greater-than constraint
            GtKey = lists:nth(RuleIdx, KeyColumns),
            GtParam = cursor_param_name(RuleIdx),
            GtPart = iolist_to_binary([GtKey, <<" > $">>, GtParam]),

            Constraints = EqParts ++ [GtPart],
            ConstraintStr = lists:join(<<", ">>, Constraints),

            iolist_to_binary([
                <<"?[">>,
                ColVars,
                <<"] := *">>,
                Source,
                <<"{">>,
                ColVars,
                <<"}, ">>,
                ConstraintStr
            ])
        end,
        lists:seq(1, NumKeys)
    ),

    RulesStr = lists:join(<<"\n">>, Rules),
    Script = iolist_to_binary([
        RulesStr,
        <<"\n">>,
        <<":order ">>,
        KeyVars,
        <<"\n">>,
        <<":limit $batch_size">>
    ]),

    %% Build params: c1, c2, ... from cursor values
    Params = lists:foldl(
        fun(I, Acc) ->
            ParamName = cursor_param_name(I),
            Value = lists:nth(I, CursorValues),
            Acc#{ParamName => Value}
        end,
        #{<<"batch_size">> => BatchSize},
        lists:seq(1, NumKeys)
    ),
    {Script, Params}.

%% =============================================================================
%% PRIVATE — Batched Copy
%% =============================================================================

-spec copy_batched(
    cozodb:db_handle(),
    binary(),
    binary(),
    [binary()],
    [binary()],
    [binary()],
    [binary()],
    fun((map()) -> map()),
    #{binary() => term()},
    pos_integer()
) -> ok | {error, term()}.

copy_batched(
    Db,
    Source,
    Target,
    SrcKeyNames,
    SrcAllNames,
    TgtKeyNames,
    TgtAllNames,
    TransformFun,
    Mapping,
    BatchSize
) ->
    do_copy_batched(
        Db,
        Source,
        Target,
        SrcKeyNames,
        SrcAllNames,
        TgtKeyNames,
        TgtAllNames,
        TransformFun,
        Mapping,
        BatchSize,
        undefined
    ).

do_copy_batched(
    Db,
    Source,
    Target,
    SrcKeyNames,
    SrcAllNames,
    TgtKeyNames,
    TgtAllNames,
    TransformFun,
    Mapping,
    BatchSize,
    CursorValues
) ->
    {Script, Params} = build_cursor_query(
        Source, SrcAllNames, SrcKeyNames, CursorValues, BatchSize
    ),
    case cozodb:run(Db, Script, #{parameters => Params}) of
        {ok, #{rows := []}} ->
            ok;
        {ok, #{rows := Rows}} ->
            %% Transform each row and collect for import
            TgtHeaders = TgtAllNames,
            TransformedRows = lists:map(
                fun(Row) ->
                    %% Zip source names with row values → map
                    RowMap = maps:from_list(lists:zip(SrcAllNames, Row)),
                    %% Add defaults for new columns from Mapping
                    RowMapWithDefaults = lists:foldl(
                        fun(H, Acc) ->
                            case maps:is_key(H, Acc) of
                                true -> Acc;
                                false -> Acc#{H => maps:get(H, Mapping, null)}
                            end
                        end,
                        RowMap,
                        TgtAllNames
                    ),
                    %% Apply transform
                    TransformedMap = TransformFun(RowMapWithDefaults),
                    %% Extract values in target column order
                    [maps:get(H, TransformedMap, null) || H <- TgtAllNames]
                end,
                Rows
            ),
            %% Import batch into target
            case
                cozodb:import(Db, #{
                    Target => #{
                        headers => TgtHeaders,
                        rows => TransformedRows
                    }
                })
            of
                ok when length(Rows) < BatchSize ->
                    %% Last batch
                    ok;
                ok ->
                    %% Advance cursor: extract key values from last row
                    LastRow = lists:last(Rows),
                    NumKeys = length(SrcKeyNames),
                    NewCursor = lists:sublist(LastRow, NumKeys),
                    do_copy_batched(
                        Db,
                        Source,
                        Target,
                        SrcKeyNames,
                        SrcAllNames,
                        TgtKeyNames,
                        TgtAllNames,
                        TransformFun,
                        Mapping,
                        BatchSize,
                        NewCursor
                    );
                {error, _} = ImportErr ->
                    ImportErr
            end;
        {error, _} = QueryErr ->
            QueryErr
    end.

%% =============================================================================
%% PRIVATE — Index/Trigger Capture & Restore
%% =============================================================================

%% Captured indices are a list of {IndexName, IndexSpec} tuples.
%% IndexSpec is a cozodb:index_spec() map suitable for cozodb:create_index/4.

-spec capture_indices(cozodb:db_handle(), binary(), recreate_opts()) ->
    {ok, [{binary(), cozodb:index_spec()}]} | {error, term()}.

capture_indices(_Db, _RelName, #{skip_indices := true}) ->
    {ok, []};
capture_indices(Db, RelName, _Opts) ->
    case cozodb:indices(Db, RelName) of
        {ok, #{rows := []}} ->
            {ok, []};
        {ok, #{rows := IdxRows}} ->
            %% Get column info to map positions back to names
            case cozodb:columns(Db, RelName) of
                {ok, #{rows := ColRows}} ->
                    PosToName = build_pos_to_name_map(ColRows),
                    NumKeys = length([1 || [_, true | _] <- ColRows]),
                    Specs = lists:filtermap(
                        fun(IdxRow) ->
                            parse_index_row(IdxRow, PosToName, NumKeys)
                        end,
                        IdxRows
                    ),
                    {ok, Specs};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

-spec capture_triggers(cozodb:db_handle(), binary(), recreate_opts()) ->
    {ok, term()} | {error, term()}.

capture_triggers(_Db, _RelName, #{skip_triggers := true}) ->
    {ok, []};
capture_triggers(Db, RelName, _Opts) ->
    case cozodb:triggers(Db, RelName) of
        {ok, #{rows := Rows, headers := Headers}} ->
            {ok, {Headers, Rows}};
        {error, _} = Error ->
            Error
    end.

-spec restore_indices(cozodb:db_handle(), binary(), [{binary(), cozodb:index_spec()}]) ->
    ok | {error, term()}.

restore_indices(_Db, _RelName, []) ->
    ok;
restore_indices(Db, RelName, [{IndexName, IndexSpec} | Rest]) ->
    case cozodb:create_index(Db, RelName, IndexName, IndexSpec) of
        ok -> restore_indices(Db, RelName, Rest);
        {error, _} = Error -> Error
    end.

-spec restore_indices_except(
    cozodb:db_handle(), binary(), [{binary(), cozodb:index_spec()}], binary()
) -> ok | {error, term()}.

restore_indices_except(Db, RelName, Indices, ExcludeName) ->
    Filtered = [{N, S} || {N, S} <- Indices, N =/= ExcludeName],
    restore_indices(Db, RelName, Filtered).

-spec restore_triggers(cozodb:db_handle(), binary(), term()) ->
    ok | {error, term()}.

restore_triggers(_Db, _RelName, []) ->
    ok;
restore_triggers(Db, RelName, {_Headers, Rows}) ->
    Scripts = [Row || Row <- Rows],
    case Scripts of
        [] ->
            ok;
        _ ->
            TriggerLines = lists:join(
                <<"\n">>,
                [iolist_to_binary(S) || S <- Scripts]
            ),
            Script = iolist_to_binary([
                <<"::set_triggers ">>, RelName, <<"\n">> | TriggerLines
            ]),
            case cozodb:run(Db, Script) of
                {ok, _} -> ok;
                {error, _} = Error -> Error
            end
    end.

-spec drop_all_indices(
    cozodb:db_handle(), binary(), [{binary(), cozodb:index_spec()}]
) -> ok | {error, term()}.

drop_all_indices(_Db, _RelName, []) ->
    ok;
drop_all_indices(Db, RelName, [{IndexName, _} | Rest]) ->
    case cozodb:drop_index(Db, RelName, IndexName) of
        ok -> drop_all_indices(Db, RelName, Rest);
        {error, _} = Error -> Error
    end.

%% @private
%% Parse a row from ::indices into {IndexName, IndexSpec} for cozodb:create_index/4.
%% ::indices returns rows: [Name, Type, Relations, Config]
%% For covering ("normal") indices, Config is {json, <<"{\"indices\":[col_positions]}">>}
-spec parse_index_row(list(), #{non_neg_integer() => binary()}, non_neg_integer()) ->
    {true, {binary(), cozodb:index_spec()}} | false.

parse_index_row([IndexName, <<"normal">>, _Rels, Config | _], PosToName, NumKeys) ->
    %% Covering index: extract column positions from config, map to field names
    Positions = extract_index_positions(Config),
    %% Positions list is: [user_field_positions..., key_positions...]
    %% The last NumKeys entries are the relation's key columns (auto-included).
    %% The preceding entries are the user-specified fields.
    NumFields = length(Positions) - NumKeys,
    FieldPositions = lists:sublist(Positions, NumFields),
    Fields = [maps:get(Pos, PosToName) || Pos <- FieldPositions],
    {true, {IndexName, #{type => covering, fields => Fields}}};
parse_index_row(_Row, _PosToName, _NumKeys) ->
    %% Non-covering indices (HNSW, FTS, LSH) are not yet supported for
    %% automatic capture/restore. Skip them.
    false.

%% @private
%% Extract the "indices" array from the index config JSON.
-spec extract_index_positions({json, binary()} | term()) -> [non_neg_integer()].

extract_index_positions({json, JsonBin}) ->
    Decoded = json:decode(JsonBin),
    maps:get(<<"indices">>, Decoded);
extract_index_positions(_) ->
    [].

%% @private
%% Build a map from column position (the "index" field from ::columns) to column name.
-spec build_pos_to_name_map([[term()]]) -> #{non_neg_integer() => binary()}.

build_pos_to_name_map(ColRows) ->
    %% ::columns rows: [Name, IsKey, Index, Type, HasDefault, DefaultExpr]
    lists:foldl(
        fun([Name, _IsKey, Pos | _], Acc) ->
            Acc#{Pos => Name}
        end,
        #{},
        ColRows
    ).

%% =============================================================================
%% PRIVATE — Cleanup & Naming
%% =============================================================================

-spec generate_tmp_name(binary()) -> binary().

generate_tmp_name(RelName) ->
    <<"cozodb_migrator_tmp_", RelName/binary>>.

-spec cleanup_stale_tmp(cozodb:db_handle(), binary(), binary()) ->
    ok | {error, term()}.

cleanup_stale_tmp(Db, RelName, TmpName) ->
    TmpExists = relation_exists(Db, TmpName),
    RelExists = relation_exists(Db, RelName),
    case {TmpExists, RelExists} of
        {true, true} ->
            %% Stale leftover — drop it
            cozodb:remove_relation(Db, TmpName);
        {true, false} ->
            %% Failed mid-rename — rename back
            rename_relation(Db, TmpName, RelName);
        _ ->
            ok
    end.

%% =============================================================================
%% PRIVATE — Value Encoding
%% =============================================================================

-spec encode_cozo_value(term()) -> binary().

encode_cozo_value(null) ->
    <<"null">>;
encode_cozo_value(nil) ->
    <<"null">>;
encode_cozo_value(undefined) ->
    <<"null">>;
encode_cozo_value(true) ->
    <<"true">>;
encode_cozo_value(false) ->
    <<"false">>;
encode_cozo_value(V) when is_integer(V) -> integer_to_binary(V);
encode_cozo_value(V) when is_float(V) -> list_to_binary(io_lib_format:fwrite_g(V));
encode_cozo_value(V) when is_binary(V) ->
    %% Escape single quotes in the string
    Escaped = binary:replace(V, <<"'">>, <<"\\'">>, [global]),
    <<"'", Escaped/binary, "'">>;
encode_cozo_value(V) when is_atom(V) ->
    encode_cozo_value(atom_to_binary(V, utf8));
encode_cozo_value(V) when is_list(V) ->
    Encoded = lists:join(<<", ">>, [encode_cozo_value(E) || E <- V]),
    iolist_to_binary([<<"[">>, Encoded, <<"]">>]).

%% =============================================================================
%% PRIVATE — Type Decoding
%% =============================================================================

-spec decode_column_type(binary()) -> atom() | {atom(), atom()} | {atom(), list()}.

decode_column_type(<<"Int">>) ->
    int;
decode_column_type(<<"Float">>) ->
    float;
decode_column_type(<<"String">>) ->
    string;
decode_column_type(<<"Bool">>) ->
    bool;
decode_column_type(<<"Bytes">>) ->
    bytes;
decode_column_type(<<"Json">>) ->
    json;
decode_column_type(<<"Uuid">>) ->
    uuid;
decode_column_type(<<"Validity">>) ->
    validity;
decode_column_type(<<"Any">>) ->
    any;
decode_column_type(<<"Int?">>) ->
    #{type => int, nullable => true};
decode_column_type(<<"Float?">>) ->
    #{type => float, nullable => true};
decode_column_type(<<"String?">>) ->
    #{type => string, nullable => true};
decode_column_type(<<"Bool?">>) ->
    #{type => bool, nullable => true};
decode_column_type(<<"Bytes?">>) ->
    #{type => bytes, nullable => true};
decode_column_type(<<"Json?">>) ->
    #{type => json, nullable => true};
decode_column_type(<<"Uuid?">>) ->
    #{type => uuid, nullable => true};
decode_column_type(<<"Validity?">>) ->
    #{type => validity, nullable => true};
decode_column_type(<<"Any?">>) ->
    #{type => any, nullable => true};
decode_column_type(_Other) ->
    %% Fallback for complex types — use undefined (no type constraint)
    undefined.

%% =============================================================================
%% PRIVATE — Helpers
%% =============================================================================

-spec cursor_param_name(pos_integer()) -> binary().

cursor_param_name(I) ->
    <<"c", (integer_to_binary(I))/binary>>.
