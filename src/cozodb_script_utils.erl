%% =============================================================================
%%  cozodb_script_utils.erl -
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

-module(cozodb_script_utils).

%% API
-export([encode_relation_spec/1]).
-export([encode_index_spec/1]).
-export([encode_triggers_spec/1]).
-export([to_quoted_string/1]).
-export([to_quoted_string/2]).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec encode_relation_spec(cozodb:relation_spec()) -> iolist() | no_return().

encode_relation_spec(#{keys := [], columns := []}) ->
    error({badarg, "invalid specification"});

encode_relation_spec(Map) when is_map(Map), not is_map_key(keys, Map) ->
    encode_relation_spec(Map#{keys => []});

encode_relation_spec(Map) when is_map(Map), not is_map_key(columns, Map) ->
    encode_relation_spec(Map#{columns => []});

encode_relation_spec(#{keys := Keys, columns := Cols}) ->
    EncodedKeys =
        case encode_relation_columns(Keys) of
            [] ->
                [];
            L ->
                L ++ [$\s, $=, $>]
        end,

    EncodedCols = encode_relation_columns(Cols),

    [${, EncodedKeys, EncodedCols, $\s, $}];

encode_relation_spec(_) ->
    error({badarg, "invalid specification"}).



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec encode_index_spec(Spec :: cozodb:index_spec()) -> iolist() | no_return().

encode_index_spec(#{type := covering, fields := Fields0}) when Fields0 =/= [] ->
    try

        Fields = encode_fields(Fields0),
        [${, $\s, Fields, $\s, $}]

    catch
        error:badarg ->
            Message =
                "invalid field value in 'fields'. "
                "Valid values are list, binary or atom",
            error({badarg, Message})
    end;

encode_index_spec(#{
        type := hnsw,
        dim := _,
        m := _,
        ef_construction := _,
        fields := _
    } = Spec)  ->
    do_encode_index_spec(Spec);

encode_index_spec(#{type := fts, extractor := _, tokenizer := _} = Spec)  ->
    do_encode_index_spec(Spec);

encode_index_spec(#{
        type := lsh,
        extractor := _,
        tokenizer := _,
        n_perm := _,
        n_gram := _,
        target_threshold := _
    } = Spec)  ->
    do_encode_index_spec(Spec);


encode_index_spec(_) ->
    error({badarg, "invalid specification"}).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec encode_triggers_spec([cozodb:trigger_spec()]) -> return.

encode_triggers_spec([]) ->
    %% Valid case when deleting triggers
    [];

encode_triggers_spec(Specs) ->
    L = [encode_trigger_spec(Spec) || Spec <- Specs],
    lists:join("\n", L).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
to_quoted_string(Term) ->
    to_quoted_string(Term, single).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
to_quoted_string(undefined, _) ->
    <<"Null">>;

to_quoted_string(nil, _) ->
    <<"Null">>;

to_quoted_string(null, _) ->
    <<"Null">>;

to_quoted_string(Val, _) when is_integer(Val) ->
    integer_to_binary(Val);

to_quoted_string(Val, _) when is_float(Val) ->
    list_to_binary(io_lib_format:fwrite_g(Val));

to_quoted_string({var, X}, _) when is_binary(X) ->
    X;

to_quoted_string(Val, single) when is_binary(Val) ->
    <<"'", Val/binary, "'">>;

to_quoted_string(Val, raw) when is_binary(Val) ->
    <<"___\"", Val/binary, "\"___">>;

to_quoted_string(Val, single) when is_atom(Val) ->
    <<"'", (atom_to_binary(Val, utf8))/binary, "'">>;

to_quoted_string(Val, raw) when is_atom(Val) ->
    <<"___\"", (atom_to_binary(Val, utf8))/binary, "\"___">>;

to_quoted_string(Values, Type) when is_list(Values) ->
    iolist_to_binary([
        <<"[">>,
        lists:join(
            <<", ">>,
            lists:map(fun (V) -> to_quoted_string(V, Type) end, Values)
        ),
        <<"]">>
    ]).


%% =============================================================================
%% PRIVATE
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
encode_relation_columns([]) ->
    [];

encode_relation_columns(Spec) when is_list(Spec) ->
    Res = lists:foldl(
        fun
            F({Col, Type}, Acc) when is_atom(Col) ->
                F({atom_to_binary(Col), Type}, Acc);

            F({Col, Type}, Acc) when is_list(Col) ->
                F({list_to_binary(Col), Type}, Acc);

            F({Col, undefined}, Acc) when is_binary(Col) ->
                [[$\s, Col] | Acc];

            F({Col, Type}, Acc)
            when is_binary(Col) andalso (is_atom(Type) orelse is_tuple(Type)) ->
                Encoded = encode_column_type(Type),
                [[$\s, Col, $:, $\s, Encoded] | Acc];

            F({Col, #{type := Type} = ColSpec}, Acc) when is_binary(Col) ->
                Encoded = encode_column_type(Type),
                DefaultNullable =
                    case maps:get(default, ColSpec, undefined) of
                        undefined ->
                            case maps:get(nullable, ColSpec, false) of
                                true ->
                                    $?;
                                false ->
                                    <<>>
                            end;
                        Val when Type == string; Type == uuid ->
                            to_quoted_string(Val);
                        Val ->
                            [<<" default ">>, value_to_binary(Val)]
                    end,

                [[$\s, Col, $:, $\s, Encoded, DefaultNullable] | Acc];

            F(Col, Acc) when is_atom(Col); is_list(Col); is_binary(Col) ->
                %% A column name without type, we coerse to tuple
                F({Col, undefined}, Acc);

            F(Term, _) ->
                M = io:format("invalid column type specification '~p'", [Term]),
                error({badarg, M})
        end,
        [],
        Spec
    ),
    lists:reverse(lists:join($,, Res));

encode_relation_columns(_Spec) ->
    error({badarg, "invalid specification"}).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
value_to_binary(Val) when Val == null; Val == nil; Val == undefined ->
    <<"Null">>;

value_to_binary(Val) when is_atom(Val) ->
    atom_to_binary(Val);

value_to_binary(Val) when is_list(Val) ->
    list_to_binary(Val);

value_to_binary(Val) when is_float(Val) ->
    float_to_binary(Val, [{decimals, 4}, compact]);

value_to_binary(Val) when is_integer(Val) ->
    integer_to_binary(Val);

value_to_binary(Val) when is_binary(Val) ->
    Val.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
encode_column_type(undefined) ->
    <<>>;

encode_column_type(any) ->
    <<"Any">>;

encode_column_type(bool) ->
    <<"Bool">>;

encode_column_type(bytes) ->
    <<"Bytes">>;

encode_column_type(json) ->
    <<"Json">>;

encode_column_type(int) ->
    <<"Int">>;

encode_column_type(float) ->
    <<"Float">>;

encode_column_type(string) ->
    <<"String">>;

encode_column_type(uuid) ->
    <<"Uuid">>;

encode_column_type(validity) ->
    <<"Validity">>;

encode_column_type({list, Type}) ->
    [$[, encode_column_type(Type), $]];

encode_column_type({list, Type, Size}) when is_integer(Size) ->
    [$[, encode_column_type(Type), $;, $\s, integer_to_binary(Size), $]];

encode_column_type({tuple, Types}) when is_list(Types) ->
    Encoded = [encode_column_type(Type) || Type <- Types],
    [$(, lists:join($,, Encoded), $)];

encode_column_type({vector, N, Size})
when (N == 32 orelse N == 64) andalso is_integer(Size) ->
    [$<, $F, integer_to_list(N), $;, $\s, integer_to_list(Size), $>];

encode_column_type(Term) ->
    M = io:format("invalid column type '~p'", [Term]),
    error({badarg, M}).


%% @private
encode_fields(Fields) ->
    lists:join(", ", lists:map(fun term_to_field/1, Fields)).


%% @private
term_to_field(Term) when is_atom(Term) ->
    atom_to_list(Term);

term_to_field(Term) when is_list(Term); is_binary(Term) ->
    Term;

term_to_field(_) ->
    error(badarg).



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
do_encode_index_spec(#{type := T} = Spec) ->
    FolderFun = fun
        (type, _, Acc) ->
            %% Ignore
            Acc;
        (K, Term, Acc) ->
            [encode_index_entry(T, K, Term) | Acc]
    end,

    %% Build iolist. We use sorted iterator so that we can have deterministic
    %% results required for testing
    Iter = maps:iterator(Spec, ordered),
    Entries = lists:join(", ", lists:reverse(maps:fold(FolderFun, [], Iter))),
    [${, $\s, Entries, $\s, $}].

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
encode_index_entry(hnsw, K, N)
when (K == dim orelse K == m orelse K == ef_construction)
andalso is_integer(N) andalso N > 0 ->
    <<(atom_to_binary(K))/binary, ": ", (integer_to_binary(N))/binary>>;

encode_index_entry(hnsw, K, _) when K == dim; K == m; K == ef_construction ->
    Cause = lists:flatten(
        io_lib:format("value for key '~p' is not a positive integer", [K])
    ),
    error({badarg, Cause});

encode_index_entry(hnsw, dtype, f32) ->
    <<"dtype: F32">>;

encode_index_entry(hnsw, dtype, f64) ->
    <<"dtype: F64">>;

encode_index_entry(hnsw, dtype, _) ->
    error({badarg, "invalid value for key 'dtype'"});

encode_index_entry(hnsw, fields, Fields) when is_list(Fields) ->
    Encoded = encode_fields(Fields),
    ["fields: [", Encoded, "]"];

encode_index_entry(hnsw, fields, _) ->
    error({badarg, "invalid value for key 'fields'"});

encode_index_entry(hnsw, distance, l2) ->
    <<"distance: L2">>;

encode_index_entry(hnsw, distance, cosine) ->
    <<"distance: Cosine">>;

encode_index_entry(hnsw, distance, ip) ->
    <<"distance: IP">>;

encode_index_entry(hnsw, distance, _) ->
    error({badarg, "invalid value for key 'distance'"});

encode_index_entry(hnsw, filter, Expr) when is_list(Expr) ->
    encode_index_entry(hnsw, filter, list_to_binary(Expr));

encode_index_entry(hnsw, filter, Expr) when is_binary(Expr) ->
    <<"filter: ", Expr/binary>>;

encode_index_entry(hnsw, filter, _) ->
    error({badarg, "invalid value for key 'filter'"});

encode_index_entry(hnsw, extend_candidates, Val) when is_boolean(Val) ->
    <<"extend_candidates: ", (atom_to_binary(Val))/binary>>;

encode_index_entry(hnsw, extend_candidates, _) ->
    error({badarg, "invalid value for key 'extend_candidates'"});

encode_index_entry(hnsw, keep_pruned_connections, Val) when is_boolean(Val) ->
    <<"keep_pruned_connections: ", (atom_to_binary(Val))/binary>>;

encode_index_entry(hnsw, keep_pruned_connections, _) ->
    error({badarg, "invalid value for key 'keep_pruned_connections'"});

encode_index_entry(Type, extractor, Col) when is_list(Col) ->
    encode_index_entry(Type, extractor, list_to_binary(Col));

encode_index_entry(Type, extractor, Col) when is_atom(Col) ->
    encode_index_entry(Type, extractor, atom_to_binary(Col));

encode_index_entry(Type, extractor, Col)
when (Type == lsh orelse Type == fts) andalso is_binary(Col) ->
    <<"extractor: ", Col/binary>>;

encode_index_entry(Type, extractor, _) when Type == lsh; Type == fts ->
    error({badarg, "invalid value for key 'extractor'"});

encode_index_entry(Type, extract_filter, Expr) when is_list(Expr) ->
    encode_index_entry(Type, extract_filter, list_to_binary(Expr));

encode_index_entry(Type, extract_filter, Expr)
when (Type == lsh orelse Type == fts) andalso is_binary(Expr) ->
    <<"extract_filter: ", Expr/binary>>;

encode_index_entry(Type, extract_filter, _) when Type == lsh; Type == fts ->
    error({badarg, "invalid value for key 'extract_filter'"});

encode_index_entry(Type, tokenizer, Tokenizer) when Type == lsh; Type == fts ->
    Encoded = encode_index_tokenizer(Tokenizer),
    <<"tokenizer: ", Encoded/binary>>;

encode_index_entry(Type, tokenizer, _) when Type == lsh; Type == fts ->
    error({badarg, "invalid value for key 'tokenizer'"});


encode_index_entry(Type, filters, Filters)
when (Type == lsh orelse Type == fts) andalso is_list(Filters) ->
    case encode_index_filters(Filters) of
        [] ->
            [];
        Encoded ->
            ["filters: [", Encoded, "]"]
    end;

encode_index_entry(Type, filters, _) when Type == lsh; Type == fts ->
    error({badarg, "invalid value for key 'filters'"});

encode_index_entry(lsh, K, N)
when (K == n_perm orelse K == n_gram) andalso is_integer(N) andalso N > 0 ->
    <<(atom_to_binary(K))/binary, ": ", (integer_to_binary(N))/binary>>;

encode_index_entry(lsh, K, _) when K == n_perm; K == n_gram ->
    Cause = lists:flatten(
        io_lib:format("value for key '~p' is not a positive integer", [K])
    ),
    error({badarg, Cause});

encode_index_entry(lsh, K, N)
when (
    K == target_threshold orelse
    K == false_positive_weight orelse
    K ==  false_negative_weight
    ) andalso is_float(N) andalso N > 0.0 ->
    <<
        (atom_to_binary(K))/binary,
        ": ",
        (float_to_binary(N, [{decimals, 4}, compact]))/binary>>;

encode_index_entry(lsh, K, _)
when K == target_threshold orelse
K == false_positive_weight orelse
K ==  false_negative_weight ->
    Cause = lists:flatten(
        io_lib:format("value for key '~p' is not a positive float", [K])
    ),
    error({badarg, Cause});

encode_index_entry(_, K, _) ->
    Cause = io_lib:format("unknown key '~s'", [K]),
    error({badarg, Cause}).



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------

encode_index_tokenizer(raw) ->
    <<"Raw">>;

encode_index_tokenizer(simple) ->
    <<"Simple">>;

encode_index_tokenizer(whitespace) ->
    <<"Whitespace">>;

encode_index_tokenizer(ngram) ->
    encode_index_tokenizer({ngram, 1, 1, false});

encode_index_tokenizer({ngram, Min, Max, PrefixOnly})
when is_integer(Min), is_integer(Max), is_boolean(PrefixOnly) ->
    <<
        "NGram(",
        (integer_to_binary(Min))/binary,
        $,,$\s,
        (integer_to_binary(Max))/binary,
        $,,$\s,
        (atom_to_binary(PrefixOnly))/binary,
        ")"
    >>;

encode_index_tokenizer({cangjie, Kind})
when Kind == all; Kind == default; Kind == search; Kind == unicode ->
    <<"Cangjie(", (atom_to_binary(Kind))/binary, ")">>;

encode_index_tokenizer(_) ->
    error({badarg, "invalid tokenizer"}).



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
encode_index_filters([]) ->
    [];

encode_index_filters(Filters) ->
    lists:join(", ", [encode_index_filter(F) || F <- Filters]).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
encode_index_filter(lowercase) ->
    <<"Lowercase">>;

encode_index_filter(alphanumonly) ->
    <<"AlphaNumOnly">>;

encode_index_filter(asciifolding) ->
    <<"AsciiFolding">>;

encode_index_filter({stemmer, Lang}) when is_list(Lang) ->
    encode_index_filter({stemmer, list_to_binary(Lang)});

encode_index_filter({stemmer, Lang}) when is_binary(Lang) ->
    <<"Stemmer('", Lang/binary, "')">>;

encode_index_filter({stopwords, Lang}) when is_list(Lang) ->
    encode_index_filter({stopwords, list_to_binary(Lang)});

encode_index_filter({stopwords, Lang}) when is_binary(Lang) ->
    <<"Stopwords('", Lang/binary, "')">>;

encode_index_filter(_) ->
    error({badarg, "on or more values for filters are invalid"}).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
encode_trigger_spec({on_put, Script})
when is_list(Script); is_binary(Script) ->
    [<<"on put">>, $\s, ${, $\s, Script, $\s, $}];

encode_trigger_spec({on_rm, Script})
when is_list(Script); is_binary(Script) ->
    [<<"on rm">>, $\s, ${, $\s, Script, $\s, $}];

encode_trigger_spec({on_replace, Script})
when is_list(Script); is_binary(Script) ->
    [<<"on replace">>, $\s, ${, $\s, Script, $\s, $}];

encode_trigger_spec({on_remove, Script}) ->
    %% util
    encode_trigger_spec({on_rm, Script}).



%% =============================================================================
%% TESTS
%% =============================================================================




-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


to_quoted_string_test() ->
    ?assertEqual(<<"Null">>, to_quoted_string(undefined, single)),
    ?assertEqual(<<"Null">>, to_quoted_string(undefined, raw)),
    ?assertEqual(<<"Null">>, to_quoted_string(nil, raw)),
    ?assertEqual(<<"Null">>, to_quoted_string(nil, raw)),
    ?assertEqual(<<"Null">>, to_quoted_string(null, raw)),
    ?assertEqual(<<"Null">>, to_quoted_string(null, raw)),
    ?assertEqual(<<"'a'">>, to_quoted_string(a, single)),
    ?assertEqual(<<"___\"a\"___">>, to_quoted_string(a, raw)),

    ?assertEqual(
        <<"0.046598684042692184">>,
        to_quoted_string(0.046598684042692184)
    ),
    ?assertEqual(<<"1000">>, to_quoted_string(1_000)),

    ?assertEqual(<<"'foo'">>, to_quoted_string(<<"foo">>, single)),
    ?assertEqual(<<"___\"foo\"___">>, to_quoted_string(<<"foo">>, raw)),

    ?assertEqual(<<"'foo'">>, to_quoted_string(<<"foo">>, single)),
    ?assertEqual(<<"___\"foo\"___">>, to_quoted_string(<<"foo">>, raw)),

    ?assertEqual(
        <<"___\"'\"foo\"'\"___">>,
        to_quoted_string(<<"'\"foo\"'">>, raw)
    ),


    ?assertEqual(
        <<"[Null, Null, Null, 'a', 'foo', 0.046598684042692184, 1000]">>,
        to_quoted_string(
            [
                undefined, nil, null, a, <<"foo">>, 0.046598684042692184, 1_000
            ],
            single
        )
    ),

    ?assertEqual(
        <<"[Null, Null, Null, ___\"a\"___, ___\"foo\"___, 0.046598684042692184, 1000]">>,
        to_quoted_string(
            [
                undefined, nil, null, a, <<"foo">>, 0.046598684042692184, 1_000
            ],
            raw
        )
    ),
    ok.

encode_relation_columns_test() ->
    ?assertError(
        {badarg, "invalid specification"},
        encode_relation_spec(#{})
    ),
    ?assertError(
        {badarg, "invalid specification"},
        encode_relation_spec(#{keys => []})
    ),
    ?assertError(
        {badarg, "invalid specification"},
        encode_relation_spec(#{columns => []})
    ),
    ?assertError(
        {badarg, "invalid specification"},
        encode_relation_spec(#{keys => [], columns => []})
    ),
    ?assertEqual(
        <<"{ a => }">>,
        iolist_to_binary(
            encode_relation_spec(#{
                keys => [{a, undefined}]
            })
        ),
        "We should support atom column names"
    ),
    ?assertEqual(
        <<"{ a => }">>,
        iolist_to_binary(
            encode_relation_spec(#{
                keys => [{"a", undefined}]
            })
        ),
        "We should support list column names"
    ),
    ?assertEqual(
        <<"{ a => }">>,
        iolist_to_binary(
            encode_relation_spec(#{
                keys => [{<<"a">>, undefined}]
            })
        ),
        "We should support binary column names"
    ),
    ?assertEqual(
        <<"{ a: String => }">>,
        iolist_to_binary(
            encode_relation_spec(#{
                keys => [{a, #{type => string}}]
            })
        )
    ),
    ?assertEqual(
        <<"{ a: String => }">>,
        iolist_to_binary(
            encode_relation_spec(#{
                keys => [{a, string}]
            })
        )
    ),
    ?assertEqual(
        <<"{ a: String? => }">>,
        iolist_to_binary(
            encode_relation_spec(#{
                keys => [{a, #{type => string, nullable => true}}],
                columns => []
            })
        )
    ),
    ?assertEqual(
        <<"{ k: String => v: <F32; 128> }">>,
        iolist_to_binary(
            encode_relation_spec(#{
                keys => [{k, string}],
                columns => [{v, {vector, 32, 128}}]
            })
        )
    ),

    ?assertEqual(
        <<
            "{ "
            "any: Any, "
            "any_spec: Any, "
            "any_spec_nullable: Any?, "
            "bool: Bool, "
            "bool_spec: Bool, "
            "bool_spec_nullable: Bool?, "
            "bytes: Bytes, "
            "bytes_spec: Bytes, "
            "bool_spec_nullable: Bool?, "
            "json: Json, "
            "json_spec: Json, "
            "json_spec_nullable: Json?, "
            "int: Int, "
            "int_spec: Int, "
            "int_spec_nullable: Int?, "
            "float: Float, "
            "float_spec: Float, "
            "float_spec_nullable: Float?, "
            "string: String, "
            "string_spec: String, "
            "string_spec_nullable: String?, "
            "uuid: Uuid, "
            "uuid_spec: Uuid, "
            "uuid_spec_nullable: Uuid?, "
            "validity: Validity, "
            "validity_spec: Validity, "
            "validity_spec_nullable: Validity? "
            "}"
        >>,
        iolist_to_binary(
            encode_relation_spec(#{
                columns => [
                    {"any", any},
                    {"any_spec", #{type => any}},
                    {"any_spec_nullable", #{type => any, nullable => true}},
                    {"bool", bool},
                    {"bool_spec", #{type => bool}},
                    {"bool_spec_nullable", #{type => bool, nullable => true}},
                    {"bytes", bytes},
                    {"bytes_spec", #{type => bytes}},
                    {"bool_spec_nullable", #{type => bool, nullable => true}},
                    {"json", json},
                    {"json_spec", #{type => json}},
                    {"json_spec_nullable", #{type => json, nullable => true}},
                    {"int", int},
                    {"int_spec", #{type => int}},
                    {"int_spec_nullable", #{type => int, nullable => true}},
                    {"float", float},
                    {"float_spec", #{type => float}},
                    {"float_spec_nullable", #{type => float, nullable => true}},
                    {"string", string},
                    {"string_spec", #{type => string}},
                    {"string_spec_nullable", #{
                        type => string, nullable => true}
                    },
                    {"uuid", uuid},
                    {"uuid_spec", #{type => uuid}},
                    {"uuid_spec_nullable", #{type => uuid, nullable => true}},
                    {"validity", validity},
                    {"validity_spec", #{type => validity}},
                    {"validity_spec_nullable", #{
                        type => validity, nullable => true}
                    }
                ]
            })
        )
    ).


encode_index_spec_test() ->
    ?assertError(
        {badarg, "invalid specification"},
        encode_index_spec(#{})
    ),
    ?assertError(
        {badarg, "invalid specification"},
        encode_index_spec(#{type => foo})
    ),
    ?assertError(
        {badarg, "invalid specification"},
        encode_index_spec(#{type => covering})
    ),
    ?assertError(
        {badarg, "invalid specification"},
        encode_index_spec(#{type => covering, fields => []})
    ),
    ?assertError(
        {badarg, "invalid specification"},
        encode_index_spec(#{type => covering})
    ),
    ?assertEqual(
        <<"{ a }">>,
        iolist_to_binary(
            encode_index_spec(#{type => covering, fields => [a]})
        )
    ),
    ?assertEqual(
        <<"{ a, b, c }">>,
        iolist_to_binary(
            encode_index_spec(#{type => covering, fields => [a, "b", <<"c">>]})
        )
    ).


encode_hsnw_index_test() ->
    Spec = #{
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
    },

    %% Check required keys: [type, dim, m, fields, ef_construction]

    ?assertError(
        {badarg, "invalid specification"},
        iolist_to_binary(encode_index_spec(maps:without([type], Spec)))
    ),

    ?assertError(
        {badarg, "invalid specification"},
        iolist_to_binary(encode_index_spec(maps:without([dim], Spec)))
    ),

    ?assertError(
        {badarg, "invalid specification"},
        iolist_to_binary(encode_index_spec(maps:without([m], Spec)))
    ),

    ?assertError(
        {badarg, "invalid specification"},
        iolist_to_binary(encode_index_spec(maps:without([fields], Spec)))
    ),
    ?assertError(
        {badarg, "invalid specification"},
        iolist_to_binary(
            encode_index_spec(maps:without([ef_construction], Spec))
        )
    ),

    ?assertError(
        {badarg, "value for key 'dim' is not a positive integer"},
        iolist_to_binary(encode_index_spec(Spec#{dim => -1}))
    ),

    ?assertError(
        {badarg, "value for key 'dim' is not a positive integer"},
        iolist_to_binary(encode_index_spec(Spec#{dim => 0}))
    ),

    ?assertError(
        {badarg, "value for key 'dim' is not a positive integer"},
        iolist_to_binary(encode_index_spec(Spec#{dim => 1.0}))
    ),

    ?assertError(
        {badarg, "value for key 'm' is not a positive integer"},
        iolist_to_binary(encode_index_spec(Spec#{m => -1}))
    ),

    ?assertError(
        {badarg, "value for key 'm' is not a positive integer"},
        iolist_to_binary(encode_index_spec(Spec#{m => 0}))
    ),

    ?assertError(
        {badarg, "value for key 'm' is not a positive integer"},
        iolist_to_binary(encode_index_spec(Spec#{m => 1.0}))
    ),

    ?assertError(
        {badarg, "value for key 'ef_construction' is not a positive integer"},
        iolist_to_binary(encode_index_spec(Spec#{ef_construction => -1}))
    ),

    ?assertError(
        {badarg, "value for key 'ef_construction' is not a positive integer"},
        iolist_to_binary(encode_index_spec(Spec#{ef_construction => 0}))
    ),

    ?assertError(
        {badarg, "value for key 'ef_construction' is not a positive integer"},
        iolist_to_binary(encode_index_spec(Spec#{ef_construction => 1.0}))
    ),

    ?assertEqual(
        <<
            "{ "
            "dim: 128, "
            "distance: L2, "
            "dtype: F32, "
            "ef_construction: 20, "
            "extend_candidates: false, "
            "fields: [v], "
            "filter: k != 'foo', "
            "keep_pruned_connections: false, "
            "m: 50"
            " }"
        >>,
        iolist_to_binary(encode_index_spec(Spec))
    ).



encode_lsh_index_test() ->
    Spec = #{
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
    },

    %% Check required keys: [type, extractor, tokenizer, n_perm, n_gram,
    %% target_threshold]

    ?assertError(
        {badarg, "invalid specification"},
        iolist_to_binary(encode_index_spec(maps:without([type], Spec)))
    ),
    ?assertError(
        {badarg, "invalid specification"},
        iolist_to_binary(encode_index_spec(maps:without([extractor], Spec)))
    ),
    ?assertError(
        {badarg, "invalid specification"},
        iolist_to_binary(encode_index_spec(maps:without([tokenizer], Spec)))
    ),
    ?assertError(
        {badarg, "invalid specification"},
        iolist_to_binary(encode_index_spec(maps:without([n_perm], Spec)))
    ),
    ?assertError(
        {badarg, "invalid specification"},
        iolist_to_binary(encode_index_spec(maps:without([n_gram], Spec)))
    ),
    ?assertError(
        {badarg, "invalid specification"},
        iolist_to_binary(
            encode_index_spec(maps:without([target_threshold], Spec)
        ))
    ),

    ?assertError(
        {badarg, "value for key 'n_perm' is not a positive integer"},
        iolist_to_binary(encode_index_spec(Spec#{n_perm => -1}))
    ),

    ?assertError(
        {badarg, "value for key 'n_perm' is not a positive integer"},
        iolist_to_binary(encode_index_spec(Spec#{n_perm => 0}))
    ),

    ?assertError(
        {badarg, "value for key 'n_perm' is not a positive integer"},
        iolist_to_binary(encode_index_spec(Spec#{n_perm => 1.0}))
    ),

    ?assertError(
        {badarg, "value for key 'n_gram' is not a positive integer"},
        iolist_to_binary(encode_index_spec(Spec#{n_gram => -1}))
    ),

    ?assertError(
        {badarg, "value for key 'n_gram' is not a positive integer"},
        iolist_to_binary(encode_index_spec(Spec#{n_gram => 0}))
    ),

    ?assertError(
        {badarg, "value for key 'n_gram' is not a positive integer"},
        iolist_to_binary(encode_index_spec(Spec#{n_gram => 1.0}))
    ),

    ?assertEqual(
        <<
            "{ "
            "extract_filter: !is_null(v), "
            "extractor: v, "
            "false_negative_weight: 1.0, "
            "false_positive_weight: 1.0, "
            "filters: [AlphaNumOnly], "
            "n_gram: 3, "
            "n_perm: 200, "
            "target_threshold: 0.7, "
            "tokenizer: Simple"
            " }"
        >>,
        iolist_to_binary(encode_index_spec(Spec))
    ).


encode_fts_index_test() ->
    Spec = #{
        type => fts,
        extractor => v,
        extract_filter => "!is_null(v)",
        tokenizer => simple,
        filters => [alphanumonly]
    },

    %% Check required keys: [type, extractor, tokenizer, n_perm, n_gram,
    %% target_threshold]

    ?assertError(
        {badarg, "invalid specification"},
        iolist_to_binary(encode_index_spec(maps:without([type], Spec)))
    ),
    ?assertError(
        {badarg, "invalid specification"},
        iolist_to_binary(encode_index_spec(maps:without([extractor], Spec)))
    ),
    ?assertError(
        {badarg, "invalid specification"},
        iolist_to_binary(encode_index_spec(maps:without([tokenizer], Spec)))
    ),

    ?assertEqual(
        <<
            "{ "
            "extract_filter: !is_null(v), "
            "extractor: v, "
            "filters: [AlphaNumOnly], "
            "tokenizer: Simple"
            " }"
        >>,
        iolist_to_binary(encode_index_spec(Spec))
    ).


encode_triggers_test() ->

    ?assertEqual([], encode_triggers_spec([])),

    ?assertEqual(
        <<"on put { query goes here }">>,
        iolist_to_binary(
            encode_triggers_spec([{on_put, "query goes here"}])
        )
    ),

    ?assertEqual(
        <<"on rm { query goes here }">>,
        iolist_to_binary(
            encode_triggers_spec([{on_rm, "query goes here"}])
        )
    ),

    ?assertEqual(
        <<"on replace { query goes here }">>,
        iolist_to_binary(
            encode_triggers_spec([{on_replace, "query goes here"}])
        )
    ),

    ?assertEqual(
        <<"on rm { query goes here }">>,
        iolist_to_binary(
            encode_triggers_spec([{on_remove, "query goes here"}])
        )
    ),

    ?assertEqual(
        <<
            "on put { query goes here }\n"
            "on put { query goes here }\n"
            "on rm { query goes here }\n"
            "on replace { query goes here }"
        >>,
        iolist_to_binary(
            encode_triggers_spec([
                {on_put, "query goes here"},
                {on_put, "query goes here"},
                {on_remove, "query goes here"},
                {on_replace, "query goes here"}
            ])
        )
    ).

-endif.
