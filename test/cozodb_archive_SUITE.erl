%% =============================================================================
%%  cozodb_archive_SUITE.erl -
%%
%%  Copyright (c) 2025 Leapsight. All rights reserved.
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
%% @doc Integration tests for the relation archiving API
%% (`cozodb:archive_config_put/3,4`, `replicate_pending/2`, `archive/3`,
%% `import_parquet/3`, ...).
%%
%% These tests replicate to a REAL S3-compatible bucket and read back from it,
%% so the whole suite is skipped unless the following are set in the
%% environment (a `.env` at the project root is loaded automatically):
%%
%%   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, COZO_TEST_S3_BUCKET
%%   AWS_ENDPOINT_URL_S3   (optional; for Tigris / R2 / MinIO / ...)
%%
%% The configured credentials must NOT grant `s3:DeleteObject` — cozo's startup
%% IAM probe refuses to replicate otherwise (set COZO_ARCHIVE_SKIP_IAM_PROBE=1
%% only for providers with coarse key scopes).
%% @end
%% -----------------------------------------------------------------------------
-module(cozodb_archive_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(REQUIRED_ENV, [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_REGION",
    "COZO_TEST_S3_BUCKET"
]).

%% =============================================================================
%% CT CALLBACKS
%% =============================================================================

suite() ->
    [{timetrap, {seconds, 120}}].

all() ->
    [
        archive_config_crud,
        replicate_and_restore_round_trip,
        replicate_and_restore_json_round_trip,
        replicate_idempotent_noop,
        archive_deletes_replicated_rows
    ].

init_per_suite(Config) ->
    ok = maybe_load_dotenv(),
    case missing_env(?REQUIRED_ENV) of
        [] ->
            {ok, _} = application:ensure_all_started(cozodb),
            [{bucket, list_to_binary(os:getenv("COZO_TEST_S3_BUCKET"))} | Config];
        Missing ->
            {skip,
                lists:flatten(
                    io_lib:format(
                        "S3 integration env not set (missing ~p). Export AWS_* and "
                        "COZO_TEST_S3_BUCKET (or place a .env at the project root) "
                        "to run this suite.",
                        [Missing]
                    )
                )}
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    {ok, Db} = cozodb:open(mem),
    Bucket = ?config(bucket, Config),
    Unique = integer_to_list(erlang:unique_integer([positive])),
    Uri = iolist_to_binary([
        <<"s3://">>, Bucket, <<"/ct-">>, atom_to_list(TestCase), <<"-">>, Unique, <<"/">>
    ]),
    [{db, Db}, {uri, Uri} | Config].

end_per_testcase(_TestCase, Config) ->
    _ = catch cozodb:close(?config(db, Config)),
    ok.

%% =============================================================================
%% TEST CASES
%% =============================================================================

-doc "put / get / remove of an archive configuration (no S3 traffic).".
archive_config_crud(Config) ->
    Db = ?config(db, Config),
    Uri = ?config(uri, Config),
    {ok, _} = cozodb:run(
        Db, <<":create orders {id: Int => ts: Int default commit_now()}">>
    ),
    ok = cozodb:archive_config_put(Db, orders, ts, #{uri => Uri}),

    {ok, #{rows := Rows1}} = cozodb:archive_config_get(Db, orders),
    ?assertMatch([[<<"orders">>, <<"ts">> | _]], Rows1),

    ok = cozodb:archive_config_remove(Db, orders),
    {ok, #{rows := Rows2}} = cozodb:archive_config_get(Db, orders),
    ?assertEqual([], Rows2).

-doc "write -> replicate to S3 -> restore from s3:// -> rows match.".
replicate_and_restore_round_trip(Config) ->
    Db = ?config(db, Config),
    Uri = ?config(uri, Config),
    {ok, _} = cozodb:run(
        Db,
        <<":create orders {id: Int => name: String, ts: Int default commit_now()}">>
    ),
    ok = cozodb:archive_config_put(Db, orders, ts, #{uri => Uri}),
    {ok, _} = cozodb:run(
        Db,
        <<"?[id, name] <- [[1, 'a'], [2, 'b'], [3, 'c']] :put orders {id => name}">>
    ),

    %% Replicate: [status, rows_replicated, segments_written, old_wm, new_wm].
    {ok, #{rows := [[<<"OK">>, RowsRepl, Segs, _Old, NewWm]]}} =
        cozodb:replicate_pending(Db, orders),
    ?assertEqual(3, RowsRepl),
    ?assert(Segs >= 1),
    %% Monotonic commit clock: the new watermark is a real ts, not i64::MIN.
    ?assert(NewWm > 0),

    %% The segment's s3:// URI lives in the manifest.
    {ok, #{rows := [[S3Path]]}} = cozodb:run(
        Db, <<"?[f] := *cozo_archive_segments{file_path: f}">>
    ),
    ?assertMatch(<<"s3://", _/binary>>, S3Path),

    %% Restore directly from s3:// into a fresh relation.
    {ok, _} = cozodb:run(
        Db, <<":create restored {id: Int => name: String, ts: Int}">>
    ),
    {ok, #{rows := [[<<"OK">>, Imported]]}} =
        cozodb:import_parquet(Db, restored, S3Path),
    ?assertEqual(3, Imported),

    {ok, #{rows := Restored}} = cozodb:run(
        Db, <<"?[id, name] := *restored{id, name}">>
    ),
    ?assertEqual(
        [[1, <<"a">>], [2, <<"b">>], [3, <<"c">>]],
        lists:sort(Restored)
    ).

-doc "write a Json column -> replicate to S3 -> restore -> JSON value round-trips.".
replicate_and_restore_json_round_trip(Config) ->
    Db = ?config(db, Config),
    Uri = ?config(uri, Config),
    {ok, _} = cozodb:run(
        Db,
        <<":create orders {id: Int => doc: Json, ts: Int default commit_now()}">>
    ),
    ok = cozodb:archive_config_put(Db, orders, ts, #{uri => Uri}),

    %% A nested JSON document. Erlang map keys are unordered and cozo emits
    %% object keys in a normalised (sorted) order, so we compare by decoding the
    %% JSON, not by comparing raw bytes.
    Doc = #{
        <<"a">> => 1,
        <<"b">> => [true, <<"x">>],
        <<"nested">> => #{<<"c">> => 2}
    },
    DocBin = iolist_to_binary(json:encode(Doc)),
    %% A Json value is passed in as the `{json, Binary}` tuple and stored in the
    %% Json column (NOT as a plain string, which would be re-wrapped as a JSON
    %% string node).
    {ok, _} = cozodb:run(
        Db,
        <<"?[id, doc] := id = 1, doc = $doc\n:put orders {id => doc}">>,
        #{parameters => #{<<"doc">> => {json, DocBin}}}
    ),

    %% Replicate: the Json column is encoded to Parquet as portable UTF-8 text.
    {ok, #{rows := [[<<"OK">>, RowsRepl, Segs, _Old, NewWm]]}} =
        cozodb:replicate_pending(Db, orders),
    ?assertEqual(1, RowsRepl),
    ?assert(Segs >= 1),
    ?assert(NewWm > 0),

    {ok, #{rows := [[S3Path]]}} = cozodb:run(
        Db, <<"?[f] := *cozo_archive_segments{file_path: f}">>
    ),
    ?assertMatch(<<"s3://", _/binary>>, S3Path),

    %% Restore into a fresh relation declaring the same Json column.
    {ok, _} = cozodb:run(
        Db, <<":create restored {id: Int => doc: Json, ts: Int}">>
    ),
    {ok, #{rows := [[<<"OK">>, Imported]]}} =
        cozodb:import_parquet(Db, restored, S3Path),
    ?assertEqual(1, Imported),

    %% The Json value comes back as `{json, Binary}`; a plain binary here would
    %% mean the double-encode regression. Decode and compare to the original.
    {ok, #{rows := [[Id, RestoredDoc]]}} = cozodb:run(
        Db, <<"?[id, doc] := *restored{id, doc}">>
    ),
    ?assertEqual(1, Id),
    ?assertMatch({json, _}, RestoredDoc),
    {json, RestoredBin} = RestoredDoc,
    ?assertEqual(Doc, json:decode(RestoredBin)).

-doc "a second drain with no new rows is a no-op (no extra segment).".
replicate_idempotent_noop(Config) ->
    Db = ?config(db, Config),
    Uri = ?config(uri, Config),
    {ok, _} = cozodb:run(
        Db, <<":create orders {id: Int => ts: Int default commit_now()}">>
    ),
    ok = cozodb:archive_config_put(Db, orders, ts, #{uri => Uri}),
    {ok, _} = cozodb:run(Db, <<"?[id] <- [[1]] :put orders {id}">>),

    {ok, #{rows := [[<<"OK">>, 1, _, _, _]]}} = cozodb:replicate_pending(Db, orders),
    {ok, #{rows := [[<<"OK">>, 0, 0, _, _]]}} = cozodb:replicate_pending(Db, orders),

    {ok, #{rows := SegRows}} = cozodb:run(
        Db, <<"?[s] := *cozo_archive_segments{segment_id: s}">>
    ),
    ?assertEqual(1, length(SegRows)).

-doc "archive/3 deletes the replicated rows from the live relation.".
archive_deletes_replicated_rows(Config) ->
    Db = ?config(db, Config),
    Uri = ?config(uri, Config),
    {ok, _} = cozodb:run(
        Db,
        <<":create orders {id: Int => name: String, ts: Int default commit_now()}">>
    ),
    ok = cozodb:archive_config_put(Db, orders, ts, #{uri => Uri}),
    {ok, _} = cozodb:run(
        Db, <<"?[id, name] <- [[1, 'a'], [2, 'b']] :put orders {id => name}">>
    ),
    {ok, _} = cozodb:replicate_pending(Db, orders),

    %% [status, archived, skipped, missing, watermark].
    {ok, #{rows := [[<<"OK">>, Archived, Skipped, _Missing, _Wm]]}} =
        cozodb:archive(Db, orders, <<"?[id] := *orders{id}">>),
    ?assertEqual(2, Archived),
    ?assertEqual(0, Skipped),

    %% Rows are gone from the live relation (now safely on S3).
    {ok, #{rows := Remaining}} = cozodb:run(Db, <<"?[id] := *orders{id}">>),
    ?assertEqual([], Remaining).

%% =============================================================================
%% PRIVATE
%% =============================================================================

missing_env(Vars) ->
    [V || V <- Vars, os:getenv(V) =:= false].

%% Best-effort: load a `.env` from the project root (or a couple of parents)
%% so the suite works the same way the Rust `integration-s3` tests do. Already
%% -exported variables are never overwritten.
maybe_load_dotenv() ->
    lists:foreach(
        fun(Path) ->
            case file:read_file(Path) of
                {ok, Bin} -> load_dotenv_bin(Bin);
                _ -> ok
            end
        end,
        [".env", "../.env", "../../.env", "../../../.env"]
    ).

load_dotenv_bin(Bin) ->
    Lines = binary:split(Bin, [<<"\n">>], [global]),
    lists:foreach(fun load_dotenv_line/1, Lines).

load_dotenv_line(Line0) ->
    case string:trim(Line0) of
        <<>> ->
            ok;
        <<"#", _/binary>> ->
            ok;
        Line ->
            case binary:split(Line, <<"=">>) of
                [K0, V0] ->
                    K = binary_to_list(string:trim(K0)),
                    V = binary_to_list(string:trim(V0)),
                    case os:getenv(K) of
                        false -> os:putenv(K, V);
                        _ -> ok
                    end;
                _ ->
                    ok
            end
    end.
