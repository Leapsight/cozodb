%% =============================================================================
%%  cozodb_migrator_SUITE.erl -
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

-module(cozodb_migrator_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile(export_all).

-define(ENGINES, [sqlite]).

%% =============================================================================
%% COMMON TEST CALLBACKS
%% =============================================================================

suite() ->
    [{timetrap, {seconds, 120}}].

all() ->
    [{group, Engine} || Engine <- ?ENGINES].

groups() ->
    Tests = [
        migrate_all_test,
        migrate_idempotent_test,
        migrate_dry_run_test,
        migrate_to_version_test,
        status_test,
        rollback_test,
        rollback_no_down_test,
        checksum_validation_test,
        baseline_test,
        failed_migration_test,
        backup_migration_test,
        concurrent_serialization_test,
        recreate_relation_test,
        recreate_relation_with_transform_test,
        recreate_relation_multi_key_test,
        recreate_relation_empty_test,
        recreate_relation_recovery_test,
        reindex_test,
        reindex_preserves_other_indices_test,
        copy_relation_no_transform_test,
        relation_exists_test,
        ensure_relation_test,
        rename_relation_test
    ],
    [{Engine, [sequence], Tests} || Engine <- ?ENGINES].

init_per_suite(Config) ->
    TMPDir = os:getenv("COZODB_TMP_DIR", "/tmp/cozodb"),
    _ = catch file:del_dir_r(TMPDir),
    _ = catch file:make_dir(TMPDir),
    %% Ensure application is started (for the gen_server)
    {ok, _} = application:ensure_all_started(cozodb),
    %% Compile and load test migration modules
    %% The migrations directory lives next to this suite file
    SuiteBeam = code:which(?MODULE),
    MigrationDir = filename:join(filename:dirname(SuiteBeam), "migrations"),
    ok = compile_migrations(MigrationDir),
    [{tmp_dir, TMPDir} | Config].

end_per_suite(Config) ->
    TMPDir = proplists:get_value(tmp_dir, Config, "/tmp/cozodb"),
    _ = catch file:del_dir_r(TMPDir),
    ok.

init_per_group(Engine, Config) ->
    [{db_engine, Engine} | Config].

end_per_group(_Engine, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Engine = proplists:get_value(db_engine, Config),
    TMPDir = proplists:get_value(tmp_dir, Config),
    DbPath = filename:join(TMPDir, atom_to_list(Engine) ++ "_migrator_test"),
    _ = catch file:del_dir_r(DbPath),
    {ok, Db} = cozodb:open(Engine, DbPath),
    [{db, Db}, {db_path, DbPath} | Config].

end_per_testcase(_TestCase, Config) ->
    Db = proplists:get_value(db, Config),
    cozodb:close(Db),
    DbPath = proplists:get_value(db_path, Config),
    _ = catch file:del_dir_r(DbPath),
    ok.

%% =============================================================================
%% TEST CASES
%% =============================================================================

migrate_all_test(Config) ->
    Db = proplists:get_value(db, Config),
    Migrations = base_migrations(),

    %% Apply all migrations
    ok = cozodb_migrator:migrate(Db, Migrations),

    %% Verify relations were created
    ?assertMatch({ok, _}, cozodb:columns(Db, <<"users">>)),
    ?assertMatch({ok, _}, cozodb:columns(Db, <<"posts">>)),

    %% Verify migration history
    {ok, Status} = cozodb_migrator:status(Db, Migrations),
    ?assertEqual(3, length(Status)),
    lists:foreach(
        fun(#{status := S}) ->
            ?assertEqual(<<"applied">>, S)
        end,
        Status
    ).

migrate_idempotent_test(Config) ->
    Db = proplists:get_value(db, Config),
    Migrations = base_migrations(),

    %% Apply twice - second should be a no-op
    ok = cozodb_migrator:migrate(Db, Migrations),
    ok = cozodb_migrator:migrate(Db, Migrations),

    %% Still only 3 applied
    {ok, Status} = cozodb_migrator:status(Db, Migrations),
    Applied = [S || #{status := St} = S <- Status, St =:= <<"applied">>],
    ?assertEqual(3, length(Applied)).

migrate_dry_run_test(Config) ->
    Db = proplists:get_value(db, Config),
    Migrations = base_migrations(),

    %% Dry run should return pending list
    {ok, Pending} = cozodb_migrator:migrate(Db, Migrations, #{dry_run => true}),
    ?assertEqual(3, length(Pending)),

    %% Verify nothing was actually applied
    {ok, Status} = cozodb_migrator:status(Db, Migrations),
    lists:foreach(
        fun(#{status := S}) ->
            ?assertEqual(<<"pending">>, S)
        end,
        Status
    ),

    %% Verify relations were NOT created
    ?assertNot(cozodb_migrator_utils:relation_exists(Db, <<"users">>)).

migrate_to_version_test(Config) ->
    Db = proplists:get_value(db, Config),
    Migrations = base_migrations(),

    %% Apply only up to the second migration
    ok = cozodb_migrator:migrate(Db, Migrations, #{to => 20260102000000}),

    %% Only first two should be applied
    {ok, Status} = cozodb_migrator:status(Db, Migrations),
    ?assertMatch(
        [#{status := <<"applied">>}, #{status := <<"applied">>}, #{status := <<"pending">>}],
        Status
    ),

    %% users and posts exist, but no index
    ?assert(cozodb_migrator_utils:relation_exists(Db, <<"users">>)),
    ?assert(cozodb_migrator_utils:relation_exists(Db, <<"posts">>)).

status_test(Config) ->
    Db = proplists:get_value(db, Config),
    Migrations = base_migrations(),

    %% Initially all pending
    {ok, Status0} = cozodb_migrator:status(Db, Migrations),
    ?assertEqual(3, length(Status0)),
    lists:foreach(
        fun(#{status := S}) ->
            ?assertEqual(<<"pending">>, S)
        end,
        Status0
    ),

    %% After migration, all applied
    ok = cozodb_migrator:migrate(Db, Migrations),
    {ok, Status1} = cozodb_migrator:status(Db, Migrations),
    lists:foreach(
        fun(#{status := S}) ->
            ?assertEqual(<<"applied">>, S)
        end,
        Status1
    ).

rollback_test(Config) ->
    Db = proplists:get_value(db, Config),
    Migrations = base_migrations(),

    %% Apply all
    ok = cozodb_migrator:migrate(Db, Migrations),

    %% Rollback last 1
    ok = cozodb_migrator:rollback(Db, Migrations, 1),

    %% Check status - last one should be rolled_back
    {ok, Status} = cozodb_migrator:status(Db, Migrations),
    ?assertMatch(
        [#{status := <<"applied">>}, #{status := <<"applied">>}, #{status := <<"rolled_back">>}],
        Status
    ),

    %% Re-apply should apply only the rolled back one
    ok = cozodb_migrator:migrate(Db, Migrations),
    {ok, Status2} = cozodb_migrator:status(Db, Migrations),
    lists:foreach(
        fun(#{status := S}) ->
            ?assertEqual(<<"applied">>, S)
        end,
        Status2
    ).

rollback_no_down_test(Config) ->
    Db = proplists:get_value(db, Config),
    %% Use the backup migration which has no down/1
    Migrations = [cozodb_migration_20260105000000_with_backup],

    ok = cozodb_migrator:migrate(Db, Migrations),

    %% Rollback should fail because there's no down/1
    ?assertMatch(
        {error, no_down_callback},
        cozodb_migrator:rollback(Db, Migrations, 1)
    ).

checksum_validation_test(Config) ->
    Db = proplists:get_value(db, Config),
    Migrations = [cozodb_migration_20260101000000_create_users],

    %% Apply
    ok = cozodb_migrator:migrate(Db, Migrations),

    %% Tamper with stored checksum
    cozodb:import(Db, #{
        cozodb_migrations => #{
            headers => [
                <<"version">>,
                <<"name">>,
                <<"checksum">>,
                <<"applied_at">>,
                <<"execution_time_ms">>,
                <<"status">>
            ],
            rows => [
                [
                    20260101000000,
                    <<"create_users">>,
                    <<"tampered_checksum">>,
                    0.0,
                    0,
                    <<"applied">>
                ]
            ]
        }
    }),

    %% Next migrate should detect checksum mismatch
    ?assertMatch(
        {error, {checksum_mismatch, _}},
        cozodb_migrator:migrate(Db, Migrations)
    ).

baseline_test(Config) ->
    Db = proplists:get_value(db, Config),
    Migrations = base_migrations(),

    %% Baseline up to version 2
    ok = cozodb_migrator:baseline(Db, Migrations, 20260102000000),

    %% First two should be applied, third pending
    {ok, Status} = cozodb_migrator:status(Db, Migrations),
    ?assertMatch(
        [#{status := <<"applied">>}, #{status := <<"applied">>}, #{status := <<"pending">>}],
        Status
    ),

    %% Note: relations won't actually exist since baseline doesn't run up/1
    ?assertNot(cozodb_migrator_utils:relation_exists(Db, <<"users">>)).

failed_migration_test(Config) ->
    Db = proplists:get_value(db, Config),
    Migrations = [
        cozodb_migration_20260101000000_create_users,
        cozodb_migration_20260106000000_failing
    ],

    %% Migrate should fail on the second migration
    ?assertMatch(
        {error, {migration_failed, 20260106000000, intentional_failure}},
        cozodb_migrator:migrate(Db, Migrations)
    ),

    %% First should be applied, second should be failed
    {ok, Applied} = cozodb_migrator_history:applied_versions(Db),
    ?assertMatch(
        [{20260101000000, _, <<"applied">>}, {20260106000000, _, <<"failed">>}],
        Applied
    ).

backup_migration_test(Config) ->
    Db = proplists:get_value(db, Config),
    TMPDir = proplists:get_value(tmp_dir, Config),
    BackupPath = filename:join(TMPDir, "migration_backup"),
    Migrations = [cozodb_migration_20260105000000_with_backup],

    ok = cozodb_migrator:migrate(Db, Migrations, #{
        backup_path => list_to_binary(BackupPath)
    }),

    %% Verify backup file was created
    ?assert(filelib:is_file(BackupPath)),

    %% Verify migration was applied
    ?assert(cozodb_migrator_utils:relation_exists(Db, <<"audit_log">>)).

concurrent_serialization_test(Config) ->
    Db = proplists:get_value(db, Config),
    Migrations = base_migrations(),

    %% Launch concurrent migrate calls
    Self = self(),
    Pids = [
        spawn(fun() ->
            Result = cozodb_migrator:migrate(Db, Migrations),
            Self ! {done, self(), Result}
        end)
     || _ <- lists:seq(1, 3)
    ],

    Results = [
        receive
            {done, Pid, Result} -> Result
        end
     || Pid <- Pids
    ],

    %% All should succeed (serialized by gen_server)
    lists:foreach(
        fun(R) -> ?assertEqual(ok, R) end,
        Results
    ),

    %% Verify only 3 migrations applied (not duplicated)
    {ok, Status} = cozodb_migrator:status(Db, Migrations),
    Applied = [S || #{status := St} = S <- Status, St =:= <<"applied">>],
    ?assertEqual(3, length(Applied)).

recreate_relation_test(Config) ->
    Db = proplists:get_value(db, Config),

    %% Create initial relation
    ok = cozodb:create_relation(Db, <<"test_rel">>, #{
        keys => [{id, int}],
        columns => [{name, string}]
    }),

    %% Insert data
    {ok, _} = cozodb:run(
        Db, <<"?[id, name] <- [[1, 'alice'], [2, 'bob']] :put test_rel {id => name}">>
    ),

    %% Recreate with new column and a default for it
    ok = cozodb_migrator_utils:recreate_relation(
        Db,
        <<"test_rel">>,
        #{
            keys => [{id, int}],
            columns => [{name, string}, {email, string}]
        },
        #{
            column_mapping => #{<<"email">> => <<"">>}
        }
    ),

    %% Verify relation exists with new schema
    {ok, #{rows := Columns}} = cozodb:columns(Db, <<"test_rel">>),
    ColumnNames = [Name || [Name | _] <- Columns],
    ?assert(lists:member(<<"email">>, ColumnNames)),

    %% Verify data was preserved (email has default empty string)
    {ok, #{rows := Rows}} = cozodb:run(Db, <<"?[id, name] := *test_rel{id, name}">>),
    ?assertEqual(2, length(Rows)).

reindex_test(Config) ->
    Db = proplists:get_value(db, Config),

    %% Create relation and index
    ok = cozodb:create_relation(Db, <<"idx_test">>, #{
        keys => [{id, int}],
        columns => [{val, string}]
    }),
    ok = cozodb:create_index(Db, <<"idx_test">>, <<"idx_val">>, #{
        type => covering,
        fields => [val]
    }),

    %% Insert data
    {ok, _} = cozodb:run(Db, <<"?[id, val] <- [[1, 'x'], [2, 'y']] :put idx_test {id => val}">>),

    %% Reindex with same spec
    ok = cozodb_migrator_utils:reindex(Db, <<"idx_test">>, <<"idx_val">>, #{
        type => covering,
        fields => [val]
    }),

    %% Verify data still exists
    {ok, #{rows := Rows}} = cozodb:run(Db, <<"?[id, val] := *idx_test{id, val}">>),
    ?assertEqual(2, length(Rows)).

relation_exists_test(Config) ->
    Db = proplists:get_value(db, Config),

    ?assertNot(cozodb_migrator_utils:relation_exists(Db, <<"nonexistent">>)),

    ok = cozodb:create_relation(Db, <<"exists_test">>, #{
        keys => [{id, int}]
    }),

    ?assert(cozodb_migrator_utils:relation_exists(Db, <<"exists_test">>)).

ensure_relation_test(Config) ->
    Db = proplists:get_value(db, Config),
    Spec = #{keys => [{id, int}], columns => [{val, string}]},

    %% First call creates
    ok = cozodb_migrator_utils:ensure_relation(Db, <<"ensure_test">>, Spec),
    ?assert(cozodb_migrator_utils:relation_exists(Db, <<"ensure_test">>)),

    %% Second call is a no-op
    ok = cozodb_migrator_utils:ensure_relation(Db, <<"ensure_test">>, Spec).

recreate_relation_with_transform_test(Config) ->
    Db = proplists:get_value(db, Config),

    %% Create initial relation
    ok = cozodb:create_relation(Db, <<"transform_rel">>, #{
        keys => [{id, int}],
        columns => [{name, string}]
    }),

    %% Insert data
    {ok, _} = cozodb:run(
        Db,
        <<
            "?[id, name] <- [[1, 'alice'], [2, 'bob'], [3, 'carol'], [4, 'dave']] "
            ":put transform_rel {id => name}"
        >>
    ),

    %% Recreate with a transform function and small batch size
    ok = cozodb_migrator_utils:recreate_relation(
        Db,
        <<"transform_rel">>,
        #{
            keys => [{id, int}],
            columns => [{name, string}, {upper_name, string}]
        },
        #{
            column_mapping => #{<<"upper_name">> => <<"">>},
            transform => fun(Row) ->
                Name = maps:get(<<"name">>, Row, <<"">>),
                Row#{<<"upper_name">> => string:uppercase(Name)}
            end,
            batch_size => 2
        }
    ),

    %% Verify all 4 rows were transformed correctly
    {ok, #{rows := Rows}} = cozodb:run(
        Db, <<"?[id, name, upper_name] := *transform_rel{id, name, upper_name} :order id">>
    ),
    ?assertEqual(4, length(Rows)),
    ?assertEqual([1, <<"alice">>, <<"ALICE">>], lists:nth(1, Rows)),
    ?assertEqual([2, <<"bob">>, <<"BOB">>], lists:nth(2, Rows)),
    ?assertEqual([3, <<"carol">>, <<"CAROL">>], lists:nth(3, Rows)),
    ?assertEqual([4, <<"dave">>, <<"DAVE">>], lists:nth(4, Rows)).

recreate_relation_multi_key_test(Config) ->
    Db = proplists:get_value(db, Config),

    %% Create relation with composite keys
    ok = cozodb:create_relation(Db, <<"multi_key_rel">>, #{
        keys => [{tenant, string}, {id, int}],
        columns => [{value, string}]
    }),

    %% Insert data with various key combinations
    {ok, _} = cozodb:run(
        Db,
        <<
            "?[tenant, id, value] <- [['a', 1, 'v1'], ['a', 2, 'v2'], "
            "['b', 1, 'v3'], ['b', 2, 'v4'], ['c', 1, 'v5']] "
            ":put multi_key_rel {tenant, id => value}"
        >>
    ),

    %% Recreate with new column, using small batch to test cursor pagination
    ok = cozodb_migrator_utils:recreate_relation(
        Db,
        <<"multi_key_rel">>,
        #{
            keys => [{tenant, string}, {id, int}],
            columns => [{value, string}, {extra, string}]
        },
        #{
            column_mapping => #{<<"extra">> => <<"default">>},
            transform => fun(Row) -> Row end,
            batch_size => 2
        }
    ),

    %% Verify all 5 rows preserved
    {ok, #{rows := Rows}} = cozodb:run(
        Db,
        <<"?[tenant, id, value, extra] := *multi_key_rel{tenant, id, value, extra} :order tenant, id">>
    ),
    ?assertEqual(5, length(Rows)),
    %% Verify defaults applied
    lists:foreach(
        fun(Row) ->
            ?assertEqual(<<"default">>, lists:nth(4, Row))
        end,
        Rows
    ).

recreate_relation_empty_test(Config) ->
    Db = proplists:get_value(db, Config),

    %% Create relation with no data
    ok = cozodb:create_relation(Db, <<"empty_rel">>, #{
        keys => [{id, int}],
        columns => [{name, string}]
    }),

    %% Recreate with new schema — should not error
    ok = cozodb_migrator_utils:recreate_relation(
        Db,
        <<"empty_rel">>,
        #{
            keys => [{id, int}],
            columns => [{name, string}, {email, string}]
        },
        #{
            column_mapping => #{<<"email">> => <<"">>}
        }
    ),

    %% Verify new schema
    {ok, #{rows := Columns}} = cozodb:columns(Db, <<"empty_rel">>),
    ColumnNames = [Name || [Name | _] <- Columns],
    ?assert(lists:member(<<"email">>, ColumnNames)),

    %% Verify still empty
    {ok, #{rows := Rows}} = cozodb:run(Db, <<"?[id] := *empty_rel{id}">>),
    ?assertEqual(0, length(Rows)).

recreate_relation_recovery_test(Config) ->
    Db = proplists:get_value(db, Config),

    %% Create a relation
    ok = cozodb:create_relation(Db, <<"recovery_rel">>, #{
        keys => [{id, int}],
        columns => [{name, string}]
    }),

    %% Insert data
    {ok, _} = cozodb:run(
        Db, <<"?[id, name] <- [[1, 'alice']] :put recovery_rel {id => name}">>
    ),

    %% Simulate a leftover tmp relation (as if a prior migration failed)
    TmpName = <<"cozodb_migrator_tmp_recovery_rel">>,
    ok = cozodb:create_relation(Db, TmpName, #{
        keys => [{id, int}],
        columns => [{name, string}]
    }),
    {ok, _} = cozodb:run(
        Db, <<"?[id, name] <- [[99, 'stale']] :put cozodb_migrator_tmp_recovery_rel {id => name}">>
    ),

    %% Recreate should clean up the stale tmp and succeed
    ok = cozodb_migrator_utils:recreate_relation(
        Db,
        <<"recovery_rel">>,
        #{
            keys => [{id, int}],
            columns => [{name, string}, {email, string}]
        },
        #{
            column_mapping => #{<<"email">> => <<"">>}
        }
    ),

    %% Verify original data preserved
    {ok, #{rows := Rows}} = cozodb:run(
        Db, <<"?[id, name] := *recovery_rel{id, name}">>
    ),
    ?assertEqual(1, length(Rows)),
    ?assertEqual([1, <<"alice">>], hd(Rows)),

    %% Verify tmp cleaned up
    ?assertNot(cozodb_migrator_utils:relation_exists(Db, TmpName)).

reindex_preserves_other_indices_test(Config) ->
    Db = proplists:get_value(db, Config),

    %% Create relation with two indices
    ok = cozodb:create_relation(Db, <<"multi_idx">>, #{
        keys => [{id, int}],
        columns => [{name, string}, {email, string}]
    }),
    ok = cozodb:create_index(Db, <<"multi_idx">>, <<"idx_name">>, #{
        type => covering,
        fields => [name]
    }),
    ok = cozodb:create_index(Db, <<"multi_idx">>, <<"idx_email">>, #{
        type => covering,
        fields => [email]
    }),

    %% Insert data
    {ok, _} = cozodb:run(
        Db,
        <<
            "?[id, name, email] <- [[1, 'alice', 'a@test.com'], [2, 'bob', 'b@test.com']] "
            ":put multi_idx {id => name, email}"
        >>
    ),

    %% Reindex only idx_email with new fields
    ok = cozodb_migrator_utils:reindex(
        Db,
        <<"multi_idx">>,
        <<"idx_email">>,
        #{type => covering, fields => [email]}
    ),

    %% Verify data preserved
    {ok, #{rows := Rows}} = cozodb:run(
        Db, <<"?[id, name, email] := *multi_idx{id, name, email} :order id">>
    ),
    ?assertEqual(2, length(Rows)),

    %% Verify both indices still exist
    {ok, #{rows := IdxRows}} = cozodb:indices(Db, <<"multi_idx">>),
    IdxNames = [Name || [Name | _] <- IdxRows],
    ?assert(lists:member(<<"idx_name">>, IdxNames)),
    ?assert(lists:member(<<"idx_email">>, IdxNames)).

copy_relation_no_transform_test(Config) ->
    Db = proplists:get_value(db, Config),

    %% Create source relation with data
    ok = cozodb:create_relation(Db, <<"copy_src">>, #{
        keys => [{id, int}],
        columns => [{name, string}]
    }),
    {ok, _} = cozodb:run(
        Db,
        <<"?[id, name] <- [[1, 'alice'], [2, 'bob']] :put copy_src {id => name}">>
    ),

    %% Create target relation (same schema)
    ok = cozodb:create_relation(Db, <<"copy_tgt">>, #{
        keys => [{id, int}],
        columns => [{name, string}]
    }),

    %% Copy with zero-memory CozoScript path
    ok = cozodb_migrator_utils:copy_relation(Db, <<"copy_src">>, <<"copy_tgt">>),

    %% Verify target has the data
    {ok, #{rows := Rows}} = cozodb:run(
        Db, <<"?[id, name] := *copy_tgt{id, name} :order id">>
    ),
    ?assertEqual(2, length(Rows)),
    ?assertEqual([1, <<"alice">>], lists:nth(1, Rows)),
    ?assertEqual([2, <<"bob">>], lists:nth(2, Rows)).

rename_relation_test(Config) ->
    Db = proplists:get_value(db, Config),

    ok = cozodb:create_relation(Db, <<"old_name">>, #{
        keys => [{id, int}]
    }),

    ok = cozodb_migrator_utils:rename_relation(Db, <<"old_name">>, <<"new_name">>),

    ?assertNot(cozodb_migrator_utils:relation_exists(Db, <<"old_name">>)),
    ?assert(cozodb_migrator_utils:relation_exists(Db, <<"new_name">>)).

%% =============================================================================
%% HELPERS
%% =============================================================================

base_migrations() ->
    [
        cozodb_migration_20260101000000_create_users,
        cozodb_migration_20260102000000_create_posts,
        cozodb_migration_20260103000000_add_posts_index
    ].

compile_migrations(Dir) ->
    {ok, Files} = file:list_dir(Dir),
    ErlFiles = [F || F <- Files, filename:extension(F) =:= ".erl"],
    lists:foreach(
        fun(File) ->
            Path = filename:join(Dir, File),
            {ok, Module, Binary} = compile:file(Path, [
                binary,
                debug_info,
                {feature, maybe_expr, enable}
            ]),
            %% Write .beam to the same directory so beam_lib can find it
            BeamFile = filename:join(Dir, atom_to_list(Module) ++ ".beam"),
            ok = file:write_file(BeamFile, Binary),
            %% Add the directory to code path and load
            true = code:add_patha(Dir),
            {module, Module} = code:load_file(Module)
        end,
        ErlFiles
    ),
    ok.
