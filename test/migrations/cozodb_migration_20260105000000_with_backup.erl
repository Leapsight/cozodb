-module(cozodb_migration_20260105000000_with_backup).
-behaviour(cozodb_migration).

-export([up/1, backup/0, description/0]).

description() ->
    <<"Migration that requests a backup before running">>.

backup() ->
    true.

up(Db) ->
    cozodb:create_relation(Db, <<"audit_log">>, #{
        keys => [{id, int}],
        columns => [{action, string}, {timestamp, float}]
    }).
