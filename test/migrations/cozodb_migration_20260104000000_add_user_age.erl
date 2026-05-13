-module(cozodb_migration_20260104000000_add_user_age).
-behaviour(cozodb_migration).

-export([up/1, down/1, description/0]).

description() ->
    <<"Add age column to users via recreate">>.

up(Db) ->
    cozodb_migrator_utils:recreate_relation(Db, <<"users">>, #{
        keys => [{id, int}],
        columns => [{name, string}, {email, string}, {age, int}]
    }, #{
        column_mapping => #{<<"age">> => 0}
    }).

down(Db) ->
    cozodb_migrator_utils:recreate_relation(Db, <<"users">>, #{
        keys => [{id, int}],
        columns => [{name, string}, {email, string}]
    }).
