-module(cozodb_migration_20260101000000_create_users).
-behaviour(cozodb_migration).

-export([up/1, down/1, description/0]).

description() ->
    <<"Create users relation">>.

up(Db) ->
    cozodb:create_relation(Db, <<"users">>, #{
        keys => [{id, int}],
        columns => [{name, string}, {email, string}]
    }).

down(Db) ->
    case cozodb:remove_relation(Db, <<"users">>) of
        ok -> ok;
        {ok, _} -> ok;
        {error, _} = Error -> Error
    end.
