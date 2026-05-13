-module(cozodb_migration_20260102000000_create_posts).
-behaviour(cozodb_migration).

-export([up/1, down/1]).

up(Db) ->
    cozodb:create_relation(Db, <<"posts">>, #{
        keys => [{id, int}],
        columns => [{user_id, int}, {title, string}, {body, string}]
    }).

down(Db) ->
    case cozodb:remove_relation(Db, <<"posts">>) of
        ok -> ok;
        {ok, _} -> ok;
        {error, _} = Error -> Error
    end.
