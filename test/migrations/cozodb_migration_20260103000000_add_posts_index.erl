-module(cozodb_migration_20260103000000_add_posts_index).
-behaviour(cozodb_migration).

-export([up/1, down/1]).

up(Db) ->
    cozodb:create_index(Db, <<"posts">>, <<"posts_by_user">>, #{
        type => covering,
        fields => [user_id, title]
    }).

down(Db) ->
    cozodb:drop_index(Db, <<"posts">>, <<"posts_by_user">>).
