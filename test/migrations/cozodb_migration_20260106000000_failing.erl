-module(cozodb_migration_20260106000000_failing).
-behaviour(cozodb_migration).

-export([up/1]).

up(_Db) ->
    {error, intentional_failure}.
