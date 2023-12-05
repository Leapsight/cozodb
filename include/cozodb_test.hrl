%%%===================================================================
%%% Copyright (c) 2023 Mathieu Kerjouan
%%%
%%% Redistribution and use in source and binary forms, with or without
%%% modification, are permitted provided that the following conditions
%%% are met:
%%%
%%% 1. Redistributions of source code must retain the above copyright
%%% notice, this list of conditions and the following disclaimer.
%%%
%%% 2. Redistributions in binary form must reproduce the above
%%% copyright notice, this list of conditions and the following
%%% disclaimer in the documentation and/or other materials provided
%%% with the distribution.
%%%
%%% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
%%% CONTRIBUTORS “AS IS” AND ANY EXPRESS OR IMPLIED WARRANTIES,
%%% INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
%%% MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
%%% DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS
%%% BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
%%% EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
%%% TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
%%% DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
%%% ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
%%% TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
%%% THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
%%% SUCH DAMAGE.
%%%
%%% @copyright 2023 Mathieu Kerjouan
%%% @author Mathieu Kerjouan
%%%===================================================================
% helper to create ok queries
-define(QUERY_OK(VAR_QUERY),
        begin
            (fun() ->
                     {ok, Db} = cozodb:open(Engine),
                     {ok, Result} = cozodb:run(Db, VAR_QUERY),
                     LogFormat = "db: ~p~n"
                         "query: ~s~n"
                         "result: ~p~n",
                     LogArgs = [Db, Engine, Result],
                     ct:pal(info, ?LOW_IMPORTANCE, LogFormat, LogArgs),
                     ok = cozodb:close(Db)
             end)()
        end).

-define(QUERY_OK(DB, VAR_QUERY),
        begin
            (fun() ->
                     {ok, Result} = cozodb:run(DB, VAR_QUERY),
                     LogFormat = "db: ~p~n"
                         "query: ~s~n"
                         "result: ~p~n",
                     LogArgs = [DB, Engine, Result],
                     ct:pal(info, ?LOW_IMPORTANCE, LogFormat, LogArgs),
                     ok
             end)()
        end).


-define(COZO_OK(DB,FUNCTION,ARGS),
    begin
        (fun() ->
             {ok, Result} = erlang:apply(Module, FUNCTION, [DB] ++ ARGS),
             LogFormat = "db: ~p~n"
             "mfa: {~p,~p,~p}~n"
             "result: (ok) ~p~n",
             LogArgs = [DB, Module, FUNCTION, ARGS, Result],
             ct:pal(info, ?LOW_IMPORTANCE, LogFormat, LogArgs)
         end)()
    end).

-define(COZO_ERROR(DB,FUNCTION,ARGS),
    begin
        (fun() ->
             {error, Result} = erlang:apply(Module, FUNCTION, [DB] ++ ARGS),
             LogFormat = "db: ~p~n"
             "mfa: {~p,~p,~p}~n"
             "result: (error) ~p~n",
             LogArgs = [DB, Module, FUNCTION, ARGS, Result],
             ct:pal(info, ?LOW_IMPORTANCE, LogFormat, LogArgs)
         end)()
    end).

% helper to create error queries
-define(QUERY_ERROR(N,G,Q),
        begin
            (fun() ->
                     {ok, N} = cozodb:open(G),
                     {error, E} = cozodb:run(N, Q),
                     LogFormat = "db: ~p~n"
                         "query: ~s~n"
                         "result: ~p~n",
                     LogArgs = [N, Q, E],
                     ct:pal(info, ?LOW_IMPORTANCE, LogFormat, LogArgs),
                     ok = cozodb:close(N)
             end)()
        end).

-define(IQUERY_LOG(DB, QUERY),
        begin
            (fun() ->
                     {ok, _} = Res = cozodb:run(DB, QUERY),
                     LogFormat = "db: ~p~n"
             "query: ~s~n",
             % "result: ~p",
                     LogArgs = [DB,QUERY],
                     ct:pal(info, ?LOW_IMPORTANCE, LogFormat, LogArgs),
                     Res
             end)()
        end).
