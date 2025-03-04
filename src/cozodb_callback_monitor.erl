%% =============================================================================
%%  plum_db_startup_coordinator.erl -
%%
%%  Copyright (c) 2016-2021 Leapsight. All rights reserved.
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


-module(cozodb_callback_monitor).
-moduledoc """
A transient worker that is used to listen to certain plum_db events and
allow watchers to wait (blocking the caller) for certain conditions.
This is used by plum_db_app during the startup process to wait for the
following conditions:

* Partition initisalisation – the worker subscribes to plum_db notifications
and keeps track of each partition initialisation until they are all
initialised (or failed to initilised) and replies to all watchers with a
`ok' or `{error, FailedPartitions}', where FailedPartitions is a map() which
keys are the partition number and the value is the reason for the failure.
* Partition hashtree build – the worker subscribes to plum_db notifications
and keeps track of each partition hashtree until they are all
built (or failed to build) and replies to all watchers with a
`ok' or `{error, FailedHashtrees}', where FailedHashtrees is a map() which
keys are the partition number and the value is the reason for the failure.

A watcher is any process which calls the functions wait_for_partitions/0,1
and/or wait_for_hashtrees/0,1. Both functions will block the caller until
the above conditions are met.

""".

-behaviour(gen_server).
-include_lib("kernel/include/logger.hrl").

-record(state, {}).

-type state() :: #state{}.

-export([start_link/0]).
-export([stop/0]).

%% gen_server callbacks
-export([init/1]).
-export([handle_continue/2]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%% =============================================================================
%% API
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @doc Start the cozodb_callback_monitor server.
%% @end
%% -----------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec stop() -> ok.

stop() ->
    gen_server:stop(?MODULE).

%% =============================================================================
%% GEN_SERVER_ CALLBACKS
%% =============================================================================

%% @private
-spec init([]) ->
    {ok, state()}
    | {ok, state(), non_neg_integer() | infinity}
    | ignore
    | {stop, term()}.

init([]) ->
    State = #state{},

    {ok, State}.

handle_continue(_, State) ->
    {noreply, State}.

%% @private
-spec handle_call(term(), {pid(), term()}, state()) ->
    {reply, term(), state()}
    | {reply, term(), state(), non_neg_integer()}
    | {reply, term(), state(), {continue, term()}}
    | {noreply, state()}
    | {noreply, state(), non_neg_integer()}
    | {noreply, state(), {continue, term()}}
    | {stop, term(), term(), state()}
    | {stop, term(), state()}.

handle_call(_Message, _From, State) ->
    {reply, {error, unsupported_call}, State}.

%% @private
-spec handle_cast(term(), state()) ->
    {noreply, state()}
    | {noreply, state(), non_neg_integer()}
    | {noreply, state(), {continue, term()}}
    | {stop, term(), state()}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
-spec handle_info(term(), state()) ->
    {noreply, state()}
    | {noreply, state(), non_neg_integer()}
    | {noreply, state(), {continue, term()}}
    | {stop, term(), state()}.

handle_info({'DOWN', _Ref, process, _Pid, _Reason}, #state{} = State) ->
    {noreply, State};
handle_info(Event, State) ->
    ?LOG_INFO(#{
        reason => unsupported_event,
        event => Event
    }),
    {noreply, State}.

%% @private
-spec terminate(term(), state()) -> term().

terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, state(), term()) -> {ok, state()}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% =============================================================================
%% PRIVATE
%% =============================================================================
