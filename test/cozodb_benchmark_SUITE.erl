%% =============================================================================
%%  cozodb_benchmark_SUITE.erl - Concurrency Benchmark Suite
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
-module(cozodb_benchmark_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% Test configuration
-define(NUM_WORKERS, 100).
-define(OPS_PER_WORKER, 100).
-define(NUM_TABLES, 10).

-compile(export_all).

%% -----------------------------------------------------------------------------
%% Common Test Callbacks
%% -----------------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 10}}].

init_per_suite(Config) ->
    %% Start telemetry application
    _ = application:ensure_all_started(telemetry),

    TMPDir = os:getenv("COZODB_TMP_DIR", "/tmp/cozodb_benchmark"),
    _ = catch file:del_dir_r(TMPDir),
    _ = catch file:make_dir(TMPDir),
    ct:pal(
        "Benchmark configuration:~n"
        "  Workers: ~p~n"
        "  Ops per worker: ~p~n"
        "  Tables: ~p~n"
        "  Dirty IO Schedulers: ~p~n",
        [
            ?NUM_WORKERS,
            ?OPS_PER_WORKER,
            ?NUM_TABLES,
            erlang:system_info(dirty_io_schedulers)
        ]
    ),
    [{tmp_dir, TMPDir} | Config].

end_per_suite(Config) ->
    TMPDir = ?config(tmp_dir, Config),
    _ = catch file:del_dir_r(TMPDir),
    ok.

init_per_testcase(TestCase, Config) ->
    TMPDir = ?config(tmp_dir, Config),
    Path = filename:join([TMPDir, atom_to_list(TestCase), integer_to_list(erlang:system_time())]),
    ok = filelib:ensure_dir(Path ++ "/"),
    [{db_path, Path} | Config].

end_per_testcase(_TestCase, Config) ->
    _ = catch file:del_dir_r(?config(db_path, Config)),
    ok.

groups() ->
    [
        {rocksdb, [sequence], rocksdb_tests()},
        {mem, [sequence], mem_tests()}
    ].

rocksdb_tests() ->
    [
        benchmark_100_read_rocksdb,
        benchmark_50_50_rw_diff_tables_rocksdb,
        benchmark_80_20_wr_diff_tables_rocksdb
    ].

mem_tests() ->
    [
        benchmark_100_read_mem,
        benchmark_50_50_rw_diff_tables_mem,
        benchmark_80_20_wr_diff_tables_mem
    ].

all() ->
    [
        {group, mem},
        {group, rocksdb}
    ].

%% -----------------------------------------------------------------------------
%% Benchmark: 100% Read Workload (Memory Backend)
%% -----------------------------------------------------------------------------

benchmark_100_read_mem(_Config) ->
    {ok, Db} = cozodb:open(mem),

    %% Create tables and seed with data
    Tables = create_tables(Db, ?NUM_TABLES),
    seed_tables(Db, Tables, 1000),

    ct:pal("~n=== Benchmark: 100% Read Workload (Memory) ===~n"),

    %% Run the benchmark
    {TotalOps, TotalTimeUs, Latencies} = run_benchmark(
        Db,
        Tables,
        ?NUM_WORKERS,
        ?OPS_PER_WORKER,
        fun read_operation/2
    ),

    report_results("100% Read (Memory)", TotalOps, TotalTimeUs, Latencies),

    ok = cozodb:close(Db).

%% -----------------------------------------------------------------------------
%% Benchmark: 100% Read Workload (RocksDB Backend)
%% -----------------------------------------------------------------------------

benchmark_100_read_rocksdb(Config) ->
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(rocksdb, Path),

    %% Create tables and seed with data
    Tables = create_tables(Db, ?NUM_TABLES),
    seed_tables(Db, Tables, 1000),

    ct:pal("~n=== Benchmark: 100% Read Workload (RocksDB) ===~n"),

    %% Run the benchmark
    {TotalOps, TotalTimeUs, Latencies} = run_benchmark(
        Db,
        Tables,
        ?NUM_WORKERS,
        ?OPS_PER_WORKER,
        fun read_operation/2
    ),

    report_results("100% Read (RocksDB)", TotalOps, TotalTimeUs, Latencies),

    ok = cozodb:close(Db).

%% -----------------------------------------------------------------------------
%% Benchmark: 50/50 Read/Write on Different Tables (Memory Backend)
%% -----------------------------------------------------------------------------

benchmark_50_50_rw_diff_tables_mem(_Config) ->
    {ok, Db} = cozodb:open(mem),

    %% Create tables - half for reads, half for writes
    Tables = create_tables(Db, ?NUM_TABLES),
    {ReadTables, WriteTables} = lists:split(?NUM_TABLES div 2, Tables),
    seed_tables(Db, Tables, 1000),

    ct:pal("~n=== Benchmark: 50/50 Read/Write on Different Tables (Memory) ===~n"),
    ct:pal("Read tables: ~p~nWrite tables: ~p~n", [ReadTables, WriteTables]),

    {TotalOps, TotalTimeUs, AllLatencies} = run_mixed_benchmark(
        Db, ReadTables, WriteTables, ?NUM_WORKERS, ?OPS_PER_WORKER, 50
    ),

    report_results("50/50 R/W Different Tables (Memory)", TotalOps, TotalTimeUs, AllLatencies),

    ok = cozodb:close(Db).

%% -----------------------------------------------------------------------------
%% Benchmark: 50/50 Read/Write on Different Tables (RocksDB Backend)
%% -----------------------------------------------------------------------------

benchmark_50_50_rw_diff_tables_rocksdb(Config) ->
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(rocksdb, Path),

    %% Create tables - half for reads, half for writes
    Tables = create_tables(Db, ?NUM_TABLES),
    {ReadTables, WriteTables} = lists:split(?NUM_TABLES div 2, Tables),
    seed_tables(Db, Tables, 1000),

    ct:pal("~n=== Benchmark: 50/50 Read/Write on Different Tables (RocksDB) ===~n"),
    ct:pal("Read tables: ~p~nWrite tables: ~p~n", [ReadTables, WriteTables]),

    {TotalOps, TotalTimeUs, AllLatencies} = run_mixed_benchmark(
        Db, ReadTables, WriteTables, ?NUM_WORKERS, ?OPS_PER_WORKER, 50
    ),

    report_results("50/50 R/W Different Tables (RocksDB)", TotalOps, TotalTimeUs, AllLatencies),

    ok = cozodb:close(Db).

%% -----------------------------------------------------------------------------
%% Benchmark: 80/20 Write/Read on Different Tables (Memory Backend)
%% -----------------------------------------------------------------------------

benchmark_80_20_wr_diff_tables_mem(_Config) ->
    {ok, Db} = cozodb:open(mem),

    %% Create tables - 20% for reads, 80% for writes
    Tables = create_tables(Db, ?NUM_TABLES),
    NumReadTables = max(1, ?NUM_TABLES div 5),
    {ReadTables, WriteTables} = lists:split(NumReadTables, Tables),
    seed_tables(Db, Tables, 1000),

    ct:pal("~n=== Benchmark: 80/20 Write/Read on Different Tables (Memory) ===~n"),
    ct:pal(
        "Read tables (~p%): ~p~nWrite tables (~p%): ~p~n",
        [20, ReadTables, 80, WriteTables]
    ),

    {TotalOps, TotalTimeUs, AllLatencies} = run_mixed_benchmark(
        Db, ReadTables, WriteTables, ?NUM_WORKERS, ?OPS_PER_WORKER, 20
    ),

    report_results("80/20 W/R Different Tables (Memory)", TotalOps, TotalTimeUs, AllLatencies),

    ok = cozodb:close(Db).

%% -----------------------------------------------------------------------------
%% Benchmark: 80/20 Write/Read on Different Tables (RocksDB Backend)
%% -----------------------------------------------------------------------------

benchmark_80_20_wr_diff_tables_rocksdb(Config) ->
    Path = ?config(db_path, Config),
    {ok, Db} = cozodb:open(rocksdb, Path),

    %% Create tables - 20% for reads, 80% for writes
    Tables = create_tables(Db, ?NUM_TABLES),
    NumReadTables = max(1, ?NUM_TABLES div 5),
    {ReadTables, WriteTables} = lists:split(NumReadTables, Tables),
    seed_tables(Db, Tables, 1000),

    ct:pal("~n=== Benchmark: 80/20 Write/Read on Different Tables (RocksDB) ===~n"),
    ct:pal(
        "Read tables (~p%): ~p~nWrite tables (~p%): ~p~n",
        [20, ReadTables, 80, WriteTables]
    ),

    {TotalOps, TotalTimeUs, AllLatencies} = run_mixed_benchmark(
        Db, ReadTables, WriteTables, ?NUM_WORKERS, ?OPS_PER_WORKER, 20
    ),

    report_results("80/20 W/R Different Tables (RocksDB)", TotalOps, TotalTimeUs, AllLatencies),

    ok = cozodb:close(Db).

%% -----------------------------------------------------------------------------
%% Helper Functions
%% -----------------------------------------------------------------------------

create_tables(Db, NumTables) ->
    Tables = [
        list_to_binary("bench_table_" ++ integer_to_list(I))
     || I <- lists:seq(1, NumTables)
    ],
    lists:foreach(
        fun(Table) ->
            ok = cozodb:create_relation(Db, Table, #{
                keys => [{id, int}],
                columns => [{value, string}, {counter, int}]
            })
        end,
        Tables
    ),
    Tables.

seed_tables(Db, Tables, RowsPerTable) ->
    lists:foreach(
        fun(Table) ->
            Rows = [[I, <<"initial_value">>, 0] || I <- lists:seq(1, RowsPerTable)],
            Query = iolist_to_binary([
                "?[id, value, counter] <- $rows\n",
                ":put ",
                Table,
                " {id => value, counter}"
            ]),
            {ok, _} = cozodb:run(Db, Query, #{parameters => #{<<"rows">> => Rows}})
        end,
        Tables
    ).

run_benchmark(Db, Tables, NumWorkers, OpsPerWorker, OpFun) ->
    Parent = self(),
    CollectorPid = spawn_link(fun() -> result_collector(Parent, NumWorkers) end),

    StartTime = erlang:monotonic_time(microsecond),

    %% Spawn workers
    lists:foreach(
        fun(_) ->
            spawn_link(fun() ->
                Latencies = worker_loop(Db, Tables, OpsPerWorker, OpFun),
                CollectorPid ! {result, OpsPerWorker, Latencies}
            end)
        end,
        lists:seq(1, NumWorkers)
    ),

    %% Wait for all results
    {TotalOps, AllLatencies} =
        receive
            {all_results, Ops, Lats} -> {Ops, Lats}
        end,

    EndTime = erlang:monotonic_time(microsecond),
    TotalTimeUs = EndTime - StartTime,

    {TotalOps, TotalTimeUs, AllLatencies}.

run_mixed_benchmark(Db, ReadTables, WriteTables, NumWorkers, OpsPerWorker, ReadPercent) ->
    Parent = self(),
    CollectorPid = spawn_link(fun() -> result_collector(Parent, NumWorkers) end),

    NumReaders = max(1, NumWorkers * ReadPercent div 100),
    NumWriters = NumWorkers - NumReaders,

    StartTime = erlang:monotonic_time(microsecond),

    %% Spawn readers
    lists:foreach(
        fun(_) ->
            spawn_link(fun() ->
                Latencies = worker_loop(Db, ReadTables, OpsPerWorker, fun read_operation/2),
                CollectorPid ! {result, OpsPerWorker, Latencies}
            end)
        end,
        lists:seq(1, NumReaders)
    ),

    %% Spawn writers
    lists:foreach(
        fun(_) ->
            spawn_link(fun() ->
                Latencies = worker_loop(Db, WriteTables, OpsPerWorker, fun write_operation/2),
                CollectorPid ! {result, OpsPerWorker, Latencies}
            end)
        end,
        lists:seq(1, NumWriters)
    ),

    %% Wait for all results
    {TotalOps, AllLatencies} =
        receive
            {all_results, Ops, Lats} -> {Ops, Lats}
        end,

    EndTime = erlang:monotonic_time(microsecond),
    TotalTimeUs = EndTime - StartTime,

    {TotalOps, TotalTimeUs, AllLatencies}.

result_collector(Parent, NumWorkers) ->
    result_collector(Parent, NumWorkers, 0, []).

result_collector(Parent, 0, TotalOps, AllLatencies) ->
    Parent ! {all_results, TotalOps, AllLatencies};
result_collector(Parent, Remaining, TotalOps, AllLatencies) ->
    receive
        {result, Ops, Latencies} ->
            result_collector(Parent, Remaining - 1, TotalOps + Ops, Latencies ++ AllLatencies)
    end.

worker_loop(Db, Tables, NumOps, OpFun) ->
    worker_loop(Db, Tables, NumOps, OpFun, []).

worker_loop(_Db, _Tables, 0, _OpFun, Latencies) ->
    Latencies;
worker_loop(Db, Tables, Remaining, OpFun, Latencies) ->
    %% Pick a random table
    Table = lists:nth(rand:uniform(length(Tables)), Tables),

    Start = erlang:monotonic_time(microsecond),
    OpFun(Db, Table),
    End = erlang:monotonic_time(microsecond),

    Latency = End - Start,
    worker_loop(Db, Tables, Remaining - 1, OpFun, [Latency | Latencies]).

read_operation(Db, Table) ->
    read_operation(Db, Table, 5).

read_operation(_Db, _Table, 0) ->
    %% Max retries exceeded
    {error, max_retries};
read_operation(Db, Table, Retries) ->
    %% Random read
    Id = rand:uniform(1000),
    Query = iolist_to_binary([
        "?[id, value, counter] := *", Table, "{id, value, counter}, id = $id"
    ]),
    case cozodb:run(Db, Query, #{parameters => #{<<"id">> => Id}}) of
        {ok, _} ->
            ok;
        {error, #{message := Msg}} when is_binary(Msg) ->
            case is_transient_error(Msg) of
                true ->
                    timer:sleep(rand:uniform(10) * (6 - Retries)),
                    read_operation(Db, Table, Retries - 1);
                false ->
                    error({read_failed, Msg})
            end;
        {error, Reason} ->
            error({read_failed, Reason})
    end.

write_operation(Db, Table) ->
    write_operation(Db, Table, 10).

write_operation(_Db, _Table, 0) ->
    %% Max retries exceeded, count as failed but don't crash
    {error, max_retries};
write_operation(Db, Table, Retries) ->
    %% Random write (update)
    Id = rand:uniform(1000),
    Value = iolist_to_binary(["value_", integer_to_list(erlang:system_time())]),
    Query = iolist_to_binary([
        "?[id, value, counter] <- [[$id, $value, 1]]\n",
        ":put ",
        Table,
        " {id => value, counter}"
    ]),
    case cozodb:run(Db, Query, #{parameters => #{<<"id">> => Id, <<"value">> => Value}}) of
        {ok, _} ->
            ok;
        {error, #{message := Msg}} when is_binary(Msg) ->
            case is_transient_error(Msg) of
                true ->
                    %% Transient error - retry with exponential backoff
                    timer:sleep(rand:uniform(10) * (11 - Retries)),
                    write_operation(Db, Table, Retries - 1);
                false ->
                    error({write_failed, Msg})
            end;
        {error, Reason} ->
            error({write_failed, Reason})
    end.

%% Check if error message indicates a transient/retryable error
is_transient_error(Msg) ->
    binary:match(Msg, <<"database is locked">>) =/= nomatch orelse
        binary:match(Msg, <<"Resource busy">>) =/= nomatch orelse
        binary:match(Msg, <<"try again">>) =/= nomatch.

report_results(Label, TotalOps, TotalTimeUs, Latencies) ->
    TotalTimeSec = TotalTimeUs / 1_000_000,
    OpsPerSec = TotalOps / TotalTimeSec,

    SortedLatencies = lists:sort(Latencies),
    NumLatencies = length(SortedLatencies),

    MinLatency = hd(SortedLatencies),
    MaxLatency = lists:last(SortedLatencies),
    AvgLatency = lists:sum(SortedLatencies) / NumLatencies,

    P50 = percentile(SortedLatencies, 50),
    P90 = percentile(SortedLatencies, 90),
    P95 = percentile(SortedLatencies, 95),
    P99 = percentile(SortedLatencies, 99),

    ct:pal(
        "~n=== Results: ~s ===~n"
        "Total Operations: ~p~n"
        "Total Time: ~.2f seconds~n"
        "Throughput: ~.2f ops/sec~n"
        "~n"
        "Latency (microseconds):~n"
        "  Min: ~p~n"
        "  Max: ~p~n"
        "  Avg: ~.2f~n"
        "  P50: ~p~n"
        "  P90: ~p~n"
        "  P95: ~p~n"
        "  P99: ~p~n",
        [
            Label,
            TotalOps,
            TotalTimeSec,
            OpsPerSec,
            MinLatency,
            MaxLatency,
            AvgLatency,
            P50,
            P90,
            P95,
            P99
        ]
    ).

percentile(SortedList, P) ->
    N = length(SortedList),
    Index = max(1, min(N, round(P / 100 * N))),
    lists:nth(Index, SortedList).
