#!/usr/bin/env elixir
#
# Stress test: 10-way join reads + heavy writes on all tables
#
# Tests that long-running read-only queries joining across many relations
# do not get "Timeout waiting to lock key" errors when concurrent writes
# are happening on those same relations.
#
# Usage:
#   ERL_FLAGS="+SDio 256" mix run lib/join_stress_test.exs [duration_secs]
#
# Default duration: 600 seconds (10 minutes)

defmodule JoinStressTest do
  @num_tables 10
  @seed_rows 10_000
  @value_size 256
  @num_readers 50
  @num_writers 50
  @join_range 2000
  @metrics_interval_ms 5_000

  def run(duration_secs \\ 600) do
    IO.puts("=== Join Stress Test ===")
    IO.puts("Duration: #{duration_secs}s")
    IO.puts("Tables: #{@num_tables}, Rows/table: #{@seed_rows}")
    IO.puts("Readers: #{@num_readers} (10-way join, range=#{@join_range})")
    IO.puts("Writers: #{@num_writers} (all tables)")
    IO.puts("")

    # Setup
    db_path = Path.join(System.tmp_dir!(), "join_stress_test_#{System.monotonic_time()}")
    File.mkdir_p!(db_path)

    IO.puts("Opening DB at #{db_path}...")
    {:ok, db} = :cozodb.open(:rocksdb, db_path)

    tables = create_and_seed(db)
    join_query = build_join_query(tables)

    IO.puts("Join query:\n  #{String.replace(join_query, "\n", "\n  ")}")
    IO.puts("")

    # Warm up: run the join once and measure baseline
    {warm_us, _} = :timer.tc(fn ->
      {:ok, result} = :cozodb.run(db, join_query, %{
        parameters: %{"lo" => 1, "hi" => @join_range},
        read_only: true
      })
      IO.puts("Warmup join returned #{length(result.rows)} rows")
    end)
    IO.puts("Warmup latency: #{div(warm_us, 1000)}ms")
    IO.puts("")

    # Counters
    read_count = :counters.new(1, [:write_concurrency])
    write_count = :counters.new(1, [:write_concurrency])
    error_count = :counters.new(1, [:write_concurrency])
    lock_timeout_count = :counters.new(1, [:write_concurrency])
    other_error_count = :counters.new(1, [:write_concurrency])
    read_latency_sum = :counters.new(1, [:write_concurrency])
    read_latency_max = :atomics.new(1, [])
    stop_flag = :atomics.new(1, [])

    # Start writers
    IO.puts("Starting #{@num_writers} writers...")
    writers = for _ <- 1..@num_writers do
      spawn_link(fn ->
        writer_loop(db, tables, stop_flag, write_count, error_count, lock_timeout_count, other_error_count)
      end)
    end

    # Start readers
    IO.puts("Starting #{@num_readers} readers...")
    readers = for _ <- 1..@num_readers do
      spawn_link(fn ->
        reader_loop(db, join_query, stop_flag, read_count, error_count,
                    lock_timeout_count, other_error_count, read_latency_sum, read_latency_max)
      end)
    end

    # Metrics reporter
    reporter = spawn_link(fn ->
      metrics_loop(read_count, write_count, error_count, lock_timeout_count,
                   other_error_count, read_latency_sum, read_latency_max, stop_flag)
    end)

    IO.puts("Running for #{duration_secs} seconds...\n")
    IO.puts(String.pad_trailing("Time", 8) <>
            String.pad_trailing("Reads", 10) <>
            String.pad_trailing("Writes", 10) <>
            String.pad_trailing("LockTmout", 12) <>
            String.pad_trailing("OtherErr", 10) <>
            String.pad_trailing("AvgReadMs", 12) <>
            "MaxReadMs")
    IO.puts(String.duplicate("-", 80))

    Process.sleep(duration_secs * 1000)

    # Stop all workers
    :atomics.put(stop_flag, 1, 1)
    Process.sleep(2000)

    # Final stats
    total_reads = :counters.get(read_count, 1)
    total_writes = :counters.get(write_count, 1)
    total_lock_timeouts = :counters.get(lock_timeout_count, 1)
    total_other_errors = :counters.get(other_error_count, 1)
    total_errors = :counters.get(error_count, 1)

    IO.puts("\n" <> String.duplicate("=", 80))
    IO.puts("RESULTS")
    IO.puts(String.duplicate("=", 80))
    IO.puts("  Total reads (10-way joins):  #{total_reads}")
    IO.puts("  Total writes:                #{total_writes}")
    IO.puts("  Total errors:                #{total_errors}")
    IO.puts("  Lock timeout errors:         #{total_lock_timeouts}")
    IO.puts("  Other errors:                #{total_other_errors}")
    IO.puts("")

    if total_lock_timeouts > 0 do
      IO.puts("  *** FAIL: #{total_lock_timeouts} lock timeout errors detected! ***")
    else
      IO.puts("  PASS: Zero lock timeout errors")
    end

    # Cleanup
    Process.exit(reporter, :kill)
    Enum.each(writers ++ readers, fn pid ->
      if Process.alive?(pid), do: Process.exit(pid, :kill)
    end)

    :cozodb.close(db)
    File.rm_rf!(db_path)

    IO.puts("\nDone.")
  end

  defp create_and_seed(db) do
    tables = for i <- 1..@num_tables, do: "bench_table_#{i}"

    Enum.each(tables, fn table ->
      IO.write("  Creating #{table}...")
      {:ok, _} = :cozodb.run(db, ":create #{table} {id: Int => value: String, counter: Int, data: String}")
      IO.puts(" seeding #{@seed_rows} rows...")
      seed_table(db, table)
    end)

    IO.puts("  All tables ready.\n")
    tables
  end

  defp seed_table(db, table) do
    batch_size = 1000
    for batch <- 0..(div(@seed_rows, batch_size) - 1) do
      start_id = batch * batch_size + 1
      end_id = (batch + 1) * batch_size

      rows = for id <- start_id..end_id do
        [id, "val_#{id}_#{:rand.uniform(100_000)}", :rand.uniform(1_000_000), random_string(@value_size)]
      end

      {:ok, _} = :cozodb.run(db, """
        ?[id, value, counter, data] <- $rows
        :put #{table} {id => value, counter, data}
      """, %{parameters: %{"rows" => rows}})
    end
  end

  defp build_join_query(tables) do
    bindings = tables
    |> Enum.with_index(1)
    |> Enum.map(fn {table, i} -> "    *#{table}{id, value: v#{i}, counter: c#{i}}" end)
    |> Enum.join(",\n")

    values = tables
    |> Enum.with_index(1)
    |> Enum.map(fn {_table, i} -> "v#{i}" end)
    |> Enum.join(", ")

    """
    ?[id, #{values}] :=
    #{bindings},
        id >= $lo, id <= $hi
    """
  end

  defp reader_loop(db, query, stop_flag, read_count, error_count,
                   lock_timeout_count, other_error_count, latency_sum, latency_max) do
    if :atomics.get(stop_flag, 1) == 1, do: :ok, else: do_reader_loop(
      db, query, stop_flag, read_count, error_count,
      lock_timeout_count, other_error_count, latency_sum, latency_max
    )
  end

  defp do_reader_loop(db, query, stop_flag, read_count, error_count,
                      lock_timeout_count, other_error_count, latency_sum, latency_max) do
    lo = :rand.uniform(@seed_rows - @join_range)
    hi = lo + @join_range

    {elapsed_us, result} = :timer.tc(fn ->
      :cozodb.run(db, query, %{
        parameters: %{"lo" => lo, "hi" => hi},
        read_only: true
      })
    end)

    case result do
      {:ok, _} ->
        :counters.add(read_count, 1, 1)
        :counters.add(latency_sum, 1, elapsed_us)
        # Update max atomically (best-effort)
        cur_max = :atomics.get(latency_max, 1)
        if elapsed_us > cur_max, do: :atomics.put(latency_max, 1, elapsed_us)

      {:error, %{message: msg}} when is_binary(msg) ->
        :counters.add(error_count, 1, 1)
        if String.contains?(msg, "Timeout waiting to lock key") do
          :counters.add(lock_timeout_count, 1, 1)
        else
          :counters.add(other_error_count, 1, 1)
        end

      {:error, _} ->
        :counters.add(error_count, 1, 1)
        :counters.add(other_error_count, 1, 1)
    end

    reader_loop(db, query, stop_flag, read_count, error_count,
                lock_timeout_count, other_error_count, latency_sum, latency_max)
  end

  defp writer_loop(db, tables, stop_flag, write_count, error_count,
                   lock_timeout_count, other_error_count) do
    if :atomics.get(stop_flag, 1) == 1, do: :ok, else: do_writer_loop(
      db, tables, stop_flag, write_count, error_count,
      lock_timeout_count, other_error_count
    )
  end

  defp do_writer_loop(db, tables, stop_flag, write_count, error_count,
                      lock_timeout_count, other_error_count) do
    table = Enum.random(tables)
    id = :rand.uniform(@seed_rows)

    query = """
    ?[id, value, counter, data] <- [[$id, $value, $counter, $data]]
    :put #{table} {id => value, counter, data}
    """

    params = %{
      "id" => id,
      "value" => "upd_#{System.monotonic_time()}",
      "counter" => :rand.uniform(1_000_000),
      "data" => random_string(@value_size)
    }

    case :cozodb.run(db, query, %{parameters: params}) do
      {:ok, _} ->
        :counters.add(write_count, 1, 1)

      {:error, %{message: msg}} when is_binary(msg) ->
        :counters.add(error_count, 1, 1)
        if String.contains?(msg, "Timeout waiting to lock key") do
          :counters.add(lock_timeout_count, 1, 1)
        else
          :counters.add(other_error_count, 1, 1)
        end

      {:error, _} ->
        :counters.add(error_count, 1, 1)
        :counters.add(other_error_count, 1, 1)
    end

    writer_loop(db, tables, stop_flag, write_count, error_count,
                lock_timeout_count, other_error_count)
  end

  defp metrics_loop(read_count, write_count, error_count, lock_timeout_count,
                    other_error_count, latency_sum, latency_max, stop_flag) do
    prev_reads = :counters.get(read_count, 1)
    prev_writes = :counters.get(write_count, 1)
    prev_lock = :counters.get(lock_timeout_count, 1)
    prev_other = :counters.get(other_error_count, 1)
    prev_lat_sum = :counters.get(latency_sum, 1)

    Process.sleep(@metrics_interval_ms)

    if :atomics.get(stop_flag, 1) == 1 do
      :ok
    else
      cur_reads = :counters.get(read_count, 1)
      cur_writes = :counters.get(write_count, 1)
      cur_lock = :counters.get(lock_timeout_count, 1)
      cur_other = :counters.get(other_error_count, 1)
      cur_lat_sum = :counters.get(latency_sum, 1)
      cur_max = :atomics.get(latency_max, 1)

      delta_reads = cur_reads - prev_reads
      delta_writes = cur_writes - prev_writes
      delta_lock = cur_lock - prev_lock
      delta_other = cur_other - prev_other
      delta_lat = cur_lat_sum - prev_lat_sum

      avg_ms = if delta_reads > 0, do: Float.round(delta_lat / delta_reads / 1000, 1), else: 0.0
      max_ms = Float.round(cur_max / 1000, 1)

      # Reset max for next interval
      :atomics.put(latency_max, 1, 0)

      elapsed = div(:counters.get(read_count, 1) + :counters.get(write_count, 1), 1)
      time_str = "#{div(cur_reads + cur_writes, 1)}"

      IO.puts(
        String.pad_trailing("#{cur_reads + cur_writes}", 8) <>
        String.pad_trailing("#{delta_reads}", 10) <>
        String.pad_trailing("#{delta_writes}", 10) <>
        String.pad_trailing("#{delta_lock}", 12) <>
        String.pad_trailing("#{delta_other}", 10) <>
        String.pad_trailing("#{avg_ms}", 12) <>
        "#{max_ms}"
      )

      metrics_loop(read_count, write_count, error_count, lock_timeout_count,
                   other_error_count, latency_sum, latency_max, stop_flag)
    end
  end

  defp random_string(size) do
    :crypto.strong_rand_bytes(size) |> Base.encode64() |> binary_part(0, size)
  end
end

# Parse duration from args
duration = case System.argv() do
  [d | _] -> String.to_integer(d)
  _ -> 600
end

JoinStressTest.run(duration)
