defmodule CozodbBenchmark do
  @moduledoc """
  Sustained load benchmark for CozoDB with configurable duration,
  workload patterns, and comprehensive metrics collection.
  """

  require Logger

  defmodule Config do
    @moduledoc "Benchmark configuration"
    defstruct [
      # Duration in seconds (main load phase)
      duration_seconds: 600,
      # Ramp-down duration in seconds (gradual worker reduction)
      rampdown_seconds: 60,
      # Number of concurrent workers
      num_workers: 100,
      # Number of tables to use
      num_tables: 10,
      # Rows to seed per table
      seed_rows: 10_000,
      # Size of value field in bytes (for generating large data)
      value_size: 1024,
      # Workload type: :read_only | :write_only | :mixed_50_50 | :mixed_80_20_wr
      workload: :mixed_50_50,
      # Database engine: :rocksdb | :sqlite | :mem
      engine: :rocksdb,
      # Database path
      db_path: "/tmp/cozodb_benchmark_sustained",
      # Metrics collection interval in milliseconds
      metrics_interval_ms: 1000,
      # Report output directory
      report_dir: "./benchmark_reports",
      # Track which allocator the NIF is using (for reporting purposes)
      # This is informational - actual allocator is determined at NIF compile time
      allocator: :unknown,

      # === Erlang VM Configuration ===
      # Number of dirty IO schedulers (from +SDio flag)
      dirty_io_schedulers: nil,
      # Number of regular schedulers
      schedulers: nil,
      # Number of dirty CPU schedulers
      dirty_cpu_schedulers: nil,

      # === jemalloc Configuration ===
      # These are read from environment variables at startup
      # Dirty page decay time in ms (default: 1000)
      jemalloc_dirty_decay_ms: 1000,
      # Muzzy page decay time in ms (default: 1000)
      jemalloc_muzzy_decay_ms: 1000,
      # Background thread enabled (default: true)
      jemalloc_background_thread: true,
      # Number of arenas (default: auto, typically 4*ncpus)
      jemalloc_narenas: nil,

      # === RocksDB Configuration ===
      # Note: RocksDB options are configured via an 'options' file in the db_path directory
      # See: https://github.com/facebook/rocksdb/wiki/RocksDB-Options-File
      rocksdb_options_file: nil
    ]
  end

  defmodule Metrics do
    @moduledoc "Metrics collection state"
    defstruct [
      start_time: nil,
      time_series: [],
      total_ops: 0,
      total_reads: 0,
      total_writes: 0,
      total_errors: 0,
      errors_by_type: %{},
      latencies: [],
      current_bucket: nil,
      prev_scheduler_wall_time: nil
    ]
  end

  defmodule TimeBucket do
    @moduledoc "Metrics for a single time bucket"
    defstruct [
      timestamp: nil,
      phase: :load,  # :load | :rampdown | :cooldown
      active_workers: 0,
      ops: 0,
      reads: 0,
      writes: 0,
      errors: 0,
      errors_by_type: %{},
      latencies: [],
      beam_memory: 0,    # BEAM/Erlang managed memory
      os_memory: 0,      # OS process memory (includes NIF allocations like RocksDB)
      cpu_percent: 0.0,
      scheduler_utilization: 0.0,
      # NIF memory stats from cozodb:memory_stats()
      nif_stats: %{}
    ]
  end

  @doc """
  Run the sustained load benchmark with the given configuration.
  """
  def run(opts \\ []) do
    config = struct(Config, opts)

    # Detect VM and runtime configuration
    config = detect_vm_config(config)
    config = detect_jemalloc_config(config)

    Logger.info("Starting sustained load benchmark")
    Logger.info("Configuration: #{inspect(config)}")

    # Enable scheduler wall time for CPU tracking
    :erlang.system_flag(:scheduler_wall_time, true)

    # Ensure report directory exists
    File.mkdir_p!(config.report_dir)

    # Clean up and prepare database
    File.rm_rf!(config.db_path)
    File.mkdir_p!(Path.dirname(config.db_path))

    # Open database (ResourceArc-based internally, lock-free)
    {:ok, db} = open_db(config)
    Logger.info("Database opened: #{config.engine} at #{config.db_path}")

    # Detect allocator from NIF stats and check for RocksDB options file
    config = detect_allocator(config)
    config = detect_rocksdb_options(config)
    Logger.info("NIF allocator: #{config.allocator}")

    # Create tables and seed data
    tables = create_and_seed_tables(db, config)
    Logger.info("Created and seeded #{length(tables)} tables with #{config.seed_rows} rows each")

    # Start metrics collector
    metrics_pid = start_metrics_collector(config)

    # Run the benchmark with ramp-down
    Logger.info("Starting #{config.num_workers} workers for #{config.duration_seconds}s + #{config.rampdown_seconds}s rampdown")
    run_benchmark_with_rampdown(db, tables, config, metrics_pid)

    # Collect final metrics
    metrics = get_final_metrics(metrics_pid)

    # Generate report
    report_path = generate_report(metrics, config)
    Logger.info("Report generated: #{report_path}")

    # Cleanup - resource is automatically closed when garbage collected
    :erlang.system_flag(:scheduler_wall_time, false)
    Logger.info("Benchmark complete!")

    {:ok, report_path, metrics}
  end

  # Open database using the standard API (ResourceArc-based internally)
  defp open_db(%Config{engine: engine, db_path: path}) do
    case engine do
      :mem -> :cozodb.open(:mem, "/tmp/mem_db")
      :sqlite -> :cozodb.open(:sqlite, path <> ".sqlite")
      :rocksdb -> :cozodb.open(:rocksdb, path)
    end
  end

  defp detect_allocator(config) do
    case get_nif_memory_stats() do
      {:ok, stats} ->
        allocator = Map.get(stats, "allocator", "unknown") |> to_string() |> String.to_atom()
        %{config | allocator: allocator}
      _ ->
        config
    end
  end

  defp detect_vm_config(config) do
    # Get scheduler info from :erlang.system_info
    schedulers = :erlang.system_info(:schedulers)
    dirty_cpu_schedulers = :erlang.system_info(:dirty_cpu_schedulers)
    dirty_io_schedulers = :erlang.system_info(:dirty_io_schedulers)

    %{config |
      schedulers: schedulers,
      dirty_cpu_schedulers: dirty_cpu_schedulers,
      dirty_io_schedulers: dirty_io_schedulers
    }
  end

  defp detect_jemalloc_config(config) do
    # Read jemalloc configuration from environment variables
    # These match the COZODB_JEMALLOC_* env vars used by the NIF
    dirty_decay = parse_env_int("COZODB_JEMALLOC_DIRTY_DECAY_MS", 1000)
    muzzy_decay = parse_env_int("COZODB_JEMALLOC_MUZZY_DECAY_MS", 1000)
    bg_thread = parse_env_bool("COZODB_JEMALLOC_BACKGROUND_THREAD", true)
    narenas = parse_env_int_optional("COZODB_JEMALLOC_NARENAS")

    %{config |
      jemalloc_dirty_decay_ms: dirty_decay,
      jemalloc_muzzy_decay_ms: muzzy_decay,
      jemalloc_background_thread: bg_thread,
      jemalloc_narenas: narenas
    }
  end

  defp detect_rocksdb_options(config) do
    # Check if a RocksDB options file exists
    options_path = Path.join(config.db_path, "options")
    if File.exists?(options_path) do
      %{config | rocksdb_options_file: options_path}
    else
      config
    end
  end

  defp parse_env_int(var, default) do
    case System.get_env(var) do
      nil -> default
      val ->
        case Integer.parse(val) do
          {n, _} -> n
          :error -> default
        end
    end
  end

  defp parse_env_int_optional(var) do
    case System.get_env(var) do
      nil -> nil
      val ->
        case Integer.parse(val) do
          {n, _} -> n
          :error -> nil
        end
    end
  end

  defp parse_env_bool(var, default) do
    case System.get_env(var) do
      nil -> default
      "false" -> false
      "0" -> false
      _ -> true
    end
  end

  defp get_nif_memory_stats do
    try do
      :cozodb.memory_stats()
    rescue
      _ -> {:error, :not_available}
    catch
      _, _ -> {:error, :not_available}
    end
  end

  defp create_and_seed_tables(db, config) do
    tables =
      for i <- 1..config.num_tables do
        "bench_table_#{i}"
      end

    # Create tables
    Enum.each(tables, fn table ->
      query = ":create #{table} {id: Int => value: String, counter: Int, data: String}"
      {:ok, _} = :cozodb.run(db, query)
    end)

    # Seed tables with data
    Enum.each(tables, fn table ->
      seed_table(db, table, config.seed_rows, config.value_size)
    end)

    tables
  end

  defp seed_table(db, table, num_rows, value_size) do
    # Seed in batches of 1000
    batch_size = 1000
    num_full_batches = div(num_rows, batch_size)
    remaining = rem(num_rows, batch_size)

    # Process full batches
    if num_full_batches > 0 do
      for batch <- 0..(num_full_batches - 1) do
        start_id = batch * batch_size + 1
        end_id = (batch + 1) * batch_size

        rows =
          for id <- start_id..end_id do
            value = generate_random_string(value_size)
            [id, "initial_#{id}", 0, value]
          end

        query = """
        ?[id, value, counter, data] <- $rows
        :put #{table} {id => value, counter, data}
        """

        {:ok, _} = :cozodb.run(db, query, %{parameters: %{"rows" => rows}})
      end
    end

    # Process remaining rows
    if remaining > 0 do
      start_id = num_full_batches * batch_size + 1
      end_id = num_rows

      rows =
        for id <- start_id..end_id do
          value = generate_random_string(value_size)
          [id, "initial_#{id}", 0, value]
        end

      query = """
      ?[id, value, counter, data] <- $rows
      :put #{table} {id => value, counter, data}
      """

      {:ok, _} = :cozodb.run(db, query, %{parameters: %{"rows" => rows}})
    end
  end

  defp generate_random_string(size) do
    :crypto.strong_rand_bytes(size)
    |> Base.encode64()
    |> binary_part(0, size)
  end

  defp start_metrics_collector(config) do
    initial_swt = :erlang.statistics(:scheduler_wall_time)
    {beam_mem, os_mem} = get_memory_usage()
    nif_stats = get_nif_stats_map()

    {:ok, pid} = Agent.start_link(fn ->
      %Metrics{
        start_time: System.monotonic_time(:millisecond),
        prev_scheduler_wall_time: initial_swt,
        current_bucket: %TimeBucket{
          timestamp: System.monotonic_time(:millisecond),
          beam_memory: beam_mem,
          os_memory: os_mem,
          cpu_percent: 0.0,
          scheduler_utilization: 0.0,
          nif_stats: nif_stats
        }
      }
    end)

    # Start periodic bucket rotation
    spawn_link(fn -> metrics_rotation_loop(pid, config.metrics_interval_ms) end)

    pid
  end

  defp get_nif_stats_map do
    case get_nif_memory_stats() do
      {:ok, stats} -> stats
      _ -> %{}
    end
  end

  defp metrics_rotation_loop(metrics_pid, interval_ms) do
    Process.sleep(interval_ms)

    # Get current scheduler wall time for CPU calculation
    current_swt = :erlang.statistics(:scheduler_wall_time)
    {beam_mem, os_mem} = get_memory_usage()
    nif_stats = get_nif_stats_map()

    Agent.update(metrics_pid, fn metrics ->
      current = metrics.current_bucket
      current = %{current | beam_memory: beam_mem, os_memory: os_mem, nif_stats: nif_stats}

      # Calculate scheduler utilization (CPU usage)
      {cpu_percent, scheduler_util} =
        calculate_scheduler_utilization(metrics.prev_scheduler_wall_time, current_swt)

      current = %{current | cpu_percent: cpu_percent, scheduler_utilization: scheduler_util}

      {new_beam_mem, new_os_mem} = get_memory_usage()
      new_nif_stats = get_nif_stats_map()
      new_bucket = %TimeBucket{
        timestamp: System.monotonic_time(:millisecond),
        phase: current.phase,
        active_workers: current.active_workers,
        beam_memory: new_beam_mem,
        os_memory: new_os_mem,
        cpu_percent: 0.0,
        scheduler_utilization: 0.0,
        nif_stats: new_nif_stats
      }

      %{metrics |
        time_series: metrics.time_series ++ [current],
        current_bucket: new_bucket,
        prev_scheduler_wall_time: current_swt
      }
    end)

    metrics_rotation_loop(metrics_pid, interval_ms)
  catch
    :exit, _ -> :ok
  end

  defp calculate_scheduler_utilization(nil, _), do: {0.0, 0.0}
  defp calculate_scheduler_utilization(prev_swt, current_swt) do
    # Sort both by scheduler id to align them
    prev_sorted = Enum.sort_by(prev_swt, fn {id, _, _} -> id end)
    curr_sorted = Enum.sort_by(current_swt, fn {id, _, _} -> id end)

    # Calculate utilization for each scheduler
    utils = Enum.zip(prev_sorted, curr_sorted)
    |> Enum.map(fn {{_id, prev_active, prev_total}, {_id2, curr_active, curr_total}} ->
      active_diff = curr_active - prev_active
      total_diff = curr_total - prev_total
      if total_diff > 0, do: active_diff / total_diff, else: 0.0
    end)

    if length(utils) > 0 do
      avg_util = Enum.sum(utils) / length(utils)
      cpu_percent = avg_util * 100
      {Float.round(cpu_percent, 2), Float.round(avg_util, 4)}
    else
      {0.0, 0.0}
    end
  end

  defp get_memory_usage do
    beam_memory = :erlang.memory(:total)
    os_memory = get_os_process_memory()
    {beam_memory, os_memory}
  end

  defp get_os_process_memory do
    # Get the OS-level RSS (Resident Set Size) which includes NIF allocations
    # like RocksDB memory that the BEAM doesn't track
    case :os.type() do
      {:unix, :darwin} ->
        # macOS: use ps to get RSS in kilobytes
        get_rss_via_ps()

      {:unix, _} ->
        # Linux: read from /proc/self/statm or use ps
        get_rss_from_proc() || get_rss_via_ps()

      _ ->
        # Fallback to BEAM memory if OS-level tracking not available
        :erlang.memory(:total)
    end
  end

  defp get_rss_via_ps do
    pid = :os.getpid() |> to_string()
    case System.cmd("ps", ["-o", "rss=", "-p", pid], stderr_to_stdout: true) do
      {output, 0} ->
        case Integer.parse(String.trim(output)) do
          {rss_kb, _} -> rss_kb * 1024  # Convert KB to bytes
          :error -> :erlang.memory(:total)
        end
      _ ->
        :erlang.memory(:total)
    end
  end

  defp get_rss_from_proc do
    case File.read("/proc/self/statm") do
      {:ok, content} ->
        # statm format: size resident shared text lib data dt (all in pages)
        # We want the resident (RSS) field
        case String.split(content) do
          [_size, resident | _] ->
            case Integer.parse(resident) do
              {pages, _} ->
                # Page size is typically 4096 bytes
                page_size = get_page_size()
                pages * page_size
              :error -> nil
            end
          _ -> nil
        end
      _ -> nil
    end
  end

  defp get_page_size do
    case System.cmd("getconf", ["PAGE_SIZE"], stderr_to_stdout: true) do
      {output, 0} ->
        case Integer.parse(String.trim(output)) do
          {size, _} -> size
          :error -> 4096
        end
      _ -> 4096
    end
  end

  defp update_phase(metrics_pid, phase, active_workers) do
    Agent.update(metrics_pid, fn metrics ->
      bucket = metrics.current_bucket
      %{metrics | current_bucket: %{bucket | phase: phase, active_workers: active_workers}}
    end)
  end

  defp record_operation(metrics_pid, type, latency_us, result) do
    Agent.update(metrics_pid, fn metrics ->
      bucket = metrics.current_bucket

      {errors, errors_by_type} =
        case result do
          :ok ->
            {bucket.errors, bucket.errors_by_type}

          {:error, error_type} ->
            new_count = Map.get(bucket.errors_by_type, error_type, 0) + 1
            {bucket.errors + 1, Map.put(bucket.errors_by_type, error_type, new_count)}
        end

      {reads, writes} =
        case type do
          :read -> {bucket.reads + 1, bucket.writes}
          :write -> {bucket.reads, bucket.writes + 1}
        end

      updated_bucket = %{bucket |
        ops: bucket.ops + 1,
        reads: reads,
        writes: writes,
        errors: errors,
        errors_by_type: errors_by_type,
        latencies: [latency_us | bucket.latencies]
      }

      total_errors = if result == :ok, do: metrics.total_errors, else: metrics.total_errors + 1
      errors_by_type_total =
        case result do
          :ok -> metrics.errors_by_type
          {:error, et} ->
            Map.update(metrics.errors_by_type, et, 1, &(&1 + 1))
        end

      %{metrics |
        current_bucket: updated_bucket,
        total_ops: metrics.total_ops + 1,
        total_reads: metrics.total_reads + (if type == :read, do: 1, else: 0),
        total_writes: metrics.total_writes + (if type == :write, do: 1, else: 0),
        total_errors: total_errors,
        errors_by_type: errors_by_type_total,
        latencies: [latency_us | metrics.latencies]
      }
    end)
  end

  defp get_final_metrics(metrics_pid) do
    Agent.get(metrics_pid, fn metrics ->
      # Include the current bucket
      %{metrics | time_series: metrics.time_series ++ [metrics.current_bucket]}
    end)
  end

  defp run_benchmark_with_rampdown(db, tables, config, metrics_pid) do
    # Determine worker distribution based on workload
    {read_ratio, _write_ratio} =
      case config.workload do
        :read_only -> {1.0, 0.0}
        :write_only -> {0.0, 1.0}
        :mixed_50_50 -> {0.5, 0.5}
        :mixed_80_20_wr -> {0.2, 0.8}
      end

    # Split tables between readers and writers
    {read_tables, write_tables} =
      case config.workload do
        :read_only -> {tables, tables}
        :write_only -> {tables, tables}
        _ ->
          mid = div(length(tables), 2)
          Enum.split(tables, mid)
      end

    read_workers = round(config.num_workers * read_ratio)
    write_workers = config.num_workers - read_workers

    Logger.info("Worker distribution: #{read_workers} readers, #{write_workers} writers")
    Logger.info("Tables: #{length(read_tables)} for reads, #{length(write_tables)} for writes")

    # Create a coordinator process to manage workers
    coordinator = self()

    # Start all workers with a stop signal mechanism
    worker_refs =
      start_workers(db, read_tables, write_tables, read_workers, write_workers, metrics_pid, config, coordinator)

    total_workers = length(worker_refs)
    update_phase(metrics_pid, :load, total_workers)

    # === PHASE 1: Main load phase ===
    Logger.info("=== PHASE 1: Main load (#{config.duration_seconds}s) ===")
    Process.sleep(config.duration_seconds * 1000)

    # === PHASE 2: Ramp-down phase ===
    Logger.info("=== PHASE 2: Ramp-down (#{config.rampdown_seconds}s) ===")
    update_phase(metrics_pid, :rampdown, total_workers)

    # Calculate how many workers to stop per step
    rampdown_steps = 10
    step_duration_ms = div(config.rampdown_seconds * 1000, rampdown_steps)
    workers_per_step = max(1, div(total_workers, rampdown_steps))

    remaining_refs = rampdown_workers(worker_refs, rampdown_steps, step_duration_ms, workers_per_step, metrics_pid)

    # === PHASE 3: Cooldown phase (observe memory with no load) ===
    cooldown_seconds = 10
    Logger.info("=== PHASE 3: Cooldown (#{cooldown_seconds}s, 0 workers) ===")
    update_phase(metrics_pid, :cooldown, 0)

    # Stop any remaining workers
    Enum.each(remaining_refs, fn {pid, _ref} ->
      send(pid, :stop)
    end)

    # Wait for cooldown to observe memory behavior
    Process.sleep(cooldown_seconds * 1000)

    # Wait for all workers to finish
    Enum.each(worker_refs, fn {pid, ref} ->
      receive do
        {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
      after
        1000 -> :ok
      end
    end)

    :ok
  end

  defp start_workers(db, read_tables, write_tables, read_workers, write_workers, metrics_pid, config, _coordinator) do
    reader_refs =
      for _ <- 1..max(1, read_workers) do
        if read_workers > 0 do
          pid = spawn(fn -> stoppable_worker_loop(db, read_tables, :read, metrics_pid, config) end)
          ref = Process.monitor(pid)
          {pid, ref}
        end
      end
      |> Enum.filter(& &1)

    writer_refs =
      for _ <- 1..max(1, write_workers) do
        if write_workers > 0 do
          pid = spawn(fn -> stoppable_worker_loop(db, write_tables, :write, metrics_pid, config) end)
          ref = Process.monitor(pid)
          {pid, ref}
        end
      end
      |> Enum.filter(& &1)

    reader_refs ++ writer_refs
  end

  defp stoppable_worker_loop(db, tables, op_type, metrics_pid, config) do
    receive do
      :stop -> :ok
    after
      0 ->
        table = Enum.random(tables)

        {latency_us, result} =
          :timer.tc(fn ->
            case op_type do
              :read -> do_read(db, table, config)
              :write -> do_write(db, table, config)
            end
          end)

        record_operation(metrics_pid, op_type, latency_us, result)
        stoppable_worker_loop(db, tables, op_type, metrics_pid, config)
    end
  end

  defp rampdown_workers(worker_refs, 0, _step_duration_ms, _workers_per_step, _metrics_pid) do
    worker_refs
  end

  defp rampdown_workers(worker_refs, steps_remaining, step_duration_ms, workers_per_step, metrics_pid) do
    # Stop a batch of workers
    {to_stop, remaining} = Enum.split(worker_refs, workers_per_step)

    Enum.each(to_stop, fn {pid, _ref} ->
      send(pid, :stop)
    end)

    active_count = length(remaining)
    update_phase(metrics_pid, :rampdown, active_count)
    Logger.info("Ramp-down: #{active_count} workers remaining")

    Process.sleep(step_duration_ms)

    rampdown_workers(remaining, steps_remaining - 1, step_duration_ms, workers_per_step, metrics_pid)
  end

  defp do_read(db, table, config) do
    id = :rand.uniform(config.seed_rows)
    query = "?[id, value, counter, data] := *#{table}{id, value, counter, data}, id = $id"

    case :cozodb.run(db, query, %{parameters: %{"id" => id}, read_only: true}) do
      {:ok, _} -> :ok
      {:error, %{message: msg}} -> {:error, classify_error(msg)}
      {:error, reason} -> {:error, inspect(reason)}
    end
  end

  defp do_write(db, table, config) do
    id = :rand.uniform(config.seed_rows)
    value = generate_random_string(config.value_size)
    counter = :rand.uniform(1_000_000)

    query = """
    ?[id, value, counter, data] <- [[$id, $value, $counter, $data]]
    :put #{table} {id => value, counter, data}
    """

    params = %{
      "id" => id,
      "value" => "updated_#{System.monotonic_time()}",
      "counter" => counter,
      "data" => value
    }

    case :cozodb.run(db, query, %{parameters: params}) do
      {:ok, _} -> :ok
      {:error, %{message: msg}} -> {:error, classify_error(msg)}
      {:error, reason} -> {:error, inspect(reason)}
    end
  end

  defp classify_error(msg) when is_binary(msg) do
    cond do
      String.contains?(msg, "Timeout waiting to lock key") -> :lock_timeout
      String.contains?(msg, "Resource busy") -> :resource_busy
      String.contains?(msg, "database is locked") -> :database_locked
      String.contains?(msg, "timeout") -> :timeout
      String.contains?(msg, "try again") -> :try_again
      true -> :other
    end
  end

  defp classify_error(_), do: :unknown

  defp generate_report(metrics, config) do
    timestamp = DateTime.utc_now() |> DateTime.to_iso8601(:basic)
    report_file = Path.join(config.report_dir, "benchmark_report_#{timestamp}.html")

    html = generate_html_report(metrics, config)
    File.write!(report_file, html)

    # Also generate JSON data file
    json_file = Path.join(config.report_dir, "benchmark_data_#{timestamp}.json")
    json_data = generate_json_data(metrics, config)
    File.write!(json_file, Jason.encode!(json_data, pretty: true))

    report_file
  end

  defp generate_json_data(metrics, config) do
    duration_ms = List.last(metrics.time_series).timestamp - metrics.start_time
    duration_sec = duration_ms / 1000

    %{
      config: %{
        # Benchmark config
        duration_seconds: config.duration_seconds,
        rampdown_seconds: config.rampdown_seconds,
        num_workers: config.num_workers,
        num_tables: config.num_tables,
        seed_rows: config.seed_rows,
        value_size: config.value_size,
        workload: config.workload,
        engine: config.engine,
        db_path: config.db_path,
        # VM config
        schedulers: config.schedulers,
        dirty_cpu_schedulers: config.dirty_cpu_schedulers,
        dirty_io_schedulers: config.dirty_io_schedulers,
        # Allocator config
        allocator: config.allocator,
        jemalloc_dirty_decay_ms: config.jemalloc_dirty_decay_ms,
        jemalloc_muzzy_decay_ms: config.jemalloc_muzzy_decay_ms,
        jemalloc_background_thread: config.jemalloc_background_thread,
        jemalloc_narenas: config.jemalloc_narenas,
        # RocksDB config
        rocksdb_options_file: config.rocksdb_options_file
      },
      summary: %{
        total_ops: metrics.total_ops,
        total_reads: metrics.total_reads,
        total_writes: metrics.total_writes,
        total_errors: metrics.total_errors,
        errors_by_type: metrics.errors_by_type,
        throughput_ops_sec: metrics.total_ops / max(duration_sec, 0.001),
        duration_seconds: duration_sec
      },
      time_series: Enum.map(metrics.time_series, fn bucket ->
        # Extract jemalloc stats if available
        nif_allocated_mb = get_nif_stat_mb(bucket.nif_stats, "allocated")
        nif_resident_mb = get_nif_stat_mb(bucket.nif_stats, "resident")
        nif_mapped_mb = get_nif_stat_mb(bucket.nif_stats, "mapped")
        nif_retained_mb = get_nif_stat_mb(bucket.nif_stats, "retained")

        %{
          timestamp_ms: bucket.timestamp - metrics.start_time,
          phase: bucket.phase,
          active_workers: bucket.active_workers,
          ops: bucket.ops,
          reads: bucket.reads,
          writes: bucket.writes,
          errors: bucket.errors,
          errors_by_type: bucket.errors_by_type,
          beam_memory_mb: bucket.beam_memory / (1024 * 1024),
          os_memory_mb: bucket.os_memory / (1024 * 1024),
          cpu_percent: bucket.cpu_percent,
          scheduler_utilization: bucket.scheduler_utilization,
          avg_latency_us: safe_avg(bucket.latencies),
          p99_latency_us: safe_percentile(bucket.latencies, 99),
          # NIF/jemalloc stats (if available)
          nif_allocated_mb: nif_allocated_mb,
          nif_resident_mb: nif_resident_mb,
          nif_mapped_mb: nif_mapped_mb,
          nif_retained_mb: nif_retained_mb,
          nif_stats: bucket.nif_stats
        }
      end)
    }
  end

  defp get_nif_stat_mb(stats, key) when is_map(stats) do
    case Map.get(stats, key) do
      nil -> nil
      bytes when is_number(bytes) -> bytes / (1024 * 1024)
      _ -> nil
    end
  end
  defp get_nif_stat_mb(_, _), do: nil

  defp generate_html_report(metrics, config) do
    duration_ms = List.last(metrics.time_series).timestamp - metrics.start_time
    duration_sec = max(duration_ms / 1000, 0.001)

    sorted_latencies = Enum.sort(metrics.latencies)
    p50 = safe_percentile(sorted_latencies, 50)
    p90 = safe_percentile(sorted_latencies, 90)
    p95 = safe_percentile(sorted_latencies, 95)
    p99 = safe_percentile(sorted_latencies, 99)
    avg_latency = safe_avg(metrics.latencies)

    time_labels = metrics.time_series
      |> Enum.map(fn b -> Float.round((b.timestamp - metrics.start_time) / 1000, 1) end)
      |> Jason.encode!()

    ops_data = metrics.time_series |> Enum.map(& &1.ops) |> Jason.encode!()
    errors_data = metrics.time_series |> Enum.map(& &1.errors) |> Jason.encode!()
    beam_memory_data = metrics.time_series |> Enum.map(fn b -> Float.round(b.beam_memory / (1024 * 1024), 2) end) |> Jason.encode!()
    os_memory_data = metrics.time_series |> Enum.map(fn b -> Float.round(b.os_memory / (1024 * 1024), 2) end) |> Jason.encode!()
    cpu_data = metrics.time_series |> Enum.map(& &1.cpu_percent) |> Jason.encode!()
    workers_data = metrics.time_series |> Enum.map(& &1.active_workers) |> Jason.encode!()
    latency_data = metrics.time_series |> Enum.map(fn b -> Float.round(safe_avg(b.latencies) / 1000, 2) end) |> Jason.encode!()
    p99_latency_data = metrics.time_series |> Enum.map(fn b -> Float.round(safe_percentile(b.latencies, 99) / 1000, 2) end) |> Jason.encode!()

    # jemalloc stats (if available)
    has_jemalloc = config.allocator == :jemalloc
    jemalloc_allocated_data = metrics.time_series
      |> Enum.map(fn b -> get_nif_stat_mb(b.nif_stats, "allocated") || 0.0 end)
      |> Enum.map(fn v -> Float.round(v / 1, 2) end)
      |> Jason.encode!()
    jemalloc_resident_data = metrics.time_series
      |> Enum.map(fn b -> get_nif_stat_mb(b.nif_stats, "resident") || 0.0 end)
      |> Enum.map(fn v -> Float.round(v / 1, 2) end)
      |> Jason.encode!()
    jemalloc_retained_data = metrics.time_series
      |> Enum.map(fn b -> get_nif_stat_mb(b.nif_stats, "retained") || 0.0 end)
      |> Enum.map(fn v -> Float.round(v / 1, 2) end)
      |> Jason.encode!()

    # Find phase boundaries for annotations
    phases = metrics.time_series
      |> Enum.with_index()
      |> Enum.chunk_by(fn {b, _} -> b.phase end)
      |> Enum.map(fn chunk ->
        {first, first_idx} = hd(chunk)
        time_sec = Float.round((first.timestamp - metrics.start_time) / 1000, 1)
        {first.phase, time_sec, first_idx}
      end)

    # Phase annotations for future chart.js annotation plugin use
    _phase_annotations = phases
      |> Enum.map(fn {phase, time_sec, _idx} ->
        label = case phase do
          :load -> "Load Phase"
          :rampdown -> "Ramp-down"
          :cooldown -> "Cooldown"
          _ -> to_string(phase)
        end
        %{type: "line", xMin: time_sec, xMax: time_sec, borderColor: "#999", borderDash: [5, 5],
          label: %{content: label, enabled: true, position: "start"}}
      end)
      |> Jason.encode!()

    """
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>CozoDB Benchmark Report</title>
      <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
      <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation"></script>
      <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; padding: 20px; }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { color: #333; margin-bottom: 20px; }
        h2 { color: #555; margin: 20px 0 10px; }
        .card { background: white; border-radius: 8px; padding: 20px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }
        .metric { text-align: center; padding: 15px; background: #f8f9fa; border-radius: 6px; }
        .metric-value { font-size: 24px; font-weight: bold; color: #333; }
        .metric-label { font-size: 12px; color: #666; margin-top: 5px; }
        .error { color: #dc3545; }
        .chart-container { position: relative; height: 300px; margin: 20px 0; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #eee; }
        th { background: #f8f9fa; font-weight: 600; }
        .config-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; }
        .config-item { padding: 10px; background: #e9ecef; border-radius: 4px; }
        .config-item strong { display: block; font-size: 11px; color: #666; text-transform: uppercase; }
        .phase-legend { display: flex; gap: 20px; margin-top: 10px; font-size: 12px; }
        .phase-legend span { display: flex; align-items: center; gap: 5px; }
        .phase-dot { width: 12px; height: 12px; border-radius: 50%; }
        .phase-load { background: #4CAF50; }
        .phase-rampdown { background: #FF9800; }
        .phase-cooldown { background: #2196F3; }
      </style>
    </head>
    <body>
      <div class="container">
        <h1>CozoDB Sustained Load Benchmark Report</h1>
        <p style="color: #666; margin-bottom: 20px;">Generated: #{DateTime.utc_now() |> DateTime.to_string()}</p>

        <div class="card">
          <h2>Benchmark Configuration</h2>
          <div class="config-grid">
            <div class="config-item"><strong>Load Duration</strong> #{config.duration_seconds}s</div>
            <div class="config-item"><strong>Ramp-down</strong> #{config.rampdown_seconds}s</div>
            <div class="config-item"><strong>Workers</strong> #{config.num_workers}</div>
            <div class="config-item"><strong>Tables</strong> #{config.num_tables}</div>
            <div class="config-item"><strong>Rows/Table</strong> #{config.seed_rows}</div>
            <div class="config-item"><strong>Value Size</strong> #{config.value_size} bytes</div>
            <div class="config-item"><strong>Workload</strong> #{config.workload}</div>
            <div class="config-item"><strong>Engine</strong> #{config.engine}</div>
          </div>
          <div class="phase-legend">
            <span><div class="phase-dot phase-load"></div> Load Phase</span>
            <span><div class="phase-dot phase-rampdown"></div> Ramp-down</span>
            <span><div class="phase-dot phase-cooldown"></div> Cooldown</span>
          </div>
        </div>

        <div class="card">
          <h2>Erlang VM Configuration</h2>
          <div class="config-grid">
            <div class="config-item"><strong>Schedulers</strong> #{config.schedulers || "auto"}</div>
            <div class="config-item"><strong>Dirty CPU Schedulers</strong> #{config.dirty_cpu_schedulers || "auto"}</div>
            <div class="config-item"><strong>Dirty IO Schedulers</strong> #{config.dirty_io_schedulers || "auto"}</div>
          </div>
        </div>

        <div class="card">
          <h2>Memory Allocator Configuration</h2>
          <div class="config-grid">
            <div class="config-item"><strong>Allocator</strong> #{config.allocator}</div>
            #{if config.allocator == :jemalloc do
              """
              <div class="config-item"><strong>Dirty Decay (ms)</strong> #{config.jemalloc_dirty_decay_ms}</div>
              <div class="config-item"><strong>Muzzy Decay (ms)</strong> #{config.jemalloc_muzzy_decay_ms}</div>
              <div class="config-item"><strong>Background Thread</strong> #{config.jemalloc_background_thread}</div>
              <div class="config-item"><strong>Arenas</strong> #{config.jemalloc_narenas || "auto"}</div>
              """
            else
              ""
            end}
          </div>
          #{if config.allocator == :jemalloc do
            """
            <p style="color: #666; font-size: 11px; margin-top: 10px;">
              Configure via: COZODB_JEMALLOC_DIRTY_DECAY_MS, COZODB_JEMALLOC_MUZZY_DECAY_MS, COZODB_JEMALLOC_BACKGROUND_THREAD, COZODB_JEMALLOC_NARENAS
            </p>
            """
          else
            ""
          end}
        </div>

        #{if config.engine == :rocksdb do
          """
          <div class="card">
            <h2>RocksDB Configuration</h2>
            <div class="config-grid">
              <div class="config-item"><strong>DB Path</strong> #{config.db_path}</div>
              <div class="config-item"><strong>Options File</strong> #{config.rocksdb_options_file || "default"}</div>
            </div>
            <p style="color: #666; font-size: 11px; margin-top: 10px;">
              RocksDB options can be customized by placing an 'options' file in the database directory.
              See: <a href="https://github.com/facebook/rocksdb/wiki/RocksDB-Options-File" target="_blank">RocksDB Options File</a>
            </p>
          </div>
          """
        else
          ""
        end}

        <div class="card">
          <h2>Summary</h2>
          <div class="grid">
            <div class="metric">
              <div class="metric-value">#{format_number(metrics.total_ops)}</div>
              <div class="metric-label">Total Operations</div>
            </div>
            <div class="metric">
              <div class="metric-value">#{format_number(Float.round(metrics.total_ops / duration_sec, 0))}</div>
              <div class="metric-label">Ops/Second (avg)</div>
            </div>
            <div class="metric">
              <div class="metric-value">#{format_number(metrics.total_reads)}</div>
              <div class="metric-label">Total Reads</div>
            </div>
            <div class="metric">
              <div class="metric-value">#{format_number(metrics.total_writes)}</div>
              <div class="metric-label">Total Writes</div>
            </div>
            <div class="metric #{if metrics.total_errors > 0, do: "error", else: ""}">
              <div class="metric-value">#{format_number(metrics.total_errors)}</div>
              <div class="metric-label">Total Errors</div>
            </div>
            <div class="metric">
              <div class="metric-value">#{Float.round(duration_sec, 1)}s</div>
              <div class="metric-label">Total Duration</div>
            </div>
          </div>
        </div>

        <div class="card">
          <h2>Latency Statistics</h2>
          <div class="grid">
            <div class="metric">
              <div class="metric-value">#{Float.round(avg_latency / 1000, 2)} ms</div>
              <div class="metric-label">Average</div>
            </div>
            <div class="metric">
              <div class="metric-value">#{Float.round(p50 / 1000, 2)} ms</div>
              <div class="metric-label">P50</div>
            </div>
            <div class="metric">
              <div class="metric-value">#{Float.round(p90 / 1000, 2)} ms</div>
              <div class="metric-label">P90</div>
            </div>
            <div class="metric">
              <div class="metric-value">#{Float.round(p95 / 1000, 2)} ms</div>
              <div class="metric-label">P95</div>
            </div>
            <div class="metric">
              <div class="metric-value">#{Float.round(p99 / 1000, 2)} ms</div>
              <div class="metric-label">P99</div>
            </div>
          </div>
        </div>

        #{if metrics.total_errors > 0 do
          """
          <div class="card">
            <h2>Errors by Type</h2>
            <table>
              <tr><th>Error Type</th><th>Count</th><th>Percentage</th></tr>
              #{Enum.map(metrics.errors_by_type, fn {type, count} ->
                "<tr><td>#{type}</td><td>#{format_number(count)}</td><td>#{Float.round(count / metrics.total_errors * 100, 2)}%</td></tr>"
              end) |> Enum.join("\n")}
            </table>
          </div>
          """
        else
          ""
        end}

        <div class="card">
          <h2>Active Workers Over Time</h2>
          <div class="chart-container">
            <canvas id="workersChart"></canvas>
          </div>
        </div>

        <div class="card">
          <h2>Throughput Over Time</h2>
          <div class="chart-container">
            <canvas id="throughputChart"></canvas>
          </div>
        </div>

        <div class="card">
          <h2>CPU Usage Over Time</h2>
          <div class="chart-container">
            <canvas id="cpuChart"></canvas>
          </div>
        </div>

        <div class="card">
          <h2>Memory Usage Over Time</h2>
          <p style="color: #666; font-size: 12px; margin-bottom: 10px;">
            <strong>OS Process RSS</strong> = Total process memory (includes RocksDB/NIF allocations)<br/>
            <strong>BEAM Memory</strong> = Erlang VM managed memory only<br/>
            The gap between them shows NIF/external memory (like RocksDB). Watch for both decreasing during ramp-down.
          </p>
          <div class="chart-container">
            <canvas id="memoryChart"></canvas>
          </div>
        </div>

        #{if has_jemalloc do
          """
          <div class="card">
            <h2>jemalloc Memory Stats Over Time</h2>
            <p style="color: #666; font-size: 12px; margin-bottom: 10px;">
              <strong>Allocated</strong> = Total bytes actively allocated by the application<br/>
              <strong>Resident</strong> = Physical memory currently held by jemalloc<br/>
              <strong>Retained</strong> = Memory retained by jemalloc but not in use (fragmentation/cache)<br/>
              High Retained-Allocated gap indicates fragmentation or aggressive caching by jemalloc.
            </p>
            <div class="chart-container">
              <canvas id="jemallocChart"></canvas>
            </div>
          </div>
          """
        else
          ""
        end}

        <div class="card">
          <h2>Errors Over Time</h2>
          <div class="chart-container">
            <canvas id="errorsChart"></canvas>
          </div>
        </div>

        <div class="card">
          <h2>Latency Over Time</h2>
          <div class="chart-container">
            <canvas id="latencyChart"></canvas>
          </div>
        </div>
      </div>

      <script>
        const timeLabels = #{time_labels};
        const opsData = #{ops_data};
        const errorsData = #{errors_data};
        const beamMemoryData = #{beam_memory_data};
        const osMemoryData = #{os_memory_data};
        const cpuData = #{cpu_data};
        const workersData = #{workers_data};
        const latencyData = #{latency_data};
        const p99LatencyData = #{p99_latency_data};
        const hasJemalloc = #{has_jemalloc};
        const jemallocAllocatedData = #{jemalloc_allocated_data};
        const jemallocResidentData = #{jemalloc_resident_data};
        const jemallocRetainedData = #{jemalloc_retained_data};

        new Chart(document.getElementById('workersChart'), {
          type: 'line',
          data: {
            labels: timeLabels,
            datasets: [{
              label: 'Active Workers',
              data: workersData,
              borderColor: '#673AB7',
              backgroundColor: 'rgba(103, 58, 183, 0.1)',
              fill: true,
              stepped: true
            }]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
              x: { title: { display: true, text: 'Time (seconds)' } },
              y: { title: { display: true, text: 'Workers' }, beginAtZero: true }
            }
          }
        });

        new Chart(document.getElementById('throughputChart'), {
          type: 'line',
          data: {
            labels: timeLabels,
            datasets: [{
              label: 'Operations per second',
              data: opsData,
              borderColor: '#4CAF50',
              backgroundColor: 'rgba(76, 175, 80, 0.1)',
              fill: true,
              tension: 0.3
            }]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
              x: { title: { display: true, text: 'Time (seconds)' } },
              y: { title: { display: true, text: 'Ops/sec' }, beginAtZero: true }
            }
          }
        });

        new Chart(document.getElementById('cpuChart'), {
          type: 'line',
          data: {
            labels: timeLabels,
            datasets: [{
              label: 'CPU Usage (%)',
              data: cpuData,
              borderColor: '#E91E63',
              backgroundColor: 'rgba(233, 30, 99, 0.1)',
              fill: true,
              tension: 0.3
            }]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
              x: { title: { display: true, text: 'Time (seconds)' } },
              y: { title: { display: true, text: 'CPU %' }, beginAtZero: true, max: 100 }
            }
          }
        });

        new Chart(document.getElementById('errorsChart'), {
          type: 'line',
          data: {
            labels: timeLabels,
            datasets: [{
              label: 'Errors per second',
              data: errorsData,
              borderColor: '#f44336',
              backgroundColor: 'rgba(244, 67, 54, 0.1)',
              fill: true,
              tension: 0.3
            }]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
              x: { title: { display: true, text: 'Time (seconds)' } },
              y: { title: { display: true, text: 'Errors' }, beginAtZero: true }
            }
          }
        });

        new Chart(document.getElementById('memoryChart'), {
          type: 'line',
          data: {
            labels: timeLabels,
            datasets: [
              {
                label: 'OS Process RSS (MB)',
                data: osMemoryData,
                borderColor: '#F44336',
                backgroundColor: 'rgba(244, 67, 54, 0.1)',
                fill: true,
                tension: 0.3
              },
              {
                label: 'BEAM Memory (MB)',
                data: beamMemoryData,
                borderColor: '#2196F3',
                backgroundColor: 'rgba(33, 150, 243, 0.1)',
                fill: true,
                tension: 0.3
              }
            ]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
              x: { title: { display: true, text: 'Time (seconds)' } },
              y: { title: { display: true, text: 'Memory (MB)' }, beginAtZero: false }
            },
            plugins: {
              tooltip: {
                callbacks: {
                  afterBody: function(context) {
                    const idx = context[0].dataIndex;
                    const os = osMemoryData[idx];
                    const beam = beamMemoryData[idx];
                    const nif = (os - beam).toFixed(2);
                    return 'NIF/External (estimated): ' + nif + ' MB';
                  }
                }
              }
            }
          }
        });

        new Chart(document.getElementById('latencyChart'), {
          type: 'line',
          data: {
            labels: timeLabels,
            datasets: [
              {
                label: 'Avg Latency (ms)',
                data: latencyData,
                borderColor: '#9C27B0',
                tension: 0.3
              },
              {
                label: 'P99 Latency (ms)',
                data: p99LatencyData,
                borderColor: '#FF9800',
                tension: 0.3
              }
            ]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
              x: { title: { display: true, text: 'Time (seconds)' } },
              y: { title: { display: true, text: 'Latency (ms)' }, beginAtZero: true }
            }
          }
        });

        // jemalloc chart (only if jemalloc is the allocator)
        if (hasJemalloc && document.getElementById('jemallocChart')) {
          new Chart(document.getElementById('jemallocChart'), {
            type: 'line',
            data: {
              labels: timeLabels,
              datasets: [
                {
                  label: 'Allocated (MB)',
                  data: jemallocAllocatedData,
                  borderColor: '#4CAF50',
                  backgroundColor: 'rgba(76, 175, 80, 0.1)',
                  fill: true,
                  tension: 0.3
                },
                {
                  label: 'Resident (MB)',
                  data: jemallocResidentData,
                  borderColor: '#2196F3',
                  tension: 0.3
                },
                {
                  label: 'Retained (MB)',
                  data: jemallocRetainedData,
                  borderColor: '#FF9800',
                  tension: 0.3
                }
              ]
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              scales: {
                x: { title: { display: true, text: 'Time (seconds)' } },
                y: { title: { display: true, text: 'Memory (MB)' }, beginAtZero: true }
              },
              plugins: {
                tooltip: {
                  callbacks: {
                    afterBody: function(context) {
                      const idx = context[0].dataIndex;
                      const allocated = jemallocAllocatedData[idx];
                      const retained = jemallocRetainedData[idx];
                      const fragmentation = ((retained - allocated) / retained * 100).toFixed(1);
                      return 'Fragmentation/Cache: ' + fragmentation + '%';
                    }
                  }
                }
              }
            }
          });
        }
      </script>
    </body>
    </html>
    """
  end

  defp safe_avg([]), do: 0
  defp safe_avg(list), do: Enum.sum(list) / length(list)

  defp safe_percentile([], _), do: 0
  defp safe_percentile(list, p) do
    sorted = if list == Enum.sort(list), do: list, else: Enum.sort(list)
    index = max(0, min(length(sorted) - 1, round(p / 100 * length(sorted))))
    Enum.at(sorted, index, 0)
  end

  defp format_number(n) when is_float(n), do: format_number(round(n))
  defp format_number(n) when n >= 1_000_000, do: "#{Float.round(n / 1_000_000, 1)}M"
  defp format_number(n) when n >= 1_000, do: "#{Float.round(n / 1_000, 1)}K"
  defp format_number(n), do: "#{n}"
end
