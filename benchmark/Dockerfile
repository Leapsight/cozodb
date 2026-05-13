# Dockerfile for testing cozodb benchmark in Docker
#
# Simulates a realistic Elixir application that depends on cozodb.
# The NIF is compiled automatically via rebar3 pre_hooks when mix compiles
# the cozodb dependency — no manual cargo-build or .so copying needed.

FROM elixir:1.18-otp-27

# Build args for compile-time feature selection
# Set COZODB_IO_URING=true to enable io_uring (not recommended for containers)
ARG COZODB_IO_URING=false

# Install build dependencies required by cozodb's Rust NIF and RocksDB
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    curl \
    clang \
    libclang-dev \
    pkg-config \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Conditionally install liburing-dev when io_uring is enabled
RUN if [ "$COZODB_IO_URING" = "true" ]; then \
        apt-get update && apt-get install -y liburing-dev && rm -rf /var/lib/apt/lists/*; \
    fi

# Install Rust (required to compile cozodb's NIF)
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# C++20 support required by newer RocksDB headers
ENV CXXFLAGS="-std=c++20"

# CRITICAL: Compile-time jemalloc configuration for container compatibility.
# These env vars are read by tikv-jemalloc-sys's build.rs during cargo build
# and baked directly into the jemalloc binary — no runtime MALLOC_CONF needed.
# See: https://github.com/tikv/jemallocator/blob/master/jemalloc-sys/README.md
ENV JEMALLOC_SYS_WITH_MALLOC_CONF="background_thread:false,dirty_decay_ms:1000,muzzy_decay_ms:1000"

# Set page size for jemalloc to match Linux x86_64 (4KB = 2^12).
# Prevents "page size mismatch" errors in Docker containers.
# macOS Apple Silicon uses 16KB (LG_PAGE=14); set accordingly if cross-compiling.
ENV JEMALLOC_SYS_WITH_LG_PAGE=12

# Install hex and rebar3 (needed to compile Elixir/Erlang deps)
RUN mix local.hex --force && \
    mix local.rebar --force

# Copy the entire cozodb project
WORKDIR /app
COPY . .

# Build the benchmark app, which depends on cozodb via path dependency.
# mix compile triggers the full dependency chain automatically:
#   mix compile
#     -> resolves {:cozodb, path: "../.."} as rebar3 project
#     -> rebar3 compile
#       -> pre_hook: make cargo-build (compiles Rust NIF + RocksDB)
#       -> erlang compilation
#     -> benchmark app compilation
WORKDIR /app/benchmark/cozodb_benchmark
RUN mix deps.get && mix compile

# Create directory for benchmark data
RUN mkdir -p /tmp/cozodb_benchmark /app/benchmark_reports

# Runtime jemalloc tuning (compile-time defaults are already baked in via
# JEMALLOC_SYS_WITH_MALLOC_CONF above; these can override at runtime if needed)
ENV COZODB_JEMALLOC_DIRTY_DECAY_MS=1000
ENV COZODB_JEMALLOC_MUZZY_DECAY_MS=1000
# background_thread defaults to false (safe for containers).
# Do NOT set COZODB_JEMALLOC_BACKGROUND_THREAD=true in containers —
# it causes SIGSEGV due to signal handling conflicts with the BEAM VM.

# Default command: run a quick benchmark to verify everything works
CMD ["mix", "run", "lib/benchmark_runner.exs", \
     "duration=30", \
     "rampdown=10", \
     "workers=10", \
     "tables=2", \
     "rows=1000", \
     "value_size=256", \
     "workload=mixed_50_50", \
     "engine=rocksdb", \
     "db_path=/tmp/cozodb_benchmark/test_db", \
     "report_dir=/app/benchmark_reports"]
