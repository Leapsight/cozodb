#!/bin/bash
# Script to build and run the Docker benchmark test
# Tests that jemalloc's disable_initial_exec_tls fix works in Docker

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "=========================================="
echo "CozoDB Docker Benchmark Test"
echo "=========================================="
echo ""
echo "This test verifies that the jemalloc TLS fix works in Docker."
echo "If you see 'cannot allocate memory in static TLS block' errors,"
echo "the fix is not working correctly."
echo ""

# Build the Docker image from project root (needed for COPY context)
echo "Building Docker image..."
cd "$PROJECT_ROOT"
docker build -f benchmark/Dockerfile -t cozodb-benchmark-test .

echo ""
echo "=========================================="
echo "Running benchmark test in Docker..."
echo "=========================================="
echo ""

# Run the benchmark
# The default CMD runs a quick 30-second test
# Note: COZODB_JEMALLOC_BACKGROUND_THREAD is intentionally NOT set (defaults to false)
# to prevent SIGSEGV in container environments.
docker run --rm \
    -e COZODB_JEMALLOC_DIRTY_DECAY_MS=1000 \
    -e COZODB_JEMALLOC_MUZZY_DECAY_MS=1000 \
    cozodb-benchmark-test "$@"

echo ""
echo "=========================================="
echo "Test completed successfully!"
echo "The jemalloc TLS fix is working correctly."
echo "=========================================="
