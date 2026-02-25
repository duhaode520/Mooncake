#!/usr/bin/env bash
#
# Mooncake Store: Embedded vs Standalone Client Mode Benchmark Runner
#
# Usage:
#   ./run_client_mode_bench.sh [options]
#
# Options:
#   --protocol        tcp or rdma (default: tcp)
#   --device-name     RDMA device name (default: "")
#   --build-dir       C++ build directory (default: ../../build)
#   --value-sizes     Comma-separated value sizes in bytes
#   --num-ops         Operations per value-size tier (default: 100)
#   --warmup-ops      Warmup operations (default: 10)
#   --batch-size      Python benchmark batch size (default: 1)
#   --num-threads     Thread count (default: 1)
#   --output-dir      Base output directory (default: ./bench_results)
#   --skip-python     Skip Python benchmark
#   --skip-cpp        Skip C++ benchmark
#   --global-segment  Global segment size for embedded mode in MB (default: 4096)
#   --lease-ttl       KV lease TTL in ms (default: 5000)

set -euo pipefail

# --------------------------------------------------------------------------
# Defaults
# --------------------------------------------------------------------------
PROTOCOL="tcp"
DEVICE_NAME=""
BUILD_DIR="../../build"
VALUE_SIZES="1024,65536,1048576,4194304,16777216"
NUM_OPS=100
WARMUP_OPS=10
BATCH_SIZE=1
NUM_THREADS=1
OUTPUT_DIR="./bench_results"
SKIP_PYTHON=false
SKIP_CPP=false
GLOBAL_SEGMENT_MB=4096
LOCAL_BUFFER_MB=512
LEASE_TTL=5000

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --------------------------------------------------------------------------
# Parse arguments
# --------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --protocol)       PROTOCOL="$2";       shift 2;;
        --device-name)    DEVICE_NAME="$2";    shift 2;;
        --build-dir)      BUILD_DIR="$2";      shift 2;;
        --value-sizes)    VALUE_SIZES="$2";    shift 2;;
        --num-ops)        NUM_OPS="$2";        shift 2;;
        --warmup-ops)     WARMUP_OPS="$2";     shift 2;;
        --batch-size)     BATCH_SIZE="$2";     shift 2;;
        --num-threads)    NUM_THREADS="$2";    shift 2;;
        --output-dir)     OUTPUT_DIR="$2";     shift 2;;
        --skip-python)    SKIP_PYTHON=true;    shift;;
        --skip-cpp)       SKIP_CPP=true;       shift;;
        --global-segment) GLOBAL_SEGMENT_MB="$2"; shift 2;;
        --lease-ttl)      LEASE_TTL="$2";      shift 2;;
        *) echo "Unknown option: $1"; exit 1;;
    esac
done

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_DIR="${OUTPUT_DIR}/${TIMESTAMP}"
EMBEDDED_DIR="${RESULT_DIR}/embedded"
STANDALONE_DIR="${RESULT_DIR}/standalone"
mkdir -p "$EMBEDDED_DIR" "$STANDALONE_DIR"

# Resolve paths
CLIENT_MODE_BENCH_BIN="${BUILD_DIR}/mooncake-store/benchmarks/client_mode_bench"
CLIENT_MODE_BENCH_PY="${SCRIPT_DIR}/client_mode_bench.py"

# PIDs to clean up
PIDS=()

cleanup() {
    echo "Cleaning up..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    echo "Cleanup done."
}
trap cleanup EXIT

# --------------------------------------------------------------------------
# Helper: wait for service to be ready
# --------------------------------------------------------------------------
wait_for_port() {
    local port=$1
    local timeout=${2:-30}
    local elapsed=0
    while ! ss -tlnp 2>/dev/null | grep -q ":${port} " && [[ $elapsed -lt $timeout ]]; do
        sleep 1
        elapsed=$((elapsed + 1))
    done
    if [[ $elapsed -ge $timeout ]]; then
        echo "ERROR: Timed out waiting for port $port"
        exit 1
    fi
}

# --------------------------------------------------------------------------
# Step 1: Start Master
# --------------------------------------------------------------------------
echo "=== Starting mooncake_master (lease_ttl=${LEASE_TTL}) ==="
mooncake_master --default_kv_lease_ttl="$LEASE_TTL" &
MASTER_PID=$!
PIDS+=("$MASTER_PID")
wait_for_port 50051
echo "Master started (PID $MASTER_PID)"

# --------------------------------------------------------------------------
# Step 2: Run Embedded Mode Benchmarks
# --------------------------------------------------------------------------
echo ""
echo "=========================================="
echo "  Phase 1: EMBEDDED Mode Benchmark"
echo "=========================================="

COMMON_PY_ARGS=(
    --protocol "$PROTOCOL"
    --device-name "$DEVICE_NAME"
    --metadata-server "http://127.0.0.1:8080/metadata"
    --master-server "localhost:50051"
    --global-segment-size "$GLOBAL_SEGMENT_MB"
    --local-buffer-size "$LOCAL_BUFFER_MB"
    --value-sizes "$VALUE_SIZES"
    --num-ops "$NUM_OPS"
    --warmup-ops "$WARMUP_OPS"
    --batch-size "$BATCH_SIZE"
    --num-threads "$NUM_THREADS"
)

if [[ "$SKIP_PYTHON" != true ]]; then
    echo "--- Python benchmark (embedded) ---"
    python3 "$CLIENT_MODE_BENCH_PY" \
        --mode embedded \
        "${COMMON_PY_ARGS[@]}" \
        --output-dir "$EMBEDDED_DIR" \
        2>&1 | tee "${EMBEDDED_DIR}/python_bench.log"
fi

if [[ "$SKIP_CPP" != true ]] && [[ -f "$CLIENT_MODE_BENCH_BIN" ]]; then
    echo "--- C++ benchmark (embedded) ---"
    "$CLIENT_MODE_BENCH_BIN" \
        --mode=embedded \
        --protocol="$PROTOCOL" \
        --device_name="$DEVICE_NAME" \
        --metadata_connection_string="http://127.0.0.1:8080/metadata" \
        --master_address="localhost:50051" \
        --value_sizes="$VALUE_SIZES" \
        --ops_per_thread="$NUM_OPS" \
        --warmup_ops="$WARMUP_OPS" \
        --num_threads="$NUM_THREADS" \
        --ram_buffer_size_gb=$((GLOBAL_SEGMENT_MB / 1024)) \
        --output_json="${EMBEDDED_DIR}/cpp_results.json" \
        2>&1 | tee "${EMBEDDED_DIR}/cpp_bench.log"
fi

# Kill master to reset state, then restart for standalone mode
kill "$MASTER_PID" 2>/dev/null || true
wait "$MASTER_PID" 2>/dev/null || true
PIDS=()
sleep 2

echo "=== Restarting mooncake_master ==="
mooncake_master --default_kv_lease_ttl="$LEASE_TTL" &
MASTER_PID=$!
PIDS+=("$MASTER_PID")
wait_for_port 50051

# --------------------------------------------------------------------------
# Step 3: Start RealClient for Standalone Mode
# --------------------------------------------------------------------------
echo ""
echo "=== Starting mooncake_client (RealClient) ==="
mooncake_client \
    --protocol="$PROTOCOL" \
    --device_names="$DEVICE_NAME" \
    --global_segment_size="${GLOBAL_SEGMENT_MB} MB" \
    --metadata_server="http://127.0.0.1:8080/metadata" \
    --master_server_address="127.0.0.1:50051" \
    --port=50052 &
CLIENT_PID=$!
PIDS+=("$CLIENT_PID")
wait_for_port 50052
echo "RealClient started (PID $CLIENT_PID)"

# --------------------------------------------------------------------------
# Step 4: Run Standalone Mode Benchmarks
# --------------------------------------------------------------------------
echo ""
echo "=========================================="
echo "  Phase 2: STANDALONE Mode Benchmark"
echo "=========================================="

if [[ "$SKIP_PYTHON" != true ]]; then
    echo "--- Python benchmark (standalone) ---"
    python3 "$CLIENT_MODE_BENCH_PY" \
        --mode standalone \
        --real-client-address "127.0.0.1:50052" \
        "${COMMON_PY_ARGS[@]}" \
        --output-dir "$STANDALONE_DIR" \
        2>&1 | tee "${STANDALONE_DIR}/python_bench.log"
fi

if [[ "$SKIP_CPP" != true ]] && [[ -f "$CLIENT_MODE_BENCH_BIN" ]]; then
    echo "--- C++ benchmark (standalone) ---"
    "$CLIENT_MODE_BENCH_BIN" \
        --mode=standalone \
        --real_client_address="127.0.0.1:50052" \
        --value_sizes="$VALUE_SIZES" \
        --ops_per_thread="$NUM_OPS" \
        --warmup_ops="$WARMUP_OPS" \
        --num_threads="$NUM_THREADS" \
        --output_json="${STANDALONE_DIR}/cpp_results.json" \
        2>&1 | tee "${STANDALONE_DIR}/cpp_bench.log"
fi

# --------------------------------------------------------------------------
# Step 5: Generate Comparison Report
# --------------------------------------------------------------------------
echo ""
echo "=========================================="
echo "  Comparison Report (Python)"
echo "=========================================="

if [[ "$SKIP_PYTHON" != true ]] && \
   [[ -f "${EMBEDDED_DIR}/results.json" ]] && \
   [[ -f "${STANDALONE_DIR}/results.json" ]]; then
    python3 "$CLIENT_MODE_BENCH_PY" --compare "$EMBEDDED_DIR" "$STANDALONE_DIR"
fi

echo ""
echo "=== All results saved to: $RESULT_DIR ==="
echo "  Embedded:   $EMBEDDED_DIR"
echo "  Standalone: $STANDALONE_DIR"
echo ""
echo "To re-run comparison:"
echo "  python3 $CLIENT_MODE_BENCH_PY --compare $EMBEDDED_DIR $STANDALONE_DIR"
