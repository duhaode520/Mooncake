#!/usr/bin/env bash
# =============================================================================
# Mooncake HA Chaos Test Runner
#
# Usage:
#   ./run.sh build         Build the Docker image
#   ./run.sh up            Start the cluster (background)
#   ./run.sh test          Run all chaos tests
#   ./run.sh test-phase1   Run only Phase 1 tests
#   ./run.sh test-phase2   Run only Phase 2 tests
#   ./run.sh test-phase3   Run only Phase 3 tests
#   ./run.sh test-phase4   Run only Phase 4 tests
#   ./run.sh logs          Follow all container logs
#   ./run.sh status        Show container status
#   ./run.sh down          Tear down the cluster
#   ./run.sh clean         Remove volumes and images
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
IMAGE_NAME="mooncake-ha-chaos:latest"

export MOONCAKE_IMAGE="$IMAGE_NAME"

cd "$SCRIPT_DIR"

# ─── Functions ────────────────────────────────────────────────────────────────

build() {
    echo "🔨 Building Docker image: $IMAGE_NAME"
    echo "   Context: $PROJECT_ROOT"
    docker build \
        -t "$IMAGE_NAME" \
        -f "$SCRIPT_DIR/Dockerfile" \
        "$PROJECT_ROOT"
    echo "✅ Image built: $IMAGE_NAME"
}

up() {
    echo "🚀 Starting cluster..."
    docker compose -f "$COMPOSE_FILE" up -d \
        etcd http-meta master-1 master-2 master-3 client-1 client-2
    echo "⏳ Waiting for services to be healthy..."
    sleep 15
    docker compose -f "$COMPOSE_FILE" ps
    echo "✅ Cluster is up"
}

run_tests() {
    local test_filter="${1:-}"
    local pytest_args="-v --tb=long --timeout=600 -x"

    if [ -n "$test_filter" ]; then
        pytest_args="$pytest_args -k $test_filter"
    fi

    echo "🧪 Running chaos tests..."
    docker compose -f "$COMPOSE_FILE" run --rm \
        -e CHAOS_SEED="${CHAOS_SEED:-}" \
        chaos-runner \
        python3 -m pytest /opt/chaos-tests/ $pytest_args
}

logs() {
    docker compose -f "$COMPOSE_FILE" logs -f
}

status() {
    echo "📊 Container status:"
    docker compose -f "$COMPOSE_FILE" ps
    echo ""
    echo "📊 Master health:"
    for port in 9001 9002 9003; do
        host="localhost"
        name="master-$((port - 9000))"
        health=$(curl -sf "http://$host:$port/health" 2>/dev/null || echo "UNREACHABLE")
        summary=$(curl -sf "http://$host:$port/metrics/summary" 2>/dev/null | head -5 || echo "N/A")
        echo "  $name ($port): $health"
        echo "    $summary"
    done
}

down() {
    echo "🛑 Stopping cluster..."
    docker compose -f "$COMPOSE_FILE" down
    echo "✅ Cluster stopped"
}

clean() {
    echo "🧹 Cleaning up..."
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans
    docker rmi "$IMAGE_NAME" 2>/dev/null || true
    echo "✅ Clean complete"
}

# ─── Main ─────────────────────────────────────────────────────────────────────

case "${1:-help}" in
    build)
        build
        ;;
    up)
        up
        ;;
    test)
        run_tests ""
        ;;
    test-phase1)
        run_tests "phase1"
        ;;
    test-phase2)
        run_tests "phase2"
        ;;
    test-phase3)
        run_tests "phase3"
        ;;
    test-phase4)
        run_tests "phase4"
        ;;
    logs)
        logs
        ;;
    status)
        status
        ;;
    down)
        down
        ;;
    clean)
        clean
        ;;
    all)
        build
        up
        sleep 10
        run_tests "" || true
        down
        ;;
    help|*)
        echo "Mooncake HA Chaos Test Runner"
        echo ""
        echo "Usage: $0 <command>"
        echo ""
        echo "Commands:"
        echo "  build         Build the Docker image (no CUDA, TCP only)"
        echo "  up            Start the cluster (etcd + 3 masters + 2 clients)"
        echo "  test          Run ALL chaos tests"
        echo "  test-phase1   Run Phase 1: Basic HA (S1-S3)"
        echo "  test-phase2   Run Phase 2: Multi-Node Failures (S4-S6)"
        echo "  test-phase3   Run Phase 3: Client+Master Joint Failures (S7-S8)"
        echo "  test-phase4   Run Phase 4: Sustained Chaos (S9-S10)"
        echo "  logs          Follow container logs"
        echo "  status        Show cluster status & master health"
        echo "  down          Stop the cluster"
        echo "  clean         Remove volumes, images, and all artifacts"
        echo "  all           Build → Up → Test → Down (full CI run)"
        echo ""
        echo "Environment:"
        echo "  CHAOS_SEED=<int>   Set random seed for S10 reproducibility"
        ;;
esac
