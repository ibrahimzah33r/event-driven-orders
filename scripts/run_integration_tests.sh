#!/usr/bin/env bash

set -e

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

PYTHON="$ROOT_DIR/.venv/bin/python"
RUN_DIR="$ROOT_DIR/.run"
LOG_DIR="$ROOT_DIR/logs"

RUN_ALL_PID=""

cleanup() {
    echo
    echo "========================================"
    echo " Cleaning up integration environment"
    echo "========================================"

    if [ -n "$RUN_ALL_PID" ]; then
        kill "$RUN_ALL_PID" 2>/dev/null || true
        wait "$RUN_ALL_PID" 2>/dev/null || true
    fi

    echo "Application test processes stopped."
    echo "Kafka and PostgreSQL left running."
}

trap cleanup EXIT INT TERM


echo
echo "========================================"
echo " Starting integration test environment"
echo "========================================"
echo

mkdir -p "$LOG_DIR"

./scripts/run_all.sh \
    > "$LOG_DIR/integration-run.log" \
    2>&1 &

RUN_ALL_PID="$!"


echo "Waiting for API readiness..."

READY=false

for _ in {1..30}; do
    if curl \
        --silent \
        --fail \
        http://127.0.0.1:8000/health/ready \
        > /dev/null; then

        READY=true
        break
    fi

    sleep 2
done


if [ "$READY" != true ]; then
    echo "ERROR: API did not become ready."
    exit 1
fi


echo
echo "========================================"
echo " Running healthy-system tests"
echo "========================================"
echo

"$PYTHON" -m pytest \
    -q \
    tests/integration \
    -m "not outbox_recovery"


echo
echo "========================================"
echo " Testing Order outbox outage"
echo "========================================"
echo


ORDER_OUTBOX_PID_FILE="$RUN_DIR/order-outbox.pid"


if [ ! -f "$ORDER_OUTBOX_PID_FILE" ]; then
    echo "ERROR: Order outbox PID file not found."
    exit 1
fi


ORDER_OUTBOX_PID="$(cat "$ORDER_OUTBOX_PID_FILE")"


echo "Stopping Order outbox worker PID $ORDER_OUTBOX_PID..."

kill "$ORDER_OUTBOX_PID"


for _ in {1..20}; do
    if ! kill -0 "$ORDER_OUTBOX_PID" 2>/dev/null; then
        break
    fi

    sleep 0.25
done


if kill -0 "$ORDER_OUTBOX_PID" 2>/dev/null; then
    echo "ERROR: Order outbox worker did not stop."
    exit 1
fi


echo "Order outbox worker stopped."
echo
echo "Running outbox recovery test..."
echo


"$PYTHON" -m pytest \
    -q \
    tests/integration/test_outbox_recovery.py


echo
echo "========================================"
echo " All integration tests passed"
echo "========================================"
echo