#!/usr/bin/env bash

set -e

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

PYTHON="$ROOT_DIR/.venv/bin/python"
UVICORN="$ROOT_DIR/.venv/bin/uvicorn"
LOG_DIR="$ROOT_DIR/logs"
RUN_DIR="$ROOT_DIR/.run"

if [ ! -x "$PYTHON" ]; then
    echo "ERROR: .venv not found."
    echo "Create it first with: python -m venv .venv"
    exit 1
fi

export PYTHONUNBUFFERED=1

mkdir -p "$LOG_DIR"
mkdir -p "$RUN_DIR"

# Remove PID files left from an earlier run.
rm -f "$RUN_DIR"/*.pid

PIDS=()


start_service() {
    NAME="$1"
    shift

    LOG_FILE="$LOG_DIR/$NAME.log"
    PID_FILE="$RUN_DIR/$NAME.pid"

    echo "Starting $NAME..."
    echo "Log: $LOG_FILE"

    "$@" \
        > >(
            sed -u "s/^/[$NAME] /" |
            tee -a "$LOG_FILE"
        ) \
        2>&1 &

    PID="$!"

    PIDS+=("$PID")

    echo "$PID" > "$PID_FILE"

    echo "PID: $PID"
}


cleanup() {
    echo
    echo "Stopping application services..."

    for PID in "${PIDS[@]}"; do
        kill "$PID" 2>/dev/null || true
    done

    wait "${PIDS[@]}" 2>/dev/null || true

    rm -f "$RUN_DIR"/*.pid

    echo "Application services stopped."
    echo "Docker containers left running."
}


trap cleanup EXIT INT TERM


echo "Starting Docker infrastructure..."

docker compose up -d

echo
docker compose ps
echo


start_service \
    order-api \
    "$UVICORN" \
    services.order_service.main:app \
    --port 8000 \
    --log-level info \
    --access-log


start_service \
    order-worker \
    "$PYTHON" \
    -m services.order_service.worker


start_service \
    order-outbox \
    "$PYTHON" \
    -m services.order_service.outbox_worker


start_service \
    payment-worker \
    "$PYTHON" \
    -m services.payment_service.worker


start_service \
    payment-outbox \
    "$PYTHON" \
    -m services.payment_service.outbox_worker


start_service \
    inventory-worker \
    "$PYTHON" \
    -m services.inventory_service.worker


start_service \
    inventory-outbox \
    "$PYTHON" \
    -m services.inventory_service.outbox_worker


start_service \
    notification-worker \
    "$PYTHON" \
    -m services.notification_service.worker


echo
echo "========================================"
echo " Event-Driven Orders is running"
echo "========================================"
echo
echo "API:  http://127.0.0.1:8000"
echo "Docs: http://127.0.0.1:8000/docs"
echo
echo "Logs: $LOG_DIR"
echo "PIDs: $RUN_DIR"
echo
echo "Useful commands:"
echo "  tail -F logs/*.log"
echo "  tail -F logs/order-outbox.log"
echo "  tail -F logs/payment-worker.log"
echo "  tail -F logs/inventory-worker.log"
echo "  tail -F logs/notification-worker.log"
echo
echo "Press Ctrl+C to stop application services."
echo

wait