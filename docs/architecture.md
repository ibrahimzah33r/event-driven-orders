# Event-Driven Orders

A production-style event-driven order-processing backend built with FastAPI, PostgreSQL and Apache Kafka.

The project demonstrates practical distributed-systems patterns rather than a basic CRUD application.

## Features

- asynchronous event-driven service communication
- transactional outbox pattern
- Kafka producers and consumers
- independent consumer groups
- at-least-once delivery
- idempotent consumers
- retries with exponential backoff
- dead-letter topics
- event schema versioning
- PostgreSQL transactions
- SQLAlchemy
- Alembic migrations
- structured JSON logging
- liveness and readiness checks
- Kafka consumer-lag monitoring
- automated integration and failure-recovery tests
- GitHub Actions CI

---

## Architecture

```text
CLIENT
  |
  v
Order API
  |
  v
Order DB + Outbox
  |
  v
Order Outbox Worker
  |
  v
Kafka: orders.events
 /                 \
v                   v
Payment           Inventory
Worker             Worker
 |                   |
v                   v
DB + Outbox       DB + Outbox
 |                   |
v                   v
Outbox Worker     Outbox Worker
 \                   /
  \                 /
        Kafka
          |
          v
   Order Status Worker
          |
          v
    Order CONFIRMED
          |
          v
    Order Outbox
          |
          v
        Kafka
          |
          v
 Notification Worker
```

Detailed architecture documentation:

```text
docs/architecture.md
```

---

## Technology Stack

- Python 3.14
- FastAPI
- PostgreSQL 17
- Apache Kafka
- SQLAlchemy
- Alembic
- Pydantic
- confluent-kafka
- Docker Compose
- pytest
- GitHub Actions

---

## Services

### Order Service

Handles order creation and order state coordination.

Processes:

```text
services/order_service/main.py
services/order_service/worker.py
services/order_service/outbox_worker.py
```

### Payment Service

Consumes `order.created`, creates a payment and publishes `payment.completed`.

### Inventory Service

Consumes `order.created`, creates a reservation and publishes `inventory.reserved`.

### Notification Service

Consumes `order.confirmed` and stores the resulting notification.

---

## Event Flow

```text
POST /orders

-> order.created

-> Payment Service
-> Inventory Service

-> payment.completed
-> inventory.reserved

-> Order Status Worker

-> order CONFIRMED

-> order.confirmed

-> Notification Service
```

---

## Transactional Outbox

Business changes and outgoing events are written into PostgreSQL in the same transaction.

For example:

```text
Order 42
+
order.created outbox row
```

Both succeed or both fail.

A separate outbox worker later publishes the event to Kafka.

This removes the dangerous failure window where a database write succeeds but Kafka publishing fails.

---

## At-Least-Once Delivery

Kafka events can be delivered more than once.

Consumers therefore implement idempotency.

Payment, Inventory and Notification use:

- application-level duplicate checks
- database uniqueness constraints

---

## Retries and Dead-Letter Topics

Temporary processing failures retry with exponential backoff.

Events that continue failing are published to service-specific dead-letter topics.

Examples:

```text
payment-service.dlq
inventory-service.dlq
order-service-status.dlq
notification-service.dlq
```

---

## Repository Structure

```text
event-driven-orders/
|
|-- services/
|   |-- order_service/
|   |-- payment_service/
|   |-- inventory_service/
|   `-- notification_service/
|
|-- shared/
|   |-- events/
|   |-- kafka/
|   |-- outbox.py
|   `-- logging_config.py
|
|-- scripts/
|-- tests/
|   `-- integration/
|
|-- docs/
|   `-- architecture.md
|
|-- .github/
|   `-- workflows/
|       `-- ci.yml
|
|-- docker-compose.yml
|-- requirements.txt
`-- README.md
```

---

# Running Locally

## 1. Requirements

Install:

- Docker Desktop
- Python 3.14

Make sure Docker Desktop is running.

---

## 2. Create the virtual environment

```bash
python -m venv .venv
```

Activate it:

```bash
source .venv/bin/activate
```

---

## 3. Install Python dependencies

```bash
python -m pip install -r requirements.txt
```

---

## 4. Start Docker infrastructure

The normal runner starts Docker automatically, but infrastructure can also be started manually:

```bash
docker compose up -d
```

Check it:

```bash
docker compose ps
```

---

## 5. Run migrations

Order Service:

```bash
alembic \
  -c services/order_service/alembic.ini \
  upgrade head
```

Payment Service:

```bash
alembic \
  -c services/payment_service/alembic.ini \
  upgrade head
```

Inventory Service:

```bash
alembic \
  -c services/inventory_service/alembic.ini \
  upgrade head
```

Notification Service:

```bash
alembic \
  -c services/notification_service/alembic.ini \
  upgrade head
```

---

## 6. Start the complete application

```bash
./scripts/run_all.sh
```

This starts:

```text
Order API
Order status worker
Order outbox worker
Payment worker
Payment outbox worker
Inventory worker
Inventory outbox worker
Notification worker
```

as well as the Docker infrastructure.

API:

```text
http://127.0.0.1:8000
```

Swagger / OpenAPI:

```text
http://127.0.0.1:8000/docs
```

---

# Using the API

Create an order:

```bash
curl \
  -X POST \
  http://127.0.0.1:8000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": 100,
    "total": 250.00
  }'
```

Retrieve it:

```bash
curl http://127.0.0.1:8000/orders/1
```

After asynchronous processing completes, the order should reach:

```json
{
  "status": "CONFIRMED",
  "payment_status": "COMPLETED",
  "inventory_status": "RESERVED"
}
```

---

# Health Checks

Liveness:

```bash
curl http://127.0.0.1:8000/health/live
```

This verifies that the FastAPI application is alive.

Readiness:

```bash
curl http://127.0.0.1:8000/health/ready
```

This verifies connectivity to:

```text
PostgreSQL
Kafka
```

---

# Monitoring

Check Kafka consumer lag:

```bash
python -m scripts.check_kafka_lag
```

Example:

```text
payment-service          lag=0
inventory-service        lag=0
order-service-status     lag=0
notification-service     lag=0

Total lag: 0
```

Run the full operational summary:

```bash
python -m scripts.system_status
```

A healthy system reports approximately:

```text
Liveness: alive
Readiness: ready
Postgres:  OK
Kafka:     OK

Total consumer lag: 0

Overall: HEALTHY
```

---

# Logs

Logs are stored in:

```text
logs/
```

Watch all services:

```bash
tail -F logs/*.log
```

Search for errors:

```bash
grep -Ri '"level": "ERROR"' logs/
```

Trace a specific order across the distributed system:

```bash
grep -R '"order_id": 18' logs/
```

Structured logs include fields such as:

```text
timestamp
level
service
event_id
event_type
order_id
topic
partition
offset
```

---

# Testing

Run the complete automated integration suite:

```bash
./scripts/run_integration_tests.sh
```

The runner automatically:

```text
starts the application
        |
runs healthy-system tests
        |
stops the Order outbox worker
        |
runs the outbox recovery test
        |
cleans up the application processes
```

The integration suite covers:

- complete order-processing flow
- idempotent duplicate handling
- poison-event handling
- exponential retries
- dead-letter publishing
- transactional outbox recovery
- consumer failure and Kafka retention
- recovery after service restart

Individual pytest tests can also be run with:

```bash
python -m pytest -q tests/integration
```

---

# CI

GitHub Actions runs the integration suite on repository changes.

The CI pipeline:

```text
checks out repository
        |
installs Python dependencies
        |
starts PostgreSQL and Kafka
        |
runs Alembic migrations
        |
starts the application
        |
runs integration tests
```

Workflow:

```text
.github/workflows/ci.yml
```

---

# Reliability Behaviour

The system has been deliberately tested against failure scenarios.

## Duplicate events

Duplicate Kafka delivery does not create duplicate Payment or Inventory records.

## Poison events

Malformed events retry and eventually move to a dead-letter topic.

## Outbox publisher outage

Outgoing events remain safely stored in PostgreSQL with:

```text
published_at = NULL
```

When the publisher returns, those events are delivered to Kafka.

## Consumer outage

Kafka retains events while a consumer is unavailable.

Consumer lag increases while work is waiting.

After the consumer returns, it continues from its committed offset and lag returns to zero.

---

# Important Distributed-System Concepts Demonstrated

This project demonstrates:

- event-driven architecture
- asynchronous processing
- eventual consistency
- transactional messaging
- transactional outbox
- at-least-once delivery
- idempotency
- Kafka topics
- consumer groups
- partitions and offsets
- consumer lag
- database transactions
- retry/backoff
- dead-letter queues
- schema evolution
- structured observability
- liveness and readiness
- failure recovery

---

# Development Status

Core implementation is complete.

The remaining project work is focused on final portfolio review and interview preparation.