# FORGER

FORGER is a deliberately small distributed object-storage prototype: one Go coordinator with authoritative SQLite/WAL metadata and four independently persistent Go storage nodes. It provides immutable versions, replication factor and write-durability controls, checksum-verified reads, repair, reconciliation, fault injection, and a dashboard.

## Start

```powershell
docker compose up --build
```

Open [http://localhost:8080](http://localhost:8080). The Coordinator API is also on port `8080`.

## API and demo commands

```powershell
# Upload with RF=3 and minimum two verified writes (BALANCED)
curl.exe -X PUT --data-binary "@./large-test.bin" "http://localhost:8080/v1/objects/demo.bin?policy=BALANCED"

# Download and inspect current object metadata
curl.exe -o downloaded.bin "http://localhost:8080/v1/objects/demo.bin"
curl.exe "http://localhost:8080/v1/objects/demo.bin"

# Stop a replica node, then observe degraded/repair events
docker compose stop node2
curl.exe "http://localhost:8080/v1/events"
docker compose start node2

# Partition a live node. It remains running but rejects coordinator traffic.
curl.exe -X POST -H "Content-Type: application/json" -d "{\"enabled\":true}" http://localhost:8080/v1/admin/nodes/n3/partition
curl.exe -X POST -H "Content-Type: application/json" -d "{\"enabled\":false}" http://localhost:8080/v1/admin/nodes/n3/partition

# Corrupt a specific replica, then detect and repair it.
curl.exe -X POST -H "Content-Type: application/json" -d "{\"key\":\"demo.bin\",\"version\":1}" http://localhost:8080/v1/admin/nodes/n1/corrupt
curl.exe -X POST http://localhost:8080/v1/admin/scan
curl.exe -X POST http://localhost:8080/v1/admin/repair

# Reconcile local storage and metadata; request safe rebalancing.
curl.exe -X POST http://localhost:8080/v1/admin/reconcile
curl.exe -X POST http://localhost:8080/v1/admin/rebalance
```

The coordinator intentionally does not implement consensus or multi-coordinator failover. SQLite is its authoritative metadata source. Node objects and SQLite are persisted as named Docker volumes.

## Development

```powershell
go test ./...
docker compose down -v
```

`MODE=node` starts a storage node. `MODE=coordinator` starts the API, workers, and dashboard. Relevant tunables include `DEFAULT_RF`, `DEFAULT_MIN_SUCCESS`, `HEARTBEAT_INTERVAL`, `HEARTBEAT_FAILURES`, `REPAIR_GRACE`, `INTEGRITY_INTERVAL`, and rebalance thresholds.
