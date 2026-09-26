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

## Public demo preparation

The production dashboard lives in [`frontend/`](frontend/). It is a Next.js app for Vercel and talks only to same-origin `/api/*`; its server-side proxy forwards requests to `FORGER_BACKEND_URL` so no coordinator URL or admin token is exposed in browser code.

```powershell
cd frontend
Copy-Item .env.example .env.local
npm install
npm run dev
```

For local frontend testing set `FORGER_BACKEND_URL=http://localhost:8080`. The Vercel project root is `frontend/`; configure `FORGER_BACKEND_URL` with the public Render coordinator URL and, when admin controls are needed, `FORGER_ADMIN_TOKEN` with the coordinator's `ADMIN_TOKEN`.

### Render services

[`render.yaml`](render.yaml) declares one coordinator and four independently persistent node web services. Each service runs the same Docker image. Before deploying, create one strong random value and configure the **same** `INTERNAL_TOKEN` on the coordinator and every node. Configure a separate strong `ADMIN_TOKEN` only on the coordinator. After the four node URLs exist, set coordinator `NODE_URLS`:

```text
n1=https://forger-node1.example.com,n2=https://forger-node2.example.com,n3=https://forger-node3.example.com,n4=https://forger-node4.example.com
```

Required variables:

| Service | Required variables |
| --- | --- |
| Coordinator | `MODE=coordinator`, `DATABASE_PATH=/data/forger.db`, `NODE_URLS`, `INTERNAL_TOKEN`, `ADMIN_TOKEN` |
| Node | `MODE=node`, `NODE_ID`, `DATA_DIR=/data`, `CAPACITY_BYTES`, `INTERNAL_TOKEN` |
| All Render services | Render supplies `PORT`; the application uses it when `LISTEN_ADDR` is unset. |

Storage nodes protect `/internal/*` with `X-Forger-Internal-Token` whenever `INTERNAL_TOKEN` is configured. The coordinator adds that header for all coordinator-to-node calls. `POST /v1/admin/*` requires `X-Forger-Admin-Token` when `ADMIN_TOKEN` is configured. Never commit either secret. Local Compose remains token-optional for the existing zero-configuration demo.
