# DBIS_Project
A proxy-based distributed PostgreSQL system that uses lazy local replication + lock-based synchronization to serve queries locally and reduce latency while maintaining consistency with a remote source of truth

# Phase 1 — Connection Setup

## What's Implemented

| File        | Role                                                      |
|-------------|-----------------------------------------------------------|
| `remote.py` | Authenticates users, fetches schema + permissions from remote Postgres, sends `SCHEMA_TRANSFER` |
| `client.py` | Receives schema, creates local DB + tables + indexes + permissions |
| `proxy.py`  | User shell — coordinates remote & client, exposes REPL    |
| `query.py`  | Parses user commands (CONNECT for now; stubs for Phase 2) |

---

## Prerequisites

```bash
pip install psycopg2-binary
```

You need **two PostgreSQL instances** (or two databases on one instance):
- **Remote** Postgres: source of truth (runs on Laptop 1)
- **Local** Postgres: cache replica (runs on Laptop 2)

### Configuration

The system uses JSON configuration files that are auto-generated on first run. You should update these files to match your setup:

1. **`remote_config.json`** (Laptop 1): Configures `remote.py` to connect to its local Postgres and listen on `0.0.0.0:5000` to accept connections from Laptop 2.
2. **`client_config.json`** (Laptop 2): Configures `client.py` to connect to its local Postgres replica and listen on `localhost:5001`.
3. **`proxy_config.json`** (Laptop 2): Configures `proxy.py`. **Important**: Update `"remote_host"` in this file to the actual IP address of Laptop 1.

---

## Running (across two machines)

### Laptop 1 — Start remote.py
```bash
python remote.py
# [remote] Listening on 0.0.0.0:5000
```

### Laptop 2, Terminal 1 — Start client.py
```bash
python client.py
# [client] Listening on localhost:5001
```

### Laptop 2, Terminal 2 — Start proxy.py (user shell)
```bash
python proxy.py
# [proxy] Ready. Type SQL-like commands
proxy> CONNECT mydb USER alice;
```

---

## What Happens on CONNECT

```
User types:  CONNECT mydb USER alice;

proxy.py
  │
  ├─► remote.py  ──► real Postgres
  │     auth alice ✓
  │     fetch schema (tables, PKs, FKs, indexes)
  │     fetch permissions for alice
  │     ◄── SCHEMA_TRANSFER
  │
  └─► client.py  ──► local Postgres
        CREATE DATABASE mydb
        CREATE TABLE ... (for each table)
        CREATE INDEX ...
        GRANT ... TO alice
        ◄── INIT_DB_ACK

proxy.py:  ✅ Connected to 'mydb' as 'alice'
           Local replica is ready (schema only, no data yet)
```

---

## Message Protocol (JSON over TCP, newline-delimited)

| Message          | From → To            | Key Fields                              |
|------------------|----------------------|-----------------------------------------|
| `INIT`           | proxy → remote/client | `client_id`                            |
| `INIT_ACK`       | remote/client → proxy | `client_id`                            |
| `CONNECT`        | proxy → remote        | `client_id`, `database`, `user`, `password` |
| `SCHEMA_TRANSFER`| remote → proxy        | `schema`, `permissions`                |
| `INIT_DB`        | proxy → client        | `database`, `user`, `schema`, `permissions` |
| `INIT_DB_ACK`    | client → proxy        | `database`, `status`                   |
| `ERROR`          | any → any             | `message`                              |

---

## Next: Phase 2

- `query.py` will parse SELECT/UPDATE/DELETE
- `proxy.py` will check local cache before routing to remote
- Cache-miss → fetch from remote → store locally → assign lock
- Cache-hit → serve from local Postgres directly
