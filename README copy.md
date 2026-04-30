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
- **Remote** Postgres: source of truth (default: localhost:5432)
- **Local** Postgres: cache replica (can be same host, different port e.g. 5433)

Update the config constants at the top of each file to match your setup.

---

## Running (3 terminals)

### Terminal 1 — Start remote.py
```bash
python remote.py
# [remote] Listening on localhost:5000
```

### Terminal 2 — Start client.py
```bash
python client.py
# [client] Listening on localhost:5001
```

### Terminal 3 — Start proxy.py (user shell)
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
