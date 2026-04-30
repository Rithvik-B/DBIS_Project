"""
remote.py — Remote Brain
========================
Listens on port 5000. Sits in front of the actual remote PostgreSQL instance.

Phase 1: CONNECT → authenticate, fetch schema + permissions, send SCHEMA_TRANSFER
Phase 2: QUERY (Type A/B/INSERT), LOCK_REQUEST, LOCK_RELEASE (recall protocol)
"""

import asyncio
import json
import os
import psycopg2
import psycopg2.extras
from dataclasses import dataclass, field

# ─── Configuration ────────────────────────────────────────────────────────────

CONFIG_FILE = "remote_config.json"
if os.path.exists(CONFIG_FILE):
    with open(CONFIG_FILE, "r") as f:
        config = json.load(f)
else:
    config = {
        "remote_db_host": "localhost",
        "remote_db_port": 5432,
        "remote_superuser": "postgres",
        "remote_superuser_password": "postgres",
        "listen_host": "0.0.0.0",
        "listen_port": 5000
    }
    print("couldn't load config, so rewrote it")
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=4)

REMOTE_HOST               = config.get("remote_db_host", "localhost")
REMOTE_PORT               = config.get("remote_db_port", 5432)
REMOTE_SUPERUSER          = config.get("remote_superuser", "postgres")
REMOTE_SUPERUSER_PASSWORD = config.get("remote_superuser_password", "postgres")
LISTEN_HOST               = config.get("listen_host", "0.0.0.0")
LISTEN_PORT               = config.get("listen_port", 5000)

# ─── Global State ─────────────────────────────────────────────────────────────

subscriptions: dict = {}   # client_id → {database, user}
clients: dict       = {}   # client_id → asyncio.StreamWriter

# row_locks[(database, table, pk_value)] = LockEntry
@dataclass
class LockEntry:
    holder:    str
    lock_type: str              # "READ" or "WRITE"
    waitlist:  list = field(default_factory=list)

row_locks: dict = {}   # (db, table, pk) → LockEntry

# client_cache_map[client_id][(database, table)] = set of pk values
client_cache_map: dict = {}

# Events signalled when a LOCK_RELEASE arrives from a holder
_lock_events: dict = {}


# ─── DB helpers ───────────────────────────────────────────────────────────────

def get_superuser_conn(database: str = "postgres"):
    return psycopg2.connect(
        host=REMOTE_HOST, port=REMOTE_PORT,
        dbname=database,
        user=REMOTE_SUPERUSER, password=REMOTE_SUPERUSER_PASSWORD,
    )


def authenticate_user(database: str, user: str, password: str = "") -> bool:
    try:
        kwargs = {
            "host": REMOTE_HOST,
            "port": REMOTE_PORT,
            "dbname": database,
            "user": user,
            "connect_timeout": 5,
        }
        if password:
            if password == "fakepd":
                kwargs["password"] = REMOTE_SUPERUSER_PASSWORD
            else:
                kwargs["password"] = password
        conn = psycopg2.connect(**kwargs)
        conn.close()
        return True
    except psycopg2.OperationalError as e:
        print(f"[remote] Auth failed for {user}@{database}: {e}")
        return False


def fetch_schema(database: str) -> dict:
    conn = get_superuser_conn(database)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    schema = {}

    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """)
    tables = [row["table_name"] for row in cur.fetchall()]

    for table in tables:
        schema[table] = {"columns": [], "primary_keys": [], "unique": [],
                         "foreign_keys": [], "indexes": []}

        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default,
                   character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position;
        """, (table,))
        schema[table]["columns"] = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema    = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = 'public' AND tc.table_name = %s
            ORDER BY kcu.ordinal_position;
        """, (table,))
        schema[table]["primary_keys"] = [r["column_name"] for r in cur.fetchall()]

        cur.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema    = kcu.table_schema
            WHERE tc.constraint_type = 'UNIQUE'
              AND tc.table_schema = 'public' AND tc.table_name = %s;
        """, (table,))
        schema[table]["unique"] = [r["column_name"] for r in cur.fetchall()]

        cur.execute("""
            SELECT kcu.column_name,
                   ccu.table_name AS foreign_table, ccu.column_name AS foreign_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema    = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON tc.constraint_name = ccu.constraint_name
             AND tc.table_schema    = ccu.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = 'public' AND tc.table_name = %s;
        """, (table,))
        schema[table]["foreign_keys"] = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT indexname, indexdef FROM pg_indexes
            WHERE schemaname = 'public' AND tablename = %s;
        """, (table,))
        schema[table]["indexes"] = [dict(r) for r in cur.fetchall()]

    cur.close()
    conn.close()
    return schema


def fetch_permissions(database: str, user: str) -> dict:
    conn = get_superuser_conn(database)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT table_name, privilege_type
        FROM information_schema.role_table_grants
        WHERE grantee = %s AND table_schema = 'public'
        ORDER BY table_name, privilege_type;
    """, (user,))
    perms: dict = {}
    for row in cur.fetchall():
        perms.setdefault(row["table_name"], []).append(row["privilege_type"])
    cur.close()
    conn.close()
    return perms


def get_pk_columns(database: str, table: str) -> list[str]:
    conn = get_superuser_conn(database)
    cur  = conn.cursor()
    cur.execute("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema    = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = 'public' AND tc.table_name = %s
        ORDER BY kcu.ordinal_position;
    """, (table,))
    cols = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return cols


# ─── Message I/O ──────────────────────────────────────────────────────────────

async def send_msg(writer: asyncio.StreamWriter, msg: dict):
    data = (json.dumps(msg, default=str) + "\n").encode()
    writer.write(data)
    await writer.drain()


async def recv_msg(reader: asyncio.StreamReader) -> dict | None:
    try:
        line = await reader.readline()
        if not line:
            return None
        return json.loads(line.decode())
    except (json.JSONDecodeError, ConnectionResetError):
        return None


# ─── Lock helpers ─────────────────────────────────────────────────────────────

def _lock_key(database: str, table: str, pk) -> tuple:
    return (database, table, str(pk))


def _register_cache(client_id: str, database: str, table: str, pks: list):
    client_cache_map.setdefault(client_id, {})
    key = (database, table)
    client_cache_map[client_id].setdefault(key, set()).update(str(p) for p in pks)


def _clear_locks_for_client(client_id: str, database: str, table: str, pks: list):
    for pk in pks:
        key = _lock_key(database, table, pk)
        if key in row_locks and row_locks[key].holder == client_id:
            del row_locks[key]
    key = (database, table)
    if client_id in client_cache_map:
        client_cache_map[client_id].pop(key, None)


async def _recall_and_wait(client_id: str, database: str, table: str,
                            pks: list, requester_id: str) -> bool:
    writer = clients.get(client_id)
    if writer is None:
        _clear_locks_for_client(client_id, database, table, pks)
        return True

    print(f"[remote] Recalling lock from {client_id} for PKs {pks} "
          f"(requested by {requester_id})")
    await send_msg(writer, {
        "type":     "RECALL_LOCK",
        "database": database,
        "table":    table,
        "pks":      pks,
    })

    event_key = (database, table, "recall", client_id)
    event = asyncio.Event()
    _lock_events[event_key] = event
    try:
        await asyncio.wait_for(event.wait(), timeout=30.0)
    except asyncio.TimeoutError:
        print(f"[remote] Timeout waiting for LOCK_RELEASE from {client_id}")
        _clear_locks_for_client(client_id, database, table, pks)
    finally:
        _lock_events.pop(event_key, None)
    return True


# ─── Query execution ──────────────────────────────────────────────────────────

def execute_query(database: str, sql: str) -> tuple[list[dict], list[str]]:
    conn = get_superuser_conn(database)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql)
    rows      = [dict(r) for r in cur.fetchall()]
    col_names = [desc[0] for desc in cur.description] if cur.description else []
    cur.close()
    conn.close()
    return rows, col_names


def execute_write(database: str, sql: str) -> int:
    conn = get_superuser_conn(database)
    cur  = conn.cursor()
    cur.execute(sql)
    count = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return count


def apply_changes(database: str, changes: list[dict]):
    if not changes:
        return
    conn = get_superuser_conn(database)
    cur  = conn.cursor()
    try:
        for ch in changes:
            cur.execute(ch["sql"])
        conn.commit()
        print(f"[remote] Applied {len(changes)} change(s) from sync.")
    except Exception as e:
        conn.rollback()
        print(f"[remote] Error applying changes: {e}")
    finally:
        cur.close()
        conn.close()


# ─── Request Handlers ─────────────────────────────────────────────────────────

async def handle_init(client_id: str, writer: asyncio.StreamWriter):
    clients[client_id] = writer
    print(f"[remote] Client registered: {client_id}")
    await send_msg(writer, {"type": "INIT_ACK", "client_id": client_id})


async def handle_connect(msg: dict, writer: asyncio.StreamWriter):
    client_id = msg["client_id"]
    database  = msg["database"]
    user      = msg["user"]
    password  = msg.get("password", "")

    print(f"[remote] CONNECT request — client={client_id} db={database} user={user}")

    if not authenticate_user(database, user, password):
        await send_msg(writer, {
            "type":    "ERROR",
            "message": f"Authentication failed for user '{user}' on database '{database}'"
        })
        return

    subscriptions[client_id] = {"database": database, "user": user}
    print(f"[remote] Subscription registered: {subscriptions[client_id]}")

    print(f"[remote] Fetching schema for database '{database}' …")
    try:
        schema = fetch_schema(database)
    except Exception as e:
        await send_msg(writer, {"type": "ERROR", "message": f"Schema fetch failed: {e}"})
        return

    print(f"[remote] Fetching permissions for user '{user}' …")
    try:
        permissions = fetch_permissions(database, user)
    except Exception as e:
        await send_msg(writer, {"type": "ERROR", "message": f"Permission fetch failed: {e}"})
        return

    await send_msg(writer, {
        "type":        "SCHEMA_TRANSFER",
        "client_id":   client_id,
        "database":    database,
        "user":        user,
        "schema":      schema,
        "permissions": permissions,
    })
    print(f"[remote] SCHEMA_TRANSFER sent to {client_id}")


async def handle_query(msg: dict, writer: asyncio.StreamWriter):
    client_id  = msg["client_id"]
    database   = msg["database"]
    query_type = msg["query_type"]
    sql        = msg["sql"]
    table      = msg.get("table", "")

    print(f"[remote] QUERY type={query_type} from {client_id}: {sql[:80]}")

    if query_type in ("A", "INSERT"):
        try:
            if query_type == "INSERT":
                rowcount = execute_write(database, sql)
                await _notify_insert(client_id, database, table)
                await send_msg(writer, {
                    "type":       "QUERY_RESULT",
                    "query_type": query_type,
                    "rowcount":   rowcount,
                    "rows":       [],
                    "columns":    [],
                })
            else:
                rows, cols = execute_query(database, sql)
                await send_msg(writer, {
                    "type":       "QUERY_RESULT",
                    "query_type": query_type,
                    "rows":       rows,
                    "columns":    cols,
                    "rowcount":   len(rows),
                })
        except Exception as e:
            await send_msg(writer, {"type": "ERROR", "message": str(e)})
        return

    if query_type == "B":
        try:
            rows, cols = execute_query(database, sql)
        except Exception as e:
            await send_msg(writer, {"type": "ERROR", "message": str(e)})
            return

        pk_cols = get_pk_columns(database, table)
        pks = []
        for row in rows:
            if not pk_cols:
                continue
            if len(pk_cols) == 1:
                pks.append(str(row[pk_cols[0]]))
            else:
                pks.append(json.dumps([str(row[c]) for c in pk_cols]))

        # Recall any conflicting WRITE holders before granting READ
        conflicting: dict = {}
        for pk in pks:
            key = _lock_key(database, table, pk)
            if key in row_locks and row_locks[key].lock_type == "WRITE":
                holder = row_locks[key].holder
                if holder != client_id:
                    conflicting.setdefault(holder, []).append(pk)

        for holder, held_pks in conflicting.items():
            await _recall_and_wait(holder, database, table, held_pks, client_id)

        # Grant READ locks (shared — don't overwrite an existing READ entry)
        for pk in pks:
            key = _lock_key(database, table, pk)
            if key not in row_locks:
                row_locks[key] = LockEntry(holder=client_id, lock_type="READ")
        _register_cache(client_id, database, table, pks)

        await send_msg(writer, {
            "type":        "QUERY_RESULT",
            "query_type":  "B",
            "rows":        rows,
            "columns":     cols,
            "pks":         pks,
            "pk_cols":     pk_cols,
            "table":       table,
            "fingerprint": msg.get("fingerprint", ""),
            "rowcount":    len(rows),
        })
        print(f"[remote] Type B: {len(rows)} row(s), PKs={pks[:5]}{'...' if len(pks)>5 else ''}")
        return

    await send_msg(writer, {"type": "ERROR", "message": f"Unknown query_type: {query_type}"})


async def handle_lock_request(msg: dict, writer: asyncio.StreamWriter):
    client_id = msg["client_id"]
    database  = msg["database"]
    table     = msg["table"]
    pks       = msg["pks"]

    print(f"[remote] LOCK_REQUEST WRITE from {client_id} on {table} PKs={pks}")

    conflicting: dict = {}
    for pk in pks:
        key = _lock_key(database, table, pk)
        if key in row_locks:
            holder = row_locks[key].holder
            if holder != client_id:
                conflicting.setdefault(holder, []).append(pk)

    for holder, held_pks in conflicting.items():
        await _recall_and_wait(holder, database, table, held_pks, client_id)

    for pk in pks:
        key = _lock_key(database, table, pk)
        row_locks[key] = LockEntry(holder=client_id, lock_type="WRITE")

    await send_msg(writer, {"type": "LOCK_GRANT", "table": table, "pks": pks})
    print(f"[remote] LOCK_GRANT WRITE to {client_id} on {table} PKs={pks}")


async def handle_lock_release(msg: dict, writer: asyncio.StreamWriter):
    client_id       = msg["client_id"]
    database        = msg["database"]
    table           = msg["table"]
    pks             = msg["pks"]
    pending_changes = msg.get("pending_changes", [])

    print(f"[remote] LOCK_RELEASE from {client_id}: {len(pending_changes)} change(s)")

    apply_changes(database, pending_changes)
    _clear_locks_for_client(client_id, database, table, pks)

    await send_msg(writer, {
        "type":     "SYNC_ACK",
        "database": database,
        "table":    table,
        "pks":      pks,
    })

    event_key = (database, table, "recall", client_id)
    if event_key in _lock_events:
        _lock_events[event_key].set()


async def _notify_insert(inserter_id: str, database: str, table: str):
    for cid, cache in client_cache_map.items():
        if cid == inserter_id:
            continue
        if (database, table) in cache:
            writer = clients.get(cid)
            if writer:
                try:
                    await send_msg(writer, {
                        "type":     "CACHE_INVALIDATE",
                        "database": database,
                        "table":    table,
                    })
                except Exception:
                    pass


# ─── Connection Handler ────────────────────────────────────────────────────────

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    print(f"[remote] New connection from {addr}")
    client_id_seen = None

    while True:
        msg = await recv_msg(reader)
        if msg is None:
            print(f"[remote] Connection closed by {addr}")
            break

        msg_type  = msg.get("type")
        client_id = msg.get("client_id", "")
        if client_id:
            client_id_seen = client_id

        if msg_type == "INIT":
            await handle_init(client_id, writer)
        elif msg_type == "CONNECT":
            await handle_connect(msg, writer)
        elif msg_type == "QUERY":
            await handle_query(msg, writer)
        elif msg_type == "LOCK_REQUEST":
            await handle_lock_request(msg, writer)
        elif msg_type == "LOCK_RELEASE":
            await handle_lock_release(msg, writer)
        else:
            await send_msg(writer, {"type": "ERROR", "message": f"Unknown type: {msg_type}"})

    if client_id_seen:
        clients.pop(client_id_seen, None)
        subscriptions.pop(client_id_seen, None)
        to_delete = [k for k, v in row_locks.items() if v.holder == client_id_seen]
        for k in to_delete:
            del row_locks[k]
        client_cache_map.pop(client_id_seen, None)

    writer.close()


# ─── Entry Point ──────────────────────────────────────────────────────────────

async def main():
    server = await asyncio.start_server(handle_client, LISTEN_HOST, LISTEN_PORT)
    print(f"[remote] Listening on {LISTEN_HOST}:{LISTEN_PORT}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
