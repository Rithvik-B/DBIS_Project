"""
remote.py — Remote Brain
========================
Listens on port 5000. Sits in front of the actual remote PostgreSQL instance.

Phase 1: CONNECT → authenticate, fetch schema + permissions, send SCHEMA_TRANSFER
Phase 2: QUERY (Type A/B), LOCK_REQUEST, LOCK_RELEASE (recall protocol), APPLY_CHANGES

Lock model (per row key):
  RowLock.write   : client_id holding exclusive write lock, or None
  RowLock.readers : set of client_ids with shared read lock

Rules:
  - Multiple readers allowed simultaneously.
  - A WRITE request recalls ALL readers + any other writer, then grants exclusively.
  - A READ request only recalls an existing writer; existing readers are untouched.
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
clients: dict       = {}   # client_id → asyncio.StreamWriter  (writer back to proxy)

# Per-writer mutex so concurrent coroutines don't interleave writes
_writer_locks: dict = {}   # writer id → asyncio.Lock


@dataclass
class RowLock:
    write:   str | None = None          # client_id holding WRITE lock, or None
    readers: set        = field(default_factory=set)  # client_ids with READ lock


# (database, table, pk_value_str) → RowLock
row_locks: dict = {}

# client_id → {(database, table): set(pk_str)}  — tracks what each client has cached
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
    """Send a message, serialised with a per-writer lock to avoid interleaving."""
    lock = _writer_locks.setdefault(id(writer), asyncio.Lock())
    data = (json.dumps(msg, default=str) + "\n").encode()
    async with lock:
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


def _get_or_create_lock(database: str, table: str, pk) -> RowLock:
    key = _lock_key(database, table, pk)
    if key not in row_locks:
        row_locks[key] = RowLock()
    return row_locks[key]


def _cleanup_lock(database: str, table: str, pk):
    """Remove RowLock entry if it has no holders."""
    key = _lock_key(database, table, pk)
    if key in row_locks:
        entry = row_locks[key]
        if entry.write is None and not entry.readers:
            del row_locks[key]


def _register_cache(client_id: str, database: str, table: str, pks: list):
    client_cache_map.setdefault(client_id, {})
    key = (database, table)
    client_cache_map[client_id].setdefault(key, set()).update(str(p) for p in pks)


def _clear_locks_for_client(client_id: str, database: str, table: str, pks: list):
    """Remove client from write/readers for the given PKs."""
    for pk in pks:
        key = _lock_key(database, table, pk)
        if key in row_locks:
            entry = row_locks[key]
            if entry.write == client_id:
                entry.write = None
            entry.readers.discard(client_id)
            _cleanup_lock(database, table, pk)
    ct_key = (database, table)
    if client_id in client_cache_map:
        client_cache_map[client_id].pop(ct_key, None)


async def _recall_and_wait(holder_id: str, database: str, table: str,
                            pks: list, requester_id: str) -> bool:
    """Send RECALL_LOCK to holder and wait for LOCK_RELEASE (up to 30 s)."""
    writer = clients.get(holder_id)
    if writer is None:
        _clear_locks_for_client(holder_id, database, table, pks)
        return True

    print(f"[remote] Recalling lock from {holder_id} PKs={pks} "
          f"(requested by {requester_id})")
    await send_msg(writer, {
        "type":     "RECALL_LOCK",
        "database": database,
        "table":    table,
        "pks":      pks,
    })

    event_key = (database, table, "recall", holder_id)
    event = asyncio.Event()
    _lock_events[event_key] = event
    try:
        await asyncio.wait_for(event.wait(), timeout=30.0)
    except asyncio.TimeoutError:
        print(f"[remote] Timeout waiting for LOCK_RELEASE from {holder_id}")
        _clear_locks_for_client(holder_id, database, table, pks)
    finally:
        _lock_events.pop(event_key, None)
    return True


def _collect_holders_for_write(client_id: str, database: str, table: str,
                                pks: list) -> dict:
    """
    Collect all current lock holders (readers + writer) that are not client_id,
    grouped by holder → [pks].  Used before granting a WRITE lock.
    """
    holders: dict = {}
    for pk in pks:
        key = _lock_key(database, table, pk)
        if key not in row_locks:
            continue
        entry = row_locks[key]
        if entry.write and entry.write != client_id:
            holders.setdefault(entry.write, []).append(pk)
        for reader in entry.readers:
            if reader != client_id:
                holders.setdefault(reader, []).append(pk)
    return holders


def _collect_writers_for_read(client_id: str, database: str, table: str,
                               pks: list) -> dict:
    """
    Collect WRITE-lock holders that are not client_id, grouped by holder → [pks].
    Used before granting a READ lock (readers may coexist; only writers block).
    """
    holders: dict = {}
    for pk in pks:
        key = _lock_key(database, table, pk)
        if key not in row_locks:
            continue
        entry = row_locks[key]
        if entry.write and entry.write != client_id:
            holders.setdefault(entry.write, []).append(pk)
    return holders


def _collect_all_writers_for_table(client_id: str, database: str, table: str) -> dict:
    """
    Collect ALL write-lock holders on any row of a table that are not client_id.
    Used before a TYPE_A query so any dirty write is flushed to remote first.
    """
    holders: dict = {}
    for (db, tbl, pk), entry in row_locks.items():
        if db == database and tbl == table and entry.write and entry.write != client_id:
            holders.setdefault(entry.write, []).append(pk)
    return holders


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


def apply_changes(database: str, changes: list[dict]) -> bool:
    """Apply a list of {sql} dicts to the remote DB in a single transaction.
    Returns True on success, False on failure."""
    if not changes:
        return True
    conn = get_superuser_conn(database)
    cur  = conn.cursor()
    try:
        for ch in changes:
            cur.execute(ch["sql"])
        conn.commit()
        print(f"[remote] Applied {len(changes)} change(s).")
        return True
    except Exception as e:
        conn.rollback()
        print(f"[remote] Error applying changes: {e}")
        return False
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

    print(f"[remote] CONNECT — client={client_id} db={database} user={user}")

    if not authenticate_user(database, user, password):
        await send_msg(writer, {
            "type":    "ERROR",
            "message": f"Authentication failed for user '{user}' on database '{database}'"
        })
        return

    subscriptions[client_id] = {"database": database, "user": user}

    try:
        schema = fetch_schema(database)
    except Exception as e:
        await send_msg(writer, {"type": "ERROR", "message": f"Schema fetch failed: {e}"})
        return

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

    # ── Type A: recall all write holders for the table, then execute ──────────
    if query_type == "A":
        if table:
            writers = _collect_all_writers_for_table(client_id, database, table)
            for holder, held_pks in writers.items():
                await _recall_and_wait(holder, database, table, held_pks, client_id)
        try:
            rows, cols = execute_query(database, sql)
            await send_msg(writer, {
                "type":       "QUERY_RESULT",
                "query_type": "A",
                "rows":       rows,
                "columns":    cols,
                "rowcount":   len(rows),
            })
        except Exception as e:
            await send_msg(writer, {"type": "ERROR", "message": str(e)})
        return

    # ── Type B: cacheable read ─────────────────────────────────────────────────
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

        # Recall any WRITE holders before granting READ (readers may coexist)
        writers_to_recall = _collect_writers_for_read(client_id, database, table, pks)
        for holder, held_pks in writers_to_recall.items():
            await _recall_and_wait(holder, database, table, held_pks, client_id)

        # Grant READ: add requester to readers set; do NOT disturb existing readers
        for pk in pks:
            entry = _get_or_create_lock(database, table, pk)
            entry.readers.add(client_id)
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
        print(f"[remote] Type B: {len(rows)} row(s), READ lock granted to {client_id}")
        return

    await send_msg(writer, {"type": "ERROR", "message": f"Unknown query_type: {query_type}"})


async def handle_meta_query(msg: dict, writer: asyncio.StreamWriter):
    """Handle psql meta-commands by translating them to catalog SQL."""
    database = msg["database"]
    raw_cmd  = msg.get("meta_cmd", "").strip()
    token    = raw_cmd.split()[0] if raw_cmd else ""
    args     = raw_cmd[len(token):].strip() if len(raw_cmd) > len(token) else ""

    print(f"[remote] META_QUERY: {raw_cmd} from db={database}")

    # Map meta-commands to SQL
    meta_sql_map = {
        r"\dt": (
            "SELECT schemaname AS \"Schema\", tablename AS \"Name\", "
            "tableowner AS \"Owner\" FROM pg_tables "
            "WHERE schemaname NOT IN ('pg_catalog', 'information_schema') "
            "ORDER BY tablename;"
        ),
        r"\d": None,   # needs special handling — may have args
        r"\l": (
            "SELECT datname AS \"Name\", pg_catalog.pg_get_userbyid(datdba) AS \"Owner\", "
            "pg_catalog.pg_encoding_to_char(encoding) AS \"Encoding\" "
            "FROM pg_catalog.pg_database ORDER BY datname;"
        ),
        r"\du": (
            "SELECT rolname AS \"Role name\", "
            "CASE WHEN rolsuper THEN 'Superuser' ELSE '' END AS \"Attributes\" "
            "FROM pg_catalog.pg_roles ORDER BY rolname;"
        ),
        r"\dn": (
            "SELECT nspname AS \"Name\", pg_catalog.pg_get_userbyid(nspowner) AS \"Owner\" "
            "FROM pg_catalog.pg_namespace "
            "WHERE nspname NOT LIKE 'pg_%' AND nspname <> 'information_schema' "
            "ORDER BY nspname;"
        ),
        r"\df": (
            "SELECT routine_name AS \"Name\", routine_type AS \"Type\", "
            "data_type AS \"Result data type\" "
            "FROM information_schema.routines "
            "WHERE routine_schema NOT IN ('pg_catalog', 'information_schema') "
            "ORDER BY routine_name;"
        ),
        r"\dv": (
            "SELECT schemaname AS \"Schema\", viewname AS \"Name\", "
            "viewowner AS \"Owner\" FROM pg_views "
            "WHERE schemaname NOT IN ('pg_catalog', 'information_schema') "
            "ORDER BY viewname;"
        ),
        r"\di": (
            "SELECT schemaname AS \"Schema\", indexname AS \"Name\", "
            "tablename AS \"Table\" FROM pg_indexes "
            "WHERE schemaname NOT IN ('pg_catalog', 'information_schema') "
            "ORDER BY indexname;"
        ),
        r"\ds": (
            "SELECT sequence_schema AS \"Schema\", sequence_name AS \"Name\" "
            "FROM information_schema.sequences ORDER BY sequence_name;"
        ),
    }

    # \d with args → describe table
    if token == r"\d" and args:
        tbl_name = args.strip('"').strip("'").strip()
        sql = (
            f"SELECT column_name AS \"Column\", data_type AS \"Type\", "
            f"is_nullable AS \"Nullable\", column_default AS \"Default\" "
            f"FROM information_schema.columns "
            f"WHERE table_name = '{tbl_name}' "
            f"AND table_schema = 'public' ORDER BY ordinal_position;"
        )
    elif token in meta_sql_map and meta_sql_map[token] is not None:
        sql = meta_sql_map[token]
    elif token == r"\d" and not args:
        # \d without args → list all relations
        sql = (
            "SELECT schemaname AS \"Schema\", tablename AS \"Name\", "
            "'table' AS \"Type\", tableowner AS \"Owner\" "
            "FROM pg_tables "
            "WHERE schemaname NOT IN ('pg_catalog', 'information_schema') "
            "UNION ALL "
            "SELECT schemaname, viewname, 'view', viewowner "
            "FROM pg_views "
            "WHERE schemaname NOT IN ('pg_catalog', 'information_schema') "
            "UNION ALL "
            "SELECT sequence_schema, sequence_name, 'sequence', NULL "
            "FROM information_schema.sequences "
            "ORDER BY 2;"
        )
    else:
        await send_msg(writer, {
            "type":    "ERROR",
            "message": f"Unsupported meta command: {raw_cmd}",
        })
        return

    try:
        rows, cols = execute_query(database, sql)
        await send_msg(writer, {
            "type":     "META_RESULT",
            "rows":     rows,
            "columns":  cols,
            "rowcount": len(rows),
        })
    except Exception as e:
        await send_msg(writer, {"type": "ERROR", "message": str(e)})


async def handle_lock_request(msg: dict, writer: asyncio.StreamWriter):
    """Grant an exclusive WRITE lock after recalling ALL current holders."""
    client_id = msg["client_id"]
    database  = msg["database"]
    table     = msg["table"]
    pks       = msg["pks"]

    print(f"[remote] LOCK_REQUEST WRITE from {client_id} on {table} PKs={pks}")

    # Collect every holder (readers + writer) that is not the requester
    holders = _collect_holders_for_write(client_id, database, table, pks)

    # Recall all of them (in parallel would be nicer; serial is simpler and safe)
    for holder, held_pks in holders.items():
        await _recall_and_wait(holder, database, table, held_pks, client_id)

    # Grant exclusive WRITE lock
    for pk in pks:
        entry = _get_or_create_lock(database, table, pk)
        entry.write   = client_id
        entry.readers = set()   # clear all readers — WRITE is exclusive

    await send_msg(writer, {"type": "LOCK_GRANT", "table": table, "pks": pks})
    print(f"[remote] LOCK_GRANT WRITE → {client_id} on {table} PKs={pks}")


async def handle_lock_release(msg: dict, writer: asyncio.StreamWriter):
    """Apply pending changes and release the lock."""
    client_id       = msg.get("client_id", "")
    database        = msg["database"]
    table           = msg["table"]
    pks             = msg["pks"]
    pending_changes = msg.get("pending_changes", [])

    print(f"[remote] LOCK_RELEASE from {client_id}: {len(pending_changes)} change(s)")

    ok = apply_changes(database, pending_changes)

    # Invalidate caches on all other clients for affected tables (even on failure,
    # so stale readers don't cache data that may be partially changed)
    affected_tables = {ch.get("table", table) for ch in pending_changes}
    for tbl in affected_tables:
        await _notify_cache_invalidate(client_id, database, tbl)

    _clear_locks_for_client(client_id, database, table, pks)

    await send_msg(writer, {
        "type":     "SYNC_ACK",
        "database": database,
        "table":    table,
        "pks":      pks,
        "success":  ok,
    })

    # Signal any waiting requester
    event_key = (database, table, "recall", client_id)
    if event_key in _lock_events:
        _lock_events[event_key].set()


async def handle_apply_changes(msg: dict, writer: asyncio.StreamWriter):
    """
    APPLY_CHANGES — receives a batch of SQL statements (from lazy inserts or
    pre-flush) and applies them to the remote DB.
    Source can be 'INSERT' or 'TYPE_C' or 'PRE_FLUSH'.
    """
    database = msg["database"]
    changes  = msg.get("changes", [])
    source   = msg.get("source", "UNKNOWN")

    print(f"[remote] APPLY_CHANGES source={source} db={database} "
          f"count={len(changes)}")

    ok = apply_changes(database, changes)
    if not ok:
        await send_msg(writer, {
            "type":    "ERROR",
            "message": f"Failed to apply {len(changes)} change(s) to '{database}'",
        })
        return
    tables_affected = {ch.get("table", "") for ch in changes if ch.get("table")}
    for tbl in tables_affected:
        await _notify_cache_invalidate(None, database, tbl)
    await send_msg(writer, {
        "type":    "APPLY_ACK",
        "database": database,
        "count":   len(changes),
    })


async def _notify_cache_invalidate(sender_id: str | None, database: str, table: str):
    """Tell all other cached clients that a table has been updated."""
    for cid, cache in client_cache_map.items():
        if cid == sender_id:
            continue
        if (database, table) in cache:
            w = clients.get(cid)
            if w:
                try:
                    await send_msg(w, {
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
        elif msg_type == "APPLY_CHANGES":
            await handle_apply_changes(msg, writer)
        elif msg_type == "META_QUERY":
            await handle_meta_query(msg, writer)
        else:
            await send_msg(writer, {"type": "ERROR", "message": f"Unknown type: {msg_type}"})

    # Cleanup on disconnect
    if client_id_seen:
        clients.pop(client_id_seen, None)
        subscriptions.pop(client_id_seen, None)
        # Release all locks held by this client
        for key in list(row_locks.keys()):
            entry = row_locks[key]
            if entry.write == client_id_seen:
                entry.write = None
            entry.readers.discard(client_id_seen)
            if entry.write is None and not entry.readers:
                del row_locks[key]
        client_cache_map.pop(client_id_seen, None)
    _writer_locks.pop(id(writer), None)
    writer.close()


# ─── Entry Point ──────────────────────────────────────────────────────────────

async def main():
    server = await asyncio.start_server(handle_client, LISTEN_HOST, LISTEN_PORT)
    print(f"[remote] Listening on {LISTEN_HOST}:{LISTEN_PORT}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
