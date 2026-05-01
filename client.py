"""
client.py — Client Brain
========================
Listens on port 5001. Sits in front of the local PostgreSQL instance.

Phase 1: INIT_DB — create local database, replicate schema, apply permissions.
Phase 2: CACHE_ROWS, CACHE_CHECK, WRITE_LOCAL, RECALL_LOCK, CACHE_INVALIDATE,
         INSERT_LOCAL, FLUSH_PENDING, FLUSH_DONE.

Lazy INSERT flow
----------------
INSERT_LOCAL  → apply to local DB, append to pending_inserts (retry_count=0).
Background task (every 1 s) opens a fresh connection to remote.py and sends
APPLY_CHANGES with pending_inserts.  On failure increments retry_count.
After 5 failures for an entry, INSERT_FLUSH_FAILED is pushed to all proxy
writers so the user sees an alert.  Entry stays in queue and keeps retrying.

FLUSH_PENDING (urgent pre-flush before a Type A query)
------------------------------------------------------
Proxy sends FLUSH_PENDING {database, tables}.
Client returns FLUSH_ACK {insert_changes, type_c_changes}.
Proxy forwards changes to remote as APPLY_CHANGES + (if type_c) LOCK_RELEASE.
Proxy then sends FLUSH_DONE {database, tables} so client can clear those entries.
"""

import asyncio
import json
import os
import psycopg2
import psycopg2.extras

# ─── Configuration ────────────────────────────────────────────────────────────

CONFIG_FILE = "client_config.json"
if os.path.exists(CONFIG_FILE):
    with open(CONFIG_FILE, "r") as f:
        config = json.load(f)
else:
    config = {
        "local_db_host":            "localhost",
        "local_db_port":            5432,
        "local_superuser":          "postgres",
        "local_superuser_password": "postgres",
        "listen_host":              "localhost",
        "listen_port":              5001,
        "remote_host":              "localhost",
        "remote_port":              5000,
    }
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=4)

LOCAL_HOST               = config.get("local_db_host", "localhost")
LOCAL_PORT               = config.get("local_db_port", 5432)
LOCAL_SUPERUSER          = config.get("local_superuser", "postgres")
LOCAL_SUPERUSER_PASSWORD = config.get("local_superuser_password", "postgres")
LISTEN_HOST              = config.get("listen_host", "localhost")
LISTEN_PORT              = config.get("listen_port", 5001)
REMOTE_HOST              = config.get("remote_host", "localhost")
REMOTE_PORT              = config.get("remote_port", 5000)

# ─── Global State ─────────────────────────────────────────────────────────────

schema_registry:   dict = {}   # database → schema dict
local_cache_index: dict = {}   # (database, table) → set of pk values (str)
local_locks:       dict = {}   # (database, table, pk_str) → "READ" | "WRITE"
pending_changes:   list = []   # type-C changes: {sql, database, table, pks}
query_cache:       dict = {}   # fingerprint → {table, pks, database, pk_cols}

# Lazy INSERT queue — each entry: {sql, database, table, retry_count}
pending_inserts: list = []

# All currently connected proxy StreamWriters (for unsolicited notifications)
proxy_writers: set = set()

# Entries being flushed via FLUSH_PENDING (in-flight, not yet confirmed done)
# key: (database, table) → {inserts: list, type_c: list}
_in_flight_flush: dict = {}


# ─── Low-level DB helpers ─────────────────────────────────────────────────────

def get_superuser_conn(database: str = "postgres", autocommit: bool = False):
    conn = psycopg2.connect(
        host=LOCAL_HOST, port=LOCAL_PORT,
        dbname=database,
        user=LOCAL_SUPERUSER, password=LOCAL_SUPERUSER_PASSWORD,
    )
    conn.autocommit = autocommit
    return conn


def db_exists(database: str) -> bool:
    conn = get_superuser_conn(autocommit=True)
    cur  = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (database,))
    exists = cur.fetchone() is not None
    cur.close(); conn.close()
    return exists


def create_local_database(database: str):
    if db_exists(database):
        print(f"[client] Database '{database}' already exists — skipping CREATE.")
        return
    conn = get_superuser_conn(autocommit=True)
    cur  = conn.cursor()
    cur.execute(f'CREATE DATABASE "{database}";')
    cur.close(); conn.close()
    print(f"[client] Created local database '{database}'.")


def user_exists_locally(user: str) -> bool:
    conn = get_superuser_conn(autocommit=True)
    cur  = conn.cursor()
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s;", (user,))
    exists = cur.fetchone() is not None
    cur.close(); conn.close()
    return exists


def ensure_local_user(user: str):
    if user_exists_locally(user):
        return
    conn = get_superuser_conn(autocommit=True)
    cur  = conn.cursor()
    cur.execute(f'CREATE ROLE "{user}" LOGIN;')
    cur.close(); conn.close()
    print(f"[client] Created local role '{user}'.")


# ─── Schema Replication ───────────────────────────────────────────────────────

def col_definition(col: dict) -> str:
    name    = col["column_name"]
    dtype   = col["data_type"]
    nullable= col["is_nullable"]
    default = col["column_default"]
    max_len = col.get("character_maximum_length")

    type_map = {
        "character varying": f"VARCHAR({max_len})" if max_len else "TEXT",
        "character":         f"CHAR({max_len})"    if max_len else "CHAR",
        "integer":           "INTEGER",
        "bigint":            "BIGINT",
        "smallint":          "SMALLINT",
        "numeric":           "NUMERIC",
        "real":              "REAL",
        "double precision":  "DOUBLE PRECISION",
        "boolean":           "BOOLEAN",
        "text":              "TEXT",
        "date":              "DATE",
        "timestamp without time zone": "TIMESTAMP",
        "timestamp with time zone":    "TIMESTAMPTZ",
        "uuid":              "UUID",
        "json":              "JSON",
        "jsonb":             "JSONB",
        "bytea":             "BYTEA",
    }
    sql_type = type_map.get(dtype, dtype.upper())
    parts = [f'"{name}" {sql_type}']
    if nullable == "NO":
        parts.append("NOT NULL")
    if default and "nextval" not in default:
        parts.append(f"DEFAULT {default}")
    return " ".join(parts)


def create_tables(database: str, schema: dict):
    conn = get_superuser_conn(database=database)
    conn.autocommit = False
    cur  = conn.cursor()

    for table, defn in schema.items():
        columns      = defn["columns"]
        primary_keys = defn["primary_keys"]
        unique_cols  = defn["unique"]
        col_defs     = [col_definition(c) for c in columns]
        if primary_keys:
            pk_cols = ", ".join(f'"{c}"' for c in primary_keys)
            col_defs.append(f"PRIMARY KEY ({pk_cols})")
        for uc in unique_cols:
            if uc not in primary_keys:
                col_defs.append(f'UNIQUE ("{uc}")')
        ddl = (
            f'CREATE TABLE IF NOT EXISTS "{table}" (\n'
            + ",\n  ".join(col_defs)
            + "\n);"
        )
        print(f"[client]   Creating table '{table}' …")
        try:
            cur.execute(ddl)
        except psycopg2.Error as e:
            print(f"[client]   ERROR creating '{table}': {e}")
            conn.rollback(); cur = conn.cursor(); continue

    for table, defn in schema.items():
        for fk in defn["foreign_keys"]:
            alter_ddl = (
                f'ALTER TABLE "{table}" ADD FOREIGN KEY ("{fk["column_name"]}") '
                f'REFERENCES "{fk["foreign_table"]}" ("{fk["foreign_column"]}");'
            )
            try:
                cur.execute(alter_ddl)
            except psycopg2.Error as e:
                print(f"[client]   Skipping FK for '{table}': {e}")
                conn.rollback(); cur = conn.cursor(); continue

    conn.commit(); cur.close(); conn.close()


def create_indexes(database: str, schema: dict):
    conn = get_superuser_conn(database=database, autocommit=True)
    cur  = conn.cursor()
    for table, defn in schema.items():
        for idx in defn["indexes"]:
            idx_def = idx["indexdef"]
            if "CREATE UNIQUE INDEX" in idx_def:
                idx_def = idx_def.replace("CREATE UNIQUE INDEX",
                                          "CREATE UNIQUE INDEX IF NOT EXISTS", 1)
            elif "CREATE INDEX" in idx_def:
                idx_def = idx_def.replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS", 1)
            try:
                cur.execute(idx_def)
                print(f"[client]   Index '{idx['indexname']}' on '{table}' created.")
            except psycopg2.Error as e:
                print(f"[client]   Skipping index '{idx['indexname']}': {e}")
    cur.close(); conn.close()


def apply_permissions(database: str, user: str, permissions: dict):
    conn = get_superuser_conn(database=database, autocommit=True)
    cur  = conn.cursor()
    for table, privs in permissions.items():
        priv_str = ", ".join(privs)
        sql = f'GRANT {priv_str} ON "{table}" TO "{user}";'
        try:
            cur.execute(sql)
            print(f"[client]   GRANT {priv_str} ON {table} TO {user}")
        except psycopg2.Error as e:
            print(f"[client]   Permission error on {table}: {e}")
    cur.close(); conn.close()


def init_db(database: str, user: str, schema: dict, permissions: dict):
    print(f"[client] INIT_DB → database='{database}' user='{user}'")
    ensure_local_user(user)
    create_local_database(database)
    create_tables(database, schema)
    create_indexes(database, schema)
    apply_permissions(database, user, permissions)
    schema_registry[database] = schema
    local_cache_index[(database, "__init__")] = set()
    print(f"[client] INIT_DB complete for '{database}'.")


# ─── Cache helpers ────────────────────────────────────────────────────────────

def cache_rows(database: str, table: str, rows: list[dict],
               pks: list, pk_cols: list, lock_type: str, fingerprint: str):
    if not rows:
        return
    conn = get_superuser_conn(database=database)
    conn.autocommit = False
    cur  = conn.cursor()
    col_names    = list(rows[0].keys())
    quoted_cols  = ", ".join(f'"{c}"' for c in col_names)
    placeholders = ", ".join(["%s"] * len(col_names))
    inserted = 0
    for row in rows:
        values = [row[c] for c in col_names]
        sql = (
            f'INSERT INTO "{table}" ({quoted_cols}) VALUES ({placeholders})'
            f' ON CONFLICT DO NOTHING;'
        )
        try:
            cur.execute(sql, values)
            inserted += cur.rowcount
        except psycopg2.Error as e:
            print(f"[client]   Row insert error: {e}")
            conn.rollback(); cur = conn.cursor()
    conn.commit(); cur.close(); conn.close()

    key = (database, table)
    local_cache_index.setdefault(key, set())
    pk_set = set(str(p) for p in pks)
    local_cache_index[key].update(pk_set)
    for pk in pks:
        local_locks[(database, table, str(pk))] = lock_type
    if fingerprint:
        query_cache[fingerprint] = {
            "database": database, "table": table,
            "pks": list(pk_set), "pk_cols": pk_cols,
        }
    print(f"[client] Cached {inserted} new row(s) in '{table}'")


def check_cache(fingerprint: str) -> dict | None:
    return query_cache.get(fingerprint)


def run_local_query(database: str, sql: str) -> tuple[list[dict], list[str]]:
    conn = get_superuser_conn(database=database)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql)
    rows      = [dict(r) for r in cur.fetchall()]
    col_names = [desc[0] for desc in cur.description] if cur.description else []
    cur.close(); conn.close()
    return rows, col_names


def apply_write_local(database: str, table: str, sql: str,
                      pk_cols: list, pks: list) -> int:
    conn = get_superuser_conn(database=database)
    conn.autocommit = False
    cur  = conn.cursor()
    cur.execute(sql)
    count = cur.rowcount
    conn.commit(); cur.close(); conn.close()
    for pk in pks:
        local_locks[(database, table, str(pk))] = "WRITE"
    pending_changes.append({"sql": sql, "database": database,
                             "table": table, "pks": pks})
    print(f"[client] Write applied locally ({count} row(s)), pending={len(pending_changes)}")
    return count


def apply_insert_local(database: str, table: str, sql: str) -> int:
    """Execute INSERT on local DB and queue for lazy remote sync."""
    conn = get_superuser_conn(database=database)
    conn.autocommit = False
    cur  = conn.cursor()
    cur.execute(sql)
    count = cur.rowcount
    conn.commit(); cur.close(); conn.close()
    pending_inserts.append({
        "sql":         sql,
        "database":    database,
        "table":       table,
        "retry_count": 0,
    })
    print(f"[client] INSERT applied locally ({count} row(s)), "
          f"queued for remote sync (total pending={len(pending_inserts)})")
    return count


def flush_pending_for_tables(database: str, tables: list) -> dict:
    """
    Collect pending inserts and type-C changes for the given tables.
    Moves them into _in_flight_flush so FLUSH_DONE can clean them up.
    Returns {insert_changes: [...], type_c_changes: [...], write_pks_by_table: {...}}
    """
    insert_items = [e for e in pending_inserts
                    if e["database"] == database and e["table"] in tables]
    type_c_items = [e for e in pending_changes
                    if e["database"] == database and e["table"] in tables]

    insert_changes = [{"sql": e["sql"], "table": e["table"]} for e in insert_items]
    type_c_changes = [{"sql": e["sql"], "table": e["table"]} for e in type_c_items]

    # Group write PKs by table for lock release
    write_pks_by_table: dict = {}
    for e in type_c_items:
        write_pks_by_table.setdefault(e["table"], []).extend(e.get("pks", []))

    # Store in-flight so FLUSH_DONE knows what to remove
    for tbl in tables:
        _in_flight_flush[(database, tbl)] = {
            "inserts": [e for e in insert_items if e["table"] == tbl],
            "type_c":  [e for e in type_c_items  if e["table"] == tbl],
        }

    return {
        "insert_changes":    insert_changes,
        "type_c_changes":    type_c_changes,
        "write_pks_by_table": write_pks_by_table,
    }


def confirm_flush_done(database: str, tables: list):
    """Called after proxy confirms changes were applied to remote."""
    for tbl in tables:
        key = (database, tbl)
        flight = _in_flight_flush.pop(key, None)
        if flight is None:
            continue
        # Remove from pending_inserts
        for e in flight["inserts"]:
            try:
                pending_inserts.remove(e)
            except ValueError:
                pass
        # Remove from pending_changes and clear locks
        for e in flight["type_c"]:
            try:
                pending_changes.remove(e)
            except ValueError:
                pass
            for pk in e.get("pks", []):
                local_locks.pop((database, tbl, str(pk)), None)
        # Invalidate local cache for the table
        local_cache_index.pop((database, tbl), None)
        to_del = [fp for fp, v in query_cache.items()
                  if v["database"] == database and v["table"] == tbl]
        for fp in to_del:
            del query_cache[fp]


def flush_pending_for_recall(database: str, table: str, pks: list) -> list[dict]:
    """Used when remote RECALL_LOCK arrives (via proxy). Returns type-C changes."""
    relevant = [c for c in pending_changes
                if c["database"] == database and c["table"] == table]
    for c in relevant:
        pending_changes.remove(c)
    for pk in pks:
        local_locks.pop((database, table, str(pk)), None)
    local_cache_index.pop((database, table), None)
    to_del = [fp for fp, v in query_cache.items()
              if v["database"] == database and v["table"] == table]
    for fp in to_del:
        del query_cache[fp]
    return relevant


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


# ─── Background INSERT Flush Task ─────────────────────────────────────────────

async def background_insert_flush():
    """
    Every 1 second, push all pending inserts to remote.py via APPLY_CHANGES.
    On failure: increment retry_count.  After > 5 failures, alert proxy writers.
    The entry is NEVER dropped — it keeps retrying until it succeeds.
    """
    while True:
        await asyncio.sleep(1.0)
        if not pending_inserts:
            continue

        # Group by database
        db_groups: dict[str, list] = {}
        for e in list(pending_inserts):
            db_groups.setdefault(e["database"], []).append(e)

        for db, entries in db_groups.items():
            changes = [{"sql": e["sql"], "table": e["table"]} for e in entries]
            success = False
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(REMOTE_HOST, REMOTE_PORT), timeout=5.0
                )
                msg_str = json.dumps({
                    "type":     "APPLY_CHANGES",
                    "database": db,
                    "changes":  changes,
                    "source":   "INSERT",
                }) + "\n"
                writer.write(msg_str.encode())
                await writer.drain()
                line = await asyncio.wait_for(reader.readline(), timeout=5.0)
                resp = json.loads(line.decode())
                writer.close()
                if resp.get("type") == "APPLY_ACK":
                    for e in entries:
                        try:
                            pending_inserts.remove(e)
                        except ValueError:
                            pass
                    success = True
                    print(f"[client] BG flush: {len(entries)} insert(s) → remote '{db}'")
                else:
                    print(f"[client] BG flush: remote returned {resp}")
            except Exception as ex:
                print(f"[client] BG flush failed for '{db}': {ex}")

            if not success:
                for e in entries:
                    e["retry_count"] = e.get("retry_count", 0) + 1

                overdue = [e for e in entries if e.get("retry_count", 0) > 5]
                if overdue:
                    notif = json.dumps({
                        "type":    "INSERT_FLUSH_FAILED",
                        "message": (
                            f"{len(overdue)} INSERT(s) in '{overdue[0]['table']}' "
                            f"failed to sync after {overdue[0]['retry_count']} retries. "
                            "Will keep retrying."
                        ),
                    }) + "\n"
                    dead: set = set()
                    for pw in list(proxy_writers):
                        try:
                            pw.write(notif.encode())
                            await pw.drain()
                        except Exception:
                            dead.add(pw)
                    proxy_writers.difference_update(dead)


# ─── Connection Handler ────────────────────────────────────────────────────────

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    print(f"[client] New connection from {addr}")

    while True:
        msg = await recv_msg(reader)
        if msg is None:
            print(f"[client] Connection closed by {addr}")
            break

        msg_type = msg.get("type")

        # ── Phase 1 ───────────────────────────────────────────────────────────
        if msg_type == "INIT":
            client_id = msg["client_id"]
            print(f"[client] INIT from client_id={client_id}")
            proxy_writers.add(writer)   # track for INSERT_FLUSH_FAILED alerts
            await send_msg(writer, {"type": "INIT_ACK", "client_id": client_id})

        elif msg_type == "INIT_DB":
            try:
                init_db(
                    database    = msg["database"],
                    user        = msg["user"],
                    schema      = msg["schema"],
                    permissions = msg["permissions"],
                )
                await send_msg(writer, {
                    "type":     "INIT_DB_ACK",
                    "database": msg["database"],
                    "status":   "ok",
                })
            except Exception as e:
                await send_msg(writer, {"type": "ERROR", "message": str(e)})

        # ── Phase 2: cache check ───────────────────────────────────────────────
        elif msg_type == "CACHE_CHECK":
            fingerprint = msg["fingerprint"]
            database    = msg["database"]
            entry = check_cache(fingerprint)
            if entry:
                sql = msg["sql"]
                try:
                    rows, cols = run_local_query(database, sql)
                    await send_msg(writer, {
                        "type":        "CACHE_HIT",
                        "fingerprint": fingerprint,
                        "rows":        rows,
                        "columns":     cols,
                        "rowcount":    len(rows),
                    })
                except Exception as e:
                    await send_msg(writer, {
                        "type":        "CACHE_MISS",
                        "fingerprint": fingerprint,
                        "reason":      str(e),
                    })
            else:
                await send_msg(writer, {"type": "CACHE_MISS", "fingerprint": fingerprint})

        # ── Phase 2: store rows from remote ───────────────────────────────────
        elif msg_type == "CACHE_ROWS":
            try:
                cache_rows(
                    database    = msg["database"],
                    table       = msg["table"],
                    rows        = msg["rows"],
                    pks         = msg["pks"],
                    pk_cols     = msg["pk_cols"],
                    lock_type   = msg.get("lock_type", "READ"),
                    fingerprint = msg.get("fingerprint", ""),
                )
                await send_msg(writer, {"type": "CACHE_ACK", "table": msg["table"]})
            except Exception as e:
                await send_msg(writer, {"type": "ERROR", "message": str(e)})

        # ── Phase 2: apply type-C write locally ───────────────────────────────
        elif msg_type == "WRITE_LOCAL":
            try:
                count = apply_write_local(
                    database = msg["database"],
                    table    = msg["table"],
                    sql      = msg["sql"],
                    pk_cols  = msg["pk_cols"],
                    pks      = msg["pks"],
                )
                await send_msg(writer, {
                    "type":     "WRITE_ACK",
                    "rowcount": count,
                    "table":    msg["table"],
                })
            except Exception as e:
                await send_msg(writer, {"type": "ERROR", "message": str(e)})

        # ── Phase 2: lazy INSERT — local first ────────────────────────────────
        elif msg_type == "INSERT_LOCAL":
            try:
                count = apply_insert_local(
                    database = msg["database"],
                    table    = msg["table"],
                    sql      = msg["sql"],
                )
                await send_msg(writer, {
                    "type":     "INSERT_ACK",
                    "rowcount": count,
                    "table":    msg["table"],
                })
            except Exception as e:
                await send_msg(writer, {"type": "ERROR", "message": str(e)})

        # ── Phase 2: pre-flush before Type A (or voluntary lock release) ──────
        elif msg_type == "FLUSH_PENDING":
            try:
                database = msg["database"]
                tables   = msg.get("tables", [])
                result   = flush_pending_for_tables(database, tables)
                await send_msg(writer, {
                    "type":              "FLUSH_ACK",
                    "database":          database,
                    "tables":            tables,
                    "insert_changes":    result["insert_changes"],
                    "type_c_changes":    result["type_c_changes"],
                    "write_pks_by_table": result["write_pks_by_table"],
                })
            except Exception as e:
                await send_msg(writer, {"type": "ERROR", "message": str(e)})

        # ── Phase 2: proxy confirms changes reached remote ────────────────────
        elif msg_type == "FLUSH_DONE":
            try:
                database = msg["database"]
                tables   = msg.get("tables", [])
                confirm_flush_done(database, tables)
                await send_msg(writer, {"type": "FLUSH_DONE_ACK", "tables": tables})
            except Exception as e:
                await send_msg(writer, {"type": "ERROR", "message": str(e)})

        # ── Phase 2: remote recalling our lock (via proxy) ────────────────────
        elif msg_type == "RECALL_LOCK":
            # Proxy forwards RECALL_LOCK here; we return the pending type-C changes
            database = msg["database"]
            table    = msg["table"]
            pks      = msg["pks"]
            print(f"[client] RECALL_LOCK for {table} PKs={pks}")
            changes = flush_pending_for_recall(database, table, pks)
            await send_msg(writer, {
                "type":            "LOCK_RELEASE",
                "database":        database,
                "table":           table,
                "pks":             pks,
                "pending_changes": changes,
            })
            print(f"[client] LOCK_RELEASE sent with {len(changes)} change(s)")

        # ── Phase 2: cache invalidation ───────────────────────────────────────
        elif msg_type == "CACHE_INVALIDATE":
            database = msg["database"]
            table    = msg["table"]
            local_cache_index.pop((database, table), None)
            to_del = [fp for fp, v in query_cache.items()
                      if v["database"] == database and v["table"] == table]
            for fp in to_del:
                del query_cache[fp]
            print(f"[client] Cache invalidated for {database}.{table}")
            await send_msg(writer, {"type": "CACHE_INVALIDATE_ACK", "table": table})

        else:
            await send_msg(writer, {
                "type":    "ERROR",
                "message": f"Unknown message type: {msg_type}",
            })

    proxy_writers.discard(writer)
    writer.close()


# ─── Entry Point ──────────────────────────────────────────────────────────────

async def main():
    server = await asyncio.start_server(handle_client, LISTEN_HOST, LISTEN_PORT)
    print(f"[client] Listening on {LISTEN_HOST}:{LISTEN_PORT}")
    print(f"[client] Remote sync target: {REMOTE_HOST}:{REMOTE_PORT}")
    asyncio.create_task(background_insert_flush())
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
