"""
client.py — Client Brain
========================
Listens on port 5001. Sits in front of the local PostgreSQL instance.
<<<<<<< HEAD

Phase 1: INIT_DB — create local database, replicate schema, apply permissions.
Phase 2: CACHE_ROWS, CACHE_CHECK, WRITE_LOCAL, RECALL_LOCK, CACHE_INVALIDATE.
=======
<<<<<<< HEAD
Responsibilities (Phase 1):
  - Accept proxy connections and handshakes
  - Handle INIT_DB: create local database, recreate schema, apply permissions
  - Track schema registry and local state
=======

Phase 1: INIT_DB — create local database, replicate schema, apply permissions.
Phase 2: CACHE_ROWS, CACHE_CHECK, WRITE_LOCAL, RECALL_LOCK, CACHE_INVALIDATE.
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
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
        "local_db_host": "localhost",
        "local_db_port": 5432,
        "local_superuser": "postgres",
        "local_superuser_password": "postgres",
        "listen_host": "localhost",
        "listen_port": 5001
    }
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=4)

<<<<<<< HEAD
=======
<<<<<<< HEAD
LOCAL_HOST = config.get("local_db_host", "localhost")
LOCAL_PORT = config.get("local_db_port", 5432)
LOCAL_SUPERUSER = config.get("local_superuser", "postgres")
LOCAL_SUPERUSER_PASSWORD = config.get("local_superuser_password", "postgres")
LISTEN_HOST = config.get("listen_host", "localhost")
LISTEN_PORT = config.get("listen_port", 5001)

# ─── Global State ─────────────────────────────────────────────────────────────

local_cache_index: dict = {}   # table → set of cached row PKs
local_locks:       dict = {}   # row_id → lock_info
schema_registry:   dict = {}   # database → schema dict

=======
>>>>>>> 130b6a3 (phase-2)
LOCAL_HOST               = config.get("local_db_host", "localhost")
LOCAL_PORT               = config.get("local_db_port", 5432)
LOCAL_SUPERUSER          = config.get("local_superuser", "postgres")
LOCAL_SUPERUSER_PASSWORD = config.get("local_superuser_password", "postgres")
LISTEN_HOST              = config.get("listen_host", "localhost")
LISTEN_PORT              = config.get("listen_port", 5001)

# ─── Global State ─────────────────────────────────────────────────────────────

schema_registry:   dict = {}   # database → schema dict

# (database, table) → set of pk values (as strings)
local_cache_index: dict = {}

# (database, table, pk_str) → "READ" | "WRITE"
local_locks: dict = {}

# write-ahead log: list of {sql, database, table, pks}
pending_changes: list = []

# fingerprint → {"table": str, "pks": list, "database": str, "pk_cols": list}
query_cache: dict = {}

<<<<<<< HEAD
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)

# ─── Low-level DB helpers ─────────────────────────────────────────────────────

def get_superuser_conn(database: str = "postgres", autocommit: bool = False):
    conn = psycopg2.connect(
<<<<<<< HEAD
        host=LOCAL_HOST, port=LOCAL_PORT,
        dbname=database,
        user=LOCAL_SUPERUSER, password=LOCAL_SUPERUSER_PASSWORD,
=======
<<<<<<< HEAD
        host=LOCAL_HOST,
        port=LOCAL_PORT,
        dbname=database,
        user=LOCAL_SUPERUSER,
        password=LOCAL_SUPERUSER_PASSWORD,
=======
        host=LOCAL_HOST, port=LOCAL_PORT,
        dbname=database,
        user=LOCAL_SUPERUSER, password=LOCAL_SUPERUSER_PASSWORD,
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
    )
    conn.autocommit = autocommit
    return conn


def db_exists(database: str) -> bool:
    conn = get_superuser_conn(autocommit=True)
    cur  = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (database,))
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()
    return exists


def create_local_database(database: str):
    if db_exists(database):
        print(f"[client] Database '{database}' already exists — skipping CREATE.")
        return
    conn = get_superuser_conn(autocommit=True)
    cur  = conn.cursor()
    cur.execute(f'CREATE DATABASE "{database}";')
    cur.close()
    conn.close()
    print(f"[client] Created local database '{database}'.")


def user_exists_locally(user: str) -> bool:
    conn = get_superuser_conn(autocommit=True)
    cur  = conn.cursor()
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s;", (user,))
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()
    return exists


def ensure_local_user(user: str):
<<<<<<< HEAD
=======
<<<<<<< HEAD
    """Create the role locally if it doesn't exist (no password needed for local trust auth)."""
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
    if user_exists_locally(user):
        return
    conn = get_superuser_conn(autocommit=True)
    cur  = conn.cursor()
    cur.execute(f'CREATE ROLE "{user}" LOGIN;')
    cur.close()
    conn.close()
    print(f"[client] Created local role '{user}'.")


# ─── Schema Replication ───────────────────────────────────────────────────────

def col_definition(col: dict) -> str:
<<<<<<< HEAD
=======
<<<<<<< HEAD
    """
    Build a SQL column definition string from the information_schema column dict.
    """
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
    name     = col["column_name"]
    dtype    = col["data_type"]
    nullable = col["is_nullable"]
    default  = col["column_default"]
    max_len  = col.get("character_maximum_length")

<<<<<<< HEAD
=======
<<<<<<< HEAD
    # Map information_schema types to Postgres DDL types
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
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
<<<<<<< HEAD
    if default and "nextval" not in default:
        parts.append(f"DEFAULT {default}")
=======
<<<<<<< HEAD
    if default and "nextval" not in default:   # skip serial sequences — handle separately
        parts.append(f"DEFAULT {default}")

=======
    if default and "nextval" not in default:
        parts.append(f"DEFAULT {default}")
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
    return " ".join(parts)


def create_tables(database: str, schema: dict):
<<<<<<< HEAD
=======
<<<<<<< HEAD
    """
    Recreate every table from the schema dict inside the local database.
    Creates PKs, unique constraints, and foreign keys.
    """
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
    conn = get_superuser_conn(database=database)
    conn.autocommit = False
    cur  = conn.cursor()

    # Pass 1: Create tables without foreign keys
    for table, defn in schema.items():
        columns      = defn["columns"]
        primary_keys = defn["primary_keys"]
        unique_cols  = defn["unique"]

        col_defs = [col_definition(c) for c in columns]
<<<<<<< HEAD
=======
<<<<<<< HEAD

        if primary_keys:
            pk_cols = ", ".join(f'"{c}"' for c in primary_keys)
            col_defs.append(f"PRIMARY KEY ({pk_cols})")

        for uc in unique_cols:
            if uc not in primary_keys:          # don't double-declare PKs as UNIQUE
=======
>>>>>>> 130b6a3 (phase-2)
        if primary_keys:
            pk_cols = ", ".join(f'"{c}"' for c in primary_keys)
            col_defs.append(f"PRIMARY KEY ({pk_cols})")
        for uc in unique_cols:
            if uc not in primary_keys:
<<<<<<< HEAD
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
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
            conn.rollback()
<<<<<<< HEAD
=======
<<<<<<< HEAD
            cur = conn.cursor()   # fresh cursor after error
            continue

    # Pass 2: Add foreign keys
    for table, defn in schema.items():
        foreign_keys = defn["foreign_keys"]
        for fk in foreign_keys:
=======
>>>>>>> 130b6a3 (phase-2)
            cur = conn.cursor()
            continue

    # Pass 2: Add foreign keys via ALTER TABLE (all tables exist by now)
    for table, defn in schema.items():
        for fk in defn["foreign_keys"]:
<<<<<<< HEAD
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
            alter_ddl = (
                f'ALTER TABLE "{table}" ADD FOREIGN KEY ("{fk["column_name"]}") '
                f'REFERENCES "{fk["foreign_table"]}" ("{fk["foreign_column"]}");'
            )
            print(f"[client]   Adding FK to '{table}' on '{fk['column_name']}' …")
            try:
                cur.execute(alter_ddl)
            except psycopg2.Error as e:
                print(f"[client]   Skipping FK for '{table}': {e}")
                conn.rollback()
                cur = conn.cursor()
                continue

    conn.commit()
    cur.close()
    conn.close()


def create_indexes(database: str, schema: dict):
<<<<<<< HEAD
=======
<<<<<<< HEAD
    """Recreate non-primary indexes."""
    conn = get_superuser_conn(database=database, autocommit=True)
    cur  = conn.cursor()

    for table, defn in schema.items():
        for idx in defn["indexes"]:
            idx_def = idx["indexdef"]
            # indexdef already is a full CREATE INDEX statement — execute as-is
            # but use IF NOT EXISTS to be idempotent
=======
>>>>>>> 130b6a3 (phase-2)
    conn = get_superuser_conn(database=database, autocommit=True)
    cur  = conn.cursor()
    for table, defn in schema.items():
        for idx in defn["indexes"]:
            idx_def = idx["indexdef"]
<<<<<<< HEAD
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
            if "CREATE UNIQUE INDEX" in idx_def:
                idx_def = idx_def.replace("CREATE UNIQUE INDEX", "CREATE UNIQUE INDEX IF NOT EXISTS", 1)
            elif "CREATE INDEX" in idx_def:
                idx_def = idx_def.replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS", 1)
            try:
                cur.execute(idx_def)
                print(f"[client]   Index '{idx['indexname']}' on '{table}' created.")
            except psycopg2.Error as e:
                print(f"[client]   Skipping index '{idx['indexname']}': {e}")
<<<<<<< HEAD
=======
<<<<<<< HEAD

=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
    cur.close()
    conn.close()


def apply_permissions(database: str, user: str, permissions: dict):
<<<<<<< HEAD
    conn = get_superuser_conn(database=database, autocommit=True)
    cur  = conn.cursor()
=======
<<<<<<< HEAD
    """
    Grant the same table-level privileges to `user` on the local database.
    permissions = { table_name: [privilege, ...] }
    """
    conn = get_superuser_conn(database=database, autocommit=True)
    cur  = conn.cursor()

=======
    conn = get_superuser_conn(database=database, autocommit=True)
    cur  = conn.cursor()
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
    for table, privs in permissions.items():
        priv_str = ", ".join(privs)
        sql = f'GRANT {priv_str} ON "{table}" TO "{user}";'
        try:
            cur.execute(sql)
            print(f"[client]   GRANT {priv_str} ON {table} TO {user}")
        except psycopg2.Error as e:
            print(f"[client]   Permission error on {table}: {e}")
<<<<<<< HEAD
=======
<<<<<<< HEAD

=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
    cur.close()
    conn.close()


<<<<<<< HEAD
def init_db(database: str, user: str, schema: dict, permissions: dict):
    print(f"[client] INIT_DB → database='{database}' user='{user}'")
=======
<<<<<<< HEAD
# ─── Main Phase-1 Handler ─────────────────────────────────────────────────────

def init_db(database: str, user: str, schema: dict, permissions: dict):
    """
    Full Phase-1 local setup:
      1. Ensure local user role exists
      2. Create local database
      3. Recreate schema (tables, constraints)
      4. Recreate indexes
      5. Apply permissions
      6. Register in schema_registry
    """
    print(f"[client] INIT_DB → database='{database}' user='{user}'")

=======
def init_db(database: str, user: str, schema: dict, permissions: dict):
    print(f"[client] INIT_DB → database='{database}' user='{user}'")
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
    ensure_local_user(user)
    create_local_database(database)
    create_tables(database, schema)
    create_indexes(database, schema)
    apply_permissions(database, user, permissions)
<<<<<<< HEAD
=======
<<<<<<< HEAD

    schema_registry[database] = schema
    local_cache_index[database] = {}   # no data cached yet

    print(f"[client] INIT_DB complete for '{database}'.")


# ─── Message I/O ──────────────────────────────────────────────────────────────

async def send_msg(writer: asyncio.StreamWriter, msg: dict):
    data = (json.dumps(msg) + "\n").encode()
=======
>>>>>>> 130b6a3 (phase-2)
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
            conn.rollback()
            cur = conn.cursor()

    conn.commit()
    cur.close()
    conn.close()

    key = (database, table)
    local_cache_index.setdefault(key, set())
    pk_set = set(str(p) for p in pks)
    local_cache_index[key].update(pk_set)

    for pk in pks:
        local_locks[(database, table, str(pk))] = lock_type

    if fingerprint:
        query_cache[fingerprint] = {
            "database": database,
            "table":    table,
            "pks":      list(pk_set),
            "pk_cols":  pk_cols,
        }

    print(f"[client] Cached {inserted} new row(s) in '{table}' "
          f"(fp={fingerprint[:8] if fingerprint else 'none'})")


def check_cache(fingerprint: str) -> dict | None:
    return query_cache.get(fingerprint)


def run_local_query(database: str, sql: str) -> tuple[list[dict], list[str]]:
    conn = get_superuser_conn(database=database)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql)
    rows      = [dict(r) for r in cur.fetchall()]
    col_names = [desc[0] for desc in cur.description] if cur.description else []
    cur.close()
    conn.close()
    return rows, col_names


def apply_write_local(database: str, table: str, sql: str,
                      pk_cols: list, pks: list) -> int:
    conn = get_superuser_conn(database=database)
    conn.autocommit = False
    cur  = conn.cursor()
    cur.execute(sql)
    count = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()

    for pk in pks:
        local_locks[(database, table, str(pk))] = "WRITE"

    pending_changes.append({"sql": sql, "database": database, "table": table, "pks": pks})
    print(f"[client] Write applied locally ({count} row(s)), pending={len(pending_changes)}")
    return count


def flush_pending(database: str, table: str, pks: list) -> list[dict]:
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
<<<<<<< HEAD
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
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

<<<<<<< HEAD
        # ── Phase 1 ───────────────────────────────────────────────────────────
=======
<<<<<<< HEAD
=======
        # ── Phase 1 ───────────────────────────────────────────────────────────
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
        if msg_type == "INIT":
            client_id = msg["client_id"]
            print(f"[client] INIT from client_id={client_id}")
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
<<<<<<< HEAD
=======
<<<<<<< HEAD
                await send_msg(writer, {
                    "type":    "ERROR",
                    "message": str(e),
                })
=======
>>>>>>> 130b6a3 (phase-2)
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

        # ── Phase 2: apply write locally ──────────────────────────────────────
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

        # ── Phase 2: remote is recalling our lock ──────────────────────────────
        elif msg_type == "RECALL_LOCK":
            database = msg["database"]
            table    = msg["table"]
            pks      = msg["pks"]
            print(f"[client] RECALL_LOCK for {table} PKs={pks}")
            changes = flush_pending(database, table, pks)
            await send_msg(writer, {
                "type":            "LOCK_RELEASE",
                "database":        database,
                "table":           table,
                "pks":             pks,
                "pending_changes": changes,
            })
            print(f"[client] LOCK_RELEASE sent with {len(changes)} change(s)")

        # ── Phase 2: cache invalidation after remote INSERT ────────────────────
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
<<<<<<< HEAD
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)

        else:
            await send_msg(writer, {
                "type":    "ERROR",
                "message": f"Unknown message type: {msg_type}"
            })

    writer.close()


# ─── Entry Point ──────────────────────────────────────────────────────────────

async def main():
    server = await asyncio.start_server(handle_client, LISTEN_HOST, LISTEN_PORT)
    print(f"[client] Listening on {LISTEN_HOST}:{LISTEN_PORT}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
