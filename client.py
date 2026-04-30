"""
client.py — Client Brain
========================
Listens on port 5001. Sits in front of the local PostgreSQL instance.
Responsibilities (Phase 1):
  - Accept proxy connections and handshakes
  - Handle INIT_DB: create local database, recreate schema, apply permissions
  - Track schema registry and local state
"""

import asyncio
import json
import psycopg2
import psycopg2.extras

# ─── Configuration ────────────────────────────────────────────────────────────

LOCAL_HOST     = "localhost"
LOCAL_PORT     = 5432          # local Postgres (could be a different port, e.g. 5433)
LOCAL_SUPERUSER          = "postgres"
LOCAL_SUPERUSER_PASSWORD = "postgres"
LISTEN_HOST = "localhost"
LISTEN_PORT = 5001

# ─── Global State ─────────────────────────────────────────────────────────────

local_cache_index: dict = {}   # table → set of cached row PKs
local_locks:       dict = {}   # row_id → lock_info
schema_registry:   dict = {}   # database → schema dict


# ─── Low-level DB helpers ─────────────────────────────────────────────────────

def get_superuser_conn(database: str = "postgres", autocommit: bool = False):
    conn = psycopg2.connect(
        host=LOCAL_HOST,
        port=LOCAL_PORT,
        dbname=database,
        user=LOCAL_SUPERUSER,
        password=LOCAL_SUPERUSER_PASSWORD,
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
    """Create the role locally if it doesn't exist (no password needed for local trust auth)."""
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
    """
    Build a SQL column definition string from the information_schema column dict.
    """
    name     = col["column_name"]
    dtype    = col["data_type"]
    nullable = col["is_nullable"]
    default  = col["column_default"]
    max_len  = col.get("character_maximum_length")

    # Map information_schema types to Postgres DDL types
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
    if default and "nextval" not in default:   # skip serial sequences — handle separately
        parts.append(f"DEFAULT {default}")

    return " ".join(parts)


def create_tables(database: str, schema: dict):
    """
    Recreate every table from the schema dict inside the local database.
    Creates PKs, unique constraints, and foreign keys.
    """
    conn = get_superuser_conn(database=database)
    conn.autocommit = False
    cur  = conn.cursor()

    for table, defn in schema.items():
        columns      = defn["columns"]
        primary_keys = defn["primary_keys"]
        unique_cols  = defn["unique"]
        foreign_keys = defn["foreign_keys"]

        col_defs = [col_definition(c) for c in columns]

        if primary_keys:
            pk_cols = ", ".join(f'"{c}"' for c in primary_keys)
            col_defs.append(f"PRIMARY KEY ({pk_cols})")

        for uc in unique_cols:
            if uc not in primary_keys:          # don't double-declare PKs as UNIQUE
                col_defs.append(f'UNIQUE ("{uc}")')

        for fk in foreign_keys:
            col_defs.append(
                f'FOREIGN KEY ("{fk["column_name"]}")'
                f' REFERENCES "{fk["foreign_table"]}" ("{fk["foreign_column"]}")'
            )

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
            cur = conn.cursor()   # fresh cursor after error
            continue

    conn.commit()
    cur.close()
    conn.close()


def create_indexes(database: str, schema: dict):
    """Recreate non-primary indexes."""
    conn = get_superuser_conn(database=database, autocommit=True)
    cur  = conn.cursor()

    for table, defn in schema.items():
        for idx in defn["indexes"]:
            idx_def = idx["indexdef"]
            # indexdef already is a full CREATE INDEX statement — execute as-is
            # but use IF NOT EXISTS to be idempotent
            if "CREATE UNIQUE INDEX" in idx_def:
                idx_def = idx_def.replace("CREATE UNIQUE INDEX", "CREATE UNIQUE INDEX IF NOT EXISTS", 1)
            elif "CREATE INDEX" in idx_def:
                idx_def = idx_def.replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS", 1)
            try:
                cur.execute(idx_def)
                print(f"[client]   Index '{idx['indexname']}' on '{table}' created.")
            except psycopg2.Error as e:
                print(f"[client]   Skipping index '{idx['indexname']}': {e}")

    cur.close()
    conn.close()


def apply_permissions(database: str, user: str, permissions: dict):
    """
    Grant the same table-level privileges to `user` on the local database.
    permissions = { table_name: [privilege, ...] }
    """
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

    cur.close()
    conn.close()


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

    ensure_local_user(user)
    create_local_database(database)
    create_tables(database, schema)
    create_indexes(database, schema)
    apply_permissions(database, user, permissions)

    schema_registry[database] = schema
    local_cache_index[database] = {}   # no data cached yet

    print(f"[client] INIT_DB complete for '{database}'.")


# ─── Message I/O ──────────────────────────────────────────────────────────────

async def send_msg(writer: asyncio.StreamWriter, msg: dict):
    data = (json.dumps(msg) + "\n").encode()
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
                await send_msg(writer, {
                    "type":    "ERROR",
                    "message": str(e),
                })

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
