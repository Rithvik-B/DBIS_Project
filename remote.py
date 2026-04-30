"""
remote.py — Remote Brain
========================
Listens on port 5000. Sits in front of the actual remote PostgreSQL instance.
Responsibilities (Phase 1):
  - Accept proxy connections and handshakes
  - Handle CONNECT: authenticate user, register subscription, fetch schema + permissions
  - Send SCHEMA_TRANSFER back to proxy
"""

import asyncio
import json
import os
import psycopg2
import psycopg2.extras

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
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=4)

REMOTE_HOST = config.get("remote_db_host", "localhost")
REMOTE_PORT = config.get("remote_db_port", 5432)
REMOTE_SUPERUSER = config.get("remote_superuser", "postgres")
REMOTE_SUPERUSER_PASSWORD = config.get("remote_superuser_password", "postgres")
LISTEN_HOST = config.get("listen_host", "0.0.0.0")
LISTEN_PORT = config.get("listen_port", 5000)

# ─── Global State ─────────────────────────────────────────────────────────────

subscriptions: dict = {}   # client_id → {database, user}
locks: dict        = {}    # row_id    → client_id
clients: dict      = {}    # client_id → asyncio.StreamWriter


# ─── Helpers ──────────────────────────────────────────────────────────────────

def get_superuser_conn(database: str = "postgres"):
    """Return a superuser psycopg2 connection to the remote PostgreSQL."""
    return psycopg2.connect(
        host=REMOTE_HOST,
        port=REMOTE_PORT,
        dbname=database,
        user=REMOTE_SUPERUSER,
        password=REMOTE_SUPERUSER_PASSWORD,
    )


def authenticate_user(database: str, user: str, password: str = "") -> bool:
    """
    Try to open a real connection as `user` to verify credentials.
    For Phase 1 we allow passwordless logins (trust auth) — tighten later.
    """
    try:
        conn = psycopg2.connect(
            host=REMOTE_HOST,
            port=REMOTE_PORT,
            dbname=database,
            user=user,
            password=password,
            connect_timeout=5,
        )
        conn.close()
        return True
    except psycopg2.OperationalError as e:
        print(f"[remote] Auth failed for {user}@{database}: {e}")
        return False


def fetch_schema(database: str) -> dict:
    """
    Fetch full schema for every table in the public schema:
      - columns (name, type, nullable, default)
      - primary keys
      - unique constraints
      - foreign keys
      - indexes
    Returns a dict keyed by table name.
    """
    conn = get_superuser_conn(database)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    schema = {}

    # ── Tables ────────────────────────────────────────────────────────────────
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type   = 'BASE TABLE'
        ORDER BY table_name;
    """)
    tables = [row["table_name"] for row in cur.fetchall()]

    for table in tables:
        schema[table] = {
            "columns":      [],
            "primary_keys": [],
            "unique":       [],
            "foreign_keys": [],
            "indexes":      [],
        }

        # ── Columns ───────────────────────────────────────────────────────────
        cur.execute("""
            SELECT column_name,
                   data_type,
                   is_nullable,
                   column_default,
                   character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name   = %s
            ORDER BY ordinal_position;
        """, (table,))
        schema[table]["columns"] = [dict(r) for r in cur.fetchall()]

        # ── Primary keys ──────────────────────────────────────────────────────
        cur.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints     tc
            JOIN information_schema.key_column_usage      kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema    = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema    = 'public'
              AND tc.table_name      = %s
            ORDER BY kcu.ordinal_position;
        """, (table,))
        schema[table]["primary_keys"] = [r["column_name"] for r in cur.fetchall()]

        # ── Unique constraints ─────────────────────────────────────────────────
        cur.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints     tc
            JOIN information_schema.key_column_usage      kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema    = kcu.table_schema
            WHERE tc.constraint_type = 'UNIQUE'
              AND tc.table_schema    = 'public'
              AND tc.table_name      = %s;
        """, (table,))
        schema[table]["unique"] = [r["column_name"] for r in cur.fetchall()]

        # ── Foreign keys ──────────────────────────────────────────────────────
        cur.execute("""
            SELECT kcu.column_name,
                   ccu.table_name  AS foreign_table,
                   ccu.column_name AS foreign_column
            FROM information_schema.table_constraints     tc
            JOIN information_schema.key_column_usage      kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema    = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON tc.constraint_name = ccu.constraint_name
             AND tc.table_schema    = ccu.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema    = 'public'
              AND tc.table_name      = %s;
        """, (table,))
        schema[table]["foreign_keys"] = [dict(r) for r in cur.fetchall()]

        # ── Indexes ───────────────────────────────────────────────────────────
        cur.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename  = %s;
        """, (table,))
        schema[table]["indexes"] = [dict(r) for r in cur.fetchall()]

    cur.close()
    conn.close()
    return schema


def fetch_permissions(database: str, user: str) -> dict:
    """
    Fetch table-level privileges granted to `user` in `database`.
    Returns: { table_name: [privilege, ...] }
    """
    conn = get_superuser_conn(database)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT table_name, privilege_type
        FROM information_schema.role_table_grants
        WHERE grantee     = %s
          AND table_schema = 'public'
        ORDER BY table_name, privilege_type;
    """, (user,))

    perms: dict = {}
    for row in cur.fetchall():
        tbl  = row["table_name"]
        priv = row["privilege_type"]
        perms.setdefault(tbl, []).append(priv)

    cur.close()
    conn.close()
    return perms


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

    # 1. Authenticate
    if not authenticate_user(database, user, password):
        await send_msg(writer, {
            "type":    "ERROR",
            "message": f"Authentication failed for user '{user}' on database '{database}'"
        })
        return

    # 2. Register subscription
    subscriptions[client_id] = {"database": database, "user": user}
    print(f"[remote] Subscription registered: {subscriptions[client_id]}")

    # 3. Fetch schema
    print(f"[remote] Fetching schema for database '{database}' …")
    try:
        schema = fetch_schema(database)
    except Exception as e:
        await send_msg(writer, {"type": "ERROR", "message": f"Schema fetch failed: {e}"})
        return

    # 4. Fetch permissions
    print(f"[remote] Fetching permissions for user '{user}' …")
    try:
        permissions = fetch_permissions(database, user)
    except Exception as e:
        await send_msg(writer, {"type": "ERROR", "message": f"Permission fetch failed: {e}"})
        return

    # 5. Send back to proxy
    await send_msg(writer, {
        "type":        "SCHEMA_TRANSFER",
        "client_id":   client_id,
        "database":    database,
        "user":        user,
        "schema":      schema,
        "permissions": permissions,
    })
    print(f"[remote] SCHEMA_TRANSFER sent to {client_id}")


# ─── Connection Handler ────────────────────────────────────────────────────────

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    print(f"[remote] New connection from {addr}")

    while True:
        msg = await recv_msg(reader)
        if msg is None:
            print(f"[remote] Connection closed by {addr}")
            break

        msg_type = msg.get("type")

        if msg_type == "INIT":
            await handle_init(msg["client_id"], writer)

        elif msg_type == "CONNECT":
            await handle_connect(msg, writer)

        else:
            await send_msg(writer, {
                "type":    "ERROR",
                "message": f"Unknown message type: {msg_type}"
            })

    writer.close()


# ─── Entry Point ──────────────────────────────────────────────────────────────

async def main():
    server = await asyncio.start_server(handle_client, LISTEN_HOST, LISTEN_PORT)
    print(f"[remote] Listening on {LISTEN_HOST}:{LISTEN_PORT}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
