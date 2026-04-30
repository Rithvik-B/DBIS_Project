"""
proxy.py — Proxy / User Shell
==============================
Acts as a psql-like shell. Coordinates between remote.py (port 5000)
and client.py (port 5001).

<<<<<<< HEAD
Phase 1: CONNECT flow — schema replication.
Phase 2: SELECT (Type A / B with cache), INSERT (remote), UPDATE/DELETE (lock+local).
=======
<<<<<<< HEAD
Phase 1 flow:
  1. On startup: connect to remote.py and client.py, send INIT handshakes
  2. User types: CONNECT <database> USER <user>;
  3. Proxy → remote.py: CONNECT message
  4. remote.py responds with SCHEMA_TRANSFER
  5. Proxy → client.py: INIT_DB message
  6. client.py responds with INIT_DB_ACK
  7. Proxy reports success to user
=======
Phase 1: CONNECT flow — schema replication.
Phase 2: SELECT (Type A / B with cache), INSERT (remote), UPDATE/DELETE (lock+local).
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
"""

import asyncio
import json
import os
import uuid
import getpass

<<<<<<< HEAD
import query as qmod
=======
<<<<<<< HEAD
import query as qmod   # our query.py
=======
import query as qmod
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)


# ─── Configuration ────────────────────────────────────────────────────────────

CONFIG_FILE = "proxy_config.json"
if os.path.exists(CONFIG_FILE):
    with open(CONFIG_FILE, "r") as f:
        config = json.load(f)
else:
    config = {
<<<<<<< HEAD
        "remote_host": "192.168.1.100",
=======
<<<<<<< HEAD
        "remote_host": "192.168.1.100",  # IP address of the remote laptop
=======
        "remote_host": "192.168.1.100",
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
        "remote_port": 5000,
        "client_host": "localhost",
        "client_port": 5001
    }
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=4)

REMOTE_HOST = config.get("remote_host", "192.168.1.100")
REMOTE_PORT = config.get("remote_port", 5000)
CLIENT_HOST = config.get("client_host", "localhost")
CLIENT_PORT = config.get("client_port", 5001)

CLIENT_ID = f"proxy-{uuid.uuid4().hex[:8]}"

<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> 130b6a3 (phase-2)
# Session state (set after CONNECT)
_session: dict = {
    "database": None,
    "user":     None,
    "schema":   {},
}

<<<<<<< HEAD
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)

# ─── Message I/O ──────────────────────────────────────────────────────────────

async def send_msg(writer: asyncio.StreamWriter, msg: dict):
<<<<<<< HEAD
    data = (json.dumps(msg, default=str) + "\n").encode()
=======
<<<<<<< HEAD
    data = (json.dumps(msg) + "\n").encode()
=======
    data = (json.dumps(msg, default=str) + "\n").encode()
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


<<<<<<< HEAD
=======
<<<<<<< HEAD
# ─── Phase 1 Handlers ─────────────────────────────────────────────────────────
=======
>>>>>>> 130b6a3 (phase-2)
# ─── Display helpers ──────────────────────────────────────────────────────────

def _print_table(rows: list[dict], columns: list[str]):
    if not rows:
        print("(0 rows)")
        return
    cols = columns or list(rows[0].keys())
    widths = {c: max(len(str(c)), max(len(str(r.get(c, ""))) for r in rows)) for c in cols}
    sep    = "+" + "+".join("-" * (widths[c] + 2) for c in cols) + "+"
    header = "|" + "|".join(f" {c:<{widths[c]}} " for c in cols) + "|"
    print(sep)
    print(header)
    print(sep)
    for row in rows:
        line = "|" + "|".join(f" {str(row.get(c,'')):<{widths[c]}} " for c in cols) + "|"
        print(line)
    print(sep)
    print(f"({len(rows)} row{'s' if len(rows) != 1 else ''})")


# ─── Phase 1: CONNECT ─────────────────────────────────────────────────────────
<<<<<<< HEAD
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)

async def do_connect(
    parsed:        qmod.ParsedCommand,
    remote_reader: asyncio.StreamReader,
    remote_writer: asyncio.StreamWriter,
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
):
    database = parsed.params["database"]
    user     = parsed.params["user"]
    password = parsed.params["password"]

    if not password:
        loop = asyncio.get_event_loop()
<<<<<<< HEAD
        password = await loop.run_in_executor(
            None, getpass.getpass, f"Password for user {user}: "
        )
=======
<<<<<<< HEAD
        password = await loop.run_in_executor(None, getpass.getpass, f"Password for user {user}: ")
=======
        password = await loop.run_in_executor(
            None, getpass.getpass, f"Password for user {user}: "
        )
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)

    print(f"\n[proxy] → Sending CONNECT to remote.py …")
    await send_msg(remote_writer, {
        "type":      "CONNECT",
        "client_id": CLIENT_ID,
        "database":  database,
        "user":      user,
        "password":  password,
    })

<<<<<<< HEAD
=======
<<<<<<< HEAD
    # ── Wait for SCHEMA_TRANSFER ───────────────────────────────────────────────
    print("[proxy] ← Waiting for SCHEMA_TRANSFER from remote.py …")
    msg = await recv_msg(remote_reader)
    if msg is None:
        print("[proxy] ERROR: remote.py closed connection unexpectedly.")
        return

    if msg["type"] == "ERROR":
        print(f"[proxy] ERROR from remote.py: {msg['message']}")
        return

    if msg["type"] != "SCHEMA_TRANSFER":
        print(f"[proxy] Unexpected message type: {msg['type']}")
        return
=======
>>>>>>> 130b6a3 (phase-2)
    print("[proxy] ← Waiting for SCHEMA_TRANSFER from remote.py …")
    msg = await recv_msg(remote_reader)
    if msg is None:
        print("[proxy] ERROR: remote.py closed connection unexpectedly."); return
    if msg["type"] == "ERROR":
        print(f"[proxy] ERROR from remote.py: {msg['message']}"); return
    if msg["type"] != "SCHEMA_TRANSFER":
        print(f"[proxy] Unexpected message type: {msg['type']}"); return
<<<<<<< HEAD
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)

    schema      = msg["schema"]
    permissions = msg["permissions"]
    print(f"[proxy] ✓ Schema received — {len(schema)} table(s): {list(schema.keys())}")

<<<<<<< HEAD
=======
<<<<<<< HEAD
    # ── Forward to client.py ───────────────────────────────────────────────────
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
    print("[proxy] → Sending INIT_DB to client.py …")
    await send_msg(client_writer, {
        "type":        "INIT_DB",
        "database":    database,
        "user":        user,
        "schema":      schema,
        "permissions": permissions,
    })

<<<<<<< HEAD
=======
<<<<<<< HEAD
    # ── Wait for ACK ──────────────────────────────────────────────────────────
    print("[proxy] ← Waiting for INIT_DB_ACK from client.py …")
    ack = await recv_msg(client_reader)
    if ack is None:
        print("[proxy] ERROR: client.py closed connection unexpectedly.")
        return

    if ack["type"] == "ERROR":
        print(f"[proxy] ERROR from client.py: {ack['message']}")
        return

    if ack["type"] == "INIT_DB_ACK" and ack.get("status") == "ok":
        print(f"\n[proxy] ✅  Connected to '{database}' as '{user}'.")
        print(f"[proxy]    Local replica is ready (schema only, no data yet).")
=======
>>>>>>> 130b6a3 (phase-2)
    print("[proxy] ← Waiting for INIT_DB_ACK from client.py …")
    ack = await recv_msg(client_reader)
    if ack is None:
        print("[proxy] ERROR: client.py closed connection unexpectedly."); return
    if ack["type"] == "ERROR":
        print(f"[proxy] ERROR from client.py: {ack['message']}"); return

    if ack["type"] == "INIT_DB_ACK" and ack.get("status") == "ok":
        _session["database"] = database
        _session["user"]     = user
        _session["schema"]   = schema
        print(f"\n[proxy] ✅  Connected to '{database}' as '{user}'.")
        print(f"[proxy]    Local replica is ready (schema only, no data yet).\n")
<<<<<<< HEAD
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
    else:
        print(f"[proxy] Unexpected response from client.py: {ack}")


<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> 130b6a3 (phase-2)
# ─── Phase 2: SELECT Type A — always remote ───────────────────────────────────

async def do_select_type_a(
    parsed:        qmod.ParsedCommand,
    remote_reader: asyncio.StreamReader,
    remote_writer: asyncio.StreamWriter,
):
    database = _session["database"]
    print(f"[proxy] Type A query → remote")
    await send_msg(remote_writer, {
        "type":       "QUERY",
        "client_id":  CLIENT_ID,
        "database":   database,
        "query_type": "A",
        "sql":        parsed.raw,
    })
    msg = await recv_msg(remote_reader)
    if msg is None:
        print("[proxy] ERROR: no response from remote."); return
    if msg["type"] == "ERROR":
        print(f"[proxy] ERROR: {msg['message']}"); return
    _print_table(msg.get("rows", []), msg.get("columns", []))


# ─── Phase 2: SELECT Type B — cache-aware ─────────────────────────────────────

async def do_select_type_b(
    parsed:        qmod.ParsedCommand,
    remote_reader: asyncio.StreamReader,
    remote_writer: asyncio.StreamWriter,
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
):
    database    = _session["database"]
    fingerprint = parsed.fingerprint
    sql         = parsed.raw

    await send_msg(client_writer, {
        "type":        "CACHE_CHECK",
        "database":    database,
        "fingerprint": fingerprint,
        "sql":         sql,
    })
    ck = await recv_msg(client_reader)
    if ck is None:
        print("[proxy] ERROR: client did not respond to CACHE_CHECK."); return

    if ck["type"] == "CACHE_HIT":
        print(f"[proxy] Cache HIT (fp={fingerprint[:8]})")
        _print_table(ck.get("rows", []), ck.get("columns", []))
        return

    print(f"[proxy] Cache MISS → fetching from remote")
    await send_msg(remote_writer, {
        "type":        "QUERY",
        "client_id":   CLIENT_ID,
        "database":    database,
        "query_type":  "B",
        "sql":         sql,
        "table":       parsed.table,
        "where_clause": parsed.where_clause,
        "fingerprint": fingerprint,
    })
    msg = await recv_msg(remote_reader)
    if msg is None:
        print("[proxy] ERROR: no response from remote."); return
    if msg["type"] == "ERROR":
        print(f"[proxy] ERROR: {msg['message']}"); return

    rows    = msg.get("rows", [])
    cols    = msg.get("columns", [])
    pks     = msg.get("pks", [])
    pk_cols = msg.get("pk_cols", [])

    await send_msg(client_writer, {
        "type":        "CACHE_ROWS",
        "database":    database,
        "table":       parsed.table,
        "rows":        rows,
        "pks":         pks,
        "pk_cols":     pk_cols,
        "lock_type":   "READ",
        "fingerprint": fingerprint,
    })
    ack = await recv_msg(client_reader)
    if ack and ack["type"] == "ERROR":
        print(f"[proxy] Cache store error: {ack['message']}")

    _print_table(rows, cols)


# ─── Phase 2: INSERT — always remote ─────────────────────────────────────────

async def do_insert(
    parsed:        qmod.ParsedCommand,
    remote_reader: asyncio.StreamReader,
    remote_writer: asyncio.StreamWriter,
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
):
    database = _session["database"]
    print(f"[proxy] INSERT → remote (always)")
    await send_msg(remote_writer, {
        "type":       "QUERY",
        "client_id":  CLIENT_ID,
        "database":   database,
        "query_type": "INSERT",
        "sql":        parsed.raw,
        "table":      parsed.table,
    })
    msg = await recv_msg(remote_reader)
    if msg is None:
        print("[proxy] ERROR: no response from remote."); return
    if msg["type"] == "ERROR":
        print(f"[proxy] ERROR: {msg['message']}"); return
    print(f"INSERT {msg.get('rowcount', 0)}")


# ─── Phase 2: UPDATE / DELETE — lock + local apply ────────────────────────────

async def do_write(
    parsed:        qmod.ParsedCommand,
    remote_reader: asyncio.StreamReader,
    remote_writer: asyncio.StreamWriter,
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
):
    database = _session["database"]
    table    = parsed.table
    schema   = _session["schema"].get(table, {})
    pk_cols  = schema.get("primary_keys", [])

    if not pk_cols:
        print(f"[proxy] ERROR: cannot determine PKs for table '{table}'.")
        return

    # Step 1: resolve matching PKs via a remote SELECT
    pk_select = f'SELECT {", ".join(pk_cols)} FROM "{table}" WHERE {parsed.where_clause};'
    print(f"[proxy] Resolving PKs: {pk_select}")
    await send_msg(remote_writer, {
        "type":       "QUERY",
        "client_id":  CLIENT_ID,
        "database":   database,
        "query_type": "A",
        "sql":        pk_select,
    })
    pk_msg = await recv_msg(remote_reader)
    if pk_msg is None or pk_msg["type"] == "ERROR":
        print(f"[proxy] ERROR resolving PKs: {pk_msg}"); return

    pk_rows = pk_msg.get("rows", [])
    if not pk_rows:
        print("(0 rows affected)")
        return

    pks = []
    for row in pk_rows:
        if len(pk_cols) == 1:
            pks.append(str(row[pk_cols[0]]))
        else:
            pks.append(json.dumps([str(row[c]) for c in pk_cols]))

    # Step 2: request WRITE lock from remote
    print(f"[proxy] Requesting WRITE lock on {len(pks)} PK(s) …")
    await send_msg(remote_writer, {
        "type":      "LOCK_REQUEST",
        "client_id": CLIENT_ID,
        "database":  database,
        "table":     table,
        "pks":       pks,
    })
    lock_msg = await recv_msg(remote_reader)
    if lock_msg is None or lock_msg["type"] != "LOCK_GRANT":
        print(f"[proxy] Lock not granted: {lock_msg}"); return
    print(f"[proxy] ✓ WRITE lock granted")

    # Step 3: apply write to local DB
    await send_msg(client_writer, {
        "type":     "WRITE_LOCAL",
        "database": database,
        "table":    table,
        "sql":      parsed.raw,
        "pk_cols":  pk_cols,
        "pks":      pks,
    })
    ack = await recv_msg(client_reader)
    if ack is None:
        print("[proxy] ERROR: no response from client."); return
    if ack["type"] == "ERROR":
        print(f"[proxy] ERROR applying write: {ack['message']}"); return

    verb = parsed.command_type.name
    print(f"{verb} {ack.get('rowcount', 0)}")
    print(f"[proxy] (changes held locally; synced to remote on lock release)")


<<<<<<< HEAD
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
# ─── Bootstrap ────────────────────────────────────────────────────────────────

async def bootstrap(
    remote_reader: asyncio.StreamReader,
    remote_writer: asyncio.StreamWriter,
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
):
<<<<<<< HEAD
=======
<<<<<<< HEAD
    """Send INIT handshakes to both servers."""
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
    init_msg = {"type": "INIT", "client_id": CLIENT_ID}

    await send_msg(remote_writer, init_msg)
    remote_ack = await recv_msg(remote_reader)
    if remote_ack and remote_ack.get("type") == "INIT_ACK":
        print(f"[proxy] ✓ remote.py handshake OK (client_id={CLIENT_ID})")
    else:
        print(f"[proxy] WARNING: unexpected INIT response from remote: {remote_ack}")

    await send_msg(client_writer, init_msg)
    client_ack = await recv_msg(client_reader)
    if client_ack and client_ack.get("type") == "INIT_ACK":
        print(f"[proxy] ✓ client.py handshake OK")
    else:
        print(f"[proxy] WARNING: unexpected INIT response from client: {client_ack}")


# ─── Main REPL ────────────────────────────────────────────────────────────────

async def main():
<<<<<<< HEAD
=======
<<<<<<< HEAD
    # Connect to remote.py
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
    print(f"[proxy] Connecting to remote.py at {REMOTE_HOST}:{REMOTE_PORT} …")
    remote_reader, remote_writer = await asyncio.open_connection(REMOTE_HOST, REMOTE_PORT)
    print("[proxy] Connected to remote.py")

<<<<<<< HEAD
=======
<<<<<<< HEAD
    # Connect to client.py
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
    print(f"[proxy] Connecting to client.py at {CLIENT_HOST}:{CLIENT_PORT} …")
    client_reader, client_writer = await asyncio.open_connection(CLIENT_HOST, CLIENT_PORT)
    print("[proxy] Connected to client.py")

<<<<<<< HEAD
=======
<<<<<<< HEAD
    # Handshake both
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
    await bootstrap(remote_reader, remote_writer, client_reader, client_writer)

    print("\n[proxy] Ready. Type SQL-like commands (e.g. CONNECT mydb USER alice;)")
    print("[proxy] Type 'exit' to quit.\n")

<<<<<<< HEAD
=======
<<<<<<< HEAD
    # REPL
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)
    loop = asyncio.get_event_loop()
    while True:
        try:
            raw = await loop.run_in_executor(None, input, "proxy> ")
        except (EOFError, KeyboardInterrupt):
            print("\n[proxy] Exiting.")
            break

        raw = raw.strip()
        if not raw:
            continue
        if raw.lower() in ("exit", "quit", r"\q"):
            print("[proxy] Goodbye.")
            break

        parsed = qmod.parse(raw)

        if parsed.command_type == qmod.CommandType.CONNECT:
<<<<<<< HEAD
=======
<<<<<<< HEAD
            await do_connect(
                parsed,
                remote_reader, remote_writer,
                client_reader, client_writer,
            )

        elif parsed.command_type == qmod.CommandType.UNKNOWN:
            print(f"[proxy] Unknown command. (Phase 2 will handle queries.)")

        else:
            print(f"[proxy] Command type '{parsed.command_type.name}' not yet implemented (Phase 2).")
=======
>>>>>>> 130b6a3 (phase-2)
            await do_connect(parsed, remote_reader, remote_writer,
                             client_reader, client_writer)

        elif parsed.command_type == qmod.CommandType.SELECT:
            if not _session["database"]:
                print("[proxy] Not connected. Use CONNECT first."); continue
            if parsed.route_type == qmod.RouteType.TYPE_B:
                await do_select_type_b(parsed, remote_reader, remote_writer,
                                       client_reader, client_writer)
            else:
                await do_select_type_a(parsed, remote_reader, remote_writer)

        elif parsed.command_type == qmod.CommandType.INSERT:
            if not _session["database"]:
                print("[proxy] Not connected."); continue
            await do_insert(parsed, remote_reader, remote_writer,
                            client_reader, client_writer)

        elif parsed.command_type in (qmod.CommandType.UPDATE, qmod.CommandType.DELETE):
            if not _session["database"]:
                print("[proxy] Not connected."); continue
            await do_write(parsed, remote_reader, remote_writer,
                           client_reader, client_writer)

        elif parsed.command_type == qmod.CommandType.UNKNOWN:
            print(f"[proxy] Unknown command.")
<<<<<<< HEAD
=======
>>>>>>> 6f6a987 (phase-2)
>>>>>>> 130b6a3 (phase-2)

    remote_writer.close()
    client_writer.close()


if __name__ == "__main__":
    asyncio.run(main())
